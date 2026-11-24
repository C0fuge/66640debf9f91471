//! Typed application mutations. No renderer-selected plugin or arbitrary RPC method.
use crate::{App, bridge::validate_spans, events, wire::{Failure, Result, Span, Thread}};
use ch_core::{host::{AccountPhase, HostError}, ingest::{Applied, Ingest, IngestError}};
use ch_db::types::{AccountRow, MessageRow};
use ch_protocol::{Empty, ErrorCode, common::is_valid_id, dto::Entity, methods::{MessagesEditParams, MessagesEditResult, MessagesDeleteParams, MessagesPinParams, MessagesUnpinParams}};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

impl App {
    // Concurrent clicks cannot mutate a thread twice while a response is pending.
    pub(crate) async fn action_lock(&self, thread: &Thread) -> Result<tokio::sync::OwnedMutexGuard<()>> {
        let key = serde_json::to_string(thread).map_err(|_| Failure::Invalid)?;
        let lock = {
            let mut locks = self.action_locks.lock().unwrap_or_else(|e| e.into_inner());
            locks.retain(|_, weak| weak.strong_count() > 0);
            let lock = locks.get(&key).and_then(std::sync::Weak::upgrade)
                .unwrap_or_else(|| std::sync::Arc::new(tokio::sync::Mutex::new(())));
            locks.insert(key, std::sync::Arc::downgrade(&lock));
            lock
        };
        lock.try_lock_owned().map_err(|_| Failure::Conflict)
    }

    pub(crate) async fn action_targets(&self, thread: &Thread, ids: &[String]) -> Result<(AccountRow, Vec<MessageRow>)> {
        if ids.is_empty() || ids.len() > 100 || ids.iter().any(|id| !is_valid_id(id) || id.starts_with("local:")) {
            return Err(Failure::Invalid);
        }
        let unique: std::collections::BTreeSet<_> = ids.iter().collect();
        if unique.len() != ids.len() { return Err(Failure::Invalid); }
        let owner = self.account(&thread.account_id).await?;
        let (chat, _) = self.thread(thread).await?; // Closed topics remain actionable.
        let ids = ids.to_vec();
        let topic = thread.topic_id.clone();
        let rows = self.read(move |db| {
            ids.iter().map(|id| {
                let row = ch_db::get_message(db.conn(), chat.id, id)?.ok_or(Failure::Missing)?;
                if row.topic_id != topic { return Err(Failure::Missing); }
                if matches!(row.delivery.as_deref(), Some("sending" | "failed")) { return Err(Failure::Conflict); }
                Ok(row)
            }).collect::<Result<Vec<_>>>()
        }).await?;
        if !owner.enabled || !self.core.host().account(&owner.plugin_id, &owner.account_id)
            .is_some_and(|a| matches!(a.phase, AccountPhase::Running)) {
            return Err(Failure::Unavailable);
        }
        Ok((owner, rows))
    }

    pub(crate) async fn action_call<P: Serialize, R: DeserializeOwned>(&self, owner: &AccountRow, method: &str, params: &P) -> Result<R> {
        let value = serde_json::to_value(params).map_err(|_| Failure::Invalid)?;
        let state = self.core.host().plugin_state(&owner.plugin_id).ok_or(Failure::Unavailable)?;
        let caps = state.capabilities().ok_or(Failure::Unavailable)?;
        let info = ch_protocol::method(method).ok_or(Failure::Internal)?;
        if info.capability.is_some_and(|c| !caps.has(c)) || info.missing_capabilities(&value, |c| caps.has(c)).next().is_some() {
            return Err(Failure::Unsupported);
        }
        // The host repeats these checks against the live handshake before writing.
        self.core.host().call(&owner.plugin_id, method, params).await.map_err(|error| {
            if definitive_rejection(&error) { Failure::from(&error) }
            else { self.action_unknown(&owner.account_id) }
        })
    }

    pub(crate) fn action_unknown(&self, account: &str) -> Failure {
        events::notify(&self.changes, account);
        Failure::OutcomeUnknown
    }

    pub(crate) async fn action_apply(&self, account: &str, apply: impl FnOnce(&mut Ingest) -> std::result::Result<Applied, IngestError> + Send + 'static) -> Result<Value> {
        match self.core.ingest().with_ingest(apply).await {
            Ok(Ok(applied)) => {
                self.core.bus().publish_local("message-actions", applied);
                Ok(Value::Null)
            }
            _ => Err(self.action_unknown(account)),
        }
    }

    pub(crate) async fn edit_text(&self, thread: Thread, id: String, text: String, spans: Vec<Span>) -> Result<Value> {
        if text.trim().is_empty() { return Err(Failure::Invalid); }
        let entities: Vec<Entity> = spans.into_iter().map(Entity::from).collect();
        validate_spans(&text, &entities)?;
        let _lock = self.action_lock(&thread).await?;
        let (owner, rows) = self.action_targets(&thread, std::slice::from_ref(&id)).await?;
        let row = &rows[0];
        if row.direction != "out" || row.text.is_none() || row.media.is_some() || row.action.is_some()
            || row.poll.is_some() || row.location.is_some() || row.contact_card.is_some() || row.dice.is_some()
            || row.deleted_at.is_some() || row.is_scheduled { return Err(Failure::Conflict); }
        let params = MessagesEditParams { account_id: thread.account_id.clone(), chat_id: thread.chat_id.clone(),
            message_id: id.clone(), text: Some(text), entities: Some(entities), ..Default::default() };
        let result: MessagesEditResult = self.action_call(&owner, "messages.edit", &params).await?;
        // MessageResult.message is REQUIRED; typed decoding rejects an absent message.
        if result.message.id != id || result.message.chat_id != thread.chat_id || result.message.topic_id != thread.topic_id {
            return Err(self.action_unknown(&thread.account_id));
        }
        let account = thread.account_id.clone();
        self.action_apply(&account, move |ingest| ingest.apply_edited(&owner.plugin_id, &thread.account_id, &result.message)).await
    }

    pub(crate) async fn delete_messages(&self, thread: Thread, ids: Vec<String>, revoke: bool) -> Result<Value> {
        let _lock = self.action_lock(&thread).await?;
        let (owner, _) = self.action_targets(&thread, &ids).await?;
        let params = MessagesDeleteParams { account_id: thread.account_id.clone(), chat_id: thread.chat_id.clone(), message_ids: ids, revoke };
        let _: Empty = self.action_call(&owner, "messages.delete", &params).await?;
        let account = thread.account_id.clone();
        // Intentionally uses the existing hard-delete rule, including remote tombstones.
        self.action_apply(&account, move |ingest| ingest.apply_deleted(&thread.account_id, &thread.chat_id, &params.message_ids)).await
    }

    pub(crate) async fn pin_message(&self, thread: Thread, id: String, silent: Option<bool>) -> Result<Value> {
        let _lock = self.action_lock(&thread).await?;
        let (owner, rows) = self.action_targets(&thread, std::slice::from_ref(&id)).await?;
        if rows[0].deleted_at.is_some() { return Err(Failure::Conflict); }
        let pinned = silent.is_some();
        if let Some(silent) = silent {
            let params = MessagesPinParams { account_id: thread.account_id.clone(), chat_id: thread.chat_id.clone(), message_id: id.clone(), silent: Some(silent), for_me_only: None };
            let _: Empty = self.action_call(&owner, "messages.pin", &params).await?;
        } else {
            let params = MessagesUnpinParams { account_id: thread.account_id.clone(), chat_id: thread.chat_id.clone(), message_id: Some(id.clone()) };
            let _: Empty = self.action_call(&owner, "messages.unpin", &params).await?;
        }
        let account = thread.account_id.clone();
        self.action_apply(&account, move |ingest| ingest.apply_pinned(&thread.account_id, &thread.chat_id, Some(&id), pinned)).await
    }
}

fn definitive_rejection(error: &HostError) -> bool {
    match error {
        HostError::UnknownPlugin(_) | HostError::UnknownAccount(_) | HostError::PluginNotRunning { .. }
        | HostError::InvalidMethod(..) | HostError::AccountBusy(..) | HostError::AccountAlreadyStarted(_)
        | HostError::SingleAccount(_) => true,
        HostError::Rpc(error) => matches!(error.code(), ErrorCode::InvalidParams | ErrorCode::InvalidRequest
            | ErrorCode::NotFound | ErrorCode::UnknownAccount | ErrorCode::MethodNotFound
            | ErrorCode::NotSupported | ErrorCode::AuthRequired | ErrorCode::Forbidden),
        _ => false,
    }
}

#[cfg(test)]
#[path = "actions_tests.rs"]
mod tests;
