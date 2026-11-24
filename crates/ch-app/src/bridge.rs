use crate::{
    App, events,
    projection::{self, blocking},
    wire::{Failure, Request, Result, Thread, Window},
};
use ch_core::{
    outbox::{OutboxKind, OutboxRequest},
    runtime::NewAccount,
    search::{Scope, SearchRequest},
};
use ch_db::types::{AccountRow, ConversationRow};
use ch_protocol::{
    Empty,
    common::is_valid_id,
    dto::Entity,
    methods::{AuthBeginParams, AuthCancelParams, AuthSubmitParams, MessagesSendParams},
};
use serde_json::{Value, json};

impl App {
    pub(crate) async fn account(&self, id: &str) -> Result<AccountRow> {
        if !is_valid_id(id) {
            return Err(Failure::Invalid);
        }
        self.core.accounts().get(id).await?.ok_or(Failure::Missing)
    }

    pub(crate) async fn thread(&self, key: &Thread) -> Result<(ConversationRow, bool)> {
        if !is_valid_id(&key.account_id)
            || !is_valid_id(&key.chat_id)
            || key.topic_id.as_deref().is_some_and(|id| !is_valid_id(id))
        {
            return Err(Failure::Invalid);
        }
        let key = key.clone();
        self.read(move |db| {
            let c = ch_db::get_conversation(db.conn(), &key.account_id, &key.chat_id)?
                .filter(|c| !c.is_gone)
                .ok_or(Failure::Missing)?;
            let closed = match key.topic_id {
                Some(topic) => {
                    ch_db::get_topic(db.conn(), c.id, &topic)?
                        .filter(|t| !t.is_gone)
                        .ok_or(Failure::Missing)?
                        .closed
                }
                None => false,
            };
            Ok((c, closed))
        })
        .await
    }

    pub(crate) async fn dispatch(&self, request: Request) -> Result<Value> {
        match request {
            Request::Inventory {} => self.inventory().await,
            Request::VotePoll { thread, message_id, option_ids } => self.poll_action(thread, message_id, crate::polls::Operation::Vote(option_ids)).await,
            Request::ClosePoll { thread, message_id } => self.poll_action(thread, message_id, crate::polls::Operation::Close).await,
            Request::RefreshPoll { thread, message_id } => self.poll_action(thread, message_id, crate::polls::Operation::Refresh).await,
            Request::EditText { thread, message_id, text, entities } => self.edit_text(thread, message_id, text, entities).await,
            Request::DeleteMessages { thread, message_ids, revoke } => self.delete_messages(thread, message_ids, revoke).await,
            Request::PinMessage { thread, message_id, silent } => self.pin_message(thread, message_id, Some(silent)).await,
            Request::UnpinMessage { thread, message_id } => self.pin_message(thread, message_id, None).await,
            Request::ListChats {
                account_id,
                archived,
            } => {
                self.account(&account_id).await?;
                self.read(move |db| {
                    Ok(Value::Array(
                        ch_db::list_conversations(db.conn(), &account_id, archived)?
                            .into_iter()
                            .map(projection::chat)
                            .collect(),
                    ))
                })
                .await
            }
            Request::History {
                thread,
                limit,
                window,
            } => {
                self.thread(&thread).await?;
                let h = self.history.clone();
                let ck = thread.chat_key();
                let page = match window {
                    Window::Latest => blocking(move || Ok(h.open(&ck, limit)?)).await?,
                    Window::Refresh => h.refresh(&ck, limit).await?,
                    Window::Older { before } => h.older(&ck, before.as_deref(), limit).await?,
                    Window::Newer { after } => h.newer(&ck, &after, limit).await?,
                };
                Ok(projection::history(page))
            }
            Request::FillGap { thread, gap_id } => {
                let (c, _) = self.thread(&thread).await?;
                let id = gap_id
                    .parse::<i64>()
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or(Failure::Invalid)?;
                let topic = thread.topic_id.clone().unwrap_or_default();
                self.read(move |db| {
                    if ch_db::list_history_gaps(db.conn(), c.id, &topic)?
                        .iter()
                        .any(|g| g.id == id)
                    {
                        Ok(())
                    } else {
                        Err(Failure::Missing)
                    }
                })
                .await?;
                Ok(json!({"closed":self.history.fill_gap(&thread.chat_key(),id).await?.closed}))
            }
            Request::Search {
                thread,
                query,
                offset,
                limit,
                utc_offset_minutes,
            } => {
                self.thread(&thread).await?;
                if !(-1440..=1440).contains(&utc_offset_minutes) {
                    return Err(Failure::Invalid);
                }
                let s = self.search.clone();
                let req = SearchRequest {
                    query,
                    scope: Scope::Chat {
                        account_id: thread.account_id,
                        chat_id: thread.chat_id,
                        topic_id: thread.topic_id,
                    },
                    offset,
                    limit,
                    utc_offset_minutes,
                    ..Default::default()
                };
                blocking(move || Ok(projection::search(s.search(&req)?))).await
            }
            Request::EnqueueText {
                thread,
                text,
                entities,
            } => {
                if text.trim().is_empty() {
                    return Err(Failure::Invalid);
                }
                let (c, closed) = self.thread(&thread).await?;
                if closed || c.can_post == Some(false) {
                    return Err(Failure::Conflict);
                }
                let owner = self.account(&thread.account_id).await?;
                let entities = if entities.is_empty() {
                    None
                } else {
                    let state = self
                        .core
                        .host()
                        .plugin_state(&owner.plugin_id)
                        .ok_or(Failure::Unavailable)?;
                    if !state
                        .capabilities()
                        .ok_or(Failure::Unavailable)?
                        .has("send.entities")
                    {
                        return Err(Failure::Unsupported);
                    }
                    let entities = entities.into_iter().map(Entity::from).collect::<Vec<_>>();
                    validate_spans(&text, &entities)?;
                    Some(entities)
                };
                let p = MessagesSendParams {
                    account_id: thread.account_id.clone(),
                    chat_id: thread.chat_id,
                    topic_id: thread.topic_id,
                    text,
                    entities,
                    ..Default::default()
                };
                let outbox = self.outbox.clone();
                blocking(move || {
                    Ok(projection::outbox(outbox.enqueue(
                        &thread.account_id,
                        OutboxRequest::SendText(p),
                        None,
                    )?))
                })
                .await
            }
            Request::ListOutbox { thread } => {
                self.thread(&thread).await?;
                let outbox = self.outbox.clone();
                blocking(move || {
                    Ok(Value::Array(
                        outbox
                            .list(&thread.account_id)?
                            .into_iter()
                            .filter(|i| {
                                i.kind == OutboxKind::SendText
                                    && i.chat_id == thread.chat_id
                                    && (thread.topic_id.is_none() || i.topic_id == thread.topic_id)
                            })
                            .map(projection::outbox)
                            .collect(),
                    ))
                })
                .await
            }
            Request::CancelOutbox { thread, id } => self.update_outbox(thread, id, false).await,
            Request::RetryOutbox { thread, id } => self.update_outbox(thread, id, true).await,
            Request::AddAccount {
                plugin_id,
                label,
                settings,
            } => {
                let row = self
                    .core
                    .accounts()
                    .add(NewAccount {
                        plugin_id,
                        label,
                        settings,
                        account_id: None,
                        start: true,
                    })
                    .await?;
                Ok(json!({"accountId":row.account_id}))
            }
            Request::GetSettings { account_id } => {
                let s = self.core.accounts().settings(&account_id).await?;
                Ok(json!({"values":s.values,"secretsSet":s.secrets_set}))
            }
            Request::SaveSettings { account_id, values } => {
                self.core
                    .accounts()
                    .set_settings(&account_id, values)
                    .await?;
                Ok(Value::Null)
            }
            Request::GetAuthStep { account_id } => {
                let a = self.account(&account_id).await?;
                let cache = self.steps.lock().unwrap_or_else(|e| e.into_inner());
                Ok(cache
                    .get(&account_id)
                    .filter(|(p, _)| p == &a.plugin_id)
                    .map_or(Value::Null, |(_, s)| projection::auth(s)))
            }
            Request::BeginAuth { account_id, method } => {
                let a = self.account(&account_id).await?;
                let _lock = self.lock_auth(&account_id).await;
                let p = self
                    .core
                    .host()
                    .plugin(&a.plugin_id)
                    .ok_or(Failure::Missing)?;
                if !p
                    .state
                    .capabilities()
                    .ok_or(Failure::Unavailable)?
                    .has("auth.interactive")
                {
                    return Err(Failure::Unsupported);
                }
                if !p
                    .info
                    .manifest
                    .is_some_and(|m| m.auth_methods.iter().any(|v| v.as_str() == method))
                {
                    return Err(Failure::Invalid);
                }
                self.clear_auth(&account_id);
                let params = AuthBeginParams { account_id, method };
                self.core
                    .host()
                    .call::<_, Empty>(&a.plugin_id, "auth.begin", &params)
                    .await
                    .map_err(|e| Failure::from(&e))?;
                Ok(Value::Null)
            }
            Request::SubmitAuth {
                account_id,
                flow,
                step_id,
                value,
            } => {
                let a = self.account(&account_id).await?;
                let _lock = self.lock_auth(&account_id).await;
                self.check_step(&a, &flow, Some(&step_id))?;
                self.core
                    .host()
                    .call::<_, Empty>(
                        &a.plugin_id,
                        "auth.submit",
                        &AuthSubmitParams {
                            account_id,
                            flow,
                            step_id,
                            value,
                        },
                    )
                    .await
                    .map_err(|e| Failure::from(&e))?;
                Ok(Value::Null)
            }
            Request::CancelAuth { account_id, flow } => {
                let a = self.account(&account_id).await?;
                let _lock = self.lock_auth(&account_id).await;
                self.check_step(&a, &flow, None)?;
                self.core
                    .host()
                    .call::<_, Empty>(
                        &a.plugin_id,
                        "auth.cancel",
                        &AuthCancelParams {
                            account_id: account_id.clone(),
                            flow,
                        },
                    )
                    .await
                    .map_err(|e| Failure::from(&e))?;
                self.clear_auth(&account_id);
                Ok(Value::Null)
            }
        }
    }

    async fn lock_auth(&self, id: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = self
            .auth_locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(id.to_owned())
            .or_default()
            .clone();
        lock.lock_owned().await
    }
    fn clear_auth(&self, id: &str) {
        self.steps
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id);
        events::notify(&self.changes, id);
    }
    fn check_step(&self, a: &AccountRow, flow: &str, step: Option<&str>) -> Result<()> {
        let cache = self.steps.lock().unwrap_or_else(|e| e.into_inner());
        if cache.get(&a.account_id).is_some_and(|(owner, s)| {
            owner == &a.plugin_id && s.flow == flow && step.is_none_or(|id| s.step_id == id)
        }) {
            Ok(())
        } else {
            Err(Failure::Conflict)
        }
    }

    async fn update_outbox(&self, thread: Thread, id: String, retry: bool) -> Result<Value> {
        self.thread(&thread).await?;
        let outbox = self.outbox.clone();
        blocking(move || {
            let item = outbox.get_by_client_id(&id)?.ok_or(Failure::Missing)?;
            if item.kind != OutboxKind::SendText
                || item.account_id != thread.account_id
                || item.chat_id != thread.chat_id
                || item.topic_id != thread.topic_id
            {
                return Err(Failure::Missing);
            }
            Ok(projection::outbox(if retry {
                outbox.retry(item.id)?
            } else {
                outbox.cancel(item.id)?
            }))
        })
        .await
    }
}

pub(crate) fn validate_spans(text: &str, entities: &[Entity]) -> Result<()> {
    let mut boundaries = vec![0u64];
    let mut pos = 0;
    for c in text.chars() {
        pos += c.len_utf16() as u64;
        boundaries.push(pos);
    }
    for e in entities {
        let end = e.offset.checked_add(e.length).ok_or(Failure::Invalid)?;
        if e.length == 0
            || boundaries.binary_search(&e.offset).is_err()
            || boundaries.binary_search(&end).is_err()
        {
            return Err(Failure::Invalid);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ch_db::types::TopicRow;
    use std::path::Path;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closed_topic_remains_readable_and_cannot_enqueue() {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let app = App::demo(&repo).await.unwrap();
        let account = app
            .core
            .accounts()
            .add(NewAccount::new("fake").no_start())
            .await
            .unwrap()
            .account_id;
        let key = json!({"accountId":account,"chatId":"closed-topic-test","topicId":"closed"});
        app.read(move |db| {
            let chat = ConversationRow {
                account_id: account,
                chat_id: "closed-topic-test".into(),
                kind: "group".into(),
                can_post: Some(true),
                ..Default::default()
            };
            let conversation_id = ch_db::insert_conversation(db.conn(), &chat)?;
            ch_db::insert_topic(
                db.conn(),
                &TopicRow {
                    conversation_id,
                    topic_id: "closed".into(),
                    title: "Closed".into(),
                    closed: true,
                    ..Default::default()
                },
            )?;
            Ok(())
        })
        .await
        .unwrap();
        let history=app.request(json!({"method":"history","params":{"thread":key,"window":{"kind":"latest"},"limit":5}})).await;
        assert_eq!(history["ok"], true);
        let send = app
            .request(json!({"method":"enqueueText","params":{"thread":key,"text":"closed topic"}}))
            .await;
        assert_eq!(send["error"]["code"], "conflict");
        let items = app
            .request(json!({"method":"listOutbox","params":{"thread":key}}))
            .await;
        assert_eq!(items["value"], json!([]));
        app.shutdown().await;
    }
}
