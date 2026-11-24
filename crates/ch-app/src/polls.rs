//! Stored poll actions; no creation path or renderer-selected RPC.
use crate::{App, events, projection, wire::{Failure, Result, Thread}};
use ch_protocol::{common::is_valid_id, methods::{MessagesGetParams, OptionalMessageResult, PollsCloseParams, PollsVoteParams}};
use serde_json::{Value, json};

pub(crate) enum Operation { Vote(Vec<String>), Close, Refresh }

impl App {
    pub(crate) async fn poll_action(&self, thread: Thread, id: String, operation: Operation) -> Result<Value> {
        let _lock = self.action_lock(&thread).await?;
        let (owner, rows) = self.action_targets(&thread, std::slice::from_ref(&id)).await?;
        let row = &rows[0];
        if row.deleted_at.is_some() || row.is_scheduled { return Err(Failure::Conflict); }
        let poll = row.poll.as_ref().ok_or(Failure::Conflict)?;
        let state = self.core.host().plugin_state(&owner.plugin_id).ok_or(Failure::Unavailable)?;
        if !state.capabilities().ok_or(Failure::Unavailable)?.has("messages.get") { return Err(Failure::Unsupported); }
        let read = matches!(operation, Operation::Refresh);
        let result: OptionalMessageResult = match operation {
            Operation::Refresh => {
                let params = MessagesGetParams { account_id: thread.account_id.clone(), chat_id: thread.chat_id.clone(), message_id: id.clone() };
                self.core.host().call(&owner.plugin_id, "messages.get", &params).await.map_err(|e| Failure::from(&e))?
            }
            Operation::Vote(ids) => {
                if poll.closed { return Err(Failure::Conflict); }
                let unique: std::collections::BTreeSet<_> = ids.iter().collect();
                if unique.len() != ids.len() || ids.iter().any(|id| !is_valid_id(id) || !poll.options.iter().any(|o| &o.id == id))
                    || !poll.multiple && ids.len() > 1 { return Err(Failure::Invalid); }
                if ids.is_empty() && (poll.quiz || !poll.options.iter().any(|o| o.chosen == Some(true))) { return Err(Failure::Conflict); }
                if poll.quiz && (poll.options.is_empty() || poll.options.iter().any(|o| o.chosen != Some(false))) { return Err(Failure::Conflict); }
                let params = PollsVoteParams { account_id: thread.account_id.clone(), chat_id: thread.chat_id.clone(), message_id: id.clone(), option_ids: ids };
                self.action_call(&owner, "polls.vote", &params).await?
            }
            Operation::Close => {
                if poll.closed || row.direction != "out" || row.forward_from.is_some() { return Err(Failure::Conflict); }
                let params = PollsCloseParams { account_id: thread.account_id.clone(), chat_id: thread.chat_id.clone(), message_id: id.clone() };
                self.action_call(&owner, "polls.close", &params).await?
            }
        };
        let Some(mut message) = result.message else {
            if !read { events::notify(&self.changes, &thread.account_id); }
            return Ok(json!({"poll":null}));
        };
        let invalid = message.id != id || message.chat_id != thread.chat_id
            || message.topic_id.as_ref().is_some_and(|t| Some(t) != thread.topic_id.as_ref())
            || message.poll.as_ref().and_then(|p|p.id.as_ref()).zip(poll.id.as_ref()).is_some_and(|(got,have)|got != have);
        if invalid { return Err(if read { Failure::Internal } else { self.action_unknown(&thread.account_id) }); }
        message.topic_id.clone_from(&thread.topic_id);
        let reported = message.poll.is_some();
        let account = thread.account_id.clone();
        if read {
            let plugin = owner.plugin_id.clone();
            let aid = account.clone();
            let applied = self.core.ingest().with_ingest(move |i| i.apply_edited(&plugin, &aid, &message)).await?.map_err(|_| Failure::Internal)?;
            self.core.bus().publish_local("poll-refresh", applied);
        } else {
            self.action_apply(&account, move |i| i.apply_edited(&owner.plugin_id, &thread.account_id, &message)).await?;
        }
        if !reported { return Ok(json!({"poll":null})); }
        let thread = rows[0].conversation_id;
        let snapshot = self.read(move |db| Ok(ch_db::get_message(db.conn(), thread, &id)?.and_then(|m| projection::poll(&m)))).await;
        let snapshot = match snapshot { Ok(value) => value, Err(error) => return Err(if read { error } else { self.action_unknown(&account) }) };
        Ok(json!({"poll":snapshot}))
    }
}
