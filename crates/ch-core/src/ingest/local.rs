//! Core-initiated effects (spec §10.1): when a request to the plugin succeeds, the core applies
//! the result itself and stays idempotent for the platform's echo events.
//!
//! The outbox state machine (retries, cancellation, upload progress) is a later task; it will
//! call [`Ingest::apply_local_echo`] before sending and [`Ingest::apply_sent`] /
//! [`Ingest::apply_send_failed`] after.

use ch_db::types::*;
use ch_db::{Millis, now_millis};
use ch_protocol::dto::*;
use ch_protocol::{Id, Timestamp};
use rusqlite::Connection;

use super::{Applied, Change, Ingest, Result};

/// Flags the user can flip on a chat through `chats.set_flags` and friends. `None` = unchanged.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ChatFlags {
    pub archived: Option<bool>,
    pub muted: Option<bool>,
    pub mute_until: Option<Option<Timestamp>>,
    pub pinned: Option<bool>,
    pub marked_unread: Option<bool>,
}

/// What a local echo of a send looks like before the plugin answers.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LocalEcho {
    pub chat_id: Id,
    pub topic_id: Option<Id>,
    /// Must match the outbox row's `client_id`; the echo's `message_id` is `local:<client_id>`.
    pub client_id: String,
    pub text: Option<String>,
    pub entities: Option<Vec<Entity>>,
    pub media: Option<Media>,
    pub reply_to: Option<ReplyRef>,
    pub sender_id: Option<Id>,
    pub sender_name: Option<String>,
    pub scheduled_at: Option<Timestamp>,
}

impl Ingest {
    fn local_tx<T>(
        &self,
        plugin_or_account: &str,
        name: &str,
        f: impl FnOnce(&Connection, &mut Applied, Millis) -> Result<T>,
    ) -> Result<(Applied, T)> {
        let tx = self.db.transaction()?;
        let mut out = Applied::new(plugin_or_account, name);
        let v = f(&tx, &mut out, now_millis())?;
        tx.commit()?;
        Ok((out, v))
    }

    fn require_account(&self, conn: &Connection, account_id: &str) -> Result<AccountRow> {
        ch_db::get_account(conn, account_id)?.ok_or_else(|| super::IngestError::UnknownAccount {
            account_id: account_id.to_owned(),
        })
    }

    /// Insert the local echo of a message about to be sent (`delivery: sending`). Returns the
    /// message rowid. Calling it again for the same `client_id` returns the existing row.
    pub fn apply_local_echo(&self, account_id: &str, echo: &LocalEcho) -> Result<(Applied, i64)> {
        let tx = self.db.transaction()?;
        let result = self.apply_local_echo_in(&tx, account_id, echo)?;
        tx.commit()?;
        Ok(result)
    }

    /// Add an echo inside the caller's write transaction, after any admission checks.
    pub(crate) fn apply_local_echo_in(
        &self,
        conn: &rusqlite::Transaction<'_>,
        account_id: &str,
        echo: &LocalEcho,
    ) -> Result<(Applied, i64)> {
        let now = now_millis();
        let mut out = Applied::new(account_id, "local.echo");
        self.require_account(conn, account_id)?;
        let (conv_id, _) = self.conversation_or_stub(conn, account_id, &echo.chat_id, now)?;
        let key = format!("local:{}", echo.client_id);
        if let Some(row) = ch_db::get_message(conn, conv_id, &key)? {
            return Ok((out, row.id));
        }
        let row = MessageRow {
            conversation_id: conv_id,
            message_id: key.clone(),
            topic_id: echo.topic_id.clone(),
            direction: "out".into(),
            sent_at: echo.scheduled_at.map(ch_db::ts_to_millis).unwrap_or(now),
            sender_id: echo.sender_id.clone(),
            sender_name: echo.sender_name.clone(),
            text: echo.text.clone(),
            entities: echo.entities.clone(),
            media: echo.media.clone(),
            reply_to: echo.reply_to.clone(),
            delivery: Some("sending".into()),
            is_scheduled: echo.scheduled_at.is_some(),
            created_at: now,
            updated_at: now,
            ..Default::default()
        };
        let id = ch_db::insert_message(conn, &row)?;
        self.bump_preview(conn, conv_id, &MessageRow { id, ..row })?;
        out.push(Change::MessagesUpserted {
            chat_id: echo.chat_id.clone(),
            created: vec![key],
            updated: vec![],
        });
        Ok((out, id))
    }

    /// A `messages.send` / `send_file` / `send_album` / `forward` succeeded: adopt the local echo
    /// (when `client_id` is given and the echo exists) and merge the returned message(s). The
    /// plugin's `messages.upsert` echo is then a no-op.
    pub fn apply_sent(
        &self,
        plugin_id: &str,
        account_id: &str,
        client_id: Option<&str>,
        messages: &[Message],
    ) -> Result<Applied> {
        Ok(self
            .local_tx(account_id, "local.sent", |conn, out, now| {
                self.check_account(conn, plugin_id, account_id)?;
                if let Some(cid) = client_id
                    && let Some(item) = ch_db::get_outbox_by_client_id(conn, cid)?
                    && item.method == "messages.send"
                    && item.params.get("reply_to").is_some_and(|value| !value.is_null())
                    && (item.account_id != account_id || messages.len() != 1
                        || messages[0].direction != Direction::Out
                        || !super::send_destination_matches(conn, &item, &messages[0])?)
                {
                    // Validate the actual observation before a requested local
                    // echo can be rekeyed/merged, or a foreign stub can be made.
                    return Err(ch_db::Error::Invalid("send result did not confirm the retained reply".into()).into());
                }
                if let (Some(cid), Some(first)) = (client_id, messages.first()) {
                    let (conv_id, _) =
                        self.conversation_or_stub(conn, account_id, &first.chat_id, now)?;
                    let key = format!("local:{cid}");
                    if let Some(echo) = ch_db::get_message(conn, conv_id, &key)?
                        && ch_db::get_message(conn, conv_id, &first.id)?.is_none()
                    {
                        // Re-key the echo to the platform id; the merge below fills the rest.
                        let mut re = echo.clone();
                        re.message_id.clone_from(&first.id);
                        re.updated_at = now;
                        ch_db::update_message(conn, &re)?;
                    }
                    if let Some(o) = ch_db::get_outbox_by_client_id(conn, cid)? {
                        let ids: Vec<String> = messages.iter().map(|m| m.id.clone()).collect();
                        ch_db::set_outbox_sent(conn, o.id, &ids)?;
                    }
                }
                self.upsert_messages(conn, plugin_id, account_id, messages, now, out)?;
                Ok(())
            })?
            .0)
    }

    /// A send failed for good: the echo shows `failed` (Retry button); the outbox row keeps the
    /// error (set by the outbox task).
    pub fn apply_send_failed(
        &self,
        account_id: &str,
        chat_id: &str,
        client_id: &str,
    ) -> Result<Applied> {
        Ok(self
            .local_tx(account_id, "local.send_failed", |conn, out, now| {
                let Some(conv_id) = ch_db::conversation_rowid(conn, account_id, chat_id)? else {
                    out.ignore("send failed for unknown chat");
                    return Ok(());
                };
                let key = format!("local:{client_id}");
                if let Some(mut row) = ch_db::get_message(conn, conv_id, &key)?
                    && row.delivery.as_deref() != Some("failed")
                {
                    row.delivery = Some("failed".into());
                    row.updated_at = now;
                    ch_db::update_message(conn, &row)?;
                    out.push(Change::MessagesUpserted {
                        chat_id: chat_id.to_owned(),
                        created: vec![],
                        updated: vec![key],
                    });
                }
                Ok(())
            })?
            .0)
    }

    /// `messages.edit` succeeded: merge the returned message (snapshotting the old content when
    /// it differs). The platform's `message.edited` echo is then a no-op.
    pub fn apply_edited(
        &self,
        plugin_id: &str,
        account_id: &str,
        message: &Message,
    ) -> Result<Applied> {
        Ok(self
            .local_tx(account_id, "local.edited", |conn, out, now| {
                self.check_account(conn, plugin_id, account_id)?;
                self.upsert_messages(
                    conn,
                    plugin_id,
                    account_id,
                    std::slice::from_ref(message),
                    now,
                    out,
                )
            })?
            .0)
    }

    /// `messages.delete` succeeded. The user deleted these on purpose, so the rows go (no
    /// tombstone); the platform's `messages.deleted` echo then finds nothing.
    pub fn apply_deleted(
        &self,
        account_id: &str,
        chat_id: &str,
        message_ids: &[Id],
    ) -> Result<Applied> {
        Ok(self
            .local_tx(account_id, "local.deleted", |conn, out, now| {
                self.delete_messages(
                    conn,
                    account_id,
                    Some(chat_id),
                    message_ids,
                    "me",
                    false,
                    now,
                    out,
                )
            })?
            .0)
    }

    /// `read.send_receipt` succeeded (or the user read the chat with ghost mode on): read
    /// watermark, unread counters and the forced-unread dot are cleared locally.
    pub fn apply_read(
        &self,
        account_id: &str,
        chat_id: &str,
        topic_id: Option<&str>,
        max_message_id: &str,
    ) -> Result<Applied> {
        Ok(self
            .local_tx(account_id, "local.read", |conn, out, now| {
                self.on_read_inbox(
                    conn,
                    account_id,
                    chat_id,
                    topic_id,
                    max_message_id,
                    Some(0),
                    now,
                    out,
                )?;
                if let Some(conv_id) = ch_db::conversation_rowid(conn, account_id, chat_id)?
                    && let Some(mut c) = ch_db::get_conversation_by_id(conn, conv_id)?
                {
                    c.local_read_at = Some(now);
                    if topic_id.is_none() {
                        c.unread_mentions = 0;
                        c.unread_reactions = 0;
                    }
                    c.updated_at = now;
                    ch_db::update_conversation(conn, &c)?;
                }
                Ok(())
            })?
            .0)
    }

    /// `messages.pin` / `messages.unpin` succeeded. `message_id: None` with `pinned: false` unpins all.
    pub fn apply_pinned(
        &self,
        account_id: &str,
        chat_id: &str,
        message_id: Option<&str>,
        pinned: bool,
    ) -> Result<Applied> {
        Ok(self
            .local_tx(account_id, "local.pinned", |conn, out, now| {
                let Some(conv_id) = ch_db::conversation_rowid(conn, account_id, chat_id)? else {
                    out.ignore("pin for unknown chat");
                    return Ok(());
                };
                let updated: Vec<Id> = match message_id {
                    Some(mid) => match ch_db::get_message(conn, conv_id, mid)? {
                        Some(row) if row.is_pinned != Some(pinned) => {
                            ch_db::set_message_pinned(conn, row.id, pinned, now)?;
                            vec![mid.to_owned()]
                        }
                        Some(_) => vec![],
                        None => {
                            out.ignore("pin for unknown message");
                            return Ok(());
                        }
                    },
                    None => {
                        let ids: Vec<Id> = ch_db::list_pinned(conn, conv_id)?
                            .into_iter()
                            .map(|m| m.message_id)
                            .collect();
                        ch_db::unpin_all(conn, conv_id, now)?;
                        ids
                    }
                };
                if !updated.is_empty() {
                    out.push(Change::MessagesUpserted {
                        chat_id: chat_id.to_owned(),
                        created: vec![],
                        updated,
                    });
                }
                Ok(())
            })?
            .0)
    }

    /// `drafts.save` succeeded, or the user typed (drafts are local-first): store with `now` as
    /// `updated_at`, so an older `draft.changed` echo is ignored.
    pub fn apply_draft(
        &self,
        account_id: &str,
        chat_id: &str,
        topic_id: Option<&str>,
        draft: &Draft,
    ) -> Result<Applied> {
        Ok(self
            .local_tx(account_id, "local.draft", |conn, out, _now| {
                let d = Draft {
                    updated_at: Some(Timestamp::now()),
                    ..draft.clone()
                };
                self.set_draft(conn, account_id, chat_id, topic_id, &d, None, out)
            })?
            .0)
    }

    /// `reactions.set` succeeded. With the returned message, merge it; without, rewrite this
    /// account's reactions from `keys` (counts adjusted by ±1).
    pub fn apply_reactions_set(
        &self,
        plugin_id: &str,
        account_id: &str,
        chat_id: &str,
        message_id: &str,
        keys: &[ReactionKey],
        result: Option<&Message>,
    ) -> Result<Applied> {
        Ok(self
            .local_tx(account_id, "local.reactions", |conn, out, now| {
                if let Some(m) = result {
                    return self.upsert_messages(
                        conn,
                        plugin_id,
                        account_id,
                        std::slice::from_ref(m),
                        now,
                        out,
                    );
                }
                let Some(conv_id) = ch_db::conversation_rowid(conn, account_id, chat_id)? else {
                    out.ignore("reactions for unknown chat");
                    return Ok(());
                };
                let Some(row) = ch_db::get_message(conn, conv_id, message_id)? else {
                    out.ignore("reactions for unknown message");
                    return Ok(());
                };
                let mut current = ch_db::reactions_dto(conn, row.id)?;
                for r in &mut current {
                    let want = keys.contains(&r.key);
                    if r.mine && !want {
                        r.mine = false;
                        r.count = r.count.saturating_sub(1);
                    } else if !r.mine && want {
                        r.mine = true;
                        r.count += 1;
                    }
                }
                for k in keys {
                    if !current.iter().any(|r| &r.key == k) {
                        current.push(Reaction {
                            key: k.clone(),
                            count: 1,
                            mine: true,
                            my_count: None,
                        });
                    }
                }
                current.retain(|r| r.count > 0);
                if ch_db::set_reactions(conn, row.id, &current)? {
                    out.push(Change::ReactionsChanged {
                        chat_id: chat_id.to_owned(),
                        message_id: message_id.to_owned(),
                    });
                }
                Ok(())
            })?
            .0)
    }

    /// `chats.set_flags` (archive, mute, pin, mark unread) succeeded.
    pub fn apply_chat_flags(
        &self,
        account_id: &str,
        chat_id: &str,
        flags: &ChatFlags,
    ) -> Result<Applied> {
        Ok(self
            .local_tx(account_id, "local.chat_flags", |conn, out, now| {
                let Some(c) = ch_db::get_conversation(conn, account_id, chat_id)? else {
                    out.ignore("flags for unknown chat");
                    return Ok(());
                };
                let mut n = c.clone();
                if let Some(v) = flags.archived {
                    n.archived = v;
                }
                if let Some(v) = flags.muted {
                    n.muted = Some(v);
                }
                if let Some(v) = &flags.mute_until {
                    n.mute_until = v.map(ch_db::ts_to_millis);
                }
                if let Some(v) = flags.pinned {
                    n.pinned = Some(v);
                }
                if let Some(v) = flags.marked_unread {
                    n.marked_unread = Some(v);
                }
                if n != c {
                    n.updated_at = now;
                    ch_db::update_conversation(conn, &n)?;
                    out.push(Change::ConversationUpserted {
                        chat_id: chat_id.to_owned(),
                        created: false,
                    });
                }
                Ok(())
            })?
            .0)
    }

    /// A `messages.fetch_recent` / `history.around` / `pins.list` page came back: ingest the
    /// messages and, for `fetch_recent` pages (`older_cursor: Some(next)`), record how far back
    /// history goes (`next_cursor == None` means the start of the chat was reached).
    pub fn apply_history_page(
        &self,
        plugin_id: &str,
        account_id: &str,
        chat_id: &str,
        topic_id: Option<&str>,
        messages: &[Message],
        older_cursor: Option<Option<&str>>,
    ) -> Result<Applied> {
        self.apply_history_page_with(
            plugin_id,
            account_id,
            chat_id,
            topic_id,
            messages,
            older_cursor,
            |_, _, _| Ok(()),
        )
        .map(|(applied, ())| applied)
    }

    /// Commit a fetched page together with the history service's cursor/gap bookkeeping.
    /// The callback runs inside the same transaction, after the page is applied, and receives
    /// the requested conversation rowid and transaction timestamp. Nothing is published here.
    /// A callback error rolls back every message, cursor and metadata write.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_history_page_with<T>(
        &self,
        plugin_id: &str,
        account_id: &str,
        chat_id: &str,
        topic_id: Option<&str>,
        messages: &[Message],
        older_cursor: Option<Option<&str>>,
        finish: impl FnOnce(&Connection, i64, Millis) -> Result<T>,
    ) -> Result<(Applied, T)> {
        self.apply_history_page_inner(plugin_id, account_id, chat_id, topic_id, messages, older_cursor, None, None, finish)
    }

    /// History owns the exact fresh Accounts binding through this transaction.
    /// The final authority check is the local page-write linearization point.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_history_page_owned<T>(
        &self, plugin_id: &str, account_id: &str, chat_id: &str, topic_id: Option<&str>,
        messages: &[Message], older_cursor: Option<Option<&str>>,
        owner: &crate::runtime::AccountStorageObservation,
        stop: &tokio_util::sync::CancellationToken,
        finish: impl FnOnce(&Connection, i64, Millis) -> Result<T>,
    ) -> Result<(Applied, T)> {
        self.apply_history_page_inner(plugin_id, account_id, chat_id, topic_id, messages, older_cursor,
            Some(owner), Some(stop), finish)
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_history_page_inner<T>(
        &self, plugin_id: &str, account_id: &str, chat_id: &str, topic_id: Option<&str>,
        messages: &[Message], older_cursor: Option<Option<&str>>,
        owner: Option<&crate::runtime::AccountStorageObservation>,
        stop: Option<&tokio_util::sync::CancellationToken>,
        finish: impl FnOnce(&Connection, i64, Millis) -> Result<T>,
    ) -> Result<(Applied, T)> {
        self.local_tx(account_id, "local.history_page", |conn, out, now| {
            check_history_authority(conn, owner, stop)?;
            self.check_account(conn, plugin_id, account_id)?;
            let existing_conv = ch_db::conversation_rowid(conn, account_id, chat_id)?;
            // Validate the ENTIRE effective page before any write. Missing topic ids keep
            // known state; new/unknown ids may inherit the requested topic context.
            let mut validated = Vec::with_capacity(messages.len());
            for message in messages {
                if message.chat_id != chat_id {
                    return Err(
                        ch_db::Error::Invalid("history page contains another chat".into()).into(),
                    );
                }
                let mut message = message.clone();
                if let Some(expected_topic) = topic_id {
                    let known_topic = match existing_conv {
                        Some(conv) => ch_db::get_message(conn, conv, &message.id)?
                            .and_then(|row| row.topic_id),
                        None => None,
                    };
                    let effective_topic = message.topic_id.as_deref().or(known_topic.as_deref());
                    if effective_topic.is_some_and(|topic| topic != expected_topic) {
                        return Err(ch_db::Error::Invalid(
                            "history page contains another topic".into(),
                        )
                        .into());
                    }
                    if effective_topic.is_none() {
                        message.topic_id = Some(expected_topic.to_owned());
                    }
                }
                validated.push(message);
            }
            let messages = validated.as_slice();
            self.upsert_messages(conn, plugin_id, account_id, messages, now, out)?;
            let (conv_id, _) = self.conversation_or_stub(conn, account_id, chat_id, now)?;
            let key = topic_id.unwrap_or("");
            let mut h = ch_db::get_history_state(conn, conv_id, key)?.unwrap_or(HistoryStateRow {
                conversation_id: conv_id,
                topic_id: key.to_owned(),
                ..Default::default()
            });
            for m in messages {
                let at = ch_db::ts_to_millis(m.sent_at);
                if h.oldest_sent_at.is_none_or(|o| at < o) {
                    h.oldest_sent_at = Some(at);
                    h.oldest_message_id = Some(m.id.clone());
                }
                if h.newest_sent_at.is_none_or(|n| at > n) {
                    h.newest_sent_at = Some(at);
                    h.newest_message_id = Some(m.id.clone());
                }
            }
            if let Some(next) = older_cursor {
                h.older_cursor = next.map(str::to_owned);
                h.reached_start = next.is_none();
            }
            h.last_fetch_at = Some(now);
            h.updated_at = now;
            ch_db::set_history_state(conn, &h)?;
            let value = finish(conn, conv_id, now)?;
            check_history_authority(conn, owner, stop)?;
            Ok(value)
        })
    }

    /// A `topics.list` page: merge topics without hiding the rest.
    pub fn apply_topics_page(
        &self,
        plugin_id: &str,
        account_id: &str,
        chat_id: &str,
        topics: &[Topic],
    ) -> Result<Applied> {
        Ok(self
            .local_tx(account_id, "local.topics_page", |conn, out, now| {
                self.check_account(conn, plugin_id, account_id)?;
                self.upsert_topics(conn, account_id, chat_id, topics, false, now, out)
            })?
            .0)
    }

    /// `members.list` result: replace what the core knows about a chat's members.
    pub fn apply_members(
        &self,
        account_id: &str,
        chat_id: &str,
        members: &[Member],
    ) -> Result<Applied> {
        Ok(self
            .local_tx(account_id, "local.members", |conn, out, now| {
                let (conv_id, _) = self.conversation_or_stub(conn, account_id, chat_id, now)?;
                let contacts: Vec<Contact> = members.iter().map(|m| m.user.clone()).collect();
                self.upsert_contacts(conn, account_id, &contacts, now, out)?;
                for m in members {
                    ch_db::upsert_member(
                        conn,
                        &MemberRow {
                            conversation_id: conv_id,
                            user_id: m.user.id.clone(),
                            role: m.role.as_str().to_owned(),
                            rank: m.rank.clone(),
                            joined_at: m.joined_at.map(ch_db::ts_to_millis),
                            invited_by: m.invited_by.clone(),
                            updated_at: now,
                        },
                    )?;
                }
                Ok(())
            })?
            .0)
    }

    /// Commit only observed preference fields. Validate the owner and
    /// row inside the same transaction so a late read cannot recreate a chat.
    pub fn apply_observed_chat_flags(
        &self,
        owner: &AccountRow,
        row: &ConversationRow,
        pinned: Option<bool>,
        marked_unread: Option<bool>,
        archived: Option<bool>,
        is_current: impl FnOnce() -> bool,
    ) -> Result<Option<Applied>> {
        let (applied, matched) = self.local_tx(&owner.account_id, "local.observed_chat_flags", |conn, out, now| {
            if !is_current() || !ch_db::get_account(conn, &owner.account_id)?.is_some_and(|a|
                a.enabled && a.plugin_id == owner.plugin_id && a.created_at == owner.created_at) {
                return Ok(false);
            }
            let Some(mut current) = ch_db::get_conversation(conn, &owner.account_id, &row.chat_id)?
                .filter(|c| !c.is_gone && c.id == row.id && c.created_at == row.created_at && c.kind == row.kind)
            else { return Ok(false); };
            let flags = (pinned.or(current.pinned), marked_unread.or(current.marked_unread), archived.unwrap_or(current.archived));
            if flags != (current.pinned, current.marked_unread, current.archived) {
                (current.pinned, current.marked_unread, current.archived) = flags;
                current.updated_at = now;
                ch_db::update_conversation(conn, &current)?;
                out.push(Change::ConversationUpserted { chat_id: row.chat_id.clone(), created: false });
            }
            Ok(true)
        })?;
        Ok(matched.then_some(applied))
    }

    /// Any other method result that returns a `Conversation` (`chats.get`, `groups.create`, …).
    pub fn apply_conversation(
        &self,
        plugin_id: &str,
        account_id: &str,
        c: &Conversation,
    ) -> Result<Applied> {
        Ok(self
            .local_tx(account_id, "local.conversation", |conn, out, now| {
                self.check_account(conn, plugin_id, account_id)?;
                self.upsert_conversation(conn, account_id, c, now, out)
                    .map(|_| ())
            })?
            .0)
    }

    /// Any other method result that returns `Contact`s (`contacts.list`, `lookup.*`).
    pub fn apply_contacts(
        &self,
        plugin_id: &str,
        account_id: &str,
        contacts: &[Contact],
    ) -> Result<Applied> {
        Ok(self
            .local_tx(account_id, "local.contacts", |conn, out, now| {
                self.check_account(conn, plugin_id, account_id)?;
                self.upsert_contacts(conn, account_id, contacts, now, out)
            })?
            .0)
    }
}

fn check_history_authority(
    conn: &Connection,
    owner: Option<&crate::runtime::AccountStorageObservation>,
    stop: Option<&tokio_util::sync::CancellationToken>,
) -> Result<()> {
    if stop.is_some_and(|token| token.is_cancelled()) {
        return Err(ch_db::Error::Invalid("history service is closed".into()).into());
    }
    if let Some(owner) = owner {
        owner.check(conn).map_err(|error| match error {
            crate::runtime::RuntimeError::Db(error) => super::IngestError::Db(error),
            _ => super::IngestError::UnknownAccount { account_id:owner.owner().account_id.clone() },
        })?;
    }
    Ok(())
}
