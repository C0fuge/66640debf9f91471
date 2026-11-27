//! Ingest: applies plugin events to the database, idempotently.
//!
//! [`Ingest::apply`] takes one `ch_protocol::Event` for a `(plugin_id, account_id)` and applies it
//! in a single transaction, returning an [`Applied`] that lists what changed (the UI layer turns
//! that into change notifications). Rules (spec §8.4, §10, §10.1):
//!
//! - `Conversation` / `Topic` / `Message` are merged field by field; absent = unchanged; on-create
//!   defaults apply only on insert.
//! - `message.edited` (and any upsert) snapshots the previous content into `message_version` only
//!   when text, entities or media really differ; an older edit (`edited_at` behind what the row
//!   holds) does not clobber newer content.
//! - Applying the same event twice is a no-op; echoes of core-initiated changes change nothing.
//! - Deletions become tombstones when `keep_deleted` is on, otherwise rows go.
//! - FTS is kept in sync by the schema's triggers.
//!
//! Core-initiated effects (a successful `messages.send`, `messages.edit`, `read.send_receipt`, …)
//! are applied through the methods in [`local`] (re-exported here); the plugin's echo events are
//! then no-ops.

mod local;
pub mod merge;

use std::sync::Arc;

use ch_db::types::*;
use ch_db::{Db, Millis, now_millis, ts_to_millis};
use ch_protocol::dto::*;
use ch_protocol::events::*;
use ch_protocol::{Event, Id};
use rusqlite::Connection;

pub use local::{ChatFlags, LocalEcho};

/// What ingest should do beyond the bare DTO rules. Each maps to a user setting.
#[derive(Clone)]
pub struct IngestOptions {
    /// Keep deleted messages as tombstones (content kept) instead of removing them.
    pub keep_deleted: bool,
    /// Snapshot the previous content when a message is edited.
    pub keep_edit_history: bool,
    /// Write online/typing/read events into `presence_log`.
    pub log_presence: bool,
    /// `(account_id, path) -> allowed`. A path the filter rejects is dropped from the DTO (spec §15).
    /// `None` accepts every path (tests).
    pub path_filter: Option<PathFilter>,
}

/// `(account_id, path) -> allowed` (spec §15).
pub type PathFilter = Arc<dyn Fn(&str, &str) -> bool + Send + Sync>;

impl Default for IngestOptions {
    fn default() -> Self {
        Self {
            keep_deleted: true,
            keep_edit_history: true,
            log_presence: false,
            path_filter: None,
        }
    }
}

impl std::fmt::Debug for IngestOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestOptions")
            .field("keep_deleted", &self.keep_deleted)
            .field("keep_edit_history", &self.keep_edit_history)
            .field("log_presence", &self.log_presence)
            .field("path_filter", &self.path_filter.is_some())
            .finish()
    }
}

/// Ingest errors. Anything else is a bug or a broken database.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// The event names an account the core does not have.
    #[error("unknown account {account_id}")]
    UnknownAccount {
        /// The account id from the event.
        account_id: String,
    },
    /// The account exists but belongs to another plugin.
    #[error("account {account_id} belongs to plugin {expected}, not {got}")]
    PluginMismatch {
        /// The account id from the event.
        account_id: String,
        /// The plugin the account is registered with.
        expected: String,
        /// The plugin that sent the event.
        got: String,
    },
    /// Storage failed.
    #[error(transparent)]
    Db(#[from] ch_db::Error),
}

impl From<rusqlite::Error> for IngestError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Db(ch_db::Error::Sqlite(e))
    }
}

/// Result alias.
pub type Result<T> = std::result::Result<T, IngestError>;

/// One thing that changed in the database.
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    /// `status`: account state (and `me`) recorded.
    AccountStatus {
        state: String,
        detail: Option<String>,
        reconnecting: bool,
        me: Option<Id>,
    },
    /// A conversation row was inserted (`created`) or changed.
    ConversationUpserted { chat_id: Id, created: bool },
    /// `conversations.synced`: chats no longer listed / listed again.
    ConversationsSynced { gone: Vec<Id>, back: Vec<Id> },
    /// Messages inserted / changed in a chat.
    MessagesUpserted {
        chat_id: Id,
        created: Vec<Id>,
        updated: Vec<Id>,
    },
    /// A message's content changed; `snapshot` says a `message_version` was written.
    MessageEdited {
        chat_id: Id,
        message_id: Id,
        snapshot: bool,
    },
    /// Messages tombstoned (`tombstoned`) or removed.
    MessagesDeleted {
        chat_id: Id,
        message_ids: Vec<Id>,
        tombstoned: bool,
    },
    /// The reaction set of a message changed.
    ReactionsChanged { chat_id: Id, message_id: Id },
    /// Handles inserted / changed; identity side effects counted.
    ContactsUpserted {
        user_ids: Vec<Id>,
        persons_created: usize,
        auto_linked: usize,
    },
    /// `folders.synced` applied.
    FoldersSynced { folder_ids: Vec<Id>, removed: usize },
    /// Topics inserted / changed / hidden in a chat.
    TopicsUpserted {
        chat_id: Id,
        created: Vec<Id>,
        updated: Vec<Id>,
        gone: Vec<Id>,
    },
    /// The peer read up to `max_message_id`; `marked_read` outgoing messages flipped to `read`.
    ReadOutbox {
        chat_id: Id,
        topic_id: Option<Id>,
        max_message_id: Id,
        marked_read: usize,
    },
    /// This account read up to `max_message_id` elsewhere.
    ReadInbox {
        chat_id: Id,
        topic_id: Option<Id>,
        max_message_id: Id,
        unread_count: Option<u64>,
    },
    /// Presence: stored (online) and/or transient (action) — the UI shows typing from this.
    Presence {
        user_id: Id,
        chat_id: Option<Id>,
        topic_id: Option<Id>,
        online: Option<OnlineStatus>,
        action: Option<PresenceAction>,
        expires_at: Option<ch_protocol::Timestamp>,
    },
    /// A draft was written (`cleared` = removed).
    DraftChanged {
        chat_id: Id,
        topic_id: Option<Id>,
        cleared: bool,
    },
    /// An avatar path was recorded.
    AvatarReady {
        kind: AvatarKind,
        id: Id,
        path: String,
    },
    /// A message's media paths were recorded.
    MediaReady {
        chat_id: Id,
        message_id: Id,
        thumb_path: Option<String>,
        path: Option<String>,
    },
    /// Not stored; passed through for the UI.
    TransferProgress(TransferProgressEvent),
    /// Not stored; passed through for the auth flow.
    AuthStep(AuthStep),
    /// The event (or part of it) referred to something the core does not have.
    Ignored { reason: String },
}

/// The outcome of applying one event.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Applied {
    /// The account the event was for.
    pub account_id: String,
    /// The event name (spec §10) or the local operation name.
    pub event: String,
    /// Everything that changed, in order.
    pub changes: Vec<Change>,
    /// Paths the filter rejected and dropped (spec §15).
    pub dropped_paths: usize,
}

impl Applied {
    fn new(account_id: &str, event: &str) -> Self {
        Self {
            account_id: account_id.to_owned(),
            event: event.to_owned(),
            ..Default::default()
        }
    }

    /// Did the database change at all? (`Presence` actions and pass-through events count as
    /// changes for the UI's sake; only `Ignored` does not.)
    pub fn is_noop(&self) -> bool {
        self.changes
            .iter()
            .all(|c| matches!(c, Change::Ignored { .. }))
    }

    fn push(&mut self, c: Change) {
        self.changes.push(c);
    }

    fn ignore(&mut self, reason: impl Into<String>) {
        self.changes.push(Change::Ignored {
            reason: reason.into(),
        });
    }
}

/// Applies events and core-initiated effects to one database.
pub struct Ingest {
    db: Db,
    opts: IngestOptions,
}

impl std::fmt::Debug for Ingest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ingest").field("opts", &self.opts).finish()
    }
}

impl Ingest {
    /// Wrap an open database.
    pub fn new(db: Db, opts: IngestOptions) -> Self {
        Self { db, opts }
    }

    /// The database (for the services that read it).
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Current options.
    pub fn options(&self) -> &IngestOptions {
        &self.opts
    }

    /// Replace the options (a setting changed).
    pub fn set_options(&mut self, opts: IngestOptions) {
        self.opts = opts;
    }

    /// Give the database back.
    pub fn into_db(self) -> Db {
        self.db
    }

    /// Apply one plugin → core event for `plugin_id` inside a transaction.
    pub fn apply(&self, plugin_id: &str, event: Event) -> Result<Applied> {
        self.apply_with_owner(plugin_id, event, None)
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn apply_owned(&self, plugin_id: &str, event: Event,
        owners: &crate::runtime::LogicalOwners, origin: &crate::runtime::LogicalOwnerObservation) -> Result<Applied> {
        self.apply_with_owner(plugin_id, event, Some((owners, origin)))
    }

    fn apply_with_owner(&self, plugin_id: &str, event: Event,
        owner: Option<(&crate::runtime::LogicalOwners, &crate::runtime::LogicalOwnerObservation)>) -> Result<Applied> {
        let account_id = event_account(&event).to_owned();
        let name = event.name();
        let tx = self.db.transaction()?;
        if let Some((owners, origin)) = owner {
            if origin.account_id() != account_id || origin.plugin_id() != plugin_id || !owners.validate(&tx, origin)? {
                // Old queued native input is an acknowledged no-op, never a
                // lookup by ID that could adopt the replacement incarnation.
                return Ok(Applied::new(&account_id, name));
            }
        }
        self.check_account(&tx, plugin_id, &account_id)?;
        let mut out = Applied::new(&account_id, name);
        let now = now_millis();
        match event {
            Event::Status(e) => self.on_status(&tx, &e, now, &mut out)?,
            Event::ConversationsUpsert(e) => {
                for c in &e.conversations {
                    self.upsert_conversation(&tx, &account_id, c, now, &mut out)?;
                }
            }
            Event::ConversationsSynced(e) => {
                let (gone, back) =
                    ch_db::mark_conversations_synced(&tx, &account_id, &e.chat_ids, now)?;
                if !gone.is_empty() || !back.is_empty() {
                    out.push(Change::ConversationsSynced { gone, back });
                }
            }
            Event::MessagesUpsert(e) => {
                self.upsert_messages(&tx, plugin_id, &account_id, &e.messages, now, &mut out)?;
            }
            Event::MessagesDeleted(e) => {
                self.delete_messages(
                    &tx,
                    &account_id,
                    e.chat_id.as_deref(),
                    &e.message_ids,
                    "platform",
                    self.opts.keep_deleted,
                    now,
                    &mut out,
                )?;
            }
            Event::MessageEdited(e) => {
                self.upsert_messages(
                    &tx,
                    plugin_id,
                    &account_id,
                    std::slice::from_ref(&e.message),
                    now,
                    &mut out,
                )?;
            }
            Event::ReactionChanged(e) => {
                self.on_reaction_changed(&tx, &account_id, &e, now, &mut out)?;
            }
            Event::ContactsUpsert(e) => {
                self.upsert_contacts(&tx, &account_id, &e.contacts, now, &mut out)?;
            }
            Event::FoldersSynced(e) => {
                self.on_folders_synced(&tx, &account_id, &e, now, &mut out)?
            }
            Event::TopicsUpsert(e) => {
                self.upsert_topics(
                    &tx,
                    &account_id,
                    &e.chat_id,
                    &e.topics,
                    e.complete,
                    now,
                    &mut out,
                )?;
            }
            Event::ReadOutbox(e) => {
                self.on_read_outbox(
                    &tx,
                    &account_id,
                    &e.chat_id,
                    e.topic_id.as_deref(),
                    &e.max_message_id,
                    now,
                    &mut out,
                )?;
            }
            Event::ReadInbox(e) => {
                self.on_read_inbox(
                    &tx,
                    &account_id,
                    &e.chat_id,
                    e.topic_id.as_deref(),
                    &e.max_message_id,
                    e.unread_count,
                    now,
                    &mut out,
                )?;
            }
            Event::Presence(e) => self.on_presence(&tx, &account_id, &e, now, &mut out)?,
            Event::DraftChanged(e) => {
                self.set_draft(
                    &tx,
                    &account_id,
                    &e.chat_id,
                    e.topic_id.as_deref(),
                    &e.draft,
                    None,
                    &mut out,
                )?;
            }
            Event::AvatarReady(e) => self.on_avatar_ready(&tx, &account_id, &e, now, &mut out)?,
            Event::MediaReady(e) => self.on_media_ready(&tx, &account_id, &e, now, &mut out)?,
            Event::TransferProgress(e) => out.push(Change::TransferProgress(e)),
            Event::AuthStep(e) => out.push(Change::AuthStep(e.step)),
        }
        if let Some((owners, origin)) = owner {
            if !owners.validate(&tx, origin)? {
                tx.rollback()?;
                return Ok(Applied::new(&account_id, name));
            }
        }
        tx.commit()?;
        Ok(out)
    }

    // ---- helpers ----------------------------------------------------------------------------

    fn check_account(
        &self,
        conn: &Connection,
        plugin_id: &str,
        account_id: &str,
    ) -> Result<AccountRow> {
        let acc =
            ch_db::get_account(conn, account_id)?.ok_or_else(|| IngestError::UnknownAccount {
                account_id: account_id.to_owned(),
            })?;
        if acc.plugin_id != plugin_id {
            return Err(IngestError::PluginMismatch {
                account_id: account_id.to_owned(),
                expected: acc.plugin_id,
                got: plugin_id.to_owned(),
            });
        }
        Ok(acc)
    }

    /// Apply the path filter to one optional path, dropping it when rejected.
    fn filter_path(&self, account_id: &str, path: &mut Option<String>, out: &mut Applied) {
        if let (Some(f), Some(p)) = (&self.opts.path_filter, path.as_deref())
            && !f(account_id, p)
        {
            tracing::warn!(
                account_id,
                path = p,
                "dropping path outside the account's directories"
            );
            *path = None;
            out.dropped_paths += 1;
        }
    }

    fn filter_media(&self, account_id: &str, media: &mut Option<Media>, out: &mut Applied) {
        if let Some(m) = media {
            self.filter_path(account_id, &mut m.path, out);
            self.filter_path(account_id, &mut m.thumb_path, out);
        }
    }

    fn filter_message_paths(&self, account_id: &str, m: &mut Message, out: &mut Applied) {
        self.filter_media(account_id, &mut m.media, out);
        if let Some(lp) = &mut m.link_preview {
            self.filter_path(account_id, &mut lp.thumb_path, out);
            self.filter_media(account_id, &mut lp.media, out);
        }
    }

    /// The conversation rowid, creating a stub (kind `unknown`) when the chat is not known yet.
    fn conversation_or_stub(
        &self,
        conn: &Connection,
        account_id: &str,
        chat_id: &str,
        now: Millis,
    ) -> Result<(i64, bool)> {
        if let Some(id) = ch_db::conversation_rowid(conn, account_id, chat_id)? {
            return Ok((id, false));
        }
        let row = merge::new_conversation(account_id, chat_id, "unknown", now);
        Ok((ch_db::insert_conversation(conn, &row)?, true))
    }

    /// The handle rowid, creating a bare row when the user is not known yet.
    fn handle_or_stub(
        &self,
        conn: &Connection,
        account_id: &str,
        user_id: &str,
        now: Millis,
    ) -> Result<HandleRow> {
        if let Some(h) = ch_db::get_handle(conn, account_id, user_id)? {
            return Ok(h);
        }
        let mut h = merge::new_handle(account_id, user_id, now);
        h.id = ch_db::insert_handle(conn, &h)?;
        Ok(h)
    }

    fn record_media_files(
        &self,
        conn: &Connection,
        account_id: &str,
        row: &MessageRow,
        now: Millis,
    ) -> Result<()> {
        if let Some(m) = &row.media {
            for (kind, path) in [("full", &m.path), ("thumb", &m.thumb_path)] {
                if let Some(p) = path {
                    ch_db::upsert_media_file(
                        conn,
                        &MediaFileRow {
                            path: p.clone(),
                            account_id: account_id.to_owned(),
                            kind: kind.to_owned(),
                            message_id: Some(row.id),
                            media_kind: Some(m.kind.as_str().to_owned()),
                            bytes: if kind == "full" {
                                m.bytes.map(|b| b as i64)
                            } else {
                                None
                            },
                            created_at: now,
                            last_access_at: now,
                        },
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Refresh the chat-list preview when `row` is the newest live message.
    fn bump_preview(&self, conn: &Connection, conv_id: i64, row: &MessageRow) -> Result<()> {
        if row.is_scheduled || row.deleted_at.is_some() {
            return Ok(());
        }
        // Order-independent: the preview follows the newest live message in the database, and
        // last_message_at never moves backwards (the DTO may know of newer messages).
        let Some(latest) = ch_db::latest_message(conn, conv_id)? else {
            return Ok(());
        };
        if latest.id != row.id {
            return Ok(());
        }
        let Some(conv) = ch_db::get_conversation_by_id(conn, conv_id)? else {
            return Ok(());
        };
        let (kind, preview) = merge::preview_of(row);
        let at = conv
            .last_message_at
            .map_or(row.sent_at, |a| a.max(row.sent_at));
        if conv.last_message_at != Some(at)
            || conv.last_message_kind != kind
            || conv.last_message_preview != preview
        {
            ch_db::set_conversation_last_message(
                conn,
                conv_id,
                Some(at),
                kind.as_deref(),
                preview.as_deref(),
            )?;
        }
        Ok(())
    }

    // ---- status -----------------------------------------------------------------------------

    fn on_status(
        &self,
        conn: &Connection,
        e: &StatusEvent,
        now: Millis,
        out: &mut Applied,
    ) -> Result<()> {
        ch_db::set_account_state(
            conn,
            &e.account_id,
            e.state.as_str(),
            e.detail.as_deref(),
            e.reconnecting,
            now,
        )?;
        let mut me = None;
        if let Some(c) = &e.me {
            self.upsert_contacts(conn, &e.account_id, std::slice::from_ref(c), now, out)?;
            ch_db::set_account_me(conn, &e.account_id, &c.id)?;
            me = Some(c.id.clone());
        }
        out.push(Change::AccountStatus {
            state: e.state.as_str().to_owned(),
            detail: e.detail.clone(),
            reconnecting: e.reconnecting,
            me,
        });
        Ok(())
    }

    // ---- conversations ----------------------------------------------------------------------

    pub(crate) fn upsert_conversation(
        &self,
        conn: &Connection,
        account_id: &str,
        c: &Conversation,
        now: Millis,
        out: &mut Applied,
    ) -> Result<i64> {
        let mut c = c.clone();
        self.filter_path(account_id, &mut c.avatar_path, out);
        let existing = ch_db::get_conversation(conn, account_id, &c.id)?;
        let (id, created, changed) = match existing {
            Some(row) => {
                let mut merged = row.clone();
                merge::merge_conversation(&mut merged, &c);
                let changed = merged != row;
                if changed {
                    merged.updated_at = now;
                    ch_db::update_conversation(conn, &merged)?;
                }
                (row.id, false, changed)
            }
            None => {
                let mut row = merge::new_conversation(account_id, &c.id, c.kind.as_str(), now);
                merge::merge_conversation(&mut row, &c);
                (ch_db::insert_conversation(conn, &row)?, true, true)
            }
        };
        let mut changed = changed;
        if let Some(fids) = &c.folder_ids {
            let mut want: Vec<String> = fids.clone();
            want.sort();
            want.dedup();
            let mut have: Vec<String> =
                ch_db::collect_chat_folder_memberships(conn, account_id, &c.id)?;
            have.sort();
            if have != want {
                ch_db::set_chat_platform_folders(conn, account_id, &c.id, &want)?;
                changed = true;
            }
        }
        if let Some(d) = &c.draft {
            self.set_draft(conn, account_id, &c.id, None, d, Some(id), out)?;
        }
        if changed {
            out.push(Change::ConversationUpserted {
                chat_id: c.id.clone(),
                created,
            });
        }
        Ok(id)
    }

    // ---- messages ---------------------------------------------------------------------------

    /// Upsert a batch (one `messages.upsert`, a history page, a single edit). Grouped per chat.
    pub(crate) fn upsert_messages(
        &self,
        conn: &Connection,
        plugin_id: &str,
        account_id: &str,
        messages: &[Message],
        now: Millis,
        out: &mut Applied,
    ) -> Result<()> {
        let mut per_chat: Vec<(Id, Vec<Id>, Vec<Id>)> = Vec::new();
        for m in messages {
            let (created, updated, edited) =
                self.upsert_message(conn, plugin_id, account_id, m, now, out)?;
            if let Some((_, c, u)) = per_chat.iter_mut().find(|(cid, _, _)| *cid == m.chat_id) {
                if created {
                    c.push(m.id.clone());
                } else if updated {
                    u.push(m.id.clone());
                }
            } else if created || updated {
                let (mut c, mut u) = (Vec::new(), Vec::new());
                if created {
                    c.push(m.id.clone());
                } else {
                    u.push(m.id.clone());
                }
                per_chat.push((m.chat_id.clone(), c, u));
            }
            if let Some(snapshot) = edited {
                out.push(Change::MessageEdited {
                    chat_id: m.chat_id.clone(),
                    message_id: m.id.clone(),
                    snapshot,
                });
            }
        }
        for (chat_id, created, updated) in per_chat {
            out.push(Change::MessagesUpserted {
                chat_id,
                created,
                updated,
            });
        }
        Ok(())
    }

    /// Returns `(created, updated, edited: Option<snapshot_written>)`.
    fn upsert_message(
        &self,
        conn: &Connection,
        plugin_id: &str,
        account_id: &str,
        m: &Message,
        now: Millis,
        out: &mut Applied,
    ) -> Result<(bool, bool, Option<bool>)> {
        let mut m = m.clone();
        self.filter_message_paths(account_id, &mut m, out);
        let (conv_id, _) = self.conversation_or_stub(conn, account_id, &m.chat_id, now)?;
        let mut existing = ch_db::get_message(conn, conv_id, &m.id)?;
        let mut confirmed = None;
        if m.direction == Direction::Out
            && let Some(cid) = client_id_of(&m, plugin_id)
            && m.id != format!("local:{cid}")
        {
            match ch_db::get_outbox_by_client_id(conn, &cid)? {
                Some(item)
                    if item.account_id == account_id
                        && item.method == "messages.send"
                        && item.params["client_id"] == cid
                        && matches!(item.state.as_str(), "queued" | "sending" | "failed")
                        && !matches!(m.delivery, Some(Delivery::Sending | Delivery::Failed))
                        && send_destination_matches(conn, &item, &m)? =>
                {
                    if let Some(echo_id) = item.echo_message_id
                        && let Some(echo) = ch_db::get_message_by_id(conn, echo_id)?
                        && echo.message_id == format!("local:{cid}")
                    {
                        if existing.is_none() && echo.conversation_id == conv_id {
                            existing = Some(echo);
                        } else {
                            self.delete_messages(
                                conn,
                                account_id,
                                Some(&item.chat_id),
                                &[echo.message_id],
                                "me",
                                false,
                                now,
                                out,
                            )?;
                        }
                    }
                    if m.delivery.is_none()
                        && !matches!(
                            existing.as_ref().and_then(|r| r.delivery.as_deref()),
                            Some("read" | "delivered")
                        )
                    {
                        m.delivery = Some(Delivery::Sent);
                    }
                    confirmed = Some(item.id);
                }
                // Preserve adoption of standalone local echoes created by apply_local_echo.
                None if existing.is_none() => {
                    existing = ch_db::get_message(conn, conv_id, &format!("local:{cid}"))?;
                }
                _ => {}
            }
        }
        let result = match existing {
            Some(row) => {
                let content_changed =
                    merge::content_differs(&row, &m) && !merge::is_stale_edit(&row, &m);
                let mut merged = row.clone();
                merge::merge_message(&mut merged, &m);
                let mut snapshot = false;
                if content_changed
                    && self.opts.keep_edit_history
                    && row.delivery.as_deref() != Some("sending")
                {
                    ch_db::insert_message_version(conn, &row, now)?;
                    snapshot = true;
                }
                let mut changed = merged != row;
                if changed {
                    merged.updated_at = now;
                    ch_db::update_message(conn, &merged)?;
                    self.record_media_files(conn, account_id, &merged, now)?;
                }
                if let Some(r) = &m.reactions
                    && ch_db::set_reactions(conn, row.id, r)?
                {
                    changed = true;
                }
                if changed {
                    self.bump_preview(conn, conv_id, &merged)?;
                }
                (false, changed, content_changed.then_some(snapshot))
            }
            None => {
                let mut row = merge::new_message(conv_id, &m, now);
                merge::merge_message(&mut row, &m);
                row.id = ch_db::insert_message(conn, &row)?;
                if let Some(r) = &m.reactions {
                    ch_db::set_reactions(conn, row.id, r)?;
                }
                self.record_media_files(conn, account_id, &row, now)?;
                self.bump_preview(conn, conv_id, &row)?;
                (true, false, None)
            }
        };
        if let Some(id) = confirmed {
            let canonical = ch_db::get_message(conn, conv_id, &m.id)?
                .ok_or(rusqlite::Error::QueryReturnedNoRows)?;
            ch_db::set_outbox_echo(conn, id, Some(canonical.id))?;
            ch_db::set_outbox_sent(conn, id, std::slice::from_ref(&m.id))?;
            return Ok((result.0, !result.0, result.2));
        }
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn delete_messages(
        &self,
        conn: &Connection,
        account_id: &str,
        chat_id: Option<&str>,
        ids: &[Id],
        by: &str,
        keep: bool,
        now: Millis,
        out: &mut Applied,
    ) -> Result<()> {
        let mut per_chat: Vec<(Id, Vec<Id>)> = Vec::new();
        let mut note = |chat: &str, id: &str| {
            if let Some((_, v)) = per_chat.iter_mut().find(|(c, _)| c == chat) {
                v.push(id.to_owned());
            } else {
                per_chat.push((chat.to_owned(), vec![id.to_owned()]));
            }
        };
        match chat_id {
            Some(chat) => {
                let Some(conv_id) = ch_db::conversation_rowid(conn, account_id, chat)? else {
                    out.ignore(format!("messages.deleted for unknown chat {chat}"));
                    return Ok(());
                };
                for id in ids {
                    if let Some(row) = ch_db::get_message(conn, conv_id, id)?
                        && delete_row(conn, &row, by, keep, now)?
                    {
                        note(chat, id);
                    }
                }
            }
            None => {
                for id in ids {
                    for row in ch_db::find_messages_in_dms(conn, account_id, id)? {
                        if delete_row(conn, &row, by, keep, now)? {
                            let conv = ch_db::get_conversation_by_id(conn, row.conversation_id)?;
                            note(&conv.map(|c| c.chat_id).unwrap_or_default(), id);
                        }
                    }
                }
            }
        }
        for (chat_id, message_ids) in per_chat {
            out.push(Change::MessagesDeleted {
                chat_id,
                message_ids,
                tombstoned: keep,
            });
        }
        Ok(())
    }

    fn on_reaction_changed(
        &self,
        conn: &Connection,
        account_id: &str,
        e: &ReactionChangedEvent,
        now: Millis,
        out: &mut Applied,
    ) -> Result<()> {
        let Some(conv_id) = ch_db::conversation_rowid(conn, account_id, &e.chat_id)? else {
            out.ignore(format!("reaction.changed for unknown chat {}", e.chat_id));
            return Ok(());
        };
        let Some(row) = ch_db::get_message(conn, conv_id, &e.message_id)? else {
            out.ignore(format!(
                "reaction.changed for unknown message {}/{}",
                e.chat_id, e.message_id
            ));
            return Ok(());
        };
        let mut changed = ch_db::set_reactions(conn, row.id, &e.reactions)?;
        if let Some(rs) = &e.reactors
            && row.reactors.as_ref() != Some(rs)
        {
            ch_db::set_reactors(conn, row.id, rs, now)?;
            changed = true;
        }
        if changed {
            if self.opts.log_presence {
                for r in e.reactors.iter().flatten() {
                    ch_db::insert_presence_log(
                        conn,
                        &PresenceLogRow {
                            account_id: account_id.to_owned(),
                            user_id: r.user_id.clone(),
                            chat_id: Some(e.chat_id.clone()),
                            at: r.at.map(ts_to_millis).unwrap_or(now),
                            kind: "reacted".into(),
                            detail: Some(
                                serde_json::json!({"message_id": e.message_id, "key": r.key}),
                            ),
                            ..Default::default()
                        },
                    )?;
                }
            }
            out.push(Change::ReactionsChanged {
                chat_id: e.chat_id.clone(),
                message_id: e.message_id.clone(),
            });
        }
        Ok(())
    }

    // ---- contacts / identity ----------------------------------------------------------------

    pub(crate) fn upsert_contacts(
        &self,
        conn: &Connection,
        account_id: &str,
        contacts: &[Contact],
        now: Millis,
        out: &mut Applied,
    ) -> Result<()> {
        let mut user_ids = Vec::new();
        let (mut persons_created, mut auto_linked) = (0, 0);
        for c in contacts {
            let mut c = c.clone();
            self.filter_path(account_id, &mut c.avatar_path, out);
            let existing = ch_db::get_handle(conn, account_id, &c.id)?;
            let mut row = existing
                .clone()
                .unwrap_or_else(|| merge::new_handle(account_id, &c.id, now));
            merge::merge_contact(&mut row, &c, now);
            if row.person_id.is_none() {
                let (pid, source, created) = self.resolve_person(conn, &row)?;
                row.person_id = Some(pid);
                row.link_source = Some(source.to_owned());
                if created {
                    persons_created += 1;
                } else {
                    auto_linked += 1;
                }
            }
            match existing {
                Some(old) => {
                    if row != old {
                        row.last_seen_at = now;
                        ch_db::update_handle(conn, &row)?;
                        user_ids.push(c.id.clone());
                    }
                }
                None => {
                    ch_db::insert_handle(conn, &row)?;
                    user_ids.push(c.id.clone());
                }
            }
        }
        if !user_ids.is_empty() {
            out.push(Change::ContactsUpserted {
                user_ids,
                persons_created,
                auto_linked,
            });
        }
        Ok(())
    }

    /// Find or create the person for an unlinked handle: a unique phone match on another account,
    /// else a unique username match, else a new person. Returns `(person_id, link_source, created)`.
    fn resolve_person(
        &self,
        conn: &Connection,
        h: &HandleRow,
    ) -> Result<(i64, &'static str, bool)> {
        if let Some(phone) = h.phone.as_deref().filter(|p| p.len() >= 5) {
            let mut persons: Vec<i64> = ch_db::handles_by_phone(conn, phone, &h.account_id)?
                .into_iter()
                .filter_map(|o| o.person_id)
                .collect();
            persons.dedup();
            if persons.len() == 1 {
                return Ok((persons[0], "auto_phone", false));
            }
        }
        if let Some(username) = h.username.as_deref().filter(|u| !u.is_empty()) {
            let mut persons: Vec<i64> = ch_db::handles_by_username(conn, username, &h.account_id)?
                .into_iter()
                .filter_map(|o| o.person_id)
                .collect();
            persons.dedup();
            if persons.len() == 1 {
                return Ok((persons[0], "auto_username", false));
            }
        }
        let name = h
            .display_name
            .clone()
            .or_else(|| match (&h.first_name, &h.last_name) {
                (Some(f), Some(l)) => Some(format!("{f} {l}")),
                (Some(f), None) => Some(f.clone()),
                (None, Some(l)) => Some(l.clone()),
                _ => None,
            })
            .or_else(|| h.username.clone());
        Ok((
            ch_db::create_person(conn, name.as_deref())?,
            "created",
            true,
        ))
    }

    // ---- folders ----------------------------------------------------------------------------

    fn on_folders_synced(
        &self,
        conn: &Connection,
        account_id: &str,
        e: &FoldersSyncedEvent,
        now: Millis,
        out: &mut Applied,
    ) -> Result<()> {
        let mut ids = Vec::new();
        let mut changed = false;
        for f in &e.folders {
            let row = FolderRow {
                account_id: account_id.to_owned(),
                folder_id: f.id.clone(),
                title: f.title.clone(),
                emoticon: f.emoticon.clone(),
                position: f.position.map(|p| p as i64),
                rules: f.rules.clone(),
                is_shared: f.is_shared,
                ext: f.ext.clone(),
                updated_at: now,
                ..Default::default()
            };
            let before = ch_db::get_folder(conn, account_id, &f.id)?;
            match before {
                Some(b)
                    if FolderRow {
                        id: b.id,
                        updated_at: b.updated_at,
                        ..row.clone()
                    } == b => {}
                _ => {
                    ch_db::upsert_folder(conn, &row)?;
                    changed = true;
                }
            }
            if let Some(chats) = &f.chat_ids {
                let mut want: Vec<(String, String)> = chats
                    .iter()
                    .map(|c| (account_id.to_owned(), c.clone()))
                    .collect();
                want.sort();
                want.dedup();
                if ch_db::folder_chats(conn, account_id, &f.id)? != want {
                    ch_db::set_folder_chats(conn, account_id, &f.id, &want)?;
                    changed = true;
                }
            }
            ids.push(f.id.clone());
        }
        let removed = ch_db::delete_folders_not_in(conn, account_id, &ids)?;
        if changed || removed > 0 {
            out.push(Change::FoldersSynced {
                folder_ids: ids,
                removed,
            });
        }
        Ok(())
    }

    // ---- topics -----------------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn upsert_topics(
        &self,
        conn: &Connection,
        account_id: &str,
        chat_id: &str,
        topics: &[Topic],
        complete: bool,
        now: Millis,
        out: &mut Applied,
    ) -> Result<()> {
        let (conv_id, _) = self.conversation_or_stub(conn, account_id, chat_id, now)?;
        let (mut created, mut updated) = (Vec::new(), Vec::new());
        for t in topics {
            let existing = ch_db::get_topic(conn, conv_id, &t.id)?;
            let topic_rowid = match existing {
                Some(row) => {
                    let mut merged = row.clone();
                    merge::merge_topic(&mut merged, t);
                    if merged != row {
                        merged.updated_at = now;
                        ch_db::update_topic(conn, &merged)?;
                        updated.push(t.id.clone());
                    }
                    row.id
                }
                None => {
                    let mut row = merge::new_topic(conv_id, &t.id, &t.title, now);
                    merge::merge_topic(&mut row, t);
                    created.push(t.id.clone());
                    ch_db::insert_topic(conn, &row)?
                }
            };
            let _ = topic_rowid;
            if let Some(d) = &t.draft {
                self.set_draft(
                    conn,
                    account_id,
                    chat_id,
                    Some(&t.id),
                    d,
                    Some(conv_id),
                    out,
                )?;
            }
        }
        let gone = if complete {
            let keep: Vec<String> = topics.iter().map(|t| t.id.clone()).collect();
            ch_db::mark_topics_synced(conn, conv_id, &keep, now)?
        } else {
            Vec::new()
        };
        if !created.is_empty() || !updated.is_empty() || !gone.is_empty() {
            out.push(Change::TopicsUpserted {
                chat_id: chat_id.to_owned(),
                created,
                updated,
                gone,
            });
        }
        Ok(())
    }

    // ---- read state -------------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_read_outbox(
        &self,
        conn: &Connection,
        account_id: &str,
        chat_id: &str,
        topic_id: Option<&str>,
        max_id: &str,
        now: Millis,
        out: &mut Applied,
    ) -> Result<()> {
        let Some(conv_id) = ch_db::conversation_rowid(conn, account_id, chat_id)? else {
            out.ignore(format!("read.outbox for unknown chat {chat_id}"));
            return Ok(());
        };
        let mut changed = false;
        match topic_id {
            Some(tid) => {
                if let Some(mut t) = ch_db::get_topic(conn, conv_id, tid)?
                    && t.read_outbox_max_id.as_deref() != Some(max_id)
                {
                    t.read_outbox_max_id = Some(max_id.to_owned());
                    t.updated_at = now;
                    ch_db::update_topic(conn, &t)?;
                    changed = true;
                }
            }
            None => {
                if let Some(mut c) = ch_db::get_conversation_by_id(conn, conv_id)?
                    && c.read_outbox_max_id.as_deref() != Some(max_id)
                {
                    c.read_outbox_max_id = Some(max_id.to_owned());
                    c.updated_at = now;
                    ch_db::update_conversation(conn, &c)?;
                    changed = true;
                }
            }
        }
        // Ids are opaque, so "up to" is derived from the referenced message's sent_at.
        let mut marked = 0;
        if let Some(anchor) = ch_db::get_message(conn, conv_id, max_id)? {
            marked = ch_db::mark_outgoing_read_until(conn, conv_id, topic_id, anchor.sent_at, now)?;
            if marked > 0
                && self.opts.log_presence
                && let Some(c) = ch_db::get_conversation_by_id(conn, conv_id)?
                && let Some(peer) = c.peer_user_id
            {
                ch_db::insert_presence_log(
                    conn,
                    &PresenceLogRow {
                        account_id: account_id.to_owned(),
                        user_id: peer,
                        chat_id: Some(chat_id.to_owned()),
                        at: now,
                        kind: "read".into(),
                        detail: Some(serde_json::json!({"max_message_id": max_id})),
                        ..Default::default()
                    },
                )?;
            }
        }
        if changed || marked > 0 {
            out.push(Change::ReadOutbox {
                chat_id: chat_id.to_owned(),
                topic_id: topic_id.map(str::to_owned),
                max_message_id: max_id.to_owned(),
                marked_read: marked,
            });
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_read_inbox(
        &self,
        conn: &Connection,
        account_id: &str,
        chat_id: &str,
        topic_id: Option<&str>,
        max_id: &str,
        unread_count: Option<u64>,
        now: Millis,
        out: &mut Applied,
    ) -> Result<()> {
        let Some(conv_id) = ch_db::conversation_rowid(conn, account_id, chat_id)? else {
            out.ignore(format!("read.inbox for unknown chat {chat_id}"));
            return Ok(());
        };
        let mut changed = false;
        match topic_id {
            Some(tid) => {
                if let Some(mut t) = ch_db::get_topic(conn, conv_id, tid)? {
                    let mut n = t.clone();
                    n.read_inbox_max_id = Some(max_id.to_owned());
                    if let Some(u) = unread_count {
                        n.unread_count = u as i64;
                    }
                    if n != t {
                        n.updated_at = now;
                        ch_db::update_topic(conn, &n)?;
                        t = n;
                        changed = true;
                    }
                    let _ = t;
                }
            }
            None => {
                if let Some(c) = ch_db::get_conversation_by_id(conn, conv_id)? {
                    let mut n = c.clone();
                    n.read_inbox_max_id = Some(max_id.to_owned());
                    if let Some(u) = unread_count {
                        n.unread_count = u as i64;
                        if u == 0 {
                            n.marked_unread = Some(false);
                        }
                    }
                    if n != c {
                        n.updated_at = now;
                        ch_db::update_conversation(conn, &n)?;
                        changed = true;
                    }
                }
            }
        }
        if changed {
            out.push(Change::ReadInbox {
                chat_id: chat_id.to_owned(),
                topic_id: topic_id.map(str::to_owned),
                max_message_id: max_id.to_owned(),
                unread_count,
            });
        }
        Ok(())
    }

    // ---- presence ---------------------------------------------------------------------------

    fn on_presence(
        &self,
        conn: &Connection,
        account_id: &str,
        e: &PresenceEvent,
        now: Millis,
        out: &mut Applied,
    ) -> Result<()> {
        if e.online.is_none() && e.action.is_none() {
            out.ignore("presence without online or action");
            return Ok(());
        }
        let h = self.handle_or_stub(conn, account_id, &e.user_id, now)?;
        if let Some(st) = &e.online
            && h.status.as_ref() != Some(st)
        {
            ch_db::set_handle_status(conn, h.id, st, now)?;
            if self.opts.log_presence {
                ch_db::insert_presence_log(
                    conn,
                    &PresenceLogRow {
                        account_id: account_id.to_owned(),
                        user_id: e.user_id.clone(),
                        chat_id: None,
                        at: now,
                        kind: st.state.as_str().to_owned(),
                        detail: Some(serde_json::to_value(st).unwrap_or_default()),
                        ..Default::default()
                    },
                )?;
            }
        }
        if let Some(a) = &e.action
            && self.opts.log_presence
            && *a != PresenceAction::Cancel
        {
            ch_db::insert_presence_log(
                conn,
                &PresenceLogRow {
                    account_id: account_id.to_owned(),
                    user_id: e.user_id.clone(),
                    chat_id: e.chat_id.clone(),
                    at: now,
                    kind: a.as_str().to_owned(),
                    detail: None,
                    ..Default::default()
                },
            )?;
        }
        out.push(Change::Presence {
            user_id: e.user_id.clone(),
            chat_id: e.chat_id.clone(),
            topic_id: e.topic_id.clone(),
            online: e.online.clone(),
            action: e.action.clone(),
            expires_at: e.expires_at,
        });
        Ok(())
    }

    // ---- drafts -----------------------------------------------------------------------------

    /// Replace a whole draft snapshot (spec §8.4, §10.1): empty text clears and older snapshots
    /// are ignored. Equal content emits no change; a newer explicit timestamp still advances
    /// the freshness watermark. Undated duplicates do not advance that watermark.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn set_draft(
        &self,
        conn: &Connection,
        account_id: &str,
        chat_id: &str,
        topic_id: Option<&str>,
        d: &Draft,
        conv_id: Option<i64>,
        out: &mut Applied,
    ) -> Result<()> {
        let conv_id = match conv_id {
            Some(id) => id,
            None => match ch_db::conversation_rowid(conn, account_id, chat_id)? {
                Some(id) => id,
                None => {
                    out.ignore(format!("draft for unknown chat {chat_id}"));
                    return Ok(());
                }
            },
        };
        let key = topic_id.unwrap_or("");
        let incoming_at = d.updated_at.map(ts_to_millis).unwrap_or_else(now_millis);
        let existing = ch_db::get_draft(conn, conv_id, key)?;
        let row = DraftRow {
            conversation_id: conv_id,
            topic_id: key.to_owned(),
            text: d.text.clone(),
            entities: d.entities.clone(),
            reply_to: d.reply_to.clone(),
            updated_at: incoming_at,
            ext: d.ext.clone(),
        };
        if let Some(cur) = &existing {
            if incoming_at < cur.updated_at {
                out.ignore("draft older than the stored one");
                return Ok(());
            }
            if cur.text == row.text
                && cur.entities == row.entities
                && cur.reply_to == row.reply_to
                && cur.ext == row.ext
            {
                // Keep freshness even when nothing visible changed, so an older conflicting
                // snapshot cannot overwrite a newer confirmation of this content.
                if d.updated_at.is_some() && incoming_at > cur.updated_at {
                    ch_db::set_draft(conn, &row)?;
                }
                return Ok(());
            }
        } else if d.text.is_empty() {
            return Ok(());
        }
        ch_db::set_draft(conn, &row)?;
        out.push(Change::DraftChanged {
            chat_id: chat_id.to_owned(),
            topic_id: topic_id.map(str::to_owned),
            cleared: d.text.is_empty(),
        });
        Ok(())
    }

    // ---- files ------------------------------------------------------------------------------

    fn on_avatar_ready(
        &self,
        conn: &Connection,
        account_id: &str,
        e: &AvatarReadyEvent,
        now: Millis,
        out: &mut Applied,
    ) -> Result<()> {
        let mut path = Some(e.path.clone());
        self.filter_path(account_id, &mut path, out);
        let Some(path) = path else {
            return Ok(());
        };
        let changed = match e.kind {
            AvatarKind::User => {
                let h = self.handle_or_stub(conn, account_id, &e.id, now)?;
                if h.avatar_path.as_deref() != Some(path.as_str()) {
                    ch_db::set_handle_avatar(conn, h.id, Some(&path))?;
                    true
                } else {
                    false
                }
            }
            AvatarKind::Chat => match ch_db::get_conversation(conn, account_id, &e.id)? {
                Some(mut c) if c.avatar_path.as_deref() != Some(path.as_str()) => {
                    c.avatar_path = Some(path.clone());
                    c.updated_at = now;
                    ch_db::update_conversation(conn, &c)?;
                    true
                }
                Some(_) => false,
                None => {
                    out.ignore(format!("avatar.ready for unknown chat {}", e.id));
                    return Ok(());
                }
            },
            AvatarKind::Unknown(_) => {
                out.ignore("avatar.ready with unknown kind");
                return Ok(());
            }
        };
        if changed {
            ch_db::upsert_media_file(
                conn,
                &MediaFileRow {
                    path: path.clone(),
                    account_id: account_id.to_owned(),
                    kind: "avatar".into(),
                    created_at: now,
                    last_access_at: now,
                    ..Default::default()
                },
            )?;
            out.push(Change::AvatarReady {
                kind: e.kind.clone(),
                id: e.id.clone(),
                path,
            });
        }
        Ok(())
    }

    fn on_media_ready(
        &self,
        conn: &Connection,
        account_id: &str,
        e: &MediaReadyEvent,
        now: Millis,
        out: &mut Applied,
    ) -> Result<()> {
        let Some(conv_id) = ch_db::conversation_rowid(conn, account_id, &e.chat_id)? else {
            out.ignore(format!("media.ready for unknown chat {}", e.chat_id));
            return Ok(());
        };
        let Some(row) = ch_db::get_message(conn, conv_id, &e.message_id)? else {
            out.ignore(format!(
                "media.ready for unknown message {}/{}",
                e.chat_id, e.message_id
            ));
            return Ok(());
        };
        let Some(mut media) = row.media.clone() else {
            out.ignore(format!(
                "media.ready for a message without media {}/{}",
                e.chat_id, e.message_id
            ));
            return Ok(());
        };
        let (mut thumb, mut full) = (e.thumb_path.clone(), e.path.clone());
        self.filter_path(account_id, &mut thumb, out);
        self.filter_path(account_id, &mut full, out);
        if thumb.is_some() {
            media.thumb_path.clone_from(&thumb);
        }
        if full.is_some() {
            media.path.clone_from(&full);
        }
        if row.media.as_ref() == Some(&media) {
            return Ok(());
        }
        let mut updated = row.clone();
        updated.media = Some(media);
        updated.updated_at = now;
        ch_db::update_message(conn, &updated)?;
        self.record_media_files(conn, account_id, &updated, now)?;
        out.push(Change::MediaReady {
            chat_id: e.chat_id.clone(),
            message_id: e.message_id.clone(),
            thumb_path: thumb,
            path: full,
        });
        Ok(())
    }
}

/// The `account_id` every event carries.
pub fn event_account(e: &Event) -> &str {
    match e {
        Event::Status(x) => &x.account_id,
        Event::ConversationsUpsert(x) => &x.account_id,
        Event::ConversationsSynced(x) => &x.account_id,
        Event::MessagesUpsert(x) => &x.account_id,
        Event::MessagesDeleted(x) => &x.account_id,
        Event::MessageEdited(x) => &x.account_id,
        Event::ReactionChanged(x) => &x.account_id,
        Event::ContactsUpsert(x) => &x.account_id,
        Event::FoldersSynced(x) => &x.account_id,
        Event::TopicsUpsert(x) => &x.account_id,
        Event::ReadOutbox(x) => &x.account_id,
        Event::ReadInbox(x) => &x.account_id,
        Event::Presence(x) => &x.account_id,
        Event::DraftChanged(x) => &x.account_id,
        Event::AvatarReady(x) => &x.account_id,
        Event::MediaReady(x) => &x.account_id,
        Event::TransferProgress(x) => &x.account_id,
        Event::AuthStep(x) => &x.account_id,
    }
}

/// Whether this amendment applies; quoted sends retain their existing behavior.
pub(crate) fn basic_text_reply(item: &OutboxRow) -> bool {
    item.method == "messages.send"
        && item.params.get("comment_to").and_then(|v| v.as_str()).is_none()
        && item.params.get("reply_to").is_some_and(|reply| {
            !reply.is_null()
                && !["quote_text", "quote_offset"].iter().any(|key| {
                    reply.get(*key).is_some_and(|value| !value.is_null())
                })
        })
}

/// Match observed relation fields, never the requested local echo's inferred reference.
pub(crate) fn text_reply_matches(
    item: &OutboxRow,
    topic: Option<&str>,
    reply: Option<&ch_protocol::dto::ReplyRef>,
) -> bool {
    let Some(source) = item.params.get("reply_to")
        .and_then(|reply| reply.get("message_id"))
        .and_then(|value| value.as_str()) else {
        return false;
    };
    let expected_topic = match item.params.get("topic_id") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(topic)) => Some(topic.as_str()),
        _ => return false,
    };
    topic == expected_topic && reply.is_some_and(|observed| {
        observed.message_id == source
            && observed.chat_id.as_deref().is_none_or(|chat| chat == item.chat_id)
    })
}

/// Validate a send result or exact client-ID echo against its destination. For a comment
/// sent from a channel, the source post names the discussion chat. An explicit cross-chat
/// reply reference can establish the same link when the source post is not cached.
pub(crate) fn send_destination_matches(
    conn: &Connection,
    item: &OutboxRow,
    m: &Message,
) -> ch_db::Result<bool> {
    let Some(post) = item.params.get("comment_to").and_then(|v| v.as_str()) else {
        if m.chat_id != item.chat_id {
            return Ok(false);
        }
        // Check the incoming observation before merging it with a local echo,
        // whose ReplyRef is only the requested relation. Plain/quoted sends and
        // other methods retain their existing behavior in this narrow amendment.
        if !basic_text_reply(item) {
            return Ok(true);
        }
        if m.id.starts_with("local:")
            || !text_reply_matches(item, m.topic_id.as_deref(), m.reply_to.as_ref())
        {
            return Ok(false);
        }
        // None is not a whole-chat topic wildcard. In particular, do not invent
        // platform-specific General None/1 normalization in this shared layer.
        if m.topic_id.is_none()
            && let Some(conv) = ch_db::conversation_rowid(conn, &item.account_id, &m.chat_id)?
            && ch_db::get_message(conn, conv, &m.id)?.is_some_and(|stored| stored.topic_id.is_some())
        {
            // Sparse None cannot clear a contradictory canonical topic during merge.
            return Ok(false);
        }
        return Ok(true);
    };
    if let Some(source) = ch_db::conversation_rowid(conn, &item.account_id, &item.chat_id)?
        && let Some(discussion) =
            ch_db::get_message(conn, source, post)?.and_then(|m| m.replies_chat_id)
    {
        return Ok(m.chat_id == discussion);
    }
    Ok(m.reply_to.as_ref().is_some_and(|r| {
        r.chat_id.as_deref() == Some(item.chat_id.as_str()) && r.message_id == post
    }))
}

/// `ext.<plugin_id>.client_id`, the optional echo of `messages.send.client_id` (spec §11.3).
fn client_id_of(m: &Message, plugin_id: &str) -> Option<String> {
    m.ext
        .as_ref()?
        .get(plugin_id)?
        .get("client_id")?
        .as_str()
        .map(str::to_owned)
}

/// Tombstone or remove one message row. Returns `false` when nothing changed (already gone).
fn delete_row(
    conn: &Connection,
    row: &MessageRow,
    by: &str,
    keep: bool,
    now: Millis,
) -> Result<bool> {
    if keep {
        Ok(ch_db::tombstone_message(conn, row.id, by, now)?)
    } else {
        Ok(ch_db::delete_message(conn, row.id)?)
    }
}
