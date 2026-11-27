//! Outbox: the state machine that sends queued messages through plugins.
//!
//! An [`Outbox`] owns one worker per account. [`Outbox::enqueue`] writes an `outbox` row and
//! (for sends) a local echo message through [`Ingest`], and returns at once; the worker calls
//! the plugin and applies the result through the ingest's core-initiated effects (spec §10.1).
//!
//! States: `queued → sending → sent | failed | cancelled`. `queued` with `next_attempt_at` set
//! is a scheduled retry (the echo keeps showing `sending`); `failed` is always final until the
//! user retries. The full retry policy is in [`policy`].
//!
//! Ordering: items are FIFO per conversation (`chat_id`); independent conversations of one
//! account send in parallel, up to [`OutboxPolicy::max_parallel_per_account`]. A waiting or
//! reconciling head blocks its own lane only; a permanently failed item blocks nothing.
//!
//! Idempotency: every item has a `client_id`, sent as `messages.send.client_id`, used as the
//! local echo's id (`local:<client_id>`) and unique in the table. A row is only ever dispatched
//! from `queued`, and `sending` rows found at start-up (or left by a `TIMEOUT`) are never
//! re-sent: they are *reconciled* against the platform's echo (see [`policy`]).
//!
//! The dedicated OutboxChange stream includes item states. With Outbox::for_core, local
//! ingest effects are also published on the runtime change bus.

mod policy;
mod transport;
mod worker;
mod text_admission;
#[cfg(target_os = "linux")]
mod managed_admission;
#[cfg(target_os="linux")]
mod managed_dispatch;
#[cfg(target_os="linux")]
pub(crate) use managed_dispatch::{ManagedPermit,Completion as ManagedCompletion};
pub use text_admission::{TextAdmissionError, TextAdmissionLifetime, TextAdmissionTarget};
#[cfg(target_os = "linux")]
pub use managed_admission::ManagedAdmissionError;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use ch_db::types::{AccountRow, OutboxRow, outbox_state};
use ch_db::{Millis, now_millis};
use ch_protocol::dto::{Media, MediaKind, ReplyRef};
use ch_protocol::events::{TransferDirection, TransferProgressEvent};
use ch_protocol::methods::{
    FileKind, MessagesDeleteParams, MessagesEditParams, MessagesForwardParams,
    MessagesSendAlbumParams, MessagesSendFileParams, MessagesSendParams,
};
use ch_protocol::{ErrorCode, ErrorObject, Id};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, broadcast};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::ingest::{Applied, Change, Ingest, IngestError, LocalEcho};
use crate::runtime::{ChangeBus, Core};

pub use policy::{OutboxPolicy, Verdict, classify};
pub use transport::{SendTransport, is_online};

/// Why an outbox operation failed.
#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    #[error("outbox has stopped")]
    Stopped,
    /// No such item.
    #[error("outbox item {0} not found")]
    NotFound(i64),
    /// The item is not in a state that allows the operation.
    #[error("outbox item {id} is {state}; cannot {op}")]
    WrongState {
        /// The item.
        id: i64,
        /// Its state.
        state: OutboxState,
        /// What was attempted.
        op: &'static str,
    },
    /// The account does not exist.
    #[error("unknown account {0:?}")]
    UnknownAccount(String),
    /// A request that cannot be built (`send_album` with fewer than two items, …).
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// The expected owner or retained intent differs. No foreign row is exposed.
    #[error("outbox admission conflict")]
    AdmissionConflict,
    /// Managed rows require the owned dispatch/admission path, never generic mutation.
    #[error("managed outbox operation is unavailable")]
    ManagedUnavailable,
    /// Database.
    #[error(transparent)]
    Db(#[from] ch_db::Error),
    /// Ingest.
    #[error(transparent)]
    Ingest(#[from] IngestError),
    /// JSON.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Result alias.
pub type Result<T> = std::result::Result<T, OutboxError>;

/// What kind of request an item is (the `kind` column).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxKind {
    /// `messages.send`.
    SendText,
    /// `messages.send_file`.
    SendFile,
    /// `messages.send_album`.
    SendAlbum,
    /// `messages.forward`.
    Forward,
    /// `messages.edit`.
    Edit,
    /// `messages.delete`.
    Delete,
}

impl OutboxKind {
    /// The `kind` column value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SendText => "send_text",
            Self::SendFile => "send_file",
            Self::SendAlbum => "send_album",
            Self::Forward => "forward",
            Self::Edit => "edit",
            Self::Delete => "delete",
        }
    }

    /// Parse the `kind` column.
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "send_text" => Self::SendText,
            "send_file" => Self::SendFile,
            "send_album" => Self::SendAlbum,
            "forward" => Self::Forward,
            "edit" => Self::Edit,
            "delete" => Self::Delete,
            _ => return None,
        })
    }

    /// The plugin method.
    pub fn method(self) -> &'static str {
        match self {
            Self::SendText => "messages.send",
            Self::SendFile => "messages.send_file",
            Self::SendAlbum => "messages.send_album",
            Self::Forward => "messages.forward",
            Self::Edit => "messages.edit",
            Self::Delete => "messages.delete",
        }
    }

    /// Sending it twice has the same effect as once (so an unknown outcome may be retried).
    pub fn is_idempotent(self) -> bool {
        matches!(self, Self::Edit | Self::Delete)
    }

    /// Creates a local echo message (and so can be reconciled through the platform's echo).
    pub fn has_echo(self) -> bool {
        matches!(self, Self::SendText | Self::SendFile | Self::SendAlbum)
    }

    /// Moves bytes (gets `transfer.progress`).
    pub fn is_upload(self) -> bool {
        matches!(self, Self::SendFile | Self::SendAlbum)
    }
}

/// Item state (the `state` column).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxState {
    /// Waiting for its lane, for the account to come online, or for `next_attempt_at`.
    Queued,
    /// A request is in flight, or its outcome is being reconciled.
    Sending,
    /// Done; `result_message_ids` names what the platform created.
    Sent,
    /// Gave up; `last_error` says why. Final until the user retries.
    Failed,
    /// The user cancelled it before it was sent.
    Cancelled,
}

impl OutboxState {
    /// The column value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => outbox_state::QUEUED,
            Self::Sending => outbox_state::SENDING,
            Self::Sent => outbox_state::SENT,
            Self::Failed => outbox_state::FAILED,
            Self::Cancelled => outbox_state::CANCELLED,
        }
    }

    /// Parse the column.
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            outbox_state::QUEUED => Self::Queued,
            outbox_state::SENDING => Self::Sending,
            outbox_state::SENT => Self::Sent,
            outbox_state::FAILED => Self::Failed,
            outbox_state::CANCELLED => Self::Cancelled,
            _ => return None,
        })
    }

    /// `sent`, `failed` or `cancelled`.
    pub fn is_final(self) -> bool {
        matches!(self, Self::Sent | Self::Failed | Self::Cancelled)
    }
}

impl std::fmt::Display for OutboxState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What to send. `account_id` (and `client_id` on `messages.send`) are filled in by the outbox;
/// whatever the caller puts there is overwritten. Unset optional fields are omitted on the wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "params", rename_all = "snake_case")]
pub enum OutboxRequest {
    /// Text message.
    SendText(MessagesSendParams),
    /// One file (`send.file`).
    SendFile(MessagesSendFileParams),
    /// An album of 2–10 files (`send.album`).
    SendAlbum(MessagesSendAlbumParams),
    /// Forward messages into a chat (`forward`).
    Forward(MessagesForwardParams),
    /// Edit a message (`edit`).
    Edit(MessagesEditParams),
    /// Delete messages (`delete`).
    Delete(MessagesDeleteParams),
}

impl OutboxRequest {
    /// The kind.
    pub fn kind(&self) -> OutboxKind {
        match self {
            Self::SendText(_) => OutboxKind::SendText,
            Self::SendFile(_) => OutboxKind::SendFile,
            Self::SendAlbum(_) => OutboxKind::SendAlbum,
            Self::Forward(_) => OutboxKind::Forward,
            Self::Edit(_) => OutboxKind::Edit,
            Self::Delete(_) => OutboxKind::Delete,
        }
    }

    /// The conversation whose lane the item belongs to (the destination for forwards).
    pub fn chat_id(&self) -> &Id {
        match self {
            Self::SendText(p) => &p.chat_id,
            Self::SendFile(p) => &p.chat_id,
            Self::SendAlbum(p) => &p.chat_id,
            Self::Forward(p) => &p.to_chat_id,
            Self::Edit(p) => &p.chat_id,
            Self::Delete(p) => &p.chat_id,
        }
    }

    /// The topic, when routed into one.
    pub fn topic_id(&self) -> Option<&Id> {
        match self {
            Self::SendText(p) => p.topic_id.as_ref(),
            Self::SendFile(p) => p.topic_id.as_ref(),
            Self::SendAlbum(p) => p.topic_id.as_ref(),
            Self::Forward(p) => p.topic_id.as_ref(),
            Self::Edit(_) | Self::Delete(_) => None,
        }
    }

    fn validate(&self) -> Result<()> {
        match self {
            Self::SendAlbum(p) if p.items.len() < 2 => Err(OutboxError::InvalidRequest(
                "send_album needs at least two items".into(),
            )),
            Self::Delete(p) if p.message_ids.is_empty() => Err(OutboxError::InvalidRequest(
                "delete needs at least one message id".into(),
            )),
            Self::Forward(p) if p.message_ids.is_empty() => Err(OutboxError::InvalidRequest(
                "forward needs at least one message id".into(),
            )),
            Self::Edit(p) if p.text.is_none() && p.path.is_none() => Err(
                OutboxError::InvalidRequest("edit needs text or path".into()),
            ),
            _ => Ok(()),
        }
    }

    /// The wire params with `account_id` and `client_id` set.
    fn params(&self, account_id: &str, client_id: &str) -> serde_json::Value {
        let mut v = match self {
            Self::SendText(p) => serde_json::to_value(MessagesSendParams {
                account_id: account_id.to_owned(),
                client_id: Some(client_id.to_owned()),
                ..p.clone()
            }),
            Self::SendFile(p) => serde_json::to_value(MessagesSendFileParams {
                account_id: account_id.to_owned(),
                ..p.clone()
            }),
            Self::SendAlbum(p) => serde_json::to_value(MessagesSendAlbumParams {
                account_id: account_id.to_owned(),
                ..p.clone()
            }),
            Self::Forward(p) => serde_json::to_value(MessagesForwardParams {
                account_id: account_id.to_owned(),
                ..p.clone()
            }),
            Self::Edit(p) => serde_json::to_value(MessagesEditParams {
                account_id: account_id.to_owned(),
                ..p.clone()
            }),
            Self::Delete(p) => serde_json::to_value(MessagesDeleteParams {
                account_id: account_id.to_owned(),
                ..p.clone()
            }),
        }
        .expect("method params serialise");
        // Belt and braces for §14.2 ("sent as false counts as present"): the structs already
        // skip `None`, but a `Some(false)` the caller did not mean is still their call.
        if let serde_json::Value::Object(m) = &mut v {
            m.retain(|_, v| !v.is_null());
        }
        v
    }

    /// The local echo for sends.
    fn echo(&self, client_id: &str, sender: Option<&EchoSender>) -> Option<LocalEcho> {
        let base = LocalEcho {
            chat_id: self.chat_id().clone(),
            topic_id: self.topic_id().cloned(),
            client_id: client_id.to_owned(),
            sender_id: sender.map(|s| s.user_id.clone()),
            sender_name: sender.and_then(|s| s.name.clone()),
            ..Default::default()
        };
        let reply = |r: &Option<ch_protocol::methods::ReplyTarget>| {
            r.as_ref().map(|t| ReplyRef {
                message_id: t.message_id.clone(),
                chat_id: None,
                quote_text: t.quote_text.clone(),
                quote_entities: None,
                quote_offset: t.quote_offset,
                sender_id: None,
                preview: None,
            })
        };
        Some(match self {
            Self::SendText(p) => LocalEcho {
                text: Some(p.text.clone()),
                entities: p.entities.clone(),
                reply_to: reply(&p.reply_to),
                scheduled_at: p.schedule_at,
                ..base
            },
            Self::SendFile(p) => LocalEcho {
                text: p.caption.clone(),
                entities: p.entities.clone(),
                reply_to: reply(&p.reply_to),
                scheduled_at: p.schedule_at,
                media: Some(file_media(
                    &p.path,
                    p.kind.as_ref(),
                    p.as_file,
                    p.spoiler,
                    p,
                )),
                ..base
            },
            Self::SendAlbum(p) => {
                let first = p.items.first()?;
                LocalEcho {
                    text: p.caption.clone().or_else(|| first.caption.clone()),
                    entities: p.entities.clone(),
                    reply_to: reply(&p.reply_to),
                    scheduled_at: p.schedule_at,
                    media: Some(Media {
                        path: Some(first.path.clone()),
                        has_spoiler: first.spoiler.unwrap_or(false),
                        ..empty_media(album_kind(first.kind.as_ref(), first.as_file))
                    }),
                    ..base
                }
            }
            Self::Forward(_) | Self::Edit(_) | Self::Delete(_) => return None,
        })
    }
}

fn album_kind(kind: Option<&FileKind>, as_file: Option<bool>) -> MediaKind {
    if as_file == Some(true) {
        return MediaKind::Document;
    }
    match kind {
        Some(FileKind::Photo) => MediaKind::Photo,
        Some(FileKind::Video) => MediaKind::Video,
        Some(FileKind::Voice) => MediaKind::Voice,
        Some(FileKind::VideoNote) => MediaKind::VideoNote,
        Some(FileKind::Document) => MediaKind::Document,
        _ => MediaKind::Other,
    }
}

fn file_media(
    path: &str,
    kind: Option<&FileKind>,
    as_file: Option<bool>,
    spoiler: Option<bool>,
    p: &MessagesSendFileParams,
) -> Media {
    Media {
        mime: p.mime.clone(),
        filename: p.filename.clone().or_else(|| {
            std::path::Path::new(path)
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
        }),
        duration: p.duration,
        path: Some(path.to_owned()),
        waveform: p.waveform.clone(),
        has_spoiler: spoiler.unwrap_or(false),
        ..empty_media(album_kind(kind, as_file))
    }
}

fn empty_media(kind: MediaKind) -> Media {
    Media {
        kind,
        mime: None,
        bytes: None,
        filename: None,
        width: None,
        height: None,
        duration: None,
        thumb_path: None,
        path: None,
        waveform: None,
        title: None,
        performer: None,
        has_spoiler: false,
        sticker: None,
    }
}

/// Who the local echo is from (the account's own user), so the thread can render it before the
/// platform answers.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct EchoSender {
    /// The account's user id.
    pub user_id: Id,
    /// Display name.
    pub name: Option<String>,
}

/// A snapshot of one item (the `outbox` row without its params).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutboxItem {
    /// Row id; the handle for `cancel` / `retry` / `delete`.
    pub id: i64,
    /// Owner account.
    pub account_id: String,
    /// Lane.
    pub chat_id: Id,
    /// Topic, if any.
    pub topic_id: Option<Id>,
    /// Idempotency key; the local echo is `local:<client_id>`.
    pub client_id: String,
    /// What it is.
    pub kind: OutboxKind,
    /// Where it is.
    pub state: OutboxState,
    /// Attempts so far.
    pub attempts: u32,
    /// When `queued` and set: not before this (unix ms).
    pub next_attempt_at: Option<Millis>,
    /// The last error, if any attempt failed.
    pub last_error: Option<ErrorObject>,
    /// The local echo's message rowid.
    pub echo_message_id: Option<i64>,
    /// What the platform created, once `sent`.
    pub result_message_ids: Option<Vec<Id>>,
    /// Upload progress.
    pub done_bytes: Option<i64>,
    /// Upload size, when known.
    pub total_bytes: Option<i64>,
    /// Enqueued at (unix ms).
    pub created_at: Millis,
    /// Last change (unix ms).
    pub updated_at: Millis,
}

impl OutboxItem {
    fn from_row(r: &OutboxRow) -> Self {
        Self {
            id: r.id,
            account_id: r.account_id.clone(),
            chat_id: r.chat_id.clone(),
            topic_id: r.topic_id.clone(),
            client_id: r.client_id.clone(),
            kind: OutboxKind::parse(&r.kind).unwrap_or(OutboxKind::SendText),
            state: OutboxState::parse(&r.state).unwrap_or(OutboxState::Failed),
            attempts: u32::try_from(r.attempts).unwrap_or(u32::MAX),
            next_attempt_at: r.next_attempt_at,
            last_error: r
                .last_error
                .clone()
                .and_then(|v| serde_json::from_value(v).ok()),
            echo_message_id: r.echo_message_id,
            result_message_ids: r.result_message_ids.clone(),
            done_bytes: r.done_bytes,
            total_bytes: r.total_bytes,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

/// Published on every change.
#[derive(Debug, Clone, PartialEq)]
// Keep the owned snapshot API, as on CoreChange; the broadcast queue is bounded.
#[allow(clippy::large_enum_variant)]
pub enum OutboxChange {
    /// An item was created or changed (state, attempt, error, progress).
    Item(OutboxItem),
    /// An item was deleted.
    Removed {
        /// Owner account.
        account_id: String,
        /// Row id.
        id: i64,
        /// Its client id.
        client_id: String,
    },
    /// The ingest effect that accompanied an outbox step (local echo, adopted result, failed
    /// echo, removed echo). Forward it like any other ingest result.
    Ingested(Applied),
}

struct Inflight {
    plugin_id: String,
    row_id: i64,
    request_id: Arc<OnceLock<i64>>,
    cancel: CancellationToken,
    account_id: String,
    chat_id: Id,
    kind: OutboxKind,
    /// `true` while the item is being reconciled (no request in flight to cancel).
    reconciling: bool,
}

struct Worker {
    plugin_id: String,
    stop: CancellationToken,
    notify: Arc<Notify>,
    task: JoinHandle<()>,
}

struct Lifetime(CancellationToken);

impl Drop for Lifetime {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[cfg(target_os="linux")]
struct MetadataJobs {
    queue:crate::runtime::reference_queue::ReferenceQueue,
    jobs:Arc<crate::managed_sources::OwnedJobs>,
}

pub(crate) struct Inner {
    #[cfg(target_os="linux")]
    metadata:Option<MetadataJobs>,
    ingest: Arc<Mutex<Ingest>>,
    transport: Arc<dyn SendTransport>,
    policy: OutboxPolicy,
    changes: broadcast::Sender<OutboxChange>,
    workers: Mutex<HashMap<String, Worker>>,
    inflight: Mutex<HashMap<String, Inflight>>,
    paused: Mutex<HashSet<String>>,
    stopping: CancellationToken,
    wall_start: Millis,
    clock_start: tokio::time::Instant,
    bus: Option<ChangeBus>,
    #[cfg(target_os = "linux")]
    managed: Option<managed_admission::Context>,
    #[cfg(test)]
    text_checkpoint: Mutex<Option<Arc<TextCheckpoint>>>,
    #[cfg(all(test, target_os = "linux"))]
    managed_checkpoint: Mutex<Option<Arc<TextCheckpoint>>>,
}
#[cfg(test)]
type TextCheckpoint = dyn Fn(&str) + Send + Sync;

/// The outbox service. Cheap to clone; all clones share one state.
#[derive(Clone)]
pub struct Outbox {
    inner: Arc<Inner>,
    _lifetime: Arc<Lifetime>,
}

impl std::fmt::Debug for Outbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Outbox")
            .field("policy", &self.inner.policy)
            .finish()
    }
}

fn validate_admission_id(id: &str) -> Result<()> {
    if id.len() == 32 && id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
        Ok(())
    } else {
        Err(OutboxError::InvalidRequest("invalid local client ID".into()))
    }
}

// Descriptive indexed barrier only; this never creates a source/read/dispatch permit.
fn managed_row(conn: &rusqlite::Connection, row: &OutboxRow) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM managed_source WHERE client_id=?1)
             OR EXISTS(SELECT 1 FROM managed_outbox_dispatch WHERE outbox_id=?2)",
        rusqlite::params![row.client_id, row.id], |r| r.get(0),
    ).map_err(ch_db::Error::from)?)
}

fn matching_admission(
    tx: &rusqlite::Transaction<'_>,
    owner: &AccountRow,
    client_id: &str,
    request: &OutboxRequest,
) -> Result<Option<OutboxItem>> {
    let current = ch_db::get_account(tx, &owner.account_id)?;
    if !current.is_some_and(|a| a.plugin_id == owner.plugin_id && a.created_at == owner.created_at) {
        return Err(OutboxError::AdmissionConflict);
    }
    let Some(row) = ch_db::get_outbox_by_client_id(tx, client_id)? else {
        return Ok(None);
    };
    let kind = request.kind();
    if row.account_id != owner.account_id
        || &row.chat_id != request.chat_id()
        || row.topic_id.as_ref() != request.topic_id()
        || row.kind != kind.as_str()
        || row.method != kind.method()
        || row.params != request.params(&owner.account_id, client_id)
    {
        return Err(OutboxError::AdmissionConflict);
    }
    Ok(Some(OutboxItem::from_row(&row)))
}

impl Outbox {
    /// Build on a shared ingest (the same one the event sink writes through) and a transport
    /// (the [`crate::host::HostHandle`]). Workers start with [`Outbox::start`].
    pub fn new(
        ingest: Arc<Mutex<Ingest>>,
        transport: Arc<dyn SendTransport>,
        policy: OutboxPolicy,
    ) -> Self {
        let (changes, _) = broadcast::channel(512);
        let stopping = CancellationToken::new();
        Self {
            _lifetime: Arc::new(Lifetime(stopping.clone())),
            inner: Arc::new(Inner {
                #[cfg(target_os="linux")]
                metadata:None,
                ingest,
                transport,
                policy,
                changes,
                workers: Mutex::new(HashMap::new()),
                inflight: Mutex::new(HashMap::new()),
                paused: Mutex::new(HashSet::new()),
                stopping,
                wall_start: now_millis(),
                clock_start: tokio::time::Instant::now(),
                bus: None,
                #[cfg(target_os = "linux")]
                managed: None,
                #[cfg(test)]
                text_checkpoint: Mutex::default(),
                #[cfg(all(test, target_os = "linux"))]
                managed_checkpoint: Mutex::default(),
            }),
        }
    }

    /// Use the runtime's main writer, live ingest options, host and change bus.
    /// Call start once after construction to recover persisted rows.
    pub fn for_core(core: &Core, policy: OutboxPolicy) -> Self {
        let mut outbox = Self::new(
            core.ingest().shared_ingest(),
            Arc::new(core.host().clone()),
            policy,
        );
        Arc::get_mut(&mut outbox.inner).expect("new outbox").bus = Some(core.bus().clone());
        #[cfg(target_os = "linux")]
        {
            let sources = core.managed_sources();
            let inner = Arc::get_mut(&mut outbox.inner).expect("new outbox");
            inner.metadata=Some(MetadataJobs {queue:core.ingest().reference_queue(),jobs:crate::managed_sources::OwnedJobs::new()});
            inner.managed = Some(managed_admission::Context {
                sources, host: core.host().clone(), accounts: core.accounts().clone(),
            });
        }
        let weak = Arc::downgrade(&outbox.inner);
        let stop = outbox.inner.stopping.clone();
        let mut events = core.subscribe();
        tokio::spawn(async move {
            loop {
                let event = tokio::select! {
                    _ = stop.cancelled() => break,
                    event = events.recv() => event,
                };
                let Some(inner) = weak.upgrade() else { break };
                match event {
                    Ok(crate::runtime::CoreChange::Ingested { plugin_id, applied }) => {
                        for change in &applied.changes {
                            if let Change::TransferProgress(progress) = change {
                                inner.record_progress(&plugin_id, progress);
                            }
                            if let Change::MessagesUpserted {
                                chat_id,
                                created,
                                updated,
                            } = change
                                && let Err(e) = inner.publish_confirmed(
                                    &applied.account_id,
                                    chat_id,
                                    created.iter().chain(updated),
                                )
                            {
                                tracing::warn!(error=%e,"outbox echo notification failed");
                            }
                        }
                    }
                    Ok(crate::runtime::CoreChange::Account(
                        crate::runtime::AccountChange::Added { account_id, .. }
                        | crate::runtime::AccountChange::Removed { account_id, .. },
                    )) => {
                        if let Err(e) = Self::refresh_worker(&inner, &account_id) {
                            tracing::warn!(error=%e,"outbox account refresh failed");
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        if let Err(e) = inner.reconcile_all_late() {
                            tracing::warn!(error=%e,"outbox echo catch-up failed");
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    _ => {}
                }
            }
        });
        outbox
    }

    /// The policy in force.
    pub fn policy(&self) -> &OutboxPolicy {
        &self.inner.policy
    }

    /// Listen for changes.
    pub fn subscribe(&self) -> broadcast::Receiver<OutboxChange> {
        self.inner.changes.subscribe()
    }

    /// Start a worker for every account in the database. Rows left in `sending` by a previous
    /// process are reconciled, not re-sent.
    pub fn start(&self) -> Result<()> {
        self.inner.reconcile_all_late()?;
        let accounts = {
            let g = self.inner.ingest();
            ch_db::list_accounts(g.db().conn())?
        };
        for a in accounts {
            Self::refresh_worker(&self.inner, &a.account_id)?;
        }
        Ok(())
    }

    /// Start (or wake) the worker of one account; call after an account is created.
    pub fn start_account(&self, account_id: &str) -> Result<()> {
        self.inner.plugin_of(account_id)?;
        Self::refresh_worker(&self.inner, account_id)?;
        Ok(())
    }

    /// Stop scheduling and wait for the account workers and their outstanding tasks to exit.
    /// Ambiguous in-flight rows remain durable for the next instance to reconcile.
    pub async fn shutdown(&self) {
        self.inner.stopping.cancel();
        let workers =
            std::mem::take(&mut *self.inner.workers.lock().unwrap_or_else(|e| e.into_inner()));
        for (_, worker) in workers {
            let _ = worker.task.await;
        }
        #[cfg(target_os="linux")]
        if let Some(metadata)=&self.inner.metadata
            && metadata.jobs.drain().await.is_err() {
            tracing::warn!("draining accepted outbox metadata writes failed");
        }
    }

    /// Queue a request. Sends get a local echo at once (`delivery: sending`). Returns the item.
    pub fn enqueue(
        &self,
        account_id: &str,
        request: OutboxRequest,
        sender: Option<EchoSender>,
    ) -> Result<OutboxItem> {
        if self.inner.stopping.is_cancelled() {
            return Err(OutboxError::Stopped);
        }
        self.enqueue_inner(account_id, None, request, sender)
    }

    /// Admit a predetermined local client ID, or return its identical retained row.
    /// After row deletion only the caller's durable claim can prevent reinsertion.
    /// This ID does not deduplicate file sends at the plugin or platform.
    pub fn enqueue_with_id(
        &self,
        owner: &AccountRow,
        client_id: &str,
        request: OutboxRequest,
        sender: Option<EchoSender>,
    ) -> Result<OutboxItem> {
        validate_admission_id(client_id)?;
        self.enqueue_inner(&owner.account_id, Some((owner, client_id)), request, sender)
    }

    /// Recover a retained admission under one read snapshot, without scheduling a worker.
    /// Absence does not establish that a previous admission never committed.
    pub fn get_matching_admission(
        &self,
        owner: &AccountRow,
        client_id: &str,
        request: &OutboxRequest,
    ) -> Result<Option<OutboxItem>> {
        validate_admission_id(client_id)?;
        request.validate()?;
        let g = self.inner.ingest();
        let tx = rusqlite::Transaction::new_unchecked(
            g.db().conn(),
            rusqlite::TransactionBehavior::Deferred,
        )
        .map_err(ch_db::Error::from)?;
        let item = matching_admission(&tx, owner, client_id, request)?;
        tx.commit().map_err(ch_db::Error::from)?;
        Ok(item)
    }

    fn enqueue_inner(
        &self,
        account_id: &str,
        fixed: Option<(&AccountRow, &str)>,
        request: OutboxRequest,
        sender: Option<EchoSender>,
    ) -> Result<OutboxItem> {
        request.validate()?;
        let kind = request.kind();
        let (applied, item) = {
            let g = self.inner.ingest();
            // BEGIN IMMEDIATE reserves the writer across connections before either lookup.
            let tx = g.db().transaction()?;
            let client_id: String = if let Some((owner, id)) = fixed {
                if let Some(item) = matching_admission(&tx, owner, id, &request)? {
                    return Ok(item);
                }
                id.to_owned()
            } else {
                if ch_db::get_account(&tx, account_id)?.is_none() {
                    return Err(OutboxError::UnknownAccount(account_id.to_owned()));
                }
                tx.query_row("SELECT lower(hex(randomblob(16)))", [], |r| r.get(0))
                    .map_err(ch_db::Error::from)?
            };
            if self.inner.stopping.is_cancelled() {
                return Err(OutboxError::Stopped);
            }
            let (applied, item) = match self.insert_in(&g, &tx, account_id, &client_id, &request, sender.as_ref()) {
                Ok(inserted) => inserted,
                Err(error) => {
                    if let Some((owner, id)) = fixed
                        && matches!(&error, OutboxError::Db(ch_db::Error::Sqlite(rusqlite::Error::SqliteFailure(code, _)))
                            if code.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE)
                    {
                        tx.rollback().map_err(ch_db::Error::from)?;
                        drop(g);
                        return self.get_matching_admission(owner, id, &request)?.ok_or(error);
                    }
                    return Err(error);
                }
            };
            tx.commit().map_err(ch_db::Error::from)?;
            (applied, item)
        };

        info!(account = account_id, id = item.id, kind = kind.as_str(), chat = %item.chat_id, "outbox: enqueued");
        if let Some(a) = applied {
            self.inner.publish(OutboxChange::Ingested(a));
        }
        self.inner.publish(OutboxChange::Item(item.clone()));
        if let Err(error) = Self::refresh_worker(&self.inner, account_id) {
            if fixed.is_none() {
                return Err(error);
            }
            // Stable-ID callers need the evidence of a successful local commit even if
            // scheduling failed. Outbox::start/start_account can pick up the queued row.
            tracing::warn!(account = account_id, id = item.id, "outbox: admitted but worker refresh failed");
        }
        Ok(item)
    }

    // The caller owns the ingest lock and writer transaction. Never re-enter enqueue here.
    fn insert_in(
        &self,
        ingest: &Ingest,
        tx: &rusqlite::Transaction<'_>,
        account_id: &str,
        client_id: &str,
        request: &OutboxRequest,
        sender: Option<&EchoSender>,
    ) -> Result<(Option<Applied>, OutboxItem)> {
        self.insert_with_echo_in(tx, account_id, client_id, request, sender,
            |echo| Ok(ingest.apply_local_echo_in(tx, account_id, echo)?))
    }

    // Both NORMAL and FULL callers supply their actual transaction and echo
    // helper. This body never reads another connection or locks shared ingest.
    fn insert_with_echo_in(
        &self,
        tx: &rusqlite::Transaction<'_>,
        account_id: &str,
        client_id: &str,
        request: &OutboxRequest,
        sender: Option<&EchoSender>,
        apply_echo: impl FnOnce(&LocalEcho) -> Result<(Applied, i64)>,
    ) -> Result<(Option<Applied>, OutboxItem)> {
        let kind = request.kind();
        let mut row = OutboxRow {
            account_id: account_id.to_owned(),
            chat_id: request.chat_id().clone(),
            topic_id: request.topic_id().cloned(),
            client_id: client_id.to_owned(),
            kind: kind.as_str().to_owned(),
            method: kind.method().to_owned(),
            params: request.params(account_id, client_id),
            created_at: self.inner.now(),
            ..Default::default()
        };
        let applied = if let Some(echo) = request.echo(client_id, sender) {
            let (change, echo_id) = apply_echo(&echo)?;
            row.echo_message_id = Some(echo_id);
            Some(change)
        } else {
            None
        };
        let id = ch_db::insert_outbox(tx, &row)?;
        let stored = ch_db::get_outbox(tx, id)?.ok_or(OutboxError::NotFound(id))?;
        Ok((applied, OutboxItem::from_row(&stored)))
    }

    /// Cancel a queued or sending item. A request already in flight gets `$/cancelRequest`; if
    /// the plugin had already finished, the result stands and the item becomes `sent` (§11.0).
    /// Returns the item as it is now (`cancelled`, or still `sending` while the plugin answers).
    pub fn cancel(&self, id: i64) -> Result<OutboxItem> {
        let g = self.inner.ingest();
        let row = Inner::row_in(&g, id)?;
        if managed_row(g.db().conn(), &row)? { return Err(OutboxError::ManagedUnavailable); }
        let state = OutboxState::parse(&row.state).unwrap_or(OutboxState::Failed);
        if let Some(mid) = Inner::adopted_id(&g, &row)?
            && matches!(state, OutboxState::Sending | OutboxState::Queued)
        {
            worker::confirm(&self.inner, &g, &row, &mid)?;
            return Inner::item_in(&g, id);
        }
        match state {
            OutboxState::Queued => {
                let item = self.inner.finish_cancelled(&g, &row)?;
                self.inner.wake(&row.account_id);
                Ok(item)
            }
            OutboxState::Sending => {
                let g = self
                    .inner
                    .inflight
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                match g.get(&row.client_id) {
                    Some(f) if !f.reconciling => {
                        debug!(id, "outbox: cancelling in-flight request");
                        f.cancel.cancel();
                        Ok(OutboxItem::from_row(&row))
                    }
                    _ => Err(OutboxError::WrongState {
                        id,
                        state,
                        op: "cancel (outcome is being reconciled)",
                    }),
                }
            }
            s => Err(OutboxError::WrongState {
                id,
                state: s,
                op: "cancel",
            }),
        }
    }

    /// Re-queue a failed item with a fresh attempt budget; the echo shows `sending` again.
    pub fn retry(&self, id: i64) -> Result<OutboxItem> {
        let g = self.inner.ingest();
        let row = Inner::row_in(&g, id)?;
        if managed_row(g.db().conn(), &row)? { return Err(OutboxError::ManagedUnavailable); }
        let state = OutboxState::parse(&row.state).unwrap_or(OutboxState::Failed);
        if state != OutboxState::Failed {
            return Err(OutboxError::WrongState {
                id,
                state,
                op: "retry",
            });
        }
        let tx = g.db().transaction()?;
        let delivery = if let Some(mid) = Inner::adopted_id(&g, &row)? {
            ch_db::set_outbox_sent(&tx, id, &[mid])?;
            "sent"
        } else {
            tx.execute(
                "UPDATE outbox SET state='queued', attempts=0, last_error_json=NULL,
                 next_attempt_at=NULL, request_id=NULL, done_bytes=NULL, total_bytes=NULL,
                 updated_at=?2 WHERE id=?1",
                rusqlite::params![id, self.inner.now()],
            )
            .map_err(ch_db::Error::from)?;
            "sending"
        };
        let applied = Inner::set_echo_delivery(&g, &row, delivery)?;
        let item = Inner::item_in(&g, id)?;
        tx.commit().map_err(ch_db::Error::from)?;
        if let Some(a) = applied {
            self.inner.publish(OutboxChange::Ingested(a));
        }
        info!(id, account = %row.account_id, "outbox: retry");
        self.inner.publish(OutboxChange::Item(item.clone()));
        self.inner.wake(&row.account_id);
        Ok(item)
    }

    /// Remove an item that is not in flight (failed, cancelled, sent, or still queued) together
    /// with its local echo, if any.
    pub fn delete(&self, id: i64) -> Result<()> {
        let g = self.inner.ingest();
        let row = Inner::row_in(&g, id)?;
        if managed_row(g.db().conn(), &row)? { return Err(OutboxError::ManagedUnavailable); }
        let state = OutboxState::parse(&row.state).unwrap_or(OutboxState::Failed);
        if state == OutboxState::Sending {
            return Err(OutboxError::WrongState {
                id,
                state,
                op: "delete",
            });
        }
        let applied = {
            let tx = g.db().transaction()?;
            let applied = if state != OutboxState::Sent {
                Inner::remove_echo(&g, &row)?
            } else {
                None
            };
            ch_db::delete_outbox(&tx, id)?;
            tx.commit().map_err(ch_db::Error::from)?;
            applied
        };
        if let Some(a) = applied {
            self.inner.publish(OutboxChange::Ingested(a));
        }
        self.inner.publish(OutboxChange::Removed {
            account_id: row.account_id.clone(),
            id,
            client_id: row.client_id,
        });
        self.inner.wake(&row.account_id);
        Ok(())
    }

    /// One item.
    pub fn get(&self, id: i64) -> Result<OutboxItem> {
        Inner::item_in(&self.inner.ingest(), id)
    }

    /// One item by client id.
    pub fn get_by_client_id(&self, client_id: &str) -> Result<Option<OutboxItem>> {
        let g = self.inner.ingest();
        Ok(ch_db::get_outbox_by_client_id(g.db().conn(), client_id)?
            .as_ref()
            .map(OutboxItem::from_row))
    }

    /// Every item of an account, oldest first.
    pub fn list(&self, account_id: &str) -> Result<Vec<OutboxItem>> {
        let g = self.inner.ingest();
        Ok(ch_db::list_outbox(g.db().conn(), account_id, None)?
            .iter()
            .map(OutboxItem::from_row)
            .collect())
    }

    /// Record progress only for the exact in-flight wire request, plugin and account.
    pub fn on_transfer_progress(&self, plugin_id: &str, event: &TransferProgressEvent) {
        self.inner.record_progress(plugin_id, event);
    }

    fn refresh_worker(inner: &Arc<Inner>, account_id: &str) -> Result<()> {
        let ingest = inner.ingest();
        let owner = ch_db::get_account(ingest.db().conn(), account_id)?;
        let mut workers = inner.workers.lock().unwrap_or_else(|e| e.into_inner());
        if inner.stopping.is_cancelled() {
            return Ok(());
        }
        if let Some(w) = workers.get(account_id)
            && owner.as_ref().is_some_and(|a| a.plugin_id == w.plugin_id)
        {
            w.notify.notify_one();
            return Ok(());
        }
        if let Some(w) = workers.remove(account_id) {
            w.stop.cancel();
            inner.clear_paused(account_id);
        }
        if let Some(a) = owner {
            let stop = inner.stopping.child_token();
            let notify = Arc::new(Notify::new());
            let task = tokio::spawn(worker::run(
                inner.clone(),
                account_id.to_owned(),
                a.plugin_id.clone(),
                notify.clone(),
                stop.clone(),
            ));
            workers.insert(
                account_id.to_owned(),
                Worker {
                    notify,
                    task,
                    stop,
                    plugin_id: a.plugin_id,
                },
            );
        }
        Ok(())
    }

    /// Requests in flight or being reconciled, across accounts.
    pub fn in_flight(&self) -> usize {
        self.inner
            .inflight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }
}

impl Inner {
    fn ingest(&self) -> MutexGuard<'_, Ingest> {
        self.ingest.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn record_progress(&self, plugin_id: &str, e: &TransferProgressEvent) {
        let Some(ch_protocol::RequestId::Number(request)) = e.request_id.as_ref() else {
            return;
        };
        if e.direction != TransferDirection::Upload
            || self.plugin_of(&e.account_id).ok().as_deref() != Some(plugin_id)
        {
            return;
        }
        let g = self.ingest();
        let active = self.inflight.lock().unwrap_or_else(|e| e.into_inner());
        let Some((client_id, f)) = active.iter().find(|(_, f)| {
            f.plugin_id == plugin_id
                && f.kind.is_upload()
                && !f.reconciling
                && !f.cancel.is_cancelled()
                && f.account_id == e.account_id
                && f.request_id.get() == Some(request)
                && e.chat_id.as_ref().is_none_or(|c| c == &f.chat_id)
        }) else {
            return;
        };
        let id = f.row_id;
        let Ok(tx) = g.db().transaction() else { return };
        let Ok(row) = Self::row_in(&g, id) else {
            return;
        };
        if !matches!(managed_row(g.db().conn(), &row), Ok(false)) { return; }
        let done = i64::try_from(e.done_bytes).unwrap_or(i64::MAX);
        if row.client_id != *client_id
            || row.state != outbox_state::SENDING
            || row.done_bytes.is_some_and(|v| v > done)
        {
            return;
        }
        let total = e
            .total_bytes
            .map(|t| i64::try_from(t).unwrap_or(i64::MAX))
            .or(row.total_bytes);
        if ch_db::set_outbox_progress(g.db().conn(), id, done, total).is_ok()
            && let Ok(item) = Self::item_in(&g, id)
            && tx.commit().is_ok()
        {
            self.publish(OutboxChange::Item(item));
        }
    }

    fn publish_confirmed<'a>(
        &self,
        account: &str,
        chat: &str,
        messages: impl Iterator<Item = &'a String>,
    ) -> Result<()> {
        let g = self.ingest();
        let conn = g.db().conn();
        let Some(conv) = ch_db::conversation_rowid(conn, account, chat)? else {
            return Ok(());
        };
        let ids = serde_json::to_string(&messages.collect::<Vec<_>>())?;
        let mut stmt = conn
            .prepare_cached(
                "SELECT o.id FROM json_each(?1) ids JOIN message m ON m.message_id=ids.value
             JOIN outbox o ON o.echo_message_id=m.id
             WHERE m.conversation_id=?2 AND o.account_id=?3 AND o.state='sent'",
            )
            .map_err(ch_db::Error::from)?;
        let mut rows = stmt
            .query(rusqlite::params![ids, conv, account])
            .map_err(ch_db::Error::from)?;
        while let Some(row) = rows.next().map_err(ch_db::Error::from)? {
            self.publish(OutboxChange::Item(Self::item_in(
                &g,
                row.get(0).map_err(ch_db::Error::from)?,
            )?));
        }
        Ok(())
    }

    fn reconcile_late(&self, account: &str, chat: Option<&str>) -> Result<()> {
        let g = self.ingest();
        for row in ch_db::list_outbox(g.db().conn(), account, Some(outbox_state::FAILED))? {
            if chat.is_none_or(|c| c == row.chat_id)
                && let Some(mid) = Self::adopted_id(&g, &row)?
            {
                worker::confirm(self, &g, &row, &mid)?;
            }
        }
        Ok(())
    }

    fn reconcile_all_late(&self) -> Result<()> {
        let accounts = ch_db::list_accounts(self.ingest().db().conn())?;
        for a in accounts {
            self.reconcile_late(&a.account_id, None)?;
        }
        Ok(())
    }

    fn publish(&self, c: OutboxChange) {
        if let (Some(bus), OutboxChange::Ingested(applied)) = (&self.bus, &c) {
            bus.publish_local("outbox", applied.clone());
        }
        let _ = self.changes.send(c);
    }

    fn wake(&self, account_id: &str) {
        if let Some(w) = self
            .workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(account_id)
        {
            w.notify.notify_one();
        }
    }

    fn plugin_of(&self, account_id: &str) -> Result<String> {
        let g = self.ingest();
        ch_db::get_account(g.db().conn(), account_id)?
            .map(|a| a.plugin_id)
            .ok_or_else(|| OutboxError::UnknownAccount(account_id.to_owned()))
    }

    fn now(&self) -> Millis {
        self.wall_start.saturating_add(
            i64::try_from(self.clock_start.elapsed().as_millis()).unwrap_or(i64::MAX),
        )
    }

    fn row_in(g: &Ingest, id: i64) -> Result<OutboxRow> {
        ch_db::get_outbox(g.db().conn(), id)?.ok_or(OutboxError::NotFound(id))
    }

    fn same_item(g: &Ingest, row: &OutboxRow) -> Result<bool> {
        if managed_row(g.db().conn(), row)? { return Ok(false); }
        Ok(ch_db::get_outbox(g.db().conn(), row.id)?
            .is_some_and(|r| r.client_id == row.client_id && r.account_id == row.account_id))
    }

    fn item_in(g: &Ingest, id: i64) -> Result<OutboxItem> {
        Ok(OutboxItem::from_row(&Self::row_in(g, id)?))
    }

    fn adopted_id(g: &Ingest, row: &OutboxRow) -> Result<Option<Id>> {
        if managed_row(g.db().conn(), row)? { return Ok(None); }
        let Some(echo) = row.echo_message_id else {
            return Ok(None);
        };
        let conn = g.db().conn();
        if crate::ingest::basic_text_reply(row)
            && !ch_db::get_outbox(conn, row.id)?.is_some_and(|current| {
                current.account_id == row.account_id && current.client_id == row.client_id
                    && current.echo_message_id == Some(echo)
            })
        {
            // Even exact-looking metadata cannot restore a pointer revoked by
            // ON DELETE SET NULL after this worker captured its outbox row.
            return Ok(None);
        }
        let Some(message) = ch_db::get_message_by_id(conn, echo)?
            .filter(|m| m.message_id != format!("local:{}", row.client_id) && m.direction == "out") else {
            return Ok(None);
        };
        if crate::ingest::basic_text_reply(row) {
            let owner = ch_db::get_account(conn, &row.account_id)?;
            let conversation = ch_db::get_conversation_by_id(conn, message.conversation_id)?;
            let exact_owner = conversation.is_some_and(|c| c.account_id == row.account_id && c.chat_id == row.chat_id);
            let correlated = owner.is_some_and(|a| message.ext.as_ref()
                .and_then(|ext| ext.get(&a.plugin_id)).and_then(|ext| ext.get("client_id"))
                .and_then(serde_json::Value::as_str) == Some(row.client_id.as_str()));
            if !exact_owner || !correlated || message.message_id.starts_with("local:")
                || matches!(message.delivery.as_deref(), Some("sending" | "failed"))
                || !crate::ingest::text_reply_matches(row, message.topic_id.as_deref(), message.reply_to.as_ref())
            {
                return Ok(None);
            }
        }
        Ok(Some(message.message_id))
    }

    /// Set the echo row's `delivery` (when the echo still carries its `local:` id or has been
    /// adopted but not marked). Returns the change to publish.
    fn set_echo_delivery(g: &Ingest, row: &OutboxRow, delivery: &str) -> Result<Option<Applied>> {
        Self::set_echo_delivery_in(g.db().conn(),row,delivery)
    }
    fn set_echo_delivery_in(conn:&rusqlite::Connection,row:&OutboxRow,delivery:&str)->Result<Option<Applied>> {
        let Some(echo_id) = row.echo_message_id else {
            return Ok(None);
        };
        let Some(mut m) = ch_db::get_message_by_id(conn, echo_id)? else {
            return Ok(None);
        };
        if crate::ingest::basic_text_reply(row) {
            let retained = ch_db::get_outbox(conn, row.id)?.is_some_and(|current| {
                current.account_id == row.account_id && current.client_id == row.client_id
                    && current.echo_message_id == Some(echo_id)
            });
            let scope = ch_db::get_conversation_by_id(conn, m.conversation_id)?
                .is_some_and(|c| c.account_id == row.account_id && c.chat_id == row.chat_id);
            if !retained || !scope || m.topic_id != row.topic_id {
                // A worker's captured pointer may outlive deletion and INTEGER PK
                // reuse; ON DELETE SET NULL invalidates that old status authority.
                return Ok(None);
            }
        }
        if m.delivery.as_deref() == Some(delivery)
            || (delivery == "sent" && matches!(m.delivery.as_deref(), Some("read" | "delivered")))
        {
            return Ok(None);
        }
        m.delivery = Some(delivery.to_owned());
        m.updated_at = now_millis();
        ch_db::update_message(conn, &m)?;
        Ok(Some(Applied {
            account_id: row.account_id.clone(),
            event: format!("local.delivery_{delivery}"),
            changes: vec![Change::MessagesUpserted {
                chat_id: ch_db::get_conversation_by_id(conn, m.conversation_id)?
                    .map_or_else(|| row.chat_id.clone(), |c| c.chat_id),
                created: vec![],
                updated: vec![m.message_id],
            }],
            dropped_paths: 0,
        }))
    }

    /// Hard-delete the local echo (cancel, delete). Only while it still is a local echo.
    fn remove_echo(g: &Ingest, row: &OutboxRow) -> Result<Option<Applied>> {
        let Some(echo_id) = row.echo_message_id else {
            return Ok(None);
        };
        let key = format!("local:{}", row.client_id);
        let Some(m) = ch_db::get_message_by_id(g.db().conn(), echo_id)? else {
            return Ok(None);
        };
        if m.message_id != key {
            // Adopted by a platform echo meanwhile: it was sent after all; leave it.
            return Ok(None);
        }
        let mut applied = Applied {
            account_id: row.account_id.clone(),
            event: "local.deleted".into(),
            ..Default::default()
        };
        g.delete_messages(
            g.db().conn(),
            &row.account_id,
            Some(&row.chat_id),
            &[key],
            "me",
            false,
            now_millis(),
            &mut applied,
        )?;
        Ok(Some(applied))
    }

    fn finish_cancelled(&self, g: &Ingest, row: &OutboxRow) -> Result<OutboxItem> {
        let (applied, item) = {
            let tx = g.db().transaction()?;
            if !Self::same_item(g, row)? {
                return Err(OutboxError::NotFound(row.id));
            }
            ch_db::set_outbox_state(
                g.db().conn(),
                row.id,
                outbox_state::CANCELLED,
                None,
                None,
                None,
            )?;
            let applied = Self::remove_echo(g, row)?;
            let item = Self::item_in(g, row.id)?;
            tx.commit().map_err(ch_db::Error::from)?;
            (applied, item)
        };
        info!(id = row.id, account = %row.account_id, "outbox: cancelled");
        if let Some(a) = applied {
            self.publish(OutboxChange::Ingested(a));
        }
        self.publish(OutboxChange::Item(item.clone()));
        Ok(item)
    }

    fn is_paused(&self, account_id: &str) -> bool {
        self.paused
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(account_id)
    }

    fn set_paused(&self, account_id: &str) {
        self.paused
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(account_id.to_owned());
    }

    fn clear_paused(&self, account_id: &str) {
        self.paused
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(account_id);
    }

    pub(crate) fn unknown_outcome_error(reason: &str) -> ErrorObject {
        ErrorObject::new(
            ErrorCode::Internal,
            format!("outcome unknown: {reason}; no platform echo arrived"),
        )
        .with_data(|d| d.reason = Some("outcome_unknown".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_round_trip() {
        for k in [
            OutboxKind::SendText,
            OutboxKind::SendFile,
            OutboxKind::SendAlbum,
            OutboxKind::Forward,
            OutboxKind::Edit,
            OutboxKind::Delete,
        ] {
            assert_eq!(OutboxKind::parse(k.as_str()), Some(k));
            assert!(ch_protocol::method(k.method()).is_some(), "{}", k.method());
        }
    }

    #[test]
    fn params_omit_unset_optionals() {
        let r = OutboxRequest::SendText(MessagesSendParams {
            chat_id: "c1".into(),
            text: "hi".into(),
            ..Default::default()
        });
        let v = r.params("acc", "cid");
        let keys: Vec<&String> = v.as_object().unwrap().keys().collect();
        assert_eq!(keys, ["account_id", "chat_id", "text", "client_id"]);
        let r = OutboxRequest::SendFile(MessagesSendFileParams {
            chat_id: "c1".into(),
            path: "/tmp/x.png".into(),
            as_file: Some(false),
            ..Default::default()
        });
        let v = r.params("acc", "cid");
        assert!(
            v.get("client_id").is_none(),
            "send_file has no client_id param"
        );
        assert_eq!(
            v["as_file"], false,
            "an explicit false is the caller's choice"
        );
        assert!(v.get("spoiler").is_none());
    }

    #[test]
    fn echo_shapes() {
        let r = OutboxRequest::SendFile(MessagesSendFileParams {
            chat_id: "c1".into(),
            path: "/tmp/photo.jpg".into(),
            caption: Some("cap".into()),
            kind: Some(FileKind::Photo),
            ..Default::default()
        });
        let e = r.echo("cid", None).unwrap();
        assert_eq!(e.text.as_deref(), Some("cap"));
        let m = e.media.unwrap();
        assert_eq!(m.kind, MediaKind::Photo);
        assert_eq!(m.filename.as_deref(), Some("photo.jpg"));
        assert!(
            OutboxRequest::Delete(MessagesDeleteParams {
                chat_id: "c".into(),
                message_ids: vec!["1".into()],
                ..Default::default()
            })
            .echo("cid", None)
            .is_none()
        );
    }
}
