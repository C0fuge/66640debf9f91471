//! Handles a plugin uses to talk to the core: [`Host`] (process-wide), [`Account`] (one started
//! account) and [`Call`] (one incoming request).

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use ch_protocol::dto::{
    AuthStep, Contact, Conversation, Draft, Folder, Message, Reaction, Reactor, Topic,
};
use ch_protocol::events::*;
use ch_protocol::methods::{
    AccountStartParams, AutoDownload, InitializeParams, SecretsDeleteParams, SecretsSetParams,
};
use ch_protocol::{
    AccountId, CancelParams, Capabilities, DEFAULT_TIMEOUT_SECONDS, Empty, Event, Id, Notification,
    Request, RequestId,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use tokio::sync::{Notify, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::error::{self, EmitError, PluginError};
use crate::settings::Settings;
use crate::transport::{Out, encode};

/// Bulk events sent as requests that may be awaiting the core's `{}` at once (spec §16).
pub const MAX_ACKED_IN_FLIGHT: usize = 4;

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

type Pending = oneshot::Sender<Result<Value, PluginError>>;

/// State shared by every handle of one plugin process.
pub(crate) struct Shared {
    out: mpsc::UnboundedSender<Out>,
    pub(crate) caps: Capabilities,
    pub(crate) accounts: Mutex<HashMap<AccountId, Arc<AccountInner>>>,
    pub(crate) pending: Mutex<HashMap<RequestId, Pending>>,
    pub(crate) inflight: Mutex<HashMap<RequestId, CancellationToken>>,
    next_id: AtomicI64,
    pub(crate) busy: AtomicBool,
    /// stdin is closed: the core can no longer answer plugin → core requests.
    gone: AtomicBool,
    acked: Semaphore,
    pub(crate) init: OnceLock<InitializeParams>,
    /// `settings_schema` defaults from `plugin_dir/plugin.json`, read at `initialize`.
    pub(crate) setting_defaults: OnceLock<Map<String, Value>>,
    pub(crate) root: CancellationToken,
}

impl Shared {
    pub(crate) fn new(out: mpsc::UnboundedSender<Out>, caps: Capabilities) -> Self {
        Self {
            out,
            caps,
            accounts: Mutex::default(),
            pending: Mutex::default(),
            inflight: Mutex::default(),
            next_id: AtomicI64::new(1),
            busy: AtomicBool::new(false),
            gone: AtomicBool::new(false),
            acked: Semaphore::new(MAX_ACKED_IN_FLIGHT),
            init: OnceLock::new(),
            setting_defaults: OnceLock::new(),
            root: CancellationToken::new(),
        }
    }

    pub(crate) fn send_frame(&self, frame: Vec<u8>) -> Result<(), EmitError> {
        self.out
            .send(Out::Frame(frame))
            .map_err(|_| EmitError::Closed)
    }

    pub(crate) fn send<T: Serialize>(&self, msg: &T) -> Result<(), EmitError> {
        self.send_frame(encode(msg)?)
    }

    /// Answer a core request. An oversized result is replaced by `INTERNAL`.
    pub(crate) fn respond(&self, id: RequestId, result: Result<Value, PluginError>) {
        let resp = match result {
            Ok(v) => ch_protocol::Response::ok(id.clone(), v),
            Err(e) => ch_protocol::Response::err(Some(id.clone()), error::normalize(e)),
        };
        let sent = match encode(&resp) {
            Ok(f) => self.send_frame(f),
            Err(e) => {
                tracing::error!("response to {id} too large: {e}");
                self.send(&ch_protocol::Response::<Value>::err(
                    Some(id),
                    error::normalize(error::internal(e.to_string())),
                ))
            }
        };
        if sent.is_err() {
            tracing::debug!("response dropped: transport closed");
        }
    }

    pub(crate) async fn close(&self) {
        let (tx, rx) = oneshot::channel();
        if self.out.send(Out::Close(tx)).is_ok() {
            let _ = rx.await;
        }
    }

    /// stdin hit EOF: fail every plugin → core request now (and later ones at once) with
    /// [`EmitError::Closed`], instead of letting them run into their timeout.
    pub(crate) fn peer_gone(&self) {
        self.gone.store(true, Ordering::SeqCst);
        lock(&self.pending).clear();
    }

    fn new_id(&self) -> RequestId {
        RequestId::Number(self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    fn account(&self, id: &str) -> Option<Arc<AccountInner>> {
        lock(&self.accounts).get(id).cloned()
    }

    fn emit(&self, ev: &Event, as_request: Option<RequestId>) -> Result<(), EmitError> {
        let acct = self
            .account(ev.account_id())
            .ok_or_else(|| EmitError::AccountNotStarted(ev.account_id().clone()))?;
        acct.emit(self, ev, as_request)
    }

    /// Send a request to the core and wait for its answer (or the timeout, then cancel it).
    async fn call(
        &self,
        frame: impl FnOnce(RequestId) -> Result<(), EmitError>,
        timeout: Duration,
    ) -> Result<Value, EmitError> {
        let id = self.new_id();
        let (tx, rx) = oneshot::channel();
        lock(&self.pending).insert(id.clone(), tx);
        if self.gone.load(Ordering::SeqCst) {
            lock(&self.pending).remove(&id);
            return Err(EmitError::Closed);
        }
        if let Err(e) = frame(id.clone()) {
            lock(&self.pending).remove(&id);
            return Err(e);
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(v))) => Ok(v),
            Ok(Ok(Err(e))) => Err(EmitError::Rejected(e)),
            Ok(Err(_)) => Err(EmitError::Closed),
            Err(_) => {
                lock(&self.pending).remove(&id);
                let _ = self.send(&CancelParams::notification(id));
                Err(EmitError::Rejected(error::timeout(
                    "the core did not answer in time",
                )))
            }
        }
    }

    pub(crate) fn complete(&self, id: &RequestId, result: Result<Value, PluginError>) {
        match lock(&self.pending).remove(id) {
            Some(tx) => {
                let _ = tx.send(result);
            }
            None => tracing::debug!("response for unknown request id {id}"),
        }
    }
}

/// Per-account state behind [`Account`].
pub(crate) struct AccountInner {
    pub(crate) params: AccountStartParams,
    settings: Settings,
    pub(crate) token: CancellationToken,
    gate: Mutex<Gate>,
    pub(crate) ghost: AtomicBool,
    data: Mutex<Option<Arc<dyn Any + Send + Sync>>>,
}

struct Gate {
    live: bool,
    state: Option<AccountState>,
    /// Frames emitted before the `account.start` response, flushed right after it (§4.2).
    /// `None` once the account is open.
    queue: Option<Vec<Vec<u8>>>,
}

impl AccountInner {
    pub(crate) fn new(
        params: AccountStartParams,
        defaults: &Map<String, Value>,
        parent: &CancellationToken,
    ) -> Self {
        Self {
            settings: Settings::new(crate::settings::with_defaults(
                params.settings.clone(),
                defaults,
            )),
            ghost: AtomicBool::new(params.ghost),
            params,
            token: parent.child_token(),
            gate: Mutex::new(Gate {
                live: true,
                state: None,
                queue: Some(Vec::new()),
            }),
            data: Mutex::new(None),
        }
    }

    fn emit(
        &self,
        shared: &Shared,
        ev: &Event,
        as_request: Option<RequestId>,
    ) -> Result<(), EmitError> {
        let params = ev.to_params();
        let frame = match as_request {
            None => encode(&Notification::new(ev.name(), params))?,
            Some(id) => encode(&Request::new(id, ev.name(), params))?,
        };
        // The gate is held while queueing, so nothing slips out after `close` (spec §4.2).
        let mut g = lock(&self.gate);
        if !g.live {
            return Err(EmitError::AccountNotStarted(self.params.account_id.clone()));
        }
        match &mut g.queue {
            Some(q) => q.push(frame),
            None => shared.send_frame(frame)?,
        }
        if let Event::Status(s) = ev {
            g.state = Some(s.state.clone());
        }
        Ok(())
    }

    /// After the `account.start` response: emit `status: connecting` unless the plugin has
    /// already reported a state, then the events queued while `on_account_start` ran, in order.
    pub(crate) fn open(&self, shared: &Shared) {
        let mut g = lock(&self.gate);
        let Some(queued) = g.queue.take() else { return };
        if !g.live {
            return;
        }
        if g.state.is_none() {
            let ev = Event::Status(status_event(
                &self.params.account_id,
                AccountState::Connecting,
                None,
                false,
                None,
            ));
            if let Ok(f) = encode(&Notification::new(ev.name(), ev.to_params())) {
                let _ = shared.send_frame(f);
                g.state = Some(AccountState::Connecting);
            }
        }
        for f in queued {
            let _ = shared.send_frame(f);
        }
    }

    /// Close the gate without a `status` (a failed `account.start`).
    pub(crate) fn close_silently(&self) {
        let mut g = lock(&self.gate);
        g.live = false;
        g.queue = None;
    }

    /// Emit `status: disconnected` and close the gate: no event for this account after this.
    pub(crate) fn close(&self, shared: &Shared) {
        let mut g = lock(&self.gate);
        g.queue = None;
        if g.live {
            let ev = Event::Status(status_event(
                &self.params.account_id,
                AccountState::Disconnected,
                None,
                false,
                None,
            ));
            if let Ok(f) = encode(&Notification::new(ev.name(), ev.to_params())) {
                let _ = shared.send_frame(f);
            }
            g.live = false;
            g.state = Some(AccountState::Disconnected);
        }
    }
}

fn status_event(
    id: &str,
    state: AccountState,
    detail: Option<String>,
    reconnecting: bool,
    me: Option<Contact>,
) -> StatusEvent {
    StatusEvent {
        account_id: id.to_owned(),
        state,
        detail,
        reconnecting,
        me,
    }
}

/// The process-wide handle to the core: emit events, call plugin → core methods, report busy.
///
/// Cheap to clone, `Send + Sync + 'static`: hand clones to background tasks. Events for an
/// account that is not started (or already stopped) are refused with
/// [`EmitError::AccountNotStarted`], which enforces spec §4.2. Prefer the typed helpers on
/// [`Account`], which fill in `account_id`.
#[derive(Clone)]
pub struct Host(pub(crate) Arc<Shared>);

impl Host {
    /// Send an event as a notification (spec §10).
    pub fn emit(&self, event: Event) -> Result<(), EmitError> {
        self.0.emit(&event, None)
    }

    /// Send an event as a *request* and wait until the core has ingested it (spec §12.2, §16).
    ///
    /// At most [`MAX_ACKED_IN_FLIGHT`] such requests are outstanding at once; further callers
    /// wait for a slot. Use this for bulk pushes during initial sync.
    pub async fn emit_acked(&self, event: Event) -> Result<(), EmitError> {
        let _slot = self
            .0
            .acked
            .acquire()
            .await
            .map_err(|_| EmitError::Closed)?;
        let shared = &self.0;
        shared
            .call(
                |id| shared.emit(&event, Some(id)),
                Duration::from_secs(DEFAULT_TIMEOUT_SECONDS.into()),
            )
            .await
            .map(drop)
    }

    /// Call a plugin → core method (spec §12) and decode its result. The timeout comes from
    /// [`ch_protocol::METHODS`] (default 60 s); on expiry the request is cancelled with
    /// `$/cancelRequest` and `TIMEOUT` is returned.
    pub async fn request<P: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        params: &P,
    ) -> Result<R, EmitError> {
        let timeout =
            ch_protocol::method(method).map_or(DEFAULT_TIMEOUT_SECONDS, |m| m.timeout_seconds);
        let shared = &self.0;
        let v = shared
            .call(
                |id| shared.send(&Request::new(id, method, params)),
                Duration::from_secs(timeout.into()),
            )
            .await?;
        serde_json::from_value(v)
            .map_err(|e| EmitError::Rejected(error::internal(format!("bad result from core: {e}"))))
    }

    /// Set the `busy` flag reported in `ping` results (spec §4.3).
    pub fn set_busy(&self, busy: bool) {
        self.0.busy.store(busy, Ordering::Relaxed);
    }

    /// The capabilities this plugin declared.
    pub fn capabilities(&self) -> &Capabilities {
        &self.0.caps
    }

    /// The core's `initialize` params (its API version, locale, log level, plugin dir).
    pub fn initialize_params(&self) -> Option<&InitializeParams> {
        self.0.init.get()
    }

    /// A started account by id.
    pub fn account(&self, account_id: &str) -> Option<Account> {
        self.0.account(account_id).map(|inner| Account {
            inner,
            host: self.clone(),
        })
    }

    /// Every started account.
    pub fn accounts(&self) -> Vec<Account> {
        lock(&self.0.accounts)
            .values()
            .map(|inner| Account {
                inner: inner.clone(),
                host: self.clone(),
            })
            .collect()
    }

    /// Cancelled when the plugin is shutting down or the core went away.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.0.root.clone()
    }
}

/// One started account: its `account.start` params, its settings and secrets, its
/// cancellation token, per-account plugin state, and typed emitters for every event.
///
/// Cheap to clone and usable from background tasks. Once `account.stop` has run, every emitter
/// returns [`EmitError::AccountNotStarted`] and [`Account::cancelled`] resolves, so a
/// background loop can simply `?` its emits or `select!` on the token.
#[derive(Clone)]
pub struct Account {
    pub(crate) inner: Arc<AccountInner>,
    pub(crate) host: Host,
}

impl std::fmt::Debug for Account {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Account")
            .field("id", &self.id())
            .finish_non_exhaustive()
    }
}

impl Account {
    /// The core-assigned, opaque account id (spec §5).
    pub fn id(&self) -> &AccountId {
        &self.inner.params.account_id
    }

    /// The process-wide handle.
    pub fn host(&self) -> &Host {
        &self.host
    }

    /// The full `account.start` params.
    pub fn start_params(&self) -> &AccountStartParams {
        &self.inner.params
    }

    /// Settings, with `x-secret` values merged in (spec §6.2). Never persist or log them.
    pub fn settings(&self) -> &Settings {
        &self.inner.settings
    }

    /// Runtime secrets previously stored with [`Account::set_secret`].
    pub fn secrets(&self) -> &BTreeMap<String, String> {
        &self.inner.params.secrets
    }

    /// One runtime secret. Reflects the value at `account.start`; a `set_secret` made since
    /// then is returned from the next start.
    pub fn secret(&self, key: &str) -> Option<&str> {
        self.inner.params.secrets.get(key).map(String::as_str)
    }

    /// Store a runtime secret in the core's keyring (`secrets.set`, spec §12.1).
    pub async fn set_secret(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), EmitError> {
        let p = SecretsSetParams {
            account_id: self.id().clone(),
            key: key.into(),
            value: value.into(),
        };
        self.host
            .request::<_, Empty>("secrets.set", &p)
            .await
            .map(drop)
    }

    /// Delete a runtime secret (`secrets.delete`, spec §12.1). Use on sign-out.
    pub async fn delete_secret(&self, key: impl Into<String>) -> Result<(), EmitError> {
        let p = SecretsDeleteParams {
            account_id: self.id().clone(),
            key: key.into(),
        };
        self.host
            .request::<_, Empty>("secrets.delete", &p)
            .await
            .map(drop)
    }

    /// Private, persistent directory; the core never reads or purges it.
    pub fn state_dir(&self) -> &Path {
        Path::new(&self.inner.params.state_dir)
    }

    /// Media cache directory; files there may vanish at any time (spec §15).
    pub fn cache_dir(&self) -> &Path {
        Path::new(&self.inner.params.cache_dir)
    }

    /// Ghost mode (spec §17): the `account.start` flag, updated by every `ghost.apply` call.
    pub fn ghost(&self) -> bool {
        self.inner.ghost.load(Ordering::Relaxed)
    }

    /// What may be fetched without being asked.
    pub fn auto_download(&self) -> &AutoDownload {
        &self.inner.params.auto_download
    }

    /// Cancelled when `account.stop` (or `shutdown`) begins. Child tokens of it are handed to
    /// every request handler for this account.
    pub fn token(&self) -> &CancellationToken {
        &self.inner.token
    }

    /// Resolves when the account is being stopped.
    pub async fn cancelled(&self) {
        self.inner.token.cancelled().await;
    }

    /// Has `account.stop` begun?
    pub fn is_stopped(&self) -> bool {
        self.inner.token.is_cancelled()
    }

    /// The last state reported through `status`, if any.
    pub fn state(&self) -> Option<AccountState> {
        lock(&self.inner.gate).state.clone()
    }

    /// `Ok` when the last status is `connected`; otherwise the universal error for the state:
    /// `AUTH_REQUIRED` for `auth_required`, `UNAVAILABLE` for everything else.
    pub fn ensure_connected(&self) -> Result<(), PluginError> {
        match self.state() {
            Some(AccountState::Connected) => Ok(()),
            Some(AccountState::AuthRequired) => Err(error::auth_required()),
            Some(s) => Err(error::unavailable(format!("account is {s}"))),
            None => Err(error::unavailable("account is starting")),
        }
    }

    /// Attach plugin state to this account (a client, a session). Replaces earlier state.
    pub fn set_data<T: Any + Send + Sync>(&self, data: T) -> Arc<T> {
        let arc = Arc::new(data);
        *lock(&self.inner.data) = Some(arc.clone());
        arc
    }

    /// The state attached with [`Account::set_data`], if it has type `T`.
    pub fn data<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        lock(&self.inner.data)
            .clone()
            .and_then(|a| a.downcast::<T>().ok())
    }

    /// Emit any event as a notification. Its `account_id` should be this account's.
    pub fn emit(&self, event: Event) -> Result<(), EmitError> {
        self.inner.emit(&self.host.0, &event, None)
    }

    /// Emit any event as a request and wait for the core's `{}` (spec §16).
    pub async fn emit_acked(&self, event: Event) -> Result<(), EmitError> {
        self.host.emit_acked(event).await
    }

    fn aid(&self) -> AccountId {
        self.id().clone()
    }

    /// `status` with just a state (spec §4.4).
    pub fn status(&self, state: AccountState) -> Result<(), EmitError> {
        self.status_with(state, None, false, None)
    }

    /// `status` with every field: `detail` (for `error`), `reconnecting` (on `connecting` or
    /// `unavailable`), and `me` once known.
    pub fn status_with(
        &self,
        state: AccountState,
        detail: Option<String>,
        reconnecting: bool,
        me: Option<Contact>,
    ) -> Result<(), EmitError> {
        self.emit(Event::Status(status_event(
            self.id(),
            state,
            detail,
            reconnecting,
            me,
        )))
    }

    /// `status: connected`, with `me` when known.
    pub fn connected(&self, me: Option<Contact>) -> Result<(), EmitError> {
        self.status_with(AccountState::Connected, None, false, me)
    }

    /// `conversations.upsert`.
    pub fn conversations_upsert(&self, conversations: Vec<Conversation>) -> Result<(), EmitError> {
        self.emit(Event::ConversationsUpsert(ConversationsUpsertEvent {
            account_id: self.aid(),
            conversations,
        }))
    }

    /// `conversations.synced`: every chat the account has, after a complete sync.
    pub fn conversations_synced(&self, chat_ids: Vec<Id>) -> Result<(), EmitError> {
        self.emit(Event::ConversationsSynced(ConversationsSyncedEvent {
            account_id: self.aid(),
            chat_ids,
        }))
    }

    /// `messages.upsert`.
    pub fn messages_upsert(&self, messages: Vec<Message>) -> Result<(), EmitError> {
        self.emit(Event::MessagesUpsert(MessagesUpsertEvent {
            account_id: self.aid(),
            messages,
        }))
    }

    /// `messages.deleted`; `chat_id` is `None` when the platform does not say.
    pub fn messages_deleted(
        &self,
        chat_id: Option<Id>,
        message_ids: Vec<Id>,
    ) -> Result<(), EmitError> {
        self.emit(Event::MessagesDeleted(MessagesDeletedEvent {
            account_id: self.aid(),
            chat_id,
            message_ids,
        }))
    }

    /// `message.edited`.
    pub fn message_edited(&self, message: Message) -> Result<(), EmitError> {
        self.emit(Event::MessageEdited(MessageEditedEvent {
            account_id: self.aid(),
            message,
        }))
    }

    /// `reaction.changed`: the full current reaction list of a message.
    pub fn reaction_changed(
        &self,
        chat_id: Id,
        message_id: Id,
        reactions: Vec<Reaction>,
        reactors: Option<Vec<Reactor>>,
    ) -> Result<(), EmitError> {
        self.emit(Event::ReactionChanged(ReactionChangedEvent {
            account_id: self.aid(),
            chat_id,
            message_id,
            reactions,
            reactors,
        }))
    }

    /// `contacts.upsert`.
    pub fn contacts_upsert(&self, contacts: Vec<Contact>) -> Result<(), EmitError> {
        self.emit(Event::ContactsUpsert(ContactsUpsertEvent {
            account_id: self.aid(),
            contacts,
        }))
    }

    /// `folders.synced`: the whole folder set.
    pub fn folders_synced(&self, folders: Vec<Folder>) -> Result<(), EmitError> {
        self.emit(Event::FoldersSynced(FoldersSyncedEvent {
            account_id: self.aid(),
            folders,
        }))
    }

    /// `topics.upsert`; `complete` means these are all the chat's topics.
    pub fn topics_upsert(
        &self,
        chat_id: Id,
        topics: Vec<Topic>,
        complete: bool,
    ) -> Result<(), EmitError> {
        self.emit(Event::TopicsUpsert(TopicsUpsertEvent {
            account_id: self.aid(),
            chat_id,
            topics,
            complete,
        }))
    }

    /// `read.outbox`: the peer read up to `max_message_id`.
    pub fn read_outbox(
        &self,
        chat_id: Id,
        topic_id: Option<Id>,
        max_message_id: Id,
    ) -> Result<(), EmitError> {
        self.emit(Event::ReadOutbox(ReadOutboxEvent {
            account_id: self.aid(),
            chat_id,
            topic_id,
            max_message_id,
        }))
    }

    /// `read.inbox`: this account read up to `max_message_id` elsewhere.
    pub fn read_inbox(
        &self,
        chat_id: Id,
        topic_id: Option<Id>,
        max_message_id: Id,
        unread_count: Option<u64>,
    ) -> Result<(), EmitError> {
        self.emit(Event::ReadInbox(ReadInboxEvent {
            account_id: self.aid(),
            chat_id,
            topic_id,
            max_message_id,
            unread_count,
        }))
    }

    /// `presence`. `account_id` in `event` is overwritten with this account's. Coalesce to one
    /// per user per 500 ms (spec §16).
    pub fn presence(&self, mut event: PresenceEvent) -> Result<(), EmitError> {
        event.account_id = self.aid();
        self.emit(Event::Presence(event))
    }

    /// `draft.changed`; an empty `draft.text` means cleared.
    pub fn draft_changed(
        &self,
        chat_id: Id,
        topic_id: Option<Id>,
        draft: Draft,
    ) -> Result<(), EmitError> {
        self.emit(Event::DraftChanged(DraftChangedEvent {
            account_id: self.aid(),
            chat_id,
            topic_id,
            draft,
        }))
    }

    /// `avatar.ready`: `path` must be inside `cache_dir` or `state_dir` (spec §15).
    pub fn avatar_ready(
        &self,
        kind: AvatarKind,
        id: Id,
        path: impl Into<String>,
    ) -> Result<(), EmitError> {
        self.emit(Event::AvatarReady(AvatarReadyEvent {
            account_id: self.aid(),
            kind,
            id,
            path: path.into(),
        }))
    }

    /// `media.ready`: a thumbnail and/or full file of a message is on disk.
    pub fn media_ready(
        &self,
        chat_id: Id,
        message_id: Id,
        thumb_path: Option<String>,
        path: Option<String>,
    ) -> Result<(), EmitError> {
        self.emit(Event::MediaReady(MediaReadyEvent {
            account_id: self.aid(),
            chat_id,
            message_id,
            thumb_path,
            path,
        }))
    }

    /// `transfer.progress` not tied to a request (an auto-download). For a request's own
    /// transfer use [`Call::progress`], which also resets the request's timeout.
    pub fn transfer_progress(&self, mut event: TransferProgressEvent) -> Result<(), EmitError> {
        event.account_id = self.aid();
        self.emit(Event::TransferProgress(event))
    }

    /// `auth.step` (spec §7). [`crate::auth::AuthFlow`] manages step ids for you.
    pub fn auth_step(&self, step: AuthStep) -> Result<(), EmitError> {
        self.emit(Event::AuthStep(AuthStepEvent {
            account_id: self.aid(),
            step,
        }))
    }
}

/// One incoming core → plugin request, handed to its handler.
///
/// Carries the decoded params, the request id, the account it addresses (when it is
/// account-scoped), and a cancellation token that fires on `$/cancelRequest`, on the
/// method's timeout (from [`ch_protocol::METHODS`]), on `account.stop` and on shutdown.
pub struct Call<P> {
    /// The decoded params.
    pub params: P,
    pub(crate) id: RequestId,
    pub(crate) method: String,
    pub(crate) host: Host,
    pub(crate) account: Option<Account>,
    pub(crate) token: CancellationToken,
    pub(crate) reset: Arc<Notify>,
}

impl<P> Call<P> {
    pub(crate) fn try_map<Q, E>(self, f: impl FnOnce(P) -> Result<Q, E>) -> Result<Call<Q>, E> {
        Ok(Call {
            params: f(self.params)?,
            id: self.id,
            method: self.method,
            host: self.host,
            account: self.account,
            token: self.token,
            reset: self.reset,
        })
    }

    /// The core's id for this request.
    pub fn id(&self) -> &RequestId {
        &self.id
    }

    /// The method name.
    pub fn method(&self) -> &str {
        &self.method
    }

    /// The process-wide handle.
    pub fn host(&self) -> &Host {
        &self.host
    }

    /// The account this request addresses. The SDK has already answered `UNKNOWN_ACCOUNT` for
    /// accounts that are not started, so this only fails (`INVALID_PARAMS`) for a vendor
    /// method that carries no `account_id`.
    pub fn account(&self) -> Result<&Account, PluginError> {
        self.account
            .as_ref()
            .ok_or_else(|| error::invalid_params("account_id", "this method needs an account_id"))
    }

    /// The request's cancellation token.
    pub fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// Has the request been cancelled?
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Resolves when the request is cancelled. `select!` on it around long platform calls.
    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }

    /// `Err(CANCELLED)` once cancelled, for `?` between steps of a long operation.
    pub fn check_cancelled(&self) -> Result<(), PluginError> {
        if self.is_cancelled() {
            Err(error::cancelled())
        } else {
            Ok(())
        }
    }

    /// `Err(NOT_SUPPORTED)` unless `capability` was declared.
    ///
    /// The parameter gates of the registry (`gated_params`, spec §14.2: `schedule_at` needs
    /// `send.schedule`, …) are already enforced before the handler runs, so this is only for
    /// plugin-specific conditions the registry does not list.
    pub fn require(&self, capability: &str) -> Result<(), PluginError> {
        if self.host.capabilities().has(capability) {
            Ok(())
        } else {
            Err(error::not_supported(capability))
        }
    }

    /// Emit `transfer.progress` for this request (spec §15: at least every 500 ms while bytes
    /// move) and reset the request's timeout.
    pub fn progress(
        &self,
        direction: TransferDirection,
        done_bytes: u64,
        total_bytes: Option<u64>,
    ) -> Result<(), EmitError> {
        self.progress_event(TransferProgressEvent {
            account_id: String::new(),
            request_id: None,
            direction,
            chat_id: None,
            message_id: None,
            done_bytes,
            total_bytes,
        })
    }

    /// Like [`Call::progress`] with every field; `account_id` and `request_id` are filled in.
    pub fn progress_event(&self, mut event: TransferProgressEvent) -> Result<(), EmitError> {
        let account = self
            .account
            .as_ref()
            .ok_or(EmitError::AccountNotStarted(String::new()))?;
        event.request_id = Some(self.id.clone());
        self.reset.notify_one();
        account.transfer_progress(event)
    }
}
