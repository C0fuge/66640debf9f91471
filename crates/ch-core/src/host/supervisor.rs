//! One task per plugin: spawn, `initialize`, ping, outgoing requests with timeouts and
//! cancellation, incoming events and requests, crash detection, restart with backoff and the
//! crash-loop cap, graceful `shutdown` (spec §4, §12, §16).
//!
//! Structure while a process is up:
//! - the **supervisor loop** (this task) serves commands from the [`super::HostHandle`],
//!   sends pings, sweeps request deadlines and watches the child;
//! - a **reader task** parses stdout frames: it resolves pending requests (and applies the
//!   account transitions that ride on lifecycle replies, in stream order), answers plugin
//!   requests, and pushes events into a bounded queue — stalling on stdout when it is full;
//! - a **writer task** owns stdin;
//! - a **delivery task** (one per plugin, outliving restarts so order is kept) feeds the
//!   [`EventSink`] and acknowledges events that were sent as requests;
//! - a **stderr task** forwards the plugin's log lines.

use std::collections::{BTreeMap, HashMap};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ch_protocol::events::{AccountState, Event};
use ch_protocol::jsonrpc::{CANCEL_METHOD, CancelParams, Envelope, Request, RequestId, Response};
use ch_protocol::methods::{AccountStartParams, InitializeParams, InitializeResult};
use ch_protocol::{Empty, ErrorCode, ErrorObject};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

use super::config::HostConfig;
use super::error::HostError;
use super::settings::fill_defaults;
use super::sink::{EventSink, SecretStore};
use super::state::{AccountInfo, AccountPhase, AccountSpec, PluginInfo, PluginState, StateChange};
use super::transport::{Frame, FrameError, FrameReader, encode};
use super::start_scope::StartScope;
use super::removal::{Decision, Evidence, RemovalStop, Request as LifecycleRequest};
#[cfg(target_os = "linux")]
use super::{OwnedAccountSnapshot, OwnedStateChange, OwnedEventSink};
#[cfg(target_os = "linux")]
use crate::runtime::LogicalOwnerObservation;

// ---------------------------------------------------------------------------------------
// Host-wide registry (shared by the handle, the supervisors and their readers)
// ---------------------------------------------------------------------------------------

#[cfg_attr(test, derive(Clone))]
pub(crate) struct AccountRecord {
    pub info: AccountInfo,
    pub spec: AccountSpec,
    start: Option<StartScope>,
    removal: Evidence,
    // One interval of successful account.start on this process. Status events
    // and duplicate starts preserve it; callers only receive child tokens.
    run: Option<CancellationToken>,
}

impl AccountRecord {
    #[cfg(target_os = "linux")]
    pub(super) fn logical_origin(&self) -> Option<&LogicalOwnerObservation> { self.start.as_ref()?.origin() }
    #[cfg(target_os = "linux")]
    pub(super) fn owned_snapshot(&self) -> Option<OwnedAccountSnapshot> {
        let origin = self.logical_origin()?;
        (!origin.is_revoked()).then(|| OwnedAccountSnapshot::new(self.info.clone(), origin.clone()))
    }
    #[cfg(test)]
    pub(super) fn same_stopped_record(&self, other: &Self) -> bool {
        self.info == other.info && self.info.phase == AccountPhase::Stopped
            && self.removal.same(&other.removal)
            && self.run.is_none() && other.run.is_none()
            && self.spec.settings == other.spec.settings && self.spec.ghost == other.spec.ghost
            && serde_json::to_value(&self.spec.auto_download).unwrap() == serde_json::to_value(&other.spec.auto_download).unwrap()
            && match (&self.start, &other.start) {
                (None, None) => true,
                (Some(a), Some(b)) => a.same(b),
                _ => false,
            }
    }
    fn revoke_run(&mut self) {
        if let Some(run) = self.run.take() {
            run.cancel();
        }
    }
}

pub(crate) struct PluginEntry {
    pub info: PluginInfo,
    pub state: PluginState,
    pub accounts: BTreeMap<String, AccountRecord>,
    /// Present while a supervisor task runs for this plugin.
    pub cmd: Option<mpsc::Sender<Command>>,
}

pub(crate) struct Registry {
    pub plugins: Mutex<BTreeMap<String, PluginEntry>>,
    pub changes: broadcast::Sender<StateChange>,
    #[cfg(target_os = "linux")]
    pub(super) owned_changes: broadcast::Sender<OwnedStateChange>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            plugins: Mutex::new(BTreeMap::new()),
            changes: broadcast::channel(1024).0,
            #[cfg(target_os = "linux")]
            owned_changes: broadcast::channel(1024).0,
        }
    }

    pub fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, PluginEntry>> {
        self.plugins
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn publish_other(&self, change: StateChange) {
        #[cfg(target_os = "linux")]
        if !matches!(change, StateChange::Account(_)) {
            let _ = self.owned_changes.send(OwnedStateChange::Other(Box::new(change.clone())));
        }
        let _ = self.changes.send(change);
    }

    #[cfg(target_os = "linux")]
    fn publish_account(&self, info: AccountInfo, origin: Option<LogicalOwnerObservation>) {
        if let Some(origin) = origin.filter(|origin| !origin.is_revoked()) {
            let _ = self.owned_changes.send(OwnedStateChange::Account(OwnedAccountSnapshot::new(info.clone(), origin)));
        }
        let _ = self.changes.send(StateChange::Account(info));
    }

    pub fn set_plugin_state(&self, plugin_id: &str, state: PluginState) {
        {
            let mut g = self.lock();
            let Some(e) = g.get_mut(plugin_id) else {
                return;
            };
            if e.state == state {
                return;
            }
            if !state.is_running() {
                for record in e.accounts.values_mut() {
                    record.revoke_run();
                }
            }
            e.state = state.clone();
        }
        self.publish_other(StateChange::Plugin {
            plugin_id: plugin_id.to_owned(),
            state,
        });
    }

    pub fn plugin_state(&self, plugin_id: &str) -> Option<PluginState> {
        self.lock().get(plugin_id).map(|e| e.state.clone())
    }

    pub fn running_account_token(
        &self,
        plugin_id: &str,
        account_id: &str,
    ) -> Option<CancellationToken> {
        let g = self.lock();
        let entry = g.get(plugin_id)?;
        if !entry.state.is_running() {
            return None;
        }
        let record = entry.accounts.get(account_id)?;
        if !record.info.wanted || record.info.phase != AccountPhase::Running {
            return None;
        }
        record.run.as_ref().map(CancellationToken::child_token)
    }

    pub fn revoke_account_run(&self, plugin_id: &str, account_id: &str) {
        if let Some(record) = self
            .lock()
            .get_mut(plugin_id)
            .and_then(|e| e.accounts.get_mut(account_id))
        {
            record.revoke_run();
        }
    }

    /// Mutate (creating if needed) one account record and broadcast the result.
    pub fn update_account(
        &self,
        plugin_id: &str,
        account_id: &str,
        f: impl FnOnce(&mut AccountRecord),
    ) -> Option<AccountInfo> {
        #[cfg(target_os = "linux")]
        let origin;
        let info = {
            let mut g = self.lock();
            let e = g.get_mut(plugin_id)?;
            let rec = e
                .accounts
                .entry(account_id.to_owned())
                .or_insert_with(|| AccountRecord {
                    info: AccountInfo::new(plugin_id, account_id),
                    spec: AccountSpec::default(),
                    run: None,
                    start: None,
                    removal: Evidence::default(),
                });
            let before = rec.info.clone();
            #[cfg(target_os = "linux")]
            { origin = rec.logical_origin().cloned(); }
            f(rec);
            if !rec.info.wanted || rec.info.phase != AccountPhase::Running {
                rec.revoke_run();
            } else if e.state.is_running()
                && (!before.wanted || before.phase != AccountPhase::Running)
            {
                rec.run = Some(CancellationToken::new());
            }
            if rec.info == before {
                return Some(before);
            }
            rec.info.clone()
        };
        #[cfg(target_os = "linux")]
        self.publish_account(info.clone(), origin);
        #[cfg(not(target_os = "linux"))]
        let _ = self.changes.send(StateChange::Account(info.clone()));
        Some(info)
    }

    #[cfg(target_os = "linux")]
    fn update_from_origin(&self, plugin: &str, account: &str, origin: &LogicalOwnerObservation,
        change: impl FnOnce(&mut AccountRecord)) -> Option<AccountInfo> {
        let info = {
            let mut state = self.lock(); let entry = state.get_mut(plugin)?;
            let running = entry.state.is_running(); let record = entry.accounts.get_mut(account)?;
            if origin.is_revoked() || origin.plugin_id() != plugin || origin.account_id() != account
                || !record.logical_origin().is_some_and(|current| current.same(origin)) { return None; }
            change(record);
            if !record.info.wanted || record.info.phase != AccountPhase::Running { record.revoke_run(); }
            else if running && record.run.is_none() { record.run = Some(CancellationToken::new()); }
            record.info.clone()
        };
        self.publish_account(info.clone(), Some(origin.clone()));
        Some(info)
    }

    fn update_pending(&self, conn: &Conn, account: &str, pending: &PendingOwner,
        change: impl FnOnce(&mut AccountRecord)) -> Option<AccountInfo> {
        #[cfg(target_os = "linux")]
        if conn.origins.is_some() {
            return self.update_from_origin(&conn.plugin_id, account, pending.origin.as_ref()?, change);
        }
        let _ = pending;
        self.update_account(&conn.plugin_id, account, change)
    }

    pub fn account_phase(&self, plugin_id: &str, account_id: &str) -> Option<AccountPhase> {
        self.lock()
            .get(plugin_id)?
            .accounts
            .get(account_id)
            .map(|r| r.info.phase)
    }

    /// Every account on the plugin: `(id, wanted, phase)`.
    fn accounts_of(&self, plugin_id: &str) -> Vec<(String, bool, AccountPhase)> {
        self.lock()
            .get(plugin_id)
            .map(|e| {
                e.accounts
                    .values()
                    .map(|r| (r.info.account_id.clone(), r.info.wanted, r.info.phase))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(super) fn account_start(&self, plugin_id: &str, account_id: &str) -> Option<(AccountSpec, StartScope)> {
        let mut state = self.lock();
        let record = state.get_mut(plugin_id)?
            .accounts
            .get_mut(account_id)?;
        let scope = record.start.as_ref()?.replay();
        record.start = Some(scope.clone());
        Some((record.spec.clone(), scope))
    }
    fn account_scope(&self, plugin: &str, account: &str) -> Option<StartScope> {
        self.lock().get(plugin)?.accounts.get(account)?.start.clone()
    }

    fn set_start(&self, plugin: &str, account: &str, spec: AccountSpec, scope: &StartScope) -> bool {
        let mut state = self.lock(); let Some(entry) = state.get_mut(plugin) else { return false; };
        // The original-owner veto is checked inside this reservation, BEFORE
        // even creating a local record. No unscoped helper can resurrect it.
        if !scope.live() { return false; }
        let record = entry.accounts.entry(account.to_owned()).or_insert_with(|| AccountRecord {
            info: AccountInfo::new(plugin, account), spec: AccountSpec::default(), run: None, start: None, removal: Evidence::default(),
        });
        if record.info.phase == AccountPhase::Stopped
            && !record.start.as_ref().is_some_and(|old| old.same_owner(scope)) {
            record.info = AccountInfo::new(plugin, account);
        }
        record.start = Some(scope.clone()); record.spec = spec;
        // Metadata-only reservation never broadcasts an event. All lifecycle
        // adoption uses update_start's original-scope + nonce check instead.
        true
    }

    // Stale failures may explain the old local wish, but must never publish an
    // Account event that could be persisted onto a replacement DB owner.
    fn update_start(&self, plugin: &str, account: &str, scope: &StartScope, require_live: bool,
        change: impl FnOnce(&mut AccountRecord)) -> Option<AccountInfo> {
        let (info, publish) = {
            let mut state = self.lock(); let entry = state.get_mut(plugin)?;
            let running = entry.state.is_running(); let record = entry.accounts.get_mut(account)?;
            if !record.start.as_ref().is_some_and(|current| current.same(scope)) { return None; }
            let live = scope.metadata_live();
            if require_live && !scope.live() { return None; }
            change(record);
            if !record.info.wanted || record.info.phase != AccountPhase::Running { record.revoke_run(); }
            else if running && record.run.is_none() { record.run = Some(CancellationToken::new()); }
            (record.info.clone(), live)
        };
        if publish && scope.metadata_live() {
            #[cfg(target_os = "linux")]
            self.publish_account(info.clone(), scope.origin().cloned());
            #[cfg(not(target_os = "linux"))]
            let _ = self.changes.send(StateChange::Account(info.clone()));
        }
        Some(info)
    }

    fn update_start_metadata(&self, plugin: &str, account: &str, scope: &StartScope,
        change: impl FnOnce(&mut AccountRecord)) -> Option<AccountInfo> {
        if !scope.metadata_live() { return None; }
        self.update_start(plugin, account, scope, false, change)
    }

    fn current_start(&self, plugin: &str, account: &str, scope: &StartScope) -> bool {
        scope.live() && self.lock().get(plugin).and_then(|entry| entry.accounts.get(account))
            .is_some_and(|record| record.start.as_ref().is_some_and(|current| current.same(scope)))
    }

    // This latch is independent of live-scope event adoption and local phase.
    fn begin_lifecycle(&self, plugin: &str, account: &str, request: LifecycleRequest, start: bool) -> bool {
        let mut state = self.lock();
        let Some(record) = state.get_mut(plugin).and_then(|entry| entry.accounts.get_mut(account)) else { return false; };
        if start { record.removal.start(request); } else { record.removal.stop(request); }
        true
    }

    fn finish_lifecycle(&self, plugin: &str, account: &str, request: &LifecycleRequest, success: bool) {
        let mut state = self.lock();
        if let Some(record) = state.get_mut(plugin).and_then(|entry| entry.accounts.get_mut(account)) {
            record.removal.finish(request, success);
        }
    }

    pub(super) fn removal_decision(&self, plugin: &str, account: &str, connection: Option<&Arc<()>>) -> Decision {
        let state = self.lock();
        let Some(record) = state.get(plugin).and_then(|entry| entry.accounts.get(account)) else { return Decision::Local; };
        match record.removal.decision(connection) {
            Decision::Local if matches!(record.info.phase, AccountPhase::Starting | AccountPhase::Stopping | AccountPhase::Running) => Decision::Refuse,
            decision => decision,
        }
    }

    fn can_stop_accepted_start(&self, plugin: &str, account: &str, connection: &Arc<()>) -> bool {
        self.lock().get(plugin).and_then(|entry| entry.accounts.get(account))
            .is_some_and(|record| record.removal.can_stop_current_peer(connection))
    }

    pub(super) fn forget_unexposed_wish(&self, plugin: &str, account: &str) {
        let mut state = self.lock();
        if let Some(record) = state.get_mut(plugin).and_then(|entry| entry.accounts.get_mut(account)) {
            record.info.wanted = false;
            record.revoke_run();
        }
    }

    fn shutdown_confirmed(&self, plugin: &str, connection: &Arc<()>) {
        let mut state = self.lock();
        if let Some(entry) = state.get_mut(plugin) {
            for record in entry.accounts.values_mut() {
                record.removal.shutdown_confirmed(connection);
            }
        }
    }

    fn refuse_unexposed_start(&self, plugin: &str, account: &str, scope: &StartScope) {
        self.update_start(plugin, account, scope, false, |record| {
            record.info.phase = AccountPhase::Stopped;
            record.info.state = AccountState::Error;
            record.info.detail = Some("Account start was cancelled; start the account again".into());
        });
    }
}

// ---------------------------------------------------------------------------------------
// Commands from the handle
// ---------------------------------------------------------------------------------------

pub(crate) enum Command {
    StartAccount {
        account_id: String,
        spec: AccountSpec,
        scope: StartScope,
        reply: oneshot::Sender<Result<(), HostError>>,
    },
    StopAccount {
        account_id: String,
        reply: oneshot::Sender<Result<(), HostError>>,
    },
    StopAccountForRemoval {
        account_id: String,
        queued: super::removal::Command,
        reply: oneshot::Sender<RemovalStop>,
    },
    Call {
        request_id: Option<Arc<std::sync::OnceLock<i64>>>,
        method: String,
        params: Value,
        cancel: Option<CancellationToken>,
        reply: oneshot::Sender<Result<Value, HostError>>,
    },
    /// Graceful stop; the supervisor task ends afterwards.
    Shutdown { reply: oneshot::Sender<()> },
    /// From `Failed`/`Incompatible`/`Restarting`: try again now with a fresh failure count.
    /// From `Running`: graceful stop, then start again.
    Restart { reply: oneshot::Sender<()> },
}

// ---------------------------------------------------------------------------------------
// Connection state shared by the supervisor loop and the reader task
// ---------------------------------------------------------------------------------------

enum PendingKind {
    Initialize,
    Ping,
    Shutdown,
    AccountStart(String, StartScope),
    AccountStop(String),
    RemovalStop(String),
    Call,
}

#[derive(Default)]
struct PendingOwner {
    #[cfg(target_os = "linux")]
    origin: Option<Arc<LogicalOwnerObservation>>,
}
struct Pending {
    owner: PendingOwner,
    kind: PendingKind,
    method: &'static str,
    deadline: Instant,
    timeout: Duration,
    reply: oneshot::Sender<Result<Value, ErrorObject>>,
    /// Dropped with the entry; a cancel watcher ends when it sees the drop.
    _done: Option<oneshot::Sender<()>>,
}

enum Out {
    Frame(Vec<u8>),
    /// Close stdin and end the writer.
    Close,
}

// Delayed transfer events can survive a plugin restart in the delivery queue.
static NEXT_REQUEST_ID: AtomicI64 = AtomicI64::new(1);

struct Conn {
    #[cfg(target_os = "linux")]
    origins: Option<super::origins::Origins>,
    plugin_id: String,
    identity: Arc<()>,
    registry: std::sync::Weak<Registry>,
    pending: Mutex<HashMap<i64, Pending>>,
    writer: mpsc::Sender<Out>,
    /// Pings missed in a row (reset by every pong).
    ping_misses: AtomicU32,
    /// The reader is blocked on the full event queue; pings are not counted meanwhile.
    stalled: AtomicBool,
    /// Set once the handshake succeeded; events before it are a protocol slip, logged.
    initialized: AtomicBool,
}

impl Conn {
    fn pending(&self) -> std::sync::MutexGuard<'_, HashMap<i64, Pending>> {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Queue a frame. `false` when the writer is gone or its queue is full (the plugin has
    /// stopped reading stdin, §2.4).
    fn send_frame(&self, frame: Vec<u8>) -> bool {
        match self.writer.try_send(Out::Frame(frame)) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(plugin = %self.plugin_id, "write queue full: plugin is not reading stdin");
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    fn send_json<T: serde::Serialize>(&self, msg: &T) -> bool {
        match encode(msg) {
            Ok(f) => self.send_frame(f),
            Err(e) => {
                error!(plugin = %self.plugin_id, "cannot encode frame: {e}");
                false
            }
        }
    }

    /// Answer a plugin request. Notifications (`id` = `None`) get nothing (§2.3); use
    /// [`Conn::reject_frame`] for frames whose id could not be read.
    fn respond(&self, id: Option<RequestId>, result: Result<Value, ErrorObject>) {
        let Some(id) = id else { return };
        let resp = match result {
            Ok(v) => Response::ok(id, v),
            Err(e) => Response::err(id, e),
        };
        self.send_json(&resp);
    }

    /// `INVALID_REQUEST` for a frame that is not a JSON-RPC message, with `id: null` when
    /// no id could be read (§2.2).
    fn reject_frame(&self, id: Option<RequestId>, reason: String) {
        self.send_json(&Response::<Value>::err(
            id,
            ErrorObject::new(ErrorCode::InvalidRequest, reason),
        ));
    }

    fn send_cancel(&self, id: i64) {
        self.send_json(&CancelParams::notification(id));
    }

    /// Register and send a request. Returns the id and the reply receiver, or `None` when
    /// the frame could not be queued (the caller treats the connection as dead).
    fn request(
        &self,
        kind: PendingKind,
        method: &'static str,
        params: Value,
        timeout: Duration,
        cancel: Option<CancellationToken>,
        conn: &Arc<Conn>,
    ) -> Option<(i64, oneshot::Receiver<Result<Value, ErrorObject>>)> {
        let id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel::<()>();
        let frame = encode(&Request::new(id, method, params)).ok()?;
        let lifecycle = match &kind {
            PendingKind::AccountStart(account, _) => Some((account.clone(), true)),
            PendingKind::AccountStop(account) | PendingKind::RemovalStop(account) => Some((account.clone(), false)),
            _ => None,
        };
        #[cfg(target_os = "linux")]
        let origin_reservation = if let Some(origins) = &self.origins {
            match &kind {
                PendingKind::AccountStart(account, scope) => Some(origins.start(account, scope.origin()?)?),
                PendingKind::AccountStop(account) | PendingKind::RemovalStop(account) =>
                    Some(origins.stop(account, LifecycleRequest::new(&self.identity, id))?),
                _ => None,
            }
        } else { None };
        let registry = if let Some((account, start)) = &lifecycle {
            let registry = self.registry.upgrade()?;
            if !registry.begin_lifecycle(&self.plugin_id, account, LifecycleRequest::new(&self.identity, id), *start) { return None; }
            Some(registry)
        } else { None };
        self.pending().insert(
            id,
            Pending {
                owner: PendingOwner { #[cfg(target_os = "linux")]
                    origin: origin_reservation.as_ref().map(|reservation| Arc::new(reservation.origin().clone())) },
                kind,
                method,
                deadline: Instant::now() + timeout,
                timeout,
                reply: tx,
                _done: Some(done_tx),
            },
        );
        if !self.send_frame(frame) {
            self.pending().remove(&id);
            if let (Some(registry), Some((account, _))) = (&registry, &lifecycle) {
                registry.finish_lifecycle(&self.plugin_id, account, &LifecycleRequest::new(&self.identity, id), false);
            }
            return None;
        }
        #[cfg(target_os = "linux")]
        if let Some(reservation) = origin_reservation { reservation.commit(); }
        if let Some(token) = cancel {
            let conn = Arc::clone(conn);
            tokio::spawn(async move {
                tokio::select! {
                    _ = token.cancelled() => {
                        if conn.pending().contains_key(&id) {
                            debug!(plugin = %conn.plugin_id, id, "caller cancelled; sending $/cancelRequest");
                            conn.send_cancel(id);
                        }
                    }
                    _ = done_rx => {}
                }
            });
        }
        Some((id, rx))
    }

    /// Fail every pending request (process died).
    fn fail_all(&self, registry: &Registry, reason: &str) {
        let drained: Vec<(i64, Pending)> = self.pending().drain().collect();
        for (id, p) in drained {
            fail_lifecycle(self, registry, id, &p.kind);
            match &p.kind {
                PendingKind::AccountStart(acc, scope) => {
                    registry.update_start_metadata(&self.plugin_id, acc, scope, |r| {
                        r.info.phase = AccountPhase::Stopped;
                        r.info.state = AccountState::Disconnected;
                        r.info.detail = Some(reason.to_owned());
                    });
                }
                PendingKind::AccountStop(acc) | PendingKind::RemovalStop(acc) => {
                    registry.update_pending(self, acc, &p.owner, |r| {
                        r.info.phase = AccountPhase::Stopped;
                        r.info.state = AccountState::Disconnected;
                        r.info.wanted = false;
                    });
                }
                _ => {}
            }
            let _ = p
                .reply
                .send(Err(ErrorObject::new(ErrorCode::Internal, reason)));
        }
    }
}

/// One queued event on its way to the sink.
struct Delivery {
    #[cfg(target_os = "linux")]
    origin: Option<LogicalOwnerObservation>,
    event: Event,
    /// Answer this id after delivery (the event was sent as a request).
    ack: Option<(RequestId, Arc<Conn>)>,
}

struct DeliveryTarget {
    raw: Arc<dyn EventSink>,
    #[cfg(target_os = "linux")]
    owned: Option<Arc<dyn OwnedEventSink>>,
}

// ---------------------------------------------------------------------------------------
// The supervisor
// ---------------------------------------------------------------------------------------

pub(crate) struct Supervisor {
    #[cfg(target_os = "linux")]
    pub(super) directories: Option<Arc<super::directories::AccountDirectories>>,
    #[cfg(target_os = "linux")]
    pub(super) owned_sink: Option<Arc<dyn OwnedEventSink>>,
    pub plugin_id: String,
    pub info: PluginInfo,
    pub config: Arc<HostConfig>,
    pub registry: Arc<Registry>,
    pub sink: Arc<dyn EventSink>,
    pub secrets: Arc<dyn SecretStore>,
    pub rx: mpsc::Receiver<Command>,
    pub failures: u32,
    /// The last failure, for `Restarting`/`Failed`.
    pub last_failure: String,
}

type QueuedStart = (String, AccountSpec, StartScope, oneshot::Sender<Result<(), HostError>>);

enum Outcome {
    /// `shutdown`/disable finished; the task ends and then answers the caller.
    Shutdown(Option<oneshot::Sender<()>>),
    /// Manual restart requested; try again with a fresh counter.
    Restart,
    /// Another major version.
    Incompatible(String),
    /// Crash, hang or protocol error.
    Failed(String),
}

/// What the loop decided to do with a command received while no process can serve it.
enum Idle {
    Shutdown(oneshot::Sender<()>),
    /// Manual restart: fresh failure count.
    Restart(oneshot::Sender<()>),
    /// The backoff elapsed: next attempt, count kept.
    Timer,
}

impl Supervisor {
    pub async fn run(mut self) {
        let (deliver_tx, deliver_rx) = mpsc::channel::<Delivery>(self.config.event_queue);
        let delivery = tokio::spawn(delivery_loop(
            self.plugin_id.clone(),
            DeliveryTarget { raw: Arc::clone(&self.sink), #[cfg(target_os = "linux")] owned: self.owned_sink.clone() },
            deliver_rx,
        ));
        let shutdown_reply: Option<oneshot::Sender<()>> = loop {
            self.registry
                .set_plugin_state(&self.plugin_id, PluginState::Starting);
            let idle = match self.run_process(&deliver_tx).await {
                Outcome::Shutdown(reply) => break reply,
                Outcome::Restart => {
                    // A requested restart must start each wanted account on the
                    // replacement process just like recovery from a process exit.
                    self.park_accounts("plugin restarting");
                    self.failures = 0;
                    continue;
                }
                Outcome::Incompatible(api_version) => {
                    warn!(plugin = %self.plugin_id, %api_version, "incompatible API major version");
                    self.park_accounts("plugin speaks an incompatible API version");
                    self.registry.set_plugin_state(
                        &self.plugin_id,
                        PluginState::Incompatible { api_version },
                    );
                    self.wait_idle(None).await
                }
                Outcome::Failed(reason) => {
                    self.failures += 1;
                    self.last_failure = reason.clone();
                    self.park_accounts(&format!("plugin restarting: {reason}"));
                    if self.failures >= self.config.max_failures {
                        error!(plugin = %self.plugin_id, failures = self.failures, "giving up: {reason}");
                        self.registry
                            .set_plugin_state(&self.plugin_id, PluginState::Failed { reason });
                        self.wait_idle(None).await
                    } else {
                        let delay = self.config.backoff(self.failures);
                        warn!(plugin = %self.plugin_id, failures = self.failures, ?delay, "restarting: {reason}");
                        self.registry.set_plugin_state(
                            &self.plugin_id,
                            PluginState::Restarting {
                                failures: self.failures,
                                delay,
                                reason,
                            },
                        );
                        self.wait_idle(Some(delay)).await
                    }
                }
            };
            match idle {
                Idle::Shutdown(reply) => break Some(reply),
                Idle::Restart(reply) => {
                    self.failures = 0;
                    let _ = reply.send(());
                }
                Idle::Timer => {}
            }
        };
        self.park_accounts("plugin stopped");
        {
            let mut g = self.registry.lock();
            if let Some(e) = g.get_mut(&self.plugin_id) {
                e.cmd = None;
                for r in e.accounts.values_mut() {
                    r.info.wanted = false;
                }
            }
        }
        self.registry
            .set_plugin_state(&self.plugin_id, PluginState::Stopped);
        drop(deliver_tx);
        let _ = delivery.await;
        info!(plugin = %self.plugin_id, "supervisor ended");
        if let Some(reply) = shutdown_reply {
            let _ = reply.send(());
        }
    }

    /// Every account to `Stopped`/`disconnected`; `wanted` is kept so a restart resumes them.
    fn park_accounts(&self, detail: &str) {
        for (id, _, phase) in self.registry.accounts_of(&self.plugin_id) {
            if phase != AccountPhase::Stopped {
                self.registry.update_account(&self.plugin_id, &id, |r| {
                    r.info.phase = AccountPhase::Stopped;
                    r.info.state = AccountState::Disconnected;
                    r.info.reconnecting = false;
                    r.info.detail = Some(detail.to_owned());
                });
            }
        }
    }

    /// Serve commands while no process runs. With a delay, returns `Restart` (no reply) when
    /// the delay elapses; without one, waits for `Shutdown`/`Restart`.
    async fn wait_idle(&mut self, delay: Option<Duration>) -> Idle {
        let sleep = tokio::time::sleep(delay.unwrap_or(Duration::from_secs(1 << 30)));
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                () = &mut sleep, if delay.is_some() => return Idle::Timer,
                cmd = self.rx.recv() => {
                    let Some(cmd) = cmd else {
                        let (tx, _rx) = oneshot::channel();
                        return Idle::Shutdown(tx);
                    };
                    match cmd {
                        Command::Shutdown { reply } => return Idle::Shutdown(reply),
                        Command::Restart { reply } => return Idle::Restart(reply),
                        Command::StartAccount { account_id, spec, scope, reply } => {
                            // Remember the wish; the account starts after the restart.
                            let r = self.remember_start(&account_id, spec, scope, delay.is_some());
                            let _ = reply.send(r);
                        }
                        Command::StopAccount { account_id, reply } => {
                            self.registry.update_account(&self.plugin_id, &account_id, |r| {
                                r.info.wanted = false;
                                r.info.phase = AccountPhase::Stopped;
                                r.info.state = AccountState::Disconnected;
                            });
                            let _ = reply.send(Ok(()));
                        }
                        Command::StopAccountForRemoval { account_id, queued, reply } => {
                            let _ = reply.send(self.local_removal_stop(&account_id, &queued));
                        }
                        Command::Call { reply, .. } => {
                            let _ = reply.send(Err(self.not_running()));
                        }
                    }
                }
            }
        }
    }

    fn not_running(&self) -> HostError {
        HostError::PluginNotRunning {
            plugin_id: self.plugin_id.clone(),
            state: self
                .registry
                .plugin_state(&self.plugin_id)
                .unwrap_or(PluginState::Stopped),
        }
    }

    /// Record a start wish while the process is down. Accepted while a restart is pending
    /// (the account resumes with the plugin); refused when the plugin has given up.
    fn remember_start(
        &self,
        account_id: &str,
        spec: AccountSpec,
        scope: StartScope,
        will_restart: bool,
    ) -> Result<(), HostError> {
        if !will_restart {
            return Err(self.not_running());
        }
        self.check_single_account(account_id)?;
        if !self.registry.set_start(&self.plugin_id, account_id, spec.clone(), &scope) { return Err(HostError::ShuttingDown); }
        if self.registry
            .update_start(&self.plugin_id, account_id, &scope, true, |r| {
                r.spec = spec;
                r.info.wanted = true;
                r.info.detail = Some("waiting for the plugin to restart".to_owned());
            }).is_none() { return Err(HostError::ShuttingDown); }
        Ok(())
    }

    fn check_single_account(&self, account_id: &str) -> Result<(), HostError> {
        let multi = self.info.manifest.as_ref().is_some_and(|m| m.multi_account);
        if multi {
            return Ok(());
        }
        let other =
            self.registry
                .accounts_of(&self.plugin_id)
                .into_iter()
                .any(|(id, wanted, phase)| {
                    id != account_id && (wanted || phase != AccountPhase::Stopped)
                });
        if other {
            Err(HostError::SingleAccount(self.plugin_id.clone()))
        } else {
            Ok(())
        }
    }

    // -----------------------------------------------------------------------------------
    // One process lifetime
    // -----------------------------------------------------------------------------------

    async fn run_process(&mut self, deliver_tx: &mpsc::Sender<Delivery>) -> Outcome {
        let Some(exe) = self.info.executable.clone() else {
            return Outcome::Failed("entry[0] is not installed".into());
        };
        let Some(manifest) = self.info.manifest.clone() else {
            return Outcome::Failed("manifest is invalid".into());
        };

        let mut cmd = tokio::process::Command::new(&exe);
        cmd.args(&manifest.entry[1..])
            .current_dir(&self.info.dir)
            .envs(&self.config.env)
            .envs(manifest.env.clone().unwrap_or_default())
            .env(ch_protocol::env::PLUGIN_ID, &self.plugin_id)
            .env(ch_protocol::env::API_VERSION, ch_protocol::API_VERSION)
            .env(ch_protocol::env::LOG_LEVEL, self.config.log_level.as_str())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => return Outcome::Failed(format!("spawn {}: {e}", exe.display())),
        };
        info!(plugin = %self.plugin_id, pid = child.id(), "spawned {}", exe.display());
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");

        let (writer_tx, writer_rx) = mpsc::channel::<Out>(self.config.write_queue);
        let writer = tokio::spawn(writer_loop(stdin, writer_rx));
        let stderr_task = tokio::spawn(stderr_loop(
            self.plugin_id.clone(),
            stderr,
            self.config.plugin_log_path(&self.plugin_id),
            self.config.stderr_lines_per_sec,
        ));
        let conn = Arc::new(Conn {
            #[cfg(target_os = "linux")]
            origins: self.owned_sink.as_ref().map(|_| super::origins::Origins::new()),
            plugin_id: self.plugin_id.clone(),
            identity: Arc::new(()),
            registry: Arc::downgrade(&self.registry),
            pending: Mutex::new(HashMap::new()),
            writer: writer_tx,
            ping_misses: AtomicU32::new(0),
            stalled: AtomicBool::new(false),
            initialized: AtomicBool::new(false),
        });
        let reader = tokio::spawn(reader_loop(
            Arc::clone(&conn),
            stdout,
            Arc::clone(&self.registry),
            Arc::clone(&self.secrets),
            deliver_tx.clone(),
        ));

        let mut proc = Process {
            child,
            conn: Arc::clone(&conn),
            reader: Some(reader),
            writer,
            stderr_task,
        };

        let outcome = self.serve(&mut proc).await;

        // Whatever happened, make sure the process is gone and the tasks are over.
        proc.finish(&self.config, &self.registry, &self.plugin_id)
            .await;
        outcome
    }

    async fn serve(&mut self, proc: &mut Process) -> Outcome {
        let conn = Arc::clone(&proc.conn);
        // ---- handshake ----
        let init_params = InitializeParams {
            api_version: ch_protocol::API_VERSION.to_owned(),
            core_version: self.config.core_version.clone(),
            plugin_dir: self.info.dir.to_string_lossy().into_owned(),
            locale: self.config.locale.clone(),
            log_level: self.config.log_level.clone(),
        };
        let Some((_, init_rx)) = conn.request(
            PendingKind::Initialize,
            "initialize",
            serde_json::to_value(&init_params).expect("serialise"),
            self.config.request_timeout("initialize"),
            None,
            &conn,
        ) else {
            return Outcome::Failed("could not write initialize".into());
        };
        let mut queued_starts: Vec<QueuedStart> = Vec::new();
        let init = {
            tokio::pin!(init_rx);
            loop {
                tokio::select! {
                    r = &mut init_rx => break r,
                    ended = proc.wait_ended() => return ended,
                    cmd = self.rx.recv() => match cmd {
                        None | Some(Command::Shutdown { .. }) => {
                            let reply = match cmd { Some(Command::Shutdown { reply }) => Some(reply), _ => None };
                            self.registry.set_plugin_state(&self.plugin_id, PluginState::ShuttingDown);
                            proc.stop_without_handshake(&self.config).await;
                            return Outcome::Shutdown(reply);
                        }
                        Some(Command::Restart { reply }) => {
                            proc.stop_without_handshake(&self.config).await;
                            let _ = reply.send(());
                            return Outcome::Restart;
                        }
                        Some(Command::StartAccount { account_id, spec, scope, reply }) => {
                            queued_starts.push((account_id, spec, scope, reply));
                        }
                        Some(Command::StopAccount { account_id, reply }) => {
                            self.registry.update_account(&self.plugin_id, &account_id, |r| {
                                r.info.wanted = false;
                            });
                            queued_starts.retain(|(id, _, _, _)| id != &account_id);
                            let _ = reply.send(Ok(()));
                        }
                        Some(Command::StopAccountForRemoval { account_id, queued, reply }) => {
                            let outcome = self.local_removal_stop(&account_id, &queued);
                            if outcome == RemovalStop::Confirmed {
                                queued_starts.retain(|(id, _, _, _)| id != &account_id);
                            }
                            let _ = reply.send(outcome);
                        }
                        Some(Command::Call { reply, .. }) => {
                            let _ = reply.send(Err(self.not_running()));
                        }
                    },
                    () = sweep_wait(&conn) => {
                        if let Some(reason) = self.sweep(&conn) {
                            return Outcome::Failed(reason);
                        }
                    }
                }
            }
        };
        let init: InitializeResult = match init {
            Ok(Ok(v)) => match serde_json::from_value(v) {
                Ok(r) => r,
                Err(e) => return Outcome::Failed(format!("initialize result is malformed: {e}")),
            },
            Ok(Err(e)) => {
                return Outcome::Failed(format!(
                    "initialize failed: {} {}",
                    e.code().as_str(),
                    e.message
                ));
            }
            Err(_) => return Outcome::Failed("initialize was not answered".into()),
        };
        if !ch_protocol::versions_compatible(&init.api_version, ch_protocol::API_VERSION) {
            for (_, _, _, reply) in queued_starts {
                let _ = reply.send(Err(HostError::PluginNotRunning {
                    plugin_id: self.plugin_id.clone(),
                    state: PluginState::Incompatible {
                        api_version: init.api_version.clone(),
                    },
                }));
            }
            proc.graceful_stop(&self.config).await;
            return Outcome::Incompatible(init.api_version);
        }
        conn.initialized.store(true, Ordering::Release);
        for k in init.capabilities.unknown_keys() {
            debug!(plugin = %self.plugin_id, key = k, "unknown capability key (newer minor?)");
        }
        let capabilities = Arc::new(init.capabilities);
        info!(plugin = %self.plugin_id, api = %init.api_version, version = ?init.plugin_version,
              capabilities = capabilities.0.len(), "initialized");
        self.registry.set_plugin_state(
            &self.plugin_id,
            PluginState::Running {
                api_version: init.api_version,
                plugin_version: init.plugin_version,
                capabilities,
            },
        );
        let healthy_since = Instant::now();

        // ---- resume accounts that were running before a crash, then the queued starts ----
        for (id, wanted, phase) in self.registry.accounts_of(&self.plugin_id) {
            if wanted
                && phase == AccountPhase::Stopped
                && let Some((spec, scope)) = self.registry.account_start(&self.plugin_id, &id)
            {
                info!(plugin = %self.plugin_id, account = %id, "resuming account");
                self.begin_account_start(&conn, &id, spec, scope, None).await;
            }
        }
        for (id, spec, scope, reply) in queued_starts {
            self.handle_start(&conn, id, spec, scope, reply).await;
        }

        // ---- serve ----
        let mut ping = tokio::time::interval(self.config.ping_interval);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ping.tick().await; // the first tick fires immediately; skip it
        let outcome = loop {
            tokio::select! {
                ended = proc.wait_ended() => break ended,
                _ = ping.tick() => {
                    if conn.request(PendingKind::Ping, "ping", Value::Object(Default::default()),
                                    self.config.request_timeout("ping"), None, &conn).is_none() {
                        break Outcome::Failed("cannot write to the plugin".into());
                    }
                }
                () = sweep_wait(&conn) => {
                    if let Some(reason) = self.sweep(&conn) {
                        break Outcome::Failed(reason);
                    }
                }
                cmd = self.rx.recv() => match cmd {
                    None | Some(Command::Shutdown { .. }) => {
                        let reply = match cmd { Some(Command::Shutdown { reply }) => Some(reply), _ => None };
                        self.registry.set_plugin_state(&self.plugin_id, PluginState::ShuttingDown);
                        proc.graceful_stop(&self.config).await;
                        break Outcome::Shutdown(reply);
                    }
                    Some(Command::Restart { reply }) => {
                        self.registry.set_plugin_state(&self.plugin_id, PluginState::ShuttingDown);
                        proc.graceful_stop(&self.config).await;
                        let _ = reply.send(());
                        break Outcome::Restart;
                    }
                    Some(Command::StartAccount { account_id, spec, scope, reply }) => {
                        self.handle_start(&conn, account_id, spec, scope, reply).await;
                    }
                    Some(Command::StopAccount { account_id, reply }) => {
                        self.handle_stop(&conn, account_id, reply);
                    }
                    Some(Command::StopAccountForRemoval { account_id, queued, reply }) => {
                        self.handle_removal_stop(&conn, account_id, queued, reply);
                    }
                    Some(Command::Call { method, params, cancel, request_id, reply }) => {
                        // A queued call can outlive the account run that authorized it.
                        if cancel.as_ref().is_some_and(|token| token.is_cancelled()) {
                            let _ = reply.send(Err(HostError::code(ErrorCode::Cancelled, "request cancelled before dispatch")));
                            continue;
                        }
                        let timeout = self.config.request_timeout(&method);
                        let name: &'static str = ch_protocol::method(&method)
                            .map_or("x-vendor", |m| m.name);
                        // Vendor methods are not in the registry; keep the dynamic name alive.
                        let frame_method = method.clone();
                        let id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
                        if let Some(cell) = request_id && cell.set(id).is_err() {
                            let _ = reply.send(Err(HostError::InvalidMethod(frame_method,"request id cell already used")));
                            continue;
                        }
                        let (tx, rx) = oneshot::channel();
                        let (done_tx, done_rx) = oneshot::channel::<()>();
                        match encode(&Request::new(id, frame_method, params)) {
                            Err(e) => { let _ = reply.send(Err(HostError::Json(e))); }
                            Ok(frame) => {
                                conn.pending().insert(id, Pending {
                                    owner: PendingOwner::default(),
                                    kind: PendingKind::Call, method: name,
                                    deadline: Instant::now() + timeout, timeout,
                                    reply: tx, _done: Some(done_tx),
                                });
                                if !conn.send_frame(frame) {
                                    conn.pending().remove(&id);
                                    let _ = reply.send(Err(HostError::code(ErrorCode::Internal, "plugin is not reading stdin")));
                                    break Outcome::Failed("plugin is not reading stdin".into());
                                }
                                let c2 = Arc::clone(&conn);
                                tokio::spawn(async move {
                                    let cancelled = async {
                                        match cancel { Some(t) => t.cancelled().await, None => std::future::pending().await }
                                    };
                                    tokio::select! {
                                        _ = cancelled => {
                                            if c2.pending().contains_key(&id) { c2.send_cancel(id); }
                                        }
                                        _ = done_rx => {}
                                    }
                                });
                                tokio::spawn(async move {
                                    let r = match rx.await {
                                        Ok(Ok(v)) => Ok(v),
                                        Ok(Err(e)) => Err(HostError::rpc(e)),
                                        Err(_) => Err(HostError::code(ErrorCode::Internal, "plugin restarted")),
                                    };
                                    let _ = reply.send(r);
                                });
                            }
                        }
                    }
                },
            }
        };
        if let Outcome::Failed(_) = &outcome
            && healthy_since.elapsed() >= self.config.healthy_after
        {
            // It ran long enough to count as healthy: this failure starts a new series.
            self.failures = 0;
        }
        outcome
    }

    /// Expire overdue requests. Returns a reason when the process must be restarted.
    fn sweep(&self, conn: &Arc<Conn>) -> Option<String> {
        let now = Instant::now();
        let expired: Vec<(i64, Pending)> = {
            let mut g = conn.pending();
            let ids: Vec<i64> = g
                .iter()
                .filter(|(_, p)| p.deadline <= now)
                .map(|(id, _)| *id)
                .collect();
            ids.into_iter()
                .filter_map(|id| g.remove(&id).map(|p| (id, p)))
                .collect()
        };
        let mut fatal = None;
        for (id, p) in expired {
            fail_lifecycle(conn, &self.registry, id, &p.kind);
            match p.kind {
                PendingKind::Ping => {
                    if conn.stalled.load(Ordering::Acquire) {
                        debug!(plugin = %self.plugin_id, "ping unanswered while the event queue is full; not counted");
                        continue;
                    }
                    let misses = conn.ping_misses.fetch_add(1, Ordering::AcqRel) + 1;
                    warn!(plugin = %self.plugin_id, misses, "ping missed");
                    if misses >= self.config.ping_misses {
                        fatal = Some(format!("hung: {misses} pings missed"));
                    }
                }
                PendingKind::Initialize => {
                    fatal = Some("initialize timed out".into());
                }
                PendingKind::Shutdown => {}
                PendingKind::AccountStart(acc, scope) => {
                    warn!(plugin = %self.plugin_id, account = %acc, "account.start timed out");
                    conn.send_cancel(id);
                    self.registry.update_start_metadata(&self.plugin_id, &acc, &scope, |r| {
                        r.info.phase = AccountPhase::Stopped;
                        r.info.wanted = false;
                        r.info.state = AccountState::Error;
                        r.info.detail = Some("account.start timed out".to_owned());
                    });
                    let _ = p.reply.send(Err(timeout_error(p.method)));
                }
                PendingKind::AccountStop(acc) | PendingKind::RemovalStop(acc) => {
                    warn!(plugin = %self.plugin_id, account = %acc, "account.stop timed out; marking stopped");
                    conn.send_cancel(id);
                    self.registry.update_pending(conn, &acc, &p.owner, |r| {
                        r.info.phase = AccountPhase::Stopped;
                        r.info.wanted = false;
                        r.info.state = AccountState::Disconnected;
                    });
                    let _ = p.reply.send(Err(timeout_error(p.method)));
                }
                PendingKind::Call => {
                    debug!(plugin = %self.plugin_id, id, method = p.method, "request timed out");
                    conn.send_cancel(id);
                    let _ = p.reply.send(Err(timeout_error(p.method)));
                }
            }
        }
        fatal
    }

    async fn handle_start(
        &self,
        conn: &Arc<Conn>,
        account_id: String,
        spec: AccountSpec,
        scope: StartScope,
        reply: oneshot::Sender<Result<(), HostError>>,
    ) {
        if !scope.live() { let _ = reply.send(Err(HostError::ShuttingDown)); return; }
        match self.registry.account_phase(&self.plugin_id, &account_id) {
            Some(AccountPhase::Starting) => {
                let _ = reply.send(Err(HostError::AccountAlreadyStarted(account_id)));
                return;
            }
            Some(AccountPhase::Running) => {
                // Fresh explicit start can replace a revoked future wish after
                // a definite refusal/rearm, without reviving saved requests.
                if self.registry.account_scope(&self.plugin_id, &account_id).is_some_and(|old| !old.live()) {
                    self.registry.set_start(&self.plugin_id, &account_id, spec, &scope);
                }
                let _ = reply.send(Err(HostError::AccountAlreadyStarted(account_id)));
                return;
            }
            Some(AccountPhase::Stopping) => {
                let _ = reply.send(Err(HostError::AccountBusy(account_id, "stopping")));
                return;
            }
            _ => {}
        }
        if let Err(e) = self.check_single_account(&account_id) {
            let _ = reply.send(Err(e));
            return;
        }
        self.begin_account_start(conn, &account_id, spec, scope, Some(reply))
            .await;
    }

    /// Build and send `account.start`; the reader applies the phase transition on the reply.
    async fn begin_account_start(
        &self,
        conn: &Arc<Conn>,
        account_id: &str,
        spec: AccountSpec,
        scope: StartScope,
        reply: Option<oneshot::Sender<Result<(), HostError>>>,
    ) {
        if !self.registry.set_start(&self.plugin_id, account_id, spec.clone(), &scope) {
            // Replay keeps its frozen observation and explains refusal locally;
            // it never falls back to a new by-ID capture.
            self.registry.refuse_unexposed_start(&self.plugin_id, account_id, &scope);
            if let Some(reply) = reply { let _ = reply.send(Err(HostError::ShuttingDown)); }
            return;
        }
        let secrets = match self.secrets.get_all(&self.plugin_id, account_id).await {
            Ok(s) => s,
            Err(e) => {
                error!(plugin = %self.plugin_id, account = %account_id, "secret store: {e}");
                self.registry
                    .update_start_metadata(&self.plugin_id, account_id, &scope, |r| {
                        r.info.wanted = false;
                        r.info.phase = AccountPhase::Stopped;
                        r.info.state = AccountState::Error;
                        r.info.detail = Some(format!("secret store: {e}"));
                    });
                if let Some(r) = reply {
                    let _ = r.send(Err(HostError::code(ErrorCode::Internal, e.to_string())));
                }
                return;
            }
        };
        let mut settings = spec.settings.clone();
        fill_defaults(
            self.info
                .manifest
                .as_ref()
                .and_then(|m| m.settings_schema.as_ref()),
            &mut settings,
        );
        let state_dir = self.config.state_dir(account_id);
        let cache_dir = self.config.cache_dir(account_id);
        if let Err(error) = self.prepare_account_directories(account_id, &scope, [&state_dir, &cache_dir]).await {
            warn!(plugin = %self.plugin_id, account = %account_id, "account directory preparation unavailable: {error}");
            self.registry.update_start(&self.plugin_id, account_id, &scope, false, |record| {
                record.spec = spec;
                // Preserve the start wish when Core closes while directory work
                // is waiting; shutdown must not persist this account disabled.
                record.info.wanted = true;
                record.info.phase = AccountPhase::Stopped;
                record.info.state = AccountState::Error;
                record.info.detail = Some("account directory preparation unavailable".into());
            });
            if let Some(reply) = reply { let _ = reply.send(Err(error)); }
            return;
        }
        if !self.registry.current_start(&self.plugin_id, account_id, &scope) {
            self.registry.refuse_unexposed_start(&self.plugin_id, account_id, &scope);
            if let Some(reply) = reply { let _ = reply.send(Err(HostError::ShuttingDown)); }
            return;
        }
        // The counted local mkdir work and mutation token have settled before
        // any account.start request or its asynchronous network wait.
        let params = AccountStartParams {
            account_id: account_id.to_owned(),
            settings,
            secrets,
            state_dir: state_dir.to_string_lossy().into_owned(),
            cache_dir: cache_dir.to_string_lossy().into_owned(),
            ghost: spec.ghost,
            auto_download: spec.auto_download.clone(),
        };
        if self.registry
            .update_start(&self.plugin_id, account_id, &scope, true, |r| {
                r.spec = spec;
                r.info.wanted = true;
                r.info.phase = AccountPhase::Starting;
                r.info.state = AccountState::Disconnected;
                r.info.detail = None;
                r.info.reconnecting = false;
            }).is_none() {
            self.registry.refuse_unexposed_start(&self.plugin_id, account_id, &scope);
            if let Some(reply) = reply { let _ = reply.send(Err(HostError::ShuttingDown)); }
            return;
        }
        #[cfg(all(test, target_os = "linux"))]
        if let Some(directories) = &self.directories { directories.checkpoint_at("before_frame"); }
        if !self.registry.current_start(&self.plugin_id, account_id, &scope) {
            self.registry.refuse_unexposed_start(&self.plugin_id, account_id, &scope);
            if let Some(reply) = reply { let _ = reply.send(Err(HostError::ShuttingDown)); }
            return;
        }
        let sent = conn.request(
            PendingKind::AccountStart(account_id.to_owned(), scope.clone()),
            "account.start",
            serde_json::to_value(&params).expect("serialise"),
            self.config.request_timeout("account.start"),
            None,
            conn,
        );
        match sent {
            None => {
                self.registry
                    .update_start(&self.plugin_id, account_id, &scope, false, |r| {
                        r.info.phase = AccountPhase::Stopped;
                    });
                if let Some(r) = reply {
                    let _ = r.send(Err(HostError::code(
                        ErrorCode::Internal,
                        "cannot write to the plugin",
                    )));
                }
            }
            Some((_, rx)) => {
                if let Some(reply) = reply {
                    tokio::spawn(async move {
                        let r = match rx.await {
                            Ok(Ok(_)) => Ok(()),
                            Ok(Err(e)) => Err(HostError::rpc(e)),
                            Err(_) => Err(HostError::code(ErrorCode::Internal, "plugin restarted")),
                        };
                        let _ = reply.send(r);
                    });
                }
            }
        }
    }

    async fn prepare_account_directories(&self, account_id: &str, scope: &StartScope, paths: [&std::path::Path; 2]) -> Result<(), HostError> {
        #[cfg(not(target_os = "linux"))]
        let _ = (account_id, scope);
        #[cfg(target_os = "linux")]
        if let Some(directories) = &self.directories {
            for (path, error) in directories.prepare(&self.config, account_id, scope).await? {
                warn!(plugin = %self.plugin_id, "cannot create {}: {error}", path.display());
            }
            return Ok(());
        }
        // Public standalone hosts retain their existing unbound behavior.
        for path in paths {
            if let Err(error) = tokio::fs::create_dir_all(path).await {
                warn!(plugin = %self.plugin_id, "cannot create {}: {error}", path.display());
            }
        }
        Ok(())
    }

    fn local_removal_stop(&self, account: &str, queued: &super::removal::Command) -> RemovalStop {
        let Some(_reservation) = queued.admit() else { return RemovalStop::Refused; };
        if !matches!(self.registry.removal_decision(&self.plugin_id, account, None), Decision::Local) {
            return RemovalStop::Refused;
        }
        self.registry.forget_unexposed_wish(&self.plugin_id, account);
        RemovalStop::Confirmed
    }

    fn handle_removal_stop(&self, conn: &Arc<Conn>, account: String, queued: super::removal::Command,
        reply: oneshot::Sender<RemovalStop>) {
        let Some(reservation) = queued.admit() else { let _ = reply.send(RemovalStop::Refused); return; };
        match self.registry.removal_decision(&self.plugin_id, &account, Some(&conn.identity)) {
            Decision::Local => {
                self.registry.forget_unexposed_wish(&self.plugin_id, &account);
                let _ = reply.send(RemovalStop::Confirmed); return;
            }
            Decision::Refuse => { let _ = reply.send(RemovalStop::Refused); return; }
            Decision::Dispatch => {}
        }
        self.registry.update_account(&self.plugin_id, &account, |record| {
            record.info.wanted = false;
            record.info.phase = AccountPhase::Stopping;
        });
        let sent = conn.request(PendingKind::RemovalStop(account.clone()), "account.stop",
            serde_json::json!({"account_id":account}), self.config.request_timeout("account.stop"),
            None, conn);
        drop(reservation); // Actual request owns its reply independently of the caller.
        let Some((id, response)) = sent else { let _ = reply.send(RemovalStop::Refused); return; };
        let request = LifecycleRequest::new(&conn.identity, id);
        let registry = self.registry.clone(); let plugin = self.plugin_id.clone();
        tokio::spawn(async move {
            let positive = match response.await {
                Ok(Ok(value)) => value.is_object(),
                Ok(Err(error)) => error.code() == ErrorCode::UnknownAccount,
                Err(_) => {
                    registry.finish_lifecycle(&plugin, &account, &request, false);
                    false
                }
            };
            let outcome = if positive && matches!(registry.removal_decision(&plugin, &account, None), Decision::Local) {
                RemovalStop::Confirmed
            } else { RemovalStop::Refused };
            let _ = reply.send(outcome);
        });
    }

    fn handle_stop(
        &self,
        conn: &Arc<Conn>,
        account_id: String,
        reply: oneshot::Sender<Result<(), HostError>>,
    ) {
        match self.registry.account_phase(&self.plugin_id, &account_id) {
            None => {
                let _ = reply.send(Err(HostError::UnknownAccount(account_id)));
                return;
            }
            Some(AccountPhase::Stopped) => {
                self.registry
                    .update_account(&self.plugin_id, &account_id, |r| {
                        r.info.wanted = false;
                    });
                let _ = reply.send(Ok(()));
                return;
            }
            Some(AccountPhase::Stopping) => {
                let _ = reply.send(Err(HostError::AccountBusy(account_id, "stopping")));
                return;
            }
            Some(AccountPhase::Starting) if !self.registry.can_stop_accepted_start(&self.plugin_id, &account_id, &conn.identity) => {
                let _ = reply.send(Err(HostError::AccountBusy(account_id, "starting")));
                return;
            }
            Some(AccountPhase::Starting | AccountPhase::Running) => {}
        }
        self.registry
            .update_account(&self.plugin_id, &account_id, |r| {
                r.info.wanted = false;
                r.info.phase = AccountPhase::Stopping;
            });
        let params = serde_json::json!({ "account_id": account_id });
        match conn.request(
            PendingKind::AccountStop(account_id.clone()),
            "account.stop",
            params,
            self.config.request_timeout("account.stop"),
            None,
            conn,
        ) {
            None => {
                self.registry
                    .update_account(&self.plugin_id, &account_id, |r| {
                        r.info.phase = AccountPhase::Stopped;
                        r.info.state = AccountState::Disconnected;
                    });
                let _ = reply.send(Ok(()));
            }
            Some((_, rx)) => {
                tokio::spawn(async move {
                    // `UNKNOWN_ACCOUNT` means "already stopped" (§4.2); the reader has marked
                    // the account stopped either way.
                    let r = match rx.await {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(e)) if e.code() == ErrorCode::UnknownAccount => Ok(()),
                        Ok(Err(e)) => Err(HostError::rpc(e)),
                        Err(_) => Ok(()),
                    };
                    let _ = reply.send(r);
                });
            }
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
#[path = "start_tests.rs"]
mod start_tests;

fn fail_lifecycle(conn: &Conn, registry: &Registry, id: i64, kind: &PendingKind) {
    let account = match kind {
        PendingKind::AccountStart(account, _) | PendingKind::AccountStop(account) | PendingKind::RemovalStop(account) => account,
        _ => return,
    };
    registry.finish_lifecycle(&conn.plugin_id, account, &LifecycleRequest::new(&conn.identity, id), false);
}

fn timeout_error(method: &str) -> ErrorObject {
    ErrorObject::new(ErrorCode::Timeout, format!("{method} timed out"))
}

/// Sleep until the earliest pending deadline (or forever when nothing is pending).
async fn sweep_wait(conn: &Arc<Conn>) {
    let next = conn.pending().values().map(|p| p.deadline).min();
    match next {
        Some(t) => tokio::time::sleep_until(tokio::time::Instant::from_std(t)).await,
        None => std::future::pending().await,
    }
}

// ---------------------------------------------------------------------------------------
// The child process and its tasks
// ---------------------------------------------------------------------------------------

struct Process {
    child: Child,
    conn: Arc<Conn>,
    /// `None` once it has been joined (a `JoinHandle` must not be polled again).
    reader: Option<JoinHandle<Result<(), FrameError>>>,
    writer: JoinHandle<Option<std::io::Error>>,
    stderr_task: JoinHandle<()>,
}

impl Process {
    /// Resolves when the process exits or the reader ends (EOF or framing error).
    async fn wait_ended(&mut self) -> Outcome {
        tokio::select! {
            status = self.child.wait() => {
                match status {
                    Ok(s) => Outcome::Failed(format!("exited unexpectedly ({s})")),
                    Err(e) => Outcome::Failed(format!("wait: {e}")),
                }
            }
            r = join_opt(&mut self.reader) => {
                self.reader = None;
                match r {
                    Ok(Ok(())) => {
                        // EOF on stdout: the process is gone or closed its end.
                        let status = tokio::time::timeout(Duration::from_secs(2), self.child.wait()).await;
                        match status {
                            Ok(Ok(s)) => Outcome::Failed(format!("exited unexpectedly ({s})")),
                            _ => Outcome::Failed("closed stdout".into()),
                        }
                    }
                    Ok(Err(e)) => Outcome::Failed(format!("protocol error: {e}")),
                    Err(e) => Outcome::Failed(format!("reader task: {e}")),
                }
            }
        }
    }

    /// §4.5: `shutdown` → wait for the reply (grace) → close stdin → wait → SIGTERM → wait →
    /// SIGKILL. Never closes stdin before the reply or the grace expiry.
    async fn graceful_stop(&mut self, config: &HostConfig) {
        let grace = config
            .scale(Duration::from_secs(ch_protocol::SHUTDOWN_GRACE_SECONDS))
            .max(config.shutdown_grace);
        if self.conn.initialized.load(Ordering::Acquire)
            && let Some((_, rx)) = self.conn.request(
                PendingKind::Shutdown,
                "shutdown",
                Value::Object(Default::default()),
                grace,
                None,
                &self.conn,
            )
        {
            match tokio::time::timeout(grace, rx).await {
                Ok(Ok(Ok(_))) => debug!(plugin = %self.conn.plugin_id, "shutdown acknowledged"),
                Ok(Ok(Err(e))) => {
                    warn!(plugin = %self.conn.plugin_id, "shutdown answered with error {}", e.code().as_str())
                }
                Ok(Err(_)) => {
                    debug!(plugin = %self.conn.plugin_id, "process ended before answering shutdown")
                }
                Err(_) => {
                    warn!(plugin = %self.conn.plugin_id, "shutdown not answered within {grace:?}")
                }
            }
        }
        self.stop_without_handshake(config).await;
    }

    /// Close stdin, wait for exit, escalate to SIGTERM then SIGKILL.
    async fn stop_without_handshake(&mut self, config: &HostConfig) {
        let _ = self.conn.writer.try_send(Out::Close);
        let exit_grace = config.exit_grace;
        if tokio::time::timeout(exit_grace, self.child.wait())
            .await
            .is_ok()
        {
            return;
        }
        if let Some(pid) = self.child.id() {
            warn!(plugin = %self.conn.plugin_id, pid, "did not exit after stdin closed; SIGTERM");
            // SAFETY: plain kill(2) on a pid we own; no memory is involved.
            #[cfg(unix)]
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGTERM);
            }
        }
        if tokio::time::timeout(exit_grace, self.child.wait())
            .await
            .is_ok()
        {
            return;
        }
        warn!(plugin = %self.conn.plugin_id, "did not exit after SIGTERM; SIGKILL");
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }

    /// Tear everything down after the loop decided the process lifetime is over.
    async fn finish(mut self, config: &HostConfig, registry: &Registry, plugin_id: &str) {
        if self.child.try_wait().ok().flatten().is_none() {
            self.stop_without_handshake(config).await;
        }
        self.conn.fail_all(registry, "plugin restarted");
        // The reader ends on EOF once the process is dead; give it a moment, then abort.
        if let Some(reader) = self.reader.take()
            && tokio::time::timeout(Duration::from_secs(2), reader)
                .await
                .is_err()
        {
            // The timeout dropped the handle; the task is detached and ends on EOF.
            warn!(plugin = %plugin_id, "reader did not end after the process exited");
        }
        let _ = self.conn.writer.try_send(Out::Close);
        self.writer.abort();
        if tokio::time::timeout(Duration::from_secs(2), &mut self.stderr_task)
            .await
            .is_err()
        {
            self.stderr_task.abort();
        }
        debug!(plugin = %plugin_id, "process finished");
    }
}

/// Await an optional `JoinHandle` without consuming it; pending forever when `None`.
async fn join_opt<T>(h: &mut Option<JoinHandle<T>>) -> Result<T, tokio::task::JoinError> {
    match h {
        Some(h) => h.await,
        None => std::future::pending().await,
    }
}

async fn writer_loop(mut stdin: ChildStdin, mut rx: mpsc::Receiver<Out>) -> Option<std::io::Error> {
    while let Some(out) = rx.recv().await {
        match out {
            Out::Close => break,
            Out::Frame(f) => {
                if let Err(e) = stdin.write_all(&f).await {
                    return Some(e);
                }
                if let Err(e) = stdin.flush().await {
                    return Some(e);
                }
            }
        }
    }
    drop(stdin);
    None
}

async fn stderr_loop(
    plugin_id: String,
    stderr: tokio::process::ChildStderr,
    log_path: Option<std::path::PathBuf>,
    per_sec: u32,
) {
    let mut file = match &log_path {
        Some(p) => {
            if let Some(dir) = p.parent() {
                let _ = tokio::fs::create_dir_all(dir).await;
            }
            tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(p)
                .await
                .ok()
        }
        None => None,
    };
    let mut lines = BufReader::new(stderr).lines();
    let mut window = Instant::now();
    let mut in_window: u32 = 0;
    let mut dropped: u64 = 0;
    while let Ok(Some(line)) = lines.next_line().await {
        if let Some(f) = file.as_mut() {
            let mut bytes = line.clone().into_bytes();
            bytes.push(b'\n');
            if f.write_all(&bytes).await.is_err() {
                file = None;
            }
        }
        if window.elapsed() >= Duration::from_secs(1) {
            if dropped > 0 {
                warn!(target: "ch_core::host::plugin_stderr", plugin = %plugin_id, "{dropped} stderr lines not shown (rate limit)");
            }
            window = Instant::now();
            in_window = 0;
            dropped = 0;
        }
        if in_window < per_sec {
            in_window += 1;
            info!(target: "ch_core::host::plugin_stderr", plugin = %plugin_id, "{line}");
        } else {
            dropped += 1;
        }
    }
    if dropped > 0 {
        warn!(target: "ch_core::host::plugin_stderr", plugin = %plugin_id, "{dropped} stderr lines not shown (rate limit)");
    }
}

// ---------------------------------------------------------------------------------------
// Reader: stdout → pending replies, plugin requests, events
// ---------------------------------------------------------------------------------------

async fn reader_loop(
    conn: Arc<Conn>,
    stdout: ChildStdout,
    registry: Arc<Registry>,
    secrets: Arc<dyn SecretStore>,
    deliver: mpsc::Sender<Delivery>,
) -> Result<(), FrameError> {
    let mut reader = FrameReader::new(stdout);
    let pid = conn.plugin_id.clone();
    while let Some(frame) = reader.next().await? {
        match frame {
            Frame::Invalid { id, reason } => {
                warn!(plugin = %pid, "invalid frame: {reason}");
                conn.reject_frame(id, reason);
            }
            Frame::Envelope(Envelope::Success(s)) => {
                let id = s.id.clone();
                resolve(&conn, &registry, id, Ok(s.result));
            }
            Frame::Envelope(Envelope::Error(e)) => match e.id {
                Some(id) => resolve(&conn, &registry, id, Err(e.error)),
                None => {
                    warn!(plugin = %pid, "error response without id: {} {}", e.error.code().as_str(), e.error.message)
                }
            },
            Frame::Envelope(Envelope::Notification(n)) => {
                handle_incoming(
                    &conn, &registry, &secrets, &deliver, None, &n.method, n.params,
                )
                .await;
            }
            Frame::Envelope(Envelope::Request(r)) => {
                handle_incoming(
                    &conn,
                    &registry,
                    &secrets,
                    &deliver,
                    Some(r.id),
                    &r.method,
                    r.params,
                )
                .await;
            }
        }
    }
    Ok(())
}

/// A response from the plugin: hand it to the waiting caller and apply lifecycle transitions
/// in stream order.
fn resolve(
    conn: &Arc<Conn>,
    registry: &Registry,
    id: RequestId,
    result: Result<Value, ErrorObject>,
) {
    let RequestId::Number(n) = id else {
        debug!(plugin = %conn.plugin_id, %id, "response with a string id (we never send those)");
        return;
    };
    let Some(p) = conn.pending().remove(&n) else {
        trace!(plugin = %conn.plugin_id, id = n, "late response for a finished request");
        return;
    };
    trace!(plugin = %conn.plugin_id, id = n, method = p.method, ok = result.is_ok(), "response");
    match &p.kind {
        PendingKind::Shutdown if result.as_ref().is_ok_and(Value::is_object) => registry.shutdown_confirmed(&conn.plugin_id, &conn.identity),
        PendingKind::AccountStart(account, _) => registry.finish_lifecycle(&conn.plugin_id, account,
            &LifecycleRequest::new(&conn.identity, n), result.as_ref().is_ok_and(Value::is_object)),
        PendingKind::AccountStop(account) | PendingKind::RemovalStop(account) => registry.finish_lifecycle(&conn.plugin_id, account,
            &LifecycleRequest::new(&conn.identity, n), result.as_ref().is_ok_and(Value::is_object)
                || result.as_ref().is_err_and(|error| error.code() == ErrorCode::UnknownAccount)),
        _ => {}
    }
    #[cfg(target_os = "linux")]
    if let (Some(origins), PendingKind::AccountStop(account) | PendingKind::RemovalStop(account)) = (&conn.origins, &p.kind)
        && (result.as_ref().is_ok_and(Value::is_object)
            || result.as_ref().is_err_and(|error| error.code() == ErrorCode::UnknownAccount)) {
        origins.stop_confirmed(account, &LifecycleRequest::new(&conn.identity, n));
    }
    match &p.kind {
        PendingKind::Ping => {
            conn.ping_misses.store(0, Ordering::Release);
        }
        PendingKind::AccountStart(acc, scope) => {
            let ok = result.is_ok();
            debug!(plugin = %conn.plugin_id, account = %acc, ok, "account.start answered");
            registry.update_start_metadata(&conn.plugin_id, acc, scope, |r| {
                if ok {
                    r.info.phase = AccountPhase::Running;
                } else {
                    r.info.phase = AccountPhase::Stopped;
                    r.info.wanted = false;
                    r.info.state = AccountState::Error;
                    r.info.detail = result.as_ref().err().map(|e| e.message.clone());
                }
            });
        }
        PendingKind::AccountStop(acc) | PendingKind::RemovalStop(acc) => {
            registry.update_pending(conn, acc, &p.owner, |r| {
                r.info.phase = AccountPhase::Stopped;
                r.info.wanted = false;
                r.info.state = AccountState::Disconnected;
                r.info.reconnecting = false;
            });
        }
        _ => {}
    }
    let _ = p.reply.send(result);
}

async fn handle_incoming(
    conn: &Arc<Conn>,
    registry: &Arc<Registry>,
    secrets: &Arc<dyn SecretStore>,
    deliver: &mpsc::Sender<Delivery>,
    id: Option<RequestId>,
    method: &str,
    params: Option<Value>,
) {
    let pid = &conn.plugin_id;
    let params = params.unwrap_or_else(|| Value::Object(Default::default()));
    if !params.is_object() {
        conn.respond(
            id,
            Err(ErrorObject::new(
                ErrorCode::InvalidParams,
                "params must be an object",
            )),
        );
        return;
    }
    if method == CANCEL_METHOD {
        // The only plugin→core work that could be cancelled is an acked event already queued;
        // it is answered when ingested regardless (§11.0: a cancel for a finished id is ignored).
        trace!(plugin = %pid, "plugin sent $/cancelRequest; ignored");
        return;
    }
    if !conn.initialized.load(Ordering::Acquire) {
        warn!(plugin = %pid, method, "message before initialize was answered (§4.1)");
    }
    match method {
        "secrets.set" | "secrets.delete" => {
            let Some(id) = id else {
                warn!(plugin = %pid, method, "sent as a notification; ignored");
                return;
            };
            let account_id = params["account_id"].as_str().map(str::to_owned);
            let key = params["key"].as_str().map(str::to_owned);
            let value = params["value"].as_str().map(str::to_owned);
            let (Some(account_id), Some(key)) = (account_id, key) else {
                conn.respond(
                    Some(id),
                    Err(ErrorObject::new(
                        ErrorCode::InvalidParams,
                        "account_id and key are required",
                    )),
                );
                return;
            };
            if registry
                .account_phase(pid, &account_id)
                .is_none_or(|p| p == AccountPhase::Stopped)
            {
                conn.respond(
                    Some(id),
                    Err(ErrorObject::new(
                        ErrorCode::UnknownAccount,
                        "account is not started",
                    )),
                );
                return;
            }
            let is_set = method == "secrets.set";
            if is_set && value.is_none() {
                conn.respond(
                    Some(id),
                    Err(
                        ErrorObject::new(ErrorCode::InvalidParams, "value is required")
                            .with_data(|d| d.field = Some("value".into())),
                    ),
                );
                return;
            }
            let (conn, secrets, pid) = (Arc::clone(conn), Arc::clone(secrets), pid.clone());
            tokio::spawn(async move {
                let r = if is_set {
                    secrets
                        .set(
                            &pid,
                            &account_id,
                            &key,
                            value.as_deref().unwrap_or_default(),
                        )
                        .await
                } else {
                    secrets.delete(&pid, &account_id, &key).await
                };
                let result = r
                    .map(|()| serde_json::to_value(Empty {}).expect("empty"))
                    .map_err(|e| ErrorObject::new(ErrorCode::Internal, e.to_string()));
                conn.respond(Some(id), result);
            });
        }
        name => match Event::from_params(name, params.clone()) {
            None => {
                if name.starts_with("x-") {
                    debug!(plugin = %pid, method = name, "vendor message ignored (§12.3)");
                } else {
                    warn!(plugin = %pid, method = name, "unknown method");
                }
                conn.respond(
                    id,
                    Err(ErrorObject::new(
                        ErrorCode::MethodNotFound,
                        format!("unknown method {name}"),
                    )),
                );
            }
            Some(Err(e)) => {
                warn!(plugin = %pid, event = name, "event params invalid: {e}");
                conn.respond(
                    id,
                    Err(ErrorObject::new(ErrorCode::InvalidParams, e.to_string())),
                );
            }
            Some(Ok(event)) => {
                let account_id = params["account_id"].as_str().unwrap_or_default();
                let phase = registry.account_phase(pid, account_id);
                if phase == Some(AccountPhase::Starting) {
                    // §4.2 says the reply comes first; tolerate the slip, the start was asked for.
                    debug!(plugin = %pid, account = account_id, event = name, "event before the account.start reply (§4.2)");
                }
                if !matches!(
                    phase,
                    Some(AccountPhase::Starting | AccountPhase::Running | AccountPhase::Stopping)
                ) {
                    debug!(plugin = %pid, account = account_id, event = name, "event for an account that is not started; dropped (§4.2)");
                    conn.respond(
                        id,
                        Err(ErrorObject::new(
                            ErrorCode::UnknownAccount,
                            "account is not started",
                        )),
                    );
                    return;
                }
                if let Event::TransferProgress(tp) = &event
                    && let Some(RequestId::Number(n)) = &tp.request_id
                    && let Some(p) = conn.pending().get_mut(n)
                {
                    // §4.6: each transfer.progress for a request resets its timeout.
                    p.deadline = Instant::now() + p.timeout;
                }
                #[cfg(target_os = "linux")]
                let origin = if let Some(origins) = &conn.origins {
                    let Some(origin) = origins.freeze(account_id) else {
                        conn.respond(id, Err(ErrorObject::new(ErrorCode::UnknownAccount, "account event has no current owner context")));
                        return;
                    };
                    Some(origin)
                } else { None };
                if let Event::Status(s) = &event {
                    let change = |r: &mut AccountRecord| {
                        r.info.state = s.state.clone();
                        r.info.detail = s.detail.clone();
                        r.info.reconnecting = s.reconnecting;
                        if s.me.is_some() {
                            r.info.me = s.me.clone();
                        }
                    };
                    #[cfg(target_os = "linux")]
                    if conn.origins.is_some() {
                        if let Some(origin) = &origin { registry.update_from_origin(pid, account_id, origin, change); }
                    } else { registry.update_account(pid, account_id, change); }
                    #[cfg(not(target_os = "linux"))]
                    registry.update_account(pid, account_id, change);
                }
                let item = Delivery {
                    #[cfg(target_os = "linux")]
                    origin,
                    event,
                    ack: id.map(|id| (id, Arc::clone(conn))),
                };
                // Bounded queue: when full, stop reading stdout until there is room (§16).
                match deliver.try_send(item) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(item)) => {
                        conn.stalled.store(true, Ordering::Release);
                        trace!(plugin = %pid, "event queue full; stalling stdout");
                        let r = deliver.send(item).await;
                        conn.stalled.store(false, Ordering::Release);
                        if r.is_err() {
                            warn!(plugin = %pid, "event queue closed");
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        warn!(plugin = %pid, "event queue closed");
                    }
                }
            }
        },
    }
}

async fn delivery_loop(
    plugin_id: String,
    target: DeliveryTarget,
    mut rx: mpsc::Receiver<Delivery>,
) {
    while let Some(item) = rx.recv().await {
        let name = item.event.name();
        #[cfg(target_os = "linux")]
        let r = if let Some(sink) = &target.owned {
            match item.origin {
                Some(origin) => sink.deliver_owned(&plugin_id, origin, item.event).await,
                None => Err("account event has no original owner context".into()),
            }
        } else { target.raw.deliver(&plugin_id, item.event).await };
        #[cfg(not(target_os = "linux"))]
        let r = target.raw.deliver(&plugin_id, item.event).await;
        if let Err(e) = &r {
            warn!(plugin = %plugin_id, event = name, "sink rejected event: {e}");
        }
        if let Some((id, conn)) = item.ack {
            let result = r
                .map(|()| serde_json::to_value(Empty {}).expect("empty"))
                .map_err(|e| ErrorObject::new(ErrorCode::InvalidParams, e));
            conn.respond(Some(id), result);
        }
    }
}
