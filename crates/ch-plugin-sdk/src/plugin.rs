//! The plugin definition ([`PluginBuilder`]) and the protocol loop ([`Plugin::run`]).

use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ch_protocol::methods::{
    AccountParams, AccountStartParams, InitializeParams, InitializeResult, PingResult,
};
use ch_protocol::{
    API_VERSION, ApiVersion, CANCEL_METHOD, CallDirection, CancelParams, Capabilities,
    DEFAULT_TIMEOUT_SECONDS, ErrorCode, ErrorObject, METHODS, REQUIRED_METHODS, RequestId,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::error::{self, PluginError, SdkError};
use crate::host::{Account, AccountInner, Call, Host, Shared, lock};
use crate::transport::{FrameReader, Incoming, stdin_reader, writer_task};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type Handler = Arc<dyn Fn(Call<Value>) -> BoxFuture<Result<Value, PluginError>> + Send + Sync>;
type InitHook = Arc<dyn Fn(&InitializeParams) -> Result<(), PluginError> + Send + Sync>;
type StartHook = Arc<dyn Fn(Account) -> BoxFuture<Result<(), PluginError>> + Send + Sync>;
type StopHook = Arc<dyn Fn(Account) -> BoxFuture<()> + Send + Sync>;

/// Methods the SDK answers itself; registering a handler for one is a definition error.
const BUILT_IN: &[&str] = &[
    "initialize",
    "shutdown",
    "ping",
    "account.start",
    "account.stop",
];

/// How long `shutdown` waits for every account's stop hook before answering anyway (its own
/// timeout is 10 s, spec §4.6).
const SHUTDOWN_STOP_BUDGET: Duration = Duration::from_secs(8);
/// How long EOF on stdin waits for in-flight requests to finish and be answered (§2.5).
const EOF_DRAIN_BUDGET: Duration = Duration::from_secs(3);
/// How long EOF on stdin then waits for stop hooks (the process must exit within 5 s, §2.5).
const EOF_STOP_BUDGET: Duration = Duration::from_millis(1500);

/// Builds a [`Plugin`]: its version, capabilities, lifecycle hooks and method handlers.
///
/// The SDK itself answers `initialize` (API version and capabilities), `ping` (from a task no
/// platform I/O can block), `shutdown`, and `account.start`/`account.stop` (bookkeeping,
/// `status` events, the "no events after stop" gate); your hooks run inside the last two.
///
/// ```no_run
/// use ch_plugin_sdk::prelude::*;
///
/// # async fn f() -> Result<(), SdkError> {
/// PluginBuilder::new()
///     .version("0.1.0")
///     .on_account_start(|acct: Account| async move { acct.connected(None)?; Ok(()) })
///     .method("messages.fetch_recent", |_c: Call<MessagesFetchRecentParams>| async move {
///         Ok(PagedMessagesResult::default())
///     })
///     .method("messages.send", |_c: Call<MessagesSendParams>| async move {
///         Err::<MessageResult, _>(error::flood_wait(30))
///     })
///     .build()?
///     .run_stdio()
///     .await
/// # }
/// ```
pub struct PluginBuilder {
    version: Option<String>,
    caps: Capabilities,
    handlers: HashMap<String, Handler>,
    on_init: Option<InitHook>,
    on_start: Option<StartHook>,
    on_stop: Option<StopHook>,
    cancel_grace: Duration,
}

impl Default for PluginBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginBuilder {
    /// An empty definition.
    pub fn new() -> Self {
        Self {
            version: None,
            caps: Capabilities::new(),
            handlers: HashMap::new(),
            on_init: None,
            on_start: None,
            on_stop: None,
            cancel_grace: Duration::from_secs(2),
        }
    }

    /// The plugin's own version, reported as `plugin_version` in `initialize`.
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Declare a capability with default parameters (spec §14). Every method it gates must
    /// have a handler, or [`PluginBuilder::build`] fails.
    pub fn capability(mut self, key: impl AsRef<str>) -> Self {
        self.caps.enable(key);
        self
    }

    /// Declare a capability with a parameter object (`{"max": 10}`, or a typed
    /// `ch_protocol::capabilities` params struct).
    pub fn capability_with<T: Serialize>(mut self, key: impl AsRef<str>, params: &T) -> Self {
        self.caps.enable_with(key, params);
        self
    }

    /// Runs on `initialize` after the SDK has parsed the core's params. Return an error only
    /// for reasons other than the API version (an unusable `plugin_dir`, say): a different
    /// major is answered normally and the core decides (§4.1, §18); the SDK logs a warning.
    pub fn on_initialize<F>(mut self, f: F) -> Self
    where
        F: Fn(&InitializeParams) -> Result<(), PluginError> + Send + Sync + 'static,
    {
        self.on_init = Some(Arc::new(f));
        self
    }

    /// Runs inside `account.start`. It MUST return fast, without waiting for the network
    /// (§4.2): spawn the connection on a task holding the [`Account`] and report through
    /// `status`. Return `INVALID_PARAMS` (see [`crate::Settings`]) for unusable settings.
    ///
    /// Events emitted before the `{}` response (from the hook or from tasks it spawned) are
    /// queued and sent right after it, in order (§4.2: no event before the reply); if none of
    /// them is a `status`, the SDK puts `status: connecting` first. So do not await
    /// [`Account::emit_acked`] here: its request only goes out after the hook returns.
    pub fn on_account_start<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Account) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), PluginError>> + Send + 'static,
    {
        self.on_start = Some(Arc::new(move |a| Box::pin(f(a))));
        self
    }

    /// Runs inside `account.stop` (and for every account on `shutdown`/EOF), after the
    /// account's token has been cancelled. Close sockets and flush the session here; events
    /// can still be emitted until it returns. Then the SDK emits `status: disconnected` and
    /// refuses every later event for the account.
    pub fn on_account_stop<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Account) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_stop = Some(Arc::new(move |a| Box::pin(f(a))));
        self
    }

    /// Handle core → plugin method `name` with typed params `P` and result `R`, normally the
    /// `ch_protocol::methods` types (`MessagesSendParams` → `MessagesSendResult`).
    ///
    /// Before the handler runs, the SDK answers: `NOT_SUPPORTED` when the method's capability
    /// (from [`ch_protocol::METHODS`]) is not declared, or when a capability-gated parameter
    /// (the registry's `gated_params`, §14.2: `silent`, `schedule_at`, `reply_to.quote_text`,
    /// …) is present without its capability; `UNKNOWN_ACCOUNT` when `account_id` is not
    /// started; `INVALID_PARAMS` when the params do not decode. A handler that panics yields
    /// `INTERNAL`. `R` must serialise to an object; `()` becomes `{}`.
    ///
    /// A name the registry does not list is a vendor method and must be
    /// `x-<plugin id>.<name>` (§12.3); [`PluginBuilder::build`] rejects anything else.
    pub fn method<P, R, F, Fut>(mut self, name: &str, f: F) -> Self
    where
        P: DeserializeOwned + Send + 'static,
        R: Serialize + Send + 'static,
        F: Fn(Call<P>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, PluginError>> + Send + 'static,
    {
        let h: Handler = Arc::new(
            move |call: Call<Value>| -> BoxFuture<Result<Value, PluginError>> {
                match call.try_map(|v| serde_json::from_value::<P>(v)) {
                    Err(e) => Box::pin(std::future::ready(Err(params_error(&e)))),
                    Ok(call) => {
                        let fut = f(call);
                        Box::pin(async move { result_object(fut.await?) })
                    }
                }
            },
        );
        self.handlers.insert(name.to_owned(), h);
        self
    }

    /// How long a cancelled handler may keep running (to clean up, or to return a result it
    /// already has) before the SDK drops it and answers `CANCELLED`. Default 2 s.
    pub fn cancel_grace(mut self, grace: Duration) -> Self {
        self.cancel_grace = grace;
        self
    }

    /// Check the definition against the spec's rules and build the plugin:
    /// - the required methods `messages.fetch_recent` and `messages.send` have handlers (§18);
    /// - every declared capability has a handler for every method it gates (§14.2);
    /// - no handler for a built-in or a plugin → core method;
    /// - every method the registry does not list is named `x-<plugin id>.<name>` (§12.3; a
    ///   different id than `CH_PLUGIN_ID` only logs a warning).
    pub fn build(self) -> Result<Plugin, SdkError> {
        let mut problems = Vec::new();
        for m in REQUIRED_METHODS.iter().filter(|m| !BUILT_IN.contains(m)) {
            if !self.handlers.contains_key(*m) {
                problems.push(format!("required method {m} has no handler"));
            }
        }
        for info in METHODS
            .iter()
            .filter(|i| i.direction == CallDirection::CoreToPlugin)
        {
            if let Some(cap) = info.capability
                && self.caps.has(cap)
                && !self.handlers.contains_key(info.name)
            {
                problems.push(format!(
                    "capability {cap} is declared but {} has no handler",
                    info.name
                ));
            }
        }
        for name in self.handlers.keys() {
            if BUILT_IN.contains(&name.as_str()) {
                problems.push(format!(
                    "{name} is handled by the SDK; use the lifecycle hooks"
                ));
            } else if ch_protocol::method(name)
                .is_some_and(|i| i.direction == CallDirection::PluginToCore)
            {
                problems.push(format!("{name} is a plugin → core method"));
            } else if ch_protocol::method(name).is_none() {
                match vendor_prefix(name) {
                    None => problems.push(format!(
                        "{name} is not an API method; a vendor method must be named \
                         x-<plugin id>.<name> (spec §12.3)"
                    )),
                    Some(id) => {
                        if let Ok(own) = std::env::var(ch_protocol::env::PLUGIN_ID)
                            && own != id
                        {
                            tracing::warn!(
                                "vendor method {name} does not use this plugin's id {own:?}"
                            );
                        }
                    }
                }
            }
        }
        if !problems.is_empty() {
            problems.sort();
            return Err(SdkError::Definition(problems.join("; ")));
        }
        Ok(Plugin {
            def: Arc::new(self),
        })
    }
}

/// The plugin id of a vendor method name `x-<id>.<name>` (`^x-[a-z][a-z0-9_]{1,31}\.`).
fn vendor_prefix(name: &str) -> Option<&str> {
    let (id, rest) = name.strip_prefix("x-")?.split_once('.')?;
    let mut chars = id.chars();
    let ok = chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && (2..=32).contains(&id.len())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !rest.is_empty();
    ok.then_some(id)
}

fn params_error(e: &serde_json::Error) -> PluginError {
    error::invalid_params("params", format!("invalid params: {e}"))
}

fn result_object<R: Serialize>(r: R) -> Result<Value, PluginError> {
    match serde_json::to_value(r) {
        Ok(Value::Null) => Ok(Value::Object(Map::new())),
        Ok(v @ Value::Object(_)) => Ok(v),
        Ok(_) => Err(error::internal("handler result is not a JSON object")),
        Err(e) => Err(error::internal(format!("result does not serialise: {e}"))),
    }
}

/// Run a hook or handler on its own task so that a panic becomes `INTERNAL`.
async fn guarded<T: Send + 'static>(
    fut: BoxFuture<Result<T, PluginError>>,
) -> Result<T, PluginError> {
    tokio::spawn(fut)
        .await
        .unwrap_or_else(|e| Err(panicked(&e)))
}

fn panicked(e: &tokio::task::JoinError) -> PluginError {
    tracing::error!("handler failed: {e}");
    error::internal("handler panicked")
}

/// A built plugin. Run it on stdio with [`Plugin::run_stdio`], or on any pipe pair with
/// [`Plugin::run`] (tests).
pub struct Plugin {
    def: Arc<PluginBuilder>,
}

struct Ctx {
    def: Arc<PluginBuilder>,
    host: Host,
    /// Every task answering a request, so EOF can let them finish (§2.5).
    tasks: Tasks,
}

/// Counts the tasks answering requests; [`Tasks::idle`] resolves when none is left.
#[derive(Clone, Default)]
struct Tasks(Arc<(AtomicUsize, Notify)>);

struct TaskGuard(Tasks);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if (self.0).0.0.fetch_sub(1, Ordering::SeqCst) == 1 {
            (self.0).0.1.notify_waiters();
        }
    }
}

impl Tasks {
    fn spawn(&self, fut: impl Future<Output = ()> + Send + 'static) {
        self.0.0.fetch_add(1, Ordering::SeqCst);
        let guard = TaskGuard(self.clone());
        tokio::spawn(async move {
            let _guard = guard;
            fut.await;
        });
    }

    async fn idle(&self) {
        loop {
            let notified = self.0.1.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.0.0.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }
}

impl Ctx {
    fn shared(&self) -> &Arc<Shared> {
        &self.host.0
    }
}

impl Plugin {
    /// Serve the protocol on the process's stdin/stdout (spec §2.1). Returns after `shutdown`
    /// has been answered, or on EOF; the caller then exits with status 0. Log to stderr only
    /// (see [`crate::init_logging`]).
    pub async fn run_stdio(self) -> Result<(), SdkError> {
        self.run(stdin_reader(), tokio::io::stdout()).await
    }

    /// Serve the protocol on `reader` (the core's requests) and `writer` (to the core).
    ///
    /// Returns `Ok` after `shutdown` has been answered and flushed, or on EOF: requests in
    /// flight get 3 s to finish and be answered (the rest are dropped), then every account is
    /// stopped (§2.5). A frame over 16 MiB is fatal (§2.2).
    pub async fn run<R, W>(self, reader: R, writer: W) -> Result<(), SdkError>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let writer = tokio::spawn(writer_task(writer, rx));
        let ctx = Arc::new(Ctx {
            host: Host(Arc::new(Shared::new(tx, self.def.caps.clone()))),
            def: self.def,
            tasks: Tasks::default(),
        });
        let mut reader = FrameReader::new(BufReader::new(reader));
        let (done_tx, mut done_rx) = oneshot::channel::<()>();
        let mut done_tx = Some(done_tx);
        let mut initialized = false;

        let outcome = loop {
            tokio::select! {
                biased;
                _ = &mut done_rx => break Ok(()),
                next = reader.next() => match next {
                    Ok(Incoming::Frame(f)) => ctx.handle_frame(&f, &mut initialized, &mut done_tx),
                    Ok(Incoming::Eof) => {
                        tracing::info!("stdin closed; finishing requests in flight, then stopping");
                        ctx.shared().peer_gone();
                        tokio::select! {
                            _ = ctx.tasks.idle() => {}
                            _ = &mut done_rx => {}
                            _ = tokio::time::sleep(EOF_DRAIN_BUDGET) => {
                                tracing::warn!("requests still running after {EOF_DRAIN_BUDGET:?}; dropping them");
                            }
                        }
                        ctx.stop_all(EOF_STOP_BUDGET).await;
                        break Ok(());
                    }
                    Ok(Incoming::TooLong) => {
                        tracing::error!("incoming frame exceeds 16 MiB; closing");
                        ctx.stop_all(EOF_STOP_BUDGET).await;
                        break Err(SdkError::FrameTooLarge);
                    }
                    Err(e) => {
                        ctx.stop_all(EOF_STOP_BUDGET).await;
                        break Err(SdkError::Io(e));
                    }
                },
            }
        };
        let shared = ctx.shared();
        shared.root.cancel();
        lock(&shared.pending).clear();
        shared.close().await;
        let _ = writer.await;
        outcome
    }
}

impl Ctx {
    fn reply(&self, id: Option<RequestId>, result: Result<Value, PluginError>) {
        match id {
            Some(id) => self.shared().respond(id, result),
            None => {
                let e = error::normalize(result.err().unwrap_or_else(|| error::internal("?")));
                let _ = self
                    .shared()
                    .send(&ch_protocol::Response::<Value>::err(None, e));
            }
        }
    }

    fn handle_frame(
        self: &Arc<Self>,
        frame: &[u8],
        initialized: &mut bool,
        done: &mut Option<oneshot::Sender<()>>,
    ) {
        let v: Value = match serde_json::from_slice(frame) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("unparseable frame: {e}");
                return self.reply(
                    None,
                    Err(
                        ErrorObject::new(ErrorCode::ParseError, format!("parse error: {e}")).into(),
                    ),
                );
            }
        };
        let Value::Object(mut obj) = v else {
            return self.reply(
                None,
                Err(error::invalid_request(
                    "a frame must be one JSON-RPC object (no batches)",
                )),
            );
        };
        let id = match obj.remove("id") {
            None => None,
            Some(v) => match serde_json::from_value::<RequestId>(v) {
                Ok(id) => Some(id),
                Err(_) if !obj.contains_key("method") => {
                    tracing::warn!(
                        "core reported an error without an id: {:?}",
                        obj.get("error")
                    );
                    return;
                }
                Err(_) => {
                    return self.reply(
                        None,
                        Err(error::invalid_request("id must be a number or a string")),
                    );
                }
            },
        };
        match obj.remove("method") {
            Some(Value::String(method)) => {
                let params = match obj.remove("params") {
                    None | Some(Value::Null) => Value::Object(Map::new()),
                    Some(p @ Value::Object(_)) => p,
                    Some(_) => {
                        if id.is_some() {
                            self.reply(
                                id,
                                Err(error::invalid_params("params", "params must be an object")),
                            );
                        }
                        return;
                    }
                };
                match id {
                    Some(id) => self.dispatch(id, method, params, initialized, done),
                    None if !*initialized => {
                        tracing::warn!("ignoring notification {method} before initialize");
                    }
                    None => self.notification(&method, params),
                }
            }
            Some(_) => self.reply(id, Err(error::invalid_request("method must be a string"))),
            None => match (id, obj.remove("error"), obj.remove("result")) {
                (Some(id), Some(e), _) => {
                    let e = serde_json::from_value::<ErrorObject>(e)
                        .map(PluginError::from)
                        .unwrap_or_else(|_| error::internal("malformed error object"));
                    self.shared().complete(&id, Err(e));
                }
                (Some(id), None, Some(r)) => self.shared().complete(&id, Ok(r)),
                (id, _, _) => self.reply(
                    id,
                    Err(error::invalid_request(
                        "not a request, notification or response",
                    )),
                ),
            },
        }
    }

    fn notification(&self, method: &str, params: Value) {
        if method == CANCEL_METHOD {
            match serde_json::from_value::<CancelParams>(params) {
                Ok(p) => {
                    if let Some(t) = lock(&self.shared().inflight).get(&p.id) {
                        t.cancel();
                    }
                }
                Err(e) => tracing::warn!("bad $/cancelRequest: {e}"),
            }
        } else {
            tracing::debug!("ignoring notification {method}");
        }
    }

    fn dispatch(
        self: &Arc<Self>,
        id: RequestId,
        method: String,
        params: Value,
        initialized: &mut bool,
        done: &mut Option<oneshot::Sender<()>>,
    ) {
        if method == "initialize" {
            if *initialized {
                return self.reply(
                    Some(id),
                    Err(error::invalid_request("initialize was already answered")),
                );
            }
            let r = self.initialize(params);
            *initialized = r.is_ok();
            return self.reply(Some(id), r);
        }
        if !*initialized {
            return self.reply(
                Some(id),
                Err(error::invalid_request(
                    "initialize must be the first request",
                )),
            );
        }
        match method.as_str() {
            "ping" => {
                let busy = self.shared().busy.load(Ordering::Relaxed).then_some(true);
                self.reply(Some(id), result_object(PingResult { busy }));
            }
            "shutdown" => {
                let Some(done) = done.take() else {
                    return self.reply(Some(id), Ok(Value::Object(Map::new())));
                };
                let ctx = self.clone();
                self.tasks.spawn(async move {
                    ctx.stop_all(SHUTDOWN_STOP_BUDGET).await;
                    ctx.reply(Some(id), Ok(Value::Object(Map::new())));
                    let _ = done.send(());
                });
            }
            "account.start" => {
                let ctx = self.clone();
                self.tasks
                    .spawn(async move { ctx.account_start(id, params).await });
            }
            "account.stop" => {
                let ctx = self.clone();
                self.tasks.spawn(async move {
                    let r = match serde_json::from_value::<AccountParams>(params) {
                        Err(e) => Err(params_error(&e)),
                        Ok(p) => match lock(&ctx.shared().accounts).remove(&p.account_id) {
                            None => Err(error::unknown_account(&p.account_id)),
                            Some(inner) => Ok(inner),
                        },
                    };
                    match r {
                        Ok(inner) => {
                            ctx.stop_account(inner).await;
                            ctx.reply(Some(id), Ok(Value::Object(Map::new())));
                        }
                        Err(e) => ctx.reply(Some(id), Err(e)),
                    }
                });
            }
            _ => self.call(id, method, params),
        }
    }

    fn initialize(&self, params: Value) -> Result<Value, PluginError> {
        let p: InitializeParams = serde_json::from_value(params).map_err(|e| params_error(&e))?;
        match ApiVersion::parse(&p.api_version) {
            Ok(v) if v.is_compatible_with(&ApiVersion::current()) => {}
            _ => tracing::warn!(
                "core speaks API {}, this plugin {API_VERSION}; the core decides",
                p.api_version
            ),
        }
        if let Some(hook) = &self.def.on_init {
            hook(&p)?;
        }
        let defaults = crate::settings::manifest_defaults(Path::new(&p.plugin_dir));
        let _ = self.shared().setting_defaults.set(defaults);
        let _ = self.shared().init.set(p);
        result_object(InitializeResult {
            api_version: API_VERSION.to_owned(),
            capabilities: self.def.caps.clone(),
            plugin_version: self.def.version.clone(),
        })
    }

    async fn account_start(self: Arc<Self>, id: RequestId, params: Value) {
        let shared = self.shared();
        let p: AccountStartParams = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return self.reply(Some(id), Err(params_error(&e))),
        };
        if p.account_id.is_empty() {
            return self.reply(
                Some(id),
                Err(error::invalid_params("account_id", "account_id is empty")),
            );
        }
        let inner = {
            let mut accounts = lock(&shared.accounts);
            if accounts.contains_key(&p.account_id) {
                drop(accounts);
                let msg = format!("account {:?} is already started", p.account_id);
                return self.reply(Some(id), Err(error::invalid_params("account_id", msg)));
            }
            let defaults = shared.setting_defaults.get().cloned().unwrap_or_default();
            let inner = Arc::new(AccountInner::new(p, &defaults, &shared.root));
            accounts.insert(inner.params.account_id.clone(), inner.clone());
            inner
        };
        let account = Account {
            inner: inner.clone(),
            host: self.host.clone(),
        };
        let r = match &self.def.on_start {
            Some(hook) => guarded(hook(account)).await,
            None => Ok(()),
        };
        match r {
            Ok(()) => {
                self.reply(Some(id), Ok(Value::Object(Map::new())));
                inner.open(shared);
            }
            Err(e) => {
                let mut accounts = lock(&shared.accounts);
                if accounts
                    .get(&inner.params.account_id)
                    .is_some_and(|a| Arc::ptr_eq(a, &inner))
                {
                    accounts.remove(&inner.params.account_id);
                }
                drop(accounts);
                inner.token.cancel();
                inner.close_silently();
                self.reply(Some(id), Err(e));
            }
        }
    }

    /// Stop one account that has already been removed from the map.
    async fn stop_account(&self, inner: Arc<AccountInner>) {
        inner.token.cancel();
        if let Some(hook) = &self.def.on_stop {
            let account = Account {
                inner: inner.clone(),
                host: self.host.clone(),
            };
            let fut = hook(account);
            if let Err(e) = tokio::spawn(fut).await {
                tracing::error!("account stop hook failed: {e}");
            }
        }
        inner.close(self.shared());
    }

    async fn stop_all(self: &Arc<Self>, budget: Duration) {
        let all: Vec<_> = lock(&self.shared().accounts)
            .drain()
            .map(|(_, a)| a)
            .collect();
        let mut set = JoinSet::new();
        for inner in all.iter().cloned() {
            let ctx = self.clone();
            set.spawn(async move { ctx.stop_account(inner).await });
        }
        if tokio::time::timeout(budget, set.join_all()).await.is_err() {
            tracing::warn!("account stop hooks did not finish in {budget:?}");
            for inner in &all {
                inner.close(self.shared());
            }
        }
    }

    /// Any method other than the built-ins.
    fn call(self: &Arc<Self>, id: RequestId, method: String, params: Value) {
        let shared = self.shared();
        let info = ch_protocol::method(&method);
        if let Some(i) = info {
            if i.direction == CallDirection::PluginToCore {
                return self.reply(Some(id), Err(method_not_found(&method)));
            }
            if let Some(cap) = i.capability
                && !shared.caps.has(cap)
            {
                return self.reply(Some(id), Err(error::not_supported(cap)));
            }
            if let Some(g) = i
                .missing_capabilities(&params, |c| shared.caps.has(c))
                .next()
            {
                return self.reply(Some(id), Err(error::not_supported(g.capability)));
            }
        }
        let Some(handler) = self.def.handlers.get(&method).cloned() else {
            let e = match info {
                Some(i) => error::not_supported(i.capability.unwrap_or(i.name)),
                None => method_not_found(&method),
            };
            return self.reply(Some(id), Err(e));
        };
        let account = match params.get("account_id") {
            Some(Value::String(a)) => match lock(&shared.accounts).get(a).cloned() {
                Some(inner) => Some(Account {
                    inner,
                    host: self.host.clone(),
                }),
                None => return self.reply(Some(id), Err(error::unknown_account(a))),
            },
            None if info.is_none() => None,
            _ => {
                return self.reply(
                    Some(id),
                    Err(error::invalid_params(
                        "account_id",
                        "account_id must be a string",
                    )),
                );
            }
        };
        if method == "ghost.apply"
            && let (Some(a), Some(Value::Bool(on))) = (&account, params.get("enabled"))
        {
            a.inner.ghost.store(*on, Ordering::Relaxed);
        }
        let token = match &account {
            Some(a) => a.inner.token.child_token(),
            None => shared.root.child_token(),
        };
        let timeout = Duration::from_secs(
            info.map_or(DEFAULT_TIMEOUT_SECONDS, |i| i.timeout_seconds)
                .into(),
        );
        let reset = Arc::new(Notify::new());
        lock(&shared.inflight).insert(id.clone(), token.clone());
        let call = Call {
            params,
            id: id.clone(),
            method,
            host: self.host.clone(),
            account,
            token: token.clone(),
            reset: reset.clone(),
        };
        let ctx = self.clone();
        self.tasks.spawn(async move {
            let result =
                supervise(handler(call), &token, &reset, timeout, ctx.def.cancel_grace).await;
            lock(&ctx.shared().inflight).remove(&id);
            ctx.reply(Some(id), result);
        });
    }
}

fn method_not_found(method: &str) -> PluginError {
    ErrorObject::new(
        ErrorCode::MethodNotFound,
        format!("unknown method {method}"),
    )
    .into()
}

/// Drive a handler: answer its result; on cancellation give it `grace` to finish, then drop it
/// and answer `CANCELLED`; on the method's timeout (reset by progress) cancel it the same way
/// and answer `TIMEOUT`.
async fn supervise(
    fut: BoxFuture<Result<Value, PluginError>>,
    token: &CancellationToken,
    reset: &Notify,
    timeout: Duration,
    grace: Duration,
) -> Result<Value, PluginError> {
    let mut task = tokio::spawn(fut);
    let mut deadline = tokio::time::Instant::now() + timeout;
    let mut timed_out = false;
    loop {
        tokio::select! {
            r = &mut task => return r.unwrap_or_else(|e| Err(panicked(&e))),
            _ = token.cancelled() => break,
            _ = reset.notified() => deadline = tokio::time::Instant::now() + timeout,
            _ = tokio::time::sleep_until(deadline) => {
                timed_out = true;
                token.cancel();
                break;
            }
        }
    }
    match tokio::time::timeout(grace, &mut task).await {
        Ok(r) => r.unwrap_or_else(|e| Err(panicked(&e))),
        Err(_) => {
            task.abort();
            Err(if timed_out {
                error::timeout("request timed out")
            } else {
                error::cancelled()
            })
        }
    }
}
