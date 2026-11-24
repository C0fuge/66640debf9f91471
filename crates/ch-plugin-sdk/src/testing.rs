//! A fake core for plugin tests: drives a plugin over in-memory pipes ([`FakeCore::new`]) or
//! a spawned plugin process ([`FakeCore::spawn`]) and checks the wire on the way.
//!
//! Like the real core it fills `settings_schema` defaults from `plugin_dir/plugin.json` into
//! `account.start` (§6.1), acknowledges events sent as requests with `{}` (§12.2), and it
//! **panics** (failing the test) on anything the core would reject: a frame that is not one
//! JSON object per line, or an event for an account that is not started — before its
//! `account.start` reply or after its `account.stop` reply (§4.2).
//!
//! ```no_run
//! # use ch_plugin_sdk::prelude::*;
//! # use ch_plugin_sdk::testing::FakeCore;
//! # use serde_json::json;
//! # async fn t(plugin: Plugin) {
//! let mut core = FakeCore::new(plugin).plugin_dir(env!("CARGO_MANIFEST_DIR"));
//! core.start("a1", json!({})).await.unwrap();
//! assert_eq!(core.event("status").await["state"], "connected");
//! let sent = core
//!     .call("messages.send", json!({"account_id": "a1", "chat_id": "c1", "text": "hi"}))
//!     .await
//!     .unwrap();
//! assert_eq!(sent["message"]["text"], "hi");
//! let e = core.call("messages.edit", json!({})).await.unwrap_err();
//! assert_eq!(e.code(), ErrorCode::NotSupported);
//! core.shutdown().await;
//! # }
//! ```

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ch_protocol::{API_VERSION, CallDirection, ErrorObject};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::task::JoinHandle;

use crate::error::{PluginError, SdkError};
use crate::plugin::Plugin;

type Reader = BufReader<Box<dyn AsyncRead + Unpin + Send>>;
type Writer = Box<dyn AsyncWrite + Unpin + Send>;

enum Runner {
    Task(JoinHandle<Result<(), SdkError>>),
    Process(tokio::process::Child),
}

/// The core side of a plugin connection. See the [module docs](self).
pub struct FakeCore {
    reader: Reader,
    writer: Option<Writer>,
    runner: Option<Runner>,
    seen: VecDeque<Value>,
    next_id: i64,
    wait: Duration,
    plugin_dir: PathBuf,
    defaults: Map<String, Value>,
    auto_answer: bool,
    initialized: bool,
    started: HashSet<String>,
    /// Request id (as JSON) → account, for `account.start`/`account.stop` in flight.
    starting: HashMap<String, String>,
    stopping: HashMap<String, String>,
    dir: PathBuf,
}

impl FakeCore {
    /// Run `plugin` on in-memory pipes (needs a tokio runtime; use `#[tokio::test]`).
    pub fn new(plugin: Plugin) -> Self {
        let (core_w, plugin_r) = tokio::io::duplex(1 << 20);
        let (plugin_w, core_r) = tokio::io::duplex(1 << 20);
        let run = tokio::spawn(plugin.run(plugin_r, plugin_w));
        Self::with_io(Box::new(core_r), Box::new(core_w), Runner::Task(run))
    }

    /// Spawn a plugin process (stdin/stdout piped, killed on drop) and talk to it over stdio.
    /// `CH_API_VERSION` is set; set `CH_PLUGIN_ID`, `CH_DATA_DIR` and `XDG_*` yourself.
    pub fn spawn(cmd: std::process::Command) -> std::io::Result<Self> {
        let mut cmd = tokio::process::Command::from(cmd);
        cmd.env(ch_protocol::env::API_VERSION, API_VERSION)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd.spawn()?;
        let r = child.stdout.take().expect("piped stdout");
        let w = child.stdin.take().expect("piped stdin");
        Ok(Self::with_io(
            Box::new(r),
            Box::new(w),
            Runner::Process(child),
        ))
    }

    fn with_io(r: Box<dyn AsyncRead + Unpin + Send>, w: Writer, runner: Runner) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "ch-fake-core-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        Self {
            reader: BufReader::new(r),
            writer: Some(w),
            runner: Some(runner),
            seen: VecDeque::new(),
            next_id: 0,
            wait: Duration::from_secs(10),
            plugin_dir: PathBuf::from("/nonexistent"),
            defaults: Map::new(),
            auto_answer: false,
            initialized: false,
            started: HashSet::new(),
            starting: HashMap::new(),
            stopping: HashMap::new(),
            dir,
        }
    }

    /// The plugin directory sent in `initialize`; its `plugin.json` supplies settings defaults.
    pub fn plugin_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.plugin_dir = dir.as_ref().to_path_buf();
        self.defaults = crate::settings::manifest_defaults(&self.plugin_dir);
        self
    }

    /// How long to wait for any one expected frame before panicking. Default 10 s.
    pub fn timeout(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }

    /// Answer every plugin → core request (`secrets.set`, `secrets.delete`) with `{}` as it
    /// arrives; it is still visible to [`FakeCore::plugin_request`]. Default: off, answer
    /// them with [`FakeCore::answer`].
    pub fn answer_plugin_requests(mut self, on: bool) -> Self {
        self.auto_answer = on;
        self
    }

    /// The temp directory holding the accounts' `state_dir`s and `cache_dir`s (removed on drop).
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// `initialize` with this crate's API version.
    pub async fn initialize(&mut self) -> Result<Value, PluginError> {
        let params = json!({"api_version": API_VERSION, "core_version": "0.0.0-fake",
            "plugin_dir": self.plugin_dir, "locale": "en-US", "log_level": "debug"});
        self.call("initialize", params).await
    }

    /// `initialize` (once), then `account.start` for `account_id` with `settings` (manifest
    /// defaults filled in), no secrets, fresh state/cache dirs, ghost off.
    pub async fn start(&mut self, account_id: &str, settings: Value) -> Result<(), PluginError> {
        self.start_with(json!({"account_id": account_id, "settings": settings}))
            .await
    }

    /// Like [`FakeCore::start`] with your own `account.start` params; missing keys get the
    /// same values as there.
    pub async fn start_with(&mut self, mut params: Value) -> Result<(), PluginError> {
        if !self.initialized {
            self.initialize().await?;
        }
        let account = params["account_id"].as_str().unwrap_or_default().to_owned();
        let p = params
            .as_object_mut()
            .expect("account.start params are an object");
        let settings = match p.remove("settings") {
            Some(Value::Object(m)) => m,
            _ => Map::new(),
        };
        p.insert(
            "settings".into(),
            Value::Object(crate::settings::with_defaults(settings, &self.defaults)),
        );
        let base = self.dir.join(account.replace(['/', '\\'], "_"));
        for (key, sub) in [("state_dir", "state"), ("cache_dir", "cache")] {
            if !p.contains_key(key) {
                let d = base.join(sub);
                std::fs::create_dir_all(&d).expect("create account dir");
                p.insert(key.into(), json!(d));
            }
        }
        p.entry("secrets").or_insert_with(|| json!({}));
        p.entry("ghost").or_insert(json!(false));
        p.entry("auto_download").or_insert_with(|| {
            json!({"photos": false, "videos_max_bytes": 0, "files_max_bytes": 0, "voice": false})
        });
        self.call("account.start", params).await.map(drop)
    }

    /// `account.stop`.
    pub async fn stop(&mut self, account_id: &str) -> Result<(), PluginError> {
        self.call("account.stop", json!({"account_id": account_id}))
            .await
            .map(drop)
    }

    /// Send a request and wait for its answer: the `result`, or the `error` as a
    /// [`PluginError`].
    pub async fn call(&mut self, method: &str, params: Value) -> Result<Value, PluginError> {
        let id = self.request(method, params).await;
        let r = self.response(id).await;
        self.initialized |= method == "initialize" && r.is_ok();
        r
    }

    /// Send a request without waiting; returns its id for [`FakeCore::response`].
    pub async fn request(&mut self, method: &str, params: Value) -> i64 {
        self.next_id += 1;
        let id = self.next_id;
        let key = json!(id).to_string();
        let account = params["account_id"].as_str().map(str::to_owned);
        match (method, account) {
            ("account.start", Some(a)) => drop(self.starting.insert(key, a)),
            ("account.stop", Some(a)) => drop(self.stopping.insert(key, a)),
            _ => {}
        }
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await;
        id
    }

    /// The answer to request `id`.
    pub async fn response(&mut self, id: i64) -> Result<Value, PluginError> {
        let r = self
            .wait_for(&format!("the response to request {id}"), |v| {
                v.get("method").is_none() && v["id"] == id
            })
            .await;
        match r.get("error") {
            Some(e) => Err(serde_json::from_value::<ErrorObject>(e.clone())
                .unwrap_or_else(|e| panic!("malformed error object in {r}: {e}"))
                .into()),
            None => Ok(r["result"].clone()),
        }
    }

    /// Send a notification (`$/cancelRequest`, …).
    pub async fn notify(&mut self, method: &str, params: Value) {
        self.send(json!({"jsonrpc": "2.0", "method": method, "params": params}))
            .await;
    }

    /// `$/cancelRequest` for request `id`.
    pub async fn cancel(&mut self, id: i64) {
        self.notify(ch_protocol::CANCEL_METHOD, json!({"id": id}))
            .await;
    }

    /// The params of the first event called `name` (sent either way), seen earlier or next.
    pub async fn event(&mut self, name: &str) -> Value {
        self.event_frame(name).await["params"].take()
    }

    /// Like [`FakeCore::event`], but the whole frame: it has an `id` if it was sent as a
    /// request (and already acknowledged).
    pub async fn event_frame(&mut self, name: &str) -> Value {
        self.wait_for(&format!("event {name}"), |v| {
            v["method"] == name && !is_plugin_request(v)
        })
        .await
    }

    /// The first plugin → core request for `method` (`secrets.set`, …), as the whole frame.
    /// Answer it with [`FakeCore::answer`] unless [`FakeCore::answer_plugin_requests`] is on.
    pub async fn plugin_request(&mut self, method: &str) -> Value {
        self.wait_for(&format!("plugin request {method}"), |v| {
            v["method"] == method && is_plugin_request(v)
        })
        .await
    }

    /// Answer a plugin → core request (the frame [`FakeCore::plugin_request`] returned).
    pub async fn answer(&mut self, request: &Value, result: Result<Value, ErrorObject>) {
        let id = request["id"].clone();
        let frame = match result {
            Ok(r) => json!({"jsonrpc": "2.0", "id": id, "result": r}),
            Err(e) => json!({"jsonrpc": "2.0", "id": id, "error": e}),
        };
        self.send(frame).await;
    }

    /// The next frame in arrival order (earlier unclaimed frames first), or `None` if
    /// nothing arrives within `wait` or the plugin closed stdout.
    pub async fn next_frame(&mut self, wait: Duration) -> Option<Value> {
        match self.seen.pop_front() {
            Some(v) => Some(v),
            None => self.read(wait).await,
        }
    }

    /// Frames received but not yet claimed by a `wait`-style method, oldest first.
    pub fn unclaimed(&self) -> &VecDeque<Value> {
        &self.seen
    }

    /// Write raw bytes to the plugin's stdin (malformed-frame tests).
    pub async fn send_raw(&mut self, bytes: &[u8]) {
        let w = self.writer.as_mut().expect("stdin already closed");
        w.write_all(bytes).await.expect("write to plugin");
        w.flush().await.expect("flush to plugin");
    }

    /// Write one JSON value as a frame.
    pub async fn send(&mut self, v: Value) {
        let mut line = serde_json::to_vec(&v).expect("serialise frame");
        line.push(b'\n');
        self.send_raw(&line).await;
    }

    /// Close the plugin's stdin (EOF, §2.5). Frames can still be read afterwards.
    pub async fn close_stdin(&mut self) {
        if let Some(mut w) = self.writer.take() {
            let _ = w.shutdown().await;
        }
    }

    /// `shutdown`, then wait for the plugin to exit; panics unless it answers `{}` and exits
    /// cleanly within 5 s.
    pub async fn shutdown(mut self) {
        let r = self.call("shutdown", json!({})).await;
        assert_eq!(r, Ok(json!({})), "shutdown");
        self.wait_exit().await.expect("clean exit after shutdown");
    }

    /// Close stdin and wait up to 5 s for the plugin to exit; `Err` says why it did not
    /// exit cleanly (an error from [`Plugin::run`], a non-zero status, or a hang).
    pub async fn wait_exit(mut self) -> Result<(), String> {
        self.close_stdin().await;
        let limit = Duration::from_secs(ch_protocol::EXIT_GRACE_SECONDS);
        match self.runner.take().expect("runner") {
            Runner::Task(run) => match tokio::time::timeout(limit, run).await {
                Err(_) => Err("the plugin did not return within 5 s".into()),
                Ok(Err(e)) => Err(format!("the plugin task failed: {e}")),
                Ok(Ok(r)) => r.map_err(|e| e.to_string()),
            },
            Runner::Process(mut child) => match tokio::time::timeout(limit, child.wait()).await {
                Err(_) => Err("the plugin did not exit within 5 s".into()),
                Ok(Err(e)) => Err(format!("waiting for the plugin: {e}")),
                Ok(Ok(s)) if s.success() => Ok(()),
                Ok(Ok(s)) => Err(format!("the plugin exited with {s}")),
            },
        }
    }

    async fn wait_for(&mut self, what: &str, pred: impl Fn(&Value) -> bool) -> Value {
        if let Some(i) = self.seen.iter().position(&pred) {
            return self.seen.remove(i).expect("index in range");
        }
        loop {
            let Some(v) = self.read(self.wait).await else {
                panic!(
                    "timed out waiting for {what}; unclaimed frames: {:?}",
                    self.seen
                );
            };
            if pred(&v) {
                return v;
            }
            self.seen.push_back(v);
        }
    }

    /// Read and check one frame; acknowledge acked events and (if enabled) plugin requests.
    async fn read(&mut self, wait: Duration) -> Option<Value> {
        let mut line = String::new();
        let n = match tokio::time::timeout(wait, self.reader.read_line(&mut line)).await {
            Err(_) => return None,
            Ok(r) => r.expect("read from plugin stdout"),
        };
        if n == 0 {
            return None;
        }
        assert!(line.ends_with('\n'), "frame without a newline: {line:?}");
        let v: Value = serde_json::from_str(&line)
            .unwrap_or_else(|e| panic!("plugin wrote a non-JSON line ({e}): {line:?}"));
        assert!(v.is_object(), "a frame must be one JSON object: {v}");
        assert_eq!(v["jsonrpc"], "2.0", "{v}");
        self.track(&v).await;
        Some(v)
    }

    async fn track(&mut self, v: &Value) {
        let Some(method) = v["method"].as_str() else {
            // A response: account bookkeeping for start/stop.
            let key = v["id"].to_string();
            let ok = v.get("result").is_some();
            if let Some(a) = self.starting.remove(&key)
                && ok
            {
                self.started.insert(a);
            }
            if let Some(a) = self.stopping.remove(&key)
                && ok
            {
                self.started.remove(&a);
            }
            return;
        };
        if is_plugin_request(v) {
            if self.auto_answer {
                self.answer(v, Ok(json!({}))).await;
            }
            return;
        }
        if method.starts_with("x-") || method.starts_with("$/") {
            return; // vendor events (the core ignores them, §12.3) and `$/cancelRequest`
        }
        let account = v["params"]["account_id"].as_str().unwrap_or_default();
        assert!(
            self.started.contains(account),
            "event {method} for account {account:?}, which is not started (spec §4.2): {v}"
        );
        if v.get("id").is_some() {
            let id = v["id"].clone();
            self.send(json!({"jsonrpc": "2.0", "id": id, "result": {}}))
                .await;
        }
    }
}

/// A plugin → core method call (not an event sent as a request).
fn is_plugin_request(v: &Value) -> bool {
    v.get("id").is_some()
        && v["method"]
            .as_str()
            .and_then(ch_protocol::method)
            .is_some_and(|m| m.direction == CallDirection::PluginToCore)
}

impl Drop for FakeCore {
    fn drop(&mut self) {
        if let Some(Runner::Process(child)) = &mut self.runner {
            let _ = child.start_kill();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
