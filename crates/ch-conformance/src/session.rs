//! A raw stdio session with one plugin process: the suite's own transport.
//!
//! `ch_core::host` is deliberately lenient (it drops events for unknown accounts, restarts
//! on stdout noise, hides frames behind typed channels). The suite needs the opposite: every
//! byte the plugin writes to stdout, in order, with a sequence number, so that checks can
//! ask "was there an event before the `account.start` reply?" or "what else was on stdout?".
//! So this module spawns the process itself and records everything.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ch_protocol::Manifest;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::Notify;

/// How one stdout line was classified.
#[derive(Debug, Clone, PartialEq)]
pub enum Kind {
    /// `{"method", "id"}`: a plugin → core request (the session answers it).
    Request {
        id: Value,
        method: String,
        params: Value,
    },
    /// `{"method"}` without `id`: an event or `$/cancelRequest`.
    Notification { method: String, params: Value },
    /// `{"id", "result"}`.
    Success { id: Value, result: Value },
    /// `{"id", "error"}`.
    Error { id: Value, error: Value },
    /// Valid JSON that is not a JSON-RPC message (an array, a scalar, an odd object).
    Invalid { reason: String },
    /// Not JSON at all: stdout noise (§2.1).
    Noise,
}

/// One stdout line, in arrival order.
#[derive(Debug, Clone)]
pub struct Line {
    /// Position in the stream, from 0.
    pub seq: usize,
    /// When it was read.
    pub at: Instant,
    /// The raw text (without the newline), truncated to 4 KiB for the record.
    pub raw: String,
    /// The whole JSON value when it parsed.
    pub value: Option<Value>,
    /// Classification.
    pub kind: Kind,
}

impl Line {
    /// Event name and params when this is a notification or an event sent as a request
    /// (anything but `$/cancelRequest`).
    pub fn event(&self) -> Option<(&str, &Value)> {
        match &self.kind {
            Kind::Notification { method, params } | Kind::Request { method, params, .. }
                if method != ch_protocol::CANCEL_METHOD && !method.starts_with("secrets.") =>
            {
                Some((method, params))
            }
            _ => None,
        }
    }

    /// `account_id` of an event, if any.
    pub fn account_id(&self) -> Option<&str> {
        self.event()
            .and_then(|(_, p)| p.get("account_id"))
            .and_then(Value::as_str)
    }
}

/// Everything recorded so far.
#[derive(Debug, Default)]
pub struct Recording {
    /// Every stdout line.
    pub lines: Vec<Line>,
    /// Which method each of our request ids was for (so results can be validated).
    pub sent: BTreeMap<String, Sent>,
    /// Secrets the plugin stored through `secrets.set`, per account.
    secrets: BTreeMap<String, Map<String, Value>>,
}

impl Recording {
    fn account_secrets(&mut self, id: &str, seed: &Map<String, Value>) -> Map<String, Value> {
        self.secrets
            .entry(id.into())
            .or_insert_with(|| seed.clone())
            .clone()
    }
}

/// One request we sent.
#[derive(Debug, Clone)]
pub struct Sent {
    pub method: String,
    pub at: Instant,
    pub timeout: Duration,
    pub params: Option<Value>,
}

/// The outcome of a call.
#[derive(Debug, Clone)]
pub struct Reply {
    /// Line of the response, when one arrived in time.
    pub seq: Option<usize>,
    /// `Ok(result)` or `Err(error object)`; `None` when no response arrived in time.
    pub response: Option<Result<Value, Value>>,
    /// Round trip.
    pub elapsed: Duration,
}

impl Reply {
    /// The error's `data.code` string, when it is an error.
    pub fn code(&self) -> Option<String> {
        match &self.response {
            Some(Err(e)) => e
                .get("data")
                .and_then(|d| d.get("code"))
                .and_then(Value::as_str)
                .map(String::from)
                .or_else(|| e.get("code").and_then(Value::as_i64).map(|n| n.to_string())),
            _ => None,
        }
    }
    /// The error's numeric code.
    pub fn numeric(&self) -> Option<i64> {
        match &self.response {
            Some(Err(e)) => e.get("code").and_then(Value::as_i64),
            _ => None,
        }
    }
    /// Short description for messages.
    pub fn describe(&self) -> String {
        match &self.response {
            None => format!("no response within {:.1?}", self.elapsed),
            Some(Ok(r)) => format!("result {}", short(&r.to_string())),
            Some(Err(e)) => format!(
                "error {} {}{}",
                e.get("code").map(|c| c.to_string()).unwrap_or_default(),
                e.get("data")
                    .and_then(|d| d.get("code"))
                    .and_then(Value::as_str)
                    .unwrap_or("?"),
                e.get("message")
                    .and_then(Value::as_str)
                    .map(|m| format!(" ({m})"))
                    .unwrap_or_default()
            ),
        }
    }
    /// Is this an error with the given `data.code`?
    pub fn is_code(&self, code: &str) -> bool {
        self.code().as_deref() == Some(code)
    }
}

/// Truncate for messages.
pub fn short(s: &str) -> String {
    const MAX: usize = 160;
    if s.chars().count() <= MAX {
        s.to_owned()
    } else {
        let cut: String = s.chars().take(MAX).collect();
        format!("{cut}…")
    }
}

struct Inner {
    redactions: crate::redact::Redactions,
    capture: crate::capture::CaptureBudget,
    rec: Mutex<Recording>,
    notify: Notify,
    stdin: tokio::sync::Mutex<Option<ChildStdin>>,
    stderr: Mutex<Vec<u8>>,
    exited: Mutex<Option<ExitStatus>>,
    eof: Mutex<bool>,
}

/// A running plugin process.
pub struct Session {
    inner: Arc<Inner>,
    child: Option<Child>,
    #[cfg(unix)]
    group: Option<libc::pid_t>,
    readers: tokio::task::JoinSet<()>,
    next_id: i64,
}

impl Session {
    /// Spawn `manifest.entry` in `dir` with the spec's environment (§3.3), isolated from the
    /// user's profile.
    pub async fn spawn(
        dir: &Path,
        manifest: &Manifest,
        exe: &Path,
        isolate_home: &Path,
        redactions: crate::redact::Redactions,
        capture: crate::capture::CaptureBudget,
    ) -> std::io::Result<Session> {
        let mut cmd = Command::new(exe);
        cmd.env_clear();
        if let Some(path) = std::env::var_os("PATH") {
            cmd.env("PATH", path);
        }
        if let Some(env) = &manifest.env {
            cmd.envs(env);
        }
        for (key, name) in [
            ("HOME", "home"),
            ("TMPDIR", "tmp"),
            ("CH_DATA_DIR", "data"),
            ("XDG_DATA_HOME", "xdg-data"),
            ("XDG_CONFIG_HOME", "xdg-config"),
            ("XDG_CACHE_HOME", "xdg-cache"),
            ("XDG_STATE_HOME", "xdg-state"),
            ("XDG_RUNTIME_DIR", "xdg-runtime"),
            ("XDG_DATA_DIRS", "xdg-data-dirs"),
            ("XDG_CONFIG_DIRS", "xdg-config-dirs"),
        ] {
            let path = isolate_home.join(name);
            let mut builder = std::fs::DirBuilder::new();
            builder.recursive(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            builder.create(&path)?;
            cmd.env(key, path);
        }
        cmd.args(&manifest.entry[1..])
            .current_dir(dir)
            .env(ch_protocol::env::PLUGIN_ID, &manifest.id)
            .env(ch_protocol::env::API_VERSION, ch_protocol::API_VERSION)
            .env(ch_protocol::env::LOG_LEVEL, "debug")
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .env("PYTHON_KEYRING_BACKEND", "keyring.backends.null.Keyring")
            .env(
                "DBUS_SESSION_BUS_ADDRESS",
                format!("unix:path={}/no-session-bus", isolate_home.display()),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            cmd.as_std_mut().process_group(0);
        }
        let mut child = cmd.spawn()?;
        #[cfg(unix)]
        let group = child
            .id()
            .and_then(|pid| libc::pid_t::try_from(pid).ok())
            .filter(|pid| *pid > 1);

        let stdout = child.stdout.take().expect("piped");
        let stderr = child.stderr.take().expect("piped");
        let stdin = child.stdin.take().expect("piped");
        let inner = Arc::new(Inner {
            redactions,
            capture,
            rec: Mutex::new(Recording::default()),
            notify: Notify::new(),
            stdin: tokio::sync::Mutex::new(Some(stdin)),
            stderr: Mutex::new(Vec::new()),
            exited: Mutex::new(None),
            eof: Mutex::new(false),
        });
        let mut readers = tokio::task::JoinSet::new();
        readers.spawn(read_stdout(inner.clone(), stdout));
        readers.spawn(read_stderr(inner.clone(), stderr));
        Ok(Session {
            inner,
            child: Some(child),
            #[cfg(unix)]
            group,
            readers,
            next_id: 1,
        })
    }

    /// Run `f` on the recording.
    pub fn with_recording<T>(&self, f: impl FnOnce(&Recording) -> T) -> T {
        f(&self.inner.rec.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// Transfer ownership once after reader completion for the post-run validation.
    pub fn take_recording(&self) -> Recording {
        self.with_recording_mut(std::mem::take)
    }

    /// Number of lines so far (the next line's seq).
    pub fn cursor(&self) -> usize {
        self.with_recording(|r| r.lines.len())
    }

    /// Seed the fixture store on its first start only. Deletions survive restart.
    pub fn account_secrets(&self, account: &str, seed: &Map<String, Value>) -> Map<String, Value> {
        self.with_recording_mut(|r| r.account_secrets(account, seed))
    }

    /// Has stdout reached EOF?
    pub fn stdout_eof(&self) -> bool {
        *self.inner.eof.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The plugin's stderr so far (lossy).
    pub fn stderr_text(&self) -> String {
        String::from_utf8_lossy(&self.inner.stderr.lock().unwrap_or_else(|e| e.into_inner()))
            .into_owned()
    }

    /// Write raw bytes to the plugin's stdin. `false` when stdin is closed or broken.
    pub async fn write_raw(&self, bytes: &[u8]) -> bool {
        write_to(&self.inner, bytes).await
    }

    /// Send a notification.
    pub async fn notify(&self, method: &str, params: Option<Value>) -> bool {
        let mut msg = json!({"jsonrpc": "2.0", "method": method});
        if let Some(p) = params {
            msg["params"] = p;
        }
        self.write_raw(&frame(&msg)).await
    }

    /// Send a request with a fresh integer id and wait up to `timeout` for its response.
    pub async fn call(&mut self, method: &str, params: Option<Value>, timeout: Duration) -> Reply {
        let id = json!(self.next_id);
        self.next_id += 1;
        self.call_with_id(id, method, params, timeout, b"\n").await
    }

    /// Write a request immediately followed by its cancellation in the same write.
    /// Use the same id registration and response validation as every ordinary call.
    pub async fn call_and_cancel(&mut self, method: &str, timeout: Duration) -> Reply {
        let id = json!(self.next_id);
        self.next_id += 1;
        let cancel =
            json!({"jsonrpc": "2.0", "method": ch_protocol::CANCEL_METHOD, "params": {"id": id}});
        let mut suffix = vec![b'\n'];
        suffix.extend(frame(&cancel));
        self.call_with_id(id, method, None, timeout, &suffix).await
    }

    /// Send a request with an explicit id and line terminator (for framing checks).
    pub async fn call_with_id(
        &mut self,
        id: Value,
        method: &str,
        params: Option<Value>,
        timeout: Duration,
        terminator: &[u8],
    ) -> Reply {
        let mut msg = json!({"jsonrpc": "2.0", "id": id, "method": method});
        if let Some(p) = &params {
            msg["params"] = p.clone();
        }
        let mut bytes = serde_json::to_vec(&msg).expect("json");
        bytes.extend_from_slice(terminator);
        let seq_sent = self.cursor();
        let at = Instant::now();
        self.with_recording_mut(|r| {
            r.sent.insert(
                id.to_string(),
                Sent {
                    method: method.to_owned(),
                    at,
                    timeout,
                    params: params.clone(),
                },
            );
        });
        if !self.write_raw(&bytes).await {
            return Reply {
                seq: None,
                response: None,
                elapsed: at.elapsed(),
            };
        }
        self.wait_reply(id, seq_sent, at, timeout).await
    }

    /// Wait for the response to `id` (which we sent at `seq_sent`).
    pub async fn wait_reply(
        &self,
        id: Value,
        seq_sent: usize,
        at: Instant,
        timeout: Duration,
    ) -> Reply {
        let found = self
            .wait_for(seq_sent, timeout, |l| matches!(&l.kind, Kind::Success { id: i, .. } | Kind::Error { id: i, .. } if *i == id))
            .await;
        let elapsed = at.elapsed();
        match found {
            Some(l) => {
                let response = match &l.kind {
                    Kind::Success { result, .. } => Some(Ok(result.clone())),
                    Kind::Error { error, .. } => Some(Err(error.clone())),
                    _ => None,
                };
                Reply {
                    seq: Some(l.seq),
                    response,
                    elapsed,
                }
            }
            None => Reply {
                seq: None,
                response: None,
                elapsed,
            },
        }
    }

    fn with_recording_mut<T>(&self, f: impl FnOnce(&mut Recording) -> T) -> T {
        f(&mut self.inner.rec.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// Wait up to `timeout` for a line at or after `from` matching `pred`.
    pub async fn wait_for(
        &self,
        from: usize,
        timeout: Duration,
        mut pred: impl FnMut(&Line) -> bool,
    ) -> Option<Line> {
        let deadline = Instant::now() + timeout;
        let mut scanned = from;
        loop {
            // Register before inspecting the buffer so notification and EOF cannot race the scan.
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (found, len) = self.with_recording(|r| {
                let found = r.lines[scanned.min(r.lines.len())..]
                    .iter()
                    .find(|l| pred(l))
                    .cloned();
                (found, r.lines.len())
            });
            if let Some(l) = found {
                return Some(l);
            }
            scanned = len;
            if self.stdout_eof() {
                return None;
            }
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            let _ = tokio::time::timeout(deadline - now, notified).await;
        }
    }

    /// Wait for an event `name` for `account_id` at or after `from`.
    pub async fn wait_event(
        &self,
        from: usize,
        timeout: Duration,
        name: &str,
        account_id: Option<&str>,
    ) -> Option<Line> {
        self.wait_for(from, timeout, |l| {
            l.event().is_some_and(|(n, p)| {
                n == name
                    && account_id
                        .is_none_or(|a| p.get("account_id").and_then(Value::as_str) == Some(a))
            })
        })
        .await
    }

    /// Close stdin (EOF for the plugin).
    pub async fn close_stdin(&self) {
        let mut guard = self.inner.stdin.lock().await;
        if let Some(mut s) = guard.take() {
            let _ = s.shutdown().await;
        }
    }

    /// Has the process exited? (non-blocking)
    pub fn try_exit(&mut self) -> Option<ExitStatus> {
        if let Some(st) = *self.inner.exited.lock().unwrap_or_else(|e| e.into_inner()) {
            return Some(st);
        }
        let st = self
            .child
            .as_mut()
            .and_then(|c| c.try_wait().ok().flatten());
        if let Some(st) = st {
            self.stop_group();
            *self.inner.exited.lock().unwrap_or_else(|e| e.into_inner()) = Some(st);
        }
        st
    }

    /// Wait up to `timeout` for the process to exit.
    pub async fn wait_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        if let Some(st) = self.try_exit() {
            return Some(st);
        }
        let child = self.child.as_mut()?;
        match tokio::time::timeout(timeout, child.wait()).await {
            Ok(Ok(st)) => {
                self.stop_group();
                *self.inner.exited.lock().unwrap_or_else(|e| e.into_inner()) = Some(st);
                Some(st)
            }
            _ => None,
        }
    }

    fn stop_group(&mut self) {
        #[cfg(unix)]
        if let Some(group) = self.group.take() {
            // SAFETY: process_group(0) made this child the leader of its own group;
            // no group id from the plugin or caller is accepted here.
            unsafe {
                libc::killpg(group, libc::SIGKILL);
            }
        }
    }

    /// Stop wrappers and workers, reap the child, and finish both pipe readers.
    pub async fn kill(&mut self) {
        self.stop_group();
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
        self.child.take();
        join_readers(&mut self.readers).await;
    }
}

async fn write_to(inner: &Inner, bytes: &[u8]) -> bool {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut guard = inner.stdin.lock().await;
        match guard.as_mut() {
            Some(s) => {
                let ok = s.write_all(bytes).await.is_ok() && s.flush().await.is_ok();
                if !ok {
                    *guard = None;
                }
                ok
            }
            None => false,
        }
    })
    .await
    .unwrap_or(false)
}

/// Serialise one frame.
pub fn frame(msg: &Value) -> Vec<u8> {
    let mut v = serde_json::to_vec(msg).expect("json");
    v.push(b'\n');
    v
}

fn classify(raw: &[u8]) -> (Option<Value>, Kind) {
    let v: Value = match serde_json::from_slice(raw) {
        Ok(v) => v,
        Err(_) => return (None, Kind::Noise),
    };
    let kind = match &v {
        Value::Object(o) => {
            let id = o.get("id").filter(|i| !i.is_null()).cloned();
            let method = o.get("method").and_then(Value::as_str).map(String::from);
            let params = o.get("params").cloned().unwrap_or(Value::Null);
            match (id, method) {
                (Some(id), Some(method)) => Kind::Request { id, method, params },
                (None, Some(method)) if !o.contains_key("id") => {
                    Kind::Notification { method, params }
                }
                (None, Some(_)) => Kind::Invalid {
                    reason: "request with a null id".into(),
                },
                (Some(id), None) if o.contains_key("result") => Kind::Success {
                    id,
                    result: o["result"].clone(),
                },
                (id, None) if o.contains_key("error") => Kind::Error {
                    id: id.unwrap_or(Value::Null),
                    error: o["error"].clone(),
                },
                _ => Kind::Invalid {
                    reason: "object is neither a request, a notification nor a response".into(),
                },
            }
        }
        Value::Array(_) => Kind::Invalid {
            reason: "top-level value is an array (batches are not supported)".into(),
        },
        _ => Kind::Invalid {
            reason: "top-level value is not an object".into(),
        },
    };
    (Some(v), kind)
}

async fn read_stdout(inner: Arc<Inner>, stdout: tokio::process::ChildStdout) {
    let mut reader = BufReader::with_capacity(64 * 1024, stdout);
    let mut buf = Vec::with_capacity(4096);
    loop {
        buf.clear();
        let limit = (ch_protocol::MAX_FRAME_BYTES + 1) as u64;
        let n = (&mut reader)
            .take(limit)
            .read_until(b'\n', &mut buf)
            .await
            .unwrap_or_default();
        if n == 0 {
            break;
        }
        if !inner.capture.accept(&buf) {
            break;
        }
        let mut line: &[u8] = &buf;
        if line.last() == Some(&b'\n') {
            line = &line[..line.len() - 1];
        }
        if line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
        }
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let malformed = if !buf.ends_with(b"\n") {
            Some("unterminated frame")
        } else if buf.ends_with(b"\r\n") {
            Some("producer emitted CRLF")
        } else {
            None
        };
        let (value, kind) = if let Some(reason) = malformed {
            (
                None,
                Kind::Invalid {
                    reason: reason.to_owned(),
                },
            )
        } else {
            classify(line)
        };
        let raw = if line.len() > 4096 {
            format!("{}…", String::from_utf8_lossy(&line[..4096]))
        } else {
            String::from_utf8_lossy(line).into_owned()
        };
        if let Kind::Request { method, params, .. } | Kind::Notification { method, params } = &kind
            && method == "secrets.set"
            && let Some(value) = params.get("value").and_then(Value::as_str)
        {
            inner.redactions.add(value);
        }
        let answer = if let Kind::Request { id, method, params } = &kind {
            Some(answer_request(&inner, id, method, params))
        } else {
            None
        };
        {
            let mut rec = inner.rec.lock().unwrap_or_else(|e| e.into_inner());
            let seq = rec.lines.len();
            rec.lines.push(Line {
                seq,
                at: Instant::now(),
                raw,
                value,
                kind,
            });
        }
        inner.notify.notify_waiters();
        if let Some(msg) = answer {
            write_to(&inner, &frame(&msg)).await;
        }
    }
    *inner.eof.lock().unwrap_or_else(|e| e.into_inner()) = true;
    inner.notify.notify_waiters();
}

/// Answer a plugin → core request the way the core would (§12).
fn answer_request(inner: &Inner, id: &Value, method: &str, params: &Value) -> Value {
    let mut rec = inner.rec.lock().unwrap_or_else(|e| e.into_inner());
    let account = params
        .get("account_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let key = params.get("key").and_then(Value::as_str).map(String::from);
    let result: Result<Value, Value> = match method {
        "secrets.set" => match (key, params.get("value").and_then(Value::as_str)) {
            (Some(k), Some(v)) => {
                rec.secrets
                    .entry(account)
                    .or_default()
                    .insert(k, Value::String(v.to_owned()));
                Ok(json!({}))
            }
            _ => Err(error_obj(
                -32602,
                "INVALID_PARAMS",
                "secrets.set needs key and value",
            )),
        },
        "secrets.delete" => match key {
            Some(k) => {
                rec.secrets.entry(account).or_default().remove(&k);
                Ok(json!({}))
            }
            None => Err(error_obj(
                -32602,
                "INVALID_PARAMS",
                "secrets.delete needs key",
            )),
        },
        m if ch_protocol::EVENT_NAMES.contains(&m) => Ok(json!({})),
        _ => Err(error_obj(
            -32601,
            "METHOD_NOT_FOUND",
            &format!("unknown method {method}"),
        )),
    };

    match result {
        Ok(r) => json!({"jsonrpc": "2.0", "id": id, "result": r}),
        Err(e) => json!({"jsonrpc": "2.0", "id": id, "error": e}),
    }
}

fn error_obj(code: i64, s: &str, message: &str) -> Value {
    json!({"code": code, "message": message, "data": {"code": s}})
}

async fn read_stderr(inner: Arc<Inner>, stderr: tokio::process::ChildStderr) {
    const CAP: usize = 64 * 1024;
    let mut reader = stderr;
    let mut buf = [0; 4096];
    while let Ok(n) = reader.read(&mut buf).await {
        if n == 0 {
            break;
        }
        let mut err = inner.stderr.lock().unwrap_or_else(|e| e.into_inner());
        err.extend_from_slice(&buf[..n]);
        if err.len() > CAP {
            let cut = err.len() - CAP;
            err.drain(..cut);
        }
    }
}

async fn join_readers(readers: &mut tokio::task::JoinSet<()>) {
    // Even a detached child outside our process group cannot hold the reader forever.
    if tokio::time::timeout(Duration::from_secs(1), async {
        while readers.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        readers.shutdown().await;
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.stop_group();
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                let mut readers = std::mem::take(&mut self.readers);
                runtime.spawn(async move {
                    let _ = child.wait().await;
                    join_readers(&mut readers).await;
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_secrets_seed_once_per_account() {
        let mut rec = Recording::default();
        let seed = json!({"delete":"initial", "replace":"old"})
            .as_object()
            .unwrap()
            .clone();
        assert_eq!(rec.account_secrets("a", &seed), seed);
        let store = rec.secrets.get_mut("a").unwrap();
        store.remove("delete");
        store.insert("replace".into(), json!("new"));
        assert_eq!(
            rec.account_secrets("a", &seed),
            json!({"replace":"new"}).as_object().unwrap().clone()
        );
        assert!(rec.account_secrets("b", &Map::new()).is_empty());
    }
}

#[cfg(all(test, unix))]
mod process_tests {
    use super::*;
    use crate::{Options, Outcome, capture::CaptureBudget, checks, redact::Redactions};

    fn fixture(script: &str) -> (tempfile::TempDir, Manifest) {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("worker.sh"), script).unwrap();
        let manifest = serde_json::from_value(json!({
            "id":"offline_fixture","name":"Fixture","version":"1.0.0","api_version":"1.0.0",
            "entry":["/bin/sh","worker.sh"],"brand_color":"#123456","multi_account":false,"auth_methods":["none"]
        })).unwrap();
        (root, manifest)
    }

    async fn launch(root: &Path, manifest: &Manifest, budget: CaptureBudget) -> Session {
        Session::spawn(
            root,
            manifest,
            Path::new("/bin/sh"),
            root,
            Redactions::default(),
            budget,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn cancellation_reply_gets_normal_duplicate_and_result_checks() {
        let (root, manifest) = fixture(
            r#"read -r request
read -r cancel
for reply in 1 2; do printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"busy":"bad"}}'; done
"#,
        );
        let mut session = launch(root.path(), &manifest, CaptureBudget::default()).await;
        let r = session
            .call_and_cancel("ping", Duration::from_secs(2))
            .await;
        assert!(r.response.is_some());
        assert!(
            session
                .wait_exit(Duration::from_secs(2))
                .await
                .unwrap()
                .success()
        );
        session.kill().await;
        let options = Options::default();
        let mut ctx = checks::Ctx::new(root.path(), &options, root.path().to_owned());
        ctx.manifest = Some(manifest);
        ctx.session = Some(session);
        checks::recording::run(&mut ctx);
        for id in ["transport.frames_valid", "results.schema"] {
            assert!(
                ctx.results
                    .iter()
                    .any(|c| c.id == id && c.outcome == Outcome::Fail),
                "{id}"
            );
        }
    }

    #[tokio::test]
    async fn wrapper_exit_and_drop_terminate_its_workers() {
        for drop_early in [false, true] {
            let (root, manifest) = fixture(if drop_early {
                "(sleep 0.7; touch survived) &\nprintf 'ready\\n'\nwait"
            } else {
                "(sleep 0.7; touch survived) &\nprintf 'ready\\n'\nexit 0"
            });
            let mut s = launch(root.path(), &manifest, CaptureBudget::default()).await;
            assert!(
                s.wait_for(0, Duration::from_secs(2), |l| l.raw == "ready")
                    .await
                    .is_some()
            );
            if !drop_early {
                assert!(s.wait_exit(Duration::from_secs(2)).await.unwrap().success());
                s.kill().await;
                assert!(s.readers.is_empty());
            }
            drop(s);
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(!root.path().join("survived").exists());
        }
    }

    #[tokio::test]
    async fn endless_single_line_fails_before_json_decode() {
        let (root, manifest) = fixture("exec cat /dev/zero");
        let budget = CaptureBudget::default();
        let mut failure = budget.subscribe();
        let mut s = launch(root.path(), &manifest, budget).await;
        tokio::time::timeout(Duration::from_secs(5), failure.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(failure.borrow().unwrap().contains("frame limit"));
        assert_eq!(s.cursor(), 0);
        s.kill().await;
    }
}
