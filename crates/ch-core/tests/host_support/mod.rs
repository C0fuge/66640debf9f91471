//! Shared helpers for the host integration tests: temp data dirs, plugin directories, a
//! recording (optionally blocking) sink, and polling helpers. Everything is isolated under a
//! temp dir; `CH_DATA_DIR`/`XDG_*` are passed to every plugin process.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use ch_core::host::{
    AccountInfo, AccountSpec, EventSink, HostConfig, HostHandle, MemorySecretStore, PluginState,
};
use ch_protocol::events::Event;
use serde_json::{Value, json};
use tokio::sync::watch;

/// Divisor applied to every spec timer in tests.
pub const DIV: u32 = 50;

pub struct Env {
    pub tmp: tempfile::TempDir,
    pub data_dir: PathBuf,
    pub plugins_dir: PathBuf,
}

impl Env {
    pub fn new() -> Self {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init();
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        let plugins_dir = data_dir.join("plugins");
        std::fs::create_dir_all(&plugins_dir).unwrap();
        Self {
            tmp,
            data_dir,
            plugins_dir,
        }
    }

    /// A config with every timer divided by [`DIV`] and the plugin environment isolated.
    pub fn config(&self) -> HostConfig {
        let mut c = HostConfig::fast(&self.data_dir, DIV);
        for var in [
            "CH_DATA_DIR",
            "XDG_DATA_HOME",
            "XDG_CONFIG_HOME",
            "XDG_CACHE_HOME",
            "XDG_STATE_HOME",
            "HOME",
        ] {
            c.env.insert(
                var.into(),
                self.tmp.path().join(var).to_string_lossy().into_owned(),
            );
        }
        c.core_version = "0.0.0-test".into();
        c
    }

    /// Write a plugin directory with the given manifest fields.
    pub fn plugin_dir(&self, id: &str, entry: &[&str], extra: Value) -> PathBuf {
        let dir = self.plugins_dir.join(id);
        std::fs::create_dir_all(&dir).unwrap();
        let mut manifest = json!({
            "id": id,
            "name": id,
            "version": "0.1.0",
            "api_version": "1.0.0",
            "entry": entry,
            "brand_color": "#123456",
            "multi_account": true,
            "auth_methods": ["none"],
        });
        if let Some(obj) = extra.as_object() {
            for (k, v) in obj {
                manifest[k] = v.clone();
            }
        }
        std::fs::write(
            dir.join("plugin.json"),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        dir
    }

    /// A plugin dir running `fixtures/misbehaving.py --mode <mode>`.
    pub fn misbehaving(&self, id: &str, mode: &str, extra: Value) -> PathBuf {
        let script = misbehaving_script();
        self.plugin_dir(
            id,
            &["python3", "-I", script.to_str().unwrap(), "--mode", mode],
            extra,
        )
    }

    /// A plugin dir running the Rust SDK `echo` example via a `./echo` symlink.
    pub fn echo(&self, id: &str) -> PathBuf {
        let dir = self.plugin_dir(id, &["./echo"], json!({}));
        std::os::unix::fs::symlink(echo_binary(), dir.join("echo")).unwrap();
        dir
    }
}

pub fn misbehaving_script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("misbehaving.py")
}

/// Path of the built `echo` example of `ch-plugin-sdk` (built on demand).
pub fn echo_binary() -> PathBuf {
    let deps = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let bin = deps
        .parent()
        .unwrap()
        .join("examples")
        .join(format!("echo{}", std::env::consts::EXE_SUFFIX));
    if !bin.exists() {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
        let st = std::process::Command::new(cargo)
            .args(["build", "-p", "ch-plugin-sdk", "--example", "echo"])
            .status()
            .unwrap();
        assert!(st.success(), "building the echo example failed");
    }
    bin
}

/// Records every event; can be paused so `deliver` blocks (a slow database).
pub struct RecSink {
    pub events: Mutex<Vec<(String, Event)>>,
    pub delivered: AtomicUsize,
    paused: watch::Sender<bool>,
    notify: tokio::sync::Notify,
}

impl RecSink {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
            delivered: AtomicUsize::new(0),
            paused: watch::channel(false).0,
            notify: tokio::sync::Notify::new(),
        })
    }

    pub fn pause(&self) {
        self.paused.send_replace(true);
    }

    pub fn resume(&self) {
        self.paused.send_replace(false);
    }

    pub fn count(&self) -> usize {
        self.delivered.load(Ordering::SeqCst)
    }

    pub fn names(&self) -> Vec<String> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .map(|(_, e)| e.name().to_owned())
            .collect()
    }

    pub fn events(&self) -> Vec<Event> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .map(|(_, e)| e.clone())
            .collect()
    }

    /// Wait until `pred` holds over the recorded events.
    pub async fn wait_for(&self, timeout: Duration, mut pred: impl FnMut(&[Event]) -> bool) {
        let deadline = Instant::now() + timeout;
        loop {
            if pred(&self.events()) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "sink condition not met in {timeout:?}; events: {:?}",
                self.names()
            );
            let _ = tokio::time::timeout(Duration::from_millis(50), self.notify.notified()).await;
        }
    }

    /// Wait for the first `status` event of `account` with this detail predicate.
    pub async fn wait_status(
        &self,
        account: &str,
        timeout: Duration,
        mut pred: impl FnMut(&ch_protocol::events::StatusEvent) -> bool,
    ) -> ch_protocol::events::StatusEvent {
        let mut found = None;
        self.wait_for(timeout, |evs| {
            found = evs.iter().find_map(|e| match e {
                Event::Status(s) if s.account_id == account && pred(s) => Some(s.clone()),
                _ => None,
            });
            found.is_some()
        })
        .await;
        found.unwrap()
    }
}

#[async_trait]
impl EventSink for RecSink {
    async fn deliver(&self, plugin_id: &str, event: Event) -> Result<(), String> {
        let mut rx = self.paused.subscribe();
        let _ = rx.wait_for(|p| !*p).await;
        self.events
            .lock()
            .unwrap()
            .push((plugin_id.to_owned(), event));
        self.delivered.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
        Ok(())
    }
}

/// Build a host over `env` with a fresh recording sink and memory secret store.
pub fn host(env: &Env) -> (HostHandle, Arc<RecSink>, Arc<MemorySecretStore>) {
    let sink = RecSink::new();
    let secrets = Arc::new(MemorySecretStore::new());
    let h = HostHandle::new(env.config(), sink.clone(), secrets.clone());
    (h, sink, secrets)
}

pub fn spec(settings: Value) -> AccountSpec {
    AccountSpec {
        settings: settings.as_object().cloned().unwrap_or_default(),
        ghost: false,
        auto_download: Default::default(),
    }
}

/// Poll until `pred(state)` holds.
pub async fn wait_plugin_state(
    h: &HostHandle,
    plugin: &str,
    timeout: Duration,
    mut pred: impl FnMut(&PluginState) -> bool,
) -> PluginState {
    let deadline = Instant::now() + timeout;
    loop {
        let st = h.plugin_state(plugin).expect("plugin known");
        if pred(&st) {
            return st;
        }
        assert!(Instant::now() < deadline, "plugin {plugin} stuck in {st}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Poll until `pred(account)` holds.
pub async fn wait_account(
    h: &HostHandle,
    plugin: &str,
    account: &str,
    timeout: Duration,
    mut pred: impl FnMut(&AccountInfo) -> bool,
) -> AccountInfo {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(a) = h.account(plugin, account)
            && pred(&a)
        {
            return a;
        }
        assert!(
            Instant::now() < deadline,
            "account {account} stuck in {:?}",
            h.account(plugin, account)
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Parse the JSON detail the misbehaving plugin puts in its `connected` status.
pub fn detail_json(s: &ch_protocol::events::StatusEvent) -> BTreeMap<String, Value> {
    serde_json::from_str(s.detail.as_deref().unwrap_or("{}")).unwrap()
}

pub fn secs(s: f64) -> Duration {
    Duration::from_secs_f64(s)
}
