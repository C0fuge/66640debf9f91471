//! Host configuration: paths, the plugin environment and the supervision policy (spec §4).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ch_protocol::common::LogLevel;

/// Everything the host needs to know. [`HostConfig::new`] gives the spec's values; tests use
/// [`HostConfig::fast`] to shrink every timer.
#[derive(Debug, Clone)]
pub struct HostConfig {
    /// The data directory (`CH_DATA_DIR`): holds `accounts/<id>/{state,cache}`,
    /// `plugins/` and `logs/plugins/`.
    pub data_dir: PathBuf,
    /// Built-in plugins (`<install>/plugins/`). `None` = none.
    pub builtin_plugins_dir: Option<PathBuf>,
    /// User plugins. Default `data_dir/plugins`.
    pub user_plugins_dir: PathBuf,
    /// Where plugin stderr is appended, one file per plugin. Default `data_dir/logs/plugins`.
    /// `None` disables the files (stderr still goes to `tracing`).
    pub plugin_log_dir: Option<PathBuf>,
    /// `initialize.core_version`.
    pub core_version: String,
    /// `initialize.locale` (BCP 47).
    pub locale: String,
    /// `initialize.log_level` and `CH_LOG_LEVEL`.
    pub log_level: LogLevel,
    /// Extra environment for every plugin process, applied before the manifest's `env` and
    /// the `CH_*` variables. Tests use it to point `XDG_*` at a temp dir.
    pub env: BTreeMap<String, String>,

    /// `ping` period (§4.3). Spec: 15 s.
    pub ping_interval: Duration,
    /// Consecutive missed pings before the process counts as hung. Spec: 3.
    pub ping_misses: u32,
    /// First restart delay (§4.6). Spec: 1 s.
    pub backoff_base: Duration,
    /// Restart delay cap. Spec: 60 s.
    pub backoff_cap: Duration,
    /// Uptime after which the failure counter resets. Spec: 5 min.
    pub healthy_after: Duration,
    /// Failures in a row after which the host gives up. Spec: 10.
    pub max_failures: u32,
    /// Time to wait for the `shutdown` reply (§4.5). Spec: 10 s.
    pub shutdown_grace: Duration,
    /// After the reply (or the grace), time to wait for exit before `SIGTERM`, and again
    /// before `SIGKILL`. Spec: 5 s.
    pub exit_grace: Duration,
    /// Multiplier on every registry timeout (`1.0` = the spec). Tests use a small value.
    pub timeout_scale: f64,
    /// Bound of the per-plugin event queue in front of the [`super::EventSink`], in frames.
    /// When it is full the host stops reading the plugin's stdout (§16): the pipe fills and
    /// the plugin's writes block. Memory stays bounded at `event_queue × 16 MiB` worst case.
    pub event_queue: usize,
    /// Bound of the outgoing frame queue. A plugin that stops reading stdin (§2.4) fills it;
    /// the host then treats the process as hung.
    pub write_queue: usize,
    /// Maximum stderr lines per second forwarded to `tracing` per plugin; the rest are counted
    /// and summarised. The log file (if any) gets every line.
    pub stderr_lines_per_sec: u32,
}

impl HostConfig {
    /// The spec's policy, with data under `data_dir`.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        Self {
            user_plugins_dir: data_dir.join("plugins"),
            plugin_log_dir: Some(data_dir.join("logs").join("plugins")),
            data_dir,
            builtin_plugins_dir: None,
            core_version: env!("CARGO_PKG_VERSION").to_owned(),
            locale: "en-US".to_owned(),
            log_level: LogLevel::Info,
            env: BTreeMap::new(),
            ping_interval: Duration::from_secs(ch_protocol::PING_INTERVAL_SECONDS),
            ping_misses: ch_protocol::PING_MISSES_BEFORE_RESTART,
            backoff_base: Duration::from_secs(1),
            backoff_cap: Duration::from_secs(60),
            healthy_after: Duration::from_secs(300),
            max_failures: 10,
            shutdown_grace: Duration::from_secs(ch_protocol::SHUTDOWN_GRACE_SECONDS),
            exit_grace: Duration::from_secs(ch_protocol::EXIT_GRACE_SECONDS),
            timeout_scale: 1.0,
            event_queue: 64,
            write_queue: 1024,
            stderr_lines_per_sec: 200,
        }
    }

    /// Same structure, every timer divided by `divisor` (tests).
    pub fn fast(data_dir: impl Into<PathBuf>, divisor: u32) -> Self {
        let mut c = Self::new(data_dir);
        let d = |t: Duration| t / divisor;
        c.ping_interval = d(c.ping_interval);
        c.backoff_base = d(c.backoff_base);
        c.backoff_cap = d(c.backoff_cap);
        c.healthy_after = d(c.healthy_after);
        c.shutdown_grace = d(c.shutdown_grace);
        c.exit_grace = d(c.exit_grace);
        c.timeout_scale = 1.0 / f64::from(divisor);
        c
    }

    /// The registry timeout of `method`, scaled. Unknown methods get the default (§4.6).
    pub fn request_timeout(&self, method: &str) -> Duration {
        let secs = ch_protocol::method(method)
            .map_or(ch_protocol::DEFAULT_TIMEOUT_SECONDS, |m| m.timeout_seconds);
        self.scale(Duration::from_secs(u64::from(secs)))
    }

    /// Scale any spec duration by `timeout_scale`, never below 1 ms.
    pub fn scale(&self, d: Duration) -> Duration {
        d.mul_f64(self.timeout_scale).max(Duration::from_millis(1))
    }

    /// Restart delay before attempt `failures` (1-based): `base × 2^(n-1)`, capped (§4.6).
    pub fn backoff(&self, failures: u32) -> Duration {
        let shift = failures.saturating_sub(1).min(16);
        self.backoff_base
            .checked_mul(1u32 << shift)
            .unwrap_or(self.backoff_cap)
            .min(self.backoff_cap)
    }

    /// `account.start.state_dir` for an account.
    pub fn state_dir(&self, account_id: &str) -> PathBuf {
        self.account_dir(account_id).join("state")
    }

    /// `account.start.cache_dir` for an account.
    pub fn cache_dir(&self, account_id: &str) -> PathBuf {
        self.account_dir(account_id).join("cache")
    }

    /// Private app-managed upload stages, separate from evictable media cache.
    pub fn upload_dir(&self, account_id: &str) -> PathBuf {
        self.account_dir(account_id).join("uploads")
    }

    fn account_dir(&self, account_id: &str) -> PathBuf {
        // Account ids are chosen by the core (§5) but never trust them as path components.
        let safe: String = account_id
            .chars()
            .map(|c| {
                if c == '/' || c == '\\' || c == '\0' {
                    '_'
                } else {
                    c
                }
            })
            .collect();
        let safe = if safe == "." || safe == ".." {
            format!("_{safe}")
        } else {
            safe
        };
        self.data_dir.join("accounts").join(safe)
    }

    /// `logs/plugins/<plugin-id>.log`.
    pub fn plugin_log_path(&self, plugin_id: &str) -> Option<PathBuf> {
        self.plugin_log_dir
            .as_ref()
            .map(|d| d.join(format!("{plugin_id}.log")))
    }

    /// The data directory as given.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}
