//! Public state types: what a [`super::HostHandle`] reports and broadcasts.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ch_protocol::capabilities::Capabilities;
use ch_protocol::dto::Contact;
use ch_protocol::events::AccountState;
use ch_protocol::manifest::Manifest;
use ch_protocol::methods::AutoDownload;
use serde_json::{Map, Value};

use super::discovery::DiscoveryProblem;

/// Static facts about a discovered plugin.
#[derive(Debug, Clone)]
pub struct PluginInfo {
    /// Manifest `id`.
    pub id: String,
    /// The plugin directory.
    pub dir: PathBuf,
    /// From `<install>/plugins/` rather than the user directory.
    pub builtin: bool,
    /// The manifest, when it parsed.
    pub manifest: Option<Manifest>,
    /// The resolved `entry[0]`.
    pub executable: Option<PathBuf>,
}

/// Where a plugin process is in its life (spec §4, §4.6).
#[derive(Debug, Clone, PartialEq)]
pub enum PluginState {
    /// Cannot run: manifest invalid or `entry[0]` missing. Shown as `error` with the reason.
    Unavailable(DiscoveryProblem),
    /// No process; nothing asked for one.
    Stopped,
    /// Spawned, `initialize` in flight.
    Starting,
    /// Handshake done; serving.
    Running {
        /// The plugin's `api_version`.
        api_version: String,
        /// `plugin_version` from `initialize`, if given.
        plugin_version: Option<String>,
        /// Capabilities from this `initialize`.
        capabilities: Arc<Capabilities>,
    },
    /// Died, hung or broke framing; waiting out the backoff before attempt `failures + 1`.
    Restarting {
        /// Failures in a row so far.
        failures: u32,
        /// Delay before the next attempt.
        delay: Duration,
        /// What went wrong.
        reason: String,
    },
    /// Ten failures in a row (§4.6); needs an explicit restart.
    Failed {
        /// The last failure.
        reason: String,
    },
    /// The plugin speaks another major version (§18); not started again.
    Incompatible {
        /// The plugin's `api_version`.
        api_version: String,
    },
    /// `shutdown` sent; waiting for the exit.
    ShuttingDown,
}

impl std::fmt::Display for PluginState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable(p) => write!(f, "unavailable ({p})"),
            Self::Stopped => f.write_str("stopped"),
            Self::Starting => f.write_str("starting"),
            Self::Running { api_version, .. } => write!(f, "running (api {api_version})"),
            Self::Restarting {
                failures, delay, ..
            } => write!(f, "restarting (failure {failures}, next in {delay:?})"),
            Self::Failed { reason } => write!(f, "failed ({reason})"),
            Self::Incompatible { api_version } => write!(f, "incompatible (api {api_version})"),
            Self::ShuttingDown => f.write_str("shutting down"),
        }
    }
}

impl PluginState {
    /// `Running`?
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running { .. })
    }
    /// The capabilities, when running.
    pub fn capabilities(&self) -> Option<&Arc<Capabilities>> {
        match self {
            Self::Running { capabilities, .. } => Some(capabilities),
            _ => None,
        }
    }
}

/// What the core asks the plugin to do with an account (`account.start` minus what the host
/// fills in itself: secrets, directories, settings defaults).
#[derive(Debug, Clone, Default)]
pub struct AccountSpec {
    /// `settings_schema` values, with `x-secret` values already merged in by the settings
    /// service (§6.2). The host adds the schema defaults (§6.1).
    pub settings: Map<String, Value>,
    /// Ghost mode (§17).
    pub ghost: bool,
    /// Auto-download limits (§11.1).
    pub auto_download: AutoDownload,
}

/// The host's own lifecycle phase for an account (distinct from the plugin-reported
/// [`AccountState`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountPhase {
    /// Not started on the process (or the process is down).
    Stopped,
    /// `account.start` sent, awaiting the reply.
    Starting,
    /// `account.start` answered `{}`; events flow.
    Running,
    /// `account.stop` sent, awaiting the reply.
    Stopping,
}

/// One account as the host sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountInfo {
    /// The plugin that serves it.
    pub plugin_id: String,
    /// The core-chosen id.
    pub account_id: String,
    /// Should this account be running? Set by `start_account`, cleared by `stop_account`;
    /// survives plugin restarts (the host re-starts the account).
    pub wanted: bool,
    /// Host phase.
    pub phase: AccountPhase,
    /// Last `status.state` from the plugin, or `disconnected`.
    pub state: AccountState,
    /// Last `status.detail`.
    pub detail: Option<String>,
    /// Last `status.reconnecting`.
    pub reconnecting: bool,
    /// Last `status.me`.
    pub me: Option<Contact>,
}

impl AccountInfo {
    pub(crate) fn new(plugin_id: &str, account_id: &str) -> Self {
        Self {
            plugin_id: plugin_id.to_owned(),
            account_id: account_id.to_owned(),
            wanted: false,
            phase: AccountPhase::Stopped,
            state: AccountState::Disconnected,
            detail: None,
            reconnecting: false,
            me: None,
        }
    }
}

/// A plugin with its state and accounts.
#[derive(Debug, Clone)]
pub struct PluginSnapshot {
    /// Static facts.
    pub info: PluginInfo,
    /// Current state.
    pub state: PluginState,
    /// Accounts the host knows on this plugin, sorted by id.
    pub accounts: Vec<AccountInfo>,
}

/// Broadcast on every state change.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum StateChange {
    /// The set of discovered plugins changed (after a rescan).
    PluginsRescanned,
    /// A plugin changed state.
    Plugin {
        /// Which plugin.
        plugin_id: String,
        /// Its new state.
        state: PluginState,
    },
    /// An account changed phase or status.
    Account(AccountInfo),
}
