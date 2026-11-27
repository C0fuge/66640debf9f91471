//! Runtime errors.

use crate::host::{HostError, SecretError};

/// One reason a settings object failed validation. Never contains a setting's value (secrets).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingsProblem {
    /// JSON pointer into the settings object (`""` = the object itself, `/proxy` = a field).
    pub path: String,
    /// What is wrong, with the value masked.
    pub message: String,
}

impl std::fmt::Display for SettingsProblem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.path.is_empty() {
            f.write_str(&self.message)
        } else {
            write!(f, "{}: {}", self.path, self.message)
        }
    }
}

/// Why a runtime operation failed.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    /// The database failed.
    #[error(transparent)]
    Db(#[from] ch_db::Error),
    /// Durable lifetime records cannot be safely bounded/identified by this
    /// composition. No source is assumed safe or absent and no records are pruned.
    #[error("managed lifetime recovery is not available in this build")]
    ManagedLifetimeRecoveryUnavailable,
    /// Local source lifetime remains unresolved; no account data was deleted.
    #[error("account sources are still in use")]
    SourceInUse,
    /// The plugin host refused or failed.
    #[error(transparent)]
    Host(#[from] HostError),
    /// The secret store failed.
    #[error(transparent)]
    Secret(#[from] SecretError),
    /// No account with this id.
    #[error("unknown account {0:?}")]
    UnknownAccount(String),
    /// No plugin with this id was discovered.
    #[error("unknown plugin {0:?}")]
    UnknownPlugin(String),
    /// The plugin was discovered but its manifest did not parse.
    #[error("plugin {plugin_id:?} is unavailable: {reason}")]
    PluginUnavailable {
        /// The plugin.
        plugin_id: String,
        /// Why.
        reason: String,
    },
    /// An account with this id exists already.
    #[error("account {0:?} already exists")]
    AccountExists(String),
    /// The settings do not satisfy the plugin's `settings_schema`.
    #[error("invalid settings: {}", .0.iter().map(ToString::to_string).collect::<Vec<_>>().join("; "))]
    InvalidSettings(Vec<SettingsProblem>),
    /// The plugin's `settings_schema` is not a valid JSON Schema.
    #[error("plugin {plugin_id:?} has an invalid settings_schema: {message}")]
    InvalidSchema {
        /// The plugin.
        plugin_id: String,
        /// The compiler's message.
        message: String,
    },
    /// Filesystem.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// A blocking task panicked or was cancelled.
    #[error("runtime task failed: {0}")]
    Task(String),
}

impl From<tokio::task::JoinError> for RuntimeError {
    fn from(e: tokio::task::JoinError) -> Self {
        Self::Task(e.to_string())
    }
}

/// Result alias.
pub type Result<T> = std::result::Result<T, RuntimeError>;
