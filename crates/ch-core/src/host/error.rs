//! Host-side errors. Anything the plugin (or the core on its behalf) answers on the wire is an
//! [`ErrorObject`] (spec §13) so the UI keys off `data.code`; the rest are host conditions.

use ch_protocol::{ErrorCode, ErrorObject};

use super::state::PluginState;

/// Why a host operation failed.
#[derive(Debug, thiserror::Error)]
pub enum HostError {
    /// The plugin answered with an error, or the core raised one in its place
    /// (`TIMEOUT`, `CANCELLED`, `INTERNAL` "plugin restarted", `NOT_SUPPORTED` for a missing
    /// capability or gated parameter).
    #[error("plugin error {}: {}", .0.code().as_str(), .0.message)]
    Rpc(Box<ErrorObject>),
    /// No plugin with this id was discovered.
    #[error("unknown plugin {0:?}")]
    UnknownPlugin(String),
    /// The plugin is not in a state that can take requests.
    #[error("plugin {plugin_id:?} is not running: {state}")]
    PluginNotRunning {
        /// The plugin.
        plugin_id: String,
        /// Its state at the time.
        state: PluginState,
    },
    /// The account is not known to this plugin.
    #[error("unknown account {0:?}")]
    UnknownAccount(String),
    /// `start_account` for an account that is started or starting (§4.2: stop first and wait).
    #[error("account {0:?} is already started")]
    AccountAlreadyStarted(String),
    /// A lifecycle change is in flight for this account; wait for it.
    #[error("account {0:?} is busy ({1})")]
    AccountBusy(String, &'static str),
    /// The method is unknown to the registry and not a vendor method, is plugin→core, or is
    /// a lifecycle method that only the host may send.
    #[error("method {0:?} cannot be called through the host: {1}")]
    InvalidMethod(String, &'static str),
    /// `multi_account: false` and another account is already started.
    #[error("plugin {0:?} runs one account at a time")]
    SingleAccount(String),
    /// Params or a result did not (de)serialise.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// The host is shutting down.
    #[error("host is shutting down")]
    ShuttingDown,
    /// I/O while spawning or talking to the process.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl HostError {
    /// Wrap a wire error.
    pub fn rpc(e: ErrorObject) -> Self {
        Self::Rpc(Box::new(e))
    }
    /// A core-raised error with the given code and message.
    pub fn code(code: ErrorCode, message: impl Into<String>) -> Self {
        Self::rpc(ErrorObject::new(code, message))
    }
    /// The wire error code, when this is a wire error.
    pub fn error_code(&self) -> Option<ErrorCode> {
        match self {
            Self::Rpc(e) => Some(e.code()),
            _ => None,
        }
    }
}

/// A [`super::SecretStore`] failure. The host maps it to `INTERNAL` on the wire.
#[derive(Debug, thiserror::Error)]
#[error("secret store: {0}")]
pub struct SecretError(pub String);
