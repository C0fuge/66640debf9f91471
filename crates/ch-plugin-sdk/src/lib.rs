//! Rust SDK for Contact Hub plugins (plugin API v1.0.0, `docs/plugin-api.md`).
//!
//! A plugin is a child process speaking NDJSON JSON-RPC 2.0 on stdio. This crate does the
//! protocol work so the author only writes platform code:
//!
//! - **Transport** (§2): framing with the 16 MiB limit, requests and notifications in both
//!   directions, out-of-order responses, a stdin reader on its own thread so `ping` is always
//!   answered, stdout written by one task, nothing else ever touching stdout.
//! - **Lifecycle** (§4): `initialize` (API version and capabilities), `ping`, `shutdown`,
//!   `account.start`/`account.stop` with `status` events and the "no events after stop" rule.
//! - **Dispatch** (§11, §13, §14): handlers per method with typed params from
//!   [`ch_protocol::methods`]; `METHOD_NOT_FOUND`, `NOT_SUPPORTED`, `UNKNOWN_ACCOUNT` and
//!   `INVALID_PARAMS` answered for you, including the capability-gated parameters from the
//!   registry's `gated_params` ([`ch_protocol::MethodInfo::missing_capabilities`], §14.2);
//!   handler errors ([`PluginError`]) normalised to spec error objects.
//! - **Cancellation and timeouts** (§11.0, §4.6): every handler gets a [`Call`] whose token
//!   fires on `$/cancelRequest`, on the method's timeout from [`ch_protocol::METHODS`] (reset by
//!   [`Call::progress`]), and on `account.stop`.
//! - **Events** (§10, §16): [`Account`] has a typed emitter for every event; [`Host`] emits any
//!   [`ch_protocol::Event`] and sends events as acknowledged requests (at most 4 in flight).
//!   Events emitted during `account.start` are held until its reply (§4.2).
//!   `Conversation`, `Topic` and `Message` are merge-updated: send only the fields that
//!   changed ([`builders`]: `Conversation::new(id, kind).with_unread_count(3)`).
//! - **Plugin → core requests** (§12): [`Account::set_secret`], [`Account::delete_secret`],
//!   [`Host::request`], with the registry's timeout (`$/cancelRequest` and `TIMEOUT` on expiry).
//! - **Auth and settings** (§6, §7): [`auth::AuthFlow`] and [`Settings`] (with the manifest's
//!   `settings_schema` defaults filled in).
//! - **Tests**: [`testing::FakeCore`] plays the core over in-memory pipes or a child process.
//!
//! Runtime: tokio (ADR 0001). See `examples/echo.rs` for a complete minimal plugin.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod auth;
pub mod builders;
pub mod error;
mod host;
mod plugin;
mod settings;
pub mod testing;
mod transport;

pub use ch_protocol;
pub use error::{EmitError, HandlerResult, PluginError, ResultExt, SdkError};
pub use host::{Account, Call, Host, MAX_ACKED_IN_FLIGHT};
pub use plugin::{Plugin, PluginBuilder};
pub use settings::Settings;
pub use tokio_util::sync::CancellationToken;

/// Install a `tracing` subscriber that writes to **stderr** (stdout is reserved for protocol
/// frames, §2.1), at the level in `CH_LOG_LEVEL` (default `info`). Call once at startup;
/// later calls do nothing.
pub fn init_logging() {
    let level = match std::env::var(ch_protocol::env::LOG_LEVEL).as_deref() {
        Ok("error") => tracing::Level::ERROR,
        Ok("warn") => tracing::Level::WARN,
        Ok("debug") => tracing::Level::DEBUG,
        Ok("trace") => tracing::Level::TRACE,
        _ => tracing::Level::INFO,
    };
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(level)
        .try_init();
}

/// Everything a typical plugin needs: `use ch_plugin_sdk::prelude::*;`.
pub mod prelude {
    pub use crate::auth::AuthFlow;
    pub use crate::builders::{
        ContactExt, ConversationExt, DraftExt, FolderExt, MessageExt, TopicExt,
    };
    pub use crate::error::{self, EmitError, HandlerResult, PluginError, ResultExt, SdkError};
    pub use crate::{Account, Call, CancellationToken, Host, Plugin, PluginBuilder, Settings};
    pub use ch_protocol::dto::*;
    pub use ch_protocol::events::{AccountState, AvatarKind, TransferDirection};
    pub use ch_protocol::methods::*;
    pub use ch_protocol::{
        Capabilities, Empty, ErrorCode, ErrorObject, Event, Id, RequestId, Timestamp,
    };
}
