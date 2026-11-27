//! Plugin host: process lifecycle, transport, handshake, crash and restart, backpressure.
//!
//! The host owns every plugin process (spec `docs/plugin-api.md` §2–§5, §12–§14, §16, §18).
//! It is deliberately ignorant of the database: everything a plugin reports is handed to an
//! [`EventSink`], and runtime secrets go through a [`SecretStore`]; the rest of the core
//! implements both. Services drive it through a [`HostHandle`].
//!
//! Layout:
//! - [`discovery`]: finds plugin directories and validates manifests (§3).
//! - [`transport`]: NDJSON framing of JSON-RPC envelopes (§2).
//! - [`settings`]: fills `settings_schema` defaults into `account.start.settings` (§6.1).
//! - [`supervisor`]: one task per plugin: spawn, `initialize`, ping, requests with timeouts
//!   and cancellation, incoming events and requests, crash detection, restart with backoff,
//!   graceful `shutdown` (§4).
//! - [`handle`]: the [`HostHandle`] API and the host-wide state snapshot.

mod config;
pub mod discovery;
mod error;
mod handle;
mod removal;
mod start_scope;
#[cfg(target_os = "linux")]
mod directories;
pub mod settings;
mod sink;
#[cfg(target_os = "linux")]
mod owned;
#[cfg(target_os = "linux")]
mod origins;
#[cfg(target_os = "linux")]
mod inputs;
#[cfg(target_os = "linux")]
mod managed;
#[cfg(target_os = "linux")]
pub(crate) use managed::{NativeProducer,nonce as managed_nonce};
#[cfg(target_os = "linux")]
pub(crate) use owned::{OwnedAccountSnapshot, OwnedStateChange};
#[cfg(target_os = "linux")]
pub(crate) use sink::OwnedEventSink;
mod state;
mod supervisor;
pub mod transport;

pub use config::HostConfig;
pub use discovery::{Discovered, DiscoveryProblem, discover, resolve_entry};
pub use error::{HostError, SecretError};
pub use handle::HostHandle;
#[cfg(target_os = "linux")]
pub(crate) use removal::RemovalStop;
pub use sink::{EventSink, MemorySecretStore, NullSink, SecretStore};
pub use state::{
    AccountInfo, AccountPhase, AccountSpec, PluginInfo, PluginSnapshot, PluginState, StateChange,
};
