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
pub mod settings;
mod sink;
mod state;
mod supervisor;
pub mod transport;

pub use config::HostConfig;
pub use discovery::{Discovered, DiscoveryProblem, discover, resolve_entry};
pub use error::{HostError, SecretError};
pub use handle::HostHandle;
pub use sink::{EventSink, MemorySecretStore, NullSink, SecretStore};
pub use state::{
    AccountInfo, AccountPhase, AccountSpec, PluginInfo, PluginSnapshot, PluginState, StateChange,
};
