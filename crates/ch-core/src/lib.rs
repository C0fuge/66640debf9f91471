//! Contact Hub core: ingest, plugin host, services, search, outbox.

pub mod history;
pub mod host;
pub mod ingest;
pub mod outbox;
pub mod runtime;
pub mod search;
pub mod secrets;

#[cfg(target_os = "linux")]
pub mod managed_stage;

#[cfg(target_os = "linux")]
mod managed_sources;
