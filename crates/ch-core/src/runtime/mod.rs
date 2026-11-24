//! Runtime: wires the plugin host to ingest, persists accounts and settings, and publishes changes.
//!
//! - [`Core`]: the composition root. Opens `<data_dir>/hub.db`, builds the [`HostHandle`] with an
//!   [`IngestSink`], runs the [`Accounts`] service and forwards everything to the [`ChangeBus`].
//! - [`IngestSink`]: the host's [`EventSink`] over [`Ingest`]; applies each event on a blocking
//!   task, in order, and publishes the [`Applied`] result.
//! - [`ChangeBus`]: a broadcast of [`CoreChange`]s (ingest results, local effects, host state,
//!   account changes) for the UI layer and the other services.
//! - [`Accounts`]: the `account` table: add, remove, enable/disable ("wanted"), per-account
//!   settings validated against the plugin's `settings_schema`, `x-secret` values routed through
//!   the [`SecretStore`], start of every wanted account on launch.
//!
//! DB connection model: every service opens its own [`ch_db::Db`] on [`Core::db_path`] (WAL; the
//! 5 s busy timeout serialises writers). Ingest is the main writer; the accounts service has its
//! own connection.
//!
//! [`HostHandle`]: crate::host::HostHandle
//! [`EventSink`]: crate::host::EventSink
//! [`SecretStore`]: crate::host::SecretStore
//! [`Ingest`]: crate::ingest::Ingest
//! [`Applied`]: crate::ingest::Applied

mod accounts;
mod bus;
mod error;
mod root;
mod sink;

pub use accounts::{
    AccountSettings, Accounts, NewAccount, SpecDefaults, settings_secret_namespace,
    validate_settings,
};
pub use bus::{AccountChange, ChangeBus, CoreChange};
pub use error::{RuntimeError, SettingsProblem};
pub use root::{Core, CoreConfig, DB_FILE, default_data_dir};
pub use sink::{IngestSink, account_path_filter};
