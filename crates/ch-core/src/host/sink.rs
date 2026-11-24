//! The two seams between the host and the rest of the core: where events go
//! ([`EventSink`]) and where runtime secrets live ([`SecretStore`]).

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use ch_protocol::events::Event;

use super::error::SecretError;

/// Receives every event a plugin emits, in the order the plugin sent them, one at a time per
/// plugin. `deliver` may be slow (it writes to the database): the host queues a bounded number
/// of events in front of it and then stops reading the plugin's stdout, so memory never grows
/// without limit (spec §16). Events sent as requests are acknowledged to the plugin after
/// `deliver` returns.
///
/// The host has already dropped events for accounts that are not started (§4.2) and updated
/// its own account state from `status` events, so the sink only has to ingest.
#[async_trait]
pub trait EventSink: Send + Sync + 'static {
    /// Ingest one event from `plugin_id`. Returning an error acknowledges the event to the
    /// plugin as `INVALID_PARAMS` (when it was a request) and logs it; the host carries on.
    async fn deliver(&self, plugin_id: &str, event: Event) -> Result<(), String>;
}

/// A sink that drops everything (tests, tools).
#[derive(Debug, Default, Clone, Copy)]
pub struct NullSink;

#[async_trait]
impl EventSink for NullSink {
    async fn deliver(&self, _plugin_id: &str, _event: Event) -> Result<(), String> {
        Ok(())
    }
}

/// Runtime secrets per plugin and account (spec §6.2, §12.1): what a plugin stores with
/// `secrets.set` and gets back in `account.start.secrets`. The keyring implementation is a
/// later task; [`MemorySecretStore`] serves tests.
#[async_trait]
pub trait SecretStore: Send + Sync + 'static {
    /// Every secret of the account.
    async fn get_all(
        &self,
        plugin_id: &str,
        account_id: &str,
    ) -> Result<BTreeMap<String, String>, SecretError>;
    /// Store (or replace) one secret.
    async fn set(
        &self,
        plugin_id: &str,
        account_id: &str,
        key: &str,
        value: &str,
    ) -> Result<(), SecretError>;
    /// Delete one secret; deleting a missing key is not an error.
    async fn delete(&self, plugin_id: &str, account_id: &str, key: &str)
    -> Result<(), SecretError>;
}

/// An in-memory [`SecretStore`].
#[derive(Debug, Default)]
pub struct MemorySecretStore {
    inner: Mutex<BTreeMap<(String, String), BTreeMap<String, String>>>,
}

impl MemorySecretStore {
    /// Empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Synchronous read for assertions.
    pub fn snapshot(&self, plugin_id: &str, account_id: &str) -> BTreeMap<String, String> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(plugin_id.to_owned(), account_id.to_owned()))
            .cloned()
            .unwrap_or_default()
    }
}

#[async_trait]
impl SecretStore for MemorySecretStore {
    async fn get_all(
        &self,
        plugin_id: &str,
        account_id: &str,
    ) -> Result<BTreeMap<String, String>, SecretError> {
        Ok(self.snapshot(plugin_id, account_id))
    }

    async fn set(
        &self,
        plugin_id: &str,
        account_id: &str,
        key: &str,
        value: &str,
    ) -> Result<(), SecretError> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry((plugin_id.to_owned(), account_id.to_owned()))
            .or_default()
            .insert(key.to_owned(), value.to_owned());
        Ok(())
    }

    async fn delete(
        &self,
        plugin_id: &str,
        account_id: &str,
        key: &str,
    ) -> Result<(), SecretError> {
        if let Some(m) = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(&(plugin_id.to_owned(), account_id.to_owned()))
        {
            m.remove(key);
        }
        Ok(())
    }
}
