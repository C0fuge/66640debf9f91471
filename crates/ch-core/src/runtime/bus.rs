//! The change bus: one broadcast of everything that changed in the core.

use std::sync::Arc;

use tokio::sync::broadcast;

use crate::host::StateChange;
use crate::ingest::Applied;

/// A change to the accounts list or an account's persisted configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountChange {
    /// A new account was persisted.
    Added {
        /// The new id.
        account_id: String,
        /// Its plugin.
        plugin_id: String,
    },
    /// The account and its data were deleted.
    Removed {
        /// The id.
        account_id: String,
        /// Its plugin.
        plugin_id: String,
    },
    /// "Wanted" was switched on or off.
    Enabled {
        /// The id.
        account_id: String,
        /// The new value.
        enabled: bool,
    },
    /// The settings (or secret settings) were saved.
    SettingsChanged {
        /// The id.
        account_id: String,
    },
}

/// Something changed in the core. Published after the change is persisted, so a subscriber
/// that reads the database sees it.
#[derive(Debug, Clone)]
#[non_exhaustive]
// Same trade-off as `StateChange` itself: matching on the variant beats boxing it.
#[allow(clippy::large_enum_variant)]
pub enum CoreChange {
    /// A plugin event was applied by ingest (no-ops are not published).
    Ingested {
        /// The plugin that sent the event.
        plugin_id: String,
        /// What changed.
        applied: Arc<Applied>,
    },
    /// A core-initiated effect (`Ingest::apply_*`) was applied by a service.
    Local {
        /// The service that applied it (`"outbox"`, `"history"`, …).
        service: &'static str,
        /// What changed.
        applied: Arc<Applied>,
    },
    /// The plugin host changed state (plugin lifecycle, account phase or status).
    Host(StateChange),
    /// The accounts list or an account's configuration changed.
    Account(AccountChange),
}

/// A broadcast of [`CoreChange`]s. Cheap to clone; every clone publishes to the same
/// subscribers. A slow subscriber gets `RecvError::Lagged` and should re-read what it shows.
#[derive(Debug, Clone)]
pub struct ChangeBus {
    tx: broadcast::Sender<CoreChange>,
}

impl ChangeBus {
    /// A bus that buffers up to `capacity` changes per subscriber.
    pub fn new(capacity: usize) -> Self {
        Self {
            tx: broadcast::channel(capacity.max(1)).0,
        }
    }

    /// Publish a change. Having no subscriber is fine.
    pub fn publish(&self, change: CoreChange) {
        let _ = self.tx.send(change);
    }

    /// Publish a local effect's result unless it changed nothing.
    pub fn publish_local(&self, service: &'static str, applied: Applied) {
        if !applied.is_noop() {
            self.publish(CoreChange::Local {
                service,
                applied: Arc::new(applied),
            });
        }
    }

    /// A new subscriber; it sees changes published from now on.
    pub fn subscribe(&self) -> broadcast::Receiver<CoreChange> {
        self.tx.subscribe()
    }

    /// Current subscriber count.
    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

impl Default for ChangeBus {
    fn default() -> Self {
        Self::new(1024)
    }
}
