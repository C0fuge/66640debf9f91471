//! The slice of the plugin host the history service needs, as a trait so tests can script a
//! plugin without spawning one.

use std::sync::Arc;

use async_trait::async_trait;
use ch_protocol::capabilities::Capabilities;
use serde_json::Value;

use crate::host::{HostError, HostHandle};

/// Calls into plugins. [`HostHandle`] implements it; tests implement it with a scripted platform.
#[async_trait]
pub trait PluginApi: Send + Sync + 'static {
    /// The capabilities of a running plugin; `None` when it is not running.
    fn capabilities(&self, plugin_id: &str) -> Option<Arc<Capabilities>>;

    /// Call a core→plugin method with JSON params (the host checks capabilities and gated
    /// params again and applies the registry timeout).
    async fn call(&self, plugin_id: &str, method: &str, params: Value) -> Result<Value, HostError>;
}

#[async_trait]
impl PluginApi for HostHandle {
    fn capabilities(&self, plugin_id: &str) -> Option<Arc<Capabilities>> {
        self.plugin_state(plugin_id)
            .and_then(|s| s.capabilities().cloned())
    }

    async fn call(&self, plugin_id: &str, method: &str, params: Value) -> Result<Value, HostError> {
        self.call_raw(plugin_id, method, params, None).await
    }
}
