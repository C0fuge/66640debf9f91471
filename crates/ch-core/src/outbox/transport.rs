//! The seam between the outbox and the plugin host. [`HostHandle`] implements it; tests use a
//! scripted fake so the state machine can run on a paused clock without a plugin process.

use async_trait::async_trait;
use ch_protocol::events::AccountState;
use serde_json::Value;
use std::sync::{Arc, OnceLock};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::host::{AccountInfo, AccountPhase, HostError, HostHandle, StateChange};

/// What the outbox needs from the host: send one request, read an account's state, and hear
/// about account state changes.
#[async_trait]
pub trait SendTransport: Send + Sync + 'static {
    /// Call `method` on `plugin_id` with `params`. Cancelling `cancel` sends `$/cancelRequest`;
    /// the plugin may still answer with the result if it had already finished (§11.0).
    async fn call(
        &self,
        plugin_id: &str,
        method: &str,
        params: Value,
        cancel: CancellationToken,
        request_id: Arc<OnceLock<i64>>,
    ) -> Result<Value, HostError>;

    /// The host's view of an account, `None` when the host does not know it (yet).
    fn account(&self, plugin_id: &str, account_id: &str) -> Option<AccountInfo>;

    /// Host state changes; the outbox listens for [`StateChange::Account`].
    fn subscribe(&self) -> broadcast::Receiver<StateChange>;
}

#[async_trait]
impl SendTransport for HostHandle {
    async fn call(
        &self,
        plugin_id: &str,
        method: &str,
        params: Value,
        cancel: CancellationToken,
        request_id: Arc<OnceLock<i64>>,
    ) -> Result<Value, HostError> {
        self.call_raw_tracked(plugin_id, method, params, Some(cancel), Some(request_id))
            .await
    }

    fn account(&self, plugin_id: &str, account_id: &str) -> Option<AccountInfo> {
        HostHandle::account(self, plugin_id, account_id)
    }

    fn subscribe(&self) -> broadcast::Receiver<StateChange> {
        HostHandle::subscribe(self)
    }
}

/// Can this account take sends right now: the host has it `Running` and the plugin reports
/// `connected`.
pub fn is_online(info: Option<&AccountInfo>) -> bool {
    info.is_some_and(|a| a.phase == AccountPhase::Running && a.state == AccountState::Connected)
}
