//! The [`HostHandle`] the services use.

use std::sync::{Arc, OnceLock};

use ch_protocol::{CallDirection, ErrorObject, REQUIRED_METHODS};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::config::HostConfig;
use super::discovery::{Discovered, discover};
use super::error::HostError;
use super::sink::{EventSink, SecretStore};
use super::state::{
    AccountInfo, AccountSpec, PluginInfo, PluginSnapshot, PluginState, StateChange,
};
use super::supervisor::{Command, PluginEntry, Registry, Supervisor};

/// Lifecycle methods only the host sends (§4); `call` refuses them.
const LIFECYCLE: &[&str] = &[
    "initialize",
    "shutdown",
    "ping",
    "account.start",
    "account.stop",
];

struct Inner {
    config: Arc<HostConfig>,
    registry: Arc<Registry>,
    sink: Arc<dyn EventSink>,
    secrets: Arc<dyn SecretStore>,
}

/// The plugin host. Cheap to clone; every clone talks to the same host.
#[derive(Clone)]
pub struct HostHandle(Arc<Inner>);

impl HostHandle {
    /// Create a host and discover plugins. Nothing is spawned until an account is started or
    /// [`HostHandle::enable_plugin`] is called.
    pub fn new(
        config: HostConfig,
        sink: Arc<dyn EventSink>,
        secrets: Arc<dyn SecretStore>,
    ) -> HostHandle {
        let handle = HostHandle(Arc::new(Inner {
            config: Arc::new(config),
            registry: Arc::new(Registry::new()),
            sink,
            secrets,
        }));
        handle.rescan();
        handle
    }

    /// The configuration.
    pub fn config(&self) -> &HostConfig {
        &self.0.config
    }

    /// Re-read the plugin directories (§3.1). Plugins that disappeared but still run keep
    /// running until stopped; new ones appear as `Stopped` (or `Unavailable`). A running
    /// plugin whose manifest changed keeps the old manifest until restarted.
    pub fn rescan(&self) {
        let found = discover(
            self.0.config.builtin_plugins_dir.as_deref(),
            &self.0.config.user_plugins_dir,
        );
        {
            let mut g = self.0.registry.lock();
            for d in found {
                let id = d.id();
                let info = to_info(&d);
                match g.get_mut(&id) {
                    Some(e) if e.cmd.is_some() => {
                        // Live: only refresh static facts that do not affect the process.
                        e.info.dir = info.dir;
                        e.info.builtin = info.builtin;
                    }
                    Some(e) => {
                        e.info = info;
                        e.state = match d.problem {
                            Some(p) => PluginState::Unavailable(p),
                            None => PluginState::Stopped,
                        };
                    }
                    None => {
                        let state = match d.problem {
                            Some(p) => PluginState::Unavailable(p),
                            None => PluginState::Stopped,
                        };
                        g.insert(
                            id,
                            PluginEntry {
                                info,
                                state,
                                accounts: Default::default(),
                                cmd: None,
                            },
                        );
                    }
                }
            }
        }
        let _ = self.0.registry.changes.send(StateChange::PluginsRescanned);
    }

    /// Every discovered plugin with its state and accounts, sorted by id.
    pub fn plugins(&self) -> Vec<PluginSnapshot> {
        self.0
            .registry
            .lock()
            .values()
            .map(|e| PluginSnapshot {
                info: e.info.clone(),
                state: e.state.clone(),
                accounts: e.accounts.values().map(|r| r.info.clone()).collect(),
            })
            .collect()
    }

    /// One plugin.
    pub fn plugin(&self, plugin_id: &str) -> Option<PluginSnapshot> {
        self.0
            .registry
            .lock()
            .get(plugin_id)
            .map(|e| PluginSnapshot {
                info: e.info.clone(),
                state: e.state.clone(),
                accounts: e.accounts.values().map(|r| r.info.clone()).collect(),
            })
    }

    /// The state of one plugin.
    pub fn plugin_state(&self, plugin_id: &str) -> Option<PluginState> {
        self.0.registry.plugin_state(plugin_id)
    }

    /// Every account the host knows, across plugins.
    pub fn accounts(&self) -> Vec<AccountInfo> {
        self.0
            .registry
            .lock()
            .values()
            .flat_map(|e| e.accounts.values().map(|r| r.info.clone()))
            .collect()
    }

    /// One account.
    pub fn account(&self, plugin_id: &str, account_id: &str) -> Option<AccountInfo> {
        self.0
            .registry
            .lock()
            .get(plugin_id)?
            .accounts
            .get(account_id)
            .map(|r| r.info.clone())
    }

    /// State changes as they happen. A slow subscriber may see `Lagged`; re-read the
    /// snapshot then.
    pub fn subscribe(&self) -> broadcast::Receiver<StateChange> {
        self.0.registry.changes.subscribe()
    }

    /// Spawn the plugin process and complete the handshake without starting accounts.
    /// Returns once the plugin is `Running` (or fails to get there).
    pub async fn enable_plugin(&self, plugin_id: &str) -> Result<(), HostError> {
        self.ensure_supervisor(plugin_id)?;
        let mut rx = self.subscribe();
        loop {
            match self.plugin_state(plugin_id) {
                Some(PluginState::Running { .. }) => return Ok(()),
                Some(PluginState::Starting) | None => {}
                Some(state) => {
                    return Err(HostError::PluginNotRunning {
                        plugin_id: plugin_id.to_owned(),
                        state,
                    });
                }
            }
            match rx.recv().await {
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => return Err(HostError::ShuttingDown),
            }
        }
    }

    /// Graceful `shutdown` of one plugin (§4.5). Its accounts are no longer wanted.
    pub async fn disable_plugin(&self, plugin_id: &str) -> Result<(), HostError> {
        let cmd = self.supervisor(plugin_id)?;
        let Some(cmd) = cmd else { return Ok(()) };
        let (tx, rx) = oneshot::channel();
        if cmd.send(Command::Shutdown { reply: tx }).await.is_ok() {
            let _ = rx.await;
        }
        Ok(())
    }

    /// The "Restart" button: from `Failed`/`Incompatible`/`Restarting`, try again now with a
    /// fresh failure count; from `Running`, graceful stop and start again (accounts that were
    /// started are resumed).
    pub async fn restart_plugin(&self, plugin_id: &str) -> Result<(), HostError> {
        match self.supervisor(plugin_id)? {
            None => {
                self.ensure_supervisor(plugin_id)?;
                Ok(())
            }
            Some(cmd) => {
                let (tx, rx) = oneshot::channel();
                if cmd.send(Command::Restart { reply: tx }).await.is_ok() {
                    let _ = rx.await;
                }
                Ok(())
            }
        }
    }

    /// Start an account (§4.2): spawns the plugin if needed, waits for the handshake, sends
    /// `account.start` with defaults and secrets filled in, and returns when the plugin has
    /// accepted it. While the plugin is in a restart backoff the wish is recorded and the
    /// account starts with the plugin.
    pub async fn start_account(
        &self,
        plugin_id: &str,
        account_id: &str,
        spec: AccountSpec,
    ) -> Result<(), HostError> {
        let cmd = self.ensure_supervisor(plugin_id)?;
        let (tx, rx) = oneshot::channel();
        cmd.send(Command::StartAccount {
            account_id: account_id.to_owned(),
            spec,
            reply: tx,
        })
        .await
        .map_err(|_| HostError::ShuttingDown)?;
        rx.await.map_err(|_| HostError::ShuttingDown)?
    }

    /// Stop an account and wait for the plugin's reply (§4.2). The account is marked
    /// `disconnected` whatever the plugin says; `UNKNOWN_ACCOUNT` counts as stopped.
    pub async fn stop_account(&self, plugin_id: &str, account_id: &str) -> Result<(), HostError> {
        let Some(cmd) = self.supervisor(plugin_id)? else {
            // No process: nothing to stop, but forget the wish.
            let known = self
                .0
                .registry
                .update_account(plugin_id, account_id, |r| r.info.wanted = false)
                .is_some();
            return if known {
                Ok(())
            } else {
                Err(HostError::UnknownAccount(account_id.to_owned()))
            };
        };
        let (tx, rx) = oneshot::channel();
        cmd.send(Command::StopAccount {
            account_id: account_id.to_owned(),
            reply: tx,
        })
        .await
        .map_err(|_| HostError::ShuttingDown)?;
        rx.await.map_err(|_| HostError::ShuttingDown)?
    }

    /// Call a core→plugin method with raw JSON params. Before anything is sent, the host
    /// checks the registry: the method must be core→plugin and not a lifecycle method, the
    /// plugin must declare its capability, and no present gated parameter may lack its
    /// capability (§14.2) — such calls fail with `NOT_SUPPORTED` locally. Timeouts come from
    /// the registry (§4.6); `cancel` sends `$/cancelRequest` and the call still waits for the
    /// plugin's reply (`CANCELLED` or the result).
    pub async fn call_raw(
        &self,
        plugin_id: &str,
        method: &str,
        params: Value,
        cancel: Option<CancellationToken>,
    ) -> Result<Value, HostError> {
        self.call_raw_tracked(plugin_id, method, params, cancel, None)
            .await
    }

    /// As call_raw, with a fresh cell for the wire id. The supervisor fills it before
    /// writing the request so even the first progress event can be correlated.
    pub async fn call_raw_tracked(
        &self,
        plugin_id: &str,
        method: &str,
        params: Value,
        cancel: Option<CancellationToken>,
        request_id: Option<Arc<OnceLock<i64>>>,
    ) -> Result<Value, HostError> {
        let (cmd, state) = {
            let g = self.0.registry.lock();
            let e = g
                .get(plugin_id)
                .ok_or_else(|| HostError::UnknownPlugin(plugin_id.to_owned()))?;
            (e.cmd.clone(), e.state.clone())
        };
        let PluginState::Running { capabilities, .. } = &state else {
            return Err(HostError::PluginNotRunning {
                plugin_id: plugin_id.to_owned(),
                state,
            });
        };
        let Some(cmd) = cmd else {
            return Err(HostError::PluginNotRunning {
                plugin_id: plugin_id.to_owned(),
                state: PluginState::Stopped,
            });
        };
        if LIFECYCLE.contains(&method) {
            return Err(HostError::InvalidMethod(
                method.to_owned(),
                "lifecycle methods are sent by the host",
            ));
        }
        match ch_protocol::method(method) {
            Some(info) => {
                if info.direction != CallDirection::CoreToPlugin {
                    return Err(HostError::InvalidMethod(
                        method.to_owned(),
                        "plugin→core method",
                    ));
                }
                if let Some(cap) = info.capability
                    && !capabilities.has(cap)
                {
                    return Err(HostError::rpc(ErrorObject::not_supported(cap)));
                }
                if let Some(g) = info
                    .missing_capabilities(&params, |c| capabilities.has(c))
                    .next()
                {
                    return Err(HostError::rpc(
                        ErrorObject::not_supported(g.capability).with_data(|d| {
                            d.field = Some(g.param.to_owned());
                        }),
                    ));
                }
            }
            None => {
                let vendor_prefix = format!("x-{plugin_id}.");
                if !method.starts_with(&vendor_prefix) {
                    return Err(HostError::InvalidMethod(
                        method.to_owned(),
                        "not in the registry and not this plugin's vendor method",
                    ));
                }
            }
        }
        if !params.is_object() {
            return Err(HostError::InvalidMethod(
                method.to_owned(),
                "params must be an object",
            ));
        }
        let _ = REQUIRED_METHODS; // documented in ch-protocol; every plugin has them
        let (tx, rx) = oneshot::channel();
        cmd.send(Command::Call {
            method: method.to_owned(),
            params,
            cancel,
            request_id,
            reply: tx,
        })
        .await
        .map_err(|_| HostError::ShuttingDown)?;
        rx.await.map_err(|_| HostError::ShuttingDown)?
    }

    /// Typed [`call_raw`](Self::call_raw): params from `ch_protocol::methods`, result decoded.
    pub async fn call<P: Serialize, R: DeserializeOwned>(
        &self,
        plugin_id: &str,
        method: &str,
        params: &P,
    ) -> Result<R, HostError> {
        let v = self
            .call_raw(plugin_id, method, serde_json::to_value(params)?, None)
            .await?;
        Ok(serde_json::from_value(v)?)
    }

    /// Typed call with a cancellation token.
    pub async fn call_cancellable<P: Serialize, R: DeserializeOwned>(
        &self,
        plugin_id: &str,
        method: &str,
        params: &P,
        cancel: CancellationToken,
    ) -> Result<R, HostError> {
        let v = self
            .call_raw(
                plugin_id,
                method,
                serde_json::to_value(params)?,
                Some(cancel),
            )
            .await?;
        Ok(serde_json::from_value(v)?)
    }

    /// Graceful shutdown of every running plugin, in parallel (app quit).
    pub async fn shutdown(&self) {
        let cmds: Vec<(String, mpsc::Sender<Command>)> = self
            .0
            .registry
            .lock()
            .iter()
            .filter_map(|(id, e)| e.cmd.clone().map(|c| (id.clone(), c)))
            .collect();
        let mut waits = Vec::new();
        for (id, cmd) in cmds {
            let (tx, rx) = oneshot::channel();
            if cmd.send(Command::Shutdown { reply: tx }).await.is_ok() {
                waits.push((id, rx));
            }
        }
        for (id, rx) in waits {
            let _ = rx.await;
            info!(plugin = %id, "shut down");
        }
    }

    // ---------------------------------------------------------------------------------

    /// The command channel of a known plugin, if a supervisor runs.
    fn supervisor(&self, plugin_id: &str) -> Result<Option<mpsc::Sender<Command>>, HostError> {
        let g = self.0.registry.lock();
        let e = g
            .get(plugin_id)
            .ok_or_else(|| HostError::UnknownPlugin(plugin_id.to_owned()))?;
        Ok(e.cmd.clone())
    }

    /// Start a supervisor task for the plugin unless one runs.
    fn ensure_supervisor(&self, plugin_id: &str) -> Result<mpsc::Sender<Command>, HostError> {
        let mut g = self.0.registry.lock();
        let e = g
            .get_mut(plugin_id)
            .ok_or_else(|| HostError::UnknownPlugin(plugin_id.to_owned()))?;
        if let Some(c) = &e.cmd {
            return Ok(c.clone());
        }
        if let PluginState::Unavailable(_) = &e.state {
            return Err(HostError::PluginNotRunning {
                plugin_id: plugin_id.to_owned(),
                state: e.state.clone(),
            });
        }
        let (tx, rx) = mpsc::channel(64);
        e.cmd = Some(tx.clone());
        // Set synchronously so a caller never observes `Stopped` with a live supervisor.
        e.state = PluginState::Starting;
        let sup = Supervisor {
            plugin_id: plugin_id.to_owned(),
            info: e.info.clone(),
            config: Arc::clone(&self.0.config),
            registry: Arc::clone(&self.0.registry),
            sink: Arc::clone(&self.0.sink),
            secrets: Arc::clone(&self.0.secrets),
            rx,
            failures: 0,
            last_failure: String::new(),
        };
        tokio::spawn(sup.run());
        Ok(tx)
    }
}

fn to_info(d: &Discovered) -> PluginInfo {
    PluginInfo {
        id: d.id(),
        dir: d.dir.clone(),
        builtin: d.builtin,
        manifest: d.manifest.clone(),
        executable: d.executable.clone(),
    }
}
