//! [`Core`]: the composition root.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use ch_db::Db;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::accounts::Accounts;
use super::bus::{ChangeBus, CoreChange};
use super::error::Result;
use super::sink::{IngestSink, account_path_filter};
use crate::host::{HostConfig, HostHandle, SecretStore};
use crate::ingest::{Ingest, IngestOptions};

/// The database file inside the data directory.
pub const DB_FILE: &str = "hub.db";

/// The data directory per ADR 0001: `CH_DATA_DIR`, else `$XDG_DATA_HOME/contact-hub-ng`, else
/// `$HOME/.local/share/contact-hub-ng`. `None` when none of those is set.
pub fn default_data_dir() -> Option<PathBuf> {
    let non_empty = |k: &str| {
        std::env::var_os(k)
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    };
    if let Some(d) = non_empty("CH_DATA_DIR") {
        return Some(d);
    }
    if let Some(x) = non_empty("XDG_DATA_HOME") {
        return Some(x.join("contact-hub-ng"));
    }
    non_empty("HOME").map(|h| h.join(".local").join("share").join("contact-hub-ng"))
}

/// How to open a [`Core`].
pub struct CoreConfig {
    /// The data directory: `hub.db`, `accounts/`, `plugins/`, `logs/`.
    pub data_dir: PathBuf,
    /// Built-in plugins (`<install>/plugins/`).
    pub builtin_plugins_dir: Option<PathBuf>,
    /// User plugins; `None` = `<data_dir>/plugins`.
    pub user_plugins_dir: Option<PathBuf>,
    /// Host policy, timers and plugin environment. Its paths are replaced by the fields above.
    pub host: HostConfig,
    /// Where runtime and setting secrets live (the keyring in the app).
    pub secrets: Arc<dyn SecretStore>,
    /// Ingest options. The path filter is always the account-directory rule (§15).
    pub ingest: IngestOptions,
    /// Start every wanted account on open (in the background). Default `true`.
    pub autostart: bool,
    /// Change bus buffer per subscriber.
    pub bus_capacity: usize,
}

impl CoreConfig {
    /// The spec's host policy over `data_dir`, with `secrets`.
    pub fn new(data_dir: impl Into<PathBuf>, secrets: Arc<dyn SecretStore>) -> Self {
        let data_dir = data_dir.into();
        Self {
            host: HostConfig::new(&data_dir),
            data_dir,
            builtin_plugins_dir: None,
            user_plugins_dir: None,
            secrets,
            ingest: IngestOptions::default(),
            autostart: true,
            bus_capacity: 1024,
        }
    }

    fn host_config(&self) -> std::io::Result<HostConfig> {
        let data_dir = std::path::absolute(&self.data_dir)?;
        let mut h = self.host.clone();
        h.user_plugins_dir = match &self.user_plugins_dir {
            Some(p) => std::path::absolute(p)?,
            None => data_dir.join("plugins"),
        };
        h.builtin_plugins_dir = self
            .builtin_plugins_dir
            .as_deref()
            .map(std::path::absolute)
            .transpose()?;
        if h.plugin_log_dir.is_some() {
            h.plugin_log_dir = Some(data_dir.join("logs").join("plugins"));
        }
        h.data_dir = data_dir;
        Ok(h)
    }
}

/// The running core: database, plugin host, ingest, accounts and the change bus.
pub struct Core {
    db_path: PathBuf,
    host: HostHandle,
    accounts: Accounts,
    bus: ChangeBus,
    sink: Arc<IngestSink>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl std::fmt::Debug for Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Core")
            .field("db_path", &self.db_path)
            .finish_non_exhaustive()
    }
}

impl Core {
    /// Open the database at `<data_dir>/hub.db` (creating and migrating it), discover plugins,
    /// and (with `autostart`) start every wanted account in the background.
    pub async fn open(config: CoreConfig) -> Result<Core> {
        let host_config = config.host_config()?;
        tokio::fs::create_dir_all(&host_config.data_dir).await?;
        tokio::fs::create_dir_all(&host_config.user_plugins_dir).await?;
        let db_path = host_config.data_dir.join(DB_FILE);

        let p = db_path.clone();
        let (ingest_db, accounts_db) =
            tokio::task::spawn_blocking(move || -> ch_db::Result<(Db, Db)> {
                // The first open migrates; the second finds the schema current.
                let a = Db::open(&p)?;
                let b = Db::open(&p)?;
                Ok((a, b))
            })
            .await??;

        let bus = ChangeBus::new(config.bus_capacity);
        let mut opts = config.ingest;
        opts.path_filter = Some(account_path_filter(&host_config));
        let sink = Arc::new(IngestSink::new(Ingest::new(ingest_db, opts), bus.clone()));
        let host = HostHandle::new(host_config, sink.clone(), Arc::clone(&config.secrets));
        let host_rx = host.subscribe();
        let accounts = Accounts::new(accounts_db, host.clone(), config.secrets, bus.clone());
        accounts.sync_plugins().await?;

        let mut tasks = vec![tokio::spawn(forward_host(
            host_rx,
            accounts.clone(),
            bus.clone(),
        ))];
        if config.autostart {
            let a = accounts.clone();
            tasks.push(tokio::spawn(async move {
                if let Err(e) = a.start_wanted().await {
                    warn!("starting wanted accounts: {e}");
                }
            }));
        }
        info!(db = %db_path.display(), "core open");
        Ok(Core {
            db_path,
            host,
            accounts,
            bus,
            sink,
            tasks: Mutex::new(tasks),
        })
    }

    /// The plugin host.
    pub fn host(&self) -> &HostHandle {
        &self.host
    }

    /// The accounts service.
    pub fn accounts(&self) -> &Accounts {
        &self.accounts
    }

    /// The change bus (services publish their local effects here).
    pub fn bus(&self) -> &ChangeBus {
        &self.bus
    }

    /// Subscribe to every change from now on.
    pub fn subscribe(&self) -> broadcast::Receiver<CoreChange> {
        self.bus.subscribe()
    }

    /// The ingest sink: the main writer, for core-initiated effects.
    pub fn ingest(&self) -> &Arc<IngestSink> {
        &self.sink
    }

    /// `<data_dir>/hub.db`. Services open their own connection with [`Core::open_db`].
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// The data directory.
    pub fn data_dir(&self) -> &Path {
        self.host.config().data_dir()
    }

    /// A new connection to the database (blocking; call it off the async threads or at
    /// service start-up).
    pub fn open_db(&self) -> ch_db::Result<Db> {
        Db::open(&self.db_path)
    }

    /// Stop every plugin gracefully (§4.5), persist the accounts' final state, checkpoint the
    /// WAL. Accounts stay wanted in the database and resume on the next open.
    pub async fn shutdown(&self) {
        let tasks: Vec<_> =
            std::mem::take(&mut *self.tasks.lock().unwrap_or_else(PoisonError::into_inner));
        // The autostart task first, so nothing starts while we stop.
        if let Some(t) = tasks.get(1) {
            t.abort();
        }
        self.host.shutdown().await;
        if let Err(e) = self.accounts.resync_from_host().await {
            warn!("persisting account state at shutdown: {e}");
        }
        for t in &tasks {
            t.abort();
        }
        for t in tasks {
            let _ = t.await;
        }
        match self.sink.with_ingest(|i| i.db().checkpoint()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("WAL checkpoint: {e}"),
            Err(e) => warn!("WAL checkpoint: {e}"),
        }
        info!("core shut down");
    }
}

/// Persist host state changes, then republish them on the bus.
async fn forward_host(
    mut rx: broadcast::Receiver<crate::host::StateChange>,
    accounts: Accounts,
    bus: ChangeBus,
) {
    loop {
        match rx.recv().await {
            Ok(change) => {
                if let Err(e) = accounts.on_host_change(&change).await {
                    warn!("persisting host state: {e}");
                }
                bus.publish(CoreChange::Host(change));
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("host state subscription lagged by {n}; resyncing");
                if let Err(e) = accounts.resync_from_host().await {
                    warn!("persisting host state: {e}");
                }
                if let Err(e) = accounts.sync_plugins().await {
                    warn!("syncing plugins: {e}");
                }
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}
