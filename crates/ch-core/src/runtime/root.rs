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
#[cfg(not(target_os="linux"))]
use super::error::RuntimeError;
use super::managed_proof::ManagedProofWriter;
use super::sink::IngestSink;
#[cfg(not(target_os="linux"))]
use super::sink::account_path_filter;
use crate::host::{HostConfig, HostHandle, SecretStore};
use crate::ingest::{Ingest, IngestOptions};

/// The database file inside the data directory.
pub const DB_FILE: &str = "hub.db";

// Non-Linux legacy composition retains its fail-closed startup boundary. Linux
// restores bounded negative barriers before Host construction, without replay or
// source-read/closure permissions. No setting bypasses historical protection.
#[cfg(not(target_os = "linux"))]
fn require_unmanaged_startup(db: &Db) -> Result<()> {
    let protected: bool = db.conn().query_row(
        "SELECT EXISTS(SELECT 1 FROM managed_source LIMIT 1)
             OR EXISTS(SELECT 1 FROM managed_dispatch LIMIT 1)
             OR EXISTS(SELECT 1 FROM managed_retirement LIMIT 1)
             OR EXISTS(SELECT 1 FROM managed_stage_publication LIMIT 1)
             OR EXISTS(SELECT 1 FROM managed_ready_retirement LIMIT 1)",
        [],
        |row| row.get(0),
    ).map_err(ch_db::Error::from)?;
    if protected {
        return Err(RuntimeError::ManagedLifetimeRecoveryUnavailable);
    }
    Ok(())
}

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
    managed_proofs: ManagedProofWriter,
    #[cfg(target_os = "linux")]
    logical_owners: super::LogicalOwners,
    #[cfg(target_os = "linux")]
    managed_sources: Arc<crate::managed_sources::ManagedSources>,
    #[cfg(target_os="linux")]
    stage_publisher:crate::managed_stage::StagePublisher,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl std::fmt::Debug for Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Core")
            .field("db_path", &self.db_path)
            .finish_non_exhaustive()
    }
}

impl Drop for Core {
    fn drop(&mut self) {
        // Revocation only. Synchronous Drop cannot certify worker settlement;
        // the App-owned shutdown still must await the actual drain above.
        #[cfg(target_os = "linux")]
        self.logical_owners.close();
        #[cfg(target_os="linux")]
        self.stage_publisher.close();
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
        let (ingest_db, accounts_db, proof_db) =
            tokio::task::spawn_blocking(move || -> Result<(Db, Db, Option<Db>)> {
                // The first open migrates; the second finds the schema current.
                let a = Db::open(&p)?;
                // Before constructing Host/Accounts or spawning any background task.
                #[cfg(not(target_os = "linux"))]
                require_unmanaged_startup(&a)?;
                let b = Db::open(&p)?;
                // Ordinary storage historically tolerates unavailable WAL. Do not
                // turn the dormant proof seam into a new startup requirement.
                let c = match Db::open_full_sync(&p) {
                    Ok(db) => Some(db),
                    Err(_) => {
                        warn!("managed proof durability unavailable; durable transactions disabled");
                        None
                    }
                };
                Ok((a, b, c))
            })
            .await??;

        let bus = ChangeBus::new(config.bus_capacity);
        let mut opts = config.ingest;
        #[cfg(target_os="linux")]
        let catalog=crate::managed_stage::catalog::PrivateByteCatalog::for_core(&host_config);
        #[cfg(target_os="linux")]
        {opts.path_filter=Some(super::sink::account_path_filter_counted(&host_config,catalog.clone()));}
        #[cfg(not(target_os="linux"))]
        {opts.path_filter = Some(account_path_filter(&host_config));}
        #[cfg(target_os = "linux")]
        let logical_owners = super::LogicalOwners::new();
        #[cfg(target_os = "linux")]
        let sink = Arc::new(IngestSink::for_core(Ingest::new(ingest_db, opts), bus.clone(), logical_owners.clone()));
        #[cfg(not(target_os = "linux"))]
        let sink = Arc::new(IngestSink::new(Ingest::new(ingest_db, opts), bus.clone()));
        let managed_proofs = ManagedProofWriter::new(sink.shared_ingest(),proof_db,sink.reference_queue());
        #[cfg(target_os = "linux")]
        let managed_sources = crate::managed_sources::ManagedSources::new(managed_proofs.clone(),catalog);
        #[cfg(target_os = "linux")]
        managed_sources.restore_negative(sink.shared_ingest(),&host_config).await?;
        #[cfg(target_os = "linux")]
        managed_sources.initialize_catalog().await;
        #[cfg(target_os = "linux")]
        let host = HostHandle::for_core(host_config, sink.clone(), Arc::clone(&config.secrets),
            &managed_sources);
        #[cfg(not(target_os = "linux"))]
        let host = HostHandle::new(host_config, sink.clone(), Arc::clone(&config.secrets));
        #[cfg(target_os = "linux")]
        let host_rx = host.subscribe_owned();
        #[cfg(not(target_os = "linux"))]
        let host_rx = host.subscribe();
        #[cfg(target_os = "linux")]
        let accounts = Accounts::for_core(accounts_db, host.clone(), config.secrets, bus.clone(),
            &managed_sources, logical_owners.clone());
        #[cfg(not(target_os = "linux"))]
        let accounts = Accounts::new(accounts_db, host.clone(), config.secrets, bus.clone());
        #[cfg(target_os = "linux")]
        host.bind_storage(accounts.host_storage_view()?)?;
        #[cfg(target_os="linux")]
        let stage_publisher=crate::managed_stage::StagePublisher::for_core(
            accounts.host_storage_view()?,managed_sources.copy_jobs(),managed_proofs.clone(),host.protected_inputs()?);
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
            managed_proofs,
            #[cfg(target_os = "linux")]
            logical_owners,
            #[cfg(target_os = "linux")]
            managed_sources,
            #[cfg(target_os="linux")]
            stage_publisher,
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

    #[cfg(all(test, target_os = "linux"))]
    pub(crate) async fn stop_host_forwarding_for_test(&self) {
        let task = self.tasks.lock().unwrap().first().map(JoinHandle::abort_handle);
        if let Some(task) = task { task.abort(); }
        tokio::task::yield_now().await;
    }

    /// The ingest sink: the main writer, for core-initiated effects.
    pub fn ingest(&self) -> &Arc<IngestSink> {
        &self.sink
    }

    /// Native, bounded FULL transaction writer. This grants no file or send authority.
    pub fn managed_proofs(&self) -> &ManagedProofWriter {
        &self.managed_proofs
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn managed_sources(&self) -> Arc<crate::managed_sources::ManagedSources> {
        Arc::clone(&self.managed_sources)
    }

    /// Opaque, invalidation-only hook for actual private-stage FS operations.
    #[cfg(target_os = "linux")]
    pub fn stage_mutations(&self) -> crate::managed_stage::StageMutations {
        self.managed_sources.stage_mutations()
    }

    /// Actual Core-owned copying service; no source-read or cleanup grant.
    #[cfg(target_os="linux")]
    pub fn stage_publisher(&self)->&crate::managed_stage::StagePublisher {&self.stage_publisher}

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
        #[cfg(target_os="linux")]
        self.sink.close_work();
        #[cfg(target_os = "linux")]
        self.managed_sources.close();
        self.managed_proofs.close();
        let tasks: Vec<_> =
            std::mem::take(&mut *self.tasks.lock().unwrap_or_else(PoisonError::into_inner));
        // The autostart task first, so nothing starts while we stop.
        if let Some(t) = tasks.get(1) {
            t.abort();
        }
        self.host.shutdown().await;
        // Native replies may use tickets reserved before close. Host is now
        // settled; drain their actual writes before revoking logical metadata.
        if let Err(e) = self.managed_proofs.drain().await {
            warn!("draining managed proof writes: {e}");
        }
        if let Err(e) = self.accounts.resync_from_host().await {
            warn!("persisting account state at shutdown: {e}");
        }
        #[cfg(target_os = "linux")]
        {
            self.logical_owners.close();
            if self.logical_owners.drain().await.is_err() {
                warn!("draining native metadata workers failed");
            }
        }
        for t in &tasks {
            t.abort();
        }
        for t in tasks {
            let _ = t.await;
        }
        #[cfg(target_os = "linux")]
        if self.managed_sources.drain().await.is_err() {
            warn!("draining managed local admission jobs failed");
        }
        #[cfg(target_os="linux")]
        if let Err(e)=self.sink.checkpoint_after_close().await {warn!("ingest shutdown/checkpoint: {e}");}
        #[cfg(not(target_os="linux"))]
        {match self.sink.with_ingest(|i| i.db().checkpoint()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("WAL checkpoint: {e}"),
            Err(e) => warn!("WAL checkpoint: {e}"),
        }}
        info!("core shut down");
    }
}

/// Persist host state changes, then republish them on the bus.
async fn forward_host(
    #[cfg(target_os = "linux")] mut rx: broadcast::Receiver<crate::host::OwnedStateChange>,
    #[cfg(not(target_os = "linux"))] mut rx: broadcast::Receiver<crate::host::StateChange>,
    accounts: Accounts,
    bus: ChangeBus,
) {
    loop {
        match rx.recv().await {
            Ok(change) => {
                #[cfg(target_os = "linux")]
                match accounts.on_owned_host_change(&change).await {
                    Ok(Some(change)) => bus.publish(CoreChange::Host(change)),
                    Ok(None) => {},
                    Err(error) => warn!("persisting native host state: {error}"),
                }
                #[cfg(not(target_os = "linux"))]
                {
                    if let Err(e) = accounts.on_host_change(&change).await { warn!("persisting host state: {e}"); }
                    bus.publish(CoreChange::Host(change));
                }
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
