//! [`IngestSink`]: the host's [`EventSink`] over [`Ingest`].

use std::path::{Component, Path};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use ch_protocol::events::Event;
use tracing::{error, warn};

use super::bus::{ChangeBus, CoreChange};
use super::error::Result;
use crate::host::{EventSink, HostConfig};
use crate::ingest::{Ingest, IngestError, IngestOptions, PathFilter};

/// Applies plugin events through one [`Ingest`] and publishes the result on the bus.
/// The Linux Core Host uses a private original-owner envelope; raw native Status
/// input is refused on that bound sink. Unbound test/native sinks retain `deliver`.
///
/// `Db` is `!Sync`, so the `Ingest` lives behind a mutex and each `apply` runs on a blocking
/// task. The host delivers a plugin's events one at a time and awaits each, so per-plugin order
/// is kept; events of different plugins serialise on the mutex.
///
/// Errors: an event for an account the database does not have (removed while events were
/// queued) is logged and dropped, matching the host's rule for stopped accounts (§4.2). A
/// plugin/account mismatch and a storage failure are rejected (the host answers a request
/// event with `INVALID_PARAMS`) and logged.
pub struct IngestSink {
    ingest: Arc<Mutex<Ingest>>,
    bus: ChangeBus,
    owners: Option<super::LogicalOwners>,
    references:super::reference_queue::ReferenceQueue,
    #[cfg(target_os="linux")]
    jobs:Arc<crate::managed_sources::OwnedJobs>,
    #[cfg(target_os="linux")]
    closing:tokio::sync::Mutex<CloseState>,
}

#[cfg(target_os="linux")]
enum CloseState {
    Open,
    Running(tokio::task::JoinHandle<std::result::Result<(),String>>),
    Done(std::result::Result<(),String>),
}

impl std::fmt::Debug for IngestSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestSink").finish_non_exhaustive()
    }
}

impl IngestSink {
    /// Wrap an ingest; results go to `bus`.
    pub fn new(ingest: Ingest, bus: ChangeBus) -> Self {
        Self {
            ingest: Arc::new(Mutex::new(ingest)),
            bus,
            owners: None,
            references:super::reference_queue::ReferenceQueue::new(),
            #[cfg(target_os="linux")]
            jobs:crate::managed_sources::OwnedJobs::new(),
            #[cfg(target_os="linux")]
            closing:tokio::sync::Mutex::new(CloseState::Open),
        }
    }

    #[cfg(target_os = "linux")]
    pub(super) fn for_core(ingest: Ingest, bus: ChangeBus, owners: super::LogicalOwners,
        references:super::reference_queue::ReferenceQueue) -> Self {
        let mut sink=Self::new(ingest,bus);sink.owners=Some(owners);sink.references=references;sink
    }

    pub(crate) fn shared_ingest(&self) -> Arc<Mutex<Ingest>> {
        Arc::clone(&self.ingest)
    }

    pub(crate) fn reference_queue(&self)->super::reference_queue::ReferenceQueue {self.references.clone()}

    /// Run `f` with the ingest (the main writer) on a blocking task. Services use it for
    /// core-initiated effects (`apply_*`) and to read through the main connection; publish the
    /// result with [`ChangeBus::publish_local`].
    /// Linux Core owns bounded admitted work through caller loss, closes new
    /// callbacks on shutdown and drains actual workers before its checkpoint.
    /// A returned metadata value grants no managed-file or cleanup authority.
    pub async fn with_ingest<R, F>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Ingest) -> R + Send + 'static,
        R: Send + 'static,
    {
        let ingest = Arc::clone(&self.ingest);
        let references=self.references.clone();
        #[cfg(target_os="linux")]
        {
            self.jobs.run_wait_until(&self.jobs.stopped(),move|| {
                let _references=references.read();
                let mut g=ingest.lock().unwrap_or_else(PoisonError::into_inner);
                f(&mut g)
            }).await.map_err(|_|super::error::RuntimeError::Task("ingest work unavailable".into()))
        }
        #[cfg(not(target_os="linux"))]
        {
        Ok(tokio::task::spawn_blocking(move || {
            let _references=references.read();
            let mut g = ingest.lock().unwrap_or_else(PoisonError::into_inner);
            f(&mut g)
        })
        .await?)
        }
    }

    #[cfg(target_os="linux")]
    pub(crate) fn close_work(&self) {self.jobs.close();}

    // The private close task survives loss of any shutdown waiter. Its real
    // worker drain precedes the hardcoded checkpoint; no caller can submit work
    // through this path after ordinary admission closes.
    #[cfg(target_os="linux")]
    pub(crate) async fn checkpoint_after_close(&self)->std::result::Result<(),String> {
        self.jobs.close();
        let mut state=self.closing.lock().await;
        if matches!(*state,CloseState::Open) {
            let jobs=self.jobs.clone();let ingest=self.ingest.clone();
            *state=CloseState::Running(tokio::spawn(async move {
                let drained=jobs.drain().await.map_err(|_|"ingest worker drain failed".to_owned());
                let checkpoint=tokio::task::spawn_blocking(move|| {
                    let ingest=ingest.lock().unwrap_or_else(PoisonError::into_inner);
                    ingest.db().checkpoint().map_err(|e|e.to_string())
                }).await.map_err(|_|"ingest checkpoint worker failed".to_owned()).and_then(|r|r);
                drained.and(checkpoint)
            }));
        }
        if let CloseState::Running(task)=&mut *state {
            let result=task.await.map_err(|_|"ingest close task failed".to_owned()).and_then(|r|r);
            *state=CloseState::Done(result);
        }
        match &*state {
            CloseState::Done(result)=>result.clone(),
            _=>Err("ingest close unavailable".into()),
        }
    }

    /// Replace the ingest options (a user setting changed). The path filter is kept when
    /// `opts.path_filter` is `None`.
    pub async fn set_options(&self, mut opts: IngestOptions) -> Result<()> {
        self.with_ingest(move |i| {
            if opts.path_filter.is_none() {
                opts.path_filter = i.options().path_filter.clone();
            }
            i.set_options(opts);
        })
        .await
    }
}

#[async_trait]
impl EventSink for IngestSink {
    async fn deliver(&self, plugin_id: &str, event: Event) -> std::result::Result<(), String> {
        if self.owners.is_some() && matches!(&event, Event::Status(_)) {
            return Err("native status provenance required".into());
        }
        let pid = plugin_id.to_owned();
        let name = event.name();
        let r = self
            .with_ingest(move |i| i.apply(&pid, event))
            .await
            .map_err(|e| e.to_string())?;
        self.publish_result(plugin_id, name, r, None)

    }
}


impl IngestSink {
    fn publish_result(&self, plugin_id: &str, name: &'static str,
        r: crate::ingest::Result<crate::ingest::Applied>, origin: Option<&super::LogicalOwnerObservation>) -> std::result::Result<(), String> {
        match r {
            Ok(applied) => {
                if !applied.is_noop() && origin.is_none_or(|owner| !owner.is_revoked()) {
                    self.bus.publish(CoreChange::Ingested {
                        plugin_id: plugin_id.to_owned(),
                        applied: Arc::new(applied),
                    });
                }
                Ok(())
            }
            Err(IngestError::UnknownAccount { account_id }) => {
                warn!(plugin = plugin_id, account = %account_id, event = name, "event for an account the database does not have; dropped");
                Ok(())
            }
            Err(e @ IngestError::PluginMismatch { .. }) => {
                error!(plugin = plugin_id, event = name, "{e}");
                Err(e.to_string())
            }
            Err(e) => {
                error!(plugin = plugin_id, event = name, "ingest failed: {e}");
                Err(e.to_string())
            }
        }
    }
}

#[cfg(target_os = "linux")]
#[async_trait]
impl crate::host::OwnedEventSink for IngestSink {
    async fn deliver_owned(&self, plugin_id: &str, origin: super::LogicalOwnerObservation,
        event: Event) -> std::result::Result<(), String> {
        let owners = self.owners.clone().ok_or("native owner issuer unavailable")?;
        let pid = plugin_id.to_owned();
        let name = event.name();
        let checked = origin.clone();
        let domain = owners.clone(); let ingest = self.ingest.clone();
        #[cfg(test)]
        let checkpoint = owners.take_checkpoint("event");
        let r = match owners.run_metadata(&origin, move || {
            #[cfg(test)]
            let _completion = checkpoint.map(|checkpoint| checkpoint.enter());
            let ingest = ingest.lock().unwrap_or_else(PoisonError::into_inner);
            ingest.apply_owned(&pid, event, &domain, &checked)
        }).await {
            Ok(result) => result,
            Err(crate::managed_sources::Error::Closed) => return Ok(()),
            Err(_) => return Err("native metadata worker failed".into()),
        };
        self.publish_result(plugin_id, name, r, Some(&origin))
    }
}

/// The spec §15 path rule for ingest: a path a plugin emits must be absolute and inside the
/// account's `state_dir` or `cache_dir` (as the host hands them out). When the file exists its
/// symlinks are resolved and the real path must stay inside too.
pub fn account_path_filter(config: &HostConfig) -> PathFilter {
    let config = config.clone();
    Arc::new(move |account_id: &str, path: &str| path_allowed(&config, account_id, Path::new(path)))
}

// Core's real composition shares this same negative reader gate with copy,
// verifier, catalog, selected-input and cache workers. Canonicalization is real
// filesystem work too, including rejection probes and root symlink resolution.
// No descriptor escapes and a true path-filter result grants no read/cleanup.
#[cfg(target_os="linux")]
pub(super) fn account_path_filter_counted(config:&HostConfig,
    catalog:Arc<crate::managed_stage::catalog::PrivateByteCatalog>) -> PathFilter {
    let config=config.clone();let mutations=catalog.mutations();
    Arc::new(move|account,path| {
        let Ok(_readers)=mutations.read_scope() else {return false;};
        path_allowed(&config,account,Path::new(path))
    })
}

fn path_allowed(config: &HostConfig, account_id: &str, p: &Path) -> bool {
    if !p.is_absolute() || p.components().any(|c| matches!(c, Component::ParentDir)) {
        return false;
    }
    let roots = [config.state_dir(account_id), config.cache_dir(account_id)];
    if !roots.iter().any(|r| p.starts_with(r)) {
        return false;
    }
    match std::fs::canonicalize(p) {
        Ok(real) => roots
            .iter()
            .filter_map(|r| std::fs::canonicalize(r).ok())
            .any(|r| real.starts_with(r)),
        // Not there (yet): the lexical check is all there is.
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_rule() {
        let tmp = tempfile::tempdir().unwrap();
        let c = HostConfig::new(tmp.path());
        let f = account_path_filter(&c);
        let cache = c.cache_dir("a#1");
        std::fs::create_dir_all(&cache).unwrap();
        let ok = cache.join("x/1.jpg");
        assert!(f("a#1", ok.to_str().unwrap()));
        assert!(f("a#1", c.state_dir("a#1").join("s").to_str().unwrap()));
        assert!(!f("a#2", ok.to_str().unwrap()), "another account's dir");
        assert!(!f("a#1", "relative/1.jpg"));
        assert!(!f(
            "a#1",
            cache.join("../../../etc/passwd").to_str().unwrap()
        ));
        assert!(!f("a#1", "/etc/passwd"));
        // A symlink out of the cache is refused once it exists.
        let outside = tmp.path().join("secret");
        std::fs::write(&outside, b"x").unwrap();
        let link = cache.join("evil");
        std::os::unix::fs::symlink(&outside, &link).unwrap();
        assert!(!f("a#1", link.to_str().unwrap()));
    }

    #[cfg(target_os="linux")]
    #[tokio::test]
    async fn actual_core_path_probe_uses_its_real_reader_gate_and_refuses_after_close() {
        use crate::{host::MemorySecretStore,runtime::{Core,CoreConfig}};
        let temp=tempfile::tempdir().unwrap();
        let mut config=CoreConfig::new(temp.path(),Arc::new(MemorySecretStore::new()));config.autostart=false;
        let core=Core::open(config).await.unwrap();
        let filter=core.ingest().with_ingest(|ing|ing.options().path_filter.clone().unwrap()).await.unwrap();
        let catalog=core.managed_sources().catalog();
        let root=core.host().config().cache_dir("owner");std::fs::create_dir_all(&root).unwrap();
        let path=root.join("cached.bin");std::fs::write(&path,b"isolated cache").unwrap();
        assert!(filter("owner",path.to_str().unwrap()));
        assert!(filter("owner",root.join("missing.bin").to_str().unwrap()),"existing lexical missing-file policy");
        let outside=temp.path().join("outside.bin");std::fs::write(&outside,b"external").unwrap();
        let link=root.join("outside-link");std::os::unix::fs::symlink(&outside,&link).unwrap();
        assert!(!filter("owner",link.to_str().unwrap()));
        let exclusive=catalog.exclude_readers_for_test().unwrap();
        assert!(!filter("owner",path.to_str().unwrap()));
        assert!(!filter("owner",root.join("missing.bin").to_str().unwrap()));
        drop(exclusive);assert!(filter("owner",path.to_str().unwrap()));
        core.shutdown().await;
        assert!(!filter("owner",path.to_str().unwrap()));
        assert!(!filter("owner",root.join("missing.bin").to_str().unwrap()));
    }

    #[cfg(target_os="linux")]
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn lost_ingest_and_close_waiters_still_require_actual_descriptor_and_worker_drop() {
        use crate::{host::MemorySecretStore,runtime::{Core,CoreConfig}};
        use std::{sync::{atomic::{AtomicBool,Ordering},mpsc},time::Duration};
        let temp=tempfile::tempdir().unwrap();
        let mut config=CoreConfig::new(temp.path(),Arc::new(MemorySecretStore::new()));config.autostart=false;
        let core=Arc::new(Core::open(config).await.unwrap());
        let path=temp.path().join("probe.bin");std::fs::write(&path,b"isolated metadata bytes").unwrap();
        let mutations=core.stage_mutations();let catalog=core.managed_sources().catalog();
        let (entered,ready)=tokio::sync::oneshot::channel();let (resume,paused)=mpsc::channel();
        let c=core.clone();
        let caller=tokio::spawn(async move {
            c.ingest().with_ingest(move|_| {
                let readers=mutations.read_scope().unwrap();
                let file=std::fs::File::open(path).unwrap();
                entered.send(()).unwrap();let _=paused.recv();
                assert_eq!(file.metadata().unwrap().len(),23);
                drop(file);drop(readers);
            }).await
        });
        tokio::time::timeout(Duration::from_secs(5),ready).await.unwrap().unwrap();
        caller.abort();assert!(caller.await.unwrap_err().is_cancelled());
        assert!(catalog.exclude_readers_for_test().is_err());
        assert!(tokio::time::timeout(Duration::from_millis(20),core.ingest().checkpoint_after_close()).await.is_err());
        assert!(matches!(&*core.ingest().closing.lock().await,CloseState::Running(_)));
        let called=Arc::new(AtomicBool::new(false));let captured=called.clone();
        let refused=tokio::time::timeout(Duration::from_secs(1),core.ingest().with_ingest(move|_|captured.store(true,Ordering::Release))).await;
        assert!(matches!(refused,Ok(Err(_))));assert!(!called.load(Ordering::Acquire));
        let mut second=Box::pin(core.shutdown());
        assert!(tokio::time::timeout(Duration::from_millis(20),&mut second).await.is_err());
        assert!(catalog.exclude_readers_for_test().is_err());
        resume.send(()).unwrap();tokio::time::timeout(Duration::from_secs(5),second).await.unwrap();
        assert!(catalog.exclude_readers_for_test().is_ok());
        core.shutdown().await; // Rejoins cached completion, never polls a finished handle twice.
        assert!(core.ingest().with_ingest(|_|()).await.is_err());
    }

    #[cfg(target_os="linux")]
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn real_ingest_full_and_account_metadata_jobs_share_queue_and_survive_waiter_loss() {
        use crate::{host::MemorySecretStore,runtime::{Core,CoreConfig}};
        use std::time::Duration;
        let temp=tempfile::tempdir().unwrap();
        let mut config=CoreConfig::new(temp.path(),Arc::new(MemorySecretStore::new()));config.autostart=false;
        let core=Arc::new(Core::open(config).await.unwrap());
        let queue=core.ingest().reference_queue();let exclusive=queue.exclusive_for_test().await;
        let c=core.clone();let normal=tokio::spawn(async move {
            c.ingest().with_ingest(|ing|ing.db().conn().execute(
                "INSERT INTO settings(key,value_json,updated_at) VALUES('queue-normal','true',0)",[])).await
        });
        let c=core.clone();let full=tokio::spawn(async move {
            c.managed_proofs().with_transaction(|scope|->rusqlite::Result<()> {
                scope.transaction().execute("INSERT INTO settings(key,value_json,updated_at) VALUES('queue-full','true',0)",[])?;
                Ok(())
            }).await
        });
        let c=core.clone();let accounts=tokio::spawn(async move {c.accounts().list().await});
        // Observe actual worker entry into the real lock wait, not a supplied
        // result/closure flag. All jobs are admitted before their callers drop.
        tokio::time::timeout(Duration::from_secs(5),async {
            while queue.waiting_for_test()<3 {tokio::time::sleep(Duration::from_millis(5)).await;}
        }).await.unwrap();
        let db=core.open_db().unwrap();
        let count=||db.conn().query_row("SELECT count(*) FROM settings WHERE key IN ('queue-normal','queue-full')",[],|r|r.get::<_,i64>(0)).unwrap();
        assert_eq!(count(),0);assert!(!normal.is_finished());assert!(!full.is_finished());assert!(!accounts.is_finished());
        normal.abort();full.abort();accounts.abort();
        assert!(normal.await.unwrap_err().is_cancelled());assert!(full.await.unwrap_err().is_cancelled());assert!(accounts.await.unwrap_err().is_cancelled());
        assert_eq!(count(),0);drop(exclusive);
        tokio::time::timeout(Duration::from_secs(5),async {
            while count()!=2 {tokio::time::sleep(Duration::from_millis(5)).await;}
        }).await.unwrap();
        core.shutdown().await;
        assert_eq!(count(),2);assert_eq!(queue.waiting_for_test(),0);
    }
}
