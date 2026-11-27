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
        }
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn for_core(ingest: Ingest, bus: ChangeBus, owners: super::LogicalOwners) -> Self {
        Self { ingest: Arc::new(Mutex::new(ingest)), bus, owners: Some(owners) }
    }

    pub(crate) fn shared_ingest(&self) -> Arc<Mutex<Ingest>> {
        Arc::clone(&self.ingest)
    }

    /// Run `f` with the ingest (the main writer) on a blocking task. Services use it for
    /// core-initiated effects (`apply_*`) and to read through the main connection; publish the
    /// result with [`ChangeBus::publish_local`].
    pub async fn with_ingest<R, F>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Ingest) -> R + Send + 'static,
        R: Send + 'static,
    {
        let ingest = Arc::clone(&self.ingest);
        Ok(tokio::task::spawn_blocking(move || {
            let mut g = ingest.lock().unwrap_or_else(PoisonError::into_inner);
            f(&mut g)
        })
        .await?)
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
}
