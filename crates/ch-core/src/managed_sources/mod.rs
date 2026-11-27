//! WIP local admission ownership only. No reader, dispatch, settlement or release grants.
//! Entries retain observations/metadata, never stage FDs, flocks or account read guards.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::managed_stage::{StageIdentity, StageMutations, catalog::PrivateByteCatalog};
use crate::host::HostConfig;
use crate::runtime::{AccountStorageObservation, ManagedProofWriter};

mod work;
mod owners;
mod restore;
pub(crate) use work::{OwnedJobs,BlockingWork};
pub(crate) use owners::{OwnerBarriers,OwnerClose, Reservation};

#[derive(Debug)]
pub(crate) enum Error { Closed, Busy, Conflict, Task }

pub(crate) struct ManagedSources {
    catalog: Arc<PrivateByteCatalog>,
    proofs: ManagedProofWriter,
    owners: OwnerBarriers,
    admissions: Arc<OwnedJobs>,
    lookups: Arc<OwnedJobs>,
    owner_jobs: Arc<OwnedJobs>,
    directory_jobs: Arc<OwnedJobs>,
    closed: CancellationToken,
}

impl ManagedSources {
    pub(crate) fn new(proofs: ManagedProofWriter, config: &HostConfig) -> Arc<Self> {
        let catalog=PrivateByteCatalog::for_core(config);
        Arc::new(Self { owners: OwnerBarriers::new(catalog.clone()),catalog, proofs,
            admissions: OwnedJobs::new(), lookups: OwnedJobs::new(), owner_jobs:OwnedJobs::new(),
            directory_jobs:OwnedJobs::new(),closed: CancellationToken::new() })
    }
    pub(crate) async fn initialize_catalog(self:&Arc<Self>) {
        let owned=self.clone();
        // Actual scan worker owns Sources and its slot, not a cancellable waiter.
        // Only descriptive metadata returns; scan failure leaves Unknown and does
        // not change ordinary startup or the durable managed-record startup gate.
        if !matches!(self.lookups.run(move||owned.catalog.refresh()).await,Ok(Ok(_))) {
            tracing::warn!("initial private-byte catalog remains unknown");
        }
    }
    // Before Host construction/discovery/autostart. The actual bounded metadata
    // worker installs only negative namespace barriers, never fresh storage Gates.
    pub(crate) async fn restore_negative(&self,ingest:Arc<std::sync::Mutex<crate::ingest::Ingest>>,
        config:&HostConfig) -> Result<(),crate::runtime::RuntimeError> {
        let config=config.clone();let barriers=self.owners.clone();
        self.lookups.run(move|| {
            let recovered={
                let ingest=ingest.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                restore::load(ingest.db().conn(),&config)?
            };
            barriers.restore(recovered).map_err(|_|crate::runtime::RuntimeError::ManagedLifetimeRecoveryUnavailable)
        }).await.map_err(|_|crate::runtime::RuntimeError::ManagedLifetimeRecoveryUnavailable)?
    }
    pub(crate) fn owner_barriers(&self) -> OwnerBarriers { self.owners.clone() }
    pub(crate) fn owner_jobs(&self) -> Arc<OwnedJobs> { self.owner_jobs.clone() }
    pub(crate) fn directory_jobs(&self) -> Arc<OwnedJobs> { self.directory_jobs.clone() }
    pub(crate) fn stage_mutations(&self) -> StageMutations { self.catalog.mutations() }
    #[cfg(test)]
    pub(crate) fn catalog(&self) -> Arc<PrivateByteCatalog> { self.catalog.clone() }

    pub(crate) fn proofs(&self) -> &ManagedProofWriter { &self.proofs }
    pub(crate) fn stopped(&self) -> CancellationToken { self.closed.clone() }
    pub(crate) fn lookups(&self) -> Arc<OwnedJobs> { Arc::clone(&self.lookups) }

    // The actual blocking closure owns the slot through FS preparation, the FULL
    // transaction await, publication and local bookkeeping, including caller loss.
    pub(crate) async fn run<R, F>(self: &Arc<Self>, work: F) -> Result<R, Error>
    where R: Send + 'static, F: FnOnce() -> R + Send + 'static {
        self.admissions.run(work).await
    }

    pub(crate) fn reserve(self: &Arc<Self>, owner: AccountStorageObservation, stage: String)
        -> Result<Reservation, Error>
    {
        if self.closed.is_cancelled() { return Err(Error::Closed); }
        self.owners.reserve(owner, stage)
    }

    pub(crate) fn close(&self) {
        self.owners.stop();
        self.owner_jobs.close();
        self.directory_jobs.close();
        self.catalog.close();
        self.closed.cancel();
        self.admissions.close();
    }

    pub(crate) async fn drain(&self) -> Result<(), Error> {
        self.close();
        let directories=self.directory_jobs.drain().await;
        let owners=self.owner_jobs.drain().await;
        let admissions = self.admissions.drain().await;
        let lookups = self.lookups.drain().await;
        directories.and(owners).and(admissions).and(lookups)
    }

    #[cfg(test)]
    pub(crate) fn census(&self) -> (usize, usize, usize) { self.owners.census() }
    #[cfg(test)]
    pub(crate) fn stop_fresh_admission(&self) { self.admissions.close(); }
}
