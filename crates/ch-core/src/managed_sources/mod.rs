//! WIP local admission ownership only. No reader, dispatch, settlement or release grants.
//! Entries retain observations/metadata, never stage FDs, flocks or account read guards.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::managed_stage::{StageIdentity, StageMutations, catalog::PrivateByteCatalog};
use crate::host::HostConfig;
use crate::runtime::{AccountStorageObservation, ManagedProofWriter};

mod work;
mod owners;
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
    closed: CancellationToken,
}

impl ManagedSources {
    pub(crate) fn new(proofs: ManagedProofWriter, config: &HostConfig) -> Arc<Self> {
        Arc::new(Self { catalog: PrivateByteCatalog::for_core(config), proofs, owners: OwnerBarriers::new(),
            admissions: OwnedJobs::new(), lookups: OwnedJobs::new(), owner_jobs:OwnedJobs::new(),closed: CancellationToken::new() })
    }
    pub(crate) fn owner_barriers(&self) -> OwnerBarriers { self.owners.clone() }
    pub(crate) fn owner_jobs(&self) -> Arc<OwnedJobs> { self.owner_jobs.clone() }
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
        self.catalog.close();
        self.closed.cancel();
        self.admissions.close();
    }

    pub(crate) async fn drain(&self) -> Result<(), Error> {
        self.close();
        let owners=self.owner_jobs.drain().await;
        let admissions = self.admissions.drain().await;
        let lookups = self.lookups.drain().await;
        owners.and(admissions).and(lookups)
    }

    #[cfg(test)]
    pub(crate) fn census(&self) -> (usize, usize, usize) { self.owners.census() }
    #[cfg(test)]
    pub(crate) fn stop_fresh_admission(&self) { self.admissions.close(); }
}
