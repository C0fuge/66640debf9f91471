//! WIP local admission ownership only. No reader, dispatch, settlement or release grants.
//! Entries retain observations/metadata, never stage FDs, flocks or account read guards.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use tokio_util::sync::CancellationToken;

use crate::managed_stage::StageIdentity;
use crate::runtime::{AccountStorageObservation, ManagedProofWriter};

const RECORDS: usize = 256;
mod work;
pub(crate) use work::OwnedJobs;

#[derive(Debug)]
pub(crate) enum Error { Closed, Busy, Conflict, Task }

struct Record {
    owner: AccountStorageObservation,
    id: Option<i64>,
    identity: Option<StageIdentity>,
}

#[derive(Default)]
struct State {
    pending: HashMap<String, AccountStorageObservation>,
    records: HashMap<String, Record>,
}

pub(crate) struct ManagedSources {
    proofs: ManagedProofWriter,
    state: Mutex<State>,
    admissions: Arc<OwnedJobs>,
    lookups: Arc<OwnedJobs>,
    closed: CancellationToken,
}

impl ManagedSources {
    pub(crate) fn new(proofs: ManagedProofWriter) -> Arc<Self> {
        Arc::new(Self { proofs, state: Mutex::default(),
            admissions: OwnedJobs::new(), lookups: OwnedJobs::new(), closed: CancellationToken::new() })
    }

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
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if self.closed.is_cancelled() || owner.is_revoked() { return Err(Error::Closed); }
        if state.pending.contains_key(&stage) || state.records.contains_key(&stage) {
            return Err(Error::Conflict);
        }
        if state.pending.len() + state.records.len() >= RECORDS { return Err(Error::Busy); }
        state.pending.insert(stage.clone(), owner.clone());
        Ok(Reservation { service: Arc::downgrade(self), owner, stage, settled: false })
    }

    pub(crate) fn close(&self) {
        // Share the admission-publication lock with run/reserve. No permit count
        // or record is dropped merely because the service closes.
        let _state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        self.closed.cancel();
        self.admissions.close();
    }

    pub(crate) async fn drain(&self) -> Result<(), Error> {
        self.close();
        let admissions = self.admissions.drain().await;
        let lookups = self.lookups.drain().await;
        admissions.and(lookups)
    }

    #[cfg(test)]
    pub(crate) fn census(&self) -> (usize, usize, usize) {
        let state = self.state.lock().unwrap();
        (state.pending.len(), state.records.values().filter(|record| record.id.is_some()).count(),
            state.records.values().filter(|record| record.id.is_none()).count())
    }
}

// This is a counted local admission, not a source lease or an OS-reader proof.
pub(crate) struct Reservation {
    service: Weak<ManagedSources>,
    owner: AccountStorageObservation,
    stage: String,
    settled: bool,
}

impl Reservation {
    pub(crate) fn publish(&mut self, id: i64, identity: StageIdentity) -> Result<(), Error> {
        let service = self.service.upgrade().ok_or(Error::Closed)?;
        let mut state = service.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.pending.get(&self.stage) != Some(&self.owner) { return Err(Error::Conflict); }
        if let Some(old) = state.records.get(&self.stage) {
            if old.owner == self.owner && old.id == Some(id) && old.identity.as_ref() == Some(&identity) {
                self.settled = true;
                return Ok(());
            }
            return Err(Error::Conflict);
        }
        state.records.insert(self.stage.clone(), Record { owner: self.owner.clone(), id: Some(id), identity: Some(identity) });
        self.settled = true;
        Ok(())
    }

    pub(crate) fn quarantine(&mut self, identity: Option<StageIdentity>) {
        if let Some(service) = self.service.upgrade() {
            service.state.lock().unwrap_or_else(PoisonError::into_inner).records
                .entry(self.stage.clone()).or_insert_with(|| Record {
                    owner: self.owner.clone(), id: None, identity,
                });
            self.settled = true;
        }
    }

    pub(crate) fn no_new_source(&mut self) { self.settled = true; }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(service) = self.service.upgrade() {
            let mut state = service.state.lock().unwrap_or_else(PoisonError::into_inner);
            if !self.settled {
                // A panic/lost local completion cannot turn a possibly committed
                // admission into an uncounted owner. No physical handles survive.
                state.records.entry(self.stage.clone()).or_insert_with(|| Record {
                    owner: self.owner.clone(), id: None, identity: None,
                });
            }
            if state.pending.get(&self.stage) == Some(&self.owner) { state.pending.remove(&self.stage); }
        }
    }
}
