//! A Core-owned durable writer, not a source registration or filesystem authority.

use std::sync::{Arc, Mutex, PoisonError};

use ch_db::{Db, Transaction};
use tokio::sync::{Semaphore, oneshot};
#[cfg(target_os="linux")]
use tokio::sync::OwnedSemaphorePermit;
use tokio::task::JoinHandle;

use crate::ingest::{Applied, Ingest, LocalEcho};

const JOB_LIMIT: u32 = 4;
#[cfg(target_os="linux")]
const NATIVE_LIMIT:u32=4;
pub(crate) type SubmittedProof<R,E> = oneshot::Receiver<Result<Result<R,E>,ManagedProofError>>;

/// Failure to execute or durably finish proof work. A failed commit is ambiguous;
/// callers must reconcile existing durable identity rather than replay an effect.
#[derive(Debug, thiserror::Error)]
pub enum ManagedProofError {
    /// This Core could not establish the dedicated WAL/FULL connection.
    #[error("managed proof durability is unavailable")]
    DurabilityUnavailable,
    /// Shutdown has closed admission.
    #[error("managed proof writer is closed")]
    Closed,
    /// The bounded work slots are occupied; no work was admitted.
    #[error("managed proof writer is busy")]
    Busy,
    /// The normal ingest connection already has an open transaction.
    #[error("ingest already has an active transaction")]
    ActiveTransaction,
    /// Opening/starting work failed before the callback.
    #[error("managed proof database: {0}")]
    Database(#[from] ch_db::Error),
    /// Commit failed; no successful durability receipt may be returned.
    #[error("managed proof commit outcome is unknown: {0}")]
    CommitUnknown(rusqlite::Error),
    /// Rollback failed; do not report a definite domain refusal.
    #[error("managed proof rollback outcome is unknown: {0}")]
    RollbackUnknown(rusqlite::Error),
    /// The owned worker failed (including callback panic).
    #[error("managed proof task failed")]
    TaskFailed,
}

/// The explicit FULL transaction and transaction-scoped ingest helpers.
///
/// No normal connection is exposed here. Every query and receipt readback must use
/// [`Self::transaction`]. Callbacks must be bounded DB-only work: no network/filesystem
/// IO, transaction-control SQL, or attempts to lock the shared ingest sink again.
pub struct ManagedProofTransaction<'scope, 'db> {
    ingest: &'scope Ingest,
    tx: &'scope Transaction<'db>,
}

impl<'db> ManagedProofTransaction<'_, 'db> {
    /// The caller's FULL transaction, including its own uncommitted writes.
    pub fn transaction(&self) -> &Transaction<'db> {
        self.tx
    }
    #[cfg(target_os="linux")]
    pub(crate) fn ingest(&self) -> &Ingest {self.ingest}

    /// Insert an echo on this transaction, without opening a nested ingest writer.
    /// The caller publishes returned changes only after successful durable commit.
    pub fn apply_local_echo(
        &self,
        account_id: &str,
        echo: &LocalEcho,
    ) -> crate::ingest::Result<(Applied, i64)> {
        self.ingest.apply_local_echo_in(self.tx, account_id, echo)
    }
}

struct State {
    closed: bool,
    #[cfg(target_os="linux")]
    native_closed:bool,
    jobs: Vec<JoinHandle<()>>,
}

struct Inner {
    ingest: Arc<Mutex<Ingest>>,
    db: Mutex<Option<Db>>,
    slots: Arc<Semaphore>,
    #[cfg(target_os="linux")]
    native:Arc<Semaphore>,
    #[cfg(target_os="linux")]
    native_work:Arc<Semaphore>,
    #[cfg(target_os="linux")]
    issuer:Arc<()>,
    state: Mutex<State>,
}

/// Core's bounded native durable-transaction seam (not an RPC or a file permit).
///
/// Only Core constructs it using its configured database. Normal ingest remains
/// NORMAL; this separate WAL/FULL connection is serialized by the same ingest
/// mutex. Dropping an awaiting caller does not cancel an admitted blocking job.
/// If the dedicated connection is unavailable, ordinary Core operation continues
/// but every proof transaction refuses without a callback or NORMAL fallback.
#[derive(Clone)]
pub struct ManagedProofWriter {
    inner: Arc<Inner>,
}
#[cfg(target_os="linux")]
#[derive(Clone)]
pub(crate) struct NativeProofTicket { _slot:Arc<OwnedSemaphorePermit>,issuer:Arc<()> }

impl ManagedProofWriter {
    pub(super) fn new(ingest: Arc<Mutex<Ingest>>, db: Option<Db>) -> Self {
        Self {
            inner: Arc::new(Inner {
                ingest,
                db: Mutex::new(db),
                slots: Arc::new(Semaphore::new(JOB_LIMIT as usize)),
                #[cfg(target_os="linux")]
                native:Arc::new(Semaphore::new(NATIVE_LIMIT as usize)),
                #[cfg(target_os="linux")]
                native_work:Arc::new(Semaphore::new((NATIVE_LIMIT*2) as usize)),
                #[cfg(target_os="linux")]
                issuer:Arc::new(()),
                state: Mutex::new(State { closed: false,#[cfg(target_os="linux")] native_closed:false, jobs: Vec::new() }),
            }),
        }
    }

    /// Run a bounded callback and return its value only after FULL commit.
    /// A domain error rolls back and is returned inside `Ok`; outer errors never
    /// provide a successful durable receipt. The callback must obey the restrictions
    /// on [`ManagedProofTransaction`], including never re-locking shared ingest.
    pub async fn with_transaction<R, E, F>(&self, f: F) -> Result<Result<R, E>, ManagedProofError>
    where
        R: Send + 'static,
        E: Send + 'static,
        F: FnOnce(&ManagedProofTransaction<'_, '_>) -> Result<R, E> + Send + 'static,
    {
        self.submit(f)?.await.map_err(|_|ManagedProofError::TaskFailed)?
    }

    // Native response resolution admits actual bounded DB work synchronously.
    // Dropping an RPC waiter or delaying its next poll cannot race past drain.
    pub(crate) fn submit<R,E,F>(&self,f:F) -> Result<SubmittedProof<R,E>,ManagedProofError>
    where R:Send+'static,E:Send+'static,
        F:FnOnce(&ManagedProofTransaction<'_,'_>)->Result<R,E>+Send+'static {
        let (send, recv) = oneshot::channel();
        {
            let mut state = self.inner.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.closed {
                return Err(ManagedProofError::Closed);
            }
            let permit = Arc::clone(&self.inner.slots)
                .try_acquire_owned().map_err(|_| ManagedProofError::Busy)?;
            state.jobs.retain(|job| !job.is_finished());
            let inner = Arc::clone(&self.inner);
            // Admission, job publication and close share the same lock. The permit
            // belongs to the actual blocking closure, not this cancellable future.
            state.jobs.push(tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let result = inner.run(f);
                let _ = send.send(result);
            }));
        }
        Ok(recv)
    }

    #[cfg(target_os="linux")]
    pub(crate) fn reserve_native(&self)->Result<NativeProofTicket,ManagedProofError> {
        let state=self.inner.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.closed {return Err(ManagedProofError::Closed);}
        let slot=self.inner.native.clone().try_acquire_owned().map_err(|_|ManagedProofError::Busy)?;
        Ok(NativeProofTicket {_slot:Arc::new(slot),issuer:self.inner.issuer.clone()})
    }

    // Only a ticket reserved BEFORE exposure may enter after ordinary close.
    // Four attempts reserve at most eight timeout/final metadata callbacks, which
    // wait for DB slots without occupying blocking threads. The actual worker
    // owns its ticket and work slot through COMMIT.
    #[cfg(target_os="linux")]
    pub(crate) fn submit_native<R,E,F>(&self,ticket:NativeProofTicket,f:F)
        ->Result<SubmittedProof<R,E>,ManagedProofError>
    where R:Send+'static,E:Send+'static,
        F:FnOnce(&ManagedProofTransaction<'_,'_>)->Result<R,E>+Send+'static {
        let (send,recv)=oneshot::channel();
        let mut state=self.inner.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.native_closed || !Arc::ptr_eq(&ticket.issuer,&self.inner.issuer) {return Err(ManagedProofError::Closed);}
        let work=self.inner.native_work.clone().try_acquire_owned().map_err(|_|ManagedProofError::Busy)?;
        state.jobs.retain(|job|!job.is_finished());
        let inner=self.inner.clone();
        state.jobs.push(tokio::spawn(async move {
            let Ok(slot)=inner.slots.clone().acquire_owned().await else {return;};
            let _=tokio::task::spawn_blocking(move|| {
                let _work=work;let _ticket=ticket;let _slot=slot;
                let _=send.send(inner.run(f));
            }).await;
        }));
        Ok(recv)
    }

    /// Synchronously deny new work. Existing jobs continue to own their DB and slot.
    pub fn close(&self) {
        self.inner.state.lock().unwrap_or_else(PoisonError::into_inner).closed = true;
    }

    /// Close and wait for actual blocking work, even after another drainer was dropped.
    /// The semaphore is never closed: an acquisition error cannot masquerade as drain.
    pub async fn drain(&self) -> Result<(), ManagedProofError> {
        let jobs = {
            let mut state = self.inner.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.closed = true;
            #[cfg(target_os="linux")]
            {state.native_closed=true;}
            std::mem::take(&mut state.jobs)
        };
        let mut failed = false;
        for job in jobs {
            failed |= job.await.is_err();
        }
        #[cfg(target_os="linux")]
        let _native=Arc::clone(&self.inner.native).acquire_many_owned(NATIVE_LIMIT)
            .await.map_err(|_|ManagedProofError::TaskFailed)?;
        let _all = Arc::clone(&self.inner.slots).acquire_many_owned(JOB_LIMIT)
            .await.map_err(|_| ManagedProofError::TaskFailed)?;
        if failed { Err(ManagedProofError::TaskFailed) } else { Ok(()) }
    }
}

impl Inner {
    fn run<R, E>(
        &self,
        f: impl FnOnce(&ManagedProofTransaction<'_, '_>) -> Result<R, E>,
    ) -> Result<Result<R, E>, ManagedProofError> {
        // The global order is ingest -> private FULL connection. No async work or
        // source registry/FS locks are permitted inside this writer reservation.
        let ingest = self.ingest.lock().unwrap_or_else(PoisonError::into_inner);
        if !ingest.db().conn().is_autocommit() {
            return Err(ManagedProofError::ActiveTransaction);
        }
        let connection = self.db.lock().unwrap_or_else(PoisonError::into_inner);
        let db = connection.as_ref().ok_or(ManagedProofError::DurabilityUnavailable)?;
        if !db.conn().is_autocommit() {
            return Err(ManagedProofError::ActiveTransaction);
        }
        db.require_full_sync()?;
        let tx = db.transaction()?;
        // Recheck under the reserved writer too: another connection must not
        // change journal policy between the first check and BEGIN.
        db.require_full_sync()?;
        let result = f(&ManagedProofTransaction { ingest: &ingest, tx: &tx });
        match result {
            Ok(value) => {
                tx.commit().map_err(ManagedProofError::CommitUnknown)?;
                Ok(Ok(value))
            }
            Err(error) => {
                tx.rollback().map_err(ManagedProofError::RollbackUnknown)?;
                Ok(Err(error))
            }
        }
    }
}

#[cfg(test)]
#[path = "managed_proof_tests.rs"]
mod tests;
