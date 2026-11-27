//! Metadata provenance for one persisted logical owner, distinct from storage Gates.
//! Issued only while holding this Accounts instance's DB mutex. No I/O grant.
use ch_db::types::AccountRow;
use rusqlite::Connection;
use std::{collections::HashMap, fmt, sync::{Arc, Mutex, PoisonError}};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(crate) struct LogicalOwnerObservation {
    issuer: Arc<()>,
    epoch: Arc<CancellationToken>,
    owner: Arc<AccountRow>,
}
impl fmt::Debug for LogicalOwnerObservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogicalOwnerObservation").finish_non_exhaustive()
    }
}
impl LogicalOwnerObservation {
    pub(crate) fn is_revoked(&self) -> bool { self.epoch.is_cancelled() }
    pub(crate) fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.issuer, &other.issuer) && Arc::ptr_eq(&self.epoch, &other.epoch)
    }
    pub(crate) fn account_id(&self) -> &str { &self.owner.account_id }
    pub(crate) fn plugin_id(&self) -> &str { &self.owner.plugin_id }
}

enum Entry { Current(LogicalOwnerObservation), Blocked }
#[derive(Default)]
struct State { closed: bool, entries: HashMap<String, Entry> }
struct Inner {
    issuer: Arc<()>,
    state: Mutex<State>,
    #[cfg(target_os = "linux")]
    jobs: Arc<crate::managed_sources::OwnedJobs>,
    #[cfg(target_os="linux")]
    references:crate::runtime::reference_queue::ReferenceQueue,
    #[cfg(all(test, target_os = "linux"))]
    checkpoint: Mutex<Option<MetadataCheckpointSlot>>,
}
#[cfg(all(test, target_os = "linux"))]
type MetadataCallback = dyn Fn() + Send + Sync;
#[cfg(all(test, target_os = "linux"))]
type MetadataCheckpointSlot = (&'static str, MetadataCheckpoint);
#[cfg(all(test, target_os = "linux"))]
pub(crate) struct MetadataCheckpoint { before: Arc<MetadataCallback>, after: Arc<MetadataCallback> }
#[cfg(all(test, target_os = "linux"))]
impl MetadataCheckpoint {
    pub(crate) fn enter(self) -> MetadataCompletion {
        (self.before)();
        MetadataCompletion(self.after)
    }
}
#[cfg(all(test, target_os = "linux"))]
pub(crate) struct MetadataCompletion(Arc<MetadataCallback>);
#[cfg(all(test, target_os = "linux"))]
impl Drop for MetadataCompletion {
    fn drop(&mut self) { (self.0)(); }
}

#[derive(Clone)]
pub(crate) struct LogicalOwners(Arc<Inner>);
impl LogicalOwners {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Inner { issuer: Arc::new(()), state: Mutex::default(),
            #[cfg(target_os = "linux")] jobs: crate::managed_sources::OwnedJobs::new(),
            #[cfg(target_os="linux")] references:crate::runtime::reference_queue::ReferenceQueue::new(),
            #[cfg(all(test, target_os = "linux"))] checkpoint: Mutex::default() }))
    }

    #[cfg(target_os="linux")]
    pub(in crate::runtime) fn for_core(references:crate::runtime::reference_queue::ReferenceQueue)->Self {
        let mut owners=Self::new();
        Arc::get_mut(&mut owners.0).expect("new owner domain is uniquely owned").references=references;
        owners
    }

    fn issue(&self, row: &AccountRow) -> LogicalOwnerObservation {
        LogicalOwnerObservation { issuer: self.0.issuer.clone(), epoch: Arc::new(CancellationToken::new()), owner: Arc::new(row.clone()) }
    }

    // The row was read on the actual Accounts connection, under its mutex.
    pub(super) fn capture(&self, row: &AccountRow) -> Option<LogicalOwnerObservation> {
        let mut state = self.0.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.closed { return None; }
        match state.entries.entry(row.account_id.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => match entry.get() {
                Entry::Current(origin) if super::storage::same(&origin.owner, row) && !origin.is_revoked() => Some(origin.clone()),
                _ => None,
            },
            std::collections::hash_map::Entry::Vacant(entry) => {
                let origin = self.issue(row);
                entry.insert(Entry::Current(origin.clone()));
                Some(origin)
            }
        }
    }

    // Only a positively committed create publishes a replacement incarnation.
    pub(super) fn created(&self, row: &AccountRow) {
        let mut state = self.0.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(Entry::Current(old)) = state.entries.remove(&row.account_id) { old.epoch.cancel(); }
        if !state.closed { state.entries.insert(row.account_id.clone(), Entry::Current(self.issue(row))); }
    }

    // Called before releasing the deleting Accounts DB mutex. Refusal/confirmed
    // rollback never calls this; unknown commit instead retains a sticky veto.
    pub(super) fn deleted(&self, account: &str) {
        let mut state = self.0.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(Entry::Current(old)) = state.entries.remove(account) { old.epoch.cancel(); }
    }
    pub(super) fn uncertain(&self, account: &str) {
        let mut state = self.0.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(Entry::Current(old)) = state.entries.remove(account) { old.epoch.cancel(); }
        if !state.closed { state.entries.insert(account.to_owned(), Entry::Blocked); }
    }

    fn current(&self, origin: &LogicalOwnerObservation) -> bool {
        !origin.is_revoked() && Arc::ptr_eq(&self.0.issuer, &origin.issuer)
            && {
                let state = self.0.state.lock().unwrap_or_else(PoisonError::into_inner);
                !state.closed && state.entries.get(origin.account_id()).is_some_and(|entry|
                    matches!(entry, Entry::Current(current) if current.same(origin)))
            }
            && !origin.is_revoked()
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn run_metadata<R, F>(&self, origin: &LogicalOwnerObservation, work: F)
        -> Result<R, crate::managed_sources::Error>
    where R: Send + 'static, F: FnOnce() -> R + Send + 'static {
        if !self.current(origin) { return Err(crate::managed_sources::Error::Closed); }
        let references=self.0.references.clone();let domain=self.clone();let original=origin.clone();
        self.0.jobs.run_wait_until(&origin.epoch,move|| {
            let _references=references.read();
            if !domain.current(&original) {return Err(crate::managed_sources::Error::Closed);}
            Ok(work())
        }).await?
    }

    #[cfg(target_os="linux")]
    pub(super) async fn run_unowned_metadata<R,F>(&self,work:F)->Result<R,crate::managed_sources::Error>
    where R:Send+'static,F:FnOnce()->R+Send+'static {
        let references=self.0.references.clone();let stopped=self.0.jobs.stopped();let admission=stopped.clone();
        self.0.jobs.run_wait_until(&admission,move|| {
            let _references=references.read();
            if stopped.is_cancelled() {return Err(crate::managed_sources::Error::Closed);}
            Ok(work())
        }).await?
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn close(&self) {
        {
            let mut state = self.0.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.closed = true;
            for entry in state.entries.values() {
                if let Entry::Current(origin) = entry { origin.epoch.cancel(); }
            }
            state.entries.clear();
        }
        self.0.jobs.close();
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn drain(&self) -> Result<(), crate::managed_sources::Error> {
        self.close();
        self.0.jobs.drain().await
    }

    #[cfg(all(test, target_os = "linux"))]
    pub(super) fn checkpoint(&self, phase: &'static str, before: Arc<MetadataCallback>, after: Arc<MetadataCallback>) {
        *self.0.checkpoint.lock().unwrap() = Some((phase, MetadataCheckpoint { before, after }));
    }
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn take_checkpoint(&self, phase: &str) -> Option<MetadataCheckpoint> {
        let mut hook = self.0.checkpoint.lock().unwrap();
        if hook.as_ref().is_some_and(|(expected, _)| *expected == phase) {
            hook.take().map(|(_, hook)| hook)
        } else { None }
    }

    // The caller holds its ACTUAL SQLite writer transaction. Never retain the
    // short map mutex while querying SQLite, waiting, or doing filesystem work.
    pub(crate) fn validate(&self, conn: &Connection, origin: &LogicalOwnerObservation) -> ch_db::Result<bool> {
        if !self.current(origin) { return Ok(false); }
        let same = ch_db::get_account(conn, origin.account_id())?
            .is_some_and(|row| super::storage::same(&row, &origin.owner));
        Ok(same && self.current(origin))
    }
}
