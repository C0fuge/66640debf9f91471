//! Original logical producer intervals on one actual connection. Metadata only.
//! No local phase, new epoch or failed stop can relabel an older interval.
use super::removal::Request;
use crate::runtime::LogicalOwnerObservation;
use std::{collections::BTreeMap, sync::{Mutex, MutexGuard, PoisonError}};

#[derive(Clone)]
struct Interval { origin: LogicalOwnerObservation, stopping: Option<Request> }
pub(super) struct Origins(Mutex<BTreeMap<String, Interval>>);
impl Origins {
    pub(super) fn new() -> Self { Self(Mutex::default()) }
    pub(super) fn start(&self, account: &str, origin: &LogicalOwnerObservation) -> Option<Reservation<'_>> {
        if origin.is_revoked() || origin.account_id() != account { return None; }
        let mut entries = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let previous = entries.get(account).cloned();
        if let Some(old) = &previous {
            if !old.origin.same(origin) || old.stopping.is_some() { return None; }
        } else {
            if entries.len() >= 4096 { return None; }
            entries.insert(account.into(), Interval { origin: origin.clone(), stopping: None });
        }
        Some(Reservation { entries, account: account.into(), previous, origin: origin.clone(), committed: false })
    }
    pub(super) fn stop(&self, account: &str, request: Request) -> Option<Reservation<'_>> {
        let mut entries = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let previous = entries.get(account)?.clone();
        entries.get_mut(account)?.stopping = Some(request);
        Some(Reservation { entries, account: account.into(), origin: previous.origin.clone(), previous: Some(previous), committed: false })
    }
    pub(super) fn freeze(&self, account: &str) -> Option<LogicalOwnerObservation> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).get(account)
            .map(|entry| entry.origin.clone()).filter(|origin| !origin.is_revoked())
    }
    // Called only by the actual response resolver after valid Empty-object success or
    // UNKNOWN_ACCOUNT. Matching the captured request is mandatory; no bool API.
    pub(super) fn stop_confirmed(&self, account: &str, request: &Request) {
        let mut entries = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if entries.get(account).is_some_and(|entry| entry.stopping.as_ref().is_some_and(|pending| pending.same(request))) {
            entries.remove(account);
        }
    }
}

pub(super) struct Reservation<'a> {
    entries: MutexGuard<'a, BTreeMap<String, Interval>>, account: String,
    previous: Option<Interval>, origin: LogicalOwnerObservation, committed: bool,
}
impl Reservation<'_> {
    pub(super) fn origin(&self) -> &LogicalOwnerObservation { &self.origin }
    pub(super) fn commit(mut self) { self.committed = true; }
}
impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if self.committed { return; }
        if let Some(previous) = self.previous.take() { self.entries.insert(self.account.clone(), previous); }
        else { self.entries.remove(&self.account); }
    }
}
