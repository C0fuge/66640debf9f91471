//! Native logical lifecycle evidence, not filesystem or whole-reader closure.
//! One sticky veto bounds older uncertainty; local phases never clear it.
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

// Dropping a queued caller and exposing its command share one short synchronous
// reservation. No frame may be marked/queued after cancellation won this lock.
pub(super) struct Wait(Arc<Mutex<bool>>);
#[derive(Clone)]
pub(super) struct Command(Arc<Mutex<bool>>);
impl Wait {
    pub(super) fn new() -> Self { Self(Arc::new(Mutex::new(false))) }
    pub(super) fn command(&self) -> Command { Command(self.0.clone()) }
}
impl Drop for Wait {
    fn drop(&mut self) { *self.0.lock().unwrap_or_else(PoisonError::into_inner) = true; }
}
impl Command {
    pub(super) fn admit(&self) -> Option<MutexGuard<'_, bool>> {
        let guard = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if *guard { None } else { Some(guard) }
    }
}

#[derive(Clone)]
pub(super) struct Request {
    connection: Arc<()>,
    id: i64,
}
impl Request {
    pub(super) fn new(connection: &Arc<()>, id: i64) -> Self {
        Self { connection: connection.clone(), id }
    }
    pub(super) fn same(&self, other: &Self) -> bool {
        self.id == other.id && Arc::ptr_eq(&self.connection, &other.connection)
    }
}

#[derive(Clone)]
enum Current { Starting(Request), Active(Request), Stopping(Request) }

#[derive(Clone, Default)]
pub(super) struct Evidence {
    unknown: bool,
    current: Option<Current>,
}

pub(super) enum Decision { Local, Dispatch, Refuse }

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RemovalStop { Confirmed, Refused }

impl Evidence {
    #[cfg(test)]
    pub(super) fn same(&self, other: &Self) -> bool {
        self.unknown == other.unknown && match (&self.current, &other.current) {
            (None, None) => true,
            (Some(Current::Starting(a)), Some(Current::Starting(b)))
                | (Some(Current::Active(a)), Some(Current::Active(b)))
                | (Some(Current::Stopping(a)), Some(Current::Stopping(b))) => a.same(b),
            _ => false,
        }
    }
    pub(super) fn shutdown_confirmed(&mut self, connection: &Arc<()>) {
        // §4.5 success settles this peer's known logical accounts. It cannot
        // erase an older unknown attempt, pending start, or source exposure.
        if !self.unknown && matches!(&self.current, Some(Current::Active(active))
            if Arc::ptr_eq(connection, &active.connection)) {
            self.current = None;
        }
    }
    pub(super) fn start(&mut self, request: Request) {
        // A later start, wish, process or response cannot discharge older work.
        self.unknown |= self.current.is_some();
        self.current = Some(Current::Starting(request));
    }
    pub(super) fn stop(&mut self, request: Request) {
        self.unknown |= !matches!(&self.current, Some(Current::Active(active))
            if Arc::ptr_eq(&active.connection, &request.connection));
        self.current = Some(Current::Stopping(request));
    }
    pub(super) fn finish(&mut self, request: &Request, success: bool) {
        match &self.current {
            Some(Current::Starting(current)) if current.same(request) => {
                if success { self.current = Some(Current::Active(request.clone())); }
                else { self.unknown = true; self.current = None; }
            }
            Some(Current::Stopping(current)) if current.same(request) => {
                self.unknown |= !success;
                self.current = None;
            }
            _ => {} // A late older or foreign-connection response grants nothing.
        }
    }
    pub(super) fn decision(&self, connection: Option<&Arc<()>>) -> Decision {
        if self.unknown { return Decision::Refuse; }
        match &self.current {
            None => Decision::Local,
            Some(Current::Active(active)) if connection.is_some_and(|connection|
                Arc::ptr_eq(connection, &active.connection)) => Decision::Dispatch,
            _ => Decision::Refuse,
        }
    }
    // Nondestructive ordinary stop recovery only: older unknown work still
    // vetoes removal, but must not prevent stopping this exactly accepted peer.
    pub(super) fn can_stop_current_peer(&self, connection: &Arc<()>) -> bool {
        matches!(&self.current, Some(Current::Active(active)) if Arc::ptr_eq(connection, &active.connection))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foreign_peer_reused_id_and_newer_success_cannot_discharge_old_unknown() {
        let peer = Arc::new(()); let foreign = Arc::new(());
        let old = Request::new(&peer, 7);
        let mut evidence = Evidence::default();
        evidence.start(old.clone());
        evidence.finish(&Request::new(&foreign, 7), true);
        assert!(matches!(evidence.decision(Some(&peer)), Decision::Refuse));
        evidence.finish(&old, false);
        let newer = Request::new(&foreign, 8);
        evidence.start(newer.clone()); evidence.finish(&newer, true);
        let stop = Request::new(&foreign, 9);
        evidence.stop(stop.clone()); evidence.finish(&stop, true);
        evidence.shutdown_confirmed(&foreign);
        assert!(matches!(evidence.decision(None), Decision::Refuse));
    }

    #[test]
    fn ordinary_current_peer_stop_can_recover_without_erasing_older_unknown() {
        let peer = Arc::new(()); let foreign = Arc::new(());
        let mut evidence = Evidence::default(); let old = Request::new(&peer, 1);
        evidence.start(old.clone()); evidence.finish(&old, false);
        let current = Request::new(&peer, 2); evidence.start(current.clone());
        assert!(!evidence.can_stop_current_peer(&peer)); // Still unresolved Starting.
        evidence.finish(&current, true);
        assert!(evidence.can_stop_current_peer(&peer));
        assert!(!evidence.can_stop_current_peer(&foreign));
        assert!(matches!(evidence.decision(Some(&peer)), Decision::Refuse));
        let stop = Request::new(&peer, 3); evidence.stop(stop.clone());
        assert!(!evidence.can_stop_current_peer(&peer)); // Stopping remains busy.
        evidence.finish(&stop, true);
        assert!(matches!(evidence.decision(None), Decision::Refuse));
    }
}
