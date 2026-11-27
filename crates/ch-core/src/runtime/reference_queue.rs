//! Negative metadata scheduling only. No path, reader-closure or cleanup grant.
use std::sync::Arc;
use tokio::sync::{RwLock,RwLockReadGuard};

#[derive(Clone)]
pub(super) struct ReferenceQueue {
    gate:Arc<RwLock<()>>,
    #[cfg(test)] waiting:Arc<std::sync::atomic::AtomicUsize>,
}
impl ReferenceQueue {
    pub(super) fn new()->Self {
        Self {gate:Arc::new(RwLock::new(())),#[cfg(test)] waiting:Arc::new(std::sync::atomic::AtomicUsize::new(0))}
    }

    // Actual already-owned blocking workers call this BEFORE the ingest mutex.
    // Caller loss therefore cannot discard a queued native/durable metadata job.
    pub(super) fn read(&self)->RwLockReadGuard<'_,()> {
        if let Ok(guard)=self.gate.try_read() {return guard;}
        #[cfg(test)]
        let _waiting=Waiting::new(&self.waiting);
        self.gate.blocking_read()
    }

    // No production exclusive/retirement factory is enabled by this foundation.
    #[cfg(all(test,target_os="linux"))]
    pub(super) async fn exclusive_for_test(&self)->tokio::sync::OwnedRwLockWriteGuard<()> {
        self.gate.clone().write_owned().await
    }
    #[cfg(all(test,target_os="linux"))]
    pub(super) fn waiting_for_test(&self)->usize {self.waiting.load(std::sync::atomic::Ordering::Relaxed)}
}

#[cfg(test)]
struct Waiting<'a>(&'a std::sync::atomic::AtomicUsize);
#[cfg(test)]
impl<'a> Waiting<'a> {
    fn new(counter:&'a std::sync::atomic::AtomicUsize)->Self {
        counter.fetch_add(1,std::sync::atomic::Ordering::Relaxed);Self(counter)
    }
}
#[cfg(test)]
impl Drop for Waiting<'_> {
    fn drop(&mut self) {self.0.fetch_sub(1,std::sync::atomic::Ordering::Relaxed);}
}
