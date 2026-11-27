//! Negative descriptor accounting only. No path/read/closure/cleanup authority.
use super::{Error,Result};
use std::sync::Arc;
use tokio::sync::{OwnedRwLockReadGuard,RwLock};
use tokio_util::sync::CancellationToken;

/// Bookkeeping for actual already-authorized private filesystem work. The
/// enclosing worker/proof must retain this until all its actual descriptors close.
/// This token cannot designate/open a path or certify any historical reader.
pub struct StageReadScope { _guard:OwnedRwLockReadGuard<()> }

pub(super) struct ReaderSet { gate:Arc<RwLock<()>>,closed:CancellationToken }
impl ReaderSet {
    pub(super) fn new()->Self {Self {gate:Arc::new(RwLock::new(())),closed:CancellationToken::new()}}
    pub(super) fn enter(&self)->Result<StageReadScope> {
        if self.closed.is_cancelled() {return Err(Error::Cancelled);}
        let guard=self.gate.clone().try_read_owned().map_err(|_|Error::Busy)?;
        if self.closed.is_cancelled() {return Err(Error::Cancelled);}
        Ok(StageReadScope {_guard:guard})
    }
    pub(super) fn close(&self) {self.closed.cancel();}

    // Test-only exclusion from the same actual gate as production descriptors.
    // No runtime retirement factory is enabled by this accounting foundation.
    #[cfg(test)]
    pub(super) fn exclude(&self)->Result<tokio::sync::OwnedRwLockWriteGuard<()>> {
        self.gate.clone().try_write_owned().map_err(|_|Error::Busy)
    }
}
