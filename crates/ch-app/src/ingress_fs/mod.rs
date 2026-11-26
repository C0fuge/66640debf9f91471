//! Linux-only private staging boundary. Callers own the data-directory writer lock,
//! lifecycle/eligibility checks, reservations, and the decision to admit a live claim.
mod fd;
mod inventory;
mod manifest;
mod mounts;
mod source;
mod store;

pub(crate) use inventory::Inventory;
pub(crate) use manifest::{Format, Intent, Manifest, Owner, Phase, Scope};
pub(crate) use source::Source;
pub(crate) use store::{LiveClaim, Stage, Store, Verified};

pub(crate) const MAX_BYTES: u64 = 1 << 31;
const CHUNK: usize = 64 * 1024;
type Result<T> = std::result::Result<T, Error>;

// Never carry paths or OS messages across this boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    UnsafeSource,
    SourceChanged,
    StageChanged,
    TooLarge,
    Storage,
    Integrity,
    Unknown,
    Cancelled,
    TargetChanged,
    Busy,
}
impl From<std::io::Error> for Error {
    fn from(_: std::io::Error) -> Self {
        Self::Storage
    }
}

fn check_size(size: u64, limit: u64) -> Result<()> {
    if size > limit.min(MAX_BYTES) {
        Err(Error::TooLarge)
    } else if size == 0 {
        Err(Error::Integrity)
    } else {
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/support/ingress_fs_cases.rs"]
mod tests;
