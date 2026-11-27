//! Linux-only rules for the private managed-upload stage namespace.
//! Manifest metadata is descriptive; only Accounts can issue owner authority.
pub mod manifest;
mod fd;
mod verify;
mod claim;
mod identity;
mod account_root;
pub(crate) use account_root::{AccountRootAnchor, AccountRootIdentity};
pub(crate) mod catalog;
mod mutations;
pub use mutations::{StageMutations, StageMutation};
pub(crate) use claim::PreparedStageClaim;
pub(crate) use identity::StageIdentity;
pub use claim::LiveStageClaim;
pub use verify::VerifiedStageBinding;

pub const MAX_BYTES: u64 = 1 << 31;
pub type Result<T> = std::result::Result<T, Error>;

// Never expose paths or OS messages through this boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    UnsafeSource, SourceChanged, StageChanged, TooLarge, Storage, Integrity,
    Unknown, Cancelled, TargetChanged, Busy,
}
impl From<std::io::Error> for Error {
    fn from(_: std::io::Error) -> Self { Self::Storage }
}
