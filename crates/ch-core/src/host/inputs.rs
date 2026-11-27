//! Private metadata observation plus the original native account interval.
use crate::managed_stage::catalog::ExternalInputs;
use tokio_util::sync::CancellationToken;

pub(super) struct InputScope { observed:ExternalInputs, run:CancellationToken }
impl InputScope {
    pub(super) fn new(observed:ExternalInputs,run:CancellationToken) -> Self { Self {observed,run} }
    pub(super) fn live(&self) -> bool { !self.run.is_cancelled() }
    pub(super) fn observed(&self) -> &ExternalInputs { &self.observed }
}

// Issued only by an actual Core Host with its unconditional private-input guard
// attached. This composition marker is not a catalog-completeness/read permit.
#[derive(Clone)]
pub(crate) struct ProtectedInputs(crate::managed_sources::OwnerBarriers);
impl ProtectedInputs {
    pub(super) fn attached(barriers:crate::managed_sources::OwnerBarriers)->Self {Self(barriers)}
    pub(crate) fn check(&self,binding:&crate::runtime::AccountStorageBinding)->crate::managed_stage::Result<()> {
        self.0.check_creation(binding).map_err(|_|crate::managed_stage::Error::TargetChanged)
    }
}
