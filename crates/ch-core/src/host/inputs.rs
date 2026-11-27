//! Private metadata observation plus the original native account interval.
use crate::managed_stage::catalog::ExternalInputs;
use tokio_util::sync::CancellationToken;

pub(super) struct InputScope { observed:ExternalInputs, run:CancellationToken }
impl InputScope {
    pub(super) fn new(observed:ExternalInputs,run:CancellationToken) -> Self { Self {observed,run} }
    pub(super) fn live(&self) -> bool { !self.run.is_cancelled() }
    pub(super) fn observed(&self) -> &ExternalInputs { &self.observed }
}
