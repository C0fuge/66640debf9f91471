use super::LiveStageClaim;
use crate::{managed_stage::{Result, identity::StageIdentity}, outbox::OutboxRequest,
    runtime::{AccountStorageBinding, AccountStorageObservation}};

/// Bounded owned preparation. A failure does not erase canonical comparison intent.
/// Future admission must inspect an exact retained receipt before using identity/error.
pub(crate) struct PreparedStageClaim {
    claim: LiveStageClaim,
    request: Result<OutboxRequest>,
    identity: Result<StageIdentity>,
}

impl LiveStageClaim {
    pub(crate) fn prepare_admission(mut self) -> PreparedStageClaim {
        // Burn first, including canonical derivation/FS failures and already-spent attempts.
        let unused = self.burn_unused();
        let request = self.canonical_request();
        let identity = unused.and_then(|_| self.validate_claimed())
            .and_then(|_| StageIdentity::capture(&self.proof));
        PreparedStageClaim { claim: self, request, identity }
    }
}

impl PreparedStageClaim {
    pub(crate) fn request(&self) -> Result<&OutboxRequest> {
        self.request.as_ref().map_err(|error| *error)
    }
    pub(crate) fn identity(&self) -> Result<&StageIdentity> {
        self.identity.as_ref().map_err(|error| *error)
    }
    pub(crate) fn owner_binding(&self) -> &AccountStorageBinding { self.claim.owner_binding() }
    pub(crate) fn owner_observation(&self) -> AccountStorageObservation {
        self.owner_binding().observation()
    }
    /// Returns the permanently spent claim for exact admitted bookkeeping or drop only.
    pub(crate) fn into_claim(self) -> LiveStageClaim { self.claim }
}
