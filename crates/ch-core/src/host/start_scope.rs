//! In-memory queued-start identity. Original owner observations are never healed
//! from an ID, matching timestamps, recovered paths or a replacement Gate.
use std::sync::Arc;

#[cfg(target_os = "linux")]
#[derive(Clone)]
pub(super) struct StartOwner {
    pub(super) issuer: crate::runtime::HostStorageView,
    pub(super) observed: crate::runtime::AccountStorageObservation,
    pub(super) stopped: tokio_util::sync::CancellationToken,
}

#[derive(Clone)]
pub(super) struct StartScope {
    nonce: Arc<()>,
    #[cfg(target_os = "linux")]
    pub(super) owner: Option<Arc<StartOwner>>,
}
impl StartScope {
    pub(super) fn unbound() -> Self {
        Self { nonce: Arc::new(()), #[cfg(target_os = "linux")] owner: None }
    }
    #[cfg(target_os = "linux")]
    pub(super) fn bound(owner: StartOwner) -> Self { Self { nonce: Arc::new(()), owner: Some(Arc::new(owner)) } }
    pub(super) fn replay(&self) -> Self {
        Self { nonce: Arc::new(()), #[cfg(target_os = "linux")] owner: self.owner.clone() }
    }
    pub(super) fn same(&self, other: &Self) -> bool { Arc::ptr_eq(&self.nonce, &other.nonce) }
    pub(super) fn same_owner(&self, other: &Self) -> bool {
        #[cfg(target_os = "linux")]
        { match (&self.owner, &other.owner) {
            (Some(a), Some(b)) => a.observed == b.observed,
            (None, None) => true,
            _ => false,
        } }
        #[cfg(not(target_os = "linux"))]
        { let _ = other; true }
    }
    pub(super) fn live(&self) -> bool {
        #[cfg(target_os = "linux")]
        if let Some(owner) = &self.owner {
            return !owner.stopped.is_cancelled() && owner.issuer.current(&owner.observed);
        }
        true
    }
}
