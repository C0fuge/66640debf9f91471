use super::{Result, catalog::{PrivateByteCatalog, CatalogMutation}};
use std::sync::Arc;

/// Opaque invalidation-only hook issued by the actual Core. It grants no byte,
/// admission, completeness, reader-closure or cleanup authority.
#[derive(Clone)]
pub struct StageMutations {
    pub(super) catalog: Arc<PrivateByteCatalog>,
}
impl StageMutations {
    /// Check only that this hook describes the configured accounts namespace.
    /// This performs no path acquisition and grants no byte/read/close authority.
    pub fn check_accounts_root(&self, accounts_root: &std::path::Path) -> Result<()> {
        self.catalog.check_accounts_root(accounts_root)
    }

    /// Check the actual Accounts/Core issuer, rather than matching owner labels.
    /// This is a negative consistency check, not a source or cleanup permit.
    pub fn check_binding(&self,binding:&crate::runtime::AccountStorageBinding)->Result<()> {
        if binding.is_revoked() || !Arc::ptr_eq(&self.catalog,&binding.stage_mutations()?.catalog) {
            return Err(super::Error::TargetChanged);
        }
        Ok(())
    }

    /// Retain inside the actual synchronous operation through all FS settlement.
    /// Bookkeeping failure leaves Unknown; a closed Core permanently vetoes start.
    pub fn begin(&self) -> Result<StageMutation> {
        Ok(StageMutation { _inner: self.catalog.observe_mutation()? })
    }
}

/// Dropping this token invalidates; it never proves physical readers drained.
pub struct StageMutation { _inner: CatalogMutation }
