//! Native metadata provenance only. No byte, filesystem or producer-closure grant.
use super::{AccountInfo, StateChange};
use crate::runtime::LogicalOwnerObservation;
use std::sync::Arc;

#[derive(Clone, Debug)]
struct AccountData { info: AccountInfo, origin: LogicalOwnerObservation }

#[derive(Clone, Debug)]
pub(crate) struct OwnedAccountSnapshot(Arc<AccountData>);
impl OwnedAccountSnapshot {
    pub(super) fn new(info: AccountInfo, origin: LogicalOwnerObservation) -> Self { Self(Arc::new(AccountData { info, origin })) }
    pub(crate) fn info(&self) -> &AccountInfo { &self.0.info }
    pub(crate) fn origin(&self) -> &LogicalOwnerObservation { &self.0.origin }
    pub(crate) fn into_parts(self) -> (AccountInfo, LogicalOwnerObservation) {
        match Arc::try_unwrap(self.0) {
            Ok(data) => (data.info, data.origin),
            Err(data) => (data.info.clone(), data.origin.clone()),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum OwnedStateChange { Account(OwnedAccountSnapshot), Other(Box<StateChange>) }
