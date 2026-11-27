//! Core-issued counted copying service. No protected-birth/closure grant yet.
use super::{CopyDescription,CopyInput,Error,Result,manifest::Manifest};
use crate::{runtime::{AccountStorageBinding,HostStorageView},managed_sources::OwnedJobs};
use std::sync::Arc;

/// Copying service issued by the actual Core. Caller loss cannot release the
/// actual worker's input descriptor, binding or bounded drain reservation.
/// This does not yet record protected publication or authorize source cleanup.
#[derive(Clone)]
pub struct StagePublisher { issuer:HostStorageView,jobs:Arc<OwnedJobs> }
impl StagePublisher {
    pub(crate) fn for_core(issuer:HostStorageView,jobs:Arc<OwnedJobs>)->Self {Self {issuer,jobs}}

    pub(crate) fn close(&self) {self.jobs.close();}

    /// Create one actual new copy on Core's counted blocking worker. No binding,
    /// input descriptor or flock escapes in the returned metadata.
    pub async fn copy<I,C>(&self,binding:AccountStorageBinding,description:CopyDescription,
        mut input:I,mut checkpoint:C)->Result<Manifest>
    where I:CopyInput,C:FnMut(u64)->Result<()>+Send+'static {
        if !self.issuer.current(&binding.observation()) {return Err(Error::TargetChanged);}
        let issuer=self.issuer.clone();let stopped=self.jobs.stopped();
        self.jobs.run(move|| {
            if stopped.is_cancelled() || !issuer.current(&binding.observation()) {return Err(Error::TargetChanged);}
            super::copy::copy_ready_stage(&binding,description,&mut input,|count| {
                if stopped.is_cancelled() {return Err(Error::Cancelled);}
                checkpoint(count)
            })
        }).await.map_err(|error|match error {
            crate::managed_sources::Error::Busy=>Error::Busy,
            crate::managed_sources::Error::Closed=>Error::Cancelled,
            crate::managed_sources::Error::Conflict=>Error::TargetChanged,
            crate::managed_sources::Error::Task=>Error::Unknown,
        })?
    }
}
