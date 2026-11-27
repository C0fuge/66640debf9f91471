//! Core-issued counted copying and durable protected publication. No reader closure grant.
use super::{CopyDescription,CopyInput,Error,Result,manifest::Manifest};
use crate::{runtime::{AccountStorageBinding,HostStorageView,ManagedProofWriter,ManagedProofError},
    managed_sources::OwnedJobs,host::ProtectedInputs};
use std::sync::Arc;

/// Copying service issued by the actual Core. Caller loss cannot release the
/// actual worker's input descriptor, binding or bounded drain reservation.
/// Protected publication never substitutes a live verifier or reader closure.
#[derive(Clone)]
pub struct StagePublisher { issuer:HostStorageView,jobs:Arc<OwnedJobs>,proofs:ManagedProofWriter,protection:ProtectedInputs }

pub(super) enum PublicationWriteError {Refused(Error),Unknown}
impl StagePublisher {
    pub(crate) fn for_core(issuer:HostStorageView,jobs:Arc<OwnedJobs>,proofs:ManagedProofWriter,protection:ProtectedInputs)->Self {
        Self {issuer,jobs,proofs,protection}
    }

    pub(crate) fn close(&self) {self.jobs.close();}

    /// Create one actual new copy on Core's counted blocking worker. No binding,
    /// input descriptor or flock escapes in the returned metadata.
    pub async fn copy<I,C>(&self,binding:AccountStorageBinding,description:CopyDescription,
        mut input:I,mut checkpoint:C)->Result<Manifest>
    where I:CopyInput,C:FnMut(u64)->Result<()>+Send+'static {
        if !self.issuer.current(&binding.observation()) {return Err(Error::TargetChanged);}
        self.protection.check(&binding)?;
        let issuer=self.issuer.clone();let stopped=self.jobs.stopped();let journal=self.clone();
        self.jobs.run(move|| {
            if stopped.is_cancelled() || !issuer.current(&binding.observation()) {return Err(Error::TargetChanged);}
            super::copy::copy_ready_stage(&binding,description,&mut input,|count| {
                if stopped.is_cancelled() {return Err(Error::Cancelled);}
                checkpoint(count)
            },|dir,manifest|journal.record(&binding,dir,manifest))
        }).await.map_err(|error|match error {
            crate::managed_sources::Error::Busy=>Error::Busy,
            crate::managed_sources::Error::Closed=>Error::Cancelled,
            crate::managed_sources::Error::Conflict=>Error::TargetChanged,
            crate::managed_sources::Error::Task=>Error::Unknown,
        })?
    }

    /// Necessary protected-birth check for existing Ready discard. The caller
    /// holds the real independently verified byte inode/flock, not parsed JSON.
    /// No descriptor or reusable permission escapes this DB-only lookup. The
    /// caller must still recheck its exact Ready/flock and all stored references
    /// under its writer lock through destructive settlement.
    pub async fn check_unadmitted_ready(&self,proof:&super::VerifiedStageBinding)->Result<()> {
        if !self.issuer.current(&proof.owner.observation()) {return Err(Error::TargetChanged);}
        self.protection.check(&proof.owner)?;proof.recheck_ready()?;
        let identity=super::PublicationIdentity::capture(&proof.owner,&proof.dir,proof.manifest())?;
        let observed=proof.owner.observation();let issuer=self.issuer.clone();let stopped=self.jobs.stopped();
        let result=self.proofs.with_transaction(move|scope| {
            let tx=scope.transaction();issuer.check_in(tx,&observed).map_err(|_|Error::TargetChanged)?;
            if stopped.is_cancelled() {return Err(Error::Cancelled);}
            identity.require_unlinked_in(tx)?;
            let m=identity.manifest();
            let referenced:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM managed_source
                WHERE stage_id=?1 OR client_id=?2 OR path=?3)
                OR EXISTS(SELECT 1 FROM outbox WHERE client_id=?2)",
                rusqlite::params![m.stage_id,m.client_id,identity.path().to_str()],|r|r.get(0)).map_err(|_|Error::Storage)?;
            if referenced {return Err(Error::Integrity);}
            issuer.check_in(tx,&observed).map_err(|_|Error::TargetChanged)?;
            Ok(())
        }).await;
        match result {Ok(result)=>result,Err(_)=>Err(Error::Unknown)}?;
        proof.recheck_ready()
    }

    fn record(&self,binding:&AccountStorageBinding,dir:&super::fd::Dir,manifest:&Manifest)
        ->std::result::Result<(),PublicationWriteError> {
        self.protection.check(binding).map_err(PublicationWriteError::Refused)?;
        let identity=super::PublicationIdentity::capture(binding,dir,manifest).map_err(PublicationWriteError::Refused)?;
        let json=identity.to_persisted_json().map_err(PublicationWriteError::Refused)?;
        let nonce=crate::host::managed_nonce().ok_or(PublicationWriteError::Refused(Error::Storage))?;
        let observed=binding.observation();let issuer=self.issuer.clone();let stopped=self.jobs.stopped();
        let result=tokio::runtime::Handle::current().block_on(self.proofs.with_transaction(move|scope| {
            let tx=scope.transaction();
            issuer.check_in(tx,&observed).map_err(|_|Error::TargetChanged)?;
            if stopped.is_cancelled() {return Err(Error::Cancelled);}
            let (count,bytes):(i64,i64)=tx.query_row("SELECT count(*),coalesce(sum(bytes),0) FROM
                (SELECT length(CAST(identity_json AS BLOB)) AS bytes FROM managed_stage_publication LIMIT 257)",
                [],|row|Ok((row.get(0)?,row.get(1)?))).map_err(|_|Error::Storage)?;
            if count>=256 || bytes+json.len() as i64>1_048_576 {return Err(Error::Busy);}
            let m=identity.manifest();
            let historical:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM managed_source WHERE stage_id=?1 OR path=?2 OR client_id=?3)",
                rusqlite::params![m.stage_id,identity.path().to_str(),m.client_id],|row|row.get(0)).map_err(|_|Error::Storage)?;
            if historical {return Err(Error::Integrity);}
            tx.execute("INSERT INTO managed_stage_publication(stage_id,nonce,client_id,account_id,plugin_id,
                account_created_at,path,identity_json,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                rusqlite::params![m.stage_id,nonce,m.client_id,m.scope.account,m.scope.plugin,m.scope.account_created,
                    identity.path().to_str(),json,ch_db::now_millis()]).map_err(|_|Error::Storage)?;
            issuer.check_in(tx,&observed).map_err(|_|Error::TargetChanged)?;
            if stopped.is_cancelled() {return Err(Error::Cancelled);}
            Ok(())
        }));
        match result {
            Ok(Ok(()))=>Ok(()),Ok(Err(error))=>Err(PublicationWriteError::Refused(error)),
            Err(ManagedProofError::CommitUnknown(_)|ManagedProofError::RollbackUnknown(_)|ManagedProofError::TaskFailed)=>Err(PublicationWriteError::Unknown),
            Err(ManagedProofError::Busy)=>Err(PublicationWriteError::Refused(Error::Busy)),
            Err(ManagedProofError::Closed)=>Err(PublicationWriteError::Refused(Error::Cancelled)),
            Err(_)=>Err(PublicationWriteError::Refused(Error::Storage)),
        }
    }

}
