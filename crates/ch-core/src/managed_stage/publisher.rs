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
            journal.preflight(&binding,&description.stage_id)?;
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

    /// Complete only local Ready publication from an actual unadmitted birth.
    /// This never copies replacement bytes, spends a claim, enqueues or dispatches.
    /// The owned bounded worker independently checks the actual namespace/inode,
    /// hash and original owner. Parsed publication metadata alone grants no IO.
    pub async fn recover_ready(&self,binding:AccountStorageBinding,stage:String)->Result<Manifest> {
        if !super::manifest::hex(&stage,32) || !self.issuer.current(&binding.observation()) {
            return Err(Error::TargetChanged);
        }
        self.protection.check(&binding)?;
        let journal=self.clone();let stopped=self.jobs.stopped();
        self.jobs.run(move|| {
            use rusqlite::OptionalExtension;
            let observed=binding.observation();let issuer=journal.issuer.clone();
            let row_stage=stage.clone();
            let result=tokio::runtime::Handle::current().block_on(journal.proofs.with_transaction(move|scope| {
                let tx=scope.transaction();issuer.check_in(tx,&observed).map_err(|_|Error::TargetChanged)?;
                let row:Option<Option<String>>=tx.query_row("SELECT CASE
                    WHEN length(CAST(identity_json AS BLOB))<=65536 THEN identity_json END
                    FROM managed_stage_publication WHERE stage_id=?1 AND account_id=?2 AND source_id IS NULL",
                    rusqlite::params![row_stage,observed.owner().account_id],|r|r.get(0))
                    .optional().map_err(|_|Error::Storage)?;
                let identity=super::PublicationIdentity::from_persisted_json(&row.flatten().ok_or(Error::Integrity)?)?;
                if identity.manifest().stage_id!=row_stage
                    || identity.path()!=observed.namespace().join("uploads").join(&row_stage).join("bytes").as_path()
                {return Err(Error::Integrity);}
                identity.require_unlinked_in(tx)?;Ok(identity)
            }));
            let expected=match result {Ok(result)=>result?,Err(_)=>return Err(Error::Storage)};
            super::VerifiedStageBinding::restore_ready(&binding,&expected,|_| {
                if stopped.is_cancelled() || !journal.issuer.current(&binding.observation()) {
                    return Err(Error::TargetChanged);
                }
                Ok(())
            },||journal.check_identity_unadmitted(&binding,expected.clone()))
        }).await.map_err(|_|Error::Unknown)?
    }

    // Bounded negative check before allocating/copying any output bytes. The
    // final birth writer repeats capacity/history checks after actual byte sync;
    // an absent row here never grants provenance or reserves an inode.
    fn preflight(&self,binding:&AccountStorageBinding,stage:&str)->Result<()> {
        if !super::manifest::hex(stage,32) {return Err(Error::Integrity);}
        self.protection.check(binding)?;
        let stage=stage.to_owned();let observed=binding.observation();let issuer=self.issuer.clone();
        let path=binding.uploads().join(&stage).join("bytes");let stopped=self.jobs.stopped();
        let result=tokio::runtime::Handle::current().block_on(self.proofs.with_transaction(move|scope| {
            let tx=scope.transaction();issuer.check_in(tx,&observed).map_err(|_|Error::TargetChanged)?;
            if stopped.is_cancelled() {return Err(Error::Cancelled);}
            let (count,bytes):(i64,i64)=tx.query_row("SELECT count(*),coalesce(sum(bytes),0) FROM
                (SELECT length(CAST(identity_json AS BLOB)) AS bytes FROM managed_stage_publication LIMIT 257)",
                [],|r|Ok((r.get(0)?,r.get(1)?))).map_err(|_|Error::Storage)?;
            if count>=256 || bytes>=1_048_576 {return Err(Error::Busy);}
            let previous:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM managed_stage_publication WHERE stage_id=?1 OR path=?2)
                OR EXISTS(SELECT 1 FROM managed_source WHERE stage_id=?1 OR path=?2)
                OR EXISTS(SELECT 1 FROM managed_ready_receipt WHERE stage_id=?1 OR path=?2)
                OR EXISTS(SELECT 1 FROM managed_ready_retirement WHERE stage_id=?1 OR path=?2)",
                rusqlite::params![stage,path.to_str()],|r|r.get(0)).map_err(|_|Error::Storage)?;
            if previous {return Err(Error::Integrity);}
            issuer.check_in(tx,&observed).map_err(|_|Error::TargetChanged)?;
            Ok(())
        }));
        // This closure performed no write and no FS allocation has started yet.
        match result {Ok(result)=>result,Err(ManagedProofError::Busy)=>Err(Error::Busy),
            Err(ManagedProofError::Closed)=>Err(Error::Cancelled),Err(_)=>Err(Error::Storage)}
    }

    /// Necessary protected-birth check for existing Ready discard. The caller
    /// holds the real independently verified byte inode/flock, not parsed JSON.
    /// No descriptor or reusable permission escapes this DB-only lookup. The
    /// caller must still recheck its exact Ready/flock and all stored references
    /// under its writer lock through destructive settlement. Existing literal
    /// and inode-index hits also veto; their absence proves neither complete
    /// coverage nor that unresolved aliases/historical readers have settled.
    pub async fn check_unadmitted_ready(&self,proof:&super::VerifiedStageBinding)->Result<()> {
        if !self.issuer.current(&proof.owner.observation()) {return Err(Error::TargetChanged);}
        self.protection.check(&proof.owner)?;proof.recheck_ready()?;
        let identity=super::PublicationIdentity::capture(&proof.owner,&proof.dir,proof.manifest())?;
        let observed=proof.owner.observation();let issuer=self.issuer.clone();let stopped=self.jobs.stopped();
        self.proofs.with_transaction(move|scope| {
            let tx=scope.transaction();
            check_unadmitted(tx,&issuer,&observed,&stopped,&identity)?;
            // Include pre-admission NULL-source slots and every consumer/column
            // extracted by the existing general ledger. Even a stale or dirty
            // hit is a negative veto, never a positive alias/read/cleanup grant.
            let fingerprint=&identity.manifest().fingerprint;
            let dev=i64::try_from(fingerprint.dev).map_err(|_|Error::Integrity)?;
            let ino=i64::try_from(fingerprint.ino).map_err(|_|Error::Integrity)?;
            let referenced:bool=tx.query_row("SELECT
                EXISTS(SELECT 1 FROM managed_ref WHERE path=?1)
                OR EXISTS(SELECT 1 FROM managed_ref WHERE resolved_dev=?2 AND resolved_ino=?3)",
                rusqlite::params![identity.path().to_str(),dev,ino],|r|r.get(0)).map_err(|_|Error::Storage)?;
            if referenced {return Err(Error::Busy);}
            Ok(())
        })
            .await.map_err(|_|Error::Unknown)??;
        proof.recheck_ready()
    }

    fn check_identity_unadmitted(&self,binding:&AccountStorageBinding,identity:super::PublicationIdentity)->Result<()> {
        self.protection.check(binding)?;
        let observed=binding.observation();let issuer=self.issuer.clone();let stopped=self.jobs.stopped();
        let result=tokio::runtime::Handle::current().block_on(self.proofs.with_transaction(move|scope|
            check_unadmitted(scope.transaction(),&issuer,&observed,&stopped,&identity)));
        match result {Ok(result)=>result,Err(_)=>Err(Error::Unknown)}?;
        Ok(())
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
            let historical:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM managed_source WHERE stage_id=?1 OR path=?2 OR client_id=?3)
                OR EXISTS(SELECT 1 FROM managed_ready_receipt WHERE stage_id=?1 OR path=?2 OR client_id=?3)
                OR EXISTS(SELECT 1 FROM managed_ready_retirement WHERE stage_id=?1 OR path=?2 OR client_id=?3)",
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

fn check_unadmitted(tx:&rusqlite::Connection,issuer:&HostStorageView,
    observed:&crate::runtime::AccountStorageObservation,stopped:&tokio_util::sync::CancellationToken,
    identity:&super::PublicationIdentity)->Result<()> {
    issuer.check_in(tx,observed).map_err(|_|Error::TargetChanged)?;
    if stopped.is_cancelled() {return Err(Error::Cancelled);}
    identity.require_unlinked_in(tx)?;
    let m=identity.manifest();
    let referenced:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM managed_source
        WHERE stage_id=?1 OR client_id=?2 OR path=?3)
        OR EXISTS(SELECT 1 FROM outbox WHERE client_id=?2)",
        rusqlite::params![m.stage_id,m.client_id,identity.path().to_str()],|r|r.get(0)).map_err(|_|Error::Storage)?;
    if referenced {return Err(Error::Integrity);}
    issuer.check_in(tx,observed).map_err(|_|Error::TargetChanged)?;
    Ok(())
}
