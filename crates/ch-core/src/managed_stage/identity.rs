//! Bounded descriptive persisted identity, never an admission/dispatch/unlink grant.
use super::{Error, Result, VerifiedStageBinding, fd, manifest::{Manifest, Phase}};
use serde::{Deserialize, Serialize};
use std::path::Path;

const RECORD_CAP: usize = 64 * 1024;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryIdentity { dev: u64, ino: u64 }
impl From<fd::Identity> for DirectoryIdentity {
    fn from(id: fd::Identity) -> Self { Self { dev: id.dev, ino: id.ino } }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u8,
    manifest: Manifest,
    path: String,
    namespace: [DirectoryIdentity; 5],
}

/// Metadata only. Parsing it never certifies current filesystem identity or an owner.
/// Authority holders (claim/verifier/binding) deliberately implement no serde traits.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct StageIdentity(Record);

impl StageIdentity {
    pub(super) fn capture(proof: &VerifiedStageBinding) -> Result<Self> {
        let mut manifest = proof.manifest.clone();
        let intent = match &manifest.phase {
            Phase::Claimed(intent) | Phase::Consumed(intent) => intent.clone(),
            _ => return Err(Error::Integrity),
        };
        manifest.phase = Phase::Claimed(intent);
        let identity=Self(capture_record(&proof.owner,&proof.dir,manifest)?);
        identity.validate()?;
        Ok(identity)
    }

    pub(crate) fn manifest(&self) -> &Manifest { &self.0.manifest }
    pub(crate) fn path(&self) -> &Path { Path::new(&self.0.path) }
    pub(crate) fn request(&self) -> Result<crate::outbox::OutboxRequest> {
        super::claim::canonical_request(self.manifest(),self.path())
    }

    pub(crate) fn to_persisted_json(&self) -> Result<String> {
        let json = serde_json::to_string(&self.0).map_err(|_| Error::Integrity)?;
        if json.len() > RECORD_CAP { return Err(Error::Integrity); }
        Ok(json)
    }

    /// Descriptive expected values from the DB. Independent verification is mandatory.
    pub(crate) fn from_persisted_json(json: &str) -> Result<Self> {
        if json.len() > RECORD_CAP { return Err(Error::Integrity); }
        let identity = Self(serde_json::from_str(json).map_err(|_| Error::Integrity)?);
        identity.validate()?;
        Ok(identity)
    }

    fn validate(&self) -> Result<()> {
        let record = &self.0;
        record.manifest.check()?;
        if record.version != 1 || !matches!(record.manifest.phase, Phase::Claimed(_)) {
            return Err(Error::Integrity);
        }
        validate_record(record)
    }
}

// A descriptive actual-birth identity, never a claim/read/cleanup grant. Only
// the exclusive creator captures it; DB parsing still requires live verification.
#[derive(Clone,PartialEq,Eq)]
pub(crate) struct PublicationIdentity(Record);
impl PublicationIdentity {
    pub(super) fn capture(owner:&crate::runtime::AccountStorageBinding,dir:&fd::Dir,manifest:&Manifest)->Result<Self> {
        let identity=Self(capture_record(owner,dir,manifest.clone())?);
        identity.validate()?;Ok(identity)
    }
    pub(crate) fn from_source(source:&StageIdentity)->Result<Self> {
        let mut record=source.0.clone();record.manifest.phase=Phase::Ready;
        let identity=Self(record);identity.validate()?;Ok(identity)
    }
    pub(crate) fn to_persisted_json(&self)->Result<String> {
        let json=serde_json::to_string(&self.0).map_err(|_|Error::Integrity)?;
        if json.len()>RECORD_CAP {return Err(Error::Integrity);}Ok(json)
    }
    pub(crate) fn from_persisted_json(json:&str)->Result<Self> {
        if json.len()>RECORD_CAP {return Err(Error::Integrity);}
        let identity=Self(serde_json::from_str(json).map_err(|_|Error::Integrity)?);
        identity.validate()?;Ok(identity)
    }
    // A historical DB fact is necessary, never sufficient: the caller supplies
    // identity captured from its actual held, independently verified live claim.
    // Bounded metadata only, inside the same FULL transaction as source linkage.
    pub(crate) fn require_unlinked_in(&self,conn:&rusqlite::Connection)->Result<()> {
        use rusqlite::OptionalExtension;
        let m=self.manifest();
        let recorded:Option<Option<String>>=conn.query_row("SELECT CASE
            WHEN length(CAST(identity_json AS BLOB))<=65536 THEN identity_json END
            FROM managed_stage_publication WHERE stage_id=?1 AND client_id=?2
            AND account_id=?3 AND plugin_id=?4 AND account_created_at=?5 AND path=?6 AND source_id IS NULL",
            rusqlite::params![m.stage_id,m.client_id,m.scope.account,m.scope.plugin,
                m.scope.account_created,self.path().to_str()],|row|row.get(0))
            .optional().map_err(|_|Error::Storage)?;
        let recorded=recorded.flatten().ok_or(Error::Integrity)?;
        if Self::from_persisted_json(&recorded)?!=*self {return Err(Error::Integrity);}
        Ok(())
    }
    pub(crate) fn manifest(&self)->&Manifest {&self.0.manifest}
    pub(crate) fn path(&self)->&Path {Path::new(&self.0.path)}
    fn validate(&self)->Result<()> {
        if self.0.manifest.phase!=Phase::Ready {return Err(Error::Integrity);}
        validate_record(&self.0)
    }
}

fn capture_record(owner:&crate::runtime::AccountStorageBinding,dir:&fd::Dir,manifest:Manifest)->Result<Record> {
    let uploads=owner.uploads();let account=uploads.parent().ok_or(Error::Integrity)?;
    let accounts=account.parent().ok_or(Error::Integrity)?;let data=accounts.parent().ok_or(Error::Integrity)?;
    let stage=uploads.join(&manifest.stage_id);
    if stage!=dir.path {return Err(Error::Integrity);}
    let namespace=[data,accounts,account,uploads.as_path(),stage.as_path()]
        .map(|path|dir.identity_for_ancestor(path).map(DirectoryIdentity::from));
    let [data,accounts,account,uploads,stage]=namespace;
    Ok(Record {version:1,manifest,path:dir.path.join("bytes").to_str().ok_or(Error::Integrity)?.into(),
        namespace:[data?,accounts?,account?,uploads?,stage?]})
}
fn validate_record(record:&Record)->Result<()> {
    record.manifest.check()?;
    if record.version!=1 {return Err(Error::Integrity);}
    let parts=fd::parts(Path::new(&record.path))?;let size=parts.len();
    if size<5 || parts[size-1]!=b"bytes" || parts[size-2]!=record.manifest.stage_id.as_bytes()
        || parts[size-3]!=b"uploads" || parts[size-5]!=b"accounts"
        || record.namespace.iter().any(|id|id.ino==0) {return Err(Error::Integrity);}
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_directory_identity_ignores_only_operation_local_mount_number() {
        let first = fd::Identity { dev: 7, ino: 41, mount: 100 };
        let next_boot = fd::Identity { mount: 900, ..first };
        let before = DirectoryIdentity::from(first);
        let after = DirectoryIdentity::from(next_boot);
        assert!(before == after);
        assert_eq!(serde_json::to_value(&before).unwrap(), serde_json::json!({"dev":7,"ino":41}));
        assert!(serde_json::from_str::<DirectoryIdentity>(r#"{"dev":7,"ino":41,"mount":100}"#).is_err());
        assert!(DirectoryIdentity::from(fd::Identity { ino: 42, ..first }) != before);
        assert!(DirectoryIdentity::from(fd::Identity { dev: 8, ..first }) != before);
    }
}
