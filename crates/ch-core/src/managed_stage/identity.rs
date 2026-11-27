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
        // These are the actual configured private namespace roots. Parent ancestry
        // outside data_dir is verified while held, never made a durable equality key.
        let uploads = proof.owner.uploads();
        let account = uploads.parent().ok_or(Error::Integrity)?;
        let accounts = account.parent().ok_or(Error::Integrity)?;
        let data = accounts.parent().ok_or(Error::Integrity)?;
        let stage = uploads.join(&manifest.stage_id);
        if stage != proof.dir.path { return Err(Error::Integrity); }
        let namespace = [data, accounts, account, uploads.as_path(), stage.as_path()]
            .map(|path| proof.dir.identity_for_ancestor(path).map(DirectoryIdentity::from));
        let [data, accounts, account, uploads, stage] = namespace;
        let identity = Self(Record { version: 1, manifest,
            path: proof.dir.path.join("bytes").to_str().ok_or(Error::Integrity)?.into(),
            namespace: [data?, accounts?, account?, uploads?, stage?],
        });
        identity.validate()?;
        Ok(identity)
    }

    pub(crate) fn manifest(&self) -> &Manifest { &self.0.manifest }
    pub(crate) fn path(&self) -> &Path { Path::new(&self.0.path) }

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
        let parts = fd::parts(self.path())?;
        let size = parts.len();
        if size < 5 || parts[size - 1] != b"bytes"
            || parts[size - 2] != record.manifest.stage_id.as_bytes()
            || parts[size - 3] != b"uploads" || parts[size - 5] != b"accounts"
            || record.namespace.iter().any(|id| id.ino == 0)
        { return Err(Error::Integrity); }
        Ok(())
    }
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
