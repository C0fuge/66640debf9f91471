use super::{Error, MAX_BYTES, Result, fd::{self, Dir, Policy}, manifest::{Fingerprint, Format, Manifest, Phase, hex}};
use crate::runtime::AccountStorageBinding;
use sha2::{Digest, Sha256};
use std::{fs::File, io::Read, os::{fd::BorrowedFd, unix::{ffi::OsStrExt, fs::MetadataExt}}, sync::Arc};

const MANIFEST_CAP: u64 = 64 * 1024;

/// Independently verified, bounded-lifetime stage authority. Not serializable or Clone.
/// A source registry must retain metadata, never this value or its byte descriptor.
///
/// ```compile_fail
/// use ch_core::managed_stage::VerifiedStageBinding;
/// fn duplicate(v: &VerifiedStageBinding) -> VerifiedStageBinding { v.clone() }
/// ```
pub struct VerifiedStageBinding {
    pub(super) dir: Arc<Dir>,
    pub(super) manifest: Manifest,
    pub(super) bytes: File,
    _stage_lock: File,
    pub(super) owner: AccountStorageBinding,
}

impl VerifiedStageBinding {
    /// Verify a Ready stage derived solely from an Accounts-issued storage binding.
    /// This is not a local-admission, dispatch-settlement or unlink certificate.
    pub fn verify_ready(
        owner: &AccountStorageBinding,
        stage_id: &str,
        limit: u64,
        checkpoint: impl FnMut(u64) -> Result<()>,
    ) -> Result<Self> {
        Self::verify_stage(owner,stage_id,limit,None,checkpoint)
    }

    pub(crate) fn check_retained(owner:&AccountStorageBinding,expected:&super::StageIdentity,
        checkpoint:impl FnMut(u64)->Result<()>) -> Result<()> {
        // No reader/guard/flock escapes this bounded synchronous verification.
        Self::verify_stage(owner,&expected.manifest().stage_id,MAX_BYTES,Some(expected),checkpoint).map(drop)
    }

    fn verify_stage(owner:&AccountStorageBinding,stage_id:&str,limit:u64,
        expected:Option<&super::StageIdentity>,mut checkpoint:impl FnMut(u64)->Result<()>) -> Result<Self> {
        if owner.is_revoked() { return Err(Error::TargetChanged); }
        if !hex(stage_id, 32) { return Err(Error::Integrity); }
        let dir = open_stage(owner, stage_id)?;
        let stage_lock = dir.lock()?;
        let manifest = read_manifest(&dir)?;
        let row = owner.owner();
        let phase_matches=if expected.is_some() {matches!(manifest.phase,Phase::Claimed(_)|Phase::Consumed(_))}
            else {manifest.phase==Phase::Ready};
        if manifest.stage_id != stage_id || manifest.scope.account != row.account_id
            || manifest.scope.plugin != row.plugin_id || manifest.scope.account_created != row.created_at
            || !phase_matches
        { return Err(Error::Integrity); }
        if manifest.size > limit.min(MAX_BYTES) { return Err(Error::TooLarge); }
        check_layout(&dir)?;
        let probe = fd::openat(dir.fd(), c"bytes", libc::O_PATH, 0)?;
        check_bytes(&dir, &manifest, &probe)?;
        let bytes = fd::openat(dir.fd(), c"bytes", libc::O_RDONLY, 0)?;
        check_bytes(&dir, &manifest, &bytes)?;
        let proof = Self { dir, manifest, bytes, _stage_lock: stage_lock,
            owner: owner.retain_binding() };
        if let Some(expected)=expected
            && super::StageIdentity::capture(&proof)?!=*expected { return Err(Error::StageChanged); }
        proof.recheck_ready()?;
        checkpoint(0)?;
        let mut hash = Sha256::new();
        let mut prefix = Vec::with_capacity(8);
        let mut buf = [0u8; 64 * 1024];
        let mut count = 0;
        let mut reader = &proof.bytes;
        while count < proof.manifest.size {
            let size = (proof.manifest.size - count).min(buf.len() as u64) as usize;
            let n = reader.read(&mut buf[..size])?;
            if n == 0 { return Err(Error::Integrity); }
            count += n as u64;
            proof.check_bytes()?;
            checkpoint(count)?;
            hash.update(&buf[..n]);
            prefix.extend_from_slice(&buf[..n.min(8 - prefix.len())]);
        }
        if reader.read(&mut buf[..1])? != 0 { return Err(Error::Integrity); }
        if format!("{:x}", hash.finalize()) != proof.manifest.sha256
            || Format::detect(&prefix) != proof.manifest.format { return Err(Error::StageChanged); }
        proof.recheck_ready()?;
        Ok(proof)
    }

    pub fn manifest(&self) -> &Manifest { &self.manifest }

    /// Revalidate an App phase publisher's held directory against this exact proof.
    /// No byte descriptor can escape into a ledger/source registry.
    pub fn check_directory(&self, directory: BorrowedFd<'_>) -> Result<()> {
        if fd::id_fd(directory)? != self.dir.identity { return Err(Error::StageChanged); }
        self.check_bytes()
    }

    pub fn recheck_ready(&self) -> Result<()> {
        self.check_bytes()?;
        check_layout(&self.dir)?;
        if read_manifest(&self.dir)? != self.manifest { return Err(Error::StageChanged); }
        Ok(())
    }

    pub(super) fn check_bytes(&self) -> Result<()> {
        if self.owner.is_revoked() { return Err(Error::TargetChanged); }
        check_bytes(&self.dir, &self.manifest, &self.bytes)
    }
}

fn open_stage(owner: &AccountStorageBinding, id: &str) -> Result<Arc<Dir>> {
    let uploads=open_uploads(owner,false)?;
    let dir=uploads.child(&fd::cname(id.as_bytes())?,Policy::Private)?;
    if dir.identity.mount!=uploads.identity.mount {return Err(Error::Integrity);}
    dir.check()?;
    Ok(dir)
}

pub(super) fn open_uploads(owner:&AccountStorageBinding,create:bool)->Result<Arc<Dir>> {
    if owner.is_revoked() {return Err(Error::TargetChanged);}
    let uploads=owner.uploads();
    let account=uploads.parent().ok_or(Error::Integrity)?;
    let accounts=account.parent().ok_or(Error::Integrity)?;
    let data=accounts.parent().ok_or(Error::Integrity)?;
    if accounts.file_name().is_none_or(|v|v!="accounts")
        || uploads.file_name().is_none_or(|v|v!="uploads") {return Err(Error::Integrity);}
    let data=fd::data_dir(data)?;
    let accounts=data.child(c"accounts",Policy::Owner)?;
    let account=accounts.child(&fd::cname(account.file_name().ok_or(Error::Integrity)?.as_bytes())?,Policy::Owner)?;
    if accounts.identity.mount!=data.identity.mount || account.identity.mount!=accounts.identity.mount {
        return Err(Error::Integrity);
    }
    if create {
        match account.mkdir(c"uploads") {
            Ok(())=>{},Err(error) if error.kind()==std::io::ErrorKind::AlreadyExists=>{},
            Err(error)=>return Err(error.into()),
        }
    }
    let uploads=account.child(c"uploads",Policy::Private)?;
    if uploads.identity.mount!=account.identity.mount {return Err(Error::Integrity);}
    uploads.check()?;
    if create {account.file.sync_all()?;accounts.file.sync_all()?;data.file.sync_all()?;}
    if owner.is_revoked() {return Err(Error::TargetChanged);}
    Ok(uploads)
}

pub(super) fn check_layout(dir: &Dir) -> Result<()> {
    let mut count = 0;
    for item in std::fs::read_dir(format!("/proc/self/fd/{}", dir.fd()))? {
        let name = item?.file_name();
        if !matches!(name.as_bytes(), b"bytes" | b"manifest.json") || count == 2 {
            return Err(Error::Integrity);
        }
        count += 1;
    }
    if count != 2 { return Err(Error::Integrity); }
    dir.check()
}

fn check_bytes(dir: &Dir, manifest: &Manifest, file: &File) -> Result<()> {
    dir.bind(c"bytes", file).map_err(|_| Error::StageChanged)?;
    if fd::id(file)?.mount != dir.identity.mount
        || Fingerprint::from(file.metadata()?) != manifest.fingerprint {
        return Err(Error::StageChanged);
    }
    Ok(())
}

fn read_manifest(dir: &Dir) -> Result<Manifest> {
    read_manifest_file(dir).map(|(manifest, _)| manifest)
}

pub(super) fn read_manifest_file(dir: &Dir) -> Result<(Manifest, File)> {
    let probe = fd::openat(dir.fd(), c"manifest.json", libc::O_PATH, 0)?;
    let meta = probe.metadata()?;
    if fd::id(&probe)?.mount != dir.identity.mount || !meta.is_file()
        || meta.uid() != fd::uid() || meta.mode() & 0o7777 != 0o600
        || meta.nlink() != 1 || meta.len() > MANIFEST_CAP { return Err(Error::Integrity); }
    let file = fd::openat(dir.fd(), c"manifest.json", libc::O_RDONLY, 0)?;
    if fd::id(&file)? != fd::id(&probe)? { return Err(Error::StageChanged); }
    let fingerprint = Fingerprint::from(meta);
    let mut bytes = Vec::new();
    (&file).take(MANIFEST_CAP + 1).read_to_end(&mut bytes)?;
    dir.bind(c"manifest.json", &file)?;
    if bytes.len() as u64 != fingerprint.size || Fingerprint::from(file.metadata()?) != fingerprint {
        return Err(Error::StageChanged);
    }
    let manifest: Manifest = serde_json::from_slice(&bytes).map_err(|_| Error::Integrity)?;
    manifest.check()?;
    Ok((manifest, file))
}
