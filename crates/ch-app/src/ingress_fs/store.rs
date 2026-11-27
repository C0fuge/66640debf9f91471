use super::{
    CHUNK, Error, Format, Intent, Manifest, Owner, Phase, Result, Scope, Source, check_size,
    fd::{self, Dir, Policy},
    manifest::{Fingerprint, hex, opaque},
};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    fs::{File, Permissions},
    io::{self, Read, Write},
    os::fd::AsFd,
    os::unix::{
        ffi::OsStrExt,
        fs::{MetadataExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    sync::Arc,
};

const MANIFEST_CAP: u64 = 64 * 1024;

pub(crate) struct Store {
    pub(super) accounts: Arc<Dir>,
    // None records a collision. Never allocate or import a receipt through that mapping.
    pub(super) owners: HashMap<OsString, Option<Owner>>,
}
impl Store {
    /// Accounts must already exist. Mapping comes from the complete persisted owner
    /// set and HostConfig::upload_dir, never from a manifest or request parameter.
    pub fn open(accounts_root: &Path, owners: Vec<Owner>) -> Result<Self> {
        let parts = fd::parts(accounts_root)?;
        let (last, parents) = parts.split_last().ok_or(Error::Integrity)?;
        if *last != b"accounts" || parents.is_empty() {
            return Err(Error::Integrity);
        }
        let mut dir = Dir::root()?;
        let (data, ancestors) = parents.split_last().ok_or(Error::Integrity)?;
        for part in ancestors {
            dir = dir.child(&fd::cname(part)?, Policy::Any)?;
        }
        let data = dir.child(&fd::cname(data)?, Policy::Owner)?;
        let accounts = data.child(c"accounts", Policy::Owner)?;
        accounts.check()?;
        let mut store = Self {
            accounts,
            owners: HashMap::new(),
        };
        for owner in owners {
            if !opaque(&owner.account) || !opaque(&owner.plugin) || owner.account_created < 0 {
                return Err(Error::Integrity);
            }
            fd::parts(&owner.uploads)?;
            let account_dir = owner.uploads.parent().ok_or(Error::Integrity)?;
            if owner.uploads.file_name() != Some(OsStr::new("uploads"))
                || account_dir.parent() != Some(accounts_root)
            {
                return Err(Error::Integrity);
            }
            let name = account_dir.file_name().ok_or(Error::Integrity)?.to_owned();
            store
                .owners
                .entry(name)
                .and_modify(|current| *current = None)
                .or_insert(Some(owner));
        }
        Ok(store)
    }
    fn account(&self, owner: &Owner) -> Result<Arc<Dir>> {
        let key = owner
            .uploads
            .parent()
            .and_then(Path::file_name)
            .ok_or(Error::Integrity)?;
        if self.owners.get(key).and_then(Option::as_ref) != Some(owner) {
            return Err(Error::Integrity);
        }
        self.accounts.check()?;
        let dir = self
            .accounts
            .child(&fd::cname(key.as_bytes())?, Policy::Owner)?;
        if dir.identity.mount != self.accounts.identity.mount {
            return Err(Error::Integrity);
        }
        Ok(dir)
    }
    fn uploads(&self, owner: &Owner, create: bool) -> Result<Arc<Dir>> {
        let account = self.account(owner)?;
        if create {
            match account.mkdir(c"uploads") {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e.into()),
            }
        }
        let uploads = account.child(c"uploads", Policy::Private)?;
        if uploads.identity.mount != account.identity.mount {
            return Err(Error::Integrity);
        }
        uploads.check()?;
        if create {
            // Existing directories might also be remnants of an interrupted mkdir.
            account.file.sync_all()?;
            self.accounts.file.sync_all()?;
            self.accounts
                .parent()
                .ok_or(Error::Integrity)?
                .file
                .sync_all()?;
        }
        Ok(uploads)
    }
    pub fn recover(&self, owner: &Owner, stage_id: &str) -> Result<Stage> {
        if !hex(stage_id, 32) {
            return Err(Error::Integrity);
        }
        let uploads = self.uploads(owner, false)?;
        let dir = uploads.child(&fd::cname(stage_id.as_bytes())?, Policy::Private)?;
        Stage::load(dir, owner, stage_id)
    }

    /// Call after atomically charging source.len() and one slot. Re-inventory on
    /// errors before releasing retained charges. The checkpoint may revoke copy
    /// or publication and gets cumulative local bytes (including the final check).
    #[allow(clippy::too_many_arguments)]
    pub fn stage(
        &self,
        owner: &Owner,
        stage_id: &str,
        scope: Scope,
        created: i64,
        source: Source,
        limit: u64,
        mut checkpoint: impl FnMut(u64) -> Result<()>,
    ) -> Result<Stage> {
        if !hex(stage_id, 32)
            || !owner.matches(&scope)
            || created < 0
            || !opaque(&scope.chat)
            || scope.topic.as_ref().is_some_and(|s| !opaque(s))
        {
            return Err(Error::Integrity);
        }
        check_size(source.len(), limit)?;
        checkpoint(0)?;
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).map_err(|_| Error::Storage)?;
        let client_id = format!("{:032x}", u128::from_be_bytes(random));
        if client_id == stage_id {
            return Err(Error::Integrity);
        }
        let uploads = self.uploads(owner, true)?;
        let name = fd::cname(stage_id.as_bytes())?;
        uploads.mkdir(&name)?; // Always exclusive. Existing stage IDs are never reopened here.
        let dir = uploads.child(&name, Policy::Private)?;
        let _lease = dir.lock()?;
        let mut published_bytes = None;
        let operation = (|| {
            uploads.file.sync_all()?;
            let mut bytes = fd::temp(&dir)?;
            let mut reader = &source.file;
            let (hash, format) = scan(&mut reader, source.len(), |count, chunk| {
                source.check()?;
                bytes.write_all(chunk)?;
                checkpoint(count)?;
                Ok(())
            })?;
            bytes.flush()?;
            bytes.sync_all()?;
            source.publication_check()?;
            checkpoint(source.len())?;
            dir.check()?;
            if bytes.metadata()?.len() != source.len() {
                return Err(Error::Storage);
            }
            fd::link(&dir, &bytes, c"bytes")?;
            published_bytes = Some(fd::id(&bytes)?);
            bytes.set_permissions(Permissions::from_mode(0o400))?;
            bytes.sync_all()?;
            let fingerprint = Fingerprint::from(bytes.metadata()?);
            dir.bind(c"bytes", &bytes)?;
            drop(bytes);
            dir.file.sync_all()?;
            let manifest = Manifest {
                version: 1,
                stage_id: stage_id.into(),
                client_id,
                scope,
                created,
                name: source.display_name.clone(),
                size: source.len(),
                format,
                fingerprint,
                sha256: hash,
                phase: Phase::Ready,
            };
            manifest.check()?;
            let stage = Stage {
                dir: dir.clone(),
                manifest,
            };
            let held = stage.open_bytes()?;
            stage.publish(None, &stage.manifest, || {
                checkpoint(source.len())?;
                source.publication_check()?;
                stage.check_bytes(&held)
            })?;
            Ok(stage)
        })();
        if matches!(&operation, Err(e) if *e != Error::Unknown) {
            // Only this fresh, unclaimed allocation may be rolled back. A foreign
            // entry, ambiguous ready publication, or failed cleanup retains charge.
            if let Some(identity) = published_bytes {
                fd::unlink(&dir, c"bytes", identity, false)?;
            }
            dir.file.sync_all()?;
            fd::unlink(&uploads, &name, dir.identity, true)?;
            uploads.file.sync_all().map_err(|_| Error::Unknown)?;
        }
        operation
    }
}

// Consume exactly the expected bytes and one bounded EOF probe. A short read
// need not be EOF; collect the signature across short reads as well.
fn scan(
    reader: &mut impl Read,
    size: u64,
    mut chunk: impl FnMut(u64, &[u8]) -> Result<()>,
) -> Result<(String, Format)> {
    let mut hash = Sha256::new();
    let mut prefix = Vec::with_capacity(8);
    let mut buffer = [0; CHUNK];
    let mut count = 0;
    while count < size {
        let limit = (size - count).min(CHUNK as u64) as usize;
        let n = reader.read(&mut buffer[..limit])?;
        if n == 0 {
            return Err(Error::Integrity);
        }
        count += n as u64;
        chunk(count, &buffer[..n])?;
        hash.update(&buffer[..n]);
        prefix.extend_from_slice(&buffer[..n.min(8 - prefix.len())]);
    }
    if reader.read(&mut buffer[..1])? != 0 {
        return Err(Error::Integrity);
    }
    Ok((format!("{:x}", hash.finalize()), Format::detect(&prefix)))
}

#[derive(Clone)]
pub(crate) struct Stage {
    pub(super) dir: Arc<Dir>,
    manifest: Manifest,
}
pub(crate) struct Verified {
    stage: Stage,
    handles: Verification,
}
enum Verification {
    Local { held: File, _lease: File },
    Core(Box<ch_core::managed_stage::VerifiedStageBinding>),
}
pub(crate) struct LiveClaim {
    stage: Stage,
    handles: ClaimHandles,
}
enum ClaimHandles {
    Core(Box<ch_core::managed_stage::LiveStageClaim>),
    Local { held: File, _lease: File, used: bool },
}

pub(super) fn read_manifest(dir: &Dir) -> Result<(Manifest, File)> {
    let probe = fd::openat(dir.fd(), c"manifest.json", libc::O_PATH, 0)?;
    let meta = probe.metadata()?;
    if fd::id(&probe)?.mount != dir.identity.mount
        || !meta.is_file()
        || meta.uid() != fd::uid()
        || meta.mode() & 0o7777 != 0o600
        || meta.nlink() != 1
        || meta.len() > MANIFEST_CAP
    {
        return Err(Error::Integrity);
    }
    let file = fd::openat(dir.fd(), c"manifest.json", libc::O_RDONLY, 0)?;
    if fd::id(&file)? != fd::id(&probe)? {
        return Err(Error::StageChanged);
    }
    let original = Fingerprint::from(meta);
    let mut buf = Vec::new();
    (&file).take(MANIFEST_CAP + 1).read_to_end(&mut buf)?;
    dir.bind(c"manifest.json", &file)?;
    if buf.len() as u64 != original.size || original != Fingerprint::from(file.metadata()?) {
        return Err(Error::StageChanged);
    }
    let m: Manifest = serde_json::from_slice(&buf).map_err(|_| Error::Integrity)?;
    m.check()?;
    Ok((m, file))
}
impl Stage {
    pub(super) fn from_inventory(dir: Arc<Dir>, manifest: Manifest) -> Self {
        Self { dir, manifest }
    }
    pub(super) fn load(dir: Arc<Dir>, owner: &Owner, stage_id: &str) -> Result<Self> {
        if dir.identity.mount != dir.parent().ok_or(Error::Integrity)?.identity.mount {
            return Err(Error::Integrity);
        }
        let (manifest, _) = read_manifest(&dir)?;
        if manifest.stage_id != stage_id || !owner.matches(&manifest.scope) {
            return Err(Error::Integrity);
        }
        let stage = Self { dir, manifest };
        if stage.manifest.phase == Phase::Ready {
            stage.check_layout()?;
            stage.open_bytes()?;
        }
        Ok(stage)
    }
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    /// Deterministic managed path for retained-row comparison; observation does
    /// not grant initial admission, which requires a fresh verified live claim.
    pub fn managed_path(&self) -> PathBuf {
        self.dir.path.join("bytes")
    }
    fn check_layout(&self) -> Result<()> {
        let mut count = 0;
        for entry in std::fs::read_dir(format!("/proc/self/fd/{}", self.dir.fd()))? {
            let name = entry?.file_name();
            if !matches!(name.as_bytes(), b"bytes" | b"manifest.json") || count == 2 {
                return Err(Error::Integrity);
            }
            count += 1;
        }
        if count != 2 {
            return Err(Error::Integrity);
        }
        self.dir.check()
    }
    fn current(&self) -> Result<File> {
        self.check_layout()?;
        let (actual, f) = read_manifest(&self.dir)?;
        if actual != self.manifest {
            return Err(Error::StageChanged);
        }
        Ok(f)
    }
    fn check_bytes(&self, f: &File) -> Result<()> {
        self.dir
            .bind(c"bytes", f)
            .map_err(|_| Error::StageChanged)?;
        if fd::id(f)?.mount != self.dir.identity.mount
            || Fingerprint::from(f.metadata()?) != self.manifest.fingerprint
        {
            return Err(Error::StageChanged);
        }
        Ok(())
    }
    pub(super) fn open_bytes(&self) -> Result<File> {
        let probe = fd::openat(self.dir.fd(), c"bytes", libc::O_PATH, 0)
            .map_err(|_| Error::StageChanged)?;
        self.check_bytes(&probe)?;
        let f = fd::openat(self.dir.fd(), c"bytes", libc::O_RDONLY, 0)
            .map_err(|_| Error::StageChanged)?;
        self.check_bytes(&f)?;
        Ok(f)
    }
    fn publish(
        &self,
        old: Option<&File>,
        next: &Manifest,
        before: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        self.publish_with(old, next, before, |f, _| f.sync_all())
    }
    pub(super) fn publish_with(
        &self,
        old: Option<&File>,
        next: &Manifest,
        before: impl FnOnce() -> Result<()>,
        sync: impl Fn(&File, bool) -> io::Result<()>,
    ) -> Result<()> {
        next.check()?;
        let json = serde_json::to_vec(next).map_err(|_| Error::Integrity)?;
        if json.len() as u64 > MANIFEST_CAP {
            return Err(Error::Integrity);
        }
        let mut temp = fd::temp(&self.dir)?;
        temp.write_all(&json)?;
        temp.flush()?;
        sync(&temp, false)?;
        self.dir.check()?;
        if let Some(previous) = old {
            fd::link(&self.dir, &temp, c".next")?;
            let result = (|| {
                before()?;
                self.dir.bind(c".next", &temp)?;
                self.dir.bind(c"manifest.json", previous)?;
                fd::replace(&self.dir, c".next", c"manifest.json")
            })();
            if result.is_err() {
                fd::unlink(&self.dir, c".next", fd::id(&temp)?, false)?;
            }
            result?;
        } else {
            before()?;
            fd::link(&self.dir, &temp, c"manifest.json")?;
        }
        // At this point the new phase may already survive a crash. Never roll
        // back or hand out first-admission authority on any subsequent failure.
        sync(&self.dir.file, true).map_err(|_| Error::Unknown)?;
        self.dir
            .bind(c"manifest.json", &temp)
            .map_err(|_| Error::Unknown)
    }
    pub fn verify(
        &self,
        limit: u64,
        mut checkpoint: impl FnMut(u64) -> Result<()>,
    ) -> Result<Verified> {
        let lease = self.dir.lock()?;
        self.current()?;
        if self.manifest.phase != Phase::Ready {
            return Err(Error::Integrity);
        }
        check_size(self.manifest.size, limit)?;
        let held = self.open_bytes()?;
        checkpoint(0)?;
        let mut reader = &held;
        let (digest, format) = scan(&mut reader, self.manifest.size, |n, _| {
            self.check_bytes(&held)?;
            checkpoint(n)
        })?;
        self.check_bytes(&held)?;
        self.current()?;
        if digest != self.manifest.sha256 || format != self.manifest.format {
            return Err(Error::StageChanged);
        }
        Ok(Verified {
            stage: self.clone(),
            handles: Verification::Local { held, _lease: lease },
        })
    }
    /// Production admission uses a Core proof bound to the exact Accounts-issued lease.
    pub fn verify_bound(
        &self,
        owner: &ch_core::runtime::AccountStorageBinding,
        limit: u64,
        checkpoint: impl FnMut(u64) -> Result<()>,
    ) -> Result<Verified> {
        let proof = ch_core::managed_stage::VerifiedStageBinding::verify_ready(
            owner, &self.manifest.stage_id, limit, checkpoint,
        )?;
        self.current()?;
        proof.check_directory(self.dir.file.as_fd())?;
        if proof.manifest() != &self.manifest { return Err(Error::StageChanged); }
        Ok(Verified { stage: self.clone(), handles: Verification::Core(Box::new(proof)) })
    }
    /// proof must check absence of the fixed outbox row AND rule out other
    /// stored references under the single-writer invariant. Ambiguity retains.
    pub fn discard_ready(
        &self,
        proof: impl FnOnce(&Manifest, &Path) -> Result<bool>,
    ) -> Result<()> {
        // A corrupt digest is also insufficient authority for destructive cleanup.
        // Hold the same bounded scan slot as verify, and retain the lease through unlink.
        let verified = self.verify(super::MAX_BYTES, |_| Ok(()))?;
        let Verification::Local { held, .. } = &verified.handles else {
            return Err(Error::Integrity);
        };
        let previous = self.current()?;
        let mut discarded = self.manifest.clone();
        discarded.phase = Phase::Discarded;
        self.publish(Some(&previous), &discarded, || {
            if !proof(&self.manifest, &self.managed_path())? {
                return Err(Error::Integrity);
            }
            self.check_bytes(held)
        })?;
        let remove = || {
            let (current, file) = read_manifest(&self.dir)?;
            if current != discarded {
                return Err(Error::StageChanged);
            }
            self.check_bytes(held)?;
            fd::unlink(&self.dir, c"bytes", fd::id(held)?, false)?;
            self.dir.file.sync_all()?;
            fd::unlink(&self.dir, c"manifest.json", fd::id(&file)?, false)?;
            self.dir.file.sync_all()?;
            let parent = self.dir.parent().ok_or(Error::Integrity)?;
            let name = fd::cname(discarded.stage_id.as_bytes())?;
            fd::unlink(parent, &name, self.dir.identity, true)?;
            parent.file.sync_all()?;
            if fd::named(parent, &name).is_err_and(|e| e.kind() == io::ErrorKind::NotFound) {
                Ok(())
            } else {
                Err(Error::Integrity)
            }
        };
        remove().map_err(|_| Error::Unknown)
    }
}
impl Verified {
    pub fn claim(self, intent: Intent) -> Result<LiveClaim> {
        let Self { mut stage, handles } = self;
        match handles {
            Verification::Core(proof) => {
                let claim = (*proof).claim(intent)?;
                stage.manifest = claim.manifest().clone();
                Ok(LiveClaim { stage, handles: ClaimHandles::Core(Box::new(claim)) })
            }
            Verification::Local { held, _lease } => {
                // Legacy helper/discard code cannot construct Core claim authority.
                intent.check()?;
                let previous = stage.current()?;
                let mut next = stage.manifest.clone();
                next.phase = Phase::Claimed(intent);
                stage.publish(Some(&previous), &next, || stage.check_bytes(&held))?;
                stage.manifest = next;
                Ok(LiveClaim { stage, handles: ClaimHandles::Local { held, _lease, used: false } })
            }
        }
    }
}
impl LiveClaim {
    pub fn manifest(&self) -> &Manifest { self.stage.manifest() }

    /// Callable once on the unique task that completed the actual durable transition.
    pub fn checked_path(&mut self) -> Result<PathBuf> {
        match &mut self.handles {
            ClaimHandles::Core(claim) => {
                let path = claim.checked_path()?;
                self.stage.current()?;
                if path != self.stage.managed_path() { return Err(Error::StageChanged); }
                Ok(path)
            }
            ClaimHandles::Local { held, used, .. } => {
                if *used { return Err(Error::Integrity); }
                *used = true;
                self.stage.current()?;
                self.stage.check_bytes(held)?;
                Ok(self.stage.managed_path())
            }
        }
    }

    /// Core independently confirms the exact canonical retained request.
    pub fn consume(self, outbox: &ch_core::outbox::Outbox) -> Result<Stage> {
        let Self { mut stage, handles } = self;
        let ClaimHandles::Core(claim) = handles else { return Err(Error::Integrity); };
        stage.manifest = (*claim).consume(outbox)?;
        Ok(stage)
    }

    #[cfg(test)]
    pub fn consume_for_test(self) -> Result<Stage> {
        let Self { mut stage, handles } = self;
        let ClaimHandles::Local { used, .. } = &handles else { return Err(Error::Integrity); };
        if !*used { return Err(Error::Integrity); }
        let previous = stage.current()?;
        let mut next = stage.manifest.clone();
        let Phase::Claimed(intent) = next.phase else { return Err(Error::Integrity); };
        next.phase = Phase::Consumed(intent);
        stage.publish(Some(&previous), &next, || Ok(()))?;
        stage.manifest = next;
        Ok(stage)
    }
}
