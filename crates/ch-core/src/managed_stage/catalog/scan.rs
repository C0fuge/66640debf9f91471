//! Independent fixed-depth walk. Never trusts App inventory or stored manifest hashes
//! as positive ownership/settlement evidence; all descriptors close in this operation.
use super::{Inode, LegacyCandidate, StageKey, StageKind,origins::{Location,Observation}};
use crate::managed_stage::{Error, Result, fd::{self, Dir, Policy}, manifest::{Fingerprint, Phase},
    verify::read_manifest_file};
use std::{collections::{BTreeMap, BTreeSet}, ffi::{OsString, CString}, fs, io,
    os::unix::{ffi::OsStrExt, fs::MetadataExt}, path::Path, sync::Arc};

const ACCOUNTS: usize = 4096;
const MANAGED: usize = 256;
const METADATA: u64 = 1024 * 1024;

pub(super) struct Scan {
    pub(super) complete: bool,
    pub(super) inodes: BTreeSet<Inode>,
    pub(super) origins:BTreeMap<Inode,Observation>,
    pub(super) stages: BTreeMap<StageKey, LegacyCandidate>,
    pub(super) root: Option<Inode>,
    remaining: usize,
    metadata: u64,
    seen_stage: BTreeMap<String, StageKey>,
    seen_client: BTreeMap<String, StageKey>,
    #[cfg(test)] probe:Option<super::Probe>,
}

fn empty()->Scan {
    Scan {complete:true,inodes:BTreeSet::new(),origins:BTreeMap::new(),stages:BTreeMap::new(),root:None,
        remaining:MANAGED,metadata:METADATA,seen_stage:BTreeMap::new(),seen_client:BTreeMap::new(),
        #[cfg(test)] probe:None}
}
#[cfg(not(test))]
pub(super) fn walk(data_dir:&Path)->Scan {
    let mut scan=empty();
    if scan.run(data_dir).is_err() {scan.complete=false;}scan
}
#[cfg(test)]
pub(super) fn walk_with_probe(data_dir:&Path,probe:Option<super::Probe>)->Scan {
    let mut scan=empty();scan.probe=probe;
    if scan.run(data_dir).is_err() {scan.complete=false;}scan
}

fn name(value: &OsString) -> Result<CString> { fd::cname(value.as_bytes()) }

fn entries(dir: &Dir, budget: &mut usize) -> Result<Vec<OsString>> {
    dir.check()?;
    let mut result = Vec::new();
    for entry in fs::read_dir(format!("/proc/self/fd/{}", dir.fd()))? {
        if *budget == 0 { return Err(Error::TooLarge); }
        *budget -= 1;
        result.push(entry?.file_name());
    }
    result.sort_unstable();
    dir.check()?;
    Ok(result)
}

impl Scan {
    fn run(&mut self, data_dir: &Path) -> Result<()> {
        let data = fd::data_dir(data_dir)?;
        self.root = Some(Inode { dev: data.identity.dev, ino: data.identity.ino });
        let accounts = match data.child(c"accounts", Policy::Owner) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                data.check()?;
                return Ok(());
            }
            other => other?,
        };
        if accounts.identity.mount != data.identity.mount { return Err(Error::Integrity); }
        let mut count = ACCOUNTS;
        for account in entries(&accounts, &mut count)? {
            if self.account(&accounts, &account,data.identity).is_err() { self.complete = false; }
        }
        accounts.check()
    }

    fn account(&mut self, accounts: &Arc<Dir>, account_name: &OsString,data:fd::Identity) -> Result<()> {
        let account = accounts.child(&name(account_name)?, Policy::Owner)?;
        if account.identity.mount != accounts.identity.mount { return Err(Error::Integrity); }
        let uploads = match account.child(c"uploads", Policy::Private) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => { account.check()?; return Ok(()); }
            other => other?,
        };
        if uploads.identity.mount != account.identity.mount { return Err(Error::Integrity); }
        for stage in entries(&uploads, &mut self.remaining)? {
            let key = StageKey { account: account_name.as_bytes().to_vec(), stage: stage.as_bytes().to_vec() };
            if self.stage(&uploads, &stage, key.clone(),[data,accounts.identity,account.identity,uploads.identity]).is_err() {
                self.complete = false;
                self.stages.entry(key.clone()).or_insert_with(|| unknown(key, None)).unproved = true;
            }
        }
        uploads.check()
    }

    fn stage(&mut self, uploads: &Arc<Dir>, stage_name: &OsString, key: StageKey,namespace:[fd::Identity;4]) -> Result<()> {
        let probe = fd::openat(uploads.fd(), &name(stage_name)?, libc::O_PATH, 0)?;
        uploads.bind(&name(stage_name)?, &probe)?;
        let metadata = probe.metadata()?;
        if metadata.is_file() {
            self.record(&probe,Observation::Ambiguous)?; // Orphan regular bytes under uploads still deny aliases.
            self.stages.insert(key.clone(), unknown(key, None));
            return Err(Error::Integrity);
        }
        let dir = uploads.child(&name(stage_name)?, Policy::Private)?;
        if dir.identity.mount != uploads.identity.mount { return Err(Error::Integrity); }
        let inode = Some(Inode { dev: dir.identity.dev, ino: dir.identity.ino });
        self.stages.insert(key.clone(), unknown(key.clone(), inode));
        let mut manifest_size = None;
        let mut bytes = false;
        let mut byte_fingerprint = None;
        let mut valid = true;
        for child in entries(&dir, &mut self.remaining)? {
            let file = fd::openat(dir.fd(), &name(&child)?, libc::O_PATH, 0)?;
            dir.bind(&name(&child)?, &file)?;
            let metadata = file.metadata()?;
            #[cfg(test)]
            if let Some(probe)=&self.probe {probe(&metadata);}
            if metadata.is_file() {
                let id=fd::id(&file)?;
                let owned=metadata.uid()==fd::uid() && metadata.nlink()==1 && id.mount==dir.identity.mount;
                let observed=if owned {Observation::At(Box::new(Location {
                    namespace:[namespace[0],namespace[1],namespace[2],namespace[3],dir.identity],
                    key:key.clone(),name:child.as_bytes().to_vec(),file:id,
                }))} else {Observation::Ambiguous};
                self.record(&file,observed)?;
                if !owned { valid=false; }
            } else { valid = false; }
            match child.as_bytes() {
                b"bytes" => {
                    bytes = metadata.is_file();
                    if bytes { byte_fingerprint = Some(Fingerprint::from(metadata)); }
                },
                b"manifest.json" => manifest_size = Some(metadata.len()),
                _ => valid = false,
            }
        }
        let length = manifest_size.ok_or(Error::Integrity)?;
        if length > 64 * 1024 || length > self.metadata { return Err(Error::TooLarge); }
        self.metadata -= length;
        let (manifest, _) = read_manifest_file(&dir)?;
        if manifest.stage_id.as_bytes() != stage_name.as_bytes() { return Err(Error::Integrity); }
        valid &= if bytes { byte_fingerprint.as_ref() == Some(&manifest.fingerprint) }
            else { manifest.phase == Phase::Discarded };
        let kind = match &manifest.phase {
            Phase::Ready => StageKind::Ready, Phase::Claimed(_) => StageKind::Claimed,
            Phase::Consumed(_) => StageKind::Consumed, Phase::Discarded => StageKind::Discarded,
        };
        // Stored phase/hash are hints only. No scan can certify physical reader closure.
        let candidate = LegacyCandidate { key: key.clone(), directory: inode, kind,
            scope: Some(manifest.scope), client: Some(manifest.client_id.clone()),
            unproved: true };
        self.stages.insert(key.clone(), candidate);
        let duplicates = [self.seen_stage.insert(manifest.stage_id, key.clone()),
            self.seen_client.insert(manifest.client_id, key.clone())];
        for duplicate in duplicates.into_iter().flatten() {
            valid = false;
            if let Some(previous) = self.stages.get_mut(&duplicate) { previous.kind = StageKind::Unknown; previous.unproved = true; }
            if let Some(current) = self.stages.get_mut(&key) { current.kind = StageKind::Unknown; current.unproved = true; }
        }
        dir.check()?;
        if valid { Ok(()) } else { Err(Error::Integrity) }
    }

    fn record(&mut self, file: &fs::File,observed:Observation) -> Result<()> {
        let id = fd::id(file)?;
        if id.ino == 0 { return Err(Error::Integrity); }
        let inode=Inode {dev:id.dev,ino:id.ino};
        if !self.inodes.insert(inode) {
            self.origins.insert(inode,Observation::Ambiguous);return Err(Error::Integrity);
        }
        self.origins.insert(inode,observed);Ok(())
    }
}

fn unknown(key: StageKey, directory: Option<Inode>) -> LegacyCandidate {
    LegacyCandidate { key, directory, kind: StageKind::Unknown, scope: None, client: None, unproved: true }
}
