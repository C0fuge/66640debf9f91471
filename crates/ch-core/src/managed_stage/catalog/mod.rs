//! Descriptive deny metadata only. No source, reader, wire or cleanup authority.
//! WIP: Core startup scans and owner-close fresh scans/epoch vetoes consume this
//! only to deny. App stage copy/phase/discard hooks invalidate observations.
//! Full writer/lifecycle coverage, Host input classification and reader/producer
//! enforcement remain separate activation gates. A complete inventory never
//! proves legacy physical readers closed;
//! stable unproved stage evidence and old inode negatives survive later absence.
mod origins;
mod scan;
mod inputs;
pub(crate) use inputs::ExternalInputs;
#[cfg(test)]
mod tests;

use super::{Error, Result, manifest::Scope};
use crate::host::HostConfig;
use std::{collections::{BTreeMap, BTreeSet}, path::PathBuf, sync::{Arc, Mutex, MutexGuard}};

const MAX_INODES: usize = 1024;
const MAX_STAGES: usize = 256;
const MAX_WRITERS: usize = 16;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Inode { dev: u64, ino: u64 }

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StageKey { account: Vec<u8>, stage: Vec<u8> }

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum StageKind { Ready, Claimed, Consumed, Discarded, Unknown }

/// Negative close-owner evidence, not a declaration that any reader has settled.
#[derive(Clone)]
pub(crate) struct LegacyCandidate {
    key: StageKey,
    directory: Option<Inode>,
    kind: StageKind,
    scope: Option<Scope>,
    client: Option<String>,
    unproved: bool,
}
impl LegacyCandidate {
    pub(crate) fn account_folder(&self) -> &[u8] { &self.key.account }
    #[cfg(test)]
    pub(crate) fn stage_folder(&self) -> &[u8] { &self.key.stage }
    #[cfg(test)]
    pub(crate) fn kind(&self) -> StageKind { self.kind }
    #[cfg(test)]
    pub(crate) fn scope_hint(&self) -> Option<&Scope> { self.scope.as_ref() }
    #[cfg(test)]
    pub(crate) fn client_hint(&self) -> Option<&str> { self.client.as_deref() }
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum InodeMatch { KnownPrivate, NotListed }

/// Missing from a complete set is still NOT a safe-input/read permission.
#[cfg(test)]
pub(crate) struct CatalogLookup { epoch: u64, complete: bool, matched: InodeMatch }
#[cfg(test)]
impl CatalogLookup {
    pub(crate) fn epoch(&self) -> u64 { self.epoch }
    pub(crate) fn complete(&self) -> bool { self.complete }
    pub(crate) fn matched(&self) -> InodeMatch { self.matched }
}

pub(crate) struct CatalogView {
    epoch:u64,complete:bool,writers:usize,
    #[cfg(test)] inodes:usize,
}
impl CatalogView {
    pub(crate) fn epoch(&self) -> u64 { self.epoch }
    pub(crate) fn complete(&self) -> bool { self.complete }
    pub(crate) fn writers(&self) -> usize { self.writers }
    #[cfg(test)]
    pub(crate) fn inode_count(&self) -> usize { self.inodes }
}

/// Negative owner-close veto only. No filesystem/read/closure/deletion authority.
/// Constructed from an actual fresh bounded scan, never from caller hints/bools.
pub(crate) struct OwnerCatalogCheck { catalog:Arc<PrivateByteCatalog>,epoch:u64,folder:Vec<u8> }
impl OwnerCatalogCheck {
    pub(crate) fn verify(&self) -> Result<()> {
        let state=self.catalog.lock();
        if state.closed || state.poisoned || !state.complete || state.active!=0 || state.epoch!=self.epoch
            || state.stages.values().any(|candidate|candidate.unproved && candidate.account_folder()==self.folder.as_slice())
        { return Err(Error::Busy); }
        Ok(())
    }
}

#[derive(Default)]
struct State {
    epoch: u64,
    active: usize,
    complete: bool,
    poisoned: bool,
    closed: bool,
    inodes: BTreeSet<Inode>,
    origins:origins::Origins,
    stages: BTreeMap<StageKey, LegacyCandidate>,
    root: Option<Inode>,
}
impl State {
    fn advance(&mut self) -> Result<u64> {
        self.complete = false;
        let Some(epoch) = self.epoch.checked_add(1) else {
            self.poisoned = true;
            return Err(Error::Integrity);
        };
        self.epoch = epoch;
        Ok(epoch)
    }
}

/// Core-owned metadata. Contains no Gate, account guard, FD or network handle.
#[cfg(test)]
type Probe=Arc<dyn Fn(&std::fs::Metadata)+Send+Sync>;

pub(crate) struct PrivateByteCatalog {
    data_dir:PathBuf,state:Mutex<State>,issuer:Arc<()>,readers:super::readers::ReaderSet,
    #[cfg(test)] probe:Mutex<Option<Probe>>,
}
impl PrivateByteCatalog {
    /// Private Core composition factory; no caller-vouched completeness or inode list.
    pub(crate) fn for_core(config: &HostConfig) -> Arc<Self> {
        Arc::new(Self { data_dir: config.data_dir.clone(), state: Mutex::new(State::default()), issuer:Arc::new(()),readers:super::readers::ReaderSet::new(),
            #[cfg(test)] probe:Mutex::new(None) })
    }
    pub(crate) fn mutations(self: &Arc<Self>) -> super::StageMutations {
        super::StageMutations { catalog: self.clone() }
    }
    pub(super) fn read_scope(&self)->Result<super::StageReadScope> {self.readers.enter()}
    #[cfg(test)]
    pub(crate) fn probe_for_test(&self,probe:Probe) {
        *self.probe.lock().unwrap_or_else(std::sync::PoisonError::into_inner)=Some(probe);
    }
    #[cfg(test)]
    pub(crate) fn exclude_readers_for_test(&self)->Result<tokio::sync::OwnedRwLockWriteGuard<()>> {
        self.readers.exclude()
    }
    pub(super) fn check_accounts_root(&self, root: &std::path::Path) -> Result<()> {
        if root == self.data_dir.join("accounts") { Ok(()) } else { Err(Error::Integrity) }
    }
    pub(crate) fn close(&self) {
        self.readers.close();
        let mut state = self.lock();
        state.closed = true;
        state.complete = false;
    }

    // Observe-only tracking cannot break otherwise valid file operations. If
    // bounded accounting fails, permanently refuse completeness for this epoch
    // of Core instead. Closing has a separate, permanent new-operation veto.
    pub(super) fn observe_mutation(self: &Arc<Self>) -> Result<CatalogMutation> {
        let mut state = self.lock();
        if state.closed { return Err(Error::Cancelled); }
        state.complete = false;
        let counted = if state.poisoned || state.active >= MAX_WRITERS || state.advance().is_err() {
            state.poisoned = true;
            false
        } else {
            state.active += 1;
            true
        };
        Ok(CatalogMutation { catalog: self.clone(), counted })
    }
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
    #[cfg(test)]
    pub(crate) fn view(&self) -> CatalogView {
        let state = self.lock();
        CatalogView { epoch: state.epoch, complete: state.complete && !state.poisoned,
            writers: state.active, inodes: state.inodes.len() }
    }
    #[cfg(test)]
    pub(crate) fn lookup_inode(&self, dev: u64, ino: u64) -> CatalogLookup {
        let state = self.lock();
        CatalogLookup { epoch: state.epoch, complete: state.complete && !state.poisoned,
            matched: if state.inodes.contains(&Inode { dev, ino }) { InodeMatch::KnownPrivate } else { InodeMatch::NotListed } }
    }
    #[cfg(test)]
    pub(crate) fn legacy_candidates(&self) -> Vec<LegacyCandidate> {
        self.lock().stages.values().filter(|entry| entry.unproved).cloned().collect()
    }

    // Strict test fault harness. Production writers use the real observe-only
    // StageMutations hook; no uncalled production mutation factory is retained.
    #[cfg(test)]
    pub(crate) fn begin_mutation(self: &Arc<Self>) -> Result<CatalogMutation> {
        let mut state = self.lock();
        state.complete = false;
        if state.closed { return Err(Error::Cancelled); }
        if state.poisoned || state.active >= MAX_WRITERS { return Err(Error::Busy); }
        state.advance()?;
        state.active += 1;
        Ok(CatalogMutation { catalog: self.clone(), counted: true })
    }

    /// Independent anchored scan is the ONLY completeness publisher. No quota or
    /// owner-close/source-settlement permission follows from its result.
    pub(crate) fn refresh(&self) -> Result<CatalogView> { self.refresh_with(|| {}, || {}) }

    pub(crate) fn check_owner(self:&Arc<Self>,folder:&[u8]) -> Result<OwnerCatalogCheck> {
        let refreshed=self.refresh()?;
        if !refreshed.complete() || refreshed.writers()!=0 { return Err(Error::Busy); }
        let check=OwnerCatalogCheck {catalog:self.clone(),epoch:refreshed.epoch(),folder:folder.to_vec()};
        check.verify()?;
        Ok(check)
    }

    fn refresh_with(&self, before_scan: impl FnOnce(), after_scan: impl FnOnce()) -> Result<CatalogView> {
        let _readers=self.read_scope()?;
        let epoch = {
            let mut state = self.lock();
            if state.closed { return Err(Error::Cancelled); }
            if state.poisoned || state.active != 0 { return Err(Error::Busy); }
            state.advance()?
        };
        before_scan();
        #[cfg(not(test))]
        let scanned = scan::walk(&self.data_dir);
        #[cfg(test)]
        let probe=self.probe.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        #[cfg(test)]
        let scanned = scan::walk_with_probe(&self.data_dir,probe);
        after_scan();
        let mut state = self.lock();
        // Partial negative evidence remains useful, even when publication is refused.
        let mut complete = scanned.complete;
        for inode in scanned.inodes {
            if !state.inodes.contains(&inode) && state.inodes.len() == MAX_INODES {
                // This negative cannot be retained. A later smaller scan must
                // never erase uncertainty about its possible outside aliases.
                state.poisoned=true;complete=false;continue;
            }
            state.inodes.insert(inode);
            if state.origins.record(inode,scanned.origins.get(&inode).cloned()) {
                state.poisoned=true;complete=false;
            }
        }
        // A supported copy may expose a temporary empty stage while this scan
        // runs. Only stable observations may become sticky legacy evidence.
        if state.epoch != epoch || state.active != 0 || state.poisoned || state.closed {
            state.complete = false;
            return Err(Error::Busy);
        }
        match (state.root, scanned.root) {
            (Some(old), Some(current)) if old != current => complete = false,
            (None, Some(current)) => state.root = Some(current),
            (Some(_), None) => complete = false,
            _ => {},
        }
        for (key, candidate) in scanned.stages {
            if let Some(old) = state.stages.get_mut(&key) {
                let rebound = old.directory.zip(candidate.directory).is_some_and(|(a,b)| a != b)
                    || old.scope.as_ref().zip(candidate.scope.as_ref()).is_some_and(|(a,b)| a != b)
                    || old.client.as_ref().zip(candidate.client.as_ref()).is_some_and(|(a,b)| a != b);
                if rebound {
                    old.kind = StageKind::Unknown; old.unproved = true; complete = false;
                } else {
                    if old.unproved && !candidate.unproved { old.kind = StageKind::Unknown; }
                    else { old.kind = candidate.kind; }
                    old.unproved |= candidate.unproved;
                    if old.directory.is_none() { old.directory = candidate.directory; }
                    if old.scope.is_none() { old.scope = candidate.scope; }
                    if old.client.is_none() { old.client = candidate.client; }
                }
            } else if state.stages.len() < MAX_STAGES {
                state.stages.insert(key, candidate);
            } else {
                // Lost owner/stage history is not repaired by later absence.
                state.poisoned=true;complete=false;
            }
        }
        // Absence NEVER deletes old inode negatives or unproved legacy candidates.
        state.complete = complete;
        let view = CatalogView { epoch: state.epoch, complete, writers: state.active,
            #[cfg(test)] inodes: state.inodes.len() };
        if complete { Ok(view) } else { Err(Error::Integrity) }
    }
}

/// Invalidation/lifetime ownership only. Dropping it cannot certify completion.
pub(crate) struct CatalogMutation { catalog: Arc<PrivateByteCatalog>, counted: bool }
impl Drop for CatalogMutation {
    fn drop(&mut self) {
        let mut state = self.catalog.lock();
        state.complete = false;
        if !self.counted { return; }
        if let Some(active) = state.active.checked_sub(1) { state.active = active; }
        else { state.poisoned = true; }
    }
}
