//! Shared deny-only owner metadata. Closing never proves reader closure or grants IO.
use super::{Error, StageIdentity};
use crate::runtime::AccountStorageObservation;
use crate::managed_stage::{AccountRootAnchor, AccountRootIdentity,catalog::{PrivateByteCatalog,OwnerCatalogCheck}};
use std::{collections::HashMap,os::unix::ffi::OsStrExt,path::PathBuf,sync::{Arc,Mutex,PoisonError,Weak}};

const RECORDS: usize = 256;
const CLOSINGS: usize = 256;

struct Record {
    owner: AccountStorageObservation,
    id: Option<i64>,
    identity: Option<StageIdentity>,
}

#[derive(Default)]
struct State {
    stopped: bool,
    pending: HashMap<String, AccountStorageObservation>,
    records: HashMap<String, Record>,
    // Restored namespaces are negative-only. No newly minted Gate/epoch may
    // convert historical uncertainty into reader, dispatch or removal authority.
    recovered: HashMap<String,PathBuf>,
    // Namespace keys, not one accumulating entry for every revoked Gate generation.
    closing: HashMap<PathBuf, Closing>,
}
struct Closing { original:AccountStorageObservation, anchor:Option<AccountRootIdentity> }

/// Opaque Core-issued negative barrier handle retained by the actual Accounts clone.
/// Contains no Sources/Accounts backreference, descriptor, storage guard or network task.
#[derive(Clone)]
pub(crate) struct OwnerBarriers(Arc<Mutex<State>>,Arc<PrivateByteCatalog>);

impl OwnerBarriers {
    pub(super) fn new(catalog:Arc<PrivateByteCatalog>) -> Self { Self(Arc::new(Mutex::new(State::default())),catalog) }
    pub(super) fn restore(&self,recovered:super::restore::Recovered) -> Result<(),Error> {
        let mut state=self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if state.stopped || !state.pending.is_empty() || !state.records.is_empty() || !state.recovered.is_empty() {
            return Err(Error::Conflict);
        }
        state.recovered=recovered.roots;
        Ok(())
    }

    pub(crate) async fn prepare_generic(&self, jobs:&super::OwnedJobs, method:&str,
        params:&serde_json::Value) -> Result<Option<crate::managed_stage::catalog::ExternalInputs>,Error> {
        if !file_input(method,params) { return Ok(None); }
        {
            let state=self.0.lock().unwrap_or_else(PoisonError::into_inner);
            if state.stopped { return Err(Error::Closed); }
            if !state.recovered.is_empty() { return Err(Error::Busy); }
            if state.pending.is_empty() && state.records.is_empty() { return Ok(None); }
        }
        let paths=input_paths(method,params)?;
        let catalog=self.1.clone();
        jobs.run(move||catalog.external_inputs(paths)).await?
            .map(Some).map_err(|_|Error::Busy)
    }

    // No path/inode or reader authority follows from an empty registry. Legacy
    // unregistered flow is unchanged. Outside inputs require a fresh observation.
    // Publication is the actual nonblocking writer.try_send, under the SAME
    // reservation lock as pending admission. No DB/FS/await runs in this closure.
    pub(crate) fn generic_frame<R>(&self, method: &str, params: &serde_json::Value,
        external:Option<&crate::managed_stage::catalog::ExternalInputs>,
        publish: impl FnOnce() -> R) -> Result<R, Error> {
        if !file_input(method, params) { return Ok(publish()); }
        let state = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if state.stopped { return Err(Error::Closed); }
        if !state.recovered.is_empty() { return Err(Error::Busy); }
        if !state.pending.is_empty() || !state.records.is_empty() {
            let paths=input_paths(method,params)?;
            let result=self.1.external_frame(external.ok_or(Error::Busy)?, &paths,publish)
                .map_err(|_|Error::Busy);
            drop(state);
            return result;
        }
        let result = publish();
        drop(state);
        Ok(result)
    }

    pub(super) fn reserve(&self, owner: AccountStorageObservation, stage: String) -> Result<Reservation, Error> {
        let mut state = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if state.stopped || owner.is_revoked() || state.closing.contains_key(owner.namespace()) {
            return Err(Error::Closed);
        }
        if state.pending.contains_key(&stage) || state.records.contains_key(&stage) || state.recovered.contains_key(&stage) { return Err(Error::Conflict); }
        if state.pending.len()+state.records.len()+state.recovered.len() >= RECORDS { return Err(Error::Busy); }
        state.pending.insert(stage.clone(),owner.clone());
        Ok(Reservation { state:Arc::downgrade(&self.0),owner,stage,settled:false })
    }

    pub(super) fn stop(&self) {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).stopped=true;
    }

    pub(crate) fn begin_close(&self, original: AccountStorageObservation, anchor:Option<&AccountRootAnchor>) -> Result<OwnerClose, Error> {
        let mut state=self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let root=original.namespace().to_owned();
        if let Some(existing)=state.closing.get(&root) {
            if existing.original != original { return Err(Error::Conflict); }
        } else {
            if state.closing.len() >= CLOSINGS { return Err(Error::Busy); }
            state.closing.insert(root.clone(),Closing {original:original.clone(),anchor:anchor.map(|a|a.identity().clone())});
        }
        Ok(OwnerClose { barriers:self.clone(),root,original })
    }

    #[cfg(test)]
    pub(crate) fn census(&self) -> (usize,usize,usize) {
        let state=self.0.lock().unwrap();
        (state.pending.len(),state.records.values().filter(|r|r.id.is_some()).count(),
            state.records.values().filter(|r|r.id.is_none()).count()+state.recovered.len())
    }
    #[cfg(test)]
    pub(crate) fn closing_count(&self) -> usize { self.0.lock().unwrap().closing.len() }
}

fn file_input(method: &str, params: &serde_json::Value) -> bool {
    match method {
        // Required inputs: malformed/missing/relative inputs do not bypass the
        // barrier. Album members need not be traversed to deny this whole route.
        "messages.send_file" | "messages.send_album" => true,
        "messages.edit" | "group.edit_photo" => params.get("path").is_some_and(|v| !v.is_null()),
        "profile.update" => params.get("photo_path").is_some_and(|v| !v.is_null()),
        // Vendor inputs have no reviewed schema; never infer independence from
        // a key name or allow arbitrary nesting to escape this deny boundary.
        _ => method.starts_with("x-"),
    }
}

fn input_paths(method:&str,params:&serde_json::Value) -> Result<Vec<String>,Error> {
    let path=|value:Option<&serde_json::Value>| -> Result<String,Error> {
        value.and_then(|v|v.as_str()).filter(|v|!v.is_empty() && v.len()<=4096)
            .map(str::to_owned).ok_or(Error::Conflict)
    };
    match method {
        "messages.send_file"|"messages.edit"|"group.edit_photo" => Ok(vec![path(params.get("path"))?]),
        "profile.update" => Ok(vec![path(params.get("photo_path"))?]),
        "messages.send_album" => {
            let items=params.get("items").and_then(|v|v.as_array())
                .filter(|items|(2..=10).contains(&items.len())).ok_or(Error::Conflict)?;
            items.iter().map(|item|path(item.get("path"))).collect()
        }
        _=>Err(Error::Conflict),
    }
}

pub(crate) struct OwnerClose {
    barriers:OwnerBarriers,
    root:PathBuf,
    original:AccountStorageObservation,
}

impl OwnerClose {
    // Runs only inside the already counted actual owner blocking worker, after
    // old storage leases have drained. Never trusts manifest scope/client hints.
    pub(crate) fn fresh_catalog(&self) -> crate::managed_stage::Result<OwnerCatalogCheck> {
        let folder=self.root.file_name().ok_or(crate::managed_stage::Error::Integrity)?.as_bytes();
        self.barriers.1.check_owner(folder)
    }

    pub(crate) fn matches_anchor(&self, anchor:&AccountRootAnchor) -> bool {
        let state=self.barriers.0.lock().unwrap_or_else(PoisonError::into_inner);
        state.closing.get(&self.root).is_some_and(|entry|
            entry.original==self.original && entry.anchor.as_ref()==Some(anchor.identity()))
    }

    pub(crate) fn in_use(&self) -> bool {
        let state=self.barriers.0.lock().unwrap_or_else(PoisonError::into_inner);
        state.pending.values().any(|owner|owner.namespace()==self.root.as_path())
            || state.records.values().any(|record|record.owner.namespace()==self.root.as_path())
            || state.recovered.values().any(|root|root==&self.root)
    }

    // Caller is Accounts' owned removal under its membership/lifecycle and old
    // storage writer, AFTER exact owner/root-anchor/current-Gate confirmation.
    pub(crate) fn rearmed(&self, fresh:&AccountStorageObservation, anchor:&AccountRootAnchor) -> Result<(), Error> {
        let old=self.original.owner();let next=fresh.owner();
        if !self.original.is_revoked() || fresh.is_revoked() || fresh==&self.original
            || fresh.namespace()!=self.root.as_path() || old.account_id!=next.account_id
            || old.plugin_id!=next.plugin_id || old.created_at!=next.created_at
        { return Err(Error::Conflict); }
        let mut state=self.barriers.0.lock().unwrap_or_else(PoisonError::into_inner);
        let entry=state.closing.get(&self.root).ok_or(Error::Conflict)?;
        if entry.original!=self.original || entry.anchor.as_ref()!=Some(anchor.identity()) { return Err(Error::Conflict); }
        state.closing.remove(&self.root);
        Ok(())
    }

    // No source permission comes from deletion. Clear just this close reservation;
    // known source/unknown records are never removed here.
    pub(crate) fn deleted(&self) -> Result<(), Error> {
        if self.in_use() { return Err(Error::Conflict); }
        self.complete()
    }

    fn complete(&self) -> Result<(), Error> {
        let mut state=self.barriers.0.lock().unwrap_or_else(PoisonError::into_inner);
        if state.closing.get(&self.root).map(|entry|&entry.original)!=Some(&self.original) { return Err(Error::Conflict); }
        state.closing.remove(&self.root);
        Ok(())
    }
    // Drop intentionally leaves the namespace denied after an ambiguous failure.
}

pub(crate) struct Reservation {
    state:Weak<Mutex<State>>,
    owner:AccountStorageObservation,
    stage:String,
    settled:bool,
}

impl Reservation {
    pub(crate) fn publish(&mut self,id:i64,identity:StageIdentity) -> Result<(),Error> {
        let shared=self.state.upgrade().ok_or(Error::Closed)?;
        let mut state=shared.lock().unwrap_or_else(PoisonError::into_inner);
        if state.pending.get(&self.stage)!=Some(&self.owner) { return Err(Error::Conflict); }
        if let Some(old)=state.records.get(&self.stage) {
            if old.owner==self.owner && old.id==Some(id) && old.identity.as_ref()==Some(&identity) {
                self.settled=true;return Ok(());
            }
            return Err(Error::Conflict);
        }
        state.records.insert(self.stage.clone(),Record {owner:self.owner.clone(),id:Some(id),identity:Some(identity)});
        self.settled=true;Ok(())
    }
    pub(crate) fn quarantine(&mut self,identity:Option<StageIdentity>) {
        if let Some(shared)=self.state.upgrade() {
            shared.lock().unwrap_or_else(PoisonError::into_inner).records.entry(self.stage.clone())
                .or_insert_with(||Record {owner:self.owner.clone(),id:None,identity});
            self.settled=true;
        }
    }
    pub(crate) fn no_new_source(&mut self) { self.settled=true; }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(shared)=self.state.upgrade() {
            let mut state=shared.lock().unwrap_or_else(PoisonError::into_inner);
            if !self.settled {
                state.records.entry(self.stage.clone()).or_insert_with(||Record {
                    owner:self.owner.clone(),id:None,identity:None,
                });
            }
            if state.pending.get(&self.stage)==Some(&self.owner) { state.pending.remove(&self.stage); }
        }
    }
}
