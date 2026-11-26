//! Local selection and one-shot admission. Only the owned claim task can enter the outbox.
mod storage;
mod work;

use crate::{
    OpenFileDialog, events, ingress_fs as fs,
    projection::blocking,
    wire::{Failure, Result, Thread},
};
use ch_core::{
    outbox::{Outbox, OutboxItem, OutboxKind, OutboxState},
    runtime::Core,
};
use ch_db::{AccountRow, Db};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
};
use tokio::sync::{Semaphore, broadcast};
use tokio_util::sync::CancellationToken;

const KEYS: usize = 4096;
const JOBS: usize = 4;
const PAGE: usize = 16;
const MAX_REPLY: usize = 128 * 1024;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
fn token() -> std::io::Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes)
        .map_err(|_| std::io::Error::other("Cannot generate an ingress identifier"))?;
    Ok(format!("{:032x}", u128::from_be_bytes(bytes)))
}
fn valid_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 64
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
}
fn valid_thread(t: &Thread) -> bool {
    use ch_protocol::common::is_valid_id;
    is_valid_id(&t.account_id)
        && is_valid_id(&t.chat_id)
        && t.topic_id.as_deref().is_none_or(is_valid_id)
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
enum State {
    Choosing,
    Copying,
    Ready,
    Verifying,
    Enqueuing,
    Enqueued,
    AdmissionUnknown,
    Cancelling,
    Cancelled,
    Failed,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct Problem {
    code: &'static str,
    reason: &'static str,
    outcome_unknown: bool,
}
impl Problem {
    fn new(code: &'static str, reason: &'static str) -> Self {
        Self {
            code,
            reason,
            outcome_unknown: false,
        }
    }
    fn unknown() -> Self {
        Self {
            code: "unavailable",
            reason: "admissionUnknown",
            outcome_unknown: true,
        }
    }
    fn capacity() -> Self {
        Self::new("conflict", "capacity")
    }
}
impl From<fs::Error> for Problem {
    fn from(e: fs::Error) -> Self {
        use fs::Error::*;
        match e {
            UnsafeSource => Self::new("invalid_request", "unsafeSource"),
            SourceChanged => Self::new("conflict", "sourceChanged"),
            StageChanged => Self::new("conflict", "stageChanged"),
            TooLarge => Self::new("unsupported", "tooLarge"),
            Integrity => Self::new("conflict", "integrity"),
            Storage | Unknown => Self::new("unavailable", "storage"),
            Cancelled => Self::new("conflict", "interrupted"),
            TargetChanged => Self::new("conflict", "targetChanged"),
            Busy => Self::capacity(),
        }
    }
}

#[derive(Clone)]
struct Binding {
    manifest: fs::Manifest,
    path: PathBuf,
}
#[derive(Clone)]
struct Entry {
    stage: String,
    key: Option<String>,
    thread: Thread,
    plugin: String,
    birth: i64,
    generation: CancellationToken,
    state: State,
    name: Option<String>,
    bytes: Option<u64>,
    format: Option<fs::Format>,
    copied: Option<u64>,
    bound: Option<Binding>,
    intent: Option<fs::Intent>,
    proven: bool,
    outbox_row: Option<&'static str>,
    problem: Option<Problem>,
    active: Option<CancellationToken>,
}
impl Entry {
    fn owned(&self, t: &Thread, a: &AccountRow) -> bool {
        !self.generation.is_cancelled()
            && &self.thread == t
            && self.plugin == a.plugin_id
            && self.birth == a.created_at
            && t.account_id == a.account_id
    }
    fn render(&self, session: &str) -> Value {
        let id = self
            .outbox_row
            .and_then(|_| self.bound.as_ref().map(|b| &b.manifest.client_id));
        json!({"sessionId":session,"stageId":self.stage,"requestKey":self.key,"thread":self.thread,
            "state":self.state,"name":self.name,"bytes":self.bytes.map(|n|n.to_string()),"format":self.format,
            "copiedBytes":self.copied.map(|n|n.to_string()),"outboxId":id,"outboxRow":self.outbox_row,"error":self.problem})
    }
    fn bind(&mut self, binding: Binding) {
        let m = &binding.manifest;
        self.name = Some(m.name.clone());
        self.bytes = Some(m.size);
        self.format = Some(m.format);
        self.copied = Some(m.size);
        match &m.phase {
            fs::Phase::Ready => {
                self.state = State::Ready;
                self.intent = None;
            }
            fs::Phase::Claimed(i) => {
                self.state = State::AdmissionUnknown;
                self.intent = Some(i.clone());
                self.outbox_row = Some("unchecked");
                self.problem = Some(Problem::unknown());
            }
            fs::Phase::Consumed(i) => {
                self.state = State::Enqueued;
                self.intent = Some(i.clone());
                self.proven = true;
                self.outbox_row = Some("unchecked");
            }
            fs::Phase::Discarded => {
                self.state = State::Cancelled;
            }
        }
        self.bound = Some(binding);
    }
}
#[derive(Default)]
struct Ledger {
    entries: BTreeMap<u64, Entry>,
    ids: HashMap<String, u64>,
    keys: HashMap<String, u64>,
    next: u64,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    dirty: BTreeSet<String>,
    closed: bool,
    unsettled_charge: bool,
    inventory_epoch: u64,
    scanned_epoch: Option<u64>,
    next_refresh: Option<std::time::Instant>,
    sending: HashMap<String, (String, u32, Option<i64>)>,
}
impl Ledger {
    fn insert(&mut self, e: Entry) -> u64 {
        self.next += 1;
        let seq = self.next;
        if let Some(key) = &e.key {
            self.keys.insert(key.clone(), seq);
        }
        self.ids.insert(e.stage.clone(), seq);
        self.entries.insert(seq, e);
        seq
    }
    fn capacity(&mut self) -> Result<()> {
        self.tasks.retain(|t| !t.is_finished());
        if self.closed {
            Err(Failure::Unavailable)
        } else if self.tasks.len() >= JOBS {
            Err(Failure::Conflict)
        } else {
            Ok(())
        }
    }
}
#[cfg(test)]
type TestCheckpoint = dyn Fn(&str) -> std::result::Result<(), fs::Error> + Send + Sync;

pub(crate) struct Service {
    core: Arc<Core>,
    db: Arc<Mutex<Db>>,
    outbox: Outbox,
    changes: broadcast::Sender<events::Invalidation>,
    session: String,
    ledger: Mutex<Ledger>,
    provider: Mutex<Option<Arc<dyn OpenFileDialog>>>,
    stop: CancellationToken,
    scan: Arc<Semaphore>,
    starts: tokio::sync::Mutex<()>,
    refresh_gate: tokio::sync::Mutex<()>,
    readers: Arc<tokio::sync::RwLock<()>>,
    #[cfg(test)]
    checkpoint: Mutex<Option<Arc<TestCheckpoint>>>,
}
impl Service {
    pub(crate) async fn new(
        core: Arc<Core>,
        db: Arc<Mutex<Db>>,
        outbox: Outbox,
        changes: broadcast::Sender<events::Invalidation>,
    ) -> std::io::Result<Arc<Self>> {
        let s = Arc::new(Self {
            core,
            db,
            outbox,
            changes,
            session: token()?,
            ledger: Mutex::default(),
            provider: Mutex::default(),
            stop: CancellationToken::new(),
            scan: Arc::new(Semaphore::new(1)),
            starts: tokio::sync::Mutex::new(()),
            refresh_gate: tokio::sync::Mutex::new(()),
            readers: Arc::new(tokio::sync::RwLock::new(())),
            #[cfg(test)]
            checkpoint: Mutex::new(None),
        });
        s.refresh().await;
        Ok(s)
    }
    pub(crate) fn install(&self, p: Arc<dyn OpenFileDialog>) {
        *lock(&self.provider) = Some(p);
    }
    pub(crate) fn available(&self) -> bool {
        !self.stop.is_cancelled() && lock(&self.provider).is_some()
    }
    pub(crate) fn close(&self) {
        let mut l = lock(&self.ledger);
        l.closed = true;
        self.stop.cancel();
    }
    pub(crate) async fn drain(&self) {
        let tasks = std::mem::take(&mut lock(&self.ledger).tasks);
        for t in tasks {
            let _ = t.await;
        }
        // Cancelled list/start callers may leave read-only blocking I/O running.
        let _drained = self.readers.write().await;
    }
    pub(crate) fn invalidate(&self, account: Option<&str>, plugin: Option<&str>) {
        let l = lock(&self.ledger);
        for e in l.entries.values().filter(|e| {
            e.active.is_some()
                && account.is_none_or(|a| e.thread.account_id == a)
                && plugin.is_none_or(|p| e.plugin == p)
        }) {
            if let Some(c) = &e.active {
                c.cancel();
            }
        }
    }
    pub(crate) fn inventory_changed(&self) {
        let mut l = lock(&self.ledger);
        l.inventory_epoch = l.inventory_epoch.wrapping_add(1);
        l.next_refresh = None;
    }
    pub(crate) fn flush(&self) {
        let dirty = std::mem::take(&mut lock(&self.ledger).dirty);
        for id in dirty {
            events::notify(&self.changes, &id);
        }
    }
    // Core publishes the exact upload progress also as an OutboxChange::Item.
    pub(crate) fn progress_only(&self, i: &OutboxItem) -> bool {
        if i.kind != OutboxKind::SendFile {
            return false;
        }
        let mut l = lock(&self.ledger);
        if i.state != OutboxState::Sending {
            l.sending.remove(&i.client_id);
            return false;
        }
        let identity = (i.account_id.clone(), i.attempts, i.echo_message_id);
        if l.sending.get(&i.client_id) == Some(&identity) {
            l.dirty.insert(i.account_id.clone());
            return true;
        }
        if l.sending.len() < KEYS {
            l.sending.insert(i.client_id.clone(), identity);
        }
        false
    }
    pub(crate) fn forget_progress(&self, id: &str) {
        lock(&self.ledger).sending.remove(id);
    }
    async fn owner(&self, t: &Thread) -> Result<AccountRow> {
        if !valid_thread(t) {
            return Err(Failure::Invalid);
        }
        self.core
            .accounts()
            .get(&t.account_id)
            .await?
            .ok_or(Failure::Missing)
    }
    fn session(&self, s: &str) -> Result<()> {
        if s == self.session {
            Ok(())
        } else {
            Err(Failure::Conflict)
        }
    }
    fn sequence(l: &Ledger, id: &str, t: &Thread, owner: &AccountRow) -> Result<u64> {
        if !valid_id(id) {
            return Err(Failure::Invalid);
        }
        l.ids
            .get(id)
            .copied()
            .filter(|n| l.entries[n].owned(t, owner))
            .ok_or(Failure::Missing)
    }
    #[cfg(test)]
    fn at(&self, phase: &str) -> std::result::Result<(), fs::Error> {
        let hook = lock(&self.checkpoint).clone();
        if let Some(f) = hook { f(phase) } else { Ok(()) }
    }
    fn notify(&self, seq: u64) {
        if let Some(e) = lock(&self.ledger).entries.get(&seq) {
            events::notify(&self.changes, &e.thread.account_id);
        }
    }
    pub(crate) async fn list(
        self: &Arc<Self>,
        thread: Thread,
        key: Option<String>,
        cursor: Option<String>,
    ) -> Result<Value> {
        if key.as_deref().is_some_and(|k| !valid_key(k)) || (key.is_some() && cursor.is_some()) {
            return Err(Failure::Invalid);
        }
        let owner = self.owner(&thread).await?;
        self.refresh().await;
        let (generation, guard) = self.core.accounts().storage_lease(&owner).await?;
        let reader = self
            .readers
            .clone()
            .try_read_owned()
            .map_err(|_| Failure::Unavailable)?;
        if self.stop.is_cancelled() {
            return Err(Failure::Unavailable);
        }
        let s = self.clone();
        blocking(move || {
            let (_reader,_guard)=(reader,guard);
            let (selected,remaining)={
                let l=lock(&s.ledger);
                let before=if let Some(c)=cursor.as_deref() {
                    let (session,id)=c.split_once('.').ok_or(Failure::Invalid)?;
                    s.session(session)?;
                    Self::sequence(&l,id,&thread,&owner).map_err(|_|Failure::Invalid)?
                }else{u64::MAX};
                let rows: Vec<_>=if let Some(k)=key.as_deref() {
                    l.keys.get(k).and_then(|n|l.entries.get(n).filter(|e|e.owned(&thread,&owner)).map(|e|(*n,e.clone()))).into_iter().collect()
                }else{
                    l.entries.range(..before).rev().filter(|(_,e)|e.owned(&thread,&owner)).take(PAGE+1).map(|(&n,e)|(n,e.clone())).collect()
                };
                (rows,KEYS-l.keys.len())
            };
            let mut items=Vec::new();
            let mut last=None;
            let mut more=false;
            for (n,old) in selected {
                if items.len()==PAGE{more=true;break;}
                s.reconcile(n,&old,&owner);
                let l=lock(&s.ledger);
                let actual=l.entries.get(&n).ok_or(Failure::Missing)?;
                if !actual.owned(&thread,&owner){continue;}
                items.push(actual.render(&s.session));
                if serde_json::to_vec(&items).map_err(|_|Failure::Internal)?.len()>MAX_REPLY-256{
                    items.pop();more=true;break;
                }
                last=Some(format!("{}.{}",s.session,actual.stage));
            }
            if generation.is_cancelled(){return Err(Failure::Missing);}
            Ok(json!({"sessionId":s.session,"items":items,"nextCursor":if more{last}else{None},"remainingRequestKeys":remaining}))
        }).await
    }
}

#[cfg(test)]
mod tests;
