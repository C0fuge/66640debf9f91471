//! Bounded, ephemeral attachment jobs. Paths never cross the application wire.
use crate::{App, cache_file::{CacheFile, CacheFileError, MAX_CACHE_FILE_BYTES}, events,
    save_file::{self, SaveFileError}, wire::{Failure, Result, Thread}};
use ch_core::{host::{AccountPhase, HostError}, ingest::{Applied, Change}, runtime::Core};
use ch_db::{Db, types::{AccountRow, MessageRow}};
use ch_protocol::{ErrorCode, common::is_valid_id, dto::Media, events::{Event, MediaReadyEvent, TransferDirection, TransferProgressEvent},
    jsonrpc::RequestId, methods::{MediaDownloadParams, MediaDownloadResult, MediaWhat}};
use serde::Serialize;
use serde_json::{Value, json};
use std::{collections::{BTreeMap, BTreeSet}, path::PathBuf, sync::{Arc, Mutex, OnceLock, Weak, atomic::{AtomicBool, AtomicU8, Ordering}}, time::{Duration, Instant}};
use tokio::{sync::{Semaphore, broadcast, oneshot}, task::JoinHandle};
use tokio_util::sync::CancellationToken;

/// A native chooser failure, without an OS error or filesystem location.
#[derive(Clone, Copy, Debug)]
pub enum SaveDialogError { Unavailable, Internal, Uncertain }
pub type SaveDialogCallback = Box<dyn FnOnce(std::result::Result<Option<PathBuf>, SaveDialogError>) + Send>;
/// Native shell injection only. Callback occurs when the chooser has actually exited.
/// Implementations must return promptly; a dropped/panicking callback fails closed.
pub trait SaveDialog: Send + Sync { fn choose(&self, suggested_name: String, callback: SaveDialogCallback); }

const MAX_KEYS: usize = 4096;
const MAX_ACTIVE: usize = 8;
const PAGE: usize = 16;
const CANCEL_AFTER: Duration = Duration::from_secs(20 * 60);
const PROGRESS_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all="camelCase")]
pub(crate) enum Kind { Download, Save }
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all="camelCase")]
enum State { Queued, Running, Choosing, Committing, Cancelling, Succeeded, Failed, Cancelled, Unknown }
impl State { fn terminal(self) -> bool { matches!(self,Self::Succeeded|Self::Failed|Self::Cancelled|Self::Unknown) } }
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all="camelCase")]
struct JobError { code: &'static str, reason: &'static str, outcome_unknown: bool }
impl JobError {
    fn new(code: &'static str, reason: &'static str) -> Self { Self { code, reason, outcome_unknown:false } }
    fn unknown() -> Self { Self { code:"unavailable", reason:"unavailable", outcome_unknown:true } }
}
#[derive(Clone, Serialize)]
#[serde(rename_all="camelCase")]
struct Snapshot {
    id:String, session_id:String, request_key:String, thread:Thread, message_id:String, kind:Kind,
    state:State, cancel_requested:bool, done_bytes:Option<String>, total_bytes:Option<String>, error:Option<JobError>,
}
struct Ledger {
    entries:BTreeMap<u64,Arc<Job>>, keys:BTreeMap<String,u64>, active:BTreeSet<u64>, native_busy:bool,
    progress_dirty:BTreeSet<String>, progress_last:BTreeMap<String,Instant>,
}
struct Target { owner:AccountRow, row:MessageRow, media:Media }
struct Job {
    seq:u64, plugin:String, conversation:i64, row:i64, fingerprint:Mutex<Option<Media>>,
    snapshot:Mutex<Snapshot>, cancel:CancellationToken, wire:Arc<OnceLock<i64>>,
    // 0=precommit, 1=cancel won, 2=commit won. No lock is held over filesystem IO.
    phase:AtomicU8, manager:Weak<MediaJobs>,
}
pub(crate) struct MediaJobs {
    session:String, core:Arc<Core>, db:Arc<Mutex<Db>>, changes:broadcast::Sender<events::Invalidation>,
    ledger:Mutex<Ledger>, downloads:Arc<Semaphore>, provider:Mutex<Option<Arc<dyn SaveDialog>>>,
    closed:Arc<AtomicBool>, stop:CancellationToken, tasks:Mutex<Vec<JoinHandle<()>>>,
    directory:crate::DirectoryLifetime,
}
fn fingerprint(mut media:Media)->Media { media.path=None; media.thumb_path=None; media }
fn valid_thread(thread:&Thread)->bool { is_valid_id(&thread.account_id) && is_valid_id(&thread.chat_id) && thread.topic_id.as_deref().is_none_or(is_valid_id) }
fn valid_key(key:&str)->bool { !key.is_empty() && key.len()<=64 && key.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'_'||b==b'-') }
fn lock<T>(value:&Mutex<T>)->std::sync::MutexGuard<'_,T> { value.lock().unwrap_or_else(|e|e.into_inner()) }

impl Job {
    fn snapshot(&self)->Snapshot { lock(&self.snapshot).clone() }
    fn notify(&self) { if let Some(manager)=self.manager.upgrade() { events::notify(&manager.changes,&self.snapshot().thread.account_id); } }
    fn cancel(&self) {
        { let mut s=lock(&self.snapshot); if s.state.terminal() { return; }
          s.cancel_requested=true; self.cancel.cancel();
          if self.phase.compare_exchange(0,1,Ordering::AcqRel,Ordering::Acquire).is_ok() { s.state=State::Cancelling; }
        }
        self.notify();
    }
    fn set_state(&self,state:State)->bool { let mut s=lock(&self.snapshot); if s.cancel_requested || s.state.terminal() {return false;} s.state=state; true }
    fn finish(&self,state:State,error:Option<JobError>,release:bool) {
        // Receipts retain only bounded wire metadata, never a full waveform or
        // remote filename. Revoke any late precommit worker before publishing.
        self.phase.compare_exchange(0,1,Ordering::AcqRel,Ordering::Acquire).ok();
        lock(&self.fingerprint).take();
        if let Some(manager)=self.manager.upgrade() {
            let account={
                let mut l=lock(&manager.ledger);
                let mut s=lock(&self.snapshot);
                if s.state.terminal() { return; }
                s.state=state; s.error=error;
                if release { l.active.remove(&self.seq); if s.kind==Kind::Save { l.native_busy=false; } }
                s.thread.account_id.clone()
            };
            events::notify(&manager.changes,&account);
        } else {
            let mut s=lock(&self.snapshot);
            if !s.state.terminal() {s.state=state;s.error=error;}
        }
    }
    fn begin_commit(&self,manager:&MediaJobs)->bool {
        if self.cancel.is_cancelled() || manager.recheck(self).is_err() { return false; }
        // Admission/close and this gate share the ledger lock; shutdown either
        // revokes permission first or observes a commit which already won.
        { let l=lock(&manager.ledger);
          if manager.closed.load(Ordering::Acquire) || self.cancel.is_cancelled() || !l.active.contains(&self.seq) { return false; }
          if self.phase.compare_exchange(0,2,Ordering::AcqRel,Ordering::Acquire).is_err() { return false; }
        }
        lock(&self.snapshot).state=State::Committing; self.notify(); true
    }
}

impl MediaJobs {
    pub(crate) fn new(core:Arc<Core>,db:Arc<Mutex<Db>>,changes:broadcast::Sender<events::Invalidation>,directory:crate::DirectoryLifetime)->std::io::Result<Arc<Self>> {
        let mut random=[0u8;16]; getrandom::fill(&mut random).map_err(|_|std::io::Error::other("Could not initialize media jobs."))?;
        Ok(Arc::new(Self { session:random.iter().map(|b|format!("{b:02x}")).collect(),core,db,changes,
            ledger:Mutex::new(Ledger { entries:BTreeMap::new(), keys:BTreeMap::new(), active:BTreeSet::new(), native_busy:false,progress_dirty:BTreeSet::new(),progress_last:BTreeMap::new() }),
            downloads:Arc::new(Semaphore::new(2)),provider:Mutex::new(None),closed:Arc::new(AtomicBool::new(false)),stop:CancellationToken::new(),tasks:Mutex::new(Vec::new()),directory }))
    }
    pub(crate) fn save_available(&self)->bool { lock(&self.provider).is_some() && !self.closed.load(Ordering::Acquire) }
    pub(crate) fn install(&self,provider:Arc<dyn SaveDialog>) { *lock(&self.provider)=Some(provider); }
    fn target(db:&Db,thread:&Thread,id:&str)->Result<Target> {
        let owner=ch_db::get_account(db.conn(),&thread.account_id)?.ok_or(Failure::Missing)?;
        if !owner.enabled { return Err(Failure::Unavailable); }
        let conv=ch_db::get_conversation(db.conn(),&thread.account_id,&thread.chat_id)?.filter(|c|!c.is_gone).ok_or(Failure::Missing)?;
        if let Some(topic)=&thread.topic_id { ch_db::get_topic(db.conn(),conv.id,topic)?.filter(|t|!t.is_gone).ok_or(Failure::Missing)?; }
        let row=ch_db::get_message(db.conn(),conv.id,id)?.ok_or(Failure::Missing)?;
        if row.topic_id!=thread.topic_id { return Err(Failure::Missing); }
        if row.deleted_at.is_some() || row.is_scheduled || id.starts_with("local:") || matches!(row.delivery.as_deref(),Some("sending"|"failed")) { return Err(Failure::Conflict); }
        let media=row.media.clone().ok_or(Failure::Conflict)?;
        if media.bytes.is_some_and(|b|b>MAX_CACHE_FILE_BYTES) { return Err(Failure::Conflict); }
        Ok(Target {owner,row,media})
    }
    fn checked(db:&Db,job:&Job)->Result<Target> {
        let s=job.snapshot(); let target=Self::target(db,&s.thread,&s.message_id)?;
        if target.owner.plugin_id!=job.plugin || target.row.id!=job.row || target.row.conversation_id!=job.conversation || lock(&job.fingerprint).as_ref()!=Some(&fingerprint(target.media.clone())) { return Err(Failure::Conflict); }
        Ok(target)
    }
    fn recheck(&self,job:&Job)->Result<Target> { Self::checked(&lock(&self.db),job) }
    fn live(&self,target:&Target)->Result<()> {
        if !self.core.host().account(&target.owner.plugin_id,&target.owner.account_id).is_some_and(|a|matches!(a.phase,AccountPhase::Running)) { return Err(Failure::Unavailable); }
        if !self.core.host().plugin_state(&target.owner.plugin_id).and_then(|p|p.capabilities().cloned()).is_some_and(|c|c.has("media.download")) { return Err(Failure::Unsupported); }
        Ok(())
    }
    pub(crate) fn close(&self) {
        let jobs={let l=lock(&self.ledger);self.closed.store(true,Ordering::Release);self.stop.cancel();l.active.iter().filter_map(|id|l.entries.get(id).cloned()).collect::<Vec<_>>()};
        for job in jobs { job.cancel(); }
        for task in lock(&self.tasks).drain(..) { task.abort(); }
    }
    fn active_jobs(&self)->Vec<Arc<Job>> { let l=lock(&self.ledger); l.active.iter().filter_map(|id|l.entries.get(id).cloned()).collect() }
    pub(crate) fn cancel_account(&self,id:&str) { for job in self.active_jobs() { if job.snapshot().thread.account_id==id { job.cancel(); } } }
    pub(crate) fn applied(&self,plugin:&str,applied:&Applied) {
        for change in &applied.changes { if let Change::TransferProgress(p)=change { self.progress(plugin,p); } }
    }
    fn progress(&self,plugin:&str,p:&TransferProgressEvent) {
        let Some(RequestId::Number(wire))=&p.request_id else { return; };
        if p.direction!=TransferDirection::Download { return; }
        for job in self.active_jobs() {
            if job.plugin!=plugin || job.wire.get()!=Some(wire) { continue; }
            let mut s=lock(&job.snapshot);
            if s.state.terminal() || s.kind!=Kind::Download || s.thread.account_id!=p.account_id || p.chat_id.as_ref().is_some_and(|id|id!=&s.thread.chat_id) || p.message_id.as_ref().is_some_and(|id|id!=&s.message_id) { continue; }
            let previous=s.done_bytes.as_deref().and_then(|n|n.parse::<u64>().ok());
            if previous.is_some_and(|old|p.done_bytes<old) { continue; }
            s.done_bytes=Some(p.done_bytes.to_string()); s.total_bytes=p.total_bytes.filter(|t|*t>=p.done_bytes).map(|v|v.to_string());
            let account=s.thread.account_id.clone(); drop(s);
            if p.done_bytes>MAX_CACHE_FILE_BYTES { job.cancel(); }
            lock(&self.ledger).progress_dirty.insert(account);
        }
    }
    pub(crate) fn flush_progress(&self) {
        let now=Instant::now(); let mut l=lock(&self.ledger);
        let ready:Vec<_>=l.progress_dirty.iter().filter(|a|l.progress_last.get(*a).is_none_or(|last|now.duration_since(*last)>=PROGRESS_INTERVAL)).cloned().collect();
        for account in ready { l.progress_dirty.remove(&account); l.progress_last.insert(account.clone(),now); events::notify(&self.changes,&account); }
    }
}

impl App {
    /// Setup-only native injection. Never exposed as an application operation.
    pub fn set_save_dialog(&mut self,provider:Arc<dyn SaveDialog>) { self.media.install(provider); }
    pub(crate) async fn start_media(&self,thread:Thread,message_id:String,session_id:String,request_key:String,kind:Kind)->Result<Value> {
        if !valid_thread(&thread)||!is_valid_id(&message_id)||!valid_key(&request_key)||session_id!=self.media.session { return Err(Failure::Invalid); }
        let owner=self.account(&thread.account_id).await?;
        { let l=lock(&self.media.ledger); if let Some(seq)=l.keys.get(&request_key) {
            let job=l.entries.get(seq).ok_or(Failure::Internal)?; let s=job.snapshot();
            if s.thread!=thread||s.message_id!=message_id||s.kind!=kind||job.plugin!=owner.plugin_id { return Err(Failure::Invalid); }
            return Ok(json!(s));
        } }
        let t=thread.clone(); let id=message_id.clone(); let target=self.read(move|db|MediaJobs::target(db,&t,&id)).await;
        let provider=lock(&self.media.provider).clone();
        let target=target.and_then(|target|{
            if kind==Kind::Download {self.media.live(&target)?;}else if provider.is_none(){return Err(Failure::Unsupported);}
            Ok(target)
        });
        let job={ let mut l=lock(&self.media.ledger);
            // A duplicate may have arrived during the asynchronous DB read.
            if let Some(seq)=l.keys.get(&request_key) { let j=l.entries.get(seq).ok_or(Failure::Internal)?; let s=j.snapshot(); if s.thread!=thread||s.message_id!=message_id||s.kind!=kind||j.plugin!=owner.plugin_id { return Err(Failure::Invalid); } return Ok(json!(s)); }
            let target=target?;
            if self.media.closed.load(Ordering::Acquire) { return Err(Failure::Unavailable); }
            if l.keys.len()>=MAX_KEYS || l.active.len()>=MAX_ACTIVE || kind==Kind::Save&&l.native_busy { return Err(Failure::Conflict); }
            if l.active.iter().filter_map(|id|l.entries.get(id)).any(|j|{let s=j.snapshot();s.thread==thread&&s.message_id==message_id&&s.kind==kind}) { return Err(Failure::Conflict); }
            let seq=l.entries.len() as u64+1; let snapshot=Snapshot {id:format!("{}-{seq}",self.media.session),session_id:session_id.clone(),request_key:request_key.clone(),thread,message_id,kind,state:State::Queued,cancel_requested:false,done_bytes:None,total_bytes:None,error:None};
            let job=Arc::new(Job {seq,plugin:target.owner.plugin_id,conversation:target.row.conversation_id,row:target.row.id,fingerprint:Mutex::new(Some(fingerprint(target.media))),snapshot:Mutex::new(snapshot),cancel:self.media.stop.child_token(),wire:Arc::new(OnceLock::new()),phase:AtomicU8::new(0),manager:Arc::downgrade(&self.media)});
            l.keys.insert(request_key,seq); l.entries.insert(seq,job.clone()); l.active.insert(seq); if kind==Kind::Save { l.native_busy=true; } job
        };
        let answer=json!(job.snapshot()); job.notify();
        let context=Worker { core:self.core.clone(),db:self.db.clone(),closed:self.media.closed.clone(),downloads:self.media.downloads.clone(),provider,directory:self.media.directory.clone() };
        let worker_job=job.clone();
        let task=tokio::spawn(async move {
            use futures_util::FutureExt;
            let result=std::panic::AssertUnwindSafe(context.run(worker_job.clone())).catch_unwind().await;
            if result.is_err() { worker_job.finish(State::Unknown,Some(JobError::unknown()),false); }
        });
        let mut tasks=lock(&self.media.tasks); tasks.retain(|t|!t.is_finished()); tasks.push(task);
        Ok(answer)
    }
    pub(crate) async fn list_media(&self,thread:Thread,cursor:Option<String>,request_key:Option<String>)->Result<Value> {
        if !valid_thread(&thread)||cursor.is_some()&&request_key.is_some()||request_key.as_deref().is_some_and(|k|!valid_key(k)) { return Err(Failure::Invalid); }
        let owner=self.account(&thread.account_id).await?; let l=lock(&self.media.ledger);
        let mut bound=u64::MAX;
        if let Some(cursor)=cursor { if !is_valid_id(&cursor) {return Err(Failure::Invalid);} let seq=cursor.strip_prefix(&format!("{}-",self.media.session)).and_then(|v|v.parse::<u64>().ok()).ok_or(Failure::Invalid)?; let job=l.entries.get(&seq).ok_or(Failure::Invalid)?;
            if job.snapshot().thread!=thread||job.snapshot().id!=cursor||job.plugin!=owner.plugin_id { return Err(Failure::Invalid); } bound=seq;
        }
        let jobs:Vec<_>=if let Some(key)=request_key {
            match l.keys.get(&key).and_then(|seq|l.entries.get(seq)) { Some(j)=>{ let s=j.snapshot(); if s.thread!=thread||j.plugin!=owner.plugin_id { return Err(Failure::Missing); } vec![s] },None=>vec![] }
        } else { l.entries.range(..bound).rev().filter(|(_,j)|j.plugin==owner.plugin_id).map(|(_,j)|j.snapshot()).filter(|s|s.thread==thread).take(PAGE+1).collect() };
        let next=if jobs.len()>PAGE {Some(jobs[PAGE-1].id.clone())}else{None};
        Ok(json!({"sessionId":self.media.session,"jobs":jobs.into_iter().take(PAGE).collect::<Vec<_>>(),"nextCursor":next,"remainingRequestKeys":MAX_KEYS-l.keys.len()}))
    }
    pub(crate) async fn cancel_media(&self,thread:Thread,session:String,id:String)->Result<Value> {
        if !valid_thread(&thread)||!is_valid_id(&id)||session!=self.media.session { return Err(Failure::Invalid); }
        let owner=self.account(&thread.account_id).await?;
        let job={let l=lock(&self.media.ledger);l.entries.values().find(|j|j.snapshot().id==id).cloned().ok_or(Failure::Missing)?};
        if job.snapshot().thread!=thread||job.plugin!=owner.plugin_id { return Err(Failure::Missing); } job.cancel(); Ok(json!(job.snapshot()))
    }
}

struct Worker { core:Arc<Core>, db:Arc<Mutex<Db>>, closed:Arc<AtomicBool>,downloads:Arc<Semaphore>,provider:Option<Arc<dyn SaveDialog>>,directory:crate::DirectoryLifetime }

// Retain negative accounting through every actual cache descriptor, including
// the save chooser and an owned blocking copy after its async waiter is lost.
// The scope is last so the file and its anchored directories close first. It
// provides no pathname, historical-reader closure, or deletion permission.
struct CacheRead {
    file: CacheFile,
    #[cfg(target_os="linux")]
    _read_scope: ch_core::managed_stage::StageReadScope,
    // Also retain the real process lock (and isolated demo root) after shutdown
    // aborts an async waiter while its blocking filesystem work still runs.
    _directory:crate::DirectoryLifetime,
}
impl CacheRead {
    fn open(core:&Core, directory:crate::DirectoryLifetime, root:&std::path::Path, path:&std::path::Path)
        -> std::result::Result<Self,CacheFileError> {
        #[cfg(target_os="linux")]
        let read_scope=core.stage_mutations().read_scope().map_err(|_|CacheFileError::Cancelled)?;
        #[cfg(not(target_os="linux"))]
        let _=core;
        let file=CacheFile::open(root,path)?;
        Ok(Self {file,#[cfg(target_os="linux")] _read_scope:read_scope,_directory:directory})
    }
}
impl Worker {
    async fn check(&self,job:&Arc<Job>)->Result<Target> { let db=self.db.clone();let j=job.clone();let closed=self.closed.clone();crate::projection::blocking(move||{if closed.load(Ordering::Acquire){return Err(Failure::Unavailable);}MediaJobs::checked(&lock(&db),&j)}).await }
    async fn run(self,job:Arc<Job>) {
        let kind=job.snapshot().kind;
        match kind {Kind::Download=>self.download(job).await,Kind::Save=>self.save(job).await}
    }
    async fn download(self,job:Arc<Job>) {
        let permit=tokio::select!{_ = job.cancel.cancelled()=>{job.finish(State::Cancelled,None,true);return;},p=self.downloads.clone().acquire_owned()=>p};
        let Ok(_permit)=permit else { job.finish(State::Failed,Some(JobError::new("unavailable","unavailable")),true);return; };
        if job.cancel.is_cancelled() {job.finish(State::Cancelled,None,true);return;}
        let target=match self.check(&job).await {Ok(t)=>t,Err(_)=>{job.finish(State::Failed,Some(JobError::new("conflict","changed")),true);return;}};
        if let Some(manager)=job.manager.upgrade() { if manager.live(&target).is_err(){job.finish(State::Failed,Some(JobError::new("unavailable","unavailable")),true);return;} }
        else { return; }
        let s=job.snapshot();let params=MediaDownloadParams{account_id:s.thread.account_id.clone(),chat_id:s.thread.chat_id.clone(),message_id:s.message_id.clone(),what:MediaWhat::Full,priority:None};
        let Ok(params)=serde_json::to_value(params) else {job.finish(State::Failed,Some(JobError::new("internal","internal")),true);return;};
        if !job.set_state(State::Running){job.finish(State::Cancelled,None,true);return;}job.notify();
        let call=self.core.host().call_raw_tracked(&job.plugin,"media.download",params,Some(job.cancel.clone()),Some(job.wire.clone()));
        tokio::pin!(call);
        let raw=tokio::select!{result=&mut call=>result,_=tokio::time::sleep(CANCEL_AFTER)=>{job.cancel();(&mut call).await}};
        let raw=match raw {Ok(v)=>v,Err(error)=>{let cancelled=matches!(&error,HostError::Rpc(e) if e.code()==ErrorCode::Cancelled);let definite=matches!(&error,HostError::Rpc(e) if matches!(e.code(),ErrorCode::NotFound|ErrorCode::NotSupported|ErrorCode::Forbidden|ErrorCode::AuthRequired|ErrorCode::InvalidParams));
            job.finish(if cancelled{State::Cancelled}else if definite{State::Failed}else{State::Unknown},if cancelled{None}else if definite{Some(JobError::new("unavailable","unavailable"))}else{Some(JobError::unknown())},true);return;}};
        let result=match serde_json::from_value::<MediaDownloadResult>(raw){Ok(r)=>r,Err(_)=>{job.finish(State::Unknown,Some(JobError::unknown()),true);return;}};
        let root=self.core.host().config().cache_dir(&s.thread.account_id);let path=PathBuf::from(&result.path);
        let core=self.core.clone();
        let directory=self.directory.clone();
        let checked=tokio::task::spawn_blocking(move||CacheRead::open(&core,directory,&root,&path)).await;
        let file=match checked {Ok(Ok(f))=>f,Ok(Err(e))=>{job.finish(State::Failed,Some(cache_error(e)),true);return;},Err(_)=>{job.finish(State::Unknown,Some(JobError::unknown()),true);return;}};
        let bytes=file.file.metadata().byte_len;
        if job.snapshot().done_bytes.as_deref().and_then(|v|v.parse::<u64>().ok()).is_some_and(|done|done>bytes) {
            job.finish(State::Failed,Some(JobError::new("conflict","changed")),true);return;
        }
        let j=job.clone();let event=Event::MediaReady(MediaReadyEvent{account_id:s.thread.account_id,chat_id:s.thread.chat_id,message_id:s.message_id,path:Some(result.path),thumb_path:None});
        let applied=self.core.ingest().with_ingest(move|ing|->Result<_>{MediaJobs::checked(ing.db(),&j)?;ing.apply(&j.plugin,event).map_err(|_|Failure::Internal)}).await;
        drop(file);
        match applied {Ok(Ok(applied))=>{self.core.bus().publish_local("media-jobs",applied);let mut snapshot=lock(&job.snapshot);snapshot.done_bytes=Some(bytes.to_string());snapshot.total_bytes=Some(bytes.to_string());drop(snapshot);job.finish(State::Succeeded,None,true);},_=>job.finish(State::Unknown,Some(JobError::unknown()),true)}
    }
    async fn save(self,job:Arc<Job>) {
        if job.cancel.is_cancelled(){job.finish(State::Cancelled,None,true);return;}
        let target=match self.check(&job).await {Ok(t)=>t,Err(_)=>{job.finish(State::Failed,Some(JobError::new("conflict","changed")),true);return;}};
        let Some(path)=target.media.path else {job.finish(State::Failed,Some(JobError::new("not_found","notCached")),true);return;};
        let name=suggested_name(target.media.filename.as_deref());
        let root=self.core.host().config().cache_dir(&target.owner.account_id);
        let core=self.core.clone();
        let directory=self.directory.clone();
        let opened=tokio::task::spawn_blocking(move||CacheRead::open(&core,directory,&root,&PathBuf::from(path))).await;
        let source=match opened {Ok(Ok(f))=>f,Ok(Err(e))=>{job.finish(State::Failed,Some(cache_error(e)),true);return;},Err(_)=>{job.finish(State::Failed,Some(JobError::new("internal","internal")),true);return;}};
        if job.cancel.is_cancelled() {job.finish(State::Cancelled,None,true);return;}
        let Some(provider)=self.provider else {job.finish(State::Failed,Some(JobError::new("unsupported","unavailable")),true);return;};
        if !job.set_state(State::Choosing){job.finish(State::Cancelled,None,true);return;}job.notify();
        let (tx,mut rx)=oneshot::channel();
        provider.choose(name,Box::new(move|result|{let _=tx.send(result);}));
        // Cancellation cannot dismiss the OS chooser. Keep its reservation until
        // the actual callback; release the held source immediately on cancellation.
        let answer=tokio::select!{result=&mut rx=>result,_=job.cancel.cancelled()=>{drop(source);let result=rx.await;match result {Err(_)|Ok(Err(SaveDialogError::Uncertain))=>job.finish(State::Unknown,Some(JobError::unknown()),false),_=>job.finish(State::Cancelled,None,true)};return;}};
        let answer=match answer {
            Err(_)|Ok(Err(SaveDialogError::Uncertain))=>{job.finish(State::Unknown,Some(JobError::unknown()),false);return;},
            Ok(answer)=>answer,
        };
        if job.cancel.is_cancelled()||self.closed.load(Ordering::Acquire){job.finish(State::Cancelled,None,true);return;}
        let destination=match answer {Ok(Some(p))=>p,Ok(None)=>{job.finish(State::Cancelled,None,true);return;},Err(_)=>{job.finish(State::Failed,Some(JobError::new("unavailable","unavailable")),true);return;}};
        if !job.set_state(State::Running){job.finish(State::Cancelled,None,true);return;}job.notify();
        let j=job.clone();let closed=self.closed;let weak=job.manager.clone();
        #[cfg(target_os="linux")]
        let mutations=self.core.stage_mutations();
        let copied=tokio::task::spawn_blocking(move||{
            // Capture the entire wrapper, not just its file field: precise
            // closure capture must not leave its scope in the async waiter.
            let mut source=source;
            // A native destination may be anywhere, including the configured
            // accounts tree. Invalidate before the first destination probe and
            // retain through actual destination handles closing in publish().
            #[cfg(target_os="linux")]
            let _mutation=mutations.begin().map_err(|_|SaveFileError::Unavailable)?;
            let cancel_job=j.clone();let progress_job=j.clone();let commit_job=j.clone();
            save_file::publish(&mut source.file,&destination,MAX_CACHE_FILE_BYTES,
                move||cancel_job.cancel.is_cancelled()||closed.load(Ordering::Acquire),
                move|done,total|{let mut s=lock(&progress_job.snapshot);s.done_bytes=Some(done.to_string());s.total_bytes=Some(total.to_string());let account=s.thread.account_id.clone();drop(s);if let Some(m)=progress_job.manager.upgrade(){lock(&m.ledger).progress_dirty.insert(account);}},
                move||{let Some(manager)=weak.upgrade()else{return false;};commit_job.begin_commit(&manager)})
        }).await;
        match copied {
            Ok(Ok(_))=>job.finish(State::Succeeded,None,true),
            Ok(Err(SaveFileError::Cancelled))=>{if job.cancel.is_cancelled(){job.finish(State::Cancelled,None,true);}else{job.finish(State::Failed,Some(JobError::new("conflict","changed")),true);}},
            Ok(Err(SaveFileError::OutcomeUnknown))|Err(_)=>job.finish(State::Unknown,Some(JobError::unknown()),true),
            Ok(Err(error))=>job.finish(State::Failed,Some(match error {
                SaveFileError::Conflict=>JobError::new("conflict","destinationExists"),SaveFileError::SourceChanged=>JobError::new("conflict","changed"),SaveFileError::TooLarge=>JobError::new("conflict","tooLarge"),SaveFileError::Unavailable=>JobError::new("unsupported","unsupportedDestination"),_=>JobError::new("internal","internal")}),true),
        }
    }
}
fn cache_error(error:CacheFileError)->JobError { match error {CacheFileError::TooLarge=>JobError::new("conflict","tooLarge"),CacheFileError::Changed=>JobError::new("conflict","changed"),CacheFileError::Unsupported=>JobError::new("unsupported","unsafeCache"),CacheFileError::Cancelled=>JobError::new("unavailable","unavailable"),_=>JobError::new("not_found","unsafeCache")} }
fn suggested_name(name:Option<&str>)->String {
    let clean:String=name.unwrap_or("").chars().filter(|c|!c.is_control()&&!matches!(*c,'/'|'\\'|'\u{202a}'..='\u{202e}'|'\u{2066}'..='\u{2069}')).take(100).collect();
    let clean=clean.trim_matches(|c:char|c=='.'||c.is_whitespace());if clean.is_empty(){"attachment".into()}else{clean.into()}
}

#[cfg(test)]
#[path="media_tests.rs"]
mod tests;
