//! Bounded local text admissions owned by the application, not by an IPC caller.
use crate::{App, bridge::validate_spans, events, projection, wire::{Failure, Result, Span, Thread}};
use ch_core::{outbox::{Outbox, TextAdmissionError, TextAdmissionLifetime, TextAdmissionTarget}, runtime::Core};
use ch_db::Db;
use ch_protocol::{common::is_valid_id, dto::Entity, methods::MessagesSendParams};
use futures_util::FutureExt;
use serde_json::Value;
use std::{panic::AssertUnwindSafe, sync::{Arc, Mutex, MutexGuard}};
use tokio::{sync::{Semaphore, OwnedSemaphorePermit, broadcast, oneshot}, task::JoinHandle};
use tokio_util::sync::CancellationToken;

const JOBS: u32 = 4;
type Directory = crate::DirectoryLifetime;
struct Lifetime { _slot: OwnedSemaphorePermit, _directory: Directory }
#[derive(Default)]
struct State { closed: bool, jobs: Vec<JoinHandle<()>> }
#[cfg(test)]
type Checkpoint = dyn Fn(&str) + Send + Sync;

pub(crate) struct Service {
    core: Arc<Core>,
    db: Arc<Mutex<Db>>,
    outbox: Outbox,
    changes: broadcast::Sender<events::Invalidation>,
    directory: Directory,
    slots: Arc<Semaphore>,
    state: Mutex<State>,
    stop: CancellationToken,
    #[cfg(test)] checkpoint: Mutex<Option<Arc<Checkpoint>>>,
}
fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> { value.lock().unwrap_or_else(|e|e.into_inner()) }
impl Service {
    pub(crate) fn new(core: Arc<Core>, db: Arc<Mutex<Db>>, outbox: Outbox,
        changes: broadcast::Sender<events::Invalidation>, directory: Directory) -> Arc<Self>
    {
        Arc::new(Self { core, db, outbox, changes, directory, slots:Arc::new(Semaphore::new(JOBS as usize)),
            state:Mutex::default(), stop:CancellationToken::new(), #[cfg(test)] checkpoint:Mutex::default() })
    }
    pub(crate) fn close(&self) { let mut state=lock(&self.state); state.closed=true; self.stop.cancel(); }
    pub(crate) async fn drain(&self) {
        let jobs=std::mem::take(&mut lock(&self.state).jobs);
        for job in jobs { let _=job.await; }
        // A panicking async wrapper may have dropped its blocking JoinHandle.
        // The actual closure retains its permit and directory/owner guards.
        #[cfg(test)] self.at("draining");
        let _settled=self.slots.clone().acquire_many_owned(JOBS).await;
    }
    async fn read<T:Send+'static>(self:&Arc<Self>, life:Arc<Lifetime>, f:impl FnOnce(&Db)->Result<T>+Send+'static)->Result<T> {
        let db=self.db.clone();
        tokio::task::spawn_blocking(move|| { let _life=life; f(&lock(&db)) }).await.map_err(|_|Failure::Internal)?
    }
    pub(crate) async fn enqueue(self:&Arc<Self>, thread:Thread, text:String, spans:Vec<Span>)->Result<Value> {
        if !is_valid_id(&thread.account_id) || !is_valid_id(&thread.chat_id)
            || thread.topic_id.as_deref().is_some_and(|id|!is_valid_id(id)) || text.trim().is_empty()
        { return Err(Failure::Invalid); }
        let (tx,rx)=oneshot::channel();
        {
            let mut state=lock(&self.state);
            if state.closed { return Err(Failure::Unavailable); }
            let slot=self.slots.clone().try_acquire_owned().map_err(|_|Failure::Conflict)?;
            state.jobs.retain(|job|!job.is_finished());
            let life=Arc::new(Lifetime { _slot:slot, _directory:self.directory.clone() });
            let service=self.clone();
            state.jobs.push(tokio::spawn(async move {
                let account=thread.account_id.clone();
                let result=AssertUnwindSafe(service.work(thread,text,spans,life)).catch_unwind().await
                    .unwrap_or(Err(Failure::OutcomeUnknown));
                if result.is_ok() || matches!(result,Err(Failure::OutcomeUnknown)) { events::notify(&service.changes,&account); }
                let _=tx.send(result);
            }));
        }
        rx.await.map_err(|_|Failure::OutcomeUnknown)?
    }
    async fn work(self:&Arc<Self>, thread:Thread, text:String, spans:Vec<Span>, life:Arc<Lifetime>)->Result<Value> {
        if self.stop.is_cancelled() { return Err(Failure::Unavailable); }
        let (owner,owner_revoked,guard)=self.core.accounts().storage_lease_for(&thread.account_id).await?;
        let guard=Arc::new(guard);
        let lease=guard.clone(); let captured=owner.clone(); let scope=thread.clone();
        let target=self.read(life.clone(),move|db| {
            let _lease=lease;
            let conversation=ch_db::get_conversation(db.conn(),&scope.account_id,&scope.chat_id)?
                .filter(|row|!row.is_gone).ok_or(Failure::Missing)?;
            let topic=scope.topic_id.as_ref().map(|id|ch_db::get_topic(db.conn(),conversation.id,id))
                .transpose()?.flatten();
            if scope.topic_id.is_some() && topic.as_ref().is_none_or(|row|row.is_gone) { return Err(Failure::Missing); }
            if conversation.can_post==Some(false) || topic.as_ref().is_some_and(|row|row.closed) { return Err(Failure::Conflict); }
            Ok(TextAdmissionTarget { owner:captured, conversation, topic })
        }).await?;
        let entities=if spans.is_empty() { None } else {
            let state=self.core.host().plugin_state(&owner.plugin_id).ok_or(Failure::Unavailable)?;
            if !state.capabilities().ok_or(Failure::Unavailable)?.has("send.entities") { return Err(Failure::Unsupported); }
            let entities=spans.into_iter().map(Entity::from).collect::<Vec<_>>();
            validate_spans(&text,&entities)?; Some(entities)
        };
        let params=MessagesSendParams { account_id:thread.account_id, chat_id:thread.chat_id, topic_id:thread.topic_id,
            text, entities, ..Default::default() };
        let lifetime=TextAdmissionLifetime { owner_revoked, app_closed:self.stop.clone() };
        let service=self.clone();
        let sink=self.core.ingest().clone();
        let writer=tokio::spawn(async move {sink.with_metadata(move|| {
            let (_life,_lease)=(life,guard);
            #[cfg(test)] service.at("before-writer");
            let receipt=service.outbox.enqueue_text_checked(&target,params,&lifetime,None).map_err(|error|match error {
                TextAdmissionError::Rejected(error)=>Failure::from(error), TextAdmissionError::CommitUnknown=>Failure::OutcomeUnknown,
            })?;
            #[cfg(test)] service.at("committed");
            Ok(projection::outbox(receipt))
        }).await.map_err(Failure::from)?});
        #[cfg(test)] self.at("spawned");
        writer.await.map_err(|_|Failure::OutcomeUnknown)?
    }
    #[cfg(test)]
    fn at(&self, phase:&str) { let hook=lock(&self.checkpoint).clone(); if let Some(hook)=hook { hook(phase); } }
}

impl App {
    pub(crate) async fn enqueue_text(&self, thread:Thread, text:String, entities:Vec<Span>)->Result<Value> {
        self.text_admissions.enqueue(thread,text,entities).await
    }
}

#[cfg(test)]
#[path="text_admission_tests.rs"]
mod tests;
