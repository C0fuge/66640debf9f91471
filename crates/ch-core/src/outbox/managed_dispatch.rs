//! One native managed send. No generic transport fallback or automatic replay.
use super::*;
use crate::{host::NativeProducer,managed_sources::SourceInput,
    runtime::{ManagedProofWriter,ManagedProofTransaction,NativeProofTicket,LogicalOwnerObservation}};
use ch_protocol::{dto::{Direction,MediaKind},methods::MessageResult};
use sha2::{Digest,Sha256};

// This is created only after the actual FULL exposed commit. It is non-Clone
// and derives the entire wire request from the retained canonical source intent.
pub(crate) struct ManagedPermit { grant:WireGrant,completion:Completion }
pub(crate) struct WireGrant {
    input:SourceInput,producer:NativeProducer,params:serde_json::Value,stopped:CancellationToken,
}
impl ManagedPermit {
    pub(crate) fn producer(&self)->&NativeProducer {&self.grant.producer}
    pub(crate) fn split(self)->(WireGrant,Completion) {(self.grant,self.completion)}
}
impl WireGrant {
    pub(crate) fn params(&self)->&serde_json::Value {&self.params}
    pub(crate) fn publish(self,publish:impl FnOnce(&NativeProducer)->bool)->bool {
        if self.stopped.is_cancelled() || !self.producer.live() {return false;}
        self.input.publish(||!self.stopped.is_cancelled() && self.producer.live() && publish(&self.producer))
            .unwrap_or(false)
    }
}

#[derive(Clone)]
struct Attempt {
    id:i64,nonce:String,source:i64,fingerprint:String,row:OutboxRow,
    owner:AccountRow,producer_nonce:String,
    origin:LogicalOwnerObservation,
}
#[derive(Clone)]
struct Publisher { changes:broadcast::Sender<OutboxChange>,bus:Option<ChangeBus>,wake:Option<Arc<Notify>> }
impl Publisher {
    fn for_row(inner:&Inner,row:&OutboxRow)->Self {
        Self {changes:inner.changes.clone(),bus:inner.bus.clone(),wake:inner.workers.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner).get(&row.account_id).map(|worker|worker.notify.clone())}
    }
    fn publish(&self,change:OutboxChange) {
        if let (Some(bus),OutboxChange::Ingested(applied))=(&self.bus,&change) {bus.publish_local("outbox",applied.clone());}
        let _=self.changes.send(change);
        if let Some(wake)=&self.wake {wake.notify_one();}
    }
}

pub(super) fn local_failure(inner:&Inner,row:OutboxRow,source:i64,ticket:NativeProofTicket) {
    let Some(context)=&inner.managed else {return;};
    let publisher=Publisher::for_row(inner,&row);
    let receipt=context.sources.proofs().submit_native(ticket,move|scope|->Result<Vec<OutboxChange>> {
        let tx=scope.transaction();
        let Some(current)=ch_db::get_outbox(tx,row.id)? else {return Ok(vec![]);};
        if current.params!=row.params || current.client_id!=row.client_id || current.account_id!=row.account_id
            || !matches!(current.state.as_str(),"queued"|"sending") {return Ok(vec![]);}
        let source_exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM managed_source WHERE id=?1
            AND client_id=?2 AND account_id=?3)",rusqlite::params![source,row.client_id,row.account_id],|r|r.get(0))
            .map_err(ch_db::Error::from)?;
        if !source_exists {return Ok(vec![]);}
        let error=serde_json::to_string(&Inner::unknown_outcome_error("managed dispatch could not establish a native result"))?;
        tx.execute("UPDATE outbox SET state='failed',last_error_json=?2,updated_at=?3 WHERE id=?1",
            rusqlite::params![row.id,error,now_millis()]).map_err(ch_db::Error::from)?;
        let current=ch_db::get_outbox(tx,row.id)?.ok_or(OutboxError::AdmissionConflict)?;
        Ok(vec![OutboxChange::Item(OutboxItem::from_row(&current))])
    });
    if let Ok(receipt)=receipt {tokio::spawn(async move {
        let result=receipt.await;
                #[cfg(test)]
                if !matches!(&result,Ok(Ok(Ok(_)))) {eprintln!("native managed proof did not commit: {result:?}");}
                if let Ok(Ok(Ok(changes)))=result {for change in changes {publisher.publish(change);}}
    });}
}

// Held by the actual native Pending/late-response observer, independently of
// RPC callers and Outbox task cancellation. Contains no Core/Outbox/FD/lease.
pub(crate) struct Completion { proofs:ManagedProofWriter,attempt:Attempt,publisher:Publisher,ticket:NativeProofTicket }
impl Completion {
    pub(crate) fn timeout(&self) {
        Self {proofs:self.proofs.clone(),attempt:self.attempt.clone(),publisher:self.publisher.clone(),ticket:self.ticket.clone()}
            .failure("unknown","native file reply timed out");
    }
    pub(crate) fn no_wire(self) {self.failure("no_wire","native frame was definitely not queued");}
    pub(crate) fn observe(self,result:std::result::Result<serde_json::Value,ch_protocol::ErrorObject>) {
        let Ok(value)=result else {self.failure("unknown","native file request failed; source readers remain unknown");return;};
        let digest=match serde_json::to_vec(&value) {
            Ok(bytes) if bytes.len()<=65536=>format!("{:x}",Sha256::digest(bytes)),
            Ok(_)=>{self.failure("unknown","native file response exceeds the bounded settlement budget");return;},
            Err(_)=>return,
        };
        let message=match validated_message(&self.attempt,&value) {
            Ok(message)=>message,
            Err(_)=>{self.failure("unknown","native file response did not match its canonical request");return;}
        };
        let attempt=self.attempt;let publisher=self.publisher;
        let receipt=self.proofs.submit_native(self.ticket,move|scope|settle(scope,&attempt,&message,&digest));
        if let Ok(receipt)=receipt {
            // DB work is ALREADY admitted and counted before this future exists.
            tokio::spawn(async move {
                let result=receipt.await;
                #[cfg(test)]
                if !matches!(&result,Ok(Ok(Ok(_)))) {eprintln!("native managed proof did not commit: {result:?}");}
                if let Ok(Ok(Ok(changes)))=result {for change in changes {publisher.publish(change);}}
            });
        }
    }
    fn failure(self,state:&'static str,reason:&'static str) {
        let attempt=self.attempt;let publisher=self.publisher;
        let receipt=self.proofs.submit_native(self.ticket,move|scope|->Result<Vec<OutboxChange>> {
            let tx=scope.transaction();
            let Some(row)=matching_attempt(tx,&attempt)? else {return Ok(vec![]);};
            tx.execute("UPDATE managed_dispatch SET state=?2,updated_at=?3 WHERE id=?1 AND state='exposed'",
                rusqlite::params![attempt.id,state,now_millis()]).map_err(ch_db::Error::from)?;
            if row.state==outbox_state::SENDING {
                let error=serde_json::to_string(&if state=="no_wire" {ErrorObject::new(ErrorCode::Internal,reason)}
                    else {Inner::unknown_outcome_error(reason)})?;
                tx.execute("UPDATE outbox SET last_error_json=?2,updated_at=?3,
                    state='failed' WHERE id=?1",
                    rusqlite::params![row.id,error,now_millis()]).map_err(ch_db::Error::from)?;
                let row=ch_db::get_outbox(tx,row.id)?.ok_or(OutboxError::AdmissionConflict)?;
                let mut changes=Vec::new();
                if let Some(delivery)=Inner::set_echo_delivery_in(tx,&row,"failed")? {changes.push(OutboxChange::Ingested(delivery));}
                changes.push(OutboxChange::Item(OutboxItem::from_row(&row)));
                return Ok(changes);
            }
            Ok(vec![])
        });
        if let Ok(receipt)=receipt {tokio::spawn(async move {
            let result=receipt.await;
                #[cfg(test)]
                if !matches!(&result,Ok(Ok(Ok(_)))) {eprintln!("native managed proof did not commit: {result:?}");}
                if let Ok(Ok(Ok(changes)))=result {for change in changes {publisher.publish(change);}}
        });}
    }
}

fn validated_message(attempt:&Attempt,value:&serde_json::Value)->Result<ch_protocol::dto::Message> {
    let result:MessageResult=serde_json::from_value(value.clone())?;
    let mut message=result.message;
    let expected=match attempt.row.params.get("kind").and_then(serde_json::Value::as_str) {
        Some("photo")=>MediaKind::Photo,Some("document")=>MediaKind::Document,
        _=>return Err(OutboxError::AdmissionConflict),
    };
    if message.id.is_empty() || message.id.len()>4096 || message.id.starts_with("local:") || message.direction!=Direction::Out
        || message.chat_id!=attempt.row.chat_id || message.topic_id!=attempt.row.topic_id
        || message.media.as_ref().is_none_or(|media|media.kind!=expected)
    {return Err(OutboxError::AdmissionConflict);}
    // Settlement is DB-only, including after a storage generation was revoked.
    // Do not call the normal filesystem-backed output filter on new cache paths.
    // Existing cache knowledge is preserved by the ordinary sparse media merge.
    if let Some(media)=&mut message.media {media.path=None;media.thumb_path=None;}
    if let Some(preview)=&mut message.link_preview {
        preview.thumb_path=None;
        if let Some(media)=&mut preview.media {media.path=None;media.thumb_path=None;}
    }
    Ok(message)
}

fn settle(scope:&ManagedProofTransaction<'_,'_>,attempt:&Attempt,message:&ch_protocol::dto::Message,
    digest:&str)->Result<Vec<OutboxChange>> {
    let tx=scope.transaction();
    let Some(row)=matching_attempt(tx,attempt)? else {return Ok(vec![]);};
    let ids=vec![message.id.clone()];
    let state:String=tx.query_row("SELECT state FROM managed_dispatch WHERE id=?1",[attempt.id],|r|r.get(0))
        .map_err(ch_db::Error::from)?;
    if !matches!(state.as_str(),"exposed"|"unknown"|"positive") {return Ok(vec![]);}
    if state=="positive" {
        let (stored_ids,stored_digest):(Option<String>,Option<String>)=tx.query_row(
            "SELECT result_ids_json,response_digest FROM managed_dispatch WHERE id=?1",[attempt.id],|r|Ok((r.get(0)?,r.get(1)?)))
            .map_err(ch_db::Error::from)?;
        if stored_ids.as_deref()!=Some(serde_json::to_string(&ids)?.as_str()) || stored_digest.as_deref()!=Some(digest) {
            return Err(OutboxError::AdmissionConflict);
        }
        return Ok(vec![]);
    }
    let mut changes=Vec::new();
    // Handle already-adopted exact Sent BEFORE the ordinary Sending-only guard.
    if row.state==outbox_state::SENT {
        if row.result_message_ids.as_ref()!=Some(&ids) {return Ok(vec![]);}
    } else if row.state==outbox_state::SENDING || (row.state==outbox_state::FAILED
        && row.last_error.as_ref().is_some_and(|error|error["data"]["reason"]=="outcome_unknown")) {
        let Some(echo_id)=row.echo_message_id else {return Ok(vec![]);};
        let Some(mut echo)=ch_db::get_message_by_id(tx,echo_id)? else {return Ok(vec![]);};
        let Some(conversation)=ch_db::get_conversation_by_id(tx,echo.conversation_id)? else {return Ok(vec![]);};
        if conversation.account_id!=row.account_id || conversation.chat_id!=row.chat_id
            || echo.topic_id!=row.topic_id || conversation.is_gone {return Ok(vec![]);}
        let mut applied=Applied {account_id:row.account_id.clone(),event:"local.send_file".into(),..Default::default()};
        if echo.message_id==format!("local:{}",row.client_id) {
            if ch_db::get_message(tx,conversation.id,&message.id)?.is_some() {
                scope.ingest().delete_messages(tx,&row.account_id,Some(&row.chat_id),&[echo.message_id],
                    "me",false,now_millis(),&mut applied)?;
            } else {
                echo.message_id.clone_from(&message.id);ch_db::update_message(tx,&echo)?;
            }
        } else if echo.message_id!=message.id {return Ok(vec![]);}
        scope.ingest().upsert_messages(tx,&attempt.owner.plugin_id,&row.account_id,std::slice::from_ref(message),
            now_millis(),&mut applied)?;
        let remote=ch_db::get_message(tx,conversation.id,&message.id)?.ok_or(OutboxError::AdmissionConflict)?;
        ch_db::set_outbox_echo(tx,row.id,Some(remote.id))?;
        ch_db::set_outbox_sent(tx,row.id,&ids)?;
        let updated=ch_db::get_outbox(tx,row.id)?.ok_or(OutboxError::AdmissionConflict)?;
        changes.push(OutboxChange::Ingested(applied));
        if let Some(delivery)=Inner::set_echo_delivery_in(tx,&updated,"sent")? {changes.push(OutboxChange::Ingested(delivery));}
        changes.push(OutboxChange::Item(OutboxItem::from_row(&updated)));
    } else {return Ok(vec![]);}
    tx.execute("UPDATE managed_dispatch SET state='positive',result_ids_json=?2,response_digest=?3,
        updated_at=?4 WHERE id=?1",rusqlite::params![attempt.id,serde_json::to_string(&ids)?,digest,now_millis()])
        .map_err(ch_db::Error::from)?;
    tx.execute("UPDATE managed_source SET settled_dispatch_id=?2,updated_at=?3 WHERE id=?1",
        rusqlite::params![attempt.source,attempt.id,now_millis()]).map_err(ch_db::Error::from)?;
    if attempt.origin.is_revoked() {return Err(OutboxError::AdmissionConflict);}
    Ok(changes)
}

fn matching_attempt(conn:&rusqlite::Connection,attempt:&Attempt)->Result<Option<OutboxRow>> {
    if attempt.origin.is_revoked() {return Ok(None);}
    let Some(row)=ch_db::get_outbox(conn,attempt.row.id)? else {return Ok(None);};
    if row.account_id!=attempt.row.account_id || row.client_id!=attempt.row.client_id
        || row.kind!="send_file" || row.method!="messages.send_file" || row.params!=attempt.row.params
        || row.chat_id!=attempt.row.chat_id || row.topic_id!=attempt.row.topic_id {return Ok(None);}
    let matched:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM managed_dispatch d
        JOIN managed_source s ON s.id=d.source_id JOIN managed_producer p ON p.id=d.producer_id
        JOIN managed_outbox_dispatch current ON current.dispatch_id=d.id
        JOIN account a ON a.account_id=s.account_id
        WHERE d.id=?1 AND d.nonce=?2 AND d.source_id=?3 AND d.outbox_id=?4 AND d.client_id=?5
        AND d.method='messages.send_file' AND d.params_json=?6 AND d.source_fingerprint=?7
        AND d.source_release='on_success' AND p.nonce=?8 AND current.outbox_id=?4
        AND s.fingerprint_json=?7 AND s.intent_json=?6 AND s.client_id=?5
        AND s.account_id=?9 AND s.plugin_id=?10 AND s.account_created_at=?11
        AND a.plugin_id=?10 AND a.created_at=?11)",
        rusqlite::params![attempt.id,attempt.nonce,attempt.source,row.id,row.client_id,
            serde_json::to_string(&row.params)?,attempt.fingerprint,attempt.producer_nonce,
            attempt.owner.account_id,attempt.owner.plugin_id,attempt.owner.created_at],|r|r.get(0))
        .map_err(ch_db::Error::from)?;
    Ok(matched.then_some(row))
}

pub(super) async fn dispatch(inner:Arc<Inner>,row:OutboxRow,input:SourceInput,
    ticket:NativeProofTicket,cancel:CancellationToken,request_id:Arc<OnceLock<i64>>) -> Result<()> {
    let context=inner.managed.clone().ok_or(OutboxError::ManagedUnavailable)?;
    let identity=input.identity().clone();let observation=input.owner().clone();
    let owner=observation.owner().clone();
    let canonical=identity.request().map_err(|_|OutboxError::AdmissionConflict)?
        .params(&owner.account_id,&identity.manifest().client_id);
    if row.params!=canonical || row.account_id!=owner.account_id || row.client_id!=identity.manifest().client_id
        || row.kind!="send_file" || row.method!="messages.send_file" || row.state!=outbox_state::QUEUED {
        return Err(OutboxError::AdmissionConflict);
    }
    let Ok(producer)=context.host.source_producer(&owner.plugin_id,&owner.account_id).await else {
        // Unsupported unregistered migration baseline never falls through to raw
        // transport. Its selected source stays parked, not a retry loop.
        return Ok(());
    };
    if producer.account()!=owner.account_id || producer.plugin()!=owner.plugin_id || !producer.live() {
        return Err(OutboxError::AdmissionConflict);
    }
    let scope=context.host.file_admission_scope(&owner.plugin_id,&owner.account_id,&canonical,identity.manifest().size)
        .map_err(|_|OutboxError::ManagedUnavailable)?;
    let issuer=context.accounts.host_storage_view().map_err(|_|OutboxError::ManagedUnavailable)?;
    let expected=identity.clone();let original=observation.clone();
    let stopped=context.sources.stopped();let stopped_work=stopped.clone();let native=producer.clone();
    let runtime=tokio::runtime::Handle::current();
    context.sources.lookups().run(move||->Result<()> {
        let binding=runtime.block_on(issuer.reacquire(&original,&stopped_work)).map_err(|_|OutboxError::ManagedUnavailable)?;
        crate::managed_stage::VerifiedStageBinding::check_retained(&binding,&expected,|_| {
            if stopped_work.is_cancelled() || !native.live() {Err(crate::managed_stage::Error::Cancelled)} else {Ok(())}
        }).map_err(|_|OutboxError::ManagedUnavailable)
    }).await.map_err(|_|OutboxError::ManagedUnavailable)??;
    // All actual byte descriptors, flocks and storage read guards are closed now.
    let core_nonce=context.sources.core_nonce().ok_or(OutboxError::ManagedUnavailable)?.to_owned();
    let fingerprint=identity.to_persisted_json().map_err(|_|OutboxError::AdmissionConflict)?;
    let source=input.id();let original=observation;let native=producer.clone();let stop=stopped.clone();
    let expected=row.clone();
    let attempt=context.sources.proofs().with_transaction(move|full|->Result<Attempt> {
        let tx=full.transaction();
        let current=ch_db::get_outbox(tx,expected.id)?.ok_or(OutboxError::AdmissionConflict)?;
        if current.client_id!=expected.client_id || current.params!=canonical || current.kind!="send_file"
            || current.method!="messages.send_file" || current.state!=outbox_state::QUEUED
            || current.chat_id!=expected.chat_id || current.topic_id!=expected.topic_id
            || current.account_id!=owner.account_id || stop.is_cancelled() || !native.live()
        {return Err(OutboxError::AdmissionConflict);}
        original.check(tx).map_err(|_|OutboxError::AdmissionConflict)?;
        scope.check().map_err(|_|OutboxError::ManagedUnavailable)?;
        let current_owner=ch_db::get_account(tx,&owner.account_id)?.ok_or(OutboxError::AdmissionConflict)?;
        if !current_owner.enabled {return Err(OutboxError::ManagedUnavailable);}
        let echo=current.echo_message_id.and_then(|id|ch_db::get_message_by_id(tx,id).ok().flatten())
            .ok_or(OutboxError::AdmissionConflict)?;
        let target=ch_db::get_conversation_by_id(tx,echo.conversation_id)?.ok_or(OutboxError::AdmissionConflict)?;
        if echo.message_id!=format!("local:{}",current.client_id) || echo.topic_id!=current.topic_id
            || target.account_id!=owner.account_id || target.chat_id!=current.chat_id || target.is_gone
            || target.can_post==Some(false) {return Err(OutboxError::AdmissionConflict);}
        if let Some(topic)=&current.topic_id
            && !ch_db::get_topic(tx,target.id,topic)?.is_some_and(|topic|!topic.closed && !topic.is_gone)
        {return Err(OutboxError::AdmissionConflict);}
        let intent=serde_json::to_string(&canonical)?;
        let stored:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM managed_source WHERE id=?1
            AND stage_id=?2 AND client_id=?3 AND account_id=?4 AND plugin_id=?5 AND account_created_at=?6
            AND fingerprint_json=?7 AND intent_json=?8 AND path=?9 AND state='retained')",
            rusqlite::params![source,identity.manifest().stage_id,current.client_id,owner.account_id,owner.plugin_id,
                owner.created_at,fingerprint,intent,identity.path().to_str()],|r|r.get(0)).map_err(ch_db::Error::from)?;
        if !stored {return Err(OutboxError::AdmissionConflict);}
        let previous:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM managed_dispatch WHERE source_id=?1)",
            [source],|r|r.get(0)).map_err(ch_db::Error::from)?;
        if previous {return Err(OutboxError::ManagedUnavailable);}
        tx.execute("INSERT INTO managed_producer(nonce,core_nonce,platform,native_pid)
            VALUES(?1,?2,?3,?4) ON CONFLICT(nonce) DO NOTHING",
            rusqlite::params![native.nonce(),core_nonce,std::env::consts::OS,native.native_pid()]).map_err(ch_db::Error::from)?;
        let producer:i64=tx.query_row("SELECT id FROM managed_producer WHERE nonce=?1 AND core_nonce=?2
            AND native_pid=?3 AND platform=?4 AND state='unresolved'",
            rusqlite::params![native.nonce(),core_nonce,native.native_pid(),std::env::consts::OS],|r|r.get(0))
            .map_err(ch_db::Error::from)?;
        let nonce:String=tx.query_row("SELECT lower(hex(randomblob(16)))",[],|r|r.get(0)).map_err(ch_db::Error::from)?;
        let now=now_millis();
        tx.execute("INSERT INTO managed_dispatch(nonce,source_id,producer_id,outbox_id,client_id,method,
            params_json,source_fingerprint,source_release,state,created_at,updated_at)
            VALUES(?1,?2,?3,?4,?5,'messages.send_file',?6,?7,'on_success','exposed',?8,?8)",
            rusqlite::params![nonce,source,producer,current.id,current.client_id,intent,fingerprint,now]).map_err(ch_db::Error::from)?;
        let id=tx.last_insert_rowid();
        tx.execute("INSERT INTO managed_outbox_dispatch(outbox_id,dispatch_id) VALUES(?1,?2)",
            rusqlite::params![current.id,id]).map_err(ch_db::Error::from)?;
        tx.execute("UPDATE outbox SET state='sending',attempts=attempts+1,last_error_json=NULL,
            next_attempt_at=NULL,updated_at=?2 WHERE id=?1",rusqlite::params![current.id,now]).map_err(ch_db::Error::from)?;
        // Final original lifetime/owner check, still before FULL commit.
        original.check(tx).map_err(|_|OutboxError::AdmissionConflict)?;
        scope.check().map_err(|_|OutboxError::ManagedUnavailable)?;
        if !ch_db::get_account(tx,&owner.account_id)?.is_some_and(|owner|owner.enabled)
            || !ch_db::get_conversation_by_id(tx,target.id)?.is_some_and(|latest|
                latest.created_at==target.created_at && latest.account_id==target.account_id
                && latest.chat_id==target.chat_id && latest.kind==target.kind && !latest.is_gone && latest.can_post!=Some(false))
        {return Err(OutboxError::AdmissionConflict);}
        if let Some(topic)=&current.topic_id
            && !ch_db::get_topic(tx,target.id,topic)?.is_some_and(|topic|!topic.closed && !topic.is_gone)
        {return Err(OutboxError::AdmissionConflict);}
        if stop.is_cancelled() || !native.live() {return Err(OutboxError::ManagedUnavailable);}
        Ok(Attempt {id,nonce,source,fingerprint,row:current,owner,producer_nonce:native.nonce().into(),origin:native.origin()})
    }).await.map_err(|_|OutboxError::ManagedUnavailable)??;
    let publisher=Publisher::for_row(&inner,&row);
    let permit=ManagedPermit {grant:WireGrant {input,producer,params:row.params,stopped},
        completion:Completion {proofs:context.sources.proofs().clone(),attempt,ticket,
            publisher}};
    #[cfg(test)]
    {
        let checkpoint=inner.managed_checkpoint.lock().unwrap().clone();
        if let Some(checkpoint)=checkpoint {
            tokio::task::spawn_blocking(move||checkpoint("native-before-frame")).await
                .map_err(|_|OutboxError::ManagedUnavailable)?;
        }
    }
    // The native Pending owns completion even when this waiter/task is dropped.
    let _=context.host.call_managed(permit,cancel,request_id).await;
    Ok(())
}
