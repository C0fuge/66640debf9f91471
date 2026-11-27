//! Per-account scheduler. The worker owns every in-flight task, including reconciliation.
use std::collections::HashSet;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ch_db::types::{OutboxRow, outbox_state};
use ch_protocol::dto::Message;
use ch_protocol::methods::{MessageResult, MessagesResult};
use ch_protocol::{ErrorCode, ErrorObject};
use tokio::sync::{Notify, broadcast};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::{Inflight, Inner, OutboxChange, OutboxKind, Result, Verdict, classify, is_online};
use crate::host::{HostError, StateChange};
use crate::ingest::{Applied, Ingest};

pub(super) async fn run(
    inner: Arc<Inner>,
    account_id: String,
    plugin_id: String,
    notify: Arc<Notify>,
    stop: CancellationToken,
) {
    let mut states = inner.transport.subscribe();
    let mut tasks = JoinSet::new();
    let mut state_open = true;
    loop {
        if stop.is_cancelled() {
            break;
        }
        let (result,next_tasks)=pass_owned(inner.clone(),account_id.clone(),plugin_id.clone(),tasks).await;
        tasks=next_tasks;
        let due = match result {
            Ok(due) => due,
            Err(e) => {
                warn!(error = %e, "outbox scheduling failed");
                Some(inner.now().saturating_add(5_000))
            }
        };
        #[cfg(test)]
        {
            let checkpoint=inner.managed_checkpoint.lock().unwrap().clone();
            if let Some(checkpoint)=checkpoint {checkpoint("worker-idle");}
        }
        let wait = async {
            if let Some(at) = due {
                tokio::time::sleep(Duration::from_millis(
                    at.saturating_sub(inner.now()).max(0) as u64
                ))
                .await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            _ = notify.notified() => {},
            change = states.recv(), if state_open => match change {
                Ok(StateChange::Account(a)) if a.account_id == account_id && a.plugin_id == plugin_id && is_online(Some(&a)) => inner.clear_paused(&account_id),
                Err(broadcast::error::RecvError::Lagged(_)) if is_online(inner.transport.account(&plugin_id,&account_id).as_ref()) => inner.clear_paused(&account_id),
                Err(broadcast::error::RecvError::Closed) => state_open = false,
                _ => {}
            },
            _ = tasks.join_next(), if !tasks.is_empty() => {},
            _ = wait => {},
        }
    }
    tasks.shutdown().await;
}

// Dropping an aborted task must release its slot too.
struct Slot(Arc<Inner>, String);
impl Drop for Slot {
    fn drop(&mut self) {
        self.0
            .inflight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.1);
    }
}

fn pass(
    inner: &Arc<Inner>,
    account: &str,
    plugin: &str,
    tasks: &mut JoinSet<()>,
) -> Result<Option<i64>> {
    let g = inner.ingest();
    let conn = g.db().conn();
    if !ch_db::get_account(conn, account)?.is_some_and(|a| a.plugin_id == plugin) {
        return Ok(None);
    }
    let mut rows = ch_db::list_outbox(conn, account, Some(outbox_state::QUEUED))?;
    rows.extend(ch_db::list_outbox(
        conn,
        account,
        Some(outbox_state::SENDING),
    )?);
    rows.sort_by_key(|r| (r.state != outbox_state::SENDING, r.id));
    let mut wake: Option<i64> = None;
    let online =
        is_online(inner.transport.account(plugin, account).as_ref()) && !inner.is_paused(account);
    let mut active = inner.inflight.lock().unwrap_or_else(|e| e.into_inner());
    let live: HashSet<&str> = rows.iter().map(|r| r.client_id.as_str()).collect();
    let mut lanes: HashSet<String> = active
        .iter()
        .filter(|(cid, v)| {
            live.contains(cid.as_str()) && v.plugin_id == plugin && v.account_id == account
        })
        .map(|(_, v)| v.chat_id.clone())
        .collect();
    let mut count = active
        .iter()
        .filter(|(cid, v)| {
            live.contains(cid.as_str())
                && v.plugin_id == plugin
                && v.account_id == account
                && !v.reconciling
        })
        .count();
    drop(live);
    for row in rows {
        if !lanes.insert(row.chat_id.clone()) || active.contains_key(&row.client_id) {
            continue;
        }
        // Only the actual live Core source and native producer can dispatch a
        // managed row. Missing live provenance or a plain Outbox::new transport
        // keeps the lane parked; generic send/reconciliation remains barred.
        if super::managed_row(conn, &row)? {
            #[cfg(target_os="linux")]
            if row.state==outbox_state::QUEUED && online && count<inner.policy.max_parallel_per_account.max(1)
                && let Some(context)=&inner.managed {
                let ticket=match context.sources.proofs().reserve_native() {
                    Ok(ticket)=>ticket,
                    Err(_)=>{wake=Some(wake.map_or(inner.now()+250,|w|w.min(inner.now()+250)));continue;},
                };
                let Ok(input)=context.sources.owner_barriers().take_dispatch(&row.client_id) else {continue;};
                let cancel=CancellationToken::new();let request_id=Arc::new(OnceLock::new());
                active.insert(row.client_id.clone(),Inflight {row_id:row.id,plugin_id:plugin.into(),request_id:request_id.clone(),
                    account_id:account.into(),chat_id:row.chat_id.clone(),kind:super::OutboxKind::SendFile,cancel:cancel.clone(),reconciling:false});
                count+=1;
                let slot=Slot(inner.clone(),row.client_id.clone());
                tasks.spawn(async move {
                    let failed=row.clone();let source=input.id();let failure_ticket=ticket.clone();
                    if let Err(error)=super::managed_dispatch::dispatch(slot.0.clone(),row,input,ticket,cancel,request_id).await {
                        warn!(error=%error,"managed source dispatch stayed local or unresolved");
                        super::managed_dispatch::local_failure(&slot.0,failed,source,failure_ticket);
                    }
                    drop(slot);
                });
            }
            continue;
        }
        let Some(kind) = OutboxKind::parse(&row.kind)
            .filter(|k| k.method() == row.method && row.params["account_id"] == row.account_id)
        else {
            let error = ErrorObject::new(ErrorCode::InvalidParams, "invalid stored outbox request");
            fail(inner, &g, &row, &error)?;
            // A newly unblocked lane needs another pass.
            wake = Some(inner.now());
            continue;
        };
        let recovery = row.state == outbox_state::SENDING;
        if !recovery {
            if !online || count >= inner.policy.max_parallel_per_account.max(1) {
                continue;
            }
            if let Some(t) = row.next_attempt_at.filter(|&at| at > inner.now()) {
                wake = Some(wake.map_or(t, |w| w.min(t)));
                continue;
            }
            // An echo received after a failure/retry can arrive before dispatch.
            if let Some(mid) = Inner::adopted_id(&g, &row)? {
                confirm(inner, &g, &row, &mid)?;
                wake = Some(inner.now());
                continue;
            }
            let claimed = conn
                .execute(
                    "UPDATE outbox SET state='sending', attempts=attempts+1, request_id=NULL,
                done_bytes=NULL, total_bytes=NULL, next_attempt_at=NULL, last_error_json=NULL,
                updated_at=?2 WHERE id=?1 AND state='queued' AND client_id=?3
                AND EXISTS (SELECT 1 FROM account a WHERE a.account_id=outbox.account_id AND a.plugin_id=?4)",
                    rusqlite::params![row.id, inner.now(),row.client_id,plugin],
                )
                .map_err(ch_db::Error::from)?;
            if claimed != 1 {
                continue;
            }
            inner.publish(OutboxChange::Item(Inner::item_in(&g, row.id)?));
            count += 1;
        }
        let cancel = CancellationToken::new();
        let request_id = Arc::new(OnceLock::new());
        active.insert(
            row.client_id.clone(),
            Inflight {
                row_id: row.id,
                plugin_id: plugin.to_owned(),
                request_id: request_id.clone(),
                account_id: account.to_owned(),
                chat_id: row.chat_id.clone(),
                kind,
                cancel: cancel.clone(),
                reconciling: recovery,
            },
        );
        let slot = Slot(Arc::clone(inner), row.client_id.clone());
        let plugin = plugin.to_owned();
        tasks.spawn(async move {
            let inner = &slot.0;
            if !matches!(owned_attempt(&inner.ingest(), &row, &plugin), Ok(Some(_))) {
                return;
            }
            let result = if recovery {
                if kind.is_idempotent() {
                    let recovery=row.clone();
                    metadata_work(inner.clone(),move|owner| {
                        let g=owner.ingest();requeue(owner,&g,&recovery,None,None)
                    }).await
                } else {
                    reconcile(
                        inner,
                        &row,
                        kind,
                        &plugin,
                        "the previous send was interrupted",
                    )
                    .await
                }
            } else {
                let res = inner
                    .transport
                    .call(
                        &plugin,
                        kind.method(),
                        row.params.clone(),
                        cancel.clone(),
                        request_id,
                    )
                    .await;
                match finish_owned(inner.clone(), plugin.clone(), row.clone(), kind, res, cancel.clone()).await {
                    Ok(Some(reason)) => {
                        if let Some(f) = inner
                            .inflight
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .get_mut(&row.client_id)
                        {
                            f.reconciling = true;
                        }
                        inner.wake(&row.account_id);
                        reconcile(inner, &row, kind, &plugin, reason).await
                    }
                    other => other.map(|_| ()),
                }
            };
            if let Err(e) = result {
                warn!(id=row.id, error=%e, "outbox result could not be persisted");
            }
            drop(slot);
        });
    }
    Ok(wake)
}

fn owned_attempt(g: &Ingest, row: &OutboxRow, plugin: &str) -> Result<Option<OutboxRow>> {
    let conn = g.db().conn();
    if super::managed_row(conn, row)? { return Ok(None); }
    if !ch_db::get_account(conn, &row.account_id)?.is_some_and(|a| a.plugin_id == plugin) {
        return Ok(None);
    }
    Ok(ch_db::get_outbox(conn, row.id)?.filter(|r| {
        r.account_id == row.account_id
            && r.client_id == row.client_id
            && r.state == outbox_state::SENDING
    }))
}


// Core-bound metadata persistence belongs to the service once admitted. The
// actual blocking callback holds the queue only for finite local DB work.
pub(super) async fn metadata_work<T,F>(inner:Arc<Inner>,work:F)->Result<T>
where T:Send+'static,F:FnOnce(&Arc<Inner>)->Result<T>+Send+'static {
    #[cfg(target_os="linux")]
    if let Some(metadata)=&inner.metadata {
        let owner=inner.clone();let queue=metadata.queue.clone();
        return metadata.jobs.run_wait_until(&inner.stopping,move|| {
            let _scope=queue.read();work(&owner)
        }).await.map_err(|_|super::OutboxError::Stopped)?;
    }
    work(&inner)
}

async fn pass_owned(inner:Arc<Inner>,account:String,plugin:String,tasks:JoinSet<()>)
    ->(Result<Option<i64>>,JoinSet<()>) {
    // Admission failure must return the original running task set so the actual
    // scheduler can await shutdown. Dropping a rejected callback is not a drain.
    let tasks=Arc::new(std::sync::Mutex::new(Some(tasks)));let owned=tasks.clone();
    let result=metadata_work(inner,move|inner| {
        // A queued scheduling pass may be revoked; an already accepted result
        // write must still finish. Never share this veto with finish_owned.
        if inner.stopping.is_cancelled() {return Err(super::OutboxError::Stopped);}
        let mut tasks=owned.lock().unwrap_or_else(|error|error.into_inner());
        pass(inner,&account,&plugin,tasks.as_mut().expect("owned scheduler task set"))
    }).await;
    let tasks=tasks.lock().unwrap_or_else(|error|error.into_inner()).take().expect("returned scheduler task set");
    (result,tasks)
}

async fn finish_owned(
    inner:Arc<Inner>,plugin:String,row:OutboxRow,kind:OutboxKind,
    res:std::result::Result<serde_json::Value,HostError>,cancel:CancellationToken,
)->Result<Option<&'static str>> {
    metadata_work(inner,move|owner|finish(owner,&plugin,&row,kind,res,&cancel)).await
}

fn finish(
    inner: &Inner,
    plugin: &str,
    row: &OutboxRow,
    kind: OutboxKind,
    res: std::result::Result<serde_json::Value, HostError>,
    cancel: &CancellationToken,
) -> Result<Option<&'static str>> {
    let g = inner.ingest();
    let Some(current) = owned_attempt(&g, row, plugin)? else {
        return Ok(None);
    };
    match res {
        Ok(v) => {
            let messages = match kind {
                OutboxKind::SendText | OutboxKind::SendFile | OutboxKind::Edit => {
                    serde_json::from_value::<MessageResult>(v)
                        .ok()
                        .map(|r| vec![r.message])
                }
                OutboxKind::SendAlbum | OutboxKind::Forward => {
                    serde_json::from_value::<MessagesResult>(v)
                        .ok()
                        .map(|r| r.messages)
                        .filter(|m| !m.is_empty())
                }
                OutboxKind::Delete => v.is_object().then(Vec::new),
            };
            let Some(messages) = messages else {
                return Ok(Some("the plugin returned an invalid result"));
            };
            for message in &messages {
                if message.direction != ch_protocol::dto::Direction::Out
                    || !crate::ingest::send_destination_matches(g.db().conn(), row, message)?
                    || (kind == OutboxKind::Edit && row.params["message_id"] != message.id)
                {
                    return Ok(Some(
                        "the plugin returned a message outside the request destination",
                    ));
                }
            }
            apply_success(inner, &g, plugin, row, kind, &messages)?;
        }
        Err(e) => {
            if let Some(mid) = Inner::adopted_id(&g, row)? {
                confirm(inner, &g, row, &mid)?;
                return Ok(None);
            }
            let cancelled = cancel.is_cancelled();
            // Once cancelled, even an idempotent operation must not automatically run again.
            let verdict = classify(
                &inner.policy,
                if cancelled {
                    OutboxKind::SendText
                } else {
                    kind
                },
                current.attempts.try_into().unwrap_or(u32::MAX),
                &e,
            );
            let err = match e {
                HostError::Rpc(o) => *o,
                _ => ErrorObject::new(ErrorCode::Internal, "host could not complete request"),
            };
            if cancelled && verdict != Verdict::Unknown {
                inner.finish_cancelled(&g, row)?;
                return Ok(None);
            }
            match verdict {
                Verdict::RetryAfter(d) => {
                    requeue(inner, &g, row, Some(&err), Some(after(inner, d)))?
                }
                Verdict::PauseUntilOnline => {
                    inner.set_paused(&row.account_id);
                    requeue(
                        inner,
                        &g,
                        row,
                        Some(&err),
                        Some(after(inner, inner.policy.pause_min)),
                    )?;
                }
                Verdict::Permanent => fail(inner, &g, row, &err)?,
                Verdict::Unknown => {
                    g.db()
                        .conn()
                        .execute(
                            "UPDATE outbox SET last_error_json=?2, updated_at=?3 WHERE id=?1 AND client_id=?4",
                            rusqlite::params![row.id, serde_json::to_string(&err)?, inner.now(),row.client_id],
                        )
                        .map_err(ch_db::Error::from)?;
                    inner.publish(OutboxChange::Item(Inner::item_in(&g, row.id)?));
                    return Ok(Some("the plugin could not confirm delivery"));
                }
            }
        }
    }
    Ok(None)
}

fn after(inner: &Inner, d: Duration) -> i64 {
    inner
        .now()
        .saturating_add(d.as_millis().try_into().unwrap_or(i64::MAX))
}

fn requeue(
    inner: &Inner,
    g: &Ingest,
    row: &OutboxRow,
    err: Option<&ErrorObject>,
    next: Option<i64>,
) -> Result<()> {
    let tx = g.db().transaction()?;
    if !Inner::same_item(g, row)? {
        return Ok(());
    }
    let e = err.map(serde_json::to_value).transpose()?;
    ch_db::set_outbox_state(
        g.db().conn(),
        row.id,
        outbox_state::QUEUED,
        e.as_ref(),
        next,
        None,
    )?;
    let item = Inner::item_in(g, row.id)?;
    tx.commit().map_err(ch_db::Error::from)?;
    inner.publish(OutboxChange::Item(item));
    Ok(())
}

fn fail(inner: &Inner, g: &Ingest, row: &OutboxRow, err: &ErrorObject) -> Result<()> {
    let tx = g.db().transaction()?;
    if !Inner::same_item(g, row)? {
        return Ok(());
    }
    ch_db::set_outbox_state(
        &tx,
        row.id,
        outbox_state::FAILED,
        Some(&serde_json::to_value(err)?),
        None,
        None,
    )?;
    let applied = Inner::set_echo_delivery(g, row, "failed")?;
    let item = Inner::item_in(g, row.id)?;
    tx.commit().map_err(ch_db::Error::from)?;
    if let Some(a) = applied {
        inner.publish(OutboxChange::Ingested(a));
    }
    inner.publish(OutboxChange::Item(item));
    Ok(())
}

pub(super) fn confirm(inner: &Inner, g: &Ingest, row: &OutboxRow, mid: &str) -> Result<()> {
    let tx = g.db().transaction()?;
    if !Inner::same_item(g, row)? {
        return Ok(());
    }
    ch_db::set_outbox_sent(&tx, row.id, &[mid.to_owned()])?;
    let applied = Inner::set_echo_delivery(g, row, "sent")?;
    let item = Inner::item_in(g, row.id)?;
    tx.commit().map_err(ch_db::Error::from)?;
    if let Some(a) = applied {
        inner.publish(OutboxChange::Ingested(a));
    }
    inner.publish(OutboxChange::Item(item));
    Ok(())
}

fn apply_success(
    inner: &Inner,
    g: &Ingest,
    plugin: &str,
    row: &OutboxRow,
    kind: OutboxKind,
    messages: &[Message],
) -> Result<()> {
    let tx = g.db().transaction()?;
    if owned_attempt(g, row, plugin)?.is_none() {
        return Ok(());
    }
    let mut applied = Applied {
        account_id: row.account_id.clone(),
        event: format!("local.{}", kind.as_str()),
        ..Default::default()
    };
    let ids = if kind == OutboxKind::Delete {
        let ids: Vec<String> = serde_json::from_value(row.params["message_ids"].clone())?;
        g.delete_messages(
            &tx,
            &row.account_id,
            Some(&row.chat_id),
            &ids,
            "me",
            false,
            inner.now(),
            &mut applied,
        )?;
        ids
    } else {
        if let (Some(echo_id), Some(first)) = (row.echo_message_id, messages.first())
            && let Some(mut echo) = ch_db::get_message_by_id(&tx, echo_id)?
            && echo.message_id == format!("local:{}", row.client_id)
        {
            let existing = if let Some(conv) =
                ch_db::conversation_rowid(&tx, &row.account_id, &first.chat_id)?
            {
                ch_db::get_message(&tx, conv, &first.id)?
            } else {
                None
            };
            if existing.is_some() || row.chat_id != first.chat_id {
                // A platform event can precede the reply without carrying our client id.
                g.delete_messages(
                    &tx,
                    &row.account_id,
                    Some(&row.chat_id),
                    &[echo.message_id],
                    "me",
                    false,
                    inner.now(),
                    &mut applied,
                )?;
            } else {
                echo.message_id.clone_from(&first.id);
                ch_db::update_message(&tx, &echo)?;
            }
        }
        g.upsert_messages(
            &tx,
            plugin,
            &row.account_id,
            messages,
            inner.now(),
            &mut applied,
        )?;
        if kind.has_echo()
            && let Some(first) = messages.first()
            && let Some(conv) = ch_db::conversation_rowid(&tx, &row.account_id, &first.chat_id)?
            && let Some(m) = ch_db::get_message(&tx, conv, &first.id)?
        {
            ch_db::set_outbox_echo(&tx, row.id, Some(m.id))?;
        }
        messages.iter().map(|m| m.id.clone()).collect()
    };
    ch_db::set_outbox_sent(&tx, row.id, &ids)?;
    let updated = Inner::row_in(g, row.id)?;
    let delivery = Inner::set_echo_delivery(g, &updated, "sent")?;
    let item = super::OutboxItem::from_row(&updated);
    tx.commit().map_err(ch_db::Error::from)?;
    inner.publish(OutboxChange::Ingested(applied));
    if let Some(a) = delivery {
        inner.publish(OutboxChange::Ingested(a));
    }
    inner.publish(OutboxChange::Item(item));
    Ok(())
}

async fn reconcile(
    inner: &Arc<Inner>,
    row: &OutboxRow,
    kind: OutboxKind,
    plugin: &str,
    reason: &str,
) -> Result<()> {
    let mut since = None;
    loop {
        let online = is_online(inner.transport.account(plugin, &row.account_id).as_ref());
        let expired = if online {
            since
                .get_or_insert_with(tokio::time::Instant::now)
                .elapsed()
                >= inner.policy.reconcile_timeout
        } else {
            since = None;
            false
        };
        let observed=row.clone();let plugin=plugin.to_owned();let reason=reason.to_owned();
        let finished=metadata_work(inner.clone(),move|owner| {
            let g=owner.ingest();
            if owned_attempt(&g,&observed,&plugin)?.is_none() {return Ok(true);}
            if let Some(mid)=Inner::adopted_id(&g,&observed)? {
                confirm(owner,&g,&observed,&mid)?;return Ok(true);
            }
            // Only text has a client_id field in the v1 wire contract.
            if kind!=OutboxKind::SendText || expired {
                fail(owner,&g,&observed,&Inner::unknown_outcome_error(&reason))?;return Ok(true);
            }
            Ok(false)
        }).await?;
        if finished {return Ok(());}
        tokio::time::sleep(inner.policy.reconcile_poll.max(Duration::from_millis(1))).await;
    }
}

#[cfg(all(test,target_os="linux"))]
mod result_queue_tests {
    use super::*;
    use crate::{host::MemorySecretStore,outbox::{Outbox,OutboxPolicy,OutboxState},runtime::{Core,CoreConfig}};
    use ch_db::{OutboxRow,PluginRow};
    use serde_json::json;

    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn accepted_actual_result_write_survives_lost_waiter_and_interrupted_outbox_drain() {
        let temp=tempfile::tempdir().unwrap();
        let mut config=CoreConfig::new(temp.path(),Arc::new(MemorySecretStore::new()));
        config.autostart=false;
        let core=Core::open(config).await.unwrap();
        let outbox=Outbox::for_core(&core,OutboxPolicy::default());
        let row={
            let ingest=outbox.inner.ingest();let conn=ingest.db().conn();
            ch_db::upsert_plugin(conn,&PluginRow {
                plugin_id:"fixture".into(),name:"Synthetic result persistence".into(),version:"0.0.1".into(),
                api_version:"1.2.0".into(),enabled:true,installed:true,install_dir:None,brand_color:None,
                icon_path:None,manifest_json:None,first_seen_at:1,last_seen_at:1
            }).unwrap();
            ch_db::create_account(conn,"a","fixture",None,&Default::default()).unwrap();
            let id=ch_db::insert_outbox(conn,&OutboxRow {
                account_id:"a".into(),chat_id:"chat".into(),client_id:"synthetic-result".into(),
                kind:"send_text".into(),method:"messages.send".into(),created_at:1,
                params:json!({"account_id":"a","chat_id":"chat","text":"synthetic body"}),..Default::default()
            }).unwrap();
            ch_db::set_outbox_state(conn,id,outbox_state::SENDING,None,None,None).unwrap();
            ch_db::get_outbox(conn,id).unwrap().unwrap()
        };
        let applied={
            let ingest=outbox.inner.ingest();
            let event=ch_protocol::Event::from_params("messages.upsert",json!({"account_id":"a","messages":[{
                "id":"existing","chat_id":"chat","direction":"in","sent_at":"2026-01-01T00:00:00Z","text":"synthetic stored event"
            }]})).unwrap().unwrap();
            ingest.apply("fixture",event).unwrap()
        };
        let queue=core.ingest().reference_queue();let guard=queue.exclusive_for_test().await;
        // This is the real result of the committed ordinary ingest above, not a
        // caller-provided completion/reader/filesystem certificate.
        core.bus().publish(crate::runtime::CoreChange::Ingested {plugin_id:"fixture".into(),applied:Arc::new(applied)});
        let scheduler={let inner=outbox.inner.clone();tokio::spawn(async move {
            let mut tasks=JoinSet::new();tasks.spawn(std::future::pending::<()>());
            pass_owned(inner,"a".into(),"fixture".into(),tasks).await
        })};
        let caller={let inner=outbox.inner.clone();let row=row.clone();tokio::spawn(async move {
            finish_owned(inner,"fixture".into(),row,OutboxKind::SendText,Ok(json!({"message":{
                "id":"sent","chat_id":"chat","direction":"out","sent_at":"2026-01-01T00:00:00Z",
                "text":"synthetic body","ext":{"fixture":{"client_id":"synthetic-result"}}
            }})),CancellationToken::new()).await
        })};
        tokio::time::timeout(Duration::from_secs(2),async {
            while queue.waiting_for_test()<3 {tokio::task::yield_now().await;}
        }).await.unwrap();
        assert!(outbox.inner.ingest.try_lock().is_ok(),"queued result never holds ingest mutex");
        assert_eq!(outbox.get(row.id).unwrap().state,OutboxState::Sending);
        caller.abort();assert!(caller.await.unwrap_err().is_cancelled());
        let shutdown={let outbox=outbox.clone();tokio::spawn(async move {outbox.shutdown().await})};
        tokio::time::timeout(Duration::from_secs(2),outbox.inner.stopping.cancelled()).await.unwrap();
        shutdown.abort();assert!(shutdown.await.unwrap_err().is_cancelled());
        let mut retry=Box::pin(outbox.shutdown());
        assert!(tokio::time::timeout(Duration::from_millis(30),&mut retry).await.is_err());
        drop(guard);tokio::time::timeout(Duration::from_secs(2),retry).await.unwrap();
        let (result,mut tasks)=tokio::time::timeout(Duration::from_secs(2),scheduler).await.unwrap().unwrap();
        assert!(matches!(result,Err(super::super::OutboxError::Stopped)));
        assert_eq!(tasks.len(),1,"revoked scheduling returns the actual existing task set");
        tasks.abort_all();assert!(tasks.join_next().await.unwrap().unwrap_err().is_cancelled());
        assert_eq!(outbox.get(row.id).unwrap().state,OutboxState::Sent);
        let conn=core.open_db().unwrap();
        let count:i64=conn.conn().query_row("SELECT COUNT(*) FROM message WHERE message_id='sent'",[],|r|r.get(0)).unwrap();
        assert_eq!(count,1,"the accepted real database result is retained once; no network call occurs");
        outbox.shutdown().await;core.shutdown().await;
    }
}
