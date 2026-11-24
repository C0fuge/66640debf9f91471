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
        let due = match pass(&inner, &account_id, &plugin_id, &mut tasks) {
            Ok(due) => due,
            Err(e) => {
                warn!(error = %e, "outbox scheduling failed");
                Some(inner.now().saturating_add(5_000))
            }
        };
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
                    let g = inner.ingest();
                    requeue(inner, &g, &row, None, None)
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
                match finish(inner, &plugin, &row, kind, res, &cancel) {
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
    if !ch_db::get_account(conn, &row.account_id)?.is_some_and(|a| a.plugin_id == plugin) {
        return Ok(None);
    }
    Ok(ch_db::get_outbox(conn, row.id)?.filter(|r| {
        r.account_id == row.account_id
            && r.client_id == row.client_id
            && r.state == outbox_state::SENDING
    }))
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
    inner: &Inner,
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
        {
            let g = inner.ingest();
            if owned_attempt(&g, row, plugin)?.is_none() {
                return Ok(());
            }
            if let Some(mid) = Inner::adopted_id(&g, row)? {
                return confirm(inner, &g, row, &mid);
            }
            // Only text has a client_id field in the v1 wire contract.
            if kind != OutboxKind::SendText || expired {
                return fail(inner, &g, row, &Inner::unknown_outcome_error(reason));
            }
        }
        tokio::time::sleep(inner.policy.reconcile_poll.max(Duration::from_millis(1))).await;
    }
}
