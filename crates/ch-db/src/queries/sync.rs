//! Outbox rows, history cursors and gaps, presence log. (State machines live in ch-core.)

use rusqlite::{Connection, params};

use crate::types::{HistoryGapRow, HistoryStateRow, OutboxRow, PresenceLogRow, outbox_state};
use crate::{Json, Millis, Result, json_opt, now_millis};

use super::{changes, collect, opt};

// ---- outbox --------------------------------------------------------------------------------

const OUTBOX_COLS: &str = "id, account_id, chat_id, topic_id, client_id, kind, method, params_json, state, attempts, \
    next_attempt_at, last_error_json, echo_message_id, result_message_ids, request_id, done_bytes, total_bytes, \
    created_at, updated_at";

fn outbox_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<OutboxRow> {
    let params: Json<serde_json::Value> = r.get(7)?;
    let last_error: Option<Json<serde_json::Value>> = r.get(11)?;
    let result_ids: Option<Json<Vec<String>>> = r.get(13)?;
    Ok(OutboxRow {
        id: r.get(0)?,
        account_id: r.get(1)?,
        chat_id: r.get(2)?,
        topic_id: r.get(3)?,
        client_id: r.get(4)?,
        kind: r.get(5)?,
        method: r.get(6)?,
        params: params.0,
        state: r.get(8)?,
        attempts: r.get(9)?,
        next_attempt_at: r.get(10)?,
        last_error: last_error.map(|j| j.0),
        echo_message_id: r.get(12)?,
        result_message_ids: result_ids.map(|j| j.0),
        request_id: r.get(14)?,
        done_bytes: r.get(15)?,
        total_bytes: r.get(16)?,
        created_at: r.get(17)?,
        updated_at: r.get(18)?,
    })
}

/// Enqueue an item (`id` ignored; `state` forced to `queued`). Returns the rowid.
pub fn insert_outbox(conn: &Connection, o: &OutboxRow) -> Result<i64> {
    conn.prepare_cached(
        "INSERT INTO outbox (account_id, chat_id, topic_id, client_id, kind, method, params_json, state, attempts,
            next_attempt_at, echo_message_id, created_at, updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,0,?9,?10,?11,?11)",
    )?
    .execute(params![
        o.account_id,
        o.chat_id,
        o.topic_id,
        o.client_id,
        o.kind,
        o.method,
        Json(&o.params),
        outbox_state::QUEUED,
        o.next_attempt_at,
        o.echo_message_id,
        o.created_at
    ])?;
    Ok(conn.last_insert_rowid())
}

/// One item.
pub fn get_outbox(conn: &Connection, id: i64) -> Result<Option<OutboxRow>> {
    opt(
        conn,
        &format!("SELECT {OUTBOX_COLS} FROM outbox WHERE id = ?1"),
        &[&id],
        outbox_from_row,
    )
}

/// One item by its client id.
pub fn get_outbox_by_client_id(conn: &Connection, client_id: &str) -> Result<Option<OutboxRow>> {
    opt(
        conn,
        &format!("SELECT {OUTBOX_COLS} FROM outbox WHERE client_id = ?1"),
        &[&client_id],
        outbox_from_row,
    )
}

/// Items of an account in a given state, oldest first.
pub fn list_outbox(
    conn: &Connection,
    account_id: &str,
    state: Option<&str>,
) -> Result<Vec<OutboxRow>> {
    collect(
        conn,
        &format!(
            "SELECT {OUTBOX_COLS} FROM outbox WHERE account_id = ?1 AND (?2 IS NULL OR state = ?2)
             ORDER BY created_at, id"
        ),
        &[&account_id, &state],
        outbox_from_row,
    )
}

/// Items due for (re)try: queued, or failed with `next_attempt_at <= now`.
pub fn list_outbox_due(conn: &Connection, account_id: &str, now: Millis) -> Result<Vec<OutboxRow>> {
    collect(
        conn,
        &format!(
            "SELECT {OUTBOX_COLS} FROM outbox WHERE account_id = ?1
               AND (state = 'queued' OR (state = 'failed' AND next_attempt_at IS NOT NULL AND next_attempt_at <= ?2))
             ORDER BY created_at, id"
        ),
        &[&account_id, &now],
        outbox_from_row,
    )
}

/// Move an item to a state. `error` is kept as the last error when given; `attempts` increments on
/// `sending`.
pub fn set_outbox_state(
    conn: &Connection,
    id: i64,
    state: &str,
    error: Option<&serde_json::Value>,
    next_attempt_at: Option<Millis>,
    request_id: Option<&str>,
) -> Result<bool> {
    conn.prepare_cached(
        "UPDATE outbox SET state = ?2, last_error_json = COALESCE(?3, last_error_json), next_attempt_at = ?4,
            request_id = ?5, attempts = attempts + (?2 = 'sending'), updated_at = ?6
         WHERE id = ?1",
    )?
    .execute(params![id, state, json_opt(&error)?, next_attempt_at, request_id, now_millis()])?;
    Ok(changes(conn) > 0)
}

/// Record a successful send: state `sent` plus the platform message ids.
pub fn set_outbox_sent(conn: &Connection, id: i64, result_message_ids: &[String]) -> Result<bool> {
    conn.prepare_cached(
        "UPDATE outbox SET state = 'sent', result_message_ids = ?2, request_id = NULL, next_attempt_at = NULL,
            updated_at = ?3 WHERE id = ?1",
    )?
    .execute(params![id, Json(result_message_ids), now_millis()])?;
    Ok(changes(conn) > 0)
}

/// Upload progress.
pub fn set_outbox_progress(
    conn: &Connection,
    id: i64,
    done: i64,
    total: Option<i64>,
) -> Result<bool> {
    conn.prepare_cached(
        "UPDATE outbox SET done_bytes = ?2, total_bytes = ?3, updated_at = ?4 WHERE id = ?1",
    )?
    .execute(params![id, done, total, now_millis()])?;
    Ok(changes(conn) > 0)
}

/// Link the local echo message row.
pub fn set_outbox_echo(conn: &Connection, id: i64, echo_message_id: Option<i64>) -> Result<bool> {
    conn.prepare_cached("UPDATE outbox SET echo_message_id = ?2, updated_at = ?3 WHERE id = ?1")?
        .execute(params![id, echo_message_id, now_millis()])?;
    Ok(changes(conn) > 0)
}

/// Remove an item (sent long ago, or cancelled and cleaned up).
pub fn delete_outbox(conn: &Connection, id: i64) -> Result<bool> {
    conn.prepare_cached("DELETE FROM outbox WHERE id = ?1")?
        .execute([id])?;
    Ok(changes(conn) > 0)
}

// ---- history_state -------------------------------------------------------------------------

fn history_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<HistoryStateRow> {
    Ok(HistoryStateRow {
        conversation_id: r.get(0)?,
        topic_id: r.get(1)?,
        older_cursor: r.get(2)?,
        reached_start: r.get(3)?,
        oldest_message_id: r.get(4)?,
        oldest_sent_at: r.get(5)?,
        newest_message_id: r.get(6)?,
        newest_sent_at: r.get(7)?,
        last_fetch_at: r.get(8)?,
        updated_at: r.get(9)?,
    })
}

const HISTORY_COLS: &str = "conversation_id, topic_id, older_cursor, reached_start, oldest_message_id, \
    oldest_sent_at, newest_message_id, newest_sent_at, last_fetch_at, updated_at";

/// History state of a chat (`topic_id = ""`) or topic.
pub fn get_history_state(
    conn: &Connection,
    conversation_id: i64,
    topic_id: &str,
) -> Result<Option<HistoryStateRow>> {
    opt(
        conn,
        &format!(
            "SELECT {HISTORY_COLS} FROM history_state WHERE conversation_id = ?1 AND topic_id = ?2"
        ),
        &[&conversation_id, &topic_id],
        history_from_row,
    )
}

/// Write the history state wholesale.
pub fn set_history_state(conn: &Connection, h: &HistoryStateRow) -> Result<()> {
    conn.prepare_cached(&format!(
        "INSERT INTO history_state ({HISTORY_COLS}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
         ON CONFLICT(conversation_id, topic_id) DO UPDATE SET older_cursor=excluded.older_cursor,
           reached_start=excluded.reached_start, oldest_message_id=excluded.oldest_message_id,
           oldest_sent_at=excluded.oldest_sent_at, newest_message_id=excluded.newest_message_id,
           newest_sent_at=excluded.newest_sent_at, last_fetch_at=excluded.last_fetch_at, updated_at=excluded.updated_at"
    ))?
    .execute(params![
        h.conversation_id,
        h.topic_id,
        h.older_cursor,
        h.reached_start,
        h.oldest_message_id,
        h.oldest_sent_at,
        h.newest_message_id,
        h.newest_sent_at,
        h.last_fetch_at,
        h.updated_at
    ])?;
    Ok(())
}

// ---- history_gap ---------------------------------------------------------------------------

fn gap_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<HistoryGapRow> {
    Ok(HistoryGapRow {
        id: r.get(0)?,
        conversation_id: r.get(1)?,
        topic_id: r.get(2)?,
        after_message_id: r.get(3)?,
        before_message_id: r.get(4)?,
        cursor: r.get(5)?,
        created_at: r.get(6)?,
        updated_at: r.get(7)?,
    })
}

/// Record a hole to fill. Returns the rowid.
pub fn insert_history_gap(conn: &Connection, g: &HistoryGapRow) -> Result<i64> {
    conn.prepare_cached(
        "INSERT INTO history_gap (conversation_id, topic_id, after_message_id, before_message_id, cursor, created_at, updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?6)",
    )?
    .execute(params![
        g.conversation_id,
        g.topic_id,
        g.after_message_id,
        g.before_message_id,
        g.cursor,
        g.created_at
    ])?;
    Ok(conn.last_insert_rowid())
}

/// Open gaps of a chat/topic, oldest first.
pub fn list_history_gaps(
    conn: &Connection,
    conversation_id: i64,
    topic_id: &str,
) -> Result<Vec<HistoryGapRow>> {
    collect(
        conn,
        "SELECT id, conversation_id, topic_id, after_message_id, before_message_id, cursor, created_at, updated_at
         FROM history_gap WHERE conversation_id = ?1 AND topic_id = ?2 ORDER BY created_at, id",
        &[&conversation_id, &topic_id],
        gap_from_row,
    )
}

/// Advance a gap's paging cursor.
pub fn set_history_gap_cursor(conn: &Connection, id: i64, cursor: Option<&str>) -> Result<bool> {
    conn.prepare_cached("UPDATE history_gap SET cursor = ?2, updated_at = ?3 WHERE id = ?1")?
        .execute(params![id, cursor, now_millis()])?;
    Ok(changes(conn) > 0)
}

/// Close a gap.
pub fn delete_history_gap(conn: &Connection, id: i64) -> Result<bool> {
    conn.prepare_cached("DELETE FROM history_gap WHERE id = ?1")?
        .execute([id])?;
    Ok(changes(conn) > 0)
}

// ---- presence_log --------------------------------------------------------------------------

/// Append a presence-log entry.
pub fn insert_presence_log(conn: &Connection, p: &PresenceLogRow) -> Result<i64> {
    conn.prepare_cached(
        "INSERT INTO presence_log (account_id, user_id, chat_id, at, kind, detail_json) VALUES (?1,?2,?3,?4,?5,?6)",
    )?
    .execute(params![
        p.account_id,
        p.user_id,
        p.chat_id,
        p.at,
        p.kind,
        json_opt(&p.detail)?
    ])?;
    Ok(conn.last_insert_rowid())
}

/// A user's presence log, newest first.
pub fn list_presence_log(
    conn: &Connection,
    account_id: &str,
    user_id: &str,
    limit: u32,
) -> Result<Vec<PresenceLogRow>> {
    collect(
        conn,
        "SELECT id, account_id, user_id, chat_id, at, kind, detail_json FROM presence_log
         WHERE account_id = ?1 AND user_id = ?2 ORDER BY at DESC, id DESC LIMIT ?3",
        &[&account_id, &user_id, &limit],
        |r| {
            let detail: Option<Json<serde_json::Value>> = r.get(6)?;
            Ok(PresenceLogRow {
                id: r.get(0)?,
                account_id: r.get(1)?,
                user_id: r.get(2)?,
                chat_id: r.get(3)?,
                at: r.get(4)?,
                kind: r.get(5)?,
                detail: detail.map(|j| j.0),
            })
        },
    )
}

/// Drop presence-log entries older than `before`.
pub fn prune_presence_log(conn: &Connection, before: Millis) -> Result<usize> {
    conn.prepare_cached("DELETE FROM presence_log WHERE at < ?1")?
        .execute([before])?;
    Ok(changes(conn))
}
