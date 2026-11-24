//! Synchronous database helpers for the history service: thread windows by `(sent_at, rowid)`
//! key, gap edges resolved to keys, and gap row updates.

use ch_db::types::{HistoryGapRow, MessageRow};
use ch_db::{Millis, Result};
use rusqlite::{Connection, OptionalExtension, params};

/// Position of a message in a thread: `(sent_at, message.id)`, the order `list_messages` uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(pub Millis, pub i64);

impl Key {
    /// Just before every message sent at `at` (so `rows_older(Key::at(t))` = `sent_at < t`).
    pub fn at(at: Millis) -> Self {
        Self(at, i64::MIN)
    }
}

/// An open gap with its edges resolved. Messages strictly between `lo` and `hi` are incomplete.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedGap {
    pub row: HistoryGapRow,
    /// Key of `after_message_id` (the newest message known to precede the hole).
    pub lo: Key,
    /// Key of `before_message_id`; when that is absent (or gone) the hole is degenerate and
    /// sits right above `lo` (`hi = (lo.sent_at, lo.id + 1)`): nothing is hidden, but a marker is
    /// shown and the gap is still filled.
    pub hi: Key,
}

const WHERE_THREAD: &str = "conversation_id = ?1 AND (?2 IS NULL OR topic_id = ?2) \
    AND is_scheduled = 0 AND (?3 = 1 OR deleted_at IS NULL)";

fn load(conn: &Connection, ids: Vec<i64>) -> Result<Vec<MessageRow>> {
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(m) = ch_db::get_message_by_id(conn, id)? {
            out.push(m);
        }
    }
    Ok(out)
}

fn ids(conn: &Connection, sql: &str, p: &[&dyn rusqlite::ToSql]) -> Result<Vec<i64>> {
    let mut stmt = conn.prepare_cached(sql)?;
    let rows = stmt.query_map(p, |r| r.get::<_, i64>(0))?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Messages with `floor <= key < below`, newest first.
#[allow(clippy::too_many_arguments)]
pub fn rows_older(
    conn: &Connection,
    conv: i64,
    topic: Option<&str>,
    below: Option<Key>,
    floor: Option<Key>,
    limit: u32,
    include_deleted: bool,
) -> Result<Vec<MessageRow>> {
    let sql = format!(
        "SELECT id FROM message WHERE {WHERE_THREAD}
           AND (?4 IS NULL OR sent_at < ?4 OR (sent_at = ?4 AND id < ?5))
           AND (?6 IS NULL OR sent_at > ?6 OR (sent_at = ?6 AND id >= ?7))
         ORDER BY sent_at DESC, id DESC LIMIT ?8"
    );
    let v = ids(
        conn,
        &sql,
        params![
            conv,
            topic,
            include_deleted,
            below.map(|k| k.0),
            below.map(|k| k.1),
            floor.map(|k| k.0),
            floor.map(|k| k.1),
            limit
        ],
    )?;
    load(conn, v)
}

/// Messages with `above < key <= ceil`, oldest first.
#[allow(clippy::too_many_arguments)]
pub fn rows_newer(
    conn: &Connection,
    conv: i64,
    topic: Option<&str>,
    above: Key,
    ceil: Option<Key>,
    limit: u32,
    include_deleted: bool,
) -> Result<Vec<MessageRow>> {
    let sql = format!(
        "SELECT id FROM message WHERE {WHERE_THREAD}
           AND (sent_at > ?4 OR (sent_at = ?4 AND id > ?5))
           AND (?6 IS NULL OR sent_at < ?6 OR (sent_at = ?6 AND id <= ?7))
         ORDER BY sent_at ASC, id ASC LIMIT ?8"
    );
    let v = ids(
        conn,
        &sql,
        params![
            conv,
            topic,
            include_deleted,
            above.0,
            above.1,
            ceil.map(|k| k.0),
            ceil.map(|k| k.1),
            limit
        ],
    )?;
    load(conn, v)
}

/// Key of a stored message by platform id.
pub fn key_of(
    conn: &Connection,
    conv: i64,
    topic: Option<&str>,
    message_id: &str,
) -> Result<Option<Key>> {
    Ok(conn
        .prepare_cached(
            "SELECT sent_at, id FROM message WHERE conversation_id = ?1 AND message_id = ?2 AND (?3 IS NULL OR topic_id = ?3)",
        )?
        .query_row(params![conv, message_id, topic], |r| Ok(Key(r.get(0)?, r.get(1)?)))
        .optional()?)
}

/// The newest platform message of a thread (local echoes and scheduled ones excluded,
/// tombstones included: they are known messages).
pub fn newest_platform(
    conn: &Connection,
    conv: i64,
    topic: Option<&str>,
) -> Result<Option<(Key, String)>> {
    Ok(conn
        .prepare_cached(
            "SELECT sent_at, id, message_id FROM message
             WHERE conversation_id = ?1 AND (?2 IS NULL OR topic_id = ?2) AND is_scheduled = 0
               AND message_id NOT LIKE 'local:%'
             ORDER BY sent_at DESC, id DESC LIMIT 1",
        )?
        .query_row(params![conv, topic], |r| {
            Ok((Key(r.get(0)?, r.get(1)?), r.get(2)?))
        })
        .optional()?)
}

/// The oldest platform message of a thread.
pub fn oldest_platform(
    conn: &Connection,
    conv: i64,
    topic: Option<&str>,
) -> Result<Option<(Key, String)>> {
    Ok(conn
        .prepare_cached(
            "SELECT sent_at, id, message_id FROM message
             WHERE conversation_id = ?1 AND (?2 IS NULL OR topic_id = ?2) AND is_scheduled = 0
               AND message_id NOT LIKE 'local:%'
             ORDER BY sent_at ASC, id ASC LIMIT 1",
        )?
        .query_row(params![conv, topic], |r| {
            Ok((Key(r.get(0)?, r.get(1)?), r.get(2)?))
        })
        .optional()?)
}

/// Open gaps with edges resolved inside their thread. Invalid edges are skipped, never
/// repaired or deleted by a read operation.
pub fn resolved_gaps(conn: &Connection, conv: i64, topic_key: &str) -> Result<Vec<ResolvedGap>> {
    let mut out = Vec::new();
    for row in ch_db::list_history_gaps(conn, conv, topic_key)? {
        let topic = (!topic_key.is_empty()).then_some(topic_key);
        let Some(lo) = key_of(conn, conv, topic, &row.after_message_id)? else {
            tracing::warn!(gap = row.id, "history gap has no lower edge in its thread");
            continue;
        };
        let hi = match &row.before_message_id {
            Some(b) => key_of(conn, conv, topic, b)?,
            None => None,
        }
        .filter(|hi| *hi > lo)
        .unwrap_or(Key(lo.0, lo.1.saturating_add(1)));
        out.push(ResolvedGap { row, lo, hi });
    }
    Ok(out)
}

/// One gap by id. Verify ownership before resolving edges; foreign or corrupt rows are
/// errors, and this function never writes to the database.
pub fn resolved_gap(
    conn: &Connection,
    gap_id: i64,
    expected_conv: Option<i64>,
    expected_topic: &str,
) -> Result<Option<ResolvedGap>> {
    let Some((conv, topic)): Option<(i64, String)> = conn
        .prepare_cached("SELECT conversation_id, topic_id FROM history_gap WHERE id = ?1")?
        .query_row([gap_id], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()?
    else {
        return Ok(None);
    };
    if expected_conv != Some(conv) || topic != expected_topic {
        return Err(ch_db::Error::Invalid(
            "history gap belongs to another thread".into(),
        ));
    }
    let gap = resolved_gaps(conn, conv, &topic)?
        .into_iter()
        .find(|g| g.row.id == gap_id);
    gap.map(Some)
        .ok_or_else(|| ch_db::Error::Invalid("history gap has no lower edge in its thread".into()))
}

/// Move a gap's edges and paging cursor.
pub fn update_gap(
    conn: &Connection,
    id: i64,
    after: &str,
    before: Option<&str>,
    cursor: Option<&str>,
    now: Millis,
) -> Result<()> {
    conn.prepare_cached(
        "UPDATE history_gap SET after_message_id = ?2, before_message_id = ?3, cursor = ?4, updated_at = ?5
         WHERE id = ?1",
    )?
    .execute(params![id, after, before, cursor, now])?;
    Ok(())
}

/// Every open gap, least recently worked on first, with the chat it belongs to:
/// `(gap_id, account_id, chat_id, topic_id)`.
pub fn all_gaps(conn: &Connection) -> Result<Vec<(i64, String, String, String)>> {
    let mut stmt = conn.prepare_cached(
        "SELECT g.id, c.account_id, c.chat_id, g.topic_id FROM history_gap g
         JOIN conversation c ON c.id = g.conversation_id ORDER BY g.updated_at, g.id",
    )?;
    let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}
