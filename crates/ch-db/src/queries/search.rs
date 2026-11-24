//! Local full-text search over `message_fts` with Telegram-style filters.

use rusqlite::{Connection, ToSql};

use crate::types::{SearchHit, SearchQuery};
use crate::{Millis, Result};

/// Turn free text into an FTS5 MATCH expression: each bare token becomes a quoted prefix term,
/// `"quoted phrases"` are kept as phrases. Returns `None` when nothing is left to match.
pub fn fts_query(text: &str) -> Option<String> {
    let mut terms = Vec::new();
    let mut rest = text.trim();
    while !rest.is_empty() {
        if let Some(stripped) = rest.strip_prefix('"') {
            let end = stripped.find('"').unwrap_or(stripped.len());
            let phrase = stripped[..end].trim();
            if !phrase.is_empty() {
                terms.push(format!("\"{}\"", phrase.replace('"', "\"\"")));
            }
            rest = stripped[end..].trim_start_matches('"').trim_start();
        } else {
            let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
            let token = rest[..end].trim_matches(|c: char| !c.is_alphanumeric());
            if !token.is_empty() {
                terms.push(format!("\"{}\"*", token.replace('"', "\"\"")));
            }
            rest = rest[end..].trim_start();
        }
    }
    if terms.is_empty() {
        None
    } else {
        Some(terms.join(" "))
    }
}

/// Search messages. Newest first. `q.limit == 0` means 50.
pub fn search_messages(conn: &Connection, q: &SearchQuery) -> Result<Vec<SearchHit>> {
    let match_expr = fts_query(&q.text);
    let limit = if q.limit == 0 { 50 } else { q.limit };
    let mut sql = String::from(
        "SELECT m.id, m.conversation_id, c.account_id, c.chat_id, m.message_id, m.sent_at, ",
    );
    let mut params: Vec<Box<dyn ToSql>> = Vec::new();
    if match_expr.is_some() {
        sql.push_str("snippet(message_fts, 0, '[', ']', '…', 12) ");
        sql.push_str(
            "FROM message_fts JOIN message m ON m.id = message_fts.rowid \
             JOIN conversation c ON c.id = m.conversation_id WHERE message_fts MATCH ?",
        );
        params.push(Box::new(match_expr.clone().unwrap()));
    } else {
        sql.push_str("COALESCE(substr(m.text, 1, 120), '') ");
        sql.push_str("FROM message m JOIN conversation c ON c.id = m.conversation_id WHERE 1 = 1");
    }
    if let Some(a) = &q.account_id {
        sql.push_str(" AND c.account_id = ?");
        params.push(Box::new(a.clone()));
    }
    if let Some(cid) = q.conversation_id {
        sql.push_str(" AND m.conversation_id = ?");
        params.push(Box::new(cid));
    }
    if let Some(t) = &q.topic_id {
        sql.push_str(" AND m.topic_id = ?");
        params.push(Box::new(t.clone()));
    }
    if let Some(s) = &q.sender_id {
        sql.push_str(" AND m.sender_id = ?");
        params.push(Box::new(s.clone()));
    }
    if !q.media.is_empty() {
        let mut parts = Vec::new();
        for kind in &q.media {
            if kind == "link" {
                parts.push("m.has_link = 1".to_owned());
            } else {
                parts.push("m.media_kind = ?".to_owned());
                params.push(Box::new(kind.clone()));
            }
        }
        sql.push_str(&format!(" AND ({})", parts.join(" OR ")));
    }
    if let Some(b) = q.before {
        sql.push_str(" AND m.sent_at < ?");
        params.push(Box::new(b));
    }
    if let Some(a) = q.after {
        sql.push_str(" AND m.sent_at >= ?");
        params.push(Box::new(a));
    }
    if !q.include_deleted {
        sql.push_str(" AND m.deleted_at IS NULL");
    }
    sql.push_str(" ORDER BY m.sent_at DESC, m.id DESC LIMIT ? OFFSET ?");
    params.push(Box::new(limit));
    params.push(Box::new(q.offset));

    let mut stmt = conn.prepare(&sql)?;
    let refs: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();
    let rows = stmt.query_map(refs.as_slice(), |r| {
        Ok(SearchHit {
            message_rowid: r.get(0)?,
            conversation_id: r.get(1)?,
            account_id: r.get(2)?,
            chat_id: r.get(3)?,
            message_id: r.get(4)?,
            sent_at: r.get::<_, Millis>(5)?,
            snippet: r.get(6)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Rebuild the FTS index from the `message` table (after a bulk import).
pub fn rebuild_fts(conn: &Connection) -> Result<()> {
    conn.execute_batch("INSERT INTO message_fts(message_fts) VALUES ('rebuild');")?;
    Ok(())
}

/// Number of indexed rows (tests).
pub fn fts_count(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row("SELECT COUNT(*) FROM message_fts", [], |r| r.get(0))?)
}
