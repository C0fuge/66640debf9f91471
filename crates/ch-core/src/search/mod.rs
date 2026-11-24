//! Search: local full-text search over stored messages.
//!
//! There is no platform-side message search in the plugin API (§11.4): everything ingested is
//! indexed in `message_fts` (FTS5, `unicode61 remove_diacritics 2`, prefix indexes 2/3) and
//! searched here, across every account at once or inside one chat.
//!
//! # Query syntax
//!
//! The search box is split on whitespace; `"…"` groups words (also as a filter value). Every
//! part must match (AND); the values of one filter key are alternatives (OR).
//!
//! | part | meaning |
//! | --- | --- |
//! | `word` | a token starting with `word`, case- and (Latin) accent-insensitive: `прив` finds `Привет`, `cafe` finds `Café` |
//! | `"two words"` | the tokens adjacent and in this order |
//! | `🎉`, `+1`, `#` | a part without letters or digits: a case-sensitive substring (the tokenizer ignores symbols) |
//! | `from:me` | sent by this account |
//! | `from:@alice` | sender's username is `alice` |
//! | `from:Alice` | sender's name (contact, person or the name on the message) contains `alice` |
//! | `in:@channel` / `in:"Family chat"` | chat username, or the title contains the value (global search only) |
//! | `media:photo` | `photo` `video` `gif` `sticker` `voice` `round` `music` `file` `link` `other`; comma = OR (`media:photo,video`); `has:` is an alias |
//! | `before:2026-01-31` | sent before 00:00 that day (local time, [`SearchRequest::utc_offset_minutes`]) |
//! | `after:2026-01-01` | sent at or after 00:00 that day |
//! | `is:deleted` | only kept-deleted messages |
//!
//! Keys are case-insensitive. A `key:value` with an unknown key is plain text (`http://…`), and a
//! filter with a bad value is ignored and listed in [`ParsedQuery::invalid`]. Deleted messages are
//! excluded unless [`SearchRequest::include_deleted`] or `is:deleted`. A query with filters but no
//! text lists the matching messages; an empty query finds nothing.
//!
//! # Results
//!
//! Newest first ([`Order::Newest`], the default) or by FTS rank ([`Order::Relevance`], bm25,
//! ties newest first), paged by offset. Every hit carries the highlighted ranges in the full
//! message text and a short [`Snippet`] around the first one. **All offsets and lengths are
//! UTF-16 code units**, the same unit as the protocol's `Entity` offsets (§8.3), so the UI can
//! apply them with the entity renderer. Highlights come from FTS5's own `highlight()`, so they
//! match exactly the tokens the index matched.

mod query;

use std::collections::BTreeSet;

use ch_db::{Millis, Result};
use ch_protocol::Id;
use rusqlite::{Connection, ToSql};

pub use query::{Date, MediaFilter, ParsedQuery, Term, fts_expr, parse_query};

use crate::history::SharedIngest;
use crate::ingest::{Ingest, IngestOptions};
use crate::runtime::Core;
use std::sync::{Arc, Mutex};

/// Where to search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    /// Every chat (of one account, if given). `in:` applies.
    Global { account_id: Option<String> },
    /// One chat (one topic, if given). `in:` is ignored.
    Chat {
        account_id: String,
        chat_id: Id,
        topic_id: Option<Id>,
    },
}

impl Default for Scope {
    fn default() -> Self {
        Self::Global { account_id: None }
    }
}

/// Result order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Order {
    #[default]
    Newest,
    Relevance,
}

/// One search.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SearchRequest {
    /// The search box, filters included (see the module docs).
    pub query: String,
    pub scope: Scope,
    pub order: Order,
    /// Also return kept-deleted messages.
    pub include_deleted: bool,
    /// Local time zone for `before:`/`after:` dates, minutes east of UTC.
    pub utc_offset_minutes: i32,
    /// `0` = 50; other values are capped at 200.
    pub limit: u32,
    pub offset: u32,
}

/// A range of text in UTF-16 code units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Span {
    pub offset: u32,
    pub length: u32,
}

/// A short excerpt of the message text with the matches marked.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Snippet {
    /// Newlines are shown as spaces; `…` marks cut ends.
    pub text: String,
    /// Into `text`, UTF-16.
    pub highlights: Vec<Span>,
}

/// One matching message.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    /// `message.id`.
    pub message_rowid: i64,
    pub conversation_id: i64,
    pub account_id: String,
    pub chat_id: Id,
    pub chat_title: Option<String>,
    pub topic_id: Option<Id>,
    pub message_id: Id,
    pub sent_at: Millis,
    pub sender_id: Option<Id>,
    pub sender_name: Option<String>,
    /// `in` | `out`.
    pub direction: String,
    pub media_kind: Option<String>,
    /// A kept-deleted message.
    pub deleted: bool,
    /// Matches in the full message text, UTF-16, sorted, non-overlapping.
    pub highlights: Vec<Span>,
    pub snippet: Snippet,
}

/// A page of hits.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SearchPage {
    pub hits: Vec<SearchHit>,
    /// `offset` of the next page, if there is one.
    pub next_offset: Option<u32>,
    /// The parsed query (for the UI's filter chips and [`ParsedQuery::invalid`]).
    pub parsed: ParsedQuery,
}

/// The search service.
#[derive(Clone)]
pub struct Search {
    ingest: SharedIngest,
}

impl std::fmt::Debug for Search {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Search").finish_non_exhaustive()
    }
}

impl Search {
    pub fn new(ingest: SharedIngest) -> Self {
        Self { ingest }
    }

    /// Use a dedicated connection to the core database. Call this blocking constructor off
    /// the async executor. Search only reads this connection and needs no plugin access.
    pub fn from_core(core: &Core) -> Result<Self> {
        Ok(Self::new(Arc::new(Mutex::new(Ingest::new(
            core.open_db()?,
            IngestOptions::default(),
        )))))
    }

    /// Run a search. This is synchronous database work; serve it on a blocking task.
    pub fn search(&self, req: &SearchRequest) -> Result<SearchPage> {
        let g = self.ingest.lock().unwrap_or_else(|p| p.into_inner());
        search(g.db().conn(), req)
    }
}

const SNIPPET_BEFORE: usize = 30;
const SNIPPET_LEN: usize = 100;

/// Run a search on a connection.
pub fn search(conn: &Connection, req: &SearchRequest) -> Result<SearchPage> {
    // The row text and FTS highlight offsets must come from one SQLite snapshot. Reuse a
    // caller-owned transaction without committing it; otherwise hold a read transaction
    // until all snippets have been built. A concurrent writer may keep updating WAL.
    if conn.is_autocommit() {
        let tx = conn.unchecked_transaction()?;
        let page = search_snapshot(&tx, req)?;
        tx.commit()?;
        Ok(page)
    } else {
        search_snapshot(conn, req)
    }
}

fn search_snapshot(conn: &Connection, req: &SearchRequest) -> Result<SearchPage> {
    let parsed = parse_query(&req.query);
    let mut page = SearchPage {
        parsed: parsed.clone(),
        ..Default::default()
    };
    if parsed.is_empty() {
        return Ok(page);
    }
    let limit = if req.limit == 0 {
        50
    } else {
        req.limit.min(200)
    };
    let fts = fts_expr(&parsed.terms);
    let literals: Vec<&str> = parsed
        .terms
        .iter()
        .filter_map(|t| match t {
            Term::Literal(l) => Some(l.as_str()),
            _ => None,
        })
        .collect();

    let mut sql = String::from(
        "SELECT m.id, m.conversation_id, c.account_id, c.chat_id, c.title, m.topic_id, m.message_id,
                m.sent_at, m.sender_id, m.sender_name, m.direction, m.media_kind, m.deleted_at, m.text ",
    );
    let mut p: Vec<Box<dyn ToSql>> = Vec::new();
    if let Some(expr) = &fts {
        sql.push_str(
            "FROM message_fts JOIN message m ON m.id = message_fts.rowid
             JOIN conversation c ON c.id = m.conversation_id WHERE message_fts MATCH ?",
        );
        p.push(Box::new(expr.clone()));
    } else {
        sql.push_str("FROM message m JOIN conversation c ON c.id = m.conversation_id WHERE 1 = 1");
    }
    sql.push_str(" AND m.is_scheduled = 0");
    for l in &literals {
        sql.push_str(" AND instr(m.text, ?) > 0");
        p.push(Box::new((*l).to_owned()));
    }
    match &req.scope {
        Scope::Global { account_id } => {
            if let Some(a) = account_id {
                sql.push_str(" AND c.account_id = ?");
                p.push(Box::new(a.clone()));
            }
            if !parsed.chats.is_empty() {
                let ids = resolve_chats(conn, account_id.as_deref(), &parsed.chats)?;
                if ids.is_empty() {
                    return Ok(page);
                }
                push_in(&mut sql, &mut p, "m.conversation_id", ids);
            }
        }
        Scope::Chat {
            account_id,
            chat_id,
            topic_id,
        } => {
            sql.push_str(" AND c.account_id = ? AND c.chat_id = ?");
            p.push(Box::new(account_id.clone()));
            p.push(Box::new(chat_id.clone()));
            if let Some(t) = topic_id {
                sql.push_str(" AND m.topic_id = ?");
                p.push(Box::new(t.clone()));
            }
        }
    }
    if !parsed.from.is_empty() {
        let account = match &req.scope {
            Scope::Global { account_id } => account_id.as_deref(),
            Scope::Chat { account_id, .. } => Some(account_id.as_str()),
        };
        let (me, senders) = resolve_senders(conn, account, &parsed.from)?;
        if !me && senders.is_empty() {
            return Ok(page);
        }
        let mut ors = Vec::new();
        if me {
            ors.push("m.direction = 'out'".to_owned());
        }
        for (a, u) in senders {
            ors.push("(c.account_id = ? AND m.sender_id = ?)".to_owned());
            p.push(Box::new(a));
            p.push(Box::new(u));
        }
        sql.push_str(&format!(" AND ({})", ors.join(" OR ")));
    }
    if !parsed.media.is_empty() {
        let mut ors = Vec::new();
        for m in &parsed.media {
            match m.media_kind() {
                Some(k) => {
                    ors.push("m.media_kind = ?");
                    p.push(Box::new(k));
                }
                None => ors.push("m.has_link = 1"),
            }
        }
        sql.push_str(&format!(" AND ({})", ors.join(" OR ")));
    }
    if let Some(d) = parsed.before {
        sql.push_str(" AND m.sent_at < ?");
        p.push(Box::new(d.start_millis(req.utc_offset_minutes)));
    }
    if let Some(d) = parsed.after {
        sql.push_str(" AND m.sent_at >= ?");
        p.push(Box::new(d.start_millis(req.utc_offset_minutes)));
    }
    if parsed.only_deleted {
        sql.push_str(" AND m.deleted_at IS NOT NULL");
    } else if !req.include_deleted {
        sql.push_str(" AND m.deleted_at IS NULL");
    }
    match (req.order, fts.is_some()) {
        (Order::Relevance, true) => {
            sql.push_str(" ORDER BY bm25(message_fts), m.sent_at DESC, m.id DESC")
        }
        _ => sql.push_str(" ORDER BY m.sent_at DESC, m.id DESC"),
    }
    sql.push_str(" LIMIT ? OFFSET ?");
    p.push(Box::new(limit + 1));
    p.push(Box::new(req.offset));

    let mut stmt = conn.prepare(&sql)?;
    let refs: Vec<&dyn ToSql> = p.iter().map(|b| b.as_ref()).collect();
    let rows = stmt.query_map(refs.as_slice(), |r| {
        Ok((
            SearchHit {
                message_rowid: r.get(0)?,
                conversation_id: r.get(1)?,
                account_id: r.get(2)?,
                chat_id: r.get(3)?,
                chat_title: r.get(4)?,
                topic_id: r.get(5)?,
                message_id: r.get(6)?,
                sent_at: r.get(7)?,
                sender_id: r.get(8)?,
                sender_name: r.get(9)?,
                direction: r.get(10)?,
                media_kind: r.get(11)?,
                deleted: r.get::<_, Option<i64>>(12)?.is_some(),
                highlights: Vec::new(),
                snippet: Snippet::default(),
            },
            r.get::<_, Option<String>>(13)?.unwrap_or_default(),
        ))
    })?;
    let mut rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.len() > limit as usize {
        rows.truncate(limit as usize);
        page.next_offset = req.offset.checked_add(limit);
    }
    for (mut hit, text) in rows {
        let mut ranges = match &fts {
            Some(expr) => fts_ranges(conn, expr, hit.message_rowid, &text)?,
            None => Vec::new(),
        };
        for l in &literals {
            ranges.extend(text.match_indices(l).map(|(i, s)| (i, i + s.len())));
        }
        let ranges = merge(ranges);
        hit.highlights = ranges.iter().map(|&(s, e)| span(&text, s, e)).collect();
        hit.snippet = snippet(&text, &ranges);
        page.hits.push(hit);
    }
    Ok(page)
}

fn push_in(sql: &mut String, p: &mut Vec<Box<dyn ToSql>>, col: &str, ids: BTreeSet<i64>) {
    let marks = vec!["?"; ids.len()].join(",");
    sql.push_str(&format!(" AND {col} IN ({marks})"));
    for id in ids {
        p.push(Box::new(id));
    }
}

fn contains_ci(hay: Option<&str>, needle_lower: &str) -> bool {
    hay.is_some_and(|h| h.to_lowercase().contains(needle_lower))
}

/// `in:` values → conversation rowids.
fn resolve_chats(
    conn: &Connection,
    account: Option<&str>,
    values: &[String],
) -> Result<BTreeSet<i64>> {
    let mut stmt = conn.prepare_cached(
        "SELECT id, chat_id, title, username FROM conversation WHERE (?1 IS NULL OR account_id = ?1)",
    )?;
    let rows = stmt.query_map([account], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, Option<String>>(3)?,
        ))
    })?;
    let rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut out = BTreeSet::new();
    for v in values {
        let (user, frag) = match v.strip_prefix('@') {
            Some(u) => (Some(u.to_lowercase()), None),
            None => (None, Some(v.to_lowercase())),
        };
        for (id, chat_id, title, username) in &rows {
            let hit = match (&user, &frag) {
                (Some(u), _) => username.as_deref().is_some_and(|n| n.to_lowercase() == *u),
                (_, Some(f)) => contains_ci(title.as_deref(), f) || chat_id == v,
                _ => false,
            };
            if hit {
                out.insert(*id);
            }
        }
    }
    Ok(out)
}

/// `from:` values → (`me`, `(account_id, user_id)` senders).
fn resolve_senders(
    conn: &Connection,
    account: Option<&str>,
    values: &[String],
) -> Result<(bool, BTreeSet<(String, String)>)> {
    let mut me = false;
    let mut out = BTreeSet::new();
    let mut handles = conn.prepare_cached(
        "SELECT h.account_id, h.user_id, h.username, h.display_name, h.first_name, h.last_name, p.display_name
         FROM platform_handle h LEFT JOIN person p ON p.id = h.person_id
         WHERE (?1 IS NULL OR h.account_id = ?1)",
    )?;
    type HandleNames = (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let handles: Vec<HandleNames> = handles
        .query_map([account], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;
    // Senders the core has no contact for still carry a name on their messages.
    let mut names = conn.prepare_cached(
        "SELECT DISTINCT c.account_id, m.sender_id, m.sender_name FROM message m
         JOIN conversation c ON c.id = m.conversation_id
         WHERE m.sender_id IS NOT NULL AND m.sender_name IS NOT NULL AND (?1 IS NULL OR c.account_id = ?1)",
    )?;
    let names: Vec<(String, String, String)> = names
        .query_map([account], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?;
    for v in values {
        if v.eq_ignore_ascii_case("me") {
            me = true;
            continue;
        }
        if let Some(u) = v.strip_prefix('@') {
            let u = u.to_lowercase();
            for h in &handles {
                if h.2.as_deref().is_some_and(|n| n.to_lowercase() == u) {
                    out.insert((h.0.clone(), h.1.clone()));
                }
            }
            continue;
        }
        let f = v.to_lowercase();
        for h in &handles {
            let full = match (&h.4, &h.5) {
                (Some(a), Some(b)) => Some(format!("{a} {b}")),
                _ => None,
            };
            if [
                h.2.as_deref(),
                h.3.as_deref(),
                h.4.as_deref(),
                h.5.as_deref(),
                full.as_deref(),
                h.6.as_deref(),
            ]
            .into_iter()
            .any(|n| contains_ci(n, &f))
            {
                out.insert((h.0.clone(), h.1.clone()));
            }
        }
        for (a, u, n) in &names {
            if contains_ci(Some(n), &f) {
                out.insert((a.clone(), u.clone()));
            }
        }
    }
    Ok((me, out))
}

/// Byte ranges of the tokens FTS5 matched in `text`.
fn fts_ranges(
    conn: &Connection,
    expr: &str,
    rowid: i64,
    text: &str,
) -> Result<Vec<(usize, usize)>> {
    // Pick markers absent from the source, so literal private-use characters in messages
    // cannot be mistaken for highlight boundaries.
    let mut index = 0u64;
    let (open, close) = loop {
        let open = format!("\u{E000}open-{index}\u{E001}");
        let close = format!("\u{E002}close-{index}\u{E003}");
        if !text.contains(&open) && !text.contains(&close) {
            break (open, close);
        }
        index += 1;
    };
    let marked: Option<String> = conn
        .prepare_cached(
            "SELECT highlight(message_fts, 0, ?3, ?4) FROM message_fts
             WHERE message_fts MATCH ?1 AND rowid = ?2",
        )?
        .query_row(rusqlite::params![expr, rowid, open, close], |r| r.get(0))
        .map(Some)
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            e => Err(e),
        })?;
    let Some(marked) = marked else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    let mut pos = 0usize;
    let mut rest = marked.as_str();
    while let Some(start) = rest.find(&open) {
        pos += start;
        rest = &rest[start + open.len()..];
        let Some(end) = rest.find(&close) else { break };
        out.push((pos, pos + end));
        pos += end;
        rest = &rest[end + close.len()..];
    }
    Ok(out)
}

/// Sort and merge overlapping or touching byte ranges.
fn merge(mut r: Vec<(usize, usize)>) -> Vec<(usize, usize)> {
    r.retain(|(s, e)| e > s);
    r.sort();
    let mut out: Vec<(usize, usize)> = Vec::with_capacity(r.len());
    for (s, e) in r {
        match out.last_mut() {
            Some(last) if s <= last.1 => last.1 = last.1.max(e),
            _ => out.push((s, e)),
        }
    }
    out
}

fn utf16_len(s: &str) -> u32 {
    s.encode_utf16().count() as u32
}

/// A byte range of `text` as a UTF-16 span.
fn span(text: &str, start: usize, end: usize) -> Span {
    Span {
        offset: utf16_len(&text[..start]),
        length: utf16_len(&text[start..end]),
    }
}

/// Byte offset of the char `n` chars before/after `from` (clamped).
fn back_chars(text: &str, from: usize, n: usize) -> usize {
    text[..from]
        .char_indices()
        .rev()
        .nth(n.saturating_sub(1))
        .map_or(0, |(i, _)| i)
}

fn fwd_chars(text: &str, from: usize, n: usize) -> usize {
    text[from..]
        .char_indices()
        .nth(n)
        .map_or(text.len(), |(i, _)| from + i)
}

/// An excerpt around the first match: up to `SNIPPET_BEFORE` chars before it (started at a word
/// boundary when one is near), `SNIPPET_LEN` chars in all.
fn snippet(text: &str, ranges: &[(usize, usize)]) -> Snippet {
    if text.is_empty() {
        return Snippet::default();
    }
    let first = ranges.first().map_or(0, |r| r.0);
    let mut start = if first == 0 {
        0
    } else {
        back_chars(text, first, SNIPPET_BEFORE)
    };
    if start > 0
        && let Some(ws) = text[start..first].find(char::is_whitespace)
    {
        let ws = start + ws;
        start = ws + text[ws..].chars().next().map_or(0, char::len_utf8);
    }
    let mut end = fwd_chars(text, start, SNIPPET_LEN);
    if let Some(r) = ranges.first() {
        end = end.max(r.1);
    }
    let prefix = if start > 0 { "…" } else { "" };
    let suffix = if end < text.len() { "…" } else { "" };
    let body: String = text[start..end]
        .chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .collect();
    let base = utf16_len(prefix);
    let highlights = ranges
        .iter()
        .filter(|(s, e)| *e > start && *s < end)
        .map(|&(s, e)| {
            let (s, e) = (s.max(start), e.min(end));
            Span {
                offset: base + utf16_len(&text[start..s]),
                length: utf16_len(&text[s..e]),
            }
        })
        .collect();
    Snippet {
        text: format!("{prefix}{body}{suffix}"),
        highlights,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_filter_syntax() {
        let q = parse_query(
            r#"привет "добрый вечер" 🎉 from:me FROM:@Alice in:"Family chat" media:photo,link has:file before:2026-02-01 after:2026-01-01 is:deleted http://x.y media:pdf before:2026-02-30"#,
        );
        assert_eq!(
            q.terms,
            vec![
                Term::Word("привет".into()),
                Term::Phrase("добрый вечер".into()),
                Term::Literal("🎉".into()),
                Term::Word("http://x.y".into()),
            ]
        );
        assert_eq!(q.from, vec!["me", "@Alice"]);
        assert_eq!(q.chats, vec!["Family chat"]);
        assert_eq!(
            q.media,
            vec![MediaFilter::Photo, MediaFilter::Link, MediaFilter::Document]
        );
        assert_eq!(
            q.before,
            Some(Date {
                year: 2026,
                month: 2,
                day: 1
            })
        );
        assert_eq!(
            q.after,
            Some(Date {
                year: 2026,
                month: 1,
                day: 1
            })
        );
        assert!(q.only_deleted);
        assert_eq!(q.invalid, vec!["media:pdf", "before:2026-02-30"]);
        assert_eq!(
            fts_expr(&q.terms).unwrap(),
            r#""привет"* "добрый вечер" "http://x.y"*"#
        );
        assert!(parse_query("   ").is_empty());
        assert_eq!(parse_query("from:").terms, vec![Term::Word("from".into())]);
    }

    #[test]
    fn dates_are_local_midnight() {
        let d = Date {
            year: 2026,
            month: 1,
            day: 1,
        };
        assert_eq!(d.start_millis(0), 1_767_225_600_000);
        assert_eq!(d.start_millis(180), 1_767_225_600_000 - 3 * 3_600_000);
        assert_eq!(
            Date {
                year: 2024,
                month: 2,
                day: 29
            }
            .start_millis(0),
            1_709_164_800_000
        );
    }

    #[test]
    fn snippets_cut_at_words_and_count_utf16() {
        let text = format!(
            "{} 🎉 найди меня {}",
            "слово ".repeat(20),
            "хвост ".repeat(30)
        );
        let s = text.find("найди").unwrap();
        let ranges = vec![(s, s + "найди".len())];
        let sn = snippet(&text, &ranges);
        assert!(sn.text.starts_with("…слово"), "{}", sn.text);
        assert!(sn.text.ends_with('…'));
        let h = sn.highlights[0];
        let units: Vec<u16> = sn.text.encode_utf16().collect();
        let got =
            String::from_utf16(&units[h.offset as usize..(h.offset + h.length) as usize]).unwrap();
        assert_eq!(got, "найди");
        let full = span(&text, ranges[0].0, ranges[0].1);
        let units: Vec<u16> = text.encode_utf16().collect();
        assert_eq!(
            String::from_utf16(&units[full.offset as usize..(full.offset + full.length) as usize])
                .unwrap(),
            "найди"
        );
    }
}
