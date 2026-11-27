//! Shared helpers for the ingest tests.
#![allow(dead_code)]

use ch_core::ingest::{Ingest, IngestOptions};
use ch_db::{Db, now_millis};
use ch_protocol::Event;
use serde_json::{Value, json};

mod managed;

/// An in-memory database with plugin + account `fake` (and optionally `max`).
pub fn ingest_with_accounts(accounts: &[(&str, &str)], opts: IngestOptions) -> Ingest {
    let db = Db::open_in_memory().unwrap();
    for (account_id, plugin_id) in accounts {
        if ch_db::get_plugin(db.conn(), plugin_id).unwrap().is_none() {
            ch_db::upsert_plugin(
                db.conn(),
                &ch_db::PluginRow {
                    plugin_id: (*plugin_id).to_owned(),
                    name: (*plugin_id).to_owned(),
                    version: "0.1.0".into(),
                    api_version: "1.0.0".into(),
                    install_dir: None,
                    brand_color: None,
                    icon_path: None,
                    manifest_json: None,
                    enabled: true,
                    installed: true,
                    first_seen_at: now_millis(),
                    last_seen_at: now_millis(),
                },
            )
            .unwrap();
        }
        ch_db::create_account(db.conn(), account_id, plugin_id, None, &Default::default()).unwrap();
    }
    Ingest::new(db, opts)
}

pub fn fake() -> Ingest {
    ingest_with_accounts(&[("fake", "fake")], IngestOptions::default())
}

/// Build an event from its wire name and params.
pub fn ev(name: &str, params: Value) -> Event {
    Event::from_params(name, params)
        .unwrap_or_else(|| panic!("unknown event {name}"))
        .unwrap_or_else(|e| panic!("bad params for {name}: {e}"))
}

pub fn conv(id: &str, extra: Value) -> Value {
    let mut v = json!({"id": id, "kind": "dm"});
    merge_json(&mut v, extra);
    v
}

pub fn msg(chat: &str, id: &str, text: &str, extra: Value) -> Value {
    let mut v = json!({"id": id, "chat_id": chat, "direction": "in", "sent_at": "2026-01-01T10:00:00.000Z", "text": text});
    merge_json(&mut v, extra);
    v
}

fn merge_json(dst: &mut Value, src: Value) {
    if let (Value::Object(d), Value::Object(s)) = (dst, src) {
        for (k, v) in s {
            d.insert(k, v);
        }
    }
}

/// Timestamp columns that legitimately move between two otherwise identical applications.
const VOLATILE: &[&str] = &[
    "created_at",
    "updated_at",
    "first_seen_at",
    "last_seen_at",
    "status_at",
    "last_access_at",
    "state_at",
    "last_fetch_at",
    "replaced_at",
    "local_read_at",
];

/// A canonical dump of every table (minus volatile timestamps) for state comparisons.
pub fn dump(db: &Db) -> String {
    let conn = db.conn();
    managed::assert_coherent(conn);
    let tables = ch_db::migrations::table_names(conn).unwrap();
    let mut out = String::new();
    for t in tables {
        if t.starts_with("message_fts") {
            continue;
        }
        let mut info = conn.prepare(&format!("PRAGMA table_info({t})")).unwrap();
        let cols: Vec<String> = info
            .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, String>(2)?)))
            .unwrap()
            .map(|c| c.unwrap())
            .filter(|(c, _)| !VOLATILE.contains(&c.as_str()))
            // Allocation order is volatile, but only after exact index/control
            // revisions and live consumer coherence were checked above.
            .filter(|(c, _)| !(c == "revision"
                && matches!(t.as_str(), "managed_ref_clock" | "managed_ref_row" | "managed_ref")))
            // Surrogate rowids and integer FKs depend on insertion order; compare by content.
            // Keep every other managed field, including source/message hint FKs.
            .filter(|(c, ty)| t.starts_with("managed_")
                || !(ty == "INTEGER" && (c == "id" || c.ends_with("_id"))))
            .map(|(c, _)| c)
            .collect();
        if cols.is_empty() {
            let n: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM {t}"), [], |r| r.get(0))
                .unwrap();
            out.push_str(&format!("## {t} ({n})\n"));
            continue;
        }
        let sql = format!("SELECT {} FROM {t}", cols.join(", "));
        let mut stmt = conn.prepare(&sql).unwrap();
        let mut rows: Vec<String> = stmt
            .query_map([], |r| {
                let mut parts = Vec::new();
                for (i, c) in cols.iter().enumerate() {
                    let mut v: rusqlite::types::Value = r.get(i)?;
                    if matches!(t.as_str(), "managed_ref_row" | "managed_ref") && c == "consumer_key" {
                        let kind_index = cols.iter().position(|c| c == "consumer_kind").unwrap();
                        let kind: String = r.get(kind_index)?;
                        let raw: String = r.get(i)?;
                        v = rusqlite::types::Value::Text(managed::key(conn, &kind, &raw));
                    } else if t == "managed_ref" {
                        if let rusqlite::types::Value::Integer(id) = &v {
                            if c == "message_id" {
                                v = rusqlite::types::Value::Text(managed::key(conn, "message", &id.to_string()));
                            } else if c == "source_id" {
                                v = rusqlite::types::Value::Text(managed::source_key(conn, *id));
                            }
                        }
                    }
                    parts.push(format!("{c}={v:?}"));
                }
                Ok(parts.join("|"))
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        rows.sort();
        out.push_str(&format!("## {t} ({})\n", rows.len()));
        for r in rows {
            out.push_str(&r);
            out.push('\n');
        }
    }
    // The FTS index must agree with the message table.
    let n_fts: i64 = conn
        .query_row("SELECT COUNT(*) FROM message_fts", [], |r| r.get(0))
        .unwrap();
    let n_msg: i64 = conn
        .query_row("SELECT COUNT(*) FROM message", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n_fts, n_msg, "FTS out of sync");
    out
}

/// Assert two dumps are equal, printing only the differing lines.
pub fn assert_same_dump(a: &str, b: &str, what: &str) {
    if a == b {
        return;
    }
    let (la, lb): (Vec<&str>, Vec<&str>) = (a.lines().collect(), b.lines().collect());
    let mut diff = String::new();
    for l in &la {
        if !lb.contains(l) {
            diff.push_str(&format!("- {}\n", &l[..l.len().min(400)]));
        }
    }
    for l in &lb {
        if !la.contains(l) {
            diff.push_str(&format!("+ {}\n", &l[..l.len().min(400)]));
        }
    }
    panic!("{what}: dumps differ\n{diff}");
}

pub fn count(db: &Db, sql: &str) -> i64 {
    db.conn().query_row(sql, [], |r| r.get(0)).unwrap()
}
