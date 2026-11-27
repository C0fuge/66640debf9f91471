//! Snapshot invariants for the descriptive reverse index. No filesystem proof is inferred.

use std::collections::{BTreeMap, BTreeSet};

use ch_db::rusqlite::{Connection, params};

struct Consumer {
    table: &'static str,
    key: &'static str,
    account: &'static str,
    message: &'static str,
    columns: &'static [(&'static str, bool)],
}

const CONSUMERS: &[Consumer] = &[
    Consumer { table: "message", key: "id",
        account: "(SELECT c.account_id FROM conversation c WHERE c.id=message.conversation_id)", message: "id",
        columns: &[("media_json",true),("link_preview_json",true),("ext_json",true),("reply_to_json",true),
            ("forward_from_json",true),("poll_json",true),("location_json",true),("contact_card_json",true),
            ("dice_json",true),("buttons_json",true),("action_json",true)] },
    Consumer { table: "message_version", key: "id",
        account: "(SELECT c.account_id FROM message m JOIN conversation c ON c.id=m.conversation_id WHERE m.id=message_version.message_id)",
        message: "message_id", columns: &[("media_json",true)] },
    Consumer { table: "outbox", key: "id", account: "account_id", message: "echo_message_id", columns: &[("params_json",true)] },
    Consumer { table: "media_file", key: "path", account: "account_id", message: "message_id", columns: &[("path",false)] },
    Consumer { table: "conversation", key: "id", account: "account_id", message: "NULL",
        columns: &[("avatar_path",false),("ext_json",true),("wallpaper",false)] },
    Consumer { table: "platform_handle", key: "id", account: "account_id", message: "NULL",
        columns: &[("avatar_path",false),("ext_json",true),("status_json",true)] },
    Consumer { table: "plugin", key: "plugin_id", account: "NULL", message: "NULL", columns: &[("icon_path",false)] },
];

/// Resolve TEXT-encoded surrogate keys through the actual live owner hierarchy.
/// Version content, including remote metadata, distinguishes historical consumers.
pub(super) fn key(conn: &Connection, kind: &str, raw: &str) -> String {
    let sql = match kind {
        "message" => "SELECT json_array(c.account_id,c.chat_id,m.message_id)
            FROM message m JOIN conversation c ON c.id=m.conversation_id WHERE CAST(m.id AS TEXT)=?1",
        "message_version" => "SELECT json_array(c.account_id,c.chat_id,m.message_id,v.text,v.entities_json,v.media_json,v.edited_at)
            FROM message_version v JOIN message m ON m.id=v.message_id JOIN conversation c ON c.id=m.conversation_id
            WHERE CAST(v.id AS TEXT)=?1",
        "conversation" => "SELECT json_array(account_id,chat_id) FROM conversation WHERE CAST(id AS TEXT)=?1",
        "platform_handle" => "SELECT json_array(account_id,user_id) FROM platform_handle WHERE CAST(id AS TEXT)=?1",
        "outbox" => "SELECT json_array(account_id,chat_id,topic_id,client_id) FROM outbox WHERE CAST(id AS TEXT)=?1",
        "media_file" => "SELECT json_array(path) FROM media_file WHERE path=?1",
        "plugin" => "SELECT json_array(plugin_id) FROM plugin WHERE plugin_id=?1",
        _ => panic!("unknown managed consumer {kind}"),
    };
    conn.query_row(sql, [raw], |row| row.get(0))
        .unwrap_or_else(|e| panic!("managed {kind} key {raw:?} has no live consumer: {e}"))
}

pub(super) fn source_key(conn: &Connection, id: i64) -> String {
    conn.query_row("SELECT json_array(account_id,plugin_id,account_created_at,stage_id,client_id,path)
        FROM managed_source WHERE id=?1", [id], |row| row.get(0)).unwrap()
}

/// Clock/revisions are excluded from semantic snapshots only after checking their
/// allocator and exact row relationship. Missing/stale/misowned references still fail.
pub(super) fn assert_coherent(conn: &Connection) {
    let clock: Vec<(i64,i64)> = conn.prepare("SELECT singleton,revision FROM managed_ref_clock").unwrap()
        .query_map([], |r| Ok((r.get(0)?,r.get(1)?))).unwrap()
        .collect::<ch_db::rusqlite::Result<_>>().unwrap();
    assert_eq!(clock.len(), 1, "managed revision allocator missing/duplicated");
    assert_eq!(clock[0].0, 1);
    let max: i64 = conn.query_row("SELECT coalesce(max(revision),0) FROM managed_ref_row", [], |r| r.get(0)).unwrap();
    assert!(clock[0].1 >= max && clock[0].1 >= 0, "managed revision allocator went backwards");
    let mut revisions = BTreeSet::new();
    let (mut consumers,mut references) = (0i64,0i64);
    for consumer in CONSUMERS {
        let sql = format!("SELECT CAST({} AS TEXT),{},{},{} FROM {}", consumer.key, consumer.account,
            consumer.message, consumer.columns.iter().map(|(c,_)|*c).collect::<Vec<_>>().join(","), consumer.table);
        let mut statement = conn.prepare(&sql).unwrap();
        let mut rows = statement.query([]).unwrap();
        while let Some(row) = rows.next().unwrap() {
            consumers += 1;
            let raw: String = row.get(0).unwrap();
            key(conn, consumer.table, &raw); // ownership joins must resolve, not just the integer row
            let owner: Option<String> = row.get(1).unwrap();
            let message: Option<i64> = row.get(2).unwrap();
            let (revision,state,reason): (i64,String,Option<String>) = conn.query_row(
                "SELECT revision,state,reason FROM managed_ref_row WHERE consumer_kind=?1 AND consumer_key=?2",
                params![consumer.table,raw], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
            assert!(revision > 0 && revisions.insert(revision), "invalid/reused managed control revision");
            assert_eq!(reason.is_some(), state == "blocked", "managed control reason/state mismatch");
            let mut expected: BTreeMap<(String,String),BTreeSet<String>> = BTreeMap::new();
            for (index,(column,json)) in consumer.columns.iter().enumerate() {
                let Some(value) = row.get::<_,Option<String>>(index+3).unwrap() else { continue };
                if !*json {
                    if !value.is_empty() && value.len() <= 4096 {
                        expected.entry(((*column).into(),"$scalar".into())).or_default().insert(value);
                    } else if value.len() > 4096 { assert_eq!(state,"blocked"); }
                    continue;
                }
                let valid: bool = value.len() <= 65536 && conn.query_row("SELECT json_valid(?1)", [&value], |r| r.get(0)).unwrap();
                if !valid { assert_eq!(state,"blocked"); continue; }
                let duplicate: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM json_tree(?1)
                    WHERE key IS NOT NULL GROUP BY parent,key HAVING count(*)>1)", [&value], |r| r.get(0)).unwrap();
                if duplicate { assert_eq!(state,"blocked"); }
                let mut slots = conn.prepare("SELECT fullkey,type,CAST(value AS TEXT) FROM json_tree(?1)
                    WHERE key='path' OR CAST(key AS TEXT) GLOB '*_path' ORDER BY id").unwrap();
                let mut slots = slots.query([&value]).unwrap();
                while let Some(slot) = slots.next().unwrap() {
                    let path: String = slot.get(0).unwrap();
                    let kind: String = slot.get(1).unwrap();
                    let literal: Option<String> = slot.get(2).unwrap();
                    if kind != "text" {
                        if kind != "null" { assert_eq!(state,"blocked"); }
                        continue;
                    }
                    let literal = literal.unwrap();
                    if literal.len() > 4096 || path.len() > 4096 { assert_eq!(state,"blocked"); continue; }
                    if !literal.is_empty() { expected.entry(((*column).into(),path)).or_default().insert(literal); }
                }
            }
            if raw.len() > 4096 || owner.as_ref().is_some_and(|a| a.len()>4096) {
                assert_eq!(state,"blocked"); expected.clear();
            }
            let mut actual = conn.prepare("SELECT column_name,slot,path,account_id,message_id,revision,source_id,classification
                FROM managed_ref WHERE consumer_kind=?1 AND consumer_key=?2").unwrap();
            let mut actual = actual.query(params![consumer.table,raw]).unwrap();
            while let Some(reference) = actual.next().unwrap() {
                references += 1;
                let slot: (String,String) = (reference.get(0).unwrap(),reference.get(1).unwrap());
                let literal: String = reference.get(2).unwrap();
                let values = expected.remove(&slot).unwrap_or_else(|| panic!("managed ref has no live path slot: {slot:?}"));
                assert!(values.contains(&literal), "managed ref path differs from its live consumer");
                assert_eq!(reference.get::<_,Option<String>>(3).unwrap(),owner,"managed ref owner hint drifted");
                assert_eq!(reference.get::<_,Option<i64>>(4).unwrap(),message,"managed ref message hint drifted");
                assert_eq!(reference.get::<_,i64>(5).unwrap(),revision,"managed ref revision differs from control");
                if let Some(source) = reference.get::<_,Option<i64>>(6).unwrap() {
                    source_key(conn,source); // retained source identity must exist
                    if reference.get::<_,String>(7).unwrap() == "unresolved" {
                        let source_path: String = conn.query_row("SELECT path FROM managed_source WHERE id=?1", [source], |r| r.get(0)).unwrap();
                        assert_eq!(source_path,literal,"unresolved literal ref points at another source");
                    }
                }
            }
            assert!(expected.is_empty(), "managed index omitted live path slots: {expected:?}");
        }
    }
    let control_count: i64 = conn.query_row("SELECT count(*) FROM managed_ref_row", [], |r| r.get(0)).unwrap();
    let reference_count: i64 = conn.query_row("SELECT count(*) FROM managed_ref", [], |r| r.get(0)).unwrap();
    assert_eq!(consumers,control_count,"managed control rows are missing or orphaned");
    assert_eq!(references,reference_count,"managed refs have no visited live consumer");
}
