//! Search uses fresh in-memory or temporary on-disk databases; no real accounts or paths.
use ch_core::search::{Scope, SearchRequest, Span, search};
use ch_db::Db;
use rusqlite::params;

const BASE: i64 = 1_767_225_600_000;

fn database() -> Db {
    initialize(Db::open_in_memory().unwrap())
}

fn initialize(db: Db) -> Db {
    db.conn().execute_batch(
        "INSERT INTO plugin(plugin_id,name,first_seen_at,last_seen_at) VALUES('fake','Fake',0,0);
         INSERT INTO account(account_id,plugin_id,created_at,updated_at) VALUES('a','fake',0,0),('b','fake',0,0);
         INSERT INTO conversation(id,account_id,chat_id,kind,title,username,created_at,updated_at)
         VALUES(1,'a','chat','group','Семья','family',0,0),(2,'b','chat','group','Other','other',0,0);"
    ).unwrap();
    db
}

fn insert(db: &Db, conversation: i64, id: &str, text: &str, sent_at: i64) {
    db.conn().execute(
        "INSERT INTO message(conversation_id,message_id,direction,sent_at,text,created_at,updated_at)
         VALUES(?1,?2,'in',?3,?4,0,0)", params![conversation, id, sent_at, text],
    ).unwrap();
}

fn find(db: &Db, query: &str) -> ch_core::search::SearchPage {
    search(
        db.conn(),
        &SearchRequest {
            query: query.into(),
            ..Default::default()
        },
    )
    .unwrap()
}

fn extract(text: &str, span: Span) -> String {
    let units: Vec<u16> = text.encode_utf16().collect();
    String::from_utf16(&units[span.offset as usize..(span.offset + span.length) as usize]).unwrap()
}

#[test]
fn cyrillic_and_emoji_highlights_use_utf16_and_fts_prefixes() {
    let db = database();
    let text = "😀 Привет, мир! 🎉";
    insert(&db, 1, "one", text, BASE);
    let page = find(&db, "прив 🎉");
    assert_eq!(page.hits.len(), 1);
    let h = &page.hits[0];
    let parts: Vec<_> = h.highlights.iter().map(|&s| extract(text, s)).collect();
    assert_eq!(parts, ["Привет", "🎉"]);
    assert_eq!(h.highlights[0].offset, 3); // non-BMP emoji occupies two UTF-16 units
    for &span in &h.snippet.highlights {
        assert!(!extract(&h.snippet.text, span).is_empty());
    }
    assert_eq!(find(&db, "🎉").hits.len(), 1);
}

#[test]
fn private_use_text_does_not_corrupt_highlight_boundaries() {
    let db = database();
    let text = "\u{E000}open-0\u{E001} \u{E002}close-0\u{E003} \u{E001} Привет \u{E002}";
    insert(&db, 1, "one", text, BASE);
    let page = find(&db, "прив");
    assert_eq!(extract(text, page.hits[0].highlights[0]), "Привет");
}

#[test]
fn filters_combine_without_crossing_accounts_or_including_deleted_or_scheduled() {
    let db = database();
    for (c, id) in [
        (1, "valid"),
        (1, "deleted"),
        (1, "scheduled"),
        (2, "other-account"),
    ] {
        insert(&db, c, id, "Привет", BASE);
    }
    db.conn()
        .execute_batch(
            "UPDATE message SET direction='out',media_kind='photo';
         UPDATE message SET deleted_at=1 WHERE message_id='deleted';
         UPDATE message SET is_scheduled=1 WHERE message_id='scheduled';",
        )
        .unwrap();
    let req = SearchRequest {
        query: "прив from:me in:@family media:photo after:2026-01-01 before:2026-01-02".into(),
        scope: Scope::Global {
            account_id: Some("a".into()),
        },
        ..Default::default()
    };
    let page = search(db.conn(), &req).unwrap();
    assert_eq!(
        page.hits
            .iter()
            .map(|h| h.message_id.as_str())
            .collect::<Vec<_>>(),
        ["valid"]
    );
    assert_eq!(find(&db, "is:deleted").hits[0].message_id, "deleted");
    assert_eq!(find(&db, "in:семья").hits.len(), 1);
    assert_eq!(find(&db, "media:photo").hits.len(), 2);
    assert!(find(&db, "прив from:@missing").hits.is_empty());
}

#[test]
fn pagination_orders_equal_timestamps_by_rowid_and_limits_are_bounded() {
    let db = database();
    for i in 0..205 {
        insert(&db, 1, &i.to_string(), "needle", BASE);
    }
    let mut req = SearchRequest {
        query: "needle".into(),
        limit: 2,
        ..Default::default()
    };
    let page = search(db.conn(), &req).unwrap();
    assert_eq!(
        page.hits
            .iter()
            .map(|h| h.message_id.as_str())
            .collect::<Vec<_>>(),
        ["204", "203"]
    );
    req.offset = page.next_offset.unwrap();
    assert_eq!(search(db.conn(), &req).unwrap().hits[0].message_id, "202");
    req.offset = 0;
    req.limit = u32::MAX;
    let page = search(db.conn(), &req).unwrap();
    assert_eq!(page.hits.len(), 200);
    assert_eq!(page.next_offset, Some(200));
    req.offset = u32::MAX;
    assert!(search(db.conn(), &req).unwrap().hits.is_empty());
}

#[test]
fn quoted_search_is_literal_fts_syntax_and_sql_injection_stays_text() {
    let db = database();
    insert(&db, 1, "one", "alpha beta café", BASE);
    assert_eq!(find(&db, "\"alpha beta\" cafe").hits.len(), 1);
    assert!(find(&db, "\"beta alpha\"").hits.is_empty());
    assert!(find(&db, "\" OR 1=1 --").hits.is_empty());
    assert_eq!(find(&db, "alpha").hits.len(), 1);
}

#[test]
fn sender_lookup_preserves_unicode_case_and_account_boundaries() {
    let db = database();
    insert(&db, 1, "one", "needle", BASE);
    insert(&db, 2, "two", "needle", BASE);
    db.conn().execute_batch(
        "INSERT INTO platform_handle(account_id,user_id,username,display_name,first_seen_at,last_seen_at)
         VALUES('a','same-id','alice','Алиса',0,0),('b','same-id','someone','Другой',0,0);
         UPDATE message SET sender_id='same-id';"
    ).unwrap();
    for filter in ["from:АЛИСА", "from:@Alice"] {
        let hits = find(&db, &format!("needle {filter}")).hits;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].account_id, "a");
    }
}

#[tokio::test]
async fn search_and_history_read_the_running_cores_database() {
    use ch_core::history::{ChatKey, History, HistoryConfig};
    use ch_core::host::MemorySecretStore;
    use ch_core::runtime::{Core, CoreConfig};
    use std::sync::Arc;
    let temp = tempfile::tempdir().unwrap();
    let mut cfg = CoreConfig::new(temp.path(), Arc::new(MemorySecretStore::new()));
    cfg.autostart = false;
    let core = Core::open(cfg).await.unwrap();
    let db = core.open_db().unwrap();
    db.conn().execute_batch(
        "INSERT INTO plugin(plugin_id,name,first_seen_at,last_seen_at) VALUES('fake','Fake',0,0);
         INSERT INTO account(account_id,plugin_id,created_at,updated_at) VALUES('a','fake',0,0);
         INSERT INTO conversation(id,account_id,chat_id,kind,title,created_at,updated_at)
         VALUES(1,'a','chat','group','Test',0,0);"
    ).unwrap();
    insert(&db, 1, "one", "searchable", BASE);
    let search = ch_core::search::Search::from_core(&core).unwrap();
    assert_eq!(
        search
            .search(&SearchRequest {
                query: "searchable".into(),
                ..Default::default()
            })
            .unwrap()
            .hits
            .len(),
        1
    );
    let history = History::from_core(&core, HistoryConfig::default());
    assert_eq!(
        history
            .open(&ChatKey::new("a", "chat"), 50)
            .await
            .unwrap()
            .message_ids(),
        ["one"]
    );
    core.shutdown().await;
}

#[test]
fn search_respects_the_callers_transaction_and_does_not_commit_it() {
    let db = database();
    let tx = db.transaction().unwrap();
    tx.execute(
        "INSERT INTO message(conversation_id,message_id,direction,sent_at,text,created_at,updated_at)
         VALUES(1,'pending','in',0,'pending transaction',0,0)", [],
    ).unwrap();
    assert_eq!(
        search(
            &tx,
            &SearchRequest {
                query: "pending".into(),
                ..Default::default()
            }
        )
        .unwrap()
        .hits
        .len(),
        1
    );
    assert!(!tx.is_autocommit());
    tx.rollback().unwrap();
    assert!(db.conn().is_autocommit());
    assert!(find(&db, "pending").hits.is_empty());
}

#[test]
fn unicode_edits_on_another_wal_connection_cannot_mix_text_and_highlight_snapshots() {
    use std::sync::{Arc, Barrier};
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("search-race.db");
    let db = initialize(Db::open(&path).unwrap());
    for n in 0..24 {
        insert(&db, 1, &n.to_string(), "😀😀 alpha русский", BASE);
    }
    let barrier = Arc::new(Barrier::new(2));
    let start = barrier.clone();
    let writer = std::thread::spawn(move || {
        let db = Db::open(path).unwrap();
        start.wait();
        for i in 0..256 {
            // Swapping ASCII and non-BMP prefixes changes both UTF-8 byte and UTF-16 offsets.
            let text = if i % 2 == 0 {
                "alpha"
            } else {
                "😀😀 alpha русский"
            };
            db.conn()
                .execute("UPDATE message SET text=?1", [text])
                .unwrap();
            std::thread::yield_now();
        }
    });
    barrier.wait();
    for _ in 0..128 {
        let page = find(&db, "alpha");
        assert_eq!(page.hits.len(), 24);
        for hit in page.hits {
            assert_eq!(hit.snippet.highlights.len(), 1);
            assert_eq!(
                extract(&hit.snippet.text, hit.snippet.highlights[0]),
                "alpha"
            );
        }
    }
    writer.join().unwrap();
}
