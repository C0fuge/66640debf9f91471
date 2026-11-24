//! The typed query layer: keys, cascades, FTS triggers, reactions, drafts, settings, outbox.

use ch_db::*;
use ch_protocol::dto::{Entity, EntityType, Media, MediaKind, Reaction, ReactionKey};

fn db_with_account() -> Db {
    let db = Db::open_in_memory().unwrap();
    let now = now_millis();
    upsert_plugin(
        db.conn(),
        &PluginRow {
            plugin_id: "fake".into(),
            name: "Fake".into(),
            version: "0.1.0".into(),
            api_version: "1.0.0".into(),
            install_dir: None,
            brand_color: Some("#8E44AD".into()),
            icon_path: None,
            manifest_json: None,
            enabled: true,
            installed: true,
            first_seen_at: now,
            last_seen_at: now,
        },
    )
    .unwrap();
    create_account(db.conn(), "fake", "fake", Some("Fake"), &Default::default()).unwrap();
    db
}

fn conv(db: &Db, chat_id: &str) -> i64 {
    let now = now_millis();
    insert_conversation(
        db.conn(),
        &ConversationRow {
            account_id: "fake".into(),
            chat_id: chat_id.into(),
            kind: "dm".into(),
            title: Some(format!("Chat {chat_id}")),
            created_at: now,
            updated_at: now,
            ..Default::default()
        },
    )
    .unwrap()
}

fn msg(db: &Db, conversation_id: i64, id: &str, text: &str, sent_at: Millis) -> i64 {
    let now = now_millis();
    insert_message(
        db.conn(),
        &MessageRow {
            conversation_id,
            message_id: id.into(),
            direction: "in".into(),
            sent_at,
            sender_id: Some("u1".into()),
            text: Some(text.into()),
            created_at: now,
            updated_at: now,
            ..Default::default()
        },
    )
    .unwrap()
}

#[test]
fn wal_writer_can_write_after_reading_while_another_writer_contends() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hub.db");
    let first = Db::open(&path).unwrap();
    let second = Db::open(&path).unwrap();
    // Probe contention synchronously while the first connection holds the transaction.
    second
        .conn()
        .busy_timeout(std::time::Duration::ZERO)
        .unwrap();

    let tx = first.transaction().unwrap();
    assert_eq!(get_setting::<u8>(&tx, "value").unwrap(), None);
    let competing_write = set_setting(second.conn(), "value", &2);
    set_setting(&tx, "value", &1).unwrap();
    tx.commit().unwrap();

    assert!(matches!(
        competing_write,
        Err(Error::Sqlite(rusqlite::Error::SqliteFailure(e, _)))
            if e.code == rusqlite::ErrorCode::DatabaseBusy
    ));
    assert_eq!(get_setting::<u8>(second.conn(), "value").unwrap(), Some(1));
    set_setting(second.conn(), "value", &2).unwrap();
    assert_eq!(get_setting::<u8>(first.conn(), "value").unwrap(), Some(2));
}

#[test]
fn accounts_and_settings_roundtrip() {
    let db = db_with_account();
    let a = get_account(db.conn(), "fake").unwrap().unwrap();
    assert_eq!(a.plugin_id, "fake");
    assert_eq!(a.state, "disconnected");
    set_account_state(db.conn(), "fake", "connected", None, false, 1).unwrap();
    assert_eq!(
        get_account(db.conn(), "fake").unwrap().unwrap().state,
        "connected"
    );
    assert!(create_account(db.conn(), "fake", "fake", None, &Default::default()).is_err());

    set_setting(db.conn(), "ui.theme", &"dark").unwrap();
    set_setting(db.conn(), "ui.scale", &1.25f64).unwrap();
    assert_eq!(
        get_setting::<String>(db.conn(), "ui.theme")
            .unwrap()
            .unwrap(),
        "dark"
    );
    assert_eq!(
        get_setting::<f64>(db.conn(), "ui.scale").unwrap().unwrap(),
        1.25
    );
    assert_eq!(list_settings(db.conn()).unwrap().len(), 2);
    assert!(delete_setting(db.conn(), "ui.theme").unwrap());
    assert!(
        get_setting::<String>(db.conn(), "ui.theme")
            .unwrap()
            .is_none()
    );
}

#[test]
fn messages_fts_and_tombstones() {
    let db = db_with_account();
    let c = conv(&db, "c1");
    let m1 = msg(&db, c, "1", "Привет, как дела? Встретимся завтра", 1000);
    let _m2 = msg(&db, c, "2", "hello world, meeting tomorrow", 2000);
    assert_eq!(fts_count(db.conn()).unwrap(), 2);

    let hits = search_messages(
        db.conn(),
        &SearchQuery {
            text: "встрет".into(),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].message_id, "1");
    assert!(
        hits[0].snippet.contains("[Встретимся]"),
        "{}",
        hits[0].snippet
    );

    // The update trigger re-indexes on text change.
    let mut row = get_message_by_id(db.conn(), m1).unwrap().unwrap();
    row.text = Some("completely different".into());
    update_message(db.conn(), &row).unwrap();
    let hits = search_messages(
        db.conn(),
        &SearchQuery {
            text: "встрет".into(),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(hits.is_empty());
    let hits = search_messages(
        db.conn(),
        &SearchQuery {
            text: "complet".into(),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(hits.len(), 1);

    // A tombstone stays searchable only with include_deleted.
    assert!(tombstone_message(db.conn(), m1, "platform", 5000).unwrap());
    assert!(!tombstone_message(db.conn(), m1, "platform", 6000).unwrap()); // already
    let q = SearchQuery {
        text: "complet".into(),
        ..Default::default()
    };
    assert!(search_messages(db.conn(), &q).unwrap().is_empty());
    let q = SearchQuery {
        include_deleted: true,
        ..q
    };
    assert_eq!(search_messages(db.conn(), &q).unwrap().len(), 1);
    assert_eq!(count_messages(db.conn(), c).unwrap(), 1);
    assert_eq!(
        list_messages(db.conn(), c, None, None, 10, false)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        list_messages(db.conn(), c, None, None, 10, true)
            .unwrap()
            .len(),
        2
    );

    // Filters: from:, media:, before:/after:
    let now = now_millis();
    insert_message(
        db.conn(),
        &MessageRow {
            conversation_id: c,
            message_id: "3".into(),
            direction: "out".into(),
            sent_at: 3000,
            sender_id: Some("me".into()),
            text: Some("photo caption tomorrow".into()),
            media: Some(Media {
                kind: MediaKind::Photo,
                mime: None,
                bytes: None,
                filename: None,
                width: None,
                height: None,
                duration: None,
                thumb_path: None,
                path: None,
                waveform: None,
                title: None,
                performer: None,
                has_spoiler: false,
                sticker: None,
            }),
            entities: Some(vec![Entity {
                kind: EntityType::Url,
                offset: 0,
                length: 5,
                url: None,
                user_id: None,
                language: None,
                custom_emoji_id: None,
                collapsed: None,
            }]),
            created_at: now,
            updated_at: now,
            ..Default::default()
        },
    )
    .unwrap();
    let q = SearchQuery {
        text: "tomorrow".into(),
        ..Default::default()
    };
    assert_eq!(search_messages(db.conn(), &q).unwrap().len(), 2);
    let q = SearchQuery {
        sender_id: Some("me".into()),
        ..q.clone()
    };
    assert_eq!(search_messages(db.conn(), &q).unwrap().len(), 1);
    let q = SearchQuery {
        media: vec!["photo".into()],
        ..SearchQuery {
            text: "tomorrow".into(),
            ..Default::default()
        }
    };
    assert_eq!(search_messages(db.conn(), &q).unwrap()[0].message_id, "3");
    let q = SearchQuery {
        media: vec!["link".into()],
        text: String::new(),
        ..Default::default()
    };
    assert_eq!(search_messages(db.conn(), &q).unwrap().len(), 1);
    let q = SearchQuery {
        text: "tomorrow".into(),
        before: Some(2500),
        ..Default::default()
    };
    assert_eq!(search_messages(db.conn(), &q).unwrap()[0].message_id, "2");
    let q = SearchQuery {
        text: "tomorrow".into(),
        after: Some(2500),
        ..Default::default()
    };
    assert_eq!(search_messages(db.conn(), &q).unwrap()[0].message_id, "3");

    // Deleting the account cascades to everything, FTS included.
    assert!(delete_account(db.conn(), "fake").unwrap());
    assert_eq!(fts_count(db.conn()).unwrap(), 0);
    let n: i64 = db
        .conn()
        .query_row("SELECT COUNT(*) FROM message", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0);
}

#[test]
fn fts_query_builder() {
    assert_eq!(fts_query("hello world").unwrap(), "\"hello\"* \"world\"*");
    assert_eq!(
        fts_query("  \"exact phrase\" tail ").unwrap(),
        "\"exact phrase\" \"tail\"*"
    );
    assert_eq!(fts_query("\"\"  ... "), None);
    assert_eq!(fts_query("c++ (test)").unwrap(), "\"c\"* \"test\"*");
}

#[test]
fn reactions_replace_wholesale_and_report_change() {
    let db = db_with_account();
    let c = conv(&db, "c1");
    let m = msg(&db, c, "1", "x", 1);
    let r1 = vec![Reaction {
        key: ReactionKey::emoji("❤"),
        count: 2,
        mine: true,
        my_count: None,
    }];
    assert!(set_reactions(db.conn(), m, &r1).unwrap());
    assert!(
        !set_reactions(db.conn(), m, &r1).unwrap(),
        "same set is a no-op"
    );
    let r2 = vec![
        Reaction {
            key: ReactionKey::emoji("❤"),
            count: 1,
            mine: false,
            my_count: None,
        },
        Reaction {
            key: ReactionKey::paid(),
            count: 5,
            mine: true,
            my_count: Some(5),
        },
    ];
    assert!(set_reactions(db.conn(), m, &r2).unwrap());
    assert_eq!(reactions_dto(db.conn(), m).unwrap(), r2);
    assert!(set_reactions(db.conn(), m, &[]).unwrap());
    assert!(list_reactions(db.conn(), m).unwrap().is_empty());
}

#[test]
fn drafts_topics_and_versions() {
    let db = db_with_account();
    let c = conv(&db, "c1");
    set_draft(
        db.conn(),
        &DraftRow {
            conversation_id: c,
            topic_id: String::new(),
            text: "hi".into(),
            updated_at: 10,
            ..Default::default()
        },
    )
    .unwrap();
    set_draft(
        db.conn(),
        &DraftRow {
            conversation_id: c,
            topic_id: "t1".into(),
            text: "topic draft".into(),
            updated_at: 11,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(list_drafts(db.conn(), c).unwrap().len(), 2);
    set_draft(
        db.conn(),
        &DraftRow {
            conversation_id: c,
            topic_id: String::new(),
            text: String::new(),
            updated_at: 12,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(get_draft(db.conn(), c, "").unwrap().is_none());
    assert_eq!(
        get_draft(db.conn(), c, "t1").unwrap().unwrap().text,
        "topic draft"
    );

    let now = now_millis();
    let t = insert_topic(
        db.conn(),
        &TopicRow {
            conversation_id: c,
            topic_id: "t1".into(),
            title: "General".into(),
            created_at: now,
            updated_at: now,
            ..Default::default()
        },
    )
    .unwrap();
    insert_topic(
        db.conn(),
        &TopicRow {
            conversation_id: c,
            topic_id: "t2".into(),
            title: "Other".into(),
            created_at: now,
            updated_at: now,
            ..Default::default()
        },
    )
    .unwrap();
    let gone = mark_topics_synced(db.conn(), c, &["t1".into()], now).unwrap();
    assert_eq!(gone, vec!["t2".to_owned()]);
    assert_eq!(list_topics(db.conn(), c).unwrap().len(), 1);
    assert_eq!(get_topic(db.conn(), c, "t1").unwrap().unwrap().id, t);

    let m = msg(&db, c, "1", "v1", 1);
    let row = get_message_by_id(db.conn(), m).unwrap().unwrap();
    insert_message_version(db.conn(), &row, 100).unwrap();
    let vs = list_message_versions(db.conn(), m).unwrap();
    assert_eq!(vs.len(), 1);
    assert_eq!(vs[0].text.as_deref(), Some("v1"));
    assert!(delete_message(db.conn(), m).unwrap());
    assert!(
        list_message_versions(db.conn(), m).unwrap().is_empty(),
        "cascade"
    );
}

#[test]
fn conversations_synced_marks_gone_and_back() {
    let db = db_with_account();
    conv(&db, "a");
    conv(&db, "b");
    conv(&db, "c");
    let (gone, back) =
        mark_conversations_synced(db.conn(), "fake", &["a".into(), "c".into()], 1).unwrap();
    assert_eq!(gone, vec!["b".to_owned()]);
    assert!(back.is_empty());
    assert_eq!(
        list_conversations(db.conn(), "fake", None).unwrap().len(),
        2
    );
    let (gone, back) =
        mark_conversations_synced(db.conn(), "fake", &["a".into(), "b".into()], 2).unwrap();
    assert_eq!(gone, vec!["c".to_owned()]);
    assert_eq!(back, vec!["b".to_owned()]);
    // Folder membership from both directions.
    upsert_folder(
        db.conn(),
        &FolderRow {
            account_id: "fake".into(),
            folder_id: "f1".into(),
            title: "Work".into(),
            updated_at: 1,
            ..Default::default()
        },
    )
    .unwrap();
    set_folder_chats(db.conn(), "fake", "f1", &[("fake".into(), "a".into())]).unwrap();
    set_chat_platform_folders(db.conn(), "fake", "b", &["f1".into(), "unknown".into()]).unwrap();
    assert_eq!(
        folder_chats(db.conn(), "fake", "f1").unwrap().len(),
        1,
        "the folder's own list"
    );
    assert_eq!(
        folder_members(db.conn(), "fake", "f1").unwrap().len(),
        2,
        "plus the chat's folder_ids"
    );
    assert_eq!(
        chat_folder_ids(db.conn(), "fake", "b").unwrap(),
        vec!["f1".to_owned()],
        "unknown folders are kept but not listed"
    );
    assert_eq!(
        collect_chat_folder_memberships(db.conn(), "fake", "b").unwrap(),
        vec!["f1".to_owned(), "unknown".to_owned()]
    );
    assert_eq!(delete_folders_not_in(db.conn(), "fake", &[]).unwrap(), 1);
    assert!(folder_members(db.conn(), "fake", "f1").unwrap().is_empty());
}

#[test]
fn identity_linking_queries() {
    let db = db_with_account();
    upsert_plugin(
        db.conn(),
        &PluginRow {
            plugin_id: "max".into(),
            name: "MAX".into(),
            version: String::new(),
            api_version: String::new(),
            install_dir: None,
            brand_color: None,
            icon_path: None,
            manifest_json: None,
            enabled: true,
            installed: true,
            first_seen_at: 0,
            last_seen_at: 0,
        },
    )
    .unwrap();
    create_account(db.conn(), "max", "max", None, &Default::default()).unwrap();
    let p = create_person(db.conn(), Some("Alice")).unwrap();
    let h1 = insert_handle(
        db.conn(),
        &HandleRow {
            account_id: "fake".into(),
            user_id: "u1".into(),
            person_id: Some(p),
            phone: Some("+491".into()),
            username: Some("Alice_x".into()),
            ..Default::default()
        },
    )
    .unwrap();
    let h2 = insert_handle(
        db.conn(),
        &HandleRow {
            account_id: "max".into(),
            user_id: "9".into(),
            phone: Some("+491".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        handles_by_phone(db.conn(), "+491", "max").unwrap()[0].id,
        h1
    );
    assert_eq!(
        handles_by_username(db.conn(), "alice_X", "max").unwrap()[0].id,
        h1
    );
    assert_eq!(unlinked_handles(db.conn()).unwrap().len(), 1);
    link_handle(db.conn(), h2, Some(p), Some("auto_phone")).unwrap();
    assert_eq!(handles_of_person(db.conn(), p).unwrap().len(), 2);
    let cat = create_category(db.conn(), "friends", Some("#0f0"), 0).unwrap();
    set_person_category(db.conn(), p, cat, true).unwrap();
    assert_eq!(person_categories(db.conn(), p).unwrap(), vec![cat]);
    // Deleting the account unlinks nothing wrongly: the person and the other handle survive.
    delete_account(db.conn(), "max").unwrap();
    assert_eq!(handles_of_person(db.conn(), p).unwrap().len(), 1);
    assert!(get_person(db.conn(), p).unwrap().is_some());
}

#[test]
fn outbox_and_history_rows() {
    let db = db_with_account();
    let c = conv(&db, "c1");
    let id = insert_outbox(
        db.conn(),
        &OutboxRow {
            account_id: "fake".into(),
            chat_id: "c1".into(),
            client_id: "cid-1".into(),
            kind: "send_text".into(),
            method: "messages.send".into(),
            params: serde_json::json!({"text": "hi"}),
            created_at: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(list_outbox_due(db.conn(), "fake", 0).unwrap().len(), 1);
    set_outbox_state(db.conn(), id, outbox_state::SENDING, None, None, Some("7")).unwrap();
    assert!(list_outbox_due(db.conn(), "fake", 0).unwrap().is_empty());
    set_outbox_state(
        db.conn(),
        id,
        outbox_state::FAILED,
        Some(&serde_json::json!({"code": "NETWORK"})),
        Some(100),
        None,
    )
    .unwrap();
    assert!(list_outbox_due(db.conn(), "fake", 50).unwrap().is_empty());
    assert_eq!(list_outbox_due(db.conn(), "fake", 100).unwrap().len(), 1);
    set_outbox_sent(db.conn(), id, &["42".into()]).unwrap();
    let o = get_outbox_by_client_id(db.conn(), "cid-1")
        .unwrap()
        .unwrap();
    assert_eq!(o.state, "sent");
    assert_eq!(o.attempts, 1);
    assert_eq!(o.result_message_ids, Some(vec!["42".to_owned()]));
    assert!(o.last_error.is_some());

    set_history_state(
        db.conn(),
        &HistoryStateRow {
            conversation_id: c,
            topic_id: String::new(),
            older_cursor: Some("p2".into()),
            updated_at: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        get_history_state(db.conn(), c, "")
            .unwrap()
            .unwrap()
            .older_cursor
            .as_deref(),
        Some("p2")
    );
    let g = insert_history_gap(
        db.conn(),
        &HistoryGapRow {
            conversation_id: c,
            after_message_id: "5".into(),
            created_at: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(list_history_gaps(db.conn(), c, "").unwrap().len(), 1);
    assert!(delete_history_gap(db.conn(), g).unwrap());
    insert_presence_log(
        db.conn(),
        &PresenceLogRow {
            account_id: "fake".into(),
            user_id: "u1".into(),
            at: 5,
            kind: "online".into(),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        list_presence_log(db.conn(), "fake", "u1", 10)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(prune_presence_log(db.conn(), 10).unwrap(), 1);
}
