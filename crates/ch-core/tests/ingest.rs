//! Ingest semantics: merge rules, edit history, idempotency, echoes, deletions, read state,
//! drafts, identity linking, reactions, topics, folders.

mod common;

use ch_core::ingest::{Change, ChatFlags, IngestError, IngestOptions, LocalEcho};
use ch_db::SearchQuery;
use ch_protocol::dto::{Draft, ReactionKey};
use common::*;
use serde_json::json;

#[test]
fn conversation_merge_absent_means_unchanged_and_defaults_on_create() {
    let ing = fake();
    let a = ing
        .apply("fake", ev("conversations.upsert", json!({"account_id": "fake", "conversations": [
            conv("c1", json!({"title": "Alice", "kind": "dm", "unread_count": 2, "pinned": true, "folder_ids": ["f1"]}))
        ]})))
        .unwrap();
    assert_eq!(
        a.changes,
        vec![Change::ConversationUpserted {
            chat_id: "c1".into(),
            created: true
        }]
    );
    let row = ch_db::get_conversation(ing.db().conn(), "fake", "c1")
        .unwrap()
        .unwrap();
    assert_eq!(row.title.as_deref(), Some("Alice"));
    assert!(!row.archived, "on-create default");
    assert_eq!(row.unread_count, 2);
    assert_eq!(row.muted, None, "unknown stays unknown");

    // Partial update: only the unread count moves.
    let before = dump(ing.db());
    ing.apply("fake", ev("conversations.upsert", json!({"account_id": "fake", "conversations": [conv("c1", json!({"unread_count": 0}))]}))).unwrap();
    let row = ch_db::get_conversation(ing.db().conn(), "fake", "c1")
        .unwrap()
        .unwrap();
    assert_eq!(row.unread_count, 0);
    assert_eq!(row.title.as_deref(), Some("Alice"));
    assert_eq!(row.pinned, Some(true));
    assert_ne!(dump(ing.db()), before);

    // The same event again is a no-op.
    let before = dump(ing.db());
    let a = ing.apply("fake", ev("conversations.upsert", json!({"account_id": "fake", "conversations": [conv("c1", json!({"unread_count": 0}))]}))).unwrap();
    assert!(a.is_noop(), "{a:?}");
    assert_eq!(dump(ing.db()), before);

    // Draft tri-state: absent leaves, empty clears.
    ing.apply("fake", ev("conversations.upsert", json!({"account_id": "fake", "conversations": [conv("c1", json!({"draft": {"text": "typing…"}}))]}))).unwrap();
    assert_eq!(
        ch_db::get_draft(ing.db().conn(), row.id, "")
            .unwrap()
            .unwrap()
            .text,
        "typing…"
    );
    ing.apply("fake", ev("conversations.upsert", json!({"account_id": "fake", "conversations": [conv("c1", json!({"title": "Alice B"}))]}))).unwrap();
    assert!(
        ch_db::get_draft(ing.db().conn(), row.id, "")
            .unwrap()
            .is_some(),
        "absent draft leaves ours"
    );
    ing.apply("fake", ev("conversations.upsert", json!({"account_id": "fake", "conversations": [conv("c1", json!({"draft": {"text": ""}}))]}))).unwrap();
    assert!(
        ch_db::get_draft(ing.db().conn(), row.id, "")
            .unwrap()
            .is_none()
    );
}

#[test]
fn message_edit_snapshots_only_real_content_changes() {
    let ing = fake();
    let db = ing.db();
    ing.apply("fake", ev("messages.upsert", json!({"account_id": "fake", "messages": [
        msg("c1", "1", "first", json!({"media": {"kind": "photo", "thumb_path": "/t/1.jpg"}}))
    ]}))).unwrap();
    // The chat was unknown: a stub conversation exists (kind unknown) and the preview is set.
    let c = ch_db::get_conversation(db.conn(), "fake", "c1")
        .unwrap()
        .unwrap();
    assert_eq!(c.kind, "unknown");
    assert_eq!(c.last_message_preview.as_deref(), Some("first"));
    assert_eq!(c.last_message_kind.as_deref(), Some("photo"));
    let m = ch_db::get_message(db.conn(), c.id, "1").unwrap().unwrap();
    assert!(!m.edited);

    // Same message again: nothing happens.
    let before = dump(db);
    let a = ing.apply("fake", ev("messages.upsert", json!({"account_id": "fake", "messages": [msg("c1", "1", "first", json!({"media": {"kind": "photo", "thumb_path": "/t/1.jpg"}}))]}))).unwrap();
    assert!(a.is_noop(), "{a:?}");
    assert_eq!(dump(db), before);

    // A real edit: one version with the old content.
    let a = ing.apply("fake", ev("message.edited", json!({"account_id": "fake", "message": msg("c1", "1", "second", json!({"edited": true, "edited_at": "2026-01-01T11:00:00.000Z"}))}))).unwrap();
    assert!(
        a.changes.contains(&Change::MessageEdited {
            chat_id: "c1".into(),
            message_id: "1".into(),
            snapshot: true
        }),
        "{a:?}"
    );
    let versions = ch_db::list_message_versions(db.conn(), m.id).unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].text.as_deref(), Some("first"));
    let m2 = ch_db::get_message(db.conn(), c.id, "1").unwrap().unwrap();
    assert_eq!(m2.text.as_deref(), Some("second"));
    assert!(m2.edited);
    assert_eq!(
        m2.media.as_ref().unwrap().thumb_path.as_deref(),
        Some("/t/1.jpg"),
        "media untouched by a text edit"
    );

    // The echo of the same edit: no new version, no change.
    let before = dump(db);
    let a = ing.apply("fake", ev("message.edited", json!({"account_id": "fake", "message": msg("c1", "1", "second", json!({"edited": true, "edited_at": "2026-01-01T11:00:00.000Z"}))}))).unwrap();
    assert!(a.is_noop(), "{a:?}");
    assert_eq!(dump(db), before);

    // A stale edit (older edited_at) does not clobber the newer text and makes no version.
    ing.apply("fake", ev("message.edited", json!({"account_id": "fake", "message": msg("c1", "1", "first", json!({"edited": true, "edited_at": "2026-01-01T10:30:00.000Z"}))}))).unwrap();
    assert_eq!(
        ch_db::get_message(db.conn(), c.id, "1")
            .unwrap()
            .unwrap()
            .text
            .as_deref(),
        Some("second")
    );
    assert_eq!(
        ch_db::list_message_versions(db.conn(), m.id).unwrap().len(),
        1
    );

    // A re-fetch where only the media path appeared: no version, path recorded.
    ing.apply("fake", ev("messages.upsert", json!({"account_id": "fake", "messages": [msg("c1", "1", "second", json!({"media": {"kind": "photo", "thumb_path": "/t/1.jpg", "path": "/f/1.png"}}))]}))).unwrap();
    assert_eq!(
        ch_db::list_message_versions(db.conn(), m.id).unwrap().len(),
        1
    );
    assert_eq!(count(db, "SELECT COUNT(*) FROM media_file"), 2);
    // media.ready updates paths; a re-fetch without paths keeps them.
    ing.apply(
        "fake",
        ev(
            "media.ready",
            json!({"account_id": "fake", "chat_id": "c1", "message_id": "1", "path": "/f/1b.png"}),
        ),
    )
    .unwrap();
    ing.apply("fake", ev("messages.upsert", json!({"account_id": "fake", "messages": [msg("c1", "1", "second", json!({"media": {"kind": "photo"}}))]}))).unwrap();
    let m3 = ch_db::get_message(db.conn(), c.id, "1").unwrap().unwrap();
    assert_eq!(
        m3.media.as_ref().unwrap().path.as_deref(),
        Some("/f/1b.png")
    );
    assert_eq!(
        ch_db::list_message_versions(db.conn(), m.id).unwrap().len(),
        1
    );

    // With edit history off nothing is snapshotted.
    let ing2 = ingest_with_accounts(
        &[("fake", "fake")],
        IngestOptions {
            keep_edit_history: false,
            ..Default::default()
        },
    );
    ing2.apply(
        "fake",
        ev(
            "messages.upsert",
            json!({"account_id": "fake", "messages": [msg("c1", "1", "a", json!({}))]}),
        ),
    )
    .unwrap();
    ing2.apply(
        "fake",
        ev(
            "message.edited",
            json!({"account_id": "fake", "message": msg("c1", "1", "b", json!({"edited": true}))}),
        ),
    )
    .unwrap();
    assert_eq!(count(ing2.db(), "SELECT COUNT(*) FROM message_version"), 0);
    assert_eq!(
        count(ing2.db(), "SELECT COUNT(*) FROM message WHERE text = 'b'"),
        1
    );
}

#[test]
fn local_echo_is_adopted_by_send_result_and_platform_echo() {
    let ing = fake();
    let db = ing.db();
    ing.apply(
        "fake",
        ev(
            "conversations.upsert",
            json!({"account_id": "fake", "conversations": [conv("c2", json!({}))]}),
        ),
    )
    .unwrap();
    let (_, echo_id) = ing
        .apply_local_echo(
            "fake",
            &LocalEcho {
                chat_id: "c2".into(),
                client_id: "cid-1".into(),
                text: Some("hello".into()),
                ..Default::default()
            },
        )
        .unwrap();
    let (_, again) = ing
        .apply_local_echo(
            "fake",
            &LocalEcho {
                chat_id: "c2".into(),
                client_id: "cid-1".into(),
                text: Some("hello".into()),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(echo_id, again);
    let c = ch_db::get_conversation(db.conn(), "fake", "c2")
        .unwrap()
        .unwrap();
    assert_eq!(
        ch_db::get_message(db.conn(), c.id, "local:cid-1")
            .unwrap()
            .unwrap()
            .delivery
            .as_deref(),
        Some("sending")
    );

    let sent: ch_protocol::dto::Message = serde_json::from_value(msg(
        "c2",
        "13",
        "hello",
        json!({"direction": "out", "delivery": "sent", "ext": {"fake": {"client_id": "cid-1"}}}),
    ))
    .unwrap();
    ing.apply_sent("fake", "fake", Some("cid-1"), std::slice::from_ref(&sent))
        .unwrap();
    assert_eq!(
        count(db, "SELECT COUNT(*) FROM message"),
        1,
        "the echo was re-keyed, not duplicated"
    );
    let row = ch_db::get_message(db.conn(), c.id, "13").unwrap().unwrap();
    assert_eq!(row.id, echo_id);
    assert_eq!(row.delivery.as_deref(), Some("sent"));
    assert_eq!(
        count(db, "SELECT COUNT(*) FROM message_version"),
        0,
        "echo confirmation is not an edit"
    );

    // The platform's echo is a no-op.
    let before = dump(db);
    let a = ing
        .apply(
            "fake",
            ev(
                "messages.upsert",
                json!({"account_id": "fake", "messages": [serde_json::to_value(&sent).unwrap()]}),
            ),
        )
        .unwrap();
    assert!(a.is_noop(), "{a:?}");
    assert_eq!(dump(db), before);

    // Without a prior apply_sent, the platform echo carrying client_id still adopts the echo row.
    ing.apply_local_echo(
        "fake",
        &LocalEcho {
            chat_id: "c2".into(),
            client_id: "cid-2".into(),
            text: Some("second".into()),
            ..Default::default()
        },
    )
    .unwrap();
    ing.apply("fake", ev("messages.upsert", json!({"account_id": "fake", "messages": [msg("c2", "14", "second", json!({"direction": "out", "delivery": "sent", "ext": {"fake": {"client_id": "cid-2"}}}))]}))).unwrap();
    assert_eq!(count(db, "SELECT COUNT(*) FROM message"), 2);
    assert!(
        ch_db::get_message(db.conn(), c.id, "local:cid-2")
            .unwrap()
            .is_none()
    );

    // A failed send marks the echo.
    ing.apply_local_echo(
        "fake",
        &LocalEcho {
            chat_id: "c2".into(),
            client_id: "cid-3".into(),
            text: Some("third".into()),
            ..Default::default()
        },
    )
    .unwrap();
    ing.apply_send_failed("fake", "c2", "cid-3").unwrap();
    assert_eq!(
        ch_db::get_message(db.conn(), c.id, "local:cid-3")
            .unwrap()
            .unwrap()
            .delivery
            .as_deref(),
        Some("failed")
    );
}

#[test]
fn deletions_tombstone_or_remove_and_match_dms_without_chat_id() {
    let ing = fake();
    let db = ing.db();
    ing.apply("fake", ev("conversations.upsert", json!({"account_id": "fake", "conversations": [conv("d1", json!({})), conv("g1", json!({"kind": "group"}))]}))).unwrap();
    ing.apply("fake", ev("messages.upsert", json!({"account_id": "fake", "messages": [msg("d1", "7", "secret", json!({})), msg("g1", "7", "group seven", json!({})), msg("d1", "8", "keep", json!({}))]}))).unwrap();
    let a = ing
        .apply(
            "fake",
            ev(
                "messages.deleted",
                json!({"account_id": "fake", "message_ids": ["7"]}),
            ),
        )
        .unwrap();
    assert_eq!(
        a.changes,
        vec![Change::MessagesDeleted {
            chat_id: "d1".into(),
            message_ids: vec!["7".into()],
            tombstoned: true
        }]
    );
    let d1 = ch_db::get_conversation(db.conn(), "fake", "d1")
        .unwrap()
        .unwrap();
    let g1 = ch_db::get_conversation(db.conn(), "fake", "g1")
        .unwrap()
        .unwrap();
    assert!(
        ch_db::get_message(db.conn(), d1.id, "7")
            .unwrap()
            .unwrap()
            .is_deleted()
    );
    assert!(
        !ch_db::get_message(db.conn(), g1.id, "7")
            .unwrap()
            .unwrap()
            .is_deleted(),
        "groups are not matched without chat_id"
    );
    // Kept-deleted stays searchable on request.
    let q = SearchQuery {
        text: "secret".into(),
        include_deleted: true,
        ..Default::default()
    };
    assert_eq!(ch_db::search_messages(db.conn(), &q).unwrap().len(), 1);
    assert!(
        ch_db::search_messages(
            db.conn(),
            &SearchQuery {
                include_deleted: false,
                ..q
            }
        )
        .unwrap()
        .is_empty()
    );
    // Again: no-op.
    let a = ing
        .apply(
            "fake",
            ev(
                "messages.deleted",
                json!({"account_id": "fake", "chat_id": "d1", "message_ids": ["7", "nope"]}),
            ),
        )
        .unwrap();
    assert!(a.is_noop(), "{a:?}");
    // Core-initiated delete removes for good.
    ing.apply_deleted("fake", "d1", &["8".into()]).unwrap();
    assert!(ch_db::get_message(db.conn(), d1.id, "8").unwrap().is_none());

    let ing2 = ingest_with_accounts(
        &[("fake", "fake")],
        IngestOptions {
            keep_deleted: false,
            ..Default::default()
        },
    );
    ing2.apply(
        "fake",
        ev(
            "messages.upsert",
            json!({"account_id": "fake", "messages": [msg("d1", "7", "x", json!({}))]}),
        ),
    )
    .unwrap();
    ing2.apply(
        "fake",
        ev(
            "messages.deleted",
            json!({"account_id": "fake", "chat_id": "d1", "message_ids": ["7"]}),
        ),
    )
    .unwrap();
    assert_eq!(count(ing2.db(), "SELECT COUNT(*) FROM message"), 0);
    assert_eq!(count(ing2.db(), "SELECT COUNT(*) FROM message_fts"), 0);
}

#[test]
fn read_state_marks_outgoing_read_by_sent_at() {
    let ing = fake();
    let db = ing.db();
    ing.apply("fake", ev("conversations.upsert", json!({"account_id": "fake", "conversations": [conv("c1", json!({"unread_count": 5}))]}))).unwrap();
    ing.apply("fake", ev("messages.upsert", json!({"account_id": "fake", "messages": [
        msg("c1", "1", "a", json!({"direction": "out", "sent_at": "2026-01-01T10:00:00.000Z", "delivery": "sent"})),
        msg("c1", "2", "b", json!({"direction": "out", "sent_at": "2026-01-01T10:01:00.000Z", "delivery": "sent"})),
        msg("c1", "3", "c", json!({"direction": "out", "sent_at": "2026-01-01T10:02:00.000Z", "delivery": "sent"})),
    ]}))).unwrap();
    let a = ing
        .apply(
            "fake",
            ev(
                "read.outbox",
                json!({"account_id": "fake", "chat_id": "c1", "max_message_id": "2"}),
            ),
        )
        .unwrap();
    assert_eq!(
        a.changes,
        vec![Change::ReadOutbox {
            chat_id: "c1".into(),
            topic_id: None,
            max_message_id: "2".into(),
            marked_read: 2
        }]
    );
    let c = ch_db::get_conversation(db.conn(), "fake", "c1")
        .unwrap()
        .unwrap();
    assert_eq!(c.read_outbox_max_id.as_deref(), Some("2"));
    assert_eq!(
        ch_db::get_message(db.conn(), c.id, "2")
            .unwrap()
            .unwrap()
            .delivery
            .as_deref(),
        Some("read")
    );
    assert_eq!(
        ch_db::get_message(db.conn(), c.id, "3")
            .unwrap()
            .unwrap()
            .delivery
            .as_deref(),
        Some("sent")
    );
    assert!(
        ing.apply(
            "fake",
            ev(
                "read.outbox",
                json!({"account_id": "fake", "chat_id": "c1", "max_message_id": "2"})
            )
        )
        .unwrap()
        .is_noop()
    );

    ing.apply("fake", ev("read.inbox", json!({"account_id": "fake", "chat_id": "c1", "max_message_id": "3", "unread_count": 1}))).unwrap();
    let c = ch_db::get_conversation(db.conn(), "fake", "c1")
        .unwrap()
        .unwrap();
    assert_eq!(
        (c.read_inbox_max_id.as_deref(), c.unread_count),
        (Some("3"), 1)
    );
    ing.apply_read("fake", "c1", None, "3").unwrap();
    let c = ch_db::get_conversation(db.conn(), "fake", "c1")
        .unwrap()
        .unwrap();
    assert_eq!(c.unread_count, 0);
    assert_eq!(c.marked_unread, Some(false));
    assert!(c.local_read_at.is_some());
    assert!(
        ing.apply(
            "fake",
            ev(
                "read.inbox",
                json!({"account_id": "fake", "chat_id": "nope", "max_message_id": "3"})
            )
        )
        .unwrap()
        .is_noop()
    );
}

#[test]
fn drafts_ignore_older_echoes() {
    let ing = fake();
    let db = ing.db();
    ing.apply(
        "fake",
        ev(
            "conversations.upsert",
            json!({"account_id": "fake", "conversations": [conv("c1", json!({}))]}),
        ),
    )
    .unwrap();
    let c = ch_db::get_conversation(db.conn(), "fake", "c1")
        .unwrap()
        .unwrap();
    ing.apply_draft(
        "fake",
        "c1",
        None,
        &Draft {
            text: "mine".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let a = ing.apply("fake", ev("draft.changed", json!({"account_id": "fake", "chat_id": "c1", "draft": {"text": "old", "updated_at": "2020-01-01T00:00:00.000Z"}}))).unwrap();
    assert!(a.is_noop(), "{a:?}");
    assert_eq!(
        ch_db::get_draft(db.conn(), c.id, "").unwrap().unwrap().text,
        "mine"
    );
    // Topic drafts live beside the chat draft; a newer one wins; empty clears.
    ing.apply("fake", ev("draft.changed", json!({"account_id": "fake", "chat_id": "c1", "topic_id": "t1", "draft": {"text": "topic"}}))).unwrap();
    assert_eq!(ch_db::list_drafts(db.conn(), c.id).unwrap().len(), 2);
    ing.apply(
        "fake",
        ev(
            "draft.changed",
            json!({"account_id": "fake", "chat_id": "c1", "draft": {"text": ""}}),
        ),
    )
    .unwrap();
    assert!(ch_db::get_draft(db.conn(), c.id, "").unwrap().is_none());
    assert!(
        ing.apply(
            "fake",
            ev(
                "draft.changed",
                json!({"account_id": "fake", "chat_id": "c1", "draft": {"text": ""}})
            )
        )
        .unwrap()
        .is_noop()
    );
}

#[test]
fn contacts_create_persons_and_auto_link_across_accounts() {
    let ing = ingest_with_accounts(
        &[("fake", "fake"), ("max", "max")],
        IngestOptions::default(),
    );
    let db = ing.db();
    let a = ing.apply("fake", ev("contacts.upsert", json!({"account_id": "fake", "contacts": [
        {"id": "u1", "display_name": "Alice", "phone": "+491700000001", "username": "alice"},
        {"id": "u2", "display_name": "Bob", "username": "bobby"},
        {"id": "u3", "first_name": "Carol", "last_name": "C"},
    ]}))).unwrap();
    assert_eq!(
        a.changes,
        vec![Change::ContactsUpserted {
            user_ids: vec!["u1".into(), "u2".into(), "u3".into()],
            persons_created: 3,
            auto_linked: 0
        }]
    );
    assert_eq!(
        ch_db::list_persons(db.conn())
            .unwrap()
            .iter()
            .filter_map(|p| p.display_name.clone())
            .collect::<Vec<_>>(),
        vec!["Alice", "Bob", "Carol C"]
    );

    let a = ing
        .apply(
            "max",
            ev(
                "contacts.upsert",
                json!({"account_id": "max", "contacts": [
                    {"id": "9", "display_name": "Alice on MAX", "phone": "+491700000001"},
                    {"id": "10", "display_name": "B.", "username": "BOBBY"},
                    {"id": "11", "display_name": "Nobody"},
                ]}),
            ),
        )
        .unwrap();
    assert_eq!(
        a.changes,
        vec![Change::ContactsUpserted {
            user_ids: vec!["9".into(), "10".into(), "11".into()],
            persons_created: 1,
            auto_linked: 2
        }]
    );
    let alice = ch_db::get_handle(db.conn(), "fake", "u1").unwrap().unwrap();
    let alice_max = ch_db::get_handle(db.conn(), "max", "9").unwrap().unwrap();
    assert_eq!(alice.person_id, alice_max.person_id);
    assert_eq!(alice_max.link_source.as_deref(), Some("auto_phone"));
    assert_eq!(
        ch_db::get_handle(db.conn(), "max", "10")
            .unwrap()
            .unwrap()
            .link_source
            .as_deref(),
        Some("auto_username")
    );
    assert_eq!(count(db, "SELECT COUNT(*) FROM person"), 4);
    // Chats stay per platform: no conversation merging happened or exists.
    assert_eq!(count(db, "SELECT COUNT(*) FROM conversation"), 0);

    // Same contacts again: no-op. A partial update changes one field only.
    let before = dump(db);
    assert!(ing.apply("fake", ev("contacts.upsert", json!({"account_id": "fake", "contacts": [{"id": "u1", "display_name": "Alice", "phone": "+491700000001", "username": "alice"}]}))).unwrap().is_noop());
    assert_eq!(dump(db), before);
    ing.apply(
        "fake",
        ev(
            "contacts.upsert",
            json!({"account_id": "fake", "contacts": [{"id": "u1", "bio": "hi"}]}),
        ),
    )
    .unwrap();
    let alice = ch_db::get_handle(db.conn(), "fake", "u1").unwrap().unwrap();
    assert_eq!(
        (alice.bio.as_deref(), alice.phone.as_deref()),
        (Some("hi"), Some("+491700000001"))
    );

    // status: me is recorded; presence updates the handle and is a no-op when unchanged.
    ing.apply("fake", ev("status", json!({"account_id": "fake", "state": "connected", "reconnecting": false, "me": {"id": "u0", "display_name": "Me"}}))).unwrap();
    let acc = ch_db::get_account(db.conn(), "fake").unwrap().unwrap();
    assert_eq!(
        (acc.state.as_str(), acc.me_user_id.as_deref()),
        ("connected", Some("u0"))
    );
    let p = json!({"account_id": "fake", "user_id": "u1", "online": {"state": "online", "until": "2026-01-01T10:05:00.000Z"}});
    ing.apply("fake", ev("presence", p.clone())).unwrap();
    assert_eq!(
        ch_db::get_handle(db.conn(), "fake", "u1")
            .unwrap()
            .unwrap()
            .status
            .unwrap()
            .state
            .as_str(),
        "online"
    );
    let before = dump(db);
    let a = ing.apply("fake", ev("presence", p)).unwrap();
    assert!(
        matches!(a.changes[0], Change::Presence { .. }),
        "presence is still reported to the UI"
    );
    assert_eq!(dump(db), before);
    // Typing for an unknown user creates a bare handle (no person yet) and is not stored otherwise.
    ing.apply(
        "fake",
        ev(
            "presence",
            json!({"account_id": "fake", "user_id": "u99", "chat_id": "g1", "action": "typing"}),
        ),
    )
    .unwrap();
    assert!(
        ch_db::get_handle(db.conn(), "fake", "u99")
            .unwrap()
            .unwrap()
            .person_id
            .is_none()
    );
    assert_eq!(count(db, "SELECT COUNT(*) FROM presence_log"), 0);

    let ing2 = ingest_with_accounts(
        &[("fake", "fake")],
        IngestOptions {
            log_presence: true,
            ..Default::default()
        },
    );
    ing2.apply("fake", ev("presence", json!({"account_id": "fake", "user_id": "u1", "online": {"state": "online"}, "action": "typing", "chat_id": "c1"}))).unwrap();
    assert_eq!(count(ing2.db(), "SELECT COUNT(*) FROM presence_log"), 2);
}

#[test]
fn topics_folders_and_synced_lists() {
    let ing = fake();
    let db = ing.db();
    ing.apply("fake", ev("conversations.upsert", json!({"account_id": "fake", "conversations": [conv("f", json!({"kind": "group", "is_forum": true})), conv("x", json!({}))]}))).unwrap();
    ing.apply("fake", ev("topics.upsert", json!({"account_id": "fake", "chat_id": "f", "complete": false, "topics": [
        {"id": "t1", "chat_id": "f", "title": "General", "is_general": true},
        {"id": "t2", "chat_id": "f", "title": "Bugs", "unread_count": 4, "draft": {"text": "wip"}},
    ]}))).unwrap();
    let f = ch_db::get_conversation(db.conn(), "fake", "f")
        .unwrap()
        .unwrap();
    assert_eq!(ch_db::list_topics(db.conn(), f.id).unwrap().len(), 2);
    assert_eq!(
        ch_db::get_draft(db.conn(), f.id, "t2")
            .unwrap()
            .unwrap()
            .text,
        "wip"
    );
    let a = ing.apply("fake", ev("topics.upsert", json!({"account_id": "fake", "chat_id": "f", "complete": true, "topics": [{"id": "t1", "chat_id": "f", "title": "General"}]}))).unwrap();
    assert_eq!(
        a.changes,
        vec![Change::TopicsUpserted {
            chat_id: "f".into(),
            created: vec![],
            updated: vec![],
            gone: vec!["t2".into()]
        }]
    );
    assert_eq!(ch_db::list_topics(db.conn(), f.id).unwrap().len(), 1);
    let t2 = ch_db::get_topic(db.conn(), f.id, "t2").unwrap().unwrap();
    assert!(t2.is_gone);
    assert_eq!(t2.unread_count, 4, "hidden topics keep their state");

    ing.apply("fake", ev("folders.synced", json!({"account_id": "fake", "folders": [
        {"id": "f1", "title": "Work", "chat_ids": ["f"], "is_shared": false},
        {"id": "f2", "title": "Fun", "rules": {"include_kinds": ["groups"], "exclude_muted": true, "exclude_read": false, "exclude_archived": false}, "is_shared": false},
    ]}))).unwrap();
    assert_eq!(ch_db::list_folders(db.conn(), "fake").unwrap().len(), 2);
    assert_eq!(
        ch_db::chat_folder_ids(db.conn(), "fake", "f").unwrap(),
        vec!["f1".to_owned()]
    );
    let before = dump(db);
    ing.apply("fake", ev("folders.synced", json!({"account_id": "fake", "folders": [
        {"id": "f1", "title": "Work", "chat_ids": ["f"], "is_shared": false},
        {"id": "f2", "title": "Fun", "rules": {"include_kinds": ["groups"], "exclude_muted": true, "exclude_read": false, "exclude_archived": false}, "is_shared": false},
    ]}))).unwrap();
    assert_eq!(dump(db), before);
    let a = ing.apply("fake", ev("folders.synced", json!({"account_id": "fake", "folders": [{"id": "f2", "title": "Fun", "is_shared": false}]}))).unwrap();
    assert_eq!(
        a.changes,
        vec![Change::FoldersSynced {
            folder_ids: vec!["f2".into()],
            removed: 1
        }]
    );
    assert!(
        ch_db::chat_folder_ids(db.conn(), "fake", "f")
            .unwrap()
            .is_empty()
    );

    let a = ing
        .apply(
            "fake",
            ev(
                "conversations.synced",
                json!({"account_id": "fake", "chat_ids": ["f"]}),
            ),
        )
        .unwrap();
    assert_eq!(
        a.changes,
        vec![Change::ConversationsSynced {
            gone: vec!["x".into()],
            back: vec![]
        }]
    );
    assert_eq!(
        ch_db::list_conversations(db.conn(), "fake", None)
            .unwrap()
            .len(),
        1
    );
    assert!(
        ing.apply(
            "fake",
            ev(
                "conversations.synced",
                json!({"account_id": "fake", "chat_ids": ["f"]})
            )
        )
        .unwrap()
        .is_noop()
    );
    // A later upsert of the gone chat brings it back.
    ing.apply(
        "fake",
        ev(
            "conversations.upsert",
            json!({"account_id": "fake", "conversations": [conv("x", json!({}))]}),
        ),
    )
    .unwrap();
    assert!(
        !ch_db::get_conversation(db.conn(), "fake", "x")
            .unwrap()
            .unwrap()
            .is_gone
    );

    ing.apply_chat_flags(
        "fake",
        "x",
        &ChatFlags {
            archived: Some(true),
            muted: Some(true),
            ..Default::default()
        },
    )
    .unwrap();
    let x = ch_db::get_conversation(db.conn(), "fake", "x")
        .unwrap()
        .unwrap();
    assert_eq!((x.archived, x.muted), (true, Some(true)));
    assert!(ing.apply("fake", ev("conversations.upsert", json!({"account_id": "fake", "conversations": [conv("x", json!({"archived": true, "muted": true}))]}))).unwrap().is_noop(), "flag echo");
}

#[test]
fn reactions_live_and_local() {
    let ing = fake();
    let db = ing.db();
    ing.apply("fake", ev("messages.upsert", json!({"account_id": "fake", "messages": [msg("c1", "1", "hi", json!({"reactions": [{"key": {"emoji": "👍"}, "count": 1, "mine": false}]}))]}))).unwrap();
    let c = ch_db::get_conversation(db.conn(), "fake", "c1")
        .unwrap()
        .unwrap();
    let m = ch_db::get_message(db.conn(), c.id, "1").unwrap().unwrap();
    assert_eq!(ch_db::reactions_dto(db.conn(), m.id).unwrap().len(), 1);
    let e = json!({"account_id": "fake", "chat_id": "c1", "message_id": "1", "reactions": [{"key": {"emoji": "👍"}, "count": 2, "mine": false}], "reactors": [{"user_id": "u2", "key": {"emoji": "👍"}}]});
    let a = ing
        .apply("fake", ev("reaction.changed", e.clone()))
        .unwrap();
    assert_eq!(
        a.changes,
        vec![Change::ReactionsChanged {
            chat_id: "c1".into(),
            message_id: "1".into()
        }]
    );
    assert!(
        ing.apply("fake", ev("reaction.changed", e))
            .unwrap()
            .is_noop()
    );
    assert_eq!(
        ch_db::get_message(db.conn(), c.id, "1")
            .unwrap()
            .unwrap()
            .reactors
            .unwrap()
            .len(),
        1
    );

    ing.apply_reactions_set(
        "fake",
        "fake",
        "c1",
        "1",
        &[ReactionKey::emoji("👍"), ReactionKey::emoji("❤")],
        None,
    )
    .unwrap();
    let r = ch_db::reactions_dto(db.conn(), m.id).unwrap();
    assert_eq!(r.len(), 2);
    assert_eq!((r[0].count, r[0].mine), (3, true));
    assert_eq!((r[1].count, r[1].mine), (1, true));
    ing.apply_reactions_set("fake", "fake", "c1", "1", &[], None)
        .unwrap();
    let r = ch_db::reactions_dto(db.conn(), m.id).unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!((r[0].count, r[0].mine), (2, false));
    assert!(ing.apply("fake", ev("reaction.changed", json!({"account_id": "fake", "chat_id": "c1", "message_id": "nope", "reactions": []}))).unwrap().is_noop());
    // Pins.
    ing.apply_pinned("fake", "c1", Some("1"), true).unwrap();
    assert_eq!(ch_db::list_pinned(db.conn(), c.id).unwrap().len(), 1);
    assert!(ing.apply("fake", ev("message.edited", json!({"account_id": "fake", "message": msg("c1", "1", "hi", json!({"is_pinned": true}))}))).unwrap().is_noop(), "pin echo");
    ing.apply_pinned("fake", "c1", None, false).unwrap();
    assert!(ch_db::list_pinned(db.conn(), c.id).unwrap().is_empty());
}

#[test]
fn unknown_account_and_plugin_mismatch_are_errors_and_paths_are_filtered() {
    let ing = ingest_with_accounts(
        &[("fake", "fake")],
        IngestOptions {
            path_filter: Some(std::sync::Arc::new(|_acc: &str, p: &str| {
                p.starts_with("/cache/")
            })),
            ..Default::default()
        },
    );
    assert!(matches!(
        ing.apply(
            "fake",
            ev(
                "conversations.synced",
                json!({"account_id": "ghost", "chat_ids": []})
            )
        ),
        Err(IngestError::UnknownAccount { .. })
    ));
    assert!(matches!(
        ing.apply(
            "other",
            ev(
                "conversations.synced",
                json!({"account_id": "fake", "chat_ids": []})
            )
        ),
        Err(IngestError::PluginMismatch { .. })
    ));
    let a = ing.apply("fake", ev("messages.upsert", json!({"account_id": "fake", "messages": [msg("c1", "1", "x", json!({"media": {"kind": "photo", "path": "/home/x/.ssh/id", "thumb_path": "/cache/t.jpg"}}))]}))).unwrap();
    assert_eq!(a.dropped_paths, 1);
    let c = ch_db::get_conversation(ing.db().conn(), "fake", "c1")
        .unwrap()
        .unwrap();
    let m = ch_db::get_message(ing.db().conn(), c.id, "1")
        .unwrap()
        .unwrap()
        .media
        .unwrap();
    assert_eq!(
        (m.path, m.thumb_path.as_deref()),
        (None, Some("/cache/t.jpg"))
    );
    let a = ing
        .apply(
            "fake",
            ev(
                "avatar.ready",
                json!({"account_id": "fake", "kind": "user", "id": "u1", "path": "/etc/passwd"}),
            ),
        )
        .unwrap();
    assert_eq!(a.dropped_paths, 1);
    assert!(a.changes.is_empty());
    ing.apply(
        "fake",
        ev(
            "avatar.ready",
            json!({"account_id": "fake", "kind": "user", "id": "u1", "path": "/cache/u1.jpg"}),
        ),
    )
    .unwrap();
    assert_eq!(
        ch_db::get_handle(ing.db().conn(), "fake", "u1")
            .unwrap()
            .unwrap()
            .avatar_path
            .as_deref(),
        Some("/cache/u1.jpg")
    );
}

#[test]
fn history_page_records_cursor_and_start() {
    let ing = fake();
    let db = ing.db();
    let page: Vec<ch_protocol::dto::Message> = vec![
        serde_json::from_value(msg(
            "c1",
            "5",
            "e",
            json!({"sent_at": "2026-01-01T10:05:00.000Z"}),
        ))
        .unwrap(),
        serde_json::from_value(msg(
            "c1",
            "4",
            "d",
            json!({"sent_at": "2026-01-01T10:04:00.000Z"}),
        ))
        .unwrap(),
    ];
    ing.apply_history_page("fake", "fake", "c1", None, &page, Some(Some("cur-2")))
        .unwrap();
    let c = ch_db::get_conversation(db.conn(), "fake", "c1")
        .unwrap()
        .unwrap();
    let h = ch_db::get_history_state(db.conn(), c.id, "")
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            h.older_cursor.as_deref(),
            h.reached_start,
            h.oldest_message_id.as_deref(),
            h.newest_message_id.as_deref()
        ),
        (Some("cur-2"), false, Some("4"), Some("5"))
    );
    let page2: Vec<ch_protocol::dto::Message> = vec![
        serde_json::from_value(msg(
            "c1",
            "3",
            "c",
            json!({"sent_at": "2026-01-01T10:03:00.000Z"}),
        ))
        .unwrap(),
    ];
    ing.apply_history_page("fake", "fake", "c1", None, &page2, Some(None))
        .unwrap();
    let h = ch_db::get_history_state(db.conn(), c.id, "")
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            h.older_cursor,
            h.reached_start,
            h.oldest_message_id.as_deref()
        ),
        (None, true, Some("3"))
    );
    assert_eq!(ch_db::count_messages(db.conn(), c.id).unwrap(), 3);
    let hits = ch_db::search_messages(
        db.conn(),
        &SearchQuery {
            text: "d".into(),
            account_id: Some("fake".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(hits.len(), 1);
}
