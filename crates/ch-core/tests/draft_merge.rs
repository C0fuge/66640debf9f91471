//! Draft content, freshness and identity regressions through the public ingest boundary.

mod common;

use ch_core::ingest::{Applied, Change, Ingest, IngestOptions};
use ch_db::types::DraftRow;
use common::*;
use serde_json::{Value, json};

fn add_chat(ing: &Ingest, account: &str, chat: &str) -> i64 {
    ing.apply(
        "fake",
        ev(
            "conversations.upsert",
            json!({"account_id": account, "conversations": [conv(chat, json!({}))]}),
        ),
    )
    .unwrap();
    ch_db::get_conversation(ing.db().conn(), account, chat)
        .unwrap()
        .unwrap()
        .id
}

fn change(ing: &Ingest, account: &str, chat: &str, topic: Option<&str>, draft: Value) -> Applied {
    ing.apply(
        "fake",
        ev(
            "draft.changed",
            json!({"account_id": account, "chat_id": chat, "topic_id": topic, "draft": draft}),
        ),
    )
    .unwrap()
}

fn draft(ing: &Ingest, chat: i64, topic: &str) -> DraftRow {
    ch_db::get_draft(ing.db().conn(), chat, topic)
        .unwrap()
        .unwrap()
}

fn assert_changed(applied: &Applied, topic: Option<&str>, cleared: bool) {
    assert_eq!(
        applied.changes,
        vec![Change::DraftChanged {
            chat_id: "chat".into(),
            topic_id: topic.map(str::to_owned),
            cleared,
        }],
    );
}

#[test]
fn same_text_formatting_reply_and_extension_changes_are_not_lost() {
    let ing = fake();
    let chat = add_chat(&ing, "fake", "chat");
    change(
        &ing,
        "fake",
        "chat",
        None,
        json!({
            "text": "draft",
            "entities": [{"type": "bold", "offset": 0, "length": 5}],
            "reply_to": "first",
            "ext": {"fake": {"preview": true}},
            "updated_at": "2026-01-01T10:00:00.000Z"
        }),
    );
    // Each event changes just one part of the same-text draft snapshot.
    for (second, reply, preview) in [
        (1, "first", true),
        (2, "second", true),
        (3, "second", false),
    ] {
        let updated = change(
            &ing,
            "fake",
            "chat",
            None,
            json!({
                "text": "draft",
                "entities": [],
                "reply_to": reply,
                "ext": {"fake": {"preview": preview}},
                "updated_at": format!("2026-01-01T10:00:0{second}.000Z")
            }),
        );
        assert_changed(&updated, None, false);
        let row = draft(&ing, chat, "");
        assert_eq!(row.text, "draft");
        assert_eq!(row.entities, Some(vec![]));
        assert_eq!(row.reply_to.as_deref(), Some(reply));
        assert_eq!(
            serde_json::to_value(row.ext).unwrap(),
            json!({"fake": {"preview": preview}}),
        );
    }
}

#[test]
fn newer_identical_content_advances_freshness_without_a_content_event() {
    let ing = fake();
    let chat = add_chat(&ing, "fake", "chat");
    let initial = json!({
        "text": "keep",
        "entities": [],
        "updated_at": "2026-01-01T10:00:00.000Z"
    });
    change(&ing, "fake", "chat", None, initial.clone());
    let first = draft(&ing, chat, "");
    let newer = json!({
        "text": "keep",
        "entities": [],
        "updated_at": "2026-01-01T10:00:02.000Z"
    });
    let applied = change(&ing, "fake", "chat", None, newer.clone());
    assert!(applied.changes.is_empty(), "{applied:?}");
    let fresh = draft(&ing, chat, "");
    assert!(fresh.updated_at > first.updated_at);

    let stale = change(
        &ing,
        "fake",
        "chat",
        None,
        json!({"text": "stale", "updated_at": "2026-01-01T10:00:01.000Z"}),
    );
    assert!(stale.is_noop(), "{stale:?}");
    assert_eq!(draft(&ing, chat, ""), fresh);
    let duplicate = change(&ing, "fake", "chat", None, newer);
    assert!(duplicate.changes.is_empty(), "{duplicate:?}");
    assert_eq!(draft(&ing, chat, ""), fresh);
}

#[test]
fn draft_changes_and_clears_are_scoped_to_account_chat_and_topic() {
    let ing = ingest_with_accounts(
        &[("first", "fake"), ("second", "fake")],
        IngestOptions::default(),
    );
    let first = add_chat(&ing, "first", "chat");
    let second = add_chat(&ing, "second", "chat");
    let other = add_chat(&ing, "first", "other");
    for (account, chat, topic, text) in [
        ("first", "chat", None, "main"),
        ("first", "chat", Some("topic"), "topic"),
        ("second", "chat", None, "second account"),
        ("first", "other", None, "other chat"),
    ] {
        change(&ing, account, chat, topic, json!({"text": text}));
    }
    let second_before = draft(&ing, second, "");
    let other_before = draft(&ing, other, "");
    let main_before = draft(&ing, first, "");
    let cleared = change(&ing, "first", "chat", Some("topic"), json!({"text": ""}));
    assert_changed(&cleared, Some("topic"), true);
    assert!(
        ch_db::get_draft(ing.db().conn(), first, "topic")
            .unwrap()
            .is_none()
    );
    assert_eq!(draft(&ing, first, ""), main_before);
    assert_eq!(draft(&ing, second, ""), second_before);
    assert_eq!(draft(&ing, other, ""), other_before);
    let duplicate = change(&ing, "first", "chat", Some("topic"), json!({"text": ""}));
    assert!(duplicate.changes.is_empty(), "{duplicate:?}");
}

#[test]
fn nested_draft_is_a_whole_snapshot_but_missing_parent_draft_is_preserved() {
    let ing = fake();
    let chat = add_chat(&ing, "fake", "chat");
    change(
        &ing,
        "fake",
        "chat",
        None,
        json!({
            "text": "draft",
            "entities": [{"type": "italic", "offset": 0, "length": 5}],
            "reply_to": "reply",
            "ext": {"fake": {"preview": true}},
            "updated_at": "2026-01-01T10:00:00.000Z"
        }),
    );
    let original = draft(&ing, chat, "");
    for snapshot in [
        conv("chat", json!({"title": "updated title"})),
        conv("chat", json!({"draft": null})),
    ] {
        ing.apply(
            "fake",
            ev(
                "conversations.upsert",
                json!({"account_id": "fake", "conversations": [snapshot]}),
            ),
        )
        .unwrap();
        assert_eq!(draft(&ing, chat, ""), original);
    }

    let replaced = change(
        &ing,
        "fake",
        "chat",
        None,
        json!({"text": "draft", "reply_to": null, "updated_at": "2026-01-01T10:00:01.000Z"}),
    );
    assert_changed(&replaced, None, false);
    let unknown = draft(&ing, chat, "");
    assert_eq!(unknown.entities, None, "omitted nested metadata is unknown");
    assert_eq!(
        unknown.reply_to, None,
        "null and omitted optional metadata agree",
    );
    assert_eq!(unknown.ext, None, "extension snapshot is replaced, not merged");

    let duplicate = change(
        &ing,
        "fake",
        "chat",
        None,
        json!({"text": "draft", "updated_at": "2026-01-01T10:00:01.000Z"}),
    );
    assert!(duplicate.changes.is_empty(), "{duplicate:?}");
    assert_eq!(draft(&ing, chat, ""), unknown);
}

#[test]
fn undated_duplicate_preserves_timestamp_and_older_clear_is_ignored() {
    let ing = fake();
    let chat = add_chat(&ing, "fake", "chat");
    change(&ing, "fake", "chat", None, json!({"text": "keep"}));
    let before = draft(&ing, chat, "");
    let duplicate = change(&ing, "fake", "chat", None, json!({"text": "keep"}));
    assert!(duplicate.changes.is_empty(), "{duplicate:?}");
    assert_eq!(draft(&ing, chat, ""), before);

    let stale_clear = change(
        &ing,
        "fake",
        "chat",
        None,
        json!({"text": "", "updated_at": "2020-01-01T00:00:00.000Z"}),
    );
    assert!(stale_clear.is_noop(), "{stale_clear:?}");
    assert_eq!(draft(&ing, chat, ""), before);
}
