//! Replay the recorded fake-plugin stream (`fixtures/fake-events.ndjson`, see `record_fake.py`):
//! applying it twice yields the same database, legal reorderings yield the same database, and
//! the result is consistent (no stub chats, FTS in sync, searchable text).

mod common;

use ch_core::ingest::Ingest;
use ch_db::SearchQuery;
use ch_protocol::dto::{Message, ReactionKey, Topic};
use common::*;
use serde_json::Value;

const FIXTURE: &str = include_str!("fixtures/fake-events.ndjson");

#[derive(Clone)]
struct Frame {
    kind: String,
    method: String,
    params: Value,
    result: Value,
}

fn frames() -> Vec<Frame> {
    FIXTURE
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let v: Value = serde_json::from_str(l).unwrap();
            Frame {
                kind: v["kind"].as_str().unwrap().to_owned(),
                method: v["method"].as_str().unwrap().to_owned(),
                params: v["params"].clone(),
                result: v.get("result").cloned().unwrap_or(Value::Null),
            }
        })
        .collect()
}

fn account(f: &Frame) -> String {
    f.params["account_id"].as_str().unwrap().to_owned()
}

fn apply_frame(ing: &Ingest, f: &Frame) {
    if f.kind == "event" {
        ing.apply("fake", ev(&f.method, f.params.clone())).unwrap();
        return;
    }
    let acc = account(f);
    let chat = f.params["chat_id"].as_str().unwrap_or("");
    let topic = f.params["topic_id"].as_str();
    match f.method.as_str() {
        "messages.fetch_recent" => {
            let msgs: Vec<Message> = serde_json::from_value(f.result["messages"].clone()).unwrap();
            ing.apply_history_page(
                "fake",
                &acc,
                chat,
                topic,
                &msgs,
                Some(f.result["next_cursor"].as_str()),
            )
            .unwrap();
        }
        "topics.list" => {
            let topics: Vec<Topic> = serde_json::from_value(f.result["topics"].clone()).unwrap();
            ing.apply_topics_page("fake", &acc, chat, &topics).unwrap();
        }
        "messages.send" => {
            let m: Message = serde_json::from_value(f.result["message"].clone()).unwrap();
            ing.apply_sent("fake", &acc, f.params["client_id"].as_str(), &[m])
                .unwrap();
        }
        "messages.edit" => {
            let m: Message = serde_json::from_value(f.result["message"].clone()).unwrap();
            ing.apply_edited("fake", &acc, &m).unwrap();
        }
        "reactions.set" => {
            let keys: Vec<ReactionKey> =
                serde_json::from_value(f.params["reactions"].clone()).unwrap();
            let m: Option<Message> = f
                .result
                .get("message")
                .cloned()
                .map(|v| serde_json::from_value(v).unwrap());
            ing.apply_reactions_set(
                "fake",
                &acc,
                chat,
                f.params["message_id"].as_str().unwrap(),
                &keys,
                m.as_ref(),
            )
            .unwrap();
        }
        "messages.pin" => ing
            .apply_pinned(&acc, chat, f.params["message_id"].as_str(), true)
            .map(|_| ())
            .unwrap(),
        "drafts.save" => {
            let d = serde_json::from_value(f.params["draft"].clone()).unwrap();
            ing.apply_draft(&acc, chat, topic, &d).unwrap();
        }
        "read.send_receipt" => ing
            .apply_read(
                &acc,
                chat,
                topic,
                f.params["max_message_id"].as_str().unwrap(),
            )
            .map(|_| ())
            .unwrap(),
        "messages.delete" => {
            let ids: Vec<String> = serde_json::from_value(f.params["message_ids"].clone()).unwrap();
            ing.apply_deleted(&acc, chat, &ids).unwrap();
        }
        other => panic!("fixture has an unhandled result kind: {other}"),
    }
}

fn run(frames: &[Frame]) -> Ingest {
    let ing = fake();
    for f in frames {
        apply_frame(&ing, f);
    }
    ing
}

/// Deterministic shuffle (no rand dependency).
fn shuffle<T>(v: &mut [T], mut seed: u64) {
    for i in (1..v.len()).rev() {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let j = (seed >> 33) as usize % (i + 1);
        v.swap(i, j);
    }
}

#[test]
fn stream_applies_and_is_consistent() {
    let fr = frames();
    assert!(fr.len() > 30, "fixture looks truncated");
    let ing = run(&fr);
    let db = ing.db();
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM conversation WHERE kind = 'unknown'"
        ),
        0,
        "every chat was announced before its messages"
    );
    assert_eq!(
        count(db, "SELECT COUNT(*) FROM conversation WHERE is_gone = 1"),
        0
    );
    assert_eq!(count(db, "SELECT COUNT(*) FROM conversation"), 8);
    assert!(
        count(db, "SELECT COUNT(*) FROM message") >= 40,
        "sync + history pages landed"
    );
    assert!(count(db, "SELECT COUNT(*) FROM platform_handle") > 1);
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM platform_handle WHERE person_id IS NULL AND display_name IS NOT NULL"
        ),
        0,
        "every named contact has a person"
    );
    assert!(count(db, "SELECT COUNT(*) FROM folder") >= 1);
    assert!(count(db, "SELECT COUNT(*) FROM topic") >= 1);
    assert!(count(db, "SELECT COUNT(*) FROM history_state") >= 4);
    // The message the core sent, edited, pinned and then deleted is gone for good (own delete).
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM message WHERE message_id = '13' AND conversation_id = (SELECT id FROM conversation WHERE chat_id = 'c2')"
        ),
        0
    );
    // The live message.edited on c3/6 made exactly one version if the original had been synced.
    let had_original = fr
        .iter()
        .filter(|f| f.method == "messages.upsert" || f.method == "messages.fetch_recent")
        .any(|f| {
            let msgs = if f.kind == "event" {
                &f.params["messages"]
            } else {
                &f.result["messages"]
            };
            msgs.as_array()
                .unwrap()
                .iter()
                .any(|m| m["chat_id"] == "c3" && m["id"] == "6")
        });
    let versions = count(db, "SELECT COUNT(*) FROM message_version");
    if had_original {
        assert_eq!(versions, 1, "one edit of a known message = one version");
    }
    // Local search finds synced text and respects in: and from:.
    let any_text: String = db.conn().query_row("SELECT text FROM message WHERE text IS NOT NULL AND length(text) > 12 AND deleted_at IS NULL ORDER BY id LIMIT 1", [], |r| r.get(0)).unwrap();
    let word = any_text
        .split_whitespace()
        .find(|w| w.chars().all(char::is_alphanumeric) && w.len() > 3)
        .unwrap();
    let hits = ch_db::search_messages(
        db.conn(),
        &SearchQuery {
            text: word.to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(!hits.is_empty(), "search for {word:?} in {any_text:?}");
    let hits_in = ch_db::search_messages(
        db.conn(),
        &SearchQuery {
            text: word.to_owned(),
            conversation_id: Some(hits[0].conversation_id),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(!hits_in.is_empty());
    assert!(db.integrity_ok().unwrap());
}

#[test]
fn applying_the_stream_twice_is_a_noop() {
    let fr = frames();
    let ing = run(&fr);
    let once = dump(ing.db());
    for f in &fr {
        apply_frame(&ing, f);
    }
    assert_same_dump(&once, &dump(ing.db()), "second pass");
    // Individually replayed events are no-ops too, where that is well-defined: whole-set syncs,
    // and the tail of the stream (live events; earlier partial updates were legitimately
    // superseded by later ones and would re-apply older counters).
    let tail = fr.len().saturating_sub(12);
    for (i, f) in fr.iter().enumerate().filter(|(i, f)| {
        f.kind == "event"
            && !matches!(
                f.method.as_str(),
                "presence" | "status" | "transfer.progress" | "auth.step" | "conversations.upsert"
            )
            && (*i >= tail
                || matches!(f.method.as_str(), "conversations.synced" | "folders.synced"))
    }) {
        let _ = i;
        let a = ing.apply("fake", ev(&f.method, f.params.clone())).unwrap();
        assert!(
            a.is_noop(),
            "{} was not a no-op on replay: {:?}",
            f.method,
            a.changes
        );
    }
}

#[test]
fn legal_reorderings_give_the_same_state() {
    let fr = frames();
    let baseline = dump(run(&fr).db());
    // Items inside one bulk event are independent (distinct chats / messages / contacts): any
    // order inside the initial conversations.upsert, messages.upsert and contacts.upsert batches
    // must end in the same state. (Later batches are live partial updates and depend on order.)
    let first = |method: &str| {
        fr.iter()
            .position(|f| f.kind == "event" && f.method == method)
            .unwrap()
    };
    let batches = [
        (first("conversations.upsert"), "conversations"),
        (first("messages.upsert"), "messages"),
        (first("contacts.upsert"), "contacts"),
    ];
    for seed in 1..=3u64 {
        let mut reordered = fr.clone();
        for (idx, key) in batches {
            let arr = reordered[idx].params[key].as_array_mut().unwrap();
            shuffle(arr, seed * 7 + idx as u64);
        }
        assert_same_dump(
            &baseline,
            &dump(run(&reordered).db()),
            &format!("seed {seed}"),
        );
    }
    // Messages before their chats: stubs are created and upgraded when the chat arrives.
    let mut swapped = fr.clone();
    let (a, b) = (first("conversations.upsert"), first("messages.upsert"));
    swapped.swap(a, b);
    let ing = run(&swapped);
    assert_eq!(
        count(
            ing.db(),
            "SELECT COUNT(*) FROM conversation WHERE kind = 'unknown'"
        ),
        0
    );
    assert_same_dump(&baseline, &dump(ing.db()), "messages before chats");
}
