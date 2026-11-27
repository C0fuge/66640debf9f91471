//! Canonical replay comparisons retain reverse-index semantics and reject corruption.
mod common;

use std::panic::{AssertUnwindSafe, catch_unwind};

use ch_core::ingest::Ingest;
use serde_json::json;

fn fixture(reverse: bool) -> Ingest {
    let ing = common::fake();
    let mut chats = vec![common::conv("first", json!({"avatar_path":"/fixture/avatar-first"})),
        common::conv("second", json!({"avatar_path":"/fixture/avatar-second"}))];
    let mut messages = vec![common::msg("first", "one", "body with /literal/path", json!({
        "media":{"kind":"document","path":"/fixture/full-one","thumb_path":"/fixture/thumb-one"}
    })), common::msg("second", "two", "body two", json!({
        "link_preview":{"thumb_path":"/fixture/preview-two","media":{"kind":"photo","path":"/fixture/full-two"}}
    }))];
    if reverse { chats.reverse(); messages.reverse(); }
    ing.apply("fake", common::ev("conversations.upsert", json!({"account_id":"fake","conversations":chats}))).unwrap();
    ing.apply("fake", common::ev("messages.upsert", json!({"account_id":"fake","messages":messages}))).unwrap();
    ing
}

#[test]
fn metadata_refresh_allocations_are_volatile_after_invariant_validation() {
    let ing = fixture(false);
    let before = common::dump(ing.db());
    let old_clock = common::count(ing.db(), "SELECT revision FROM managed_ref_clock");
    let row = ch_db::get_conversation(ing.db().conn(), "fake", "first").unwrap().unwrap();
    ch_db::update_conversation(ing.db().conn(), &row).unwrap();
    assert!(common::count(ing.db(), "SELECT revision FROM managed_ref_clock") > old_clock);
    common::assert_same_dump(&before, &common::dump(ing.db()), "metadata refresh");
}

#[test]
fn reordered_row_ids_preserve_actual_owner_message_and_path_relationships() {
    let original = fixture(false);
    let reversed = fixture(true);
    let first = ch_db::conversation_rowid(original.db().conn(), "fake", "first").unwrap();
    let other = ch_db::conversation_rowid(reversed.db().conn(), "fake", "first").unwrap();
    assert_ne!(first, other, "fixture must actually reorder surrogate keys");
    common::assert_same_dump(&common::dump(original.db()), &common::dump(reversed.db()), "logical keys");
}

#[test]
fn snapshot_validation_rejects_stale_missing_misowned_and_wrong_slot_references() {
    for sql in [
        "UPDATE managed_ref SET revision=revision+1 WHERE consumer_kind='message'",
        "UPDATE managed_ref SET path='/fixture/wrong-path' WHERE consumer_kind='message'",
        "UPDATE managed_ref SET slot=slot || '.wrong' WHERE consumer_kind='message'",
        "UPDATE managed_ref SET account_id='foreign' WHERE consumer_kind='message'",
        "UPDATE managed_ref SET message_id=NULL WHERE consumer_kind='message'",
        "DELETE FROM managed_ref WHERE consumer_kind='message'",
        "DELETE FROM managed_ref_row WHERE consumer_kind='message'",
        "UPDATE managed_ref_clock SET revision=0",
    ] {
        let ing = fixture(false);
        let _ = common::dump(ing.db());
        ing.db().conn().execute(sql, []).unwrap();
        assert!(catch_unwind(AssertUnwindSafe(|| common::dump(ing.db()))).is_err(),
            "snapshot accepted corrupted managed state: {sql}");
    }
}

#[test]
fn semantic_classification_and_control_state_are_not_erased_from_comparison() {
    let ing = fixture(false);
    let before = common::dump(ing.db());
    // A conservative classification is descriptive, not an unlink permission.
    ing.db().conn().execute("UPDATE managed_ref SET classification='blocked' WHERE consumer_kind='message'", []).unwrap();
    let classified = common::dump(ing.db());
    assert_ne!(before, classified);
    ing.db().conn().execute("UPDATE managed_ref_row SET state='blocked',reason='test_blocker' WHERE consumer_kind='message'", []).unwrap();
    assert_ne!(classified, common::dump(ing.db()));
}
