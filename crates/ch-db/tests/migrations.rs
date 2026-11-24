//! Migrations: from empty, step by step, idempotent, and the pragmas.

use ch_db::migrations::{LATEST, MIGRATIONS, table_names};
use ch_db::{Db, Error};

fn expected_tables() -> Vec<&'static str> {
    vec![
        "account",
        "category",
        "conversation",
        "draft",
        "folder",
        "folder_chat",
        "history_gap",
        "history_state",
        "media_file",
        "member",
        "message",
        "message_fts",
        "message_reaction",
        "message_version",
        "outbox",
        "person",
        "person_category",
        "platform_handle",
        "plugin",
        "presence_log",
        "settings",
        "topic",
    ]
}

#[test]
fn from_empty_reaches_latest_with_all_tables() {
    let db = Db::open_in_memory().unwrap();
    assert_eq!(db.schema_version().unwrap(), LATEST);
    let names = table_names(db.conn()).unwrap();
    for t in expected_tables() {
        assert!(names.iter().any(|n| n == t), "missing table {t}: {names:?}");
    }
    assert!(db.integrity_ok().unwrap());
}

#[test]
fn step_by_step_and_rerun_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hub.db");
    let db = Db::open_unmigrated(&path).unwrap();
    assert_eq!(db.schema_version().unwrap(), 0);
    for m in MIGRATIONS {
        db.migrate_to(m.version).unwrap();
        assert_eq!(db.schema_version().unwrap(), m.version, "after {}", m.name);
        // Running the same step's SQL again must be harmless (IF NOT EXISTS everywhere).
        db.conn().execute_batch(m.sql).unwrap();
        assert_eq!(db.schema_version().unwrap(), m.version);
    }
    assert_eq!(db.schema_version().unwrap(), LATEST);
    db.migrate_to(LATEST).unwrap(); // no-op
    drop(db);
    // Reopening a migrated file is a no-op too.
    let db = Db::open(&path).unwrap();
    assert_eq!(db.schema_version().unwrap(), LATEST);
    assert!(db.integrity_ok().unwrap());
}

#[test]
fn newer_schema_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hub.db");
    {
        let db = Db::open(&path).unwrap();
        db.conn()
            .pragma_update(None, "user_version", LATEST + 1)
            .unwrap();
    }
    match Db::open(&path) {
        Err(Error::SchemaTooNew { found, supported }) => {
            assert_eq!(found, LATEST + 1);
            assert_eq!(supported, LATEST);
        }
        other => panic!("expected SchemaTooNew, got {other:?}"),
    }
}

#[test]
fn pragmas_wal_foreign_keys_fts5() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path().join("hub.db")).unwrap();
    let mode: String = db
        .conn()
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
    let fk: i64 = db
        .conn()
        .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
        .unwrap();
    assert_eq!(fk, 1);
    let fts5: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM pragma_compile_options WHERE compile_options = 'ENABLE_FTS5'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(fts5, 1, "bundled SQLite must have FTS5");
    db.checkpoint().unwrap();
}

#[test]
fn ensure_columns_fixup_is_idempotent() {
    let db = Db::open_in_memory().unwrap();
    ch_db::migrations::fixups(db.conn()).unwrap();
    ch_db::migrations::fixups(db.conn()).unwrap();
    assert!(ch_db::migrations::has_column(db.conn(), "message", "deleted_at").unwrap());
    assert!(!ch_db::migrations::has_column(db.conn(), "message", "nope").unwrap());
}
