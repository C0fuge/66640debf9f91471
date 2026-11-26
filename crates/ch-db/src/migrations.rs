//! Numbered, idempotent migrations stamped into `PRAGMA user_version`.
//!
//! Each migration is one SQL file in `migrations/` whose statements are all `IF NOT EXISTS`, so
//! re-running one against a database that already has its objects is harmless. Each runs in its
//! own transaction together with the `user_version` bump: a crash leaves the database at a whole
//! version. [`fixups`] runs on every open and is idempotent (add-missing-column style repairs).

use rusqlite::Connection;

use crate::{Error, Result};

/// One migration step.
pub struct Migration {
    /// Target `user_version` after this step.
    pub version: i64,
    /// Short name (the file stem).
    pub name: &'static str,
    /// The SQL to run.
    pub sql: &'static str,
}

/// All migrations in order. Append only; never edit a shipped one.
pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "0001_identity",
        sql: include_str!("../migrations/0001_identity.sql"),
    },
    Migration {
        version: 2,
        name: "0002_chats",
        sql: include_str!("../migrations/0002_chats.sql"),
    },
    Migration {
        version: 3,
        name: "0003_search",
        sql: include_str!("../migrations/0003_search.sql"),
    },
    Migration {
        version: 4,
        name: "0004_sync_outbox",
        sql: include_str!("../migrations/0004_sync_outbox.sql"),
    },
    Migration {
        version: 5,
        name: "0005_managed_sources",
        sql: include_str!("../migrations/0005_managed_sources.sql"),
    },
];

/// The newest schema version this build knows.
pub const LATEST: i64 = MIGRATIONS[MIGRATIONS.len() - 1].version;

/// Read `PRAGMA user_version`.
pub fn current_version(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row("PRAGMA user_version", [], |r| r.get(0))?)
}

/// Apply every migration with `version <= target` that has not run yet, then the fix-ups.
/// Fails with [`Error::SchemaTooNew`] when the file is ahead of `LATEST`.
pub fn run(conn: &Connection, target: i64) -> Result<()> {
    let found = current_version(conn)?;
    if found > LATEST {
        return Err(Error::SchemaTooNew {
            found,
            supported: LATEST,
        });
    }
    for m in MIGRATIONS
        .iter()
        .filter(|m| m.version > found && m.version <= target)
    {
        let tx = conn.unchecked_transaction()?;
        tx.execute_batch(m.sql)?;
        tx.pragma_update(None, "user_version", m.version)?;
        tx.commit()?;
    }
    if target >= LATEST {
        fixups(conn)?;
    }
    Ok(())
}

/// Columns that later builds added to existing tables: `(table, column, declaration)`. Adding one
/// here (instead of a new migration) is for emergencies; prefer a migration. Idempotent.
const ENSURED_COLUMNS: &[(&str, &str, &str)] = &[];

/// Idempotent repairs run on every open at `LATEST`.
pub fn fixups(conn: &Connection) -> Result<()> {
    for (table, column, decl) in ENSURED_COLUMNS {
        if !has_column(conn, table, column)? {
            conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {decl}"))?;
        }
    }
    Ok(())
}

/// Does `table` have `column`?
pub fn has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let names = stmt.query_map([], |r| r.get::<_, String>(1))?;
    for n in names {
        if n? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Names of all tables and virtual tables (tests).
pub fn table_names(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let rows = stmt.query_map([], |r| r.get(0))?;
    Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}
