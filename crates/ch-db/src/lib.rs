//! Contact Hub storage: SQLite schema, migrations and typed queries.
//!
//! One [`Db`] wraps one `rusqlite::Connection` (WAL, foreign keys on, busy timeout). Opening runs
//! the numbered migrations in `migrations/` up to [`migrations::LATEST`] and stamps
//! `PRAGMA user_version`. The query layer (`queries::*`, re-exported here) is a thin typed layer
//! over SQL — not an ORM. The schema is documented in `SCHEMA.md`.
//!
//! Conventions: timestamps are `INTEGER` unix milliseconds (see [`Millis`]); booleans are `0/1`
//! with `NULL` = unknown; nested DTOs are stored as JSON text in the wire format via [`Json`].

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod migrations;
pub mod queries;
pub mod types;

use std::path::Path;
use std::time::Duration;

pub use rusqlite;
pub use rusqlite::Transaction;

pub use queries::*;
pub use types::*;

/// Storage errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// SQLite returned an error.
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// A JSON column could not be (de)serialised.
    #[error("json column: {0}")]
    Json(#[from] serde_json::Error),
    /// The database on disk is newer than this build knows.
    #[error("database schema version {found} is newer than supported {supported}")]
    SchemaTooNew {
        /// `PRAGMA user_version` found on disk.
        found: i64,
        /// The latest migration this build has.
        supported: i64,
    },
    /// A caller passed something the schema cannot hold.
    #[error("{0}")]
    Invalid(String),
}

/// `Result` with this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// Unix milliseconds, the on-disk timestamp form.
pub type Millis = i64;

/// Now, in unix milliseconds.
pub fn now_millis() -> Millis {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Convert a protocol timestamp to the on-disk form.
pub fn ts_to_millis(t: ch_protocol::Timestamp) -> Millis {
    t.unix_millis()
}

/// Convert the on-disk form back to a protocol timestamp.
pub fn millis_to_ts(ms: Millis) -> Option<ch_protocol::Timestamp> {
    ch_protocol::Timestamp::from_unix_millis(ms)
}

/// A JSON-encoded column holding `T`.
#[derive(Debug, Clone, PartialEq)]
pub struct Json<T>(pub T);

impl<T: serde::Serialize> rusqlite::ToSql for Json<T> {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        let s = serde_json::to_string(&self.0)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        Ok(rusqlite::types::ToSqlOutput::from(s))
    }
}

impl<T: serde::de::DeserializeOwned> rusqlite::types::FromSql for Json<T> {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let s = value.as_str()?;
        serde_json::from_str(s)
            .map(Json)
            .map_err(|e| rusqlite::types::FromSqlError::Other(Box::new(e)))
    }
}

/// Serialise an optional value to an optional JSON string (for `Option<T>` columns).
pub fn json_opt<T: serde::Serialize>(v: &Option<T>) -> Result<Option<String>> {
    Ok(match v {
        Some(v) => Some(serde_json::to_string(v)?),
        None => None,
    })
}

/// Parse an optional JSON column.
pub fn parse_json_opt<T: serde::de::DeserializeOwned>(s: Option<String>) -> Result<Option<T>> {
    Ok(match s {
        Some(s) => Some(serde_json::from_str(&s)?),
        None => None,
    })
}

/// One open database.
pub struct Db {
    conn: rusqlite::Connection,
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db").finish_non_exhaustive()
    }
}

impl Db {
    /// Open (or create) the database at `path`, apply pragmas and run pending migrations.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = rusqlite::Connection::open(path)?;
        Self::init(conn)
    }

    /// An in-memory database with the full schema (tests).
    pub fn open_in_memory() -> Result<Self> {
        let conn = rusqlite::Connection::open_in_memory()?;
        Self::init(conn)
    }

    /// Open without running migrations (migration tests only). Pragmas are still applied.
    pub fn open_unmigrated(path: impl AsRef<Path>) -> Result<Self> {
        let conn = rusqlite::Connection::open(path)?;
        Self::pragmas(&conn)?;
        Ok(Self { conn })
    }

    fn init(conn: rusqlite::Connection) -> Result<Self> {
        Self::pragmas(&conn)?;
        let db = Self { conn };
        migrations::run(&db.conn, migrations::LATEST)?;
        Ok(db)
    }

    fn pragmas(conn: &rusqlite::Connection) -> Result<()> {
        conn.busy_timeout(Duration::from_secs(5))?;
        // WAL is a no-op on :memory: databases; ignore its result there.
        let _ = conn.pragma_update(None, "journal_mode", "WAL");
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "temp_store", "MEMORY")?;
        Ok(())
    }

    /// The raw connection, for queries this crate does not wrap.
    pub fn conn(&self) -> &rusqlite::Connection {
        &self.conn
    }

    /// Begin a write transaction. Reserve the WAL writer before reading so that a concurrent
    /// commit cannot invalidate this transaction's snapshot. Commit with [`Transaction::commit`].
    /// Dropping the transaction rolls it back.
    pub fn transaction(&self) -> Result<Transaction<'_>> {
        Ok(Transaction::new_unchecked(
            &self.conn,
            rusqlite::TransactionBehavior::Immediate,
        )?)
    }

    /// Current `PRAGMA user_version`.
    pub fn schema_version(&self) -> Result<i64> {
        migrations::current_version(&self.conn)
    }

    /// Run migrations up to `target` (tests use this to step through them one by one).
    pub fn migrate_to(&self, target: i64) -> Result<()> {
        migrations::run(&self.conn, target)
    }

    /// `PRAGMA wal_checkpoint(TRUNCATE)`: fold the WAL into the main file (before backup/restore).
    pub fn checkpoint(&self) -> Result<()> {
        self.conn
            .pragma_update(None, "wal_checkpoint", "TRUNCATE")?;
        Ok(())
    }

    /// `PRAGMA integrity_check` == "ok"?
    pub fn integrity_ok(&self) -> Result<bool> {
        let s: String = self
            .conn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
        Ok(s == "ok")
    }
}
