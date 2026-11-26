//! Typed queries, grouped by area. Every function takes a `&Connection` so it works on a [`crate::Db`]
//! (`db.conn()`) or inside a [`crate::Transaction`] alike.

pub mod accounts;
pub mod chats;
pub mod identity;
pub mod messages;
pub mod managed;
pub mod search;
pub mod sync;

pub use accounts::*;
pub use chats::*;
pub use identity::*;
pub use messages::*;
pub use managed::*;
pub use search::*;
pub use sync::*;

use rusqlite::Connection;

use crate::Result;

/// `SELECT changes()` after the last statement.
pub(crate) fn changes(conn: &Connection) -> usize {
    conn.changes() as usize
}

/// Run `f` for every row of `sql` and collect.
pub(crate) fn collect<T>(
    conn: &Connection,
    sql: &str,
    params: &[&dyn rusqlite::ToSql],
    f: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> Result<Vec<T>> {
    let mut stmt = conn.prepare_cached(sql)?;
    let rows = stmt.query_map(params, f)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// `query_row` that yields `None` for no rows.
pub(crate) fn opt<T>(
    conn: &Connection,
    sql: &str,
    params: &[&dyn rusqlite::ToSql],
    f: impl FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> Result<Option<T>> {
    use rusqlite::OptionalExtension;
    let mut stmt = conn.prepare_cached(sql)?;
    Ok(stmt.query_row(params, f).optional()?)
}
