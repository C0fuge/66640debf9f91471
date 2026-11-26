//! Persons, categories and platform handles (identity across platforms).
//!
//! Chats are never merged across platforms; only the person (card, categories) is shared.

use rusqlite::{Connection, params};

use crate::types::{CategoryRow, HandleRow, PersonRow};
use crate::{Json, Result, json_opt, now_millis, parse_json_opt};

use super::{changes, collect, opt};
use super::managed::{RefConsumer, RefWrite};

// ---- person --------------------------------------------------------------------------------

fn person_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<PersonRow> {
    Ok(PersonRow {
        id: r.get(0)?,
        display_name: r.get(1)?,
        notes: r.get(2)?,
        created_at: r.get(3)?,
        updated_at: r.get(4)?,
    })
}

const PERSON_COLS: &str = "id, display_name, notes, created_at, updated_at";

/// Create a person.
pub fn create_person(conn: &Connection, display_name: Option<&str>) -> Result<i64> {
    let now = now_millis();
    conn.prepare_cached(
        "INSERT INTO person (display_name, created_at, updated_at) VALUES (?1, ?2, ?2)",
    )?
    .execute(params![display_name, now])?;
    Ok(conn.last_insert_rowid())
}

/// One person.
pub fn get_person(conn: &Connection, id: i64) -> Result<Option<PersonRow>> {
    opt(
        conn,
        &format!("SELECT {PERSON_COLS} FROM person WHERE id = ?1"),
        &[&id],
        person_from_row,
    )
}

/// Rename / annotate a person.
pub fn update_person(
    conn: &Connection,
    id: i64,
    display_name: Option<&str>,
    notes: Option<&str>,
) -> Result<bool> {
    conn.prepare_cached(
        "UPDATE person SET display_name = ?2, notes = ?3, updated_at = ?4 WHERE id = ?1",
    )?
    .execute(params![id, display_name, notes, now_millis()])?;
    Ok(changes(conn) > 0)
}

/// Delete a person; its handles become unlinked (`ON DELETE SET NULL`).
pub fn delete_person(conn: &Connection, id: i64) -> Result<bool> {
    conn.execute("DELETE FROM person WHERE id = ?1", [id])?;
    Ok(changes(conn) > 0)
}

/// Move every handle and category of `from` onto `into`, then delete `from`.
pub fn merge_persons(conn: &Connection, from: i64, into: i64) -> Result<()> {
    if from == into {
        return Ok(());
    }
    conn.execute(
        "UPDATE platform_handle SET person_id = ?2, link_source = 'manual' WHERE person_id = ?1",
        [from, into],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO person_category (person_id, category_id)
         SELECT ?2, category_id FROM person_category WHERE person_id = ?1",
        [from, into],
    )?;
    conn.execute("DELETE FROM person WHERE id = ?1", [from])?;
    conn.execute(
        "UPDATE person SET updated_at = ?2 WHERE id = ?1",
        params![into, now_millis()],
    )?;
    Ok(())
}

/// Persons that have at least one handle, by display name.
pub fn list_persons(conn: &Connection) -> Result<Vec<PersonRow>> {
    collect(
        conn,
        &format!("SELECT {PERSON_COLS} FROM person ORDER BY display_name COLLATE NOCASE, id"),
        &[],
        person_from_row,
    )
}

// ---- category ------------------------------------------------------------------------------

fn category_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<CategoryRow> {
    Ok(CategoryRow {
        id: r.get(0)?,
        name: r.get(1)?,
        color: r.get(2)?,
        sort_order: r.get(3)?,
    })
}

/// Create a category (name is unique).
pub fn create_category(
    conn: &Connection,
    name: &str,
    color: Option<&str>,
    sort_order: i64,
) -> Result<i64> {
    conn.prepare_cached("INSERT INTO category (name, color, sort_order) VALUES (?1, ?2, ?3)")?
        .execute(params![name, color, sort_order])?;
    Ok(conn.last_insert_rowid())
}

/// All categories in order.
pub fn list_categories(conn: &Connection) -> Result<Vec<CategoryRow>> {
    collect(
        conn,
        "SELECT id, name, color, sort_order FROM category ORDER BY sort_order, id",
        &[],
        category_from_row,
    )
}

/// Rename / recolour / reorder.
pub fn update_category(
    conn: &Connection,
    id: i64,
    name: &str,
    color: Option<&str>,
    sort_order: i64,
) -> Result<bool> {
    conn.prepare_cached(
        "UPDATE category SET name = ?2, color = ?3, sort_order = ?4 WHERE id = ?1",
    )?
    .execute(params![id, name, color, sort_order])?;
    Ok(changes(conn) > 0)
}

/// Delete a category (memberships cascade).
pub fn delete_category(conn: &Connection, id: i64) -> Result<bool> {
    conn.execute("DELETE FROM category WHERE id = ?1", [id])?;
    Ok(changes(conn) > 0)
}

/// Tag / untag a person.
pub fn set_person_category(
    conn: &Connection,
    person_id: i64,
    category_id: i64,
    on: bool,
) -> Result<()> {
    if on {
        conn.prepare_cached(
            "INSERT OR IGNORE INTO person_category (person_id, category_id) VALUES (?1, ?2)",
        )?
        .execute([person_id, category_id])?;
    } else {
        conn.prepare_cached(
            "DELETE FROM person_category WHERE person_id = ?1 AND category_id = ?2",
        )?
        .execute([person_id, category_id])?;
    }
    Ok(())
}

/// Category ids of a person.
pub fn person_categories(conn: &Connection, person_id: i64) -> Result<Vec<i64>> {
    collect(
        conn,
        "SELECT category_id FROM person_category WHERE person_id = ?1 ORDER BY category_id",
        &[&person_id],
        |r| r.get(0),
    )
}

// ---- platform_handle -----------------------------------------------------------------------

const HANDLE_COLS: &str = "id, account_id, user_id, person_id, link_source, display_name, first_name, \
    last_name, username, phone, avatar_path, is_bot, is_contact, is_premium, is_verified, is_deleted, \
    bio, status_json, status_at, ext_json, first_seen_at, last_seen_at";

fn handle_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<HandleRow> {
    let status: Option<Json<_>> = r.get(17)?;
    let ext: Option<Json<_>> = r.get(19)?;
    Ok(HandleRow {
        id: r.get(0)?,
        account_id: r.get(1)?,
        user_id: r.get(2)?,
        person_id: r.get(3)?,
        link_source: r.get(4)?,
        display_name: r.get(5)?,
        first_name: r.get(6)?,
        last_name: r.get(7)?,
        username: r.get(8)?,
        phone: r.get(9)?,
        avatar_path: r.get(10)?,
        is_bot: r.get(11)?,
        is_contact: r.get(12)?,
        is_premium: r.get(13)?,
        is_verified: r.get(14)?,
        is_deleted: r.get(15)?,
        bio: r.get(16)?,
        status: status.map(|j| j.0),
        status_at: r.get(18)?,
        ext: ext.map(|j| j.0),
        first_seen_at: r.get(20)?,
        last_seen_at: r.get(21)?,
    })
}

/// One handle by platform key.
pub fn get_handle(conn: &Connection, account_id: &str, user_id: &str) -> Result<Option<HandleRow>> {
    opt(
        conn,
        &format!(
            "SELECT {HANDLE_COLS} FROM platform_handle WHERE account_id = ?1 AND user_id = ?2"
        ),
        &[&account_id, &user_id],
        handle_from_row,
    )
}

/// One handle by rowid.
pub fn get_handle_by_id(conn: &Connection, id: i64) -> Result<Option<HandleRow>> {
    opt(
        conn,
        &format!("SELECT {HANDLE_COLS} FROM platform_handle WHERE id = ?1"),
        &[&id],
        handle_from_row,
    )
}

/// Insert a handle (`id` ignored). Returns the rowid.
pub fn insert_handle(conn: &Connection, h: &HandleRow) -> Result<i64> {
    let write = RefWrite::begin(conn)?;
    conn.prepare_cached(&format!(
        "INSERT INTO platform_handle ({}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
        &HANDLE_COLS[4..] // without "id, "
    ))?
    .execute(params![
        h.account_id,
        h.user_id,
        h.person_id,
        h.link_source,
        h.display_name,
        h.first_name,
        h.last_name,
        h.username,
        h.phone,
        h.avatar_path,
        h.is_bot,
        h.is_contact,
        h.is_premium,
        h.is_verified,
        h.is_deleted,
        h.bio,
        json_opt(&h.status)?,
        h.status_at,
        json_opt(&h.ext)?,
        h.first_seen_at,
        h.last_seen_at
    ])?;
    let id = conn.last_insert_rowid();
    write.finish(RefConsumer::PlatformHandle, id)?;
    Ok(id)
}

/// Write every mutable column of a handle (by `h.id`).
pub fn update_handle(conn: &Connection, h: &HandleRow) -> Result<bool> {
    let write = RefWrite::begin(conn)?;
    conn.prepare_cached(
        "UPDATE platform_handle SET person_id=?2, link_source=?3, display_name=?4, first_name=?5,
            last_name=?6, username=?7, phone=?8, avatar_path=?9, is_bot=?10, is_contact=?11,
            is_premium=?12, is_verified=?13, is_deleted=?14, bio=?15, status_json=?16, status_at=?17,
            ext_json=?18, last_seen_at=?19
         WHERE id = ?1",
    )?
    .execute(params![
        h.id,
        h.person_id,
        h.link_source,
        h.display_name,
        h.first_name,
        h.last_name,
        h.username,
        h.phone,
        h.avatar_path,
        h.is_bot,
        h.is_contact,
        h.is_premium,
        h.is_verified,
        h.is_deleted,
        h.bio,
        json_opt(&h.status)?,
        h.status_at,
        json_opt(&h.ext)?,
        h.last_seen_at
    ])?;
    let changed = changes(conn) > 0;
    write.finish(RefConsumer::PlatformHandle, h.id)?;
    Ok(changed)
}

/// Just the presence snapshot.
pub fn set_handle_status(
    conn: &Connection,
    id: i64,
    status: &ch_protocol::dto::OnlineStatus,
    at: crate::Millis,
) -> Result<bool> {
    let write = RefWrite::begin(conn)?;
    conn.prepare_cached(
        "UPDATE platform_handle SET status_json = ?2, status_at = ?3, last_seen_at = ?3 WHERE id = ?1",
    )?
    .execute(params![id, Json(status), at])?;
    let changed = changes(conn) > 0;
    write.finish(RefConsumer::PlatformHandle, id)?;
    Ok(changed)
}

/// Just the avatar.
pub fn set_handle_avatar(conn: &Connection, id: i64, path: Option<&str>) -> Result<bool> {
    let write = RefWrite::begin(conn)?;
    conn.prepare_cached("UPDATE platform_handle SET avatar_path = ?2 WHERE id = ?1")?
        .execute(params![id, path])?;
    let changed = changes(conn) > 0;
    write.finish(RefConsumer::PlatformHandle, id)?;
    Ok(changed)
}

/// Link a handle to a person (or unlink with `None`), recording how.
pub fn link_handle(
    conn: &Connection,
    id: i64,
    person_id: Option<i64>,
    source: Option<&str>,
) -> Result<bool> {
    conn.prepare_cached(
        "UPDATE platform_handle SET person_id = ?2, link_source = ?3 WHERE id = ?1",
    )?
    .execute(params![id, person_id, source])?;
    Ok(changes(conn) > 0)
}

/// Handles of a person, across accounts.
pub fn handles_of_person(conn: &Connection, person_id: i64) -> Result<Vec<HandleRow>> {
    collect(
        conn,
        &format!(
            "SELECT {HANDLE_COLS} FROM platform_handle WHERE person_id = ?1 ORDER BY account_id, user_id"
        ),
        &[&person_id],
        handle_from_row,
    )
}

/// Handles with no person ("Unlinked handles" in the sidebar).
pub fn unlinked_handles(conn: &Connection) -> Result<Vec<HandleRow>> {
    collect(
        conn,
        &format!(
            "SELECT {HANDLE_COLS} FROM platform_handle WHERE person_id IS NULL ORDER BY account_id, display_name COLLATE NOCASE"
        ),
        &[],
        handle_from_row,
    )
}

/// Handles on *other* accounts with this phone (for safe auto-linking).
pub fn handles_by_phone(
    conn: &Connection,
    phone: &str,
    not_account: &str,
) -> Result<Vec<HandleRow>> {
    collect(
        conn,
        &format!("SELECT {HANDLE_COLS} FROM platform_handle WHERE phone = ?1 AND account_id <> ?2"),
        &[&phone, &not_account],
        handle_from_row,
    )
}

/// Handles on *other* accounts with this username (case-insensitive).
pub fn handles_by_username(
    conn: &Connection,
    username: &str,
    not_account: &str,
) -> Result<Vec<HandleRow>> {
    collect(
        conn,
        &format!(
            "SELECT {HANDLE_COLS} FROM platform_handle WHERE username = ?1 COLLATE NOCASE AND account_id <> ?2"
        ),
        &[&username, &not_account],
        handle_from_row,
    )
}

/// All handles of an account.
pub fn list_handles(conn: &Connection, account_id: &str) -> Result<Vec<HandleRow>> {
    collect(
        conn,
        &format!(
            "SELECT {HANDLE_COLS} FROM platform_handle WHERE account_id = ?1 ORDER BY display_name COLLATE NOCASE, user_id"
        ),
        &[&account_id],
        handle_from_row,
    )
}

/// Parse a stored `status_json` (helper for callers reading raw rows).
pub fn parse_status(s: Option<String>) -> Result<Option<ch_protocol::dto::OnlineStatus>> {
    parse_json_opt(s)
}
