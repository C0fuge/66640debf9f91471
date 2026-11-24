//! Plugins, accounts and settings.

use rusqlite::{Connection, params};

use crate::types::{AccountRow, PluginRow};
use crate::{Json, Millis, Result, now_millis};

use super::{changes, collect, opt};

const PLUGIN_COLS: &str = "plugin_id, name, version, api_version, install_dir, brand_color, icon_path, \
    manifest_json, enabled, installed, first_seen_at, last_seen_at";

fn plugin_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<PluginRow> {
    Ok(PluginRow {
        plugin_id: r.get(0)?,
        name: r.get(1)?,
        version: r.get(2)?,
        api_version: r.get(3)?,
        install_dir: r.get(4)?,
        brand_color: r.get(5)?,
        icon_path: r.get(6)?,
        manifest_json: r.get(7)?,
        enabled: r.get(8)?,
        installed: r.get(9)?,
        first_seen_at: r.get(10)?,
        last_seen_at: r.get(11)?,
    })
}

/// Insert or refresh a plugin's manifest facts. `enabled` is kept on update.
pub fn upsert_plugin(conn: &Connection, p: &PluginRow) -> Result<()> {
    conn.prepare_cached(&format!(
        "INSERT INTO plugin ({PLUGIN_COLS}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)
         ON CONFLICT(plugin_id) DO UPDATE SET name=excluded.name, version=excluded.version,
           api_version=excluded.api_version, install_dir=excluded.install_dir,
           brand_color=excluded.brand_color, icon_path=excluded.icon_path,
           manifest_json=excluded.manifest_json, installed=excluded.installed,
           last_seen_at=excluded.last_seen_at"
    ))?
    .execute(params![
        p.plugin_id,
        p.name,
        p.version,
        p.api_version,
        p.install_dir,
        p.brand_color,
        p.icon_path,
        p.manifest_json,
        p.enabled,
        p.installed,
        p.first_seen_at,
        p.last_seen_at
    ])?;
    Ok(())
}

/// One plugin.
pub fn get_plugin(conn: &Connection, plugin_id: &str) -> Result<Option<PluginRow>> {
    opt(
        conn,
        &format!("SELECT {PLUGIN_COLS} FROM plugin WHERE plugin_id = ?1"),
        &[&plugin_id],
        plugin_from_row,
    )
}

/// All plugins, by id.
pub fn list_plugins(conn: &Connection) -> Result<Vec<PluginRow>> {
    collect(
        conn,
        &format!("SELECT {PLUGIN_COLS} FROM plugin ORDER BY plugin_id"),
        &[],
        plugin_from_row,
    )
}

/// Enable or disable a plugin.
pub fn set_plugin_enabled(conn: &Connection, plugin_id: &str, enabled: bool) -> Result<bool> {
    conn.prepare_cached("UPDATE plugin SET enabled = ?2 WHERE plugin_id = ?1")?
        .execute(params![plugin_id, enabled])?;
    Ok(changes(conn) > 0)
}

const ACCOUNT_COLS: &str = "account_id, plugin_id, label, position, enabled, settings_json, state, \
    state_detail, reconnecting, state_at, me_user_id, created_at, updated_at";

fn account_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<AccountRow> {
    let settings: Json<serde_json::Map<String, serde_json::Value>> = r.get(5)?;
    Ok(AccountRow {
        account_id: r.get(0)?,
        plugin_id: r.get(1)?,
        label: r.get(2)?,
        position: r.get(3)?,
        enabled: r.get(4)?,
        settings: settings.0,
        state: r.get(6)?,
        state_detail: r.get(7)?,
        reconnecting: r.get(8)?,
        state_at: r.get(9)?,
        me_user_id: r.get(10)?,
        created_at: r.get(11)?,
        updated_at: r.get(12)?,
    })
}

/// Create an account. The plugin row must exist. Fails on a duplicate id.
pub fn create_account(
    conn: &Connection,
    account_id: &str,
    plugin_id: &str,
    label: Option<&str>,
    settings: &serde_json::Map<String, serde_json::Value>,
) -> Result<AccountRow> {
    let now = now_millis();
    let position: i64 = conn.query_row(
        "SELECT COALESCE(MAX(position), -1) + 1 FROM account",
        [],
        |r| r.get(0),
    )?;
    conn.prepare_cached(
        "INSERT INTO account (account_id, plugin_id, label, position, enabled, settings_json, state,
            reconnecting, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, 1, ?5, 'disconnected', 0, ?6, ?6)",
    )?
    .execute(params![account_id, plugin_id, label, position, Json(settings), now])?;
    Ok(get_account(conn, account_id)?.expect("just inserted"))
}

/// One account.
pub fn get_account(conn: &Connection, account_id: &str) -> Result<Option<AccountRow>> {
    opt(
        conn,
        &format!("SELECT {ACCOUNT_COLS} FROM account WHERE account_id = ?1"),
        &[&account_id],
        account_from_row,
    )
}

/// All accounts in display order.
pub fn list_accounts(conn: &Connection) -> Result<Vec<AccountRow>> {
    collect(
        conn,
        &format!("SELECT {ACCOUNT_COLS} FROM account ORDER BY position, account_id"),
        &[],
        account_from_row,
    )
}

/// Accounts of one plugin.
pub fn list_accounts_of_plugin(conn: &Connection, plugin_id: &str) -> Result<Vec<AccountRow>> {
    collect(
        conn,
        &format!(
            "SELECT {ACCOUNT_COLS} FROM account WHERE plugin_id = ?1 ORDER BY position, account_id"
        ),
        &[&plugin_id],
        account_from_row,
    )
}

/// Replace the account's settings object.
pub fn set_account_settings(
    conn: &Connection,
    account_id: &str,
    settings: &serde_json::Map<String, serde_json::Value>,
) -> Result<bool> {
    conn.prepare_cached(
        "UPDATE account SET settings_json = ?2, updated_at = ?3 WHERE account_id = ?1",
    )?
    .execute(params![account_id, Json(settings), now_millis()])?;
    Ok(changes(conn) > 0)
}

/// Record a `status` event's state.
pub fn set_account_state(
    conn: &Connection,
    account_id: &str,
    state: &str,
    detail: Option<&str>,
    reconnecting: bool,
    at: Millis,
) -> Result<bool> {
    conn.prepare_cached(
        "UPDATE account SET state = ?2, state_detail = ?3, reconnecting = ?4, state_at = ?5, updated_at = ?5
         WHERE account_id = ?1",
    )?
    .execute(params![account_id, state, detail, reconnecting, at])?;
    Ok(changes(conn) > 0)
}

/// Record which user this account is.
pub fn set_account_me(conn: &Connection, account_id: &str, me_user_id: &str) -> Result<bool> {
    conn.prepare_cached(
        "UPDATE account SET me_user_id = ?2, updated_at = ?3 WHERE account_id = ?1",
    )?
    .execute(params![account_id, me_user_id, now_millis()])?;
    Ok(changes(conn) > 0)
}

/// Label, position and enabled flag.
pub fn update_account_meta(
    conn: &Connection,
    account_id: &str,
    label: Option<&str>,
    position: i64,
    enabled: bool,
) -> Result<bool> {
    conn.prepare_cached(
        "UPDATE account SET label = ?2, position = ?3, enabled = ?4, updated_at = ?5 WHERE account_id = ?1",
    )?
    .execute(params![account_id, label, position, enabled, now_millis()])?;
    Ok(changes(conn) > 0)
}

/// Delete an account and everything that hangs off it (cascades). Folders are cleaned explicitly.
pub fn delete_account(conn: &Connection, account_id: &str) -> Result<bool> {
    conn.execute(
        "DELETE FROM folder_chat WHERE folder_account_id = ?1 OR account_id = ?1",
        [account_id],
    )?;
    conn.execute("DELETE FROM folder WHERE account_id = ?1", [account_id])?;
    conn.execute("DELETE FROM media_file WHERE account_id = ?1", [account_id])?;
    conn.execute("DELETE FROM account WHERE account_id = ?1", [account_id])?;
    Ok(changes(conn) > 0)
}

/// Read a setting (any JSON value).
pub fn get_setting<T: serde::de::DeserializeOwned>(
    conn: &Connection,
    key: &str,
) -> Result<Option<T>> {
    let v: Option<Json<T>> = opt(
        conn,
        "SELECT value_json FROM settings WHERE key = ?1",
        &[&key],
        |r| r.get(0),
    )?;
    Ok(v.map(|j| j.0))
}

/// Write a setting.
pub fn set_setting<T: serde::Serialize>(conn: &Connection, key: &str, value: &T) -> Result<()> {
    conn.prepare_cached(
        "INSERT INTO settings (key, value_json, updated_at) VALUES (?1, ?2, ?3)
         ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at",
    )?
    .execute(params![key, Json(value), now_millis()])?;
    Ok(())
}

/// Remove a setting.
pub fn delete_setting(conn: &Connection, key: &str) -> Result<bool> {
    conn.execute("DELETE FROM settings WHERE key = ?1", [key])?;
    Ok(changes(conn) > 0)
}

/// Every setting as raw JSON, by key.
pub fn list_settings(conn: &Connection) -> Result<Vec<(String, serde_json::Value)>> {
    collect(
        conn,
        "SELECT key, value_json FROM settings ORDER BY key",
        &[],
        |r| {
            let v: Json<serde_json::Value> = r.get(1)?;
            Ok((r.get(0)?, v.0))
        },
    )
}
