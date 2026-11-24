//! Messages, edit history, reactions and the media-file index.

use ch_protocol::dto::{Reaction, Reactor};
use rusqlite::{Connection, params};

use crate::types::{MediaFileRow, MessageRow, MessageVersionRow, ReactionRow, reaction_key_text};
use crate::{Json, Millis, Result, json_opt};

use super::{changes, collect, opt};

const MSG_COLS: &str = "id, conversation_id, message_id, topic_id, direction, sent_at, sender_id, sender_name, \
    sender_title, text, entities_json, media_kind, media_json, reply_to_json, reply_to_id, forward_from_json, \
    grouped_id, reactors_json, poll_json, location_json, contact_card_json, dice_json, link_preview_json, \
    buttons_json, views, replies_count, replies_chat_id, effect_id, action_json, mentioned, edited, edited_at, \
    delivery, read_by_json, is_scheduled, is_pinned, silent, ttl_seconds, via_bot_id, ext_json, deleted_at, \
    deleted_by, has_link, created_at, updated_at";

fn j<T: serde::de::DeserializeOwned>(
    r: &rusqlite::Row<'_>,
    i: usize,
) -> rusqlite::Result<Option<T>> {
    let v: Option<Json<T>> = r.get(i)?;
    Ok(v.map(|j| j.0))
}

fn msg_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<MessageRow> {
    Ok(MessageRow {
        id: r.get(0)?,
        conversation_id: r.get(1)?,
        message_id: r.get(2)?,
        topic_id: r.get(3)?,
        direction: r.get(4)?,
        sent_at: r.get(5)?,
        sender_id: r.get(6)?,
        sender_name: r.get(7)?,
        sender_title: r.get(8)?,
        text: r.get(9)?,
        entities: j(r, 10)?,
        // 11 = media_kind (derived)
        media: j(r, 12)?,
        reply_to: j(r, 13)?,
        // 14 = reply_to_id (derived)
        forward_from: j(r, 15)?,
        grouped_id: r.get(16)?,
        reactors: j(r, 17)?,
        poll: j(r, 18)?,
        location: j(r, 19)?,
        contact_card: j(r, 20)?,
        dice: j(r, 21)?,
        link_preview: j(r, 22)?,
        buttons: j(r, 23)?,
        views: r.get(24)?,
        replies_count: r.get(25)?,
        replies_chat_id: r.get(26)?,
        effect_id: r.get(27)?,
        action: j(r, 28)?,
        mentioned: r.get(29)?,
        edited: r.get(30)?,
        edited_at: r.get(31)?,
        delivery: r.get(32)?,
        read_by: j(r, 33)?,
        is_scheduled: r.get(34)?,
        is_pinned: r.get(35)?,
        silent: r.get(36)?,
        ttl_seconds: r.get(37)?,
        via_bot_id: r.get(38)?,
        ext: j(r, 39)?,
        deleted_at: r.get(40)?,
        deleted_by: r.get(41)?,
        has_link: r.get(42)?,
        created_at: r.get(43)?,
        updated_at: r.get(44)?,
    })
}

/// One message by platform key.
pub fn get_message(
    conn: &Connection,
    conversation_id: i64,
    message_id: &str,
) -> Result<Option<MessageRow>> {
    opt(
        conn,
        &format!("SELECT {MSG_COLS} FROM message WHERE conversation_id = ?1 AND message_id = ?2"),
        &[&conversation_id, &message_id],
        msg_from_row,
    )
}

/// One message by rowid.
pub fn get_message_by_id(conn: &Connection, id: i64) -> Result<Option<MessageRow>> {
    opt(
        conn,
        &format!("SELECT {MSG_COLS} FROM message WHERE id = ?1"),
        &[&id],
        msg_from_row,
    )
}

/// Messages with this platform id in any DM of the account (for `messages.deleted` without `chat_id`).
pub fn find_messages_in_dms(
    conn: &Connection,
    account_id: &str,
    message_id: &str,
) -> Result<Vec<MessageRow>> {
    collect(
        conn,
        &format!(
            "SELECT {} FROM message m JOIN conversation c ON c.id = m.conversation_id
             WHERE c.account_id = ?1 AND c.kind = 'dm' AND m.message_id = ?2",
            MSG_COLS
                .split(", ")
                .map(|c| format!("m.{c}"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        &[&account_id, &message_id],
        msg_from_row,
    )
}

/// Insert a message (`id` ignored). Derived columns are computed here. Returns the rowid.
pub fn insert_message(conn: &Connection, m: &MessageRow) -> Result<i64> {
    conn.prepare_cached(&format!(
        "INSERT INTO message ({}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,\
         ?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31,?32,?33,?34,?35,?36,?37,?38,?39,?40,?41,?42,?43,?44)",
        &MSG_COLS[4..]
    ))?
    .execute(params![
        m.conversation_id,
        m.message_id,
        m.topic_id,
        m.direction,
        m.sent_at,
        m.sender_id,
        m.sender_name,
        m.sender_title,
        m.text,
        json_opt(&m.entities)?,
        m.media_kind(),
        json_opt(&m.media)?,
        json_opt(&m.reply_to)?,
        m.reply_to_id(),
        json_opt(&m.forward_from)?,
        m.grouped_id,
        json_opt(&m.reactors)?,
        json_opt(&m.poll)?,
        json_opt(&m.location)?,
        json_opt(&m.contact_card)?,
        json_opt(&m.dice)?,
        json_opt(&m.link_preview)?,
        json_opt(&m.buttons)?,
        m.views,
        m.replies_count,
        m.replies_chat_id,
        m.effect_id,
        json_opt(&m.action)?,
        m.mentioned,
        m.edited,
        m.edited_at,
        m.delivery,
        json_opt(&m.read_by)?,
        m.is_scheduled,
        m.is_pinned,
        m.silent,
        m.ttl_seconds,
        m.via_bot_id,
        json_opt(&m.ext)?,
        m.deleted_at,
        m.deleted_by,
        m.compute_has_link(),
        m.created_at,
        m.updated_at
    ])?;
    Ok(conn.last_insert_rowid())
}

/// Write every mutable column of a message (by `m.id`), including a re-key of `message_id`.
pub fn update_message(conn: &Connection, m: &MessageRow) -> Result<bool> {
    conn.prepare_cached(
        "UPDATE message SET message_id=?2, topic_id=?3, direction=?4, sent_at=?5, sender_id=?6, sender_name=?7,
            sender_title=?8, text=?9, entities_json=?10, media_kind=?11, media_json=?12, reply_to_json=?13,
            reply_to_id=?14, forward_from_json=?15, grouped_id=?16, reactors_json=?17, poll_json=?18,
            location_json=?19, contact_card_json=?20, dice_json=?21, link_preview_json=?22, buttons_json=?23,
            views=?24, replies_count=?25, replies_chat_id=?26, effect_id=?27, action_json=?28, mentioned=?29,
            edited=?30, edited_at=?31, delivery=?32, read_by_json=?33, is_scheduled=?34, is_pinned=?35,
            silent=?36, ttl_seconds=?37, via_bot_id=?38, ext_json=?39, deleted_at=?40, deleted_by=?41,
            has_link=?42, updated_at=?43
         WHERE id = ?1",
    )?
    .execute(params![
        m.id,
        m.message_id,
        m.topic_id,
        m.direction,
        m.sent_at,
        m.sender_id,
        m.sender_name,
        m.sender_title,
        m.text,
        json_opt(&m.entities)?,
        m.media_kind(),
        json_opt(&m.media)?,
        json_opt(&m.reply_to)?,
        m.reply_to_id(),
        json_opt(&m.forward_from)?,
        m.grouped_id,
        json_opt(&m.reactors)?,
        json_opt(&m.poll)?,
        json_opt(&m.location)?,
        json_opt(&m.contact_card)?,
        json_opt(&m.dice)?,
        json_opt(&m.link_preview)?,
        json_opt(&m.buttons)?,
        m.views,
        m.replies_count,
        m.replies_chat_id,
        m.effect_id,
        json_opt(&m.action)?,
        m.mentioned,
        m.edited,
        m.edited_at,
        m.delivery,
        json_opt(&m.read_by)?,
        m.is_scheduled,
        m.is_pinned,
        m.silent,
        m.ttl_seconds,
        m.via_bot_id,
        json_opt(&m.ext)?,
        m.deleted_at,
        m.deleted_by,
        m.compute_has_link(),
        m.updated_at
    ])?;
    Ok(changes(conn) > 0)
}

/// Hard-delete a message row (versions and reactions cascade; FTS via trigger).
pub fn delete_message(conn: &Connection, id: i64) -> Result<bool> {
    conn.prepare_cached("DELETE FROM message WHERE id = ?1")?
        .execute([id])?;
    Ok(changes(conn) > 0)
}

/// Tombstone a message: keep the content, mark who deleted it.
pub fn tombstone_message(conn: &Connection, id: i64, by: &str, at: Millis) -> Result<bool> {
    conn.prepare_cached(
        "UPDATE message SET deleted_at = ?2, deleted_by = ?3, updated_at = ?2 WHERE id = ?1 AND deleted_at IS NULL",
    )?
    .execute(params![id, at, by])?;
    Ok(changes(conn) > 0)
}

/// A page of a thread, newest first: messages with `sent_at < before` (or all), limit `limit`.
/// `topic_id`: `None` = whole chat. Tombstones are included when `include_deleted`.
pub fn list_messages(
    conn: &Connection,
    conversation_id: i64,
    topic_id: Option<&str>,
    before: Option<(Millis, i64)>,
    limit: u32,
    include_deleted: bool,
) -> Result<Vec<MessageRow>> {
    let (b_at, b_id) = match before {
        Some((at, id)) => (Some(at), Some(id)),
        None => (None, None),
    };
    collect(
        conn,
        &format!(
            "SELECT {MSG_COLS} FROM message
             WHERE conversation_id = ?1
               AND (?2 IS NULL OR topic_id = ?2)
               AND (?3 IS NULL OR sent_at < ?3 OR (sent_at = ?3 AND id < ?4))
               AND (?5 = 1 OR deleted_at IS NULL)
               AND is_scheduled = 0
             ORDER BY sent_at DESC, id DESC LIMIT ?6"
        ),
        &[
            &conversation_id,
            &topic_id,
            &b_at,
            &b_id,
            &include_deleted,
            &limit,
        ],
        msg_from_row,
    )
}

/// Newest live message of a chat (for the chat-list preview).
pub fn latest_message(conn: &Connection, conversation_id: i64) -> Result<Option<MessageRow>> {
    opt(
        conn,
        &format!(
            "SELECT {MSG_COLS} FROM message WHERE conversation_id = ?1 AND deleted_at IS NULL AND is_scheduled = 0
             ORDER BY sent_at DESC, id DESC LIMIT 1"
        ),
        &[&conversation_id],
        msg_from_row,
    )
}

/// Scheduled messages of a chat, soonest first.
pub fn list_scheduled(conn: &Connection, conversation_id: i64) -> Result<Vec<MessageRow>> {
    collect(
        conn,
        &format!(
            "SELECT {MSG_COLS} FROM message WHERE conversation_id = ?1 AND is_scheduled = 1 AND deleted_at IS NULL
             ORDER BY sent_at, id"
        ),
        &[&conversation_id],
        msg_from_row,
    )
}

/// Pinned messages of a chat, newest first.
pub fn list_pinned(conn: &Connection, conversation_id: i64) -> Result<Vec<MessageRow>> {
    collect(
        conn,
        &format!(
            "SELECT {MSG_COLS} FROM message WHERE conversation_id = ?1 AND is_pinned = 1 AND deleted_at IS NULL
             ORDER BY sent_at DESC, id DESC"
        ),
        &[&conversation_id],
        msg_from_row,
    )
}

/// Messages of an album.
pub fn list_album(
    conn: &Connection,
    conversation_id: i64,
    grouped_id: &str,
) -> Result<Vec<MessageRow>> {
    collect(
        conn,
        &format!(
            "SELECT {MSG_COLS} FROM message WHERE conversation_id = ?1 AND grouped_id = ?2 ORDER BY sent_at, id"
        ),
        &[&conversation_id, &grouped_id],
        msg_from_row,
    )
}

/// Number of messages (live) in a chat.
pub fn count_messages(conn: &Connection, conversation_id: i64) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM message WHERE conversation_id = ?1 AND deleted_at IS NULL",
        [conversation_id],
        |r| r.get(0),
    )?)
}

/// Mark outgoing messages up to (and including) `sent_at` as read by the peer. Returns how many changed.
pub fn mark_outgoing_read_until(
    conn: &Connection,
    conversation_id: i64,
    topic_id: Option<&str>,
    sent_at: Millis,
    now: Millis,
) -> Result<usize> {
    conn.prepare_cached(
        "UPDATE message SET delivery = 'read', updated_at = ?4
         WHERE conversation_id = ?1 AND (?2 IS NULL OR topic_id = ?2) AND direction = 'out'
           AND sent_at <= ?3 AND (delivery IS NULL OR delivery IN ('sending', 'sent', 'delivered'))",
    )?
    .execute(params![conversation_id, topic_id, sent_at, now])?;
    Ok(changes(conn))
}

/// Set / clear the pin flag directly.
pub fn set_message_pinned(conn: &Connection, id: i64, pinned: bool, now: Millis) -> Result<bool> {
    conn.prepare_cached("UPDATE message SET is_pinned = ?2, updated_at = ?3 WHERE id = ?1")?
        .execute(params![id, pinned, now])?;
    Ok(changes(conn) > 0)
}

/// Unpin every message of a chat.
pub fn unpin_all(conn: &Connection, conversation_id: i64, now: Millis) -> Result<usize> {
    conn.prepare_cached(
        "UPDATE message SET is_pinned = 0, updated_at = ?2 WHERE conversation_id = ?1 AND is_pinned = 1",
    )?
    .execute(params![conversation_id, now])?;
    Ok(changes(conn))
}

// ---- message_version -----------------------------------------------------------------------

fn version_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<MessageVersionRow> {
    Ok(MessageVersionRow {
        id: r.get(0)?,
        message_id: r.get(1)?,
        text: r.get(2)?,
        entities: j(r, 3)?,
        media: j(r, 4)?,
        edited_at: r.get(5)?,
        replaced_at: r.get(6)?,
    })
}

/// Snapshot the content a message currently holds (call *before* overwriting it).
pub fn insert_message_version(
    conn: &Connection,
    m: &MessageRow,
    replaced_at: Millis,
) -> Result<i64> {
    conn.prepare_cached(
        "INSERT INTO message_version (message_id, text, entities_json, media_json, edited_at, replaced_at)
         VALUES (?1,?2,?3,?4,?5,?6)",
    )?
    .execute(params![
        m.id,
        m.text,
        json_opt(&m.entities)?,
        json_opt(&m.media)?,
        m.edited_at,
        replaced_at
    ])?;
    Ok(conn.last_insert_rowid())
}

/// Edit history of a message, oldest first.
pub fn list_message_versions(
    conn: &Connection,
    message_rowid: i64,
) -> Result<Vec<MessageVersionRow>> {
    collect(
        conn,
        "SELECT id, message_id, text, entities_json, media_json, edited_at, replaced_at
         FROM message_version WHERE message_id = ?1 ORDER BY replaced_at, id",
        &[&message_rowid],
        version_from_row,
    )
}

/// Drop a message's edit history.
pub fn delete_message_versions(conn: &Connection, message_rowid: i64) -> Result<usize> {
    conn.prepare_cached("DELETE FROM message_version WHERE message_id = ?1")?
        .execute([message_rowid])?;
    Ok(changes(conn))
}

// ---- message_reaction ----------------------------------------------------------------------

fn reaction_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<ReactionRow> {
    let key_json: Json<_> = r.get(2)?;
    Ok(ReactionRow {
        message_id: r.get(0)?,
        key: r.get(1)?,
        key_json: key_json.0,
        count: r.get(3)?,
        mine: r.get(4)?,
        my_count: r.get(5)?,
        position: r.get(6)?,
    })
}

/// Current reactions of a message, in reported order.
pub fn list_reactions(conn: &Connection, message_rowid: i64) -> Result<Vec<ReactionRow>> {
    collect(
        conn,
        "SELECT message_id, key, key_json, count, mine, my_count, position
         FROM message_reaction WHERE message_id = ?1 ORDER BY position, key",
        &[&message_rowid],
        reaction_from_row,
    )
}

/// Current reactions as wire DTOs.
pub fn reactions_dto(conn: &Connection, message_rowid: i64) -> Result<Vec<Reaction>> {
    Ok(list_reactions(conn, message_rowid)?
        .iter()
        .map(ReactionRow::to_dto)
        .collect())
}

/// Replace the reaction set of a message wholesale. Returns `true` when anything changed.
pub fn set_reactions(
    conn: &Connection,
    message_rowid: i64,
    reactions: &[Reaction],
) -> Result<bool> {
    let before = reactions_dto(conn, message_rowid)?;
    if before == reactions {
        return Ok(false);
    }
    conn.prepare_cached("DELETE FROM message_reaction WHERE message_id = ?1")?
        .execute([message_rowid])?;
    let mut ins = conn.prepare_cached(
        "INSERT OR REPLACE INTO message_reaction (message_id, key, key_json, count, mine, my_count, position)
         VALUES (?1,?2,?3,?4,?5,?6,?7)",
    )?;
    for (i, r) in reactions.iter().enumerate() {
        ins.execute(params![
            message_rowid,
            reaction_key_text(&r.key),
            Json(&r.key),
            r.count as i64,
            r.mine,
            r.my_count.map(|c| c as i64),
            i as i64
        ])?;
    }
    Ok(true)
}

/// Replace the reactor list (newest first) stored on the message.
pub fn set_reactors(
    conn: &Connection,
    message_rowid: i64,
    reactors: &[Reactor],
    now: Millis,
) -> Result<()> {
    conn.prepare_cached("UPDATE message SET reactors_json = ?2, updated_at = ?3 WHERE id = ?1")?
        .execute(params![message_rowid, Json(reactors), now])?;
    Ok(())
}

// ---- media_file ----------------------------------------------------------------------------

fn media_file_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<MediaFileRow> {
    Ok(MediaFileRow {
        path: r.get(0)?,
        account_id: r.get(1)?,
        kind: r.get(2)?,
        message_id: r.get(3)?,
        media_kind: r.get(4)?,
        bytes: r.get(5)?,
        created_at: r.get(6)?,
        last_access_at: r.get(7)?,
    })
}

/// Record (or touch) a cache file.
pub fn upsert_media_file(conn: &Connection, f: &MediaFileRow) -> Result<()> {
    conn.prepare_cached(
        "INSERT INTO media_file (path, account_id, kind, message_id, media_kind, bytes, created_at, last_access_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8)
         ON CONFLICT(path) DO UPDATE SET account_id=excluded.account_id, kind=excluded.kind,
           message_id=COALESCE(excluded.message_id, media_file.message_id),
           media_kind=COALESCE(excluded.media_kind, media_file.media_kind),
           bytes=COALESCE(excluded.bytes, media_file.bytes), last_access_at=excluded.last_access_at",
    )?
    .execute(params![
        f.path,
        f.account_id,
        f.kind,
        f.message_id,
        f.media_kind,
        f.bytes,
        f.created_at,
        f.last_access_at
    ])?;
    Ok(())
}

/// Forget a cache file (after deleting it from disk).
pub fn delete_media_file(conn: &Connection, path: &str) -> Result<bool> {
    conn.prepare_cached("DELETE FROM media_file WHERE path = ?1")?
        .execute([path])?;
    Ok(changes(conn) > 0)
}

/// Cache files of an account, least recently used first (for eviction).
pub fn list_media_files_lru(
    conn: &Connection,
    account_id: &str,
    limit: u32,
) -> Result<Vec<MediaFileRow>> {
    collect(
        conn,
        "SELECT path, account_id, kind, message_id, media_kind, bytes, created_at, last_access_at
         FROM media_file WHERE account_id = ?1 ORDER BY last_access_at, path LIMIT ?2",
        &[&account_id, &limit],
        media_file_from_row,
    )
}

/// Total recorded bytes of an account's cache (`kind`: `None` = all).
pub fn media_cache_bytes(conn: &Connection, account_id: &str, kind: Option<&str>) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT COALESCE(SUM(bytes), 0) FROM media_file WHERE account_id = ?1 AND (?2 IS NULL OR kind = ?2)",
        params![account_id, kind],
        |r| r.get(0),
    )?)
}

/// Files recorded for a message.
pub fn media_files_of_message(conn: &Connection, message_rowid: i64) -> Result<Vec<MediaFileRow>> {
    collect(
        conn,
        "SELECT path, account_id, kind, message_id, media_kind, bytes, created_at, last_access_at
         FROM media_file WHERE message_id = ?1 ORDER BY kind, path",
        &[&message_rowid],
        media_file_from_row,
    )
}
