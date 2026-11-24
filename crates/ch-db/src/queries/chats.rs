//! Conversations, folders, topics, drafts and members.

use rusqlite::{Connection, params};

use crate::types::{ConversationRow, DraftRow, FolderRow, MemberRow, TopicRow};
use crate::{Json, Millis, Result, json_opt, now_millis};

use super::{changes, collect, opt};

// ---- conversation --------------------------------------------------------------------------

const CONV_COLS: &str = "id, account_id, chat_id, kind, title, peer_user_id, parent_chat_id, username, about, \
    archived, muted, mute_until, pinned, pin_order, unread_count, unread_mentions, unread_reactions, \
    marked_unread, read_inbox_max_id, read_outbox_max_id, last_message_at, is_self_chat, avatar_path, \
    member_count, can_post, is_forum, color_index, scheduled_count, auto_delete_seconds, slow_mode_seconds, \
    permissions_json, is_verified, is_scam, is_restricted, ext_json, is_gone, notify_override, notify_sound, \
    wallpaper, local_read_at, last_message_preview, last_message_kind, created_at, updated_at";

fn conv_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<ConversationRow> {
    let permissions: Option<Json<_>> = r.get(30)?;
    let ext: Option<Json<_>> = r.get(34)?;
    Ok(ConversationRow {
        id: r.get(0)?,
        account_id: r.get(1)?,
        chat_id: r.get(2)?,
        kind: r.get(3)?,
        title: r.get(4)?,
        peer_user_id: r.get(5)?,
        parent_chat_id: r.get(6)?,
        username: r.get(7)?,
        about: r.get(8)?,
        archived: r.get(9)?,
        muted: r.get(10)?,
        mute_until: r.get(11)?,
        pinned: r.get(12)?,
        pin_order: r.get(13)?,
        unread_count: r.get(14)?,
        unread_mentions: r.get(15)?,
        unread_reactions: r.get(16)?,
        marked_unread: r.get(17)?,
        read_inbox_max_id: r.get(18)?,
        read_outbox_max_id: r.get(19)?,
        last_message_at: r.get(20)?,
        is_self_chat: r.get(21)?,
        avatar_path: r.get(22)?,
        member_count: r.get(23)?,
        can_post: r.get(24)?,
        is_forum: r.get(25)?,
        color_index: r.get(26)?,
        scheduled_count: r.get(27)?,
        auto_delete_seconds: r.get(28)?,
        slow_mode_seconds: r.get(29)?,
        permissions: permissions.map(|j| j.0),
        is_verified: r.get(31)?,
        is_scam: r.get(32)?,
        is_restricted: r.get(33)?,
        ext: ext.map(|j| j.0),
        is_gone: r.get(35)?,
        notify_override: r.get(36)?,
        notify_sound: r.get(37)?,
        wallpaper: r.get(38)?,
        local_read_at: r.get(39)?,
        last_message_preview: r.get(40)?,
        last_message_kind: r.get(41)?,
        created_at: r.get(42)?,
        updated_at: r.get(43)?,
    })
}

/// One conversation by platform key.
pub fn get_conversation(
    conn: &Connection,
    account_id: &str,
    chat_id: &str,
) -> Result<Option<ConversationRow>> {
    opt(
        conn,
        &format!("SELECT {CONV_COLS} FROM conversation WHERE account_id = ?1 AND chat_id = ?2"),
        &[&account_id, &chat_id],
        conv_from_row,
    )
}

/// One conversation by rowid.
pub fn get_conversation_by_id(conn: &Connection, id: i64) -> Result<Option<ConversationRow>> {
    opt(
        conn,
        &format!("SELECT {CONV_COLS} FROM conversation WHERE id = ?1"),
        &[&id],
        conv_from_row,
    )
}

/// Rowid of a conversation by platform key (cheap lookup for hot paths).
pub fn conversation_rowid(
    conn: &Connection,
    account_id: &str,
    chat_id: &str,
) -> Result<Option<i64>> {
    opt(
        conn,
        "SELECT id FROM conversation WHERE account_id = ?1 AND chat_id = ?2",
        &[&account_id, &chat_id],
        |r| r.get(0),
    )
}

/// Insert a conversation (`id` ignored). Returns the rowid.
pub fn insert_conversation(conn: &Connection, c: &ConversationRow) -> Result<i64> {
    conn.prepare_cached(&format!(
        "INSERT INTO conversation ({}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,\
         ?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31,?32,?33,?34,?35,?36,?37,?38,?39,?40,?41,?42,?43)",
        &CONV_COLS[4..]
    ))?
    .execute(params![
        c.account_id,
        c.chat_id,
        c.kind,
        c.title,
        c.peer_user_id,
        c.parent_chat_id,
        c.username,
        c.about,
        c.archived,
        c.muted,
        c.mute_until,
        c.pinned,
        c.pin_order,
        c.unread_count,
        c.unread_mentions,
        c.unread_reactions,
        c.marked_unread,
        c.read_inbox_max_id,
        c.read_outbox_max_id,
        c.last_message_at,
        c.is_self_chat,
        c.avatar_path,
        c.member_count,
        c.can_post,
        c.is_forum,
        c.color_index,
        c.scheduled_count,
        c.auto_delete_seconds,
        c.slow_mode_seconds,
        json_opt(&c.permissions)?,
        c.is_verified,
        c.is_scam,
        c.is_restricted,
        json_opt(&c.ext)?,
        c.is_gone,
        c.notify_override,
        c.notify_sound,
        c.wallpaper,
        c.local_read_at,
        c.last_message_preview,
        c.last_message_kind,
        c.created_at,
        c.updated_at
    ])?;
    Ok(conn.last_insert_rowid())
}

/// Write every mutable column of a conversation (by `c.id`).
pub fn update_conversation(conn: &Connection, c: &ConversationRow) -> Result<bool> {
    conn.prepare_cached(
        "UPDATE conversation SET kind=?2, title=?3, peer_user_id=?4, parent_chat_id=?5, username=?6, about=?7,
            archived=?8, muted=?9, mute_until=?10, pinned=?11, pin_order=?12, unread_count=?13,
            unread_mentions=?14, unread_reactions=?15, marked_unread=?16, read_inbox_max_id=?17,
            read_outbox_max_id=?18, last_message_at=?19, is_self_chat=?20, avatar_path=?21, member_count=?22,
            can_post=?23, is_forum=?24, color_index=?25, scheduled_count=?26, auto_delete_seconds=?27,
            slow_mode_seconds=?28, permissions_json=?29, is_verified=?30, is_scam=?31, is_restricted=?32,
            ext_json=?33, is_gone=?34, notify_override=?35, notify_sound=?36, wallpaper=?37, local_read_at=?38,
            last_message_preview=?39, last_message_kind=?40, updated_at=?41
         WHERE id = ?1",
    )?
    .execute(params![
        c.id,
        c.kind,
        c.title,
        c.peer_user_id,
        c.parent_chat_id,
        c.username,
        c.about,
        c.archived,
        c.muted,
        c.mute_until,
        c.pinned,
        c.pin_order,
        c.unread_count,
        c.unread_mentions,
        c.unread_reactions,
        c.marked_unread,
        c.read_inbox_max_id,
        c.read_outbox_max_id,
        c.last_message_at,
        c.is_self_chat,
        c.avatar_path,
        c.member_count,
        c.can_post,
        c.is_forum,
        c.color_index,
        c.scheduled_count,
        c.auto_delete_seconds,
        c.slow_mode_seconds,
        json_opt(&c.permissions)?,
        c.is_verified,
        c.is_scam,
        c.is_restricted,
        json_opt(&c.ext)?,
        c.is_gone,
        c.notify_override,
        c.notify_sound,
        c.wallpaper,
        c.local_read_at,
        c.last_message_preview,
        c.last_message_kind,
        c.updated_at
    ])?;
    Ok(changes(conn) > 0)
}

/// Chat list of an account: live chats (not gone), newest first. `archived`: `None` = both.
pub fn list_conversations(
    conn: &Connection,
    account_id: &str,
    archived: Option<bool>,
) -> Result<Vec<ConversationRow>> {
    let sql = format!(
        "SELECT {CONV_COLS} FROM conversation WHERE account_id = ?1 AND is_gone = 0
           AND (?2 IS NULL OR archived = ?2)
         ORDER BY COALESCE(pinned, 0) DESC, pin_order, last_message_at DESC, id DESC"
    );
    collect(conn, &sql, &[&account_id, &archived], conv_from_row)
}

/// Every conversation of an account, gone ones included.
pub fn list_all_conversations(conn: &Connection, account_id: &str) -> Result<Vec<ConversationRow>> {
    collect(
        conn,
        &format!("SELECT {CONV_COLS} FROM conversation WHERE account_id = ?1 ORDER BY id"),
        &[&account_id],
        conv_from_row,
    )
}

/// Mark every conversation of the account not in `keep` as gone, and those in it as present.
/// Returns the chat ids that changed state (newly gone first, then newly back).
pub fn mark_conversations_synced(
    conn: &Connection,
    account_id: &str,
    keep: &[String],
    now: Millis,
) -> Result<(Vec<String>, Vec<String>)> {
    conn.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS synced_ids (chat_id TEXT PRIMARY KEY) WITHOUT ROWID;
         DELETE FROM synced_ids;",
    )?;
    {
        let mut ins =
            conn.prepare_cached("INSERT OR IGNORE INTO synced_ids (chat_id) VALUES (?1)")?;
        for id in keep {
            ins.execute([id])?;
        }
    }
    let gone: Vec<String> = collect(
        conn,
        "SELECT chat_id FROM conversation WHERE account_id = ?1 AND is_gone = 0
           AND chat_id NOT IN (SELECT chat_id FROM synced_ids)",
        &[&account_id],
        |r| r.get(0),
    )?;
    let back: Vec<String> = collect(
        conn,
        "SELECT chat_id FROM conversation WHERE account_id = ?1 AND is_gone = 1
           AND chat_id IN (SELECT chat_id FROM synced_ids)",
        &[&account_id],
        |r| r.get(0),
    )?;
    conn.execute(
        "UPDATE conversation SET is_gone = 1, updated_at = ?2 WHERE account_id = ?1 AND is_gone = 0
           AND chat_id NOT IN (SELECT chat_id FROM synced_ids)",
        params![account_id, now],
    )?;
    conn.execute(
        "UPDATE conversation SET is_gone = 0, updated_at = ?2 WHERE account_id = ?1 AND is_gone = 1
           AND chat_id IN (SELECT chat_id FROM synced_ids)",
        params![account_id, now],
    )?;
    conn.execute_batch("DELETE FROM synced_ids;")?;
    Ok((gone, back))
}

/// Local per-chat notification exception and sound.
pub fn set_conversation_notify(
    conn: &Connection,
    id: i64,
    notify_override: Option<&str>,
    notify_sound: Option<&str>,
) -> Result<bool> {
    conn.prepare_cached(
        "UPDATE conversation SET notify_override = ?2, notify_sound = ?3, updated_at = ?4 WHERE id = ?1",
    )?
    .execute(params![id, notify_override, notify_sound, now_millis()])?;
    Ok(changes(conn) > 0)
}

/// Denormalised chat-list preview.
pub fn set_conversation_last_message(
    conn: &Connection,
    id: i64,
    at: Option<Millis>,
    kind: Option<&str>,
    preview: Option<&str>,
) -> Result<bool> {
    conn.prepare_cached(
        "UPDATE conversation SET last_message_at = ?2, last_message_kind = ?3, last_message_preview = ?4,
            updated_at = ?5 WHERE id = ?1",
    )?
    .execute(params![id, at, kind, preview, now_millis()])?;
    Ok(changes(conn) > 0)
}

/// Delete a conversation and its messages (the user chose "delete chat" locally).
pub fn delete_conversation(conn: &Connection, id: i64) -> Result<bool> {
    conn.execute("DELETE FROM conversation WHERE id = ?1", [id])?;
    Ok(changes(conn) > 0)
}

// ---- folder --------------------------------------------------------------------------------

const FOLDER_COLS: &str = "id, account_id, folder_id, title, emoticon, position, rules_json, is_shared, ext_json, updated_at";

fn folder_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<FolderRow> {
    let rules: Option<Json<_>> = r.get(6)?;
    let ext: Option<Json<_>> = r.get(8)?;
    Ok(FolderRow {
        id: r.get(0)?,
        account_id: r.get(1)?,
        folder_id: r.get(2)?,
        title: r.get(3)?,
        emoticon: r.get(4)?,
        position: r.get(5)?,
        rules: rules.map(|j| j.0),
        is_shared: r.get(7)?,
        ext: ext.map(|j| j.0),
        updated_at: r.get(9)?,
    })
}

/// Insert or replace a folder's fields by `(account_id, folder_id)`. Returns the rowid.
pub fn upsert_folder(conn: &Connection, f: &FolderRow) -> Result<i64> {
    conn.prepare_cached(
        "INSERT INTO folder (account_id, folder_id, title, emoticon, position, rules_json, is_shared, ext_json, updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)
         ON CONFLICT(account_id, folder_id) DO UPDATE SET title=excluded.title, emoticon=excluded.emoticon,
           position=excluded.position, rules_json=excluded.rules_json, is_shared=excluded.is_shared,
           ext_json=excluded.ext_json, updated_at=excluded.updated_at",
    )?
    .execute(params![
        f.account_id,
        f.folder_id,
        f.title,
        f.emoticon,
        f.position,
        json_opt(&f.rules)?,
        f.is_shared,
        json_opt(&f.ext)?,
        f.updated_at
    ])?;
    Ok(opt(
        conn,
        "SELECT id FROM folder WHERE account_id = ?1 AND folder_id = ?2",
        &[&f.account_id, &f.folder_id],
        |r| r.get(0),
    )?
    .expect("just upserted"))
}

/// One folder.
pub fn get_folder(
    conn: &Connection,
    account_id: &str,
    folder_id: &str,
) -> Result<Option<FolderRow>> {
    opt(
        conn,
        &format!("SELECT {FOLDER_COLS} FROM folder WHERE account_id = ?1 AND folder_id = ?2"),
        &[&account_id, &folder_id],
        folder_from_row,
    )
}

/// Folders of an account (`""` = local folders), in bar order.
pub fn list_folders(conn: &Connection, account_id: &str) -> Result<Vec<FolderRow>> {
    collect(
        conn,
        &format!("SELECT {FOLDER_COLS} FROM folder WHERE account_id = ?1 ORDER BY position, id"),
        &[&account_id],
        folder_from_row,
    )
}

/// Delete the account's folders (and their memberships) whose `folder_id` is not in `keep`.
/// Returns how many went.
pub fn delete_folders_not_in(
    conn: &Connection,
    account_id: &str,
    keep: &[String],
) -> Result<usize> {
    let existing: Vec<String> = collect(
        conn,
        "SELECT folder_id FROM folder WHERE account_id = ?1",
        &[&account_id],
        |r| r.get(0),
    )?;
    let mut n = 0;
    for fid in existing {
        if !keep.contains(&fid) {
            delete_folder(conn, account_id, &fid)?;
            n += 1;
        }
    }
    // Memberships reported for folders the account no longer has.
    conn.execute(
        "DELETE FROM folder_chat WHERE folder_account_id = ?1
           AND folder_id NOT IN (SELECT folder_id FROM folder WHERE account_id = ?1)",
        [account_id],
    )?;
    Ok(n)
}

/// Delete one folder (local: `account_id = ""`) and its memberships.
pub fn delete_folder(conn: &Connection, account_id: &str, folder_id: &str) -> Result<bool> {
    conn.prepare_cached("DELETE FROM folder_chat WHERE folder_account_id = ?1 AND folder_id = ?2")?
        .execute(params![account_id, folder_id])?;
    conn.prepare_cached("DELETE FROM folder WHERE account_id = ?1 AND folder_id = ?2")?
        .execute(params![account_id, folder_id])?;
    Ok(changes(conn) > 0)
}

/// Replace the explicit chat list of a folder with `(account_id, chat_id)` pairs.
pub fn set_folder_chats(
    conn: &Connection,
    folder_account_id: &str,
    folder_id: &str,
    chats: &[(String, String)],
) -> Result<()> {
    conn.prepare_cached("DELETE FROM folder_chat WHERE folder_account_id = ?1 AND folder_id = ?2 AND source = 'folder'")?
        .execute(params![folder_account_id, folder_id])?;
    let mut ins = conn.prepare_cached(
        "INSERT OR IGNORE INTO folder_chat (folder_account_id, folder_id, account_id, chat_id, source) VALUES (?1, ?2, ?3, ?4, 'folder')",
    )?;
    for (account_id, chat_id) in chats {
        ins.execute(params![folder_account_id, folder_id, account_id, chat_id])?;
    }
    Ok(())
}

/// Add / remove one chat in a folder by hand (source `local`; for hub folders and manual edits).
pub fn set_folder_chat(
    conn: &Connection,
    folder_account_id: &str,
    folder_id: &str,
    account_id: &str,
    chat_id: &str,
    on: bool,
) -> Result<()> {
    if on {
        conn.prepare_cached(
            "INSERT OR IGNORE INTO folder_chat (folder_account_id, folder_id, account_id, chat_id, source) VALUES (?1, ?2, ?3, ?4, 'local')",
        )?
        .execute(params![folder_account_id, folder_id, account_id, chat_id])?;
    } else {
        conn.prepare_cached(
            "DELETE FROM folder_chat WHERE folder_account_id = ?1 AND folder_id = ?2 AND account_id = ?3 AND chat_id = ?4 AND source = 'local'",
        )?
        .execute(params![folder_account_id, folder_id, account_id, chat_id])?;
    }
    Ok(())
}

/// Set exactly which platform folders of its own account a chat is in (`Conversation.folder_ids`).
/// Folders the core has not seen yet are kept as memberships and show up once `folders.synced`
/// delivers them.
pub fn set_chat_platform_folders(
    conn: &Connection,
    account_id: &str,
    chat_id: &str,
    folder_ids: &[String],
) -> Result<()> {
    conn.prepare_cached(
        "DELETE FROM folder_chat WHERE folder_account_id = ?1 AND account_id = ?1 AND chat_id = ?2 AND source = 'chat'",
    )?
    .execute(params![account_id, chat_id])?;
    let mut ins = conn.prepare_cached(
        "INSERT OR IGNORE INTO folder_chat (folder_account_id, folder_id, account_id, chat_id, source) VALUES (?1, ?2, ?1, ?3, 'chat')",
    )?;
    for fid in folder_ids {
        ins.execute(params![account_id, fid, chat_id])?;
    }
    Ok(())
}

/// Chats the folder itself lists (`Folder.chat_ids`) as `(account_id, chat_id)`.
pub fn folder_chats(
    conn: &Connection,
    folder_account_id: &str,
    folder_id: &str,
) -> Result<Vec<(String, String)>> {
    collect(
        conn,
        "SELECT account_id, chat_id FROM folder_chat WHERE folder_account_id = ?1 AND folder_id = ?2 AND source = 'folder'
         ORDER BY account_id, chat_id",
        &[&folder_account_id, &folder_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
}

/// Chats in a folder from any source (folder list, chat's own folder_ids, local), de-duplicated.
pub fn folder_members(
    conn: &Connection,
    folder_account_id: &str,
    folder_id: &str,
) -> Result<Vec<(String, String)>> {
    collect(
        conn,
        "SELECT DISTINCT account_id, chat_id FROM folder_chat WHERE folder_account_id = ?1 AND folder_id = ?2
         ORDER BY account_id, chat_id",
        &[&folder_account_id, &folder_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
}

/// Platform folder ids (of the chat's own account) a chat is in by any source, known folders only.
pub fn chat_folder_ids(conn: &Connection, account_id: &str, chat_id: &str) -> Result<Vec<String>> {
    collect(
        conn,
        "SELECT DISTINCT f.folder_id FROM folder_chat fc
           JOIN folder f ON f.account_id = fc.folder_account_id AND f.folder_id = fc.folder_id
         WHERE fc.folder_account_id = ?1 AND fc.account_id = ?1 AND fc.chat_id = ?2
         ORDER BY f.position, f.id",
        &[&account_id, &chat_id],
        |r| r.get(0),
    )
}

/// Raw platform-folder memberships of a chat (known or not yet known folders).
pub fn collect_chat_folder_memberships(
    conn: &Connection,
    account_id: &str,
    chat_id: &str,
) -> Result<Vec<String>> {
    collect(
        conn,
        "SELECT folder_id FROM folder_chat WHERE folder_account_id = ?1 AND account_id = ?1 AND chat_id = ?2 AND source = 'chat' ORDER BY folder_id",
        &[&account_id, &chat_id],
        |r| r.get(0),
    )
}

/// Local (cross-platform) folders a chat is in.
pub fn chat_local_folder_ids(
    conn: &Connection,
    account_id: &str,
    chat_id: &str,
) -> Result<Vec<String>> {
    collect(
        conn,
        "SELECT DISTINCT f.folder_id FROM folder_chat fc
           JOIN folder f ON f.account_id = '' AND f.folder_id = fc.folder_id
         WHERE fc.folder_account_id = '' AND fc.account_id = ?1 AND fc.chat_id = ?2
         ORDER BY f.position, f.id",
        &[&account_id, &chat_id],
        |r| r.get(0),
    )
}

// ---- topic ---------------------------------------------------------------------------------

const TOPIC_COLS: &str = "id, conversation_id, topic_id, title, icon_emoji, icon_custom_emoji_id, icon_color, \
    is_general, unread_count, unread_mentions, unread_reactions, pinned, closed, hidden, last_message_at, \
    last_message_preview, read_inbox_max_id, read_outbox_max_id, ext_json, is_gone, created_at, updated_at";

fn topic_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<TopicRow> {
    let ext: Option<Json<_>> = r.get(18)?;
    Ok(TopicRow {
        id: r.get(0)?,
        conversation_id: r.get(1)?,
        topic_id: r.get(2)?,
        title: r.get(3)?,
        icon_emoji: r.get(4)?,
        icon_custom_emoji_id: r.get(5)?,
        icon_color: r.get(6)?,
        is_general: r.get(7)?,
        unread_count: r.get(8)?,
        unread_mentions: r.get(9)?,
        unread_reactions: r.get(10)?,
        pinned: r.get(11)?,
        closed: r.get(12)?,
        hidden: r.get(13)?,
        last_message_at: r.get(14)?,
        last_message_preview: r.get(15)?,
        read_inbox_max_id: r.get(16)?,
        read_outbox_max_id: r.get(17)?,
        ext: ext.map(|j| j.0),
        is_gone: r.get(19)?,
        created_at: r.get(20)?,
        updated_at: r.get(21)?,
    })
}

/// One topic.
pub fn get_topic(
    conn: &Connection,
    conversation_id: i64,
    topic_id: &str,
) -> Result<Option<TopicRow>> {
    opt(
        conn,
        &format!("SELECT {TOPIC_COLS} FROM topic WHERE conversation_id = ?1 AND topic_id = ?2"),
        &[&conversation_id, &topic_id],
        topic_from_row,
    )
}

/// Insert a topic (`id` ignored).
pub fn insert_topic(conn: &Connection, t: &TopicRow) -> Result<i64> {
    conn.prepare_cached(&format!(
        "INSERT INTO topic ({}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
        &TOPIC_COLS[4..]
    ))?
    .execute(params![
        t.conversation_id,
        t.topic_id,
        t.title,
        t.icon_emoji,
        t.icon_custom_emoji_id,
        t.icon_color,
        t.is_general,
        t.unread_count,
        t.unread_mentions,
        t.unread_reactions,
        t.pinned,
        t.closed,
        t.hidden,
        t.last_message_at,
        t.last_message_preview,
        t.read_inbox_max_id,
        t.read_outbox_max_id,
        json_opt(&t.ext)?,
        t.is_gone,
        t.created_at,
        t.updated_at
    ])?;
    Ok(conn.last_insert_rowid())
}

/// Write every mutable column of a topic (by `t.id`).
pub fn update_topic(conn: &Connection, t: &TopicRow) -> Result<bool> {
    conn.prepare_cached(
        "UPDATE topic SET title=?2, icon_emoji=?3, icon_custom_emoji_id=?4, icon_color=?5, is_general=?6,
            unread_count=?7, unread_mentions=?8, unread_reactions=?9, pinned=?10, closed=?11, hidden=?12,
            last_message_at=?13, last_message_preview=?14, read_inbox_max_id=?15, read_outbox_max_id=?16,
            ext_json=?17, is_gone=?18, updated_at=?19
         WHERE id = ?1",
    )?
    .execute(params![
        t.id,
        t.title,
        t.icon_emoji,
        t.icon_custom_emoji_id,
        t.icon_color,
        t.is_general,
        t.unread_count,
        t.unread_mentions,
        t.unread_reactions,
        t.pinned,
        t.closed,
        t.hidden,
        t.last_message_at,
        t.last_message_preview,
        t.read_inbox_max_id,
        t.read_outbox_max_id,
        json_opt(&t.ext)?,
        t.is_gone,
        t.updated_at
    ])?;
    Ok(changes(conn) > 0)
}

/// Topics of a chat (not gone), pinned first.
pub fn list_topics(conn: &Connection, conversation_id: i64) -> Result<Vec<TopicRow>> {
    collect(
        conn,
        &format!(
            "SELECT {TOPIC_COLS} FROM topic WHERE conversation_id = ?1 AND is_gone = 0
             ORDER BY pinned DESC, last_message_at DESC, id"
        ),
        &[&conversation_id],
        topic_from_row,
    )
}

/// Mark the chat's topics not in `keep` as gone (after a complete `topics.upsert`). Returns their ids.
pub fn mark_topics_synced(
    conn: &Connection,
    conversation_id: i64,
    keep: &[String],
    now: Millis,
) -> Result<Vec<String>> {
    let existing: Vec<(i64, String)> = collect(
        conn,
        "SELECT id, topic_id FROM topic WHERE conversation_id = ?1 AND is_gone = 0",
        &[&conversation_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let mut gone = Vec::new();
    for (id, tid) in existing {
        if !keep.contains(&tid) {
            conn.prepare_cached("UPDATE topic SET is_gone = 1, updated_at = ?2 WHERE id = ?1")?
                .execute(params![id, now])?;
            gone.push(tid);
        }
    }
    Ok(gone)
}

// ---- draft ---------------------------------------------------------------------------------

fn draft_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<DraftRow> {
    let entities: Option<Json<_>> = r.get(3)?;
    let ext: Option<Json<_>> = r.get(6)?;
    Ok(DraftRow {
        conversation_id: r.get(0)?,
        topic_id: r.get(1)?,
        text: r.get(2)?,
        entities: entities.map(|j| j.0),
        reply_to: r.get(4)?,
        updated_at: r.get(5)?,
        ext: ext.map(|j| j.0),
    })
}

/// The draft of a chat (`topic_id = ""`) or topic.
pub fn get_draft(
    conn: &Connection,
    conversation_id: i64,
    topic_id: &str,
) -> Result<Option<DraftRow>> {
    opt(
        conn,
        "SELECT conversation_id, topic_id, text, entities_json, reply_to, updated_at, ext_json
         FROM draft WHERE conversation_id = ?1 AND topic_id = ?2",
        &[&conversation_id, &topic_id],
        draft_from_row,
    )
}

/// Write a draft. Empty text deletes the row.
pub fn set_draft(conn: &Connection, d: &DraftRow) -> Result<()> {
    if d.text.is_empty() {
        conn.prepare_cached("DELETE FROM draft WHERE conversation_id = ?1 AND topic_id = ?2")?
            .execute(params![d.conversation_id, d.topic_id])?;
        return Ok(());
    }
    conn.prepare_cached(
        "INSERT INTO draft (conversation_id, topic_id, text, entities_json, reply_to, updated_at, ext_json)
         VALUES (?1,?2,?3,?4,?5,?6,?7)
         ON CONFLICT(conversation_id, topic_id) DO UPDATE SET text=excluded.text,
           entities_json=excluded.entities_json, reply_to=excluded.reply_to, updated_at=excluded.updated_at,
           ext_json=excluded.ext_json",
    )?
    .execute(params![
        d.conversation_id,
        d.topic_id,
        d.text,
        json_opt(&d.entities)?,
        d.reply_to,
        d.updated_at,
        json_opt(&d.ext)?
    ])?;
    Ok(())
}

/// All drafts of a conversation (chat + topics).
pub fn list_drafts(conn: &Connection, conversation_id: i64) -> Result<Vec<DraftRow>> {
    collect(
        conn,
        "SELECT conversation_id, topic_id, text, entities_json, reply_to, updated_at, ext_json
         FROM draft WHERE conversation_id = ?1 ORDER BY topic_id",
        &[&conversation_id],
        draft_from_row,
    )
}

// ---- member --------------------------------------------------------------------------------

fn member_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<MemberRow> {
    Ok(MemberRow {
        conversation_id: r.get(0)?,
        user_id: r.get(1)?,
        role: r.get(2)?,
        rank: r.get(3)?,
        joined_at: r.get(4)?,
        invited_by: r.get(5)?,
        updated_at: r.get(6)?,
    })
}

/// Insert or update a member.
pub fn upsert_member(conn: &Connection, m: &MemberRow) -> Result<()> {
    conn.prepare_cached(
        "INSERT INTO member (conversation_id, user_id, role, rank, joined_at, invited_by, updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7)
         ON CONFLICT(conversation_id, user_id) DO UPDATE SET role=excluded.role, rank=excluded.rank,
           joined_at=excluded.joined_at, invited_by=excluded.invited_by, updated_at=excluded.updated_at",
    )?
    .execute(params![
        m.conversation_id,
        m.user_id,
        m.role,
        m.rank,
        m.joined_at,
        m.invited_by,
        m.updated_at
    ])?;
    Ok(())
}

/// Members of a chat.
pub fn list_members(conn: &Connection, conversation_id: i64) -> Result<Vec<MemberRow>> {
    collect(
        conn,
        "SELECT conversation_id, user_id, role, rank, joined_at, invited_by, updated_at
         FROM member WHERE conversation_id = ?1 ORDER BY role, user_id",
        &[&conversation_id],
        member_from_row,
    )
}

/// Remove a member row.
pub fn delete_member(conn: &Connection, conversation_id: i64, user_id: &str) -> Result<bool> {
    conn.prepare_cached("DELETE FROM member WHERE conversation_id = ?1 AND user_id = ?2")?
        .execute(params![conversation_id, user_id])?;
    Ok(changes(conn) > 0)
}
