//! Field-by-field merge of wire DTOs into rows (spec §8.4): absent = unchanged; on-create
//! defaults apply only when the row is created; nested value objects are replaced wholesale
//! (except cache paths, which the core owns and keeps when the plugin omits them).

use ch_db::types::{ConversationRow, HandleRow, MessageRow, TopicRow};
use ch_db::{Millis, ts_to_millis};
use ch_protocol::dto::*;

/// `dst = src` when `src` is reported.
fn set_opt<T: Clone>(dst: &mut Option<T>, src: &Option<T>) {
    if src.is_some() {
        *dst = src.clone();
    }
}

/// `dst = src` for a non-nullable column when `src` is reported.
fn set_val<T: Copy>(dst: &mut T, src: Option<T>) {
    if let Some(v) = src {
        *dst = v;
    }
}

fn set_i64(dst: &mut Option<i64>, src: Option<u64>) {
    if let Some(v) = src {
        *dst = Some(v as i64);
    }
}

fn set_ts(dst: &mut Option<Millis>, src: Option<ch_protocol::Timestamp>) {
    if let Some(t) = src {
        *dst = Some(ts_to_millis(t));
    }
}

/// A fresh conversation row for a chat the core has never seen (on-create defaults).
pub fn new_conversation(
    account_id: &str,
    chat_id: &str,
    kind: &str,
    now: Millis,
) -> ConversationRow {
    ConversationRow {
        account_id: account_id.to_owned(),
        chat_id: chat_id.to_owned(),
        kind: kind.to_owned(),
        archived: false,
        unread_count: 0,
        unread_mentions: 0,
        unread_reactions: 0,
        is_self_chat: false,
        is_gone: false,
        created_at: now,
        updated_at: now,
        ..Default::default()
    }
}

/// Merge a `Conversation` DTO into a row. Folder ids and the draft are handled by the caller.
pub fn merge_conversation(row: &mut ConversationRow, c: &Conversation) {
    row.kind = c.kind.as_str().to_owned();
    set_opt(&mut row.title, &c.title);
    set_opt(&mut row.peer_user_id, &c.peer_user_id);
    set_opt(&mut row.parent_chat_id, &c.parent_chat_id);
    set_opt(&mut row.username, &c.username);
    set_opt(&mut row.about, &c.about);
    set_val(&mut row.archived, c.archived);
    set_opt(&mut row.muted, &c.muted);
    set_ts(&mut row.mute_until, c.mute_until);
    set_opt(&mut row.pinned, &c.pinned);
    set_i64(&mut row.pin_order, c.pin_order);
    set_val(&mut row.unread_count, c.unread_count.map(|v| v as i64));
    set_val(
        &mut row.unread_mentions,
        c.unread_mentions.map(|v| v as i64),
    );
    set_val(
        &mut row.unread_reactions,
        c.unread_reactions.map(|v| v as i64),
    );
    set_opt(&mut row.marked_unread, &c.marked_unread);
    set_opt(&mut row.read_inbox_max_id, &c.read_inbox_max_id);
    set_opt(&mut row.read_outbox_max_id, &c.read_outbox_max_id);
    if let Some(t) = c.last_message_at {
        let ms = ts_to_millis(t);
        if row.last_message_at.is_none_or(|cur| ms >= cur) {
            row.last_message_at = Some(ms);
        }
    }
    set_val(&mut row.is_self_chat, c.is_self_chat);
    set_opt(&mut row.avatar_path, &c.avatar_path);
    set_i64(&mut row.member_count, c.member_count);
    set_opt(&mut row.can_post, &c.can_post);
    set_opt(&mut row.is_forum, &c.is_forum);
    set_i64(&mut row.color_index, c.color_index);
    set_i64(&mut row.scheduled_count, c.scheduled_count);
    set_i64(&mut row.auto_delete_seconds, c.auto_delete_seconds);
    set_i64(&mut row.slow_mode_seconds, c.slow_mode_seconds);
    set_opt(&mut row.permissions, &c.permissions);
    set_opt(&mut row.is_verified, &c.is_verified);
    set_opt(&mut row.is_scam, &c.is_scam);
    set_opt(&mut row.is_restricted, &c.is_restricted);
    set_opt(&mut row.ext, &c.ext);
    row.is_gone = false;
}

/// A fresh topic row (on-create defaults).
pub fn new_topic(conversation_id: i64, topic_id: &str, title: &str, now: Millis) -> TopicRow {
    TopicRow {
        conversation_id,
        topic_id: topic_id.to_owned(),
        title: title.to_owned(),
        created_at: now,
        updated_at: now,
        ..Default::default()
    }
}

/// Merge a `Topic` DTO into a row. The draft is handled by the caller.
pub fn merge_topic(row: &mut TopicRow, t: &Topic) {
    row.title.clone_from(&t.title);
    set_opt(&mut row.icon_emoji, &t.icon_emoji);
    set_opt(&mut row.icon_custom_emoji_id, &t.icon_custom_emoji_id);
    if let Some(c) = t.icon_color {
        row.icon_color = Some(i64::from(c));
    }
    set_val(&mut row.is_general, t.is_general);
    set_val(&mut row.unread_count, t.unread_count.map(|v| v as i64));
    set_val(
        &mut row.unread_mentions,
        t.unread_mentions.map(|v| v as i64),
    );
    set_val(
        &mut row.unread_reactions,
        t.unread_reactions.map(|v| v as i64),
    );
    set_val(&mut row.pinned, t.pinned);
    set_val(&mut row.closed, t.closed);
    set_val(&mut row.hidden, t.hidden);
    set_ts(&mut row.last_message_at, t.last_message_at);
    set_opt(&mut row.last_message_preview, &t.last_message_preview);
    set_opt(&mut row.read_inbox_max_id, &t.read_inbox_max_id);
    set_opt(&mut row.ext, &t.ext);
    row.is_gone = false;
}

/// A fresh message row (on-create defaults).
pub fn new_message(conversation_id: i64, m: &Message, now: Millis) -> MessageRow {
    MessageRow {
        conversation_id,
        message_id: m.id.clone(),
        direction: m.direction.as_str().to_owned(),
        sent_at: ts_to_millis(m.sent_at),
        edited: false,
        is_scheduled: false,
        created_at: now,
        updated_at: now,
        ..Default::default()
    }
}

/// Media equality that ignores the cache paths (`path`, `thumb_path`), which change when a file is
/// downloaded or evicted without the message having been edited.
pub fn media_content_eq(a: &Option<Media>, b: &Option<Media>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => {
            let strip = |m: &Media| Media {
                path: None,
                thumb_path: None,
                ..m.clone()
            };
            strip(a) == strip(b)
        }
        _ => false,
    }
}

/// Would merging `m` change the message's content (text, entities, media)?
pub fn content_differs(row: &MessageRow, m: &Message) -> bool {
    (m.text.is_some() && m.text != row.text)
        || (m.entities.is_some() && m.entities != row.entities)
        || (m.media.is_some() && !media_content_eq(&row.media, &m.media))
}

/// Is `m` older than the edit the row holds? Once a message is known to have been edited at
/// `edited_at`, only content carrying a newer (or equal) `edited_at` may replace it: a re-fetch
/// or a replayed pre-edit upsert without `edited_at` never rolls the text back.
pub fn is_stale_edit(row: &MessageRow, m: &Message) -> bool {
    match (row.edited_at, m.edited_at) {
        (Some(have), Some(got)) => ts_to_millis(got) < have,
        (Some(_), None) => true,
        _ => false,
    }
}

/// Merge a `Message` DTO into a row. Reactions go to their own table (caller). When `m` is a
/// stale edit, content fields and the edit markers are left alone.
pub fn merge_message(row: &mut MessageRow, m: &Message) {
    let stale = is_stale_edit(row, m);
    row.message_id.clone_from(&m.id);
    set_opt(&mut row.topic_id, &m.topic_id);
    row.direction = m.direction.as_str().to_owned();
    row.sent_at = ts_to_millis(m.sent_at);
    set_opt(&mut row.sender_id, &m.sender_id);
    set_opt(&mut row.sender_name, &m.sender_name);
    set_opt(&mut row.sender_title, &m.sender_title);
    if !stale {
        set_opt(&mut row.text, &m.text);
        set_opt(&mut row.entities, &m.entities);
        if let Some(media) = &m.media {
            let mut media = media.clone();
            // Cache paths are the core's knowledge: keep them when the plugin omits them for the
            // same media.
            if let Some(cur) = &row.media
                && cur.kind == media.kind
            {
                if media.path.is_none() {
                    media.path.clone_from(&cur.path);
                }
                if media.thumb_path.is_none() {
                    media.thumb_path.clone_from(&cur.thumb_path);
                }
            }
            row.media = Some(media);
        }
        set_val(&mut row.edited, m.edited);
        set_ts(&mut row.edited_at, m.edited_at);
    } else if let (Some(cur), Some(inc)) = (&mut row.media, &m.media)
        && cur.kind == inc.kind
    {
        // Even a stale re-fetch may tell us where the files are.
        if inc.path.is_some() {
            cur.path.clone_from(&inc.path);
        }
        if inc.thumb_path.is_some() {
            cur.thumb_path.clone_from(&inc.thumb_path);
        }
    }
    set_opt(&mut row.reply_to, &m.reply_to);
    set_opt(&mut row.forward_from, &m.forward_from);
    set_opt(&mut row.grouped_id, &m.grouped_id);
    set_opt(&mut row.reactors, &m.reactors);
    set_opt(&mut row.poll, &m.poll);
    set_opt(&mut row.location, &m.location);
    set_opt(&mut row.contact_card, &m.contact_card);
    set_opt(&mut row.dice, &m.dice);
    set_opt(&mut row.link_preview, &m.link_preview);
    set_opt(&mut row.buttons, &m.buttons);
    set_i64(&mut row.views, m.views);
    set_i64(&mut row.replies_count, m.replies_count);
    set_opt(&mut row.replies_chat_id, &m.replies_chat_id);
    set_opt(&mut row.effect_id, &m.effect_id);
    set_opt(&mut row.action, &m.action);
    set_opt(&mut row.mentioned, &m.mentioned);
    if let Some(d) = &m.delivery {
        row.delivery = Some(d.as_str().to_owned());
    }
    set_opt(&mut row.read_by, &m.read_by);
    set_val(&mut row.is_scheduled, m.is_scheduled);
    set_opt(&mut row.is_pinned, &m.is_pinned);
    set_opt(&mut row.silent, &m.silent);
    set_i64(&mut row.ttl_seconds, m.ttl_seconds);
    set_opt(&mut row.via_bot_id, &m.via_bot_id);
    set_opt(&mut row.ext, &m.ext);
}

/// A fresh handle row.
pub fn new_handle(account_id: &str, user_id: &str, now: Millis) -> HandleRow {
    HandleRow {
        account_id: account_id.to_owned(),
        user_id: user_id.to_owned(),
        first_seen_at: now,
        last_seen_at: now,
        ..Default::default()
    }
}

/// Merge a `Contact` DTO into a handle row (absent = unchanged, like the merged DTOs).
pub fn merge_contact(row: &mut HandleRow, c: &Contact, now: Millis) {
    set_opt(&mut row.display_name, &c.display_name);
    set_opt(&mut row.first_name, &c.first_name);
    set_opt(&mut row.last_name, &c.last_name);
    set_opt(&mut row.username, &c.username);
    set_opt(&mut row.phone, &c.phone);
    set_opt(&mut row.avatar_path, &c.avatar_path);
    set_opt(&mut row.is_bot, &c.is_bot);
    set_opt(&mut row.is_contact, &c.is_contact);
    set_opt(&mut row.is_premium, &c.is_premium);
    set_opt(&mut row.is_verified, &c.is_verified);
    set_opt(&mut row.is_deleted, &c.is_deleted);
    set_opt(&mut row.bio, &c.bio);
    if c.status.is_some() {
        row.status.clone_from(&c.status);
        row.status_at = Some(now);
    }
    set_opt(&mut row.ext, &c.ext);
}

/// A one-line chat-list preview for a message.
pub fn preview_of(row: &MessageRow) -> (Option<String>, Option<String>) {
    let kind = if row.action.is_some() {
        Some("service".to_owned())
    } else {
        row.media_kind().or_else(|| Some("text".to_owned()))
    };
    let text = row
        .text
        .as_deref()
        .filter(|t| !t.is_empty())
        .map(|t| t.chars().take(160).collect::<String>())
        .or_else(|| row.action.as_ref().and_then(|a| a.text.clone()));
    (kind, text)
}
