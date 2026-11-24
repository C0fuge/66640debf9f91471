//! Row types: what the query layer reads and writes. Field names follow the columns.
//!
//! Nested DTOs are the `ch_protocol` types (stored as JSON text). `Option<bool>` is a tri-state
//! column (`NULL` = unknown); plain `bool` columns have an "on create" default.

#![allow(clippy::struct_excessive_bools, missing_docs)]

use ch_protocol::dto::*;
use ch_protocol::{Ext, Id};

use crate::Millis;

/// `plugin` row.
#[derive(Debug, Clone, PartialEq)]
pub struct PluginRow {
    pub plugin_id: String,
    pub name: String,
    pub version: String,
    pub api_version: String,
    pub install_dir: Option<String>,
    pub brand_color: Option<String>,
    pub icon_path: Option<String>,
    pub manifest_json: Option<String>,
    pub enabled: bool,
    pub installed: bool,
    pub first_seen_at: Millis,
    pub last_seen_at: Millis,
}

/// `account` row.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountRow {
    pub account_id: String,
    pub plugin_id: String,
    pub label: Option<String>,
    pub position: i64,
    pub enabled: bool,
    /// The plugin's `account.start.settings` object.
    pub settings: serde_json::Map<String, serde_json::Value>,
    /// Wire value of `AccountState`.
    pub state: String,
    pub state_detail: Option<String>,
    pub reconnecting: bool,
    pub state_at: Option<Millis>,
    pub me_user_id: Option<Id>,
    pub created_at: Millis,
    pub updated_at: Millis,
}

/// `person` row.
#[derive(Debug, Clone, PartialEq)]
pub struct PersonRow {
    pub id: i64,
    pub display_name: Option<String>,
    pub notes: Option<String>,
    pub created_at: Millis,
    pub updated_at: Millis,
}

/// `category` row.
#[derive(Debug, Clone, PartialEq)]
pub struct CategoryRow {
    pub id: i64,
    pub name: String,
    pub color: Option<String>,
    pub sort_order: i64,
}

/// `platform_handle` row: one `Contact` on one account plus identity state.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct HandleRow {
    pub id: i64,
    pub account_id: String,
    pub user_id: Id,
    pub person_id: Option<i64>,
    pub link_source: Option<String>,
    pub display_name: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub username: Option<String>,
    pub phone: Option<String>,
    pub avatar_path: Option<String>,
    pub is_bot: Option<bool>,
    pub is_contact: Option<bool>,
    pub is_premium: Option<bool>,
    pub is_verified: Option<bool>,
    pub is_deleted: Option<bool>,
    pub bio: Option<String>,
    pub status: Option<OnlineStatus>,
    pub status_at: Option<Millis>,
    pub ext: Option<Ext>,
    pub first_seen_at: Millis,
    pub last_seen_at: Millis,
}

/// `conversation` row.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ConversationRow {
    pub id: i64,
    pub account_id: String,
    pub chat_id: Id,
    /// Wire value of `ConversationKind`.
    pub kind: String,
    pub title: Option<String>,
    pub peer_user_id: Option<Id>,
    pub parent_chat_id: Option<Id>,
    pub username: Option<String>,
    pub about: Option<String>,
    pub archived: bool,
    pub muted: Option<bool>,
    pub mute_until: Option<Millis>,
    pub pinned: Option<bool>,
    pub pin_order: Option<i64>,
    pub unread_count: i64,
    pub unread_mentions: i64,
    pub unread_reactions: i64,
    pub marked_unread: Option<bool>,
    pub read_inbox_max_id: Option<Id>,
    pub read_outbox_max_id: Option<Id>,
    pub last_message_at: Option<Millis>,
    pub is_self_chat: bool,
    pub avatar_path: Option<String>,
    pub member_count: Option<i64>,
    pub can_post: Option<bool>,
    pub is_forum: Option<bool>,
    pub color_index: Option<i64>,
    pub scheduled_count: Option<i64>,
    pub auto_delete_seconds: Option<i64>,
    pub slow_mode_seconds: Option<i64>,
    pub permissions: Option<std::collections::BTreeMap<String, bool>>,
    pub is_verified: Option<bool>,
    pub is_scam: Option<bool>,
    pub is_restricted: Option<bool>,
    pub ext: Option<Ext>,
    pub is_gone: bool,
    pub notify_override: Option<String>,
    pub notify_sound: Option<String>,
    pub wallpaper: Option<String>,
    pub local_read_at: Option<Millis>,
    pub last_message_preview: Option<String>,
    pub last_message_kind: Option<String>,
    pub created_at: Millis,
    pub updated_at: Millis,
}

/// `folder` row. `account_id == ""` is a local (cross-platform) folder.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FolderRow {
    pub id: i64,
    pub account_id: String,
    pub folder_id: Id,
    pub title: String,
    pub emoticon: Option<String>,
    pub position: Option<i64>,
    pub rules: Option<FolderRules>,
    pub is_shared: bool,
    pub ext: Option<Ext>,
    pub updated_at: Millis,
}

impl FolderRow {
    /// Is this a local hub folder (no account)?
    pub fn is_local(&self) -> bool {
        self.account_id.is_empty()
    }
}

/// `topic` row.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TopicRow {
    pub id: i64,
    pub conversation_id: i64,
    pub topic_id: Id,
    pub title: String,
    pub icon_emoji: Option<String>,
    pub icon_custom_emoji_id: Option<Id>,
    pub icon_color: Option<i64>,
    pub is_general: bool,
    pub unread_count: i64,
    pub unread_mentions: i64,
    pub unread_reactions: i64,
    pub pinned: bool,
    pub closed: bool,
    pub hidden: bool,
    pub last_message_at: Option<Millis>,
    pub last_message_preview: Option<String>,
    pub read_inbox_max_id: Option<Id>,
    pub read_outbox_max_id: Option<Id>,
    pub ext: Option<Ext>,
    pub is_gone: bool,
    pub created_at: Millis,
    pub updated_at: Millis,
}

/// `message` row. Reactions live in `message_reaction` (see [`ReactionRow`]).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct MessageRow {
    pub id: i64,
    pub conversation_id: i64,
    pub message_id: Id,
    pub topic_id: Option<Id>,
    /// Wire value of `Direction` (`in` | `out`).
    pub direction: String,
    pub sent_at: Millis,
    pub sender_id: Option<Id>,
    pub sender_name: Option<String>,
    pub sender_title: Option<String>,
    pub text: Option<String>,
    pub entities: Option<Vec<Entity>>,
    pub media: Option<Media>,
    pub reply_to: Option<ReplyRef>,
    pub forward_from: Option<ForwardInfo>,
    pub grouped_id: Option<Id>,
    pub reactors: Option<Vec<Reactor>>,
    pub poll: Option<Poll>,
    pub location: Option<Location>,
    pub contact_card: Option<ContactCard>,
    pub dice: Option<Dice>,
    pub link_preview: Option<LinkPreview>,
    pub buttons: Option<Vec<Vec<InlineButton>>>,
    pub views: Option<i64>,
    pub replies_count: Option<i64>,
    pub replies_chat_id: Option<Id>,
    pub effect_id: Option<Id>,
    pub action: Option<ServiceAction>,
    pub mentioned: Option<bool>,
    pub edited: bool,
    pub edited_at: Option<Millis>,
    /// Wire value of `Delivery`.
    pub delivery: Option<String>,
    pub read_by: Option<Vec<Id>>,
    pub is_scheduled: bool,
    pub is_pinned: Option<bool>,
    pub silent: Option<bool>,
    pub ttl_seconds: Option<i64>,
    pub via_bot_id: Option<Id>,
    pub ext: Option<Ext>,
    pub deleted_at: Option<Millis>,
    pub deleted_by: Option<String>,
    pub has_link: bool,
    pub created_at: Millis,
    pub updated_at: Millis,
}

impl MessageRow {
    /// Is this a tombstone (deleted, content kept)?
    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }
    /// The derived `media_kind` column.
    pub fn media_kind(&self) -> Option<String> {
        self.media.as_ref().map(|m| m.kind.as_str().to_owned())
    }
    /// The derived `reply_to_id` column.
    pub fn reply_to_id(&self) -> Option<&str> {
        self.reply_to.as_ref().map(|r| r.message_id.as_str())
    }
    /// Does the message carry a link (an url/text_url entity or a link preview)?
    pub fn compute_has_link(&self) -> bool {
        self.link_preview.is_some()
            || self.entities.as_ref().is_some_and(|es| {
                es.iter()
                    .any(|e| matches!(e.kind, EntityType::Url | EntityType::TextUrl))
            })
    }
}

/// `message_version` row: content before an edit replaced it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct MessageVersionRow {
    pub id: i64,
    pub message_id: i64,
    pub text: Option<String>,
    pub entities: Option<Vec<Entity>>,
    pub media: Option<Media>,
    pub edited_at: Option<Millis>,
    pub replaced_at: Millis,
}

/// `message_reaction` row.
#[derive(Debug, Clone, PartialEq)]
pub struct ReactionRow {
    pub message_id: i64,
    /// Canonical key: `e:<emoji>` | `c:<custom_emoji_id>` | `paid`.
    pub key: String,
    pub key_json: ReactionKey,
    pub count: i64,
    pub mine: bool,
    pub my_count: Option<i64>,
    pub position: i64,
}

/// Canonical text form of a reaction key (the `message_reaction.key` column).
pub fn reaction_key_text(k: &ReactionKey) -> String {
    match k {
        ReactionKey::Emoji { emoji } => format!("e:{emoji}"),
        ReactionKey::CustomEmoji { custom_emoji_id } => format!("c:{custom_emoji_id}"),
        ReactionKey::Paid { .. } => "paid".to_owned(),
    }
}

impl ReactionRow {
    /// Back to the wire `Reaction`.
    pub fn to_dto(&self) -> Reaction {
        Reaction {
            key: self.key_json.clone(),
            count: self.count.max(0) as u64,
            mine: self.mine,
            my_count: self.my_count.map(|c| c.max(0) as u64),
        }
    }
}

/// `draft` row. `topic_id == ""` is the chat's own draft.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DraftRow {
    pub conversation_id: i64,
    pub topic_id: String,
    pub text: String,
    pub entities: Option<Vec<Entity>>,
    pub reply_to: Option<Id>,
    pub updated_at: Millis,
    pub ext: Option<Ext>,
}

impl DraftRow {
    /// Back to the wire `Draft`.
    pub fn to_dto(&self) -> Draft {
        Draft {
            text: self.text.clone(),
            entities: self.entities.clone(),
            reply_to: self.reply_to.clone(),
            updated_at: crate::millis_to_ts(self.updated_at),
            ext: self.ext.clone(),
        }
    }
}

/// `member` row.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct MemberRow {
    pub conversation_id: i64,
    pub user_id: Id,
    /// Wire value of `MemberRole`.
    pub role: String,
    pub rank: Option<String>,
    pub joined_at: Option<Millis>,
    pub invited_by: Option<Id>,
    pub updated_at: Millis,
}

/// Outbox states (the `outbox.state` column).
pub mod outbox_state {
    /// Waiting for a slot.
    pub const QUEUED: &str = "queued";
    /// Request in flight.
    pub const SENDING: &str = "sending";
    /// The plugin accepted it; `result_message_ids` is set.
    pub const SENT: &str = "sent";
    /// Failed; `last_error_json` says why. Retryable when `next_attempt_at` is set.
    pub const FAILED: &str = "failed";
    /// The user cancelled it.
    pub const CANCELLED: &str = "cancelled";
}

/// `outbox` row.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OutboxRow {
    pub id: i64,
    pub account_id: String,
    pub chat_id: Id,
    pub topic_id: Option<Id>,
    pub client_id: String,
    pub kind: String,
    pub method: String,
    pub params: serde_json::Value,
    pub state: String,
    pub attempts: i64,
    pub next_attempt_at: Option<Millis>,
    pub last_error: Option<serde_json::Value>,
    pub echo_message_id: Option<i64>,
    pub result_message_ids: Option<Vec<Id>>,
    pub request_id: Option<String>,
    pub done_bytes: Option<i64>,
    pub total_bytes: Option<i64>,
    pub created_at: Millis,
    pub updated_at: Millis,
}

/// `history_state` row.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct HistoryStateRow {
    pub conversation_id: i64,
    pub topic_id: String,
    pub older_cursor: Option<String>,
    pub reached_start: bool,
    pub oldest_message_id: Option<Id>,
    pub oldest_sent_at: Option<Millis>,
    pub newest_message_id: Option<Id>,
    pub newest_sent_at: Option<Millis>,
    pub last_fetch_at: Option<Millis>,
    pub updated_at: Millis,
}

/// `history_gap` row.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct HistoryGapRow {
    pub id: i64,
    pub conversation_id: i64,
    pub topic_id: String,
    pub after_message_id: Id,
    pub before_message_id: Option<Id>,
    pub cursor: Option<String>,
    pub created_at: Millis,
    pub updated_at: Millis,
}

/// `media_file` row.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct MediaFileRow {
    pub path: String,
    pub account_id: String,
    /// `full` | `thumb` | `avatar` | `sticker` | `other`.
    pub kind: String,
    pub message_id: Option<i64>,
    pub media_kind: Option<String>,
    pub bytes: Option<i64>,
    pub created_at: Millis,
    pub last_access_at: Millis,
}

/// `presence_log` row.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PresenceLogRow {
    pub id: i64,
    pub account_id: String,
    pub user_id: Id,
    pub chat_id: Option<Id>,
    pub at: Millis,
    pub kind: String,
    pub detail: Option<serde_json::Value>,
}

/// A local-search hit.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    /// `message.id`.
    pub message_rowid: i64,
    pub conversation_id: i64,
    pub account_id: String,
    pub chat_id: Id,
    pub message_id: Id,
    pub sent_at: Millis,
    /// Matching text with `[` `]` around the hits.
    pub snippet: String,
}

/// Filters for [`crate::queries::search::search_messages`]. All optional; `text` empty = no FTS
/// clause (filters only).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SearchQuery {
    /// Free text. Tokens are matched as prefixes; quoted phrases are kept.
    pub text: String,
    pub account_id: Option<String>,
    /// `in:` — restrict to one conversation (rowid).
    pub conversation_id: Option<i64>,
    pub topic_id: Option<Id>,
    /// `from:` — sender user id.
    pub sender_id: Option<Id>,
    /// `media:` — `Media.kind` values, or `"link"` for messages with links.
    pub media: Vec<String>,
    /// `before:` (exclusive) and `after:` (inclusive), unix ms.
    pub before: Option<Millis>,
    pub after: Option<Millis>,
    pub include_deleted: bool,
    pub limit: u32,
    pub offset: u32,
}
