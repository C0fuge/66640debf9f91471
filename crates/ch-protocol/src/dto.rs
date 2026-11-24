//! Types generated once from `schemas/dto/*.json` (API v1.0.0). The schemas are the contract; the
//! tests in `tests/` keep these types in step with them. `str_enum!` enums keep unknown values
//! as `Unknown(String)` (spec §18).
#![allow(clippy::struct_excessive_bools, missing_docs, unused_imports)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::common::*;
#[allow(unused_imports)]
use crate::jsonrpc::RequestId;

// ---- dto/auth.json ----

str_enum! {
    AuthStepKind {
    Phone = "phone",
    Code = "code",
    Password = "password",
    Qr = "qr",
    Email = "email",
    EmailCode = "email_code",
    Text = "text",
    Info = "info",
    Done = "done",
    }
}

/// One step of an interactive flow, rendered generically by the core. docs/plugin-api.md section 7.1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthStep {
    /// sign_in, two_factor, or plugin-defined.
    pub flow: String,
    pub step_id: String,
    pub kind: AuthStepKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<String>,
    /// qr: the payload to render.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Timestamp>,
    /// Default: false.
    #[serde(default)]
    pub can_resend: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_length: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_length: Option<u64>,
    /// Why the previous submission was rejected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ---- dto/contact.json ----

/// A user on the platform as seen by this account. docs/plugin-api.md section 9.1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Contact {
    pub id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_name: Option<String>,
    /// Without the @.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// E.164 where possible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phone: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_bot: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_contact: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_premium: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_verified: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_deleted: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bio: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<OnlineStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext: Option<Ext>,
}

// ---- dto/conversation.json ----

str_enum! {
    ConversationKind {
    Dm = "dm",
    Group = "group",
    Channel = "channel",
    }
}

/// A chat. docs/plugin-api.md section 9.2. Merge-updated: an absent field means 'unchanged' and
/// leaves the core's row alone; creation defaults are `archived: false`, `unread_*: 0`,
/// `is_self_chat: false`. Send a value explicitly to reset it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Conversation {
    pub id: Id,
    pub kind: ConversationKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_user_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_chat_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub about: Option<String>,
    /// Absent = unchanged; the core uses `false` when it creates the row (spec §8.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub muted: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mute_until: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin_order: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder_ids: Option<Vec<Id>>,
    /// Absent = unchanged; the core uses `0` when it creates the row (spec §8.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unread_count: Option<u64>,
    /// Absent = unchanged; the core uses `0` when it creates the row (spec §8.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unread_mentions: Option<u64>,
    /// Absent = unchanged; the core uses `0` when it creates the row (spec §8.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unread_reactions: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub marked_unread: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_inbox_max_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_outbox_max_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_message_at: Option<Timestamp>,
    /// Absent = unchanged; the core uses `false` when it creates the row (spec §8.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_self_chat: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub can_post: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_forum: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub draft: Option<Draft>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduled_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_delete_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slow_mode_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permissions: Option<BTreeMap<String, bool>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_verified: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_scam: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_restricted: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext: Option<Ext>,
}

// ---- dto/draft.json ----

/// An unsent draft. Empty text means no draft.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Draft {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entities: Option<Vec<Entity>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext: Option<Ext>,
}

// ---- dto/folder.json ----

str_enum! {
    FolderIncludeKind {
    Contacts = "contacts",
    NonContacts = "non_contacts",
    Groups = "groups",
    Channels = "channels",
    Bots = "bots",
    }
}

/// A chat is in the folder when it is in chat_ids or its kind is in include_kinds, and it is not excluded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct FolderRules {
    /// Default: [].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_kinds: Option<Vec<FolderIncludeKind>>,
    /// Default: false.
    #[serde(default)]
    pub exclude_muted: bool,
    /// Default: false.
    #[serde(default)]
    pub exclude_read: bool,
    /// Default: false.
    #[serde(default)]
    pub exclude_archived: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_chat_ids: Option<Vec<Id>>,
    /// The folder's own pin order, top first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_chat_ids: Option<Vec<Id>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Folder {
    pub id: Id,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emoticon: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_ids: Option<Vec<Id>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rules: Option<FolderRules>,
    /// Default: false.
    #[serde(default)]
    pub is_shared: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext: Option<Ext>,
}

// ---- dto/member.json ----

str_enum! {
    MemberRole {
    Owner = "owner",
    Admin = "admin",
    Member = "member",
    Restricted = "restricted",
    Banned = "banned",
    Left = "left",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Member {
    pub user: Contact,
    pub role: MemberRole,
    /// Custom admin title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub joined_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invited_by: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext: Option<Ext>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinRequest {
    pub user: Contact,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub about: Option<String>,
}

// ---- dto/message.json ----

str_enum! {
    Direction {
    In = "in",
    Out = "out",
    }
}

str_enum! {
    Delivery {
    Sending = "sending",
    Sent = "sent",
    Delivered = "delivered",
    Read = "read",
    Failed = "failed",
    }
}

str_enum! {
    EntityType {
    Bold = "bold",
    Italic = "italic",
    Underline = "underline",
    Strike = "strike",
    Spoiler = "spoiler",
    Code = "code",
    Pre = "pre",
    Blockquote = "blockquote",
    Url = "url",
    TextUrl = "text_url",
    Mention = "mention",
    MentionName = "mention_name",
    Hashtag = "hashtag",
    Cashtag = "cashtag",
    BotCommand = "bot_command",
    Email = "email",
    Phone = "phone",
    CustomEmoji = "custom_emoji",
    BankCard = "bank_card",
    }
}

/// A formatting span over the message text. offset and length are UTF-16 code units.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entity {
    #[serde(rename = "type")]
    pub kind: EntityType,
    pub offset: u64,
    pub length: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_emoji_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collapsed: Option<bool>,
}

str_enum! {
    MediaKind {
    Photo = "photo",
    Video = "video",
    Gif = "gif",
    Sticker = "sticker",
    Voice = "voice",
    VideoNote = "video_note",
    Music = "music",
    Document = "document",
    Other = "other",
    }
}

str_enum! {
    StickerFormat {
    Static = "static",
    Animated = "animated",
    Video = "video",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StickerRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pack_id: Option<Id>,
    pub format: StickerFormat,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Media {
    pub kind: MediaKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u64>,
    /// Seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thumb_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waveform: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub performer: Option<String>,
    /// Default: false.
    #[serde(default)]
    pub has_spoiler: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sticker: Option<StickerRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReplyRef {
    pub message_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote_entities: Option<Vec<Entity>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote_offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ForwardInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reaction {
    pub key: ReactionKey,
    pub count: u64,
    pub mine: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub my_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reactor {
    pub user_id: Id,
    pub key: ReactionKey,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unread: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PollOption {
    pub id: Id,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub votes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chosen: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correct: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Poll {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<Id>,
    pub question: String,
    pub options: Vec<PollOption>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_voters: Option<u64>,
    /// Default: false.
    #[serde(default)]
    pub closed: bool,
    /// Default: false.
    #[serde(default)]
    pub multiple: bool,
    /// Default: false.
    #[serde(default)]
    pub quiz: bool,
    /// Default: false.
    #[serde(default)]
    pub anonymous: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub solution: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub solution_entities: Option<Vec<Entity>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Location {
    pub lat: f64,
    pub lon: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    /// Default: false.
    #[serde(default)]
    pub live: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_until: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heading: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accuracy: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ContactCard {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phone: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vcard: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Dice {
    pub emoticon: String,
    /// 0 while still rolling.
    pub value: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct LinkPreview {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub site: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thumb_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<Media>,
}

str_enum! {
    InlineButtonKind {
    Callback = "callback",
    Url = "url",
    LoginUrl = "login_url",
    SwitchInline = "switch_inline",
    WebApp = "web_app",
    Buy = "buy",
    Other = "other",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InlineButton {
    pub text: String,
    pub kind: InlineButtonKind,
    /// Opaque; echoed in bots.press_button.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

str_enum! {
    ServiceActionKind {
    Joined = "joined",
    Left = "left",
    Pinned = "pinned",
    TitleChanged = "title_changed",
    PhotoChanged = "photo_changed",
    PhotoRemoved = "photo_removed",
    Created = "created",
    Call = "call",
    TtlChanged = "ttl_changed",
    TopicCreated = "topic_created",
    TopicEdited = "topic_edited",
    HistoryCleared = "history_cleared",
    ContactJoined = "contact_joined",
    Screenshot = "screenshot",
    Other = "other",
    }
}

/// A platform-generated timeline entry. Its presence makes the message a service message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceAction {
    pub kind: ServiceActionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_ids: Option<Vec<Id>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub video: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

/// One message as the platform reports it. docs/plugin-api.md section 9.3. Merge-updated: absent
/// scalars and arrays mean 'unchanged'; creation defaults are `edited: false`, `is_scheduled: false`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub id: Id,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    pub direction: Direction,
    pub sent_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entities: Option<Vec<Entity>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<Media>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<ReplyRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forward_from: Option<ForwardInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grouped_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reactions: Option<Vec<Reaction>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reactors: Option<Vec<Reactor>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poll: Option<Poll>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<Location>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact_card: Option<ContactCard>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dice: Option<Dice>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link_preview: Option<LinkPreview>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buttons: Option<Vec<Vec<InlineButton>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub views: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replies_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replies_chat_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<ServiceAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mentioned: Option<bool>,
    /// Absent = unchanged; the core uses `false` when it creates the row (spec §8.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edited: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edited_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<Delivery>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_by: Option<Vec<Id>>,
    /// Absent = unchanged; the core uses `false` when it creates the row (spec §8.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_scheduled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_pinned: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silent: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via_bot_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext: Option<Ext>,
}

// ---- dto/presence.json ----

str_enum! {
    OnlineState {
    Online = "online",
    Offline = "offline",
    Recently = "recently",
    WithinWeek = "within_week",
    WithinMonth = "within_month",
    LongAgo = "long_ago",
    Hidden = "hidden",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OnlineStatus {
    pub state: OnlineState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_at: Option<Timestamp>,
    /// For online: when the platform will consider the user offline without further news.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<Timestamp>,
}

str_enum! {
    /// What someone is doing in a chat. cancel clears whatever was showing.
    PresenceAction {
    Typing = "typing",
    RecordingVoice = "recording_voice",
    RecordingVideo = "recording_video",
    Uploading = "uploading",
    ChoosingSticker = "choosing_sticker",
    Playing = "playing",
    Speaking = "speaking",
    Cancel = "cancel",
    }
}

// ---- dto/privacy.json ----

str_enum! {
    PrivacyValue {
    Everybody = "everybody",
    Contacts = "contacts",
    Nobody = "nobody",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrivacyRule {
    pub key: String,
    pub label: String,
    pub value: PrivacyValue,
    pub choices: Vec<PrivacyValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext: Option<Ext>,
}

// ---- dto/profile.json ----

/// This account's own profile, or another user's full card.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Profile {
    pub user_id: Id,
    pub display_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phone: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bio: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_path: Option<String>,
    /// YYYY-MM-DD, or --MM-DD when the year is hidden.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub birthday: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_blocked: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_contact: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_premium: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub common_chats_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<OnlineStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext: Option<Ext>,
}

// ---- dto/session.json ----

/// A logged-in device.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Session {
    pub id: Id,
    pub device: String,
    /// Operating system.
    pub platform: String,
    pub app: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_at: Option<Timestamp>,
    pub current: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub official: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext: Option<Ext>,
}

// ---- dto/sticker.json ----

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sticker {
    pub id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pack_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
    pub format: StickerFormat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thumb_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u64>,
    /// Default: false.
    #[serde(default)]
    pub premium: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext: Option<Ext>,
}

str_enum! {
    StickerPackKind {
    Stickers = "stickers",
    Emoji = "emoji",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StickerPack {
    pub id: Id,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub short_name: Option<String>,
    /// Default: 0.
    #[serde(default)]
    pub count: u64,
    pub kind: StickerPackKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thumb_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext: Option<Ext>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Gif {
    pub id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thumb_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext: Option<Ext>,
}

// ---- dto/story.json ----

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Story {
    pub id: Id,
    pub user_id: Id,
    pub media: Media,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caption: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entities: Option<Vec<Entity>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub posted_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext: Option<Ext>,
}

// ---- dto/topic.json ----

/// A forum topic inside a group. Merge-updated like `Conversation` (absent = unchanged).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Topic {
    pub id: Id,
    pub chat_id: Id,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon_emoji: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon_custom_emoji_id: Option<Id>,
    /// 24-bit RGB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon_color: Option<u32>,
    /// Absent = unchanged; the core uses `false` when it creates the row (spec §8.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_general: Option<bool>,
    /// Absent = unchanged; the core uses `0` when it creates the row (spec §8.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unread_count: Option<u64>,
    /// Absent = unchanged; the core uses `0` when it creates the row (spec §8.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unread_mentions: Option<u64>,
    /// Absent = unchanged; the core uses `0` when it creates the row (spec §8.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unread_reactions: Option<u64>,
    /// Absent = unchanged; the core uses `false` when it creates the row (spec §8.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned: Option<bool>,
    /// Absent = unchanged; the core uses `false` when it creates the row (spec §8.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed: Option<bool>,
    /// Absent = unchanged; the core uses `false` when it creates the row (spec §8.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hidden: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_message_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_message_preview: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub draft: Option<Draft>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_inbox_max_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext: Option<Ext>,
}

// ---- hand-written: ReactionKey (a one-of, not expressible as a plain struct) ----

/// Exactly one of `{"emoji": "❤"}`, `{"custom_emoji_id": "…"}`, `{"paid": true}`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ReactionKey {
    /// A standard emoji.
    Emoji {
        /// The emoji itself.
        emoji: String,
    },
    /// A custom emoji (needs `reactions.custom`).
    CustomEmoji {
        /// The custom emoji's id.
        custom_emoji_id: Id,
    },
    /// A paid reaction (needs `paid.react`).
    Paid {
        /// Always `true`.
        paid: bool,
    },
}

impl ReactionKey {
    /// A standard emoji.
    pub fn emoji(e: impl Into<String>) -> Self {
        Self::Emoji { emoji: e.into() }
    }
    /// A custom emoji.
    pub fn custom(id: impl Into<Id>) -> Self {
        Self::CustomEmoji {
            custom_emoji_id: id.into(),
        }
    }
    /// The paid reaction.
    pub fn paid() -> Self {
        Self::Paid { paid: true }
    }
}
