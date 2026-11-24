//! Types generated once from `schemas/capabilities.json` (API v1.0.0). The schemas are the contract; the
//! tests in `tests/` keep these types in step with them. `str_enum!` enums keep unknown values
//! as `Unknown(String)` (spec §18).
#![allow(clippy::struct_excessive_bools, missing_docs, unused_imports)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::common::*;
use crate::dto::{EntityType, PresenceAction};
use crate::methods::{GroupKind, MediaSearchKind};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SendEntitiesParams {
    /// Entity types the platform can send. Default: all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub types: Option<Vec<EntityType>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SendFileParams {
    /// Default: 2147483648.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SendAlbumParams {
    /// Default: 10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<u64>,
}

/// edit / delete: how long after sending the action is still allowed. null = no limit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct WindowParams {
    /// Default: null.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MediaSearchParams {
    /// Default: ["photo", "video", "music", "voice", "document", "link"].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kinds: Option<Vec<MediaSearchKind>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReactionsSetParams {
    /// Reactions one account may put on one message. Default: 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PollsCreateParams {
    /// Default: false.
    #[serde(default)]
    pub quiz: bool,
    /// Default: false.
    #[serde(default)]
    pub multiple: bool,
    /// Whether the anonymous switch is offered. Default: false.
    #[serde(default)]
    pub anonymous: bool,
    /// Default: 10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_options: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TypingParams {
    /// Default: ["typing"].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actions: Option<Vec<PresenceAction>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ChatClearParams {
    /// Whether clearing for everyone is possible. Default: false.
    #[serde(default)]
    pub revoke: bool,
}

/// auto_delete / group.slow_mode: the values (seconds) the platform accepts; 0 = off.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ChoicesParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub choices: Option<Vec<u64>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GroupCreateParams {
    /// Default: ["group"].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kinds: Option<Vec<GroupKind>>,
}

/// group.permissions / privacy: the keys the platform knows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct KeysParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<String>>,
}

str_enum! {
    /// Every capability key of v1 (spec §14.3). Vendor keys `x-<plugin-id>.<name>` and keys from newer minors land in `Unknown`.
    CapabilityKey {
        MessagesGet = "messages.get",
        SendEntities = "send.entities",
        SendSchedule = "send.schedule",
        SendSilent = "send.silent",
        SendNoWebpage = "send.no_webpage",
        SendEffects = "send.effects",
        QuoteReply = "quote_reply",
        CommentsSend = "comments.send",
        SendFile = "send.file",
        SendVoice = "send.voice",
        SendVideoNote = "send.video_note",
        SendSpoiler = "send.spoiler",
        SendAsFile = "send.as_file",
        SendTtl = "send.ttl",
        SendAlbum = "send.album",
        Edit = "edit",
        EditCaption = "edit.caption",
        EditMedia = "edit.media",
        Delete = "delete",
        DeleteForMe = "delete.for_me",
        Forward = "forward",
        ForwardDropAuthor = "forward.drop_author",
        Pin = "pin",
        Unpin = "unpin",
        SeenBy = "seen_by",
        Translate = "translate",
        Transcribe = "transcribe",
        HistoryJumpToDate = "history.jump_to_date",
        HistoryFillGap = "history.fill_gap",
        MediaDownload = "media.download",
        MediaSearch = "media.search",
        Avatars = "avatars",
        ReactionsSet = "reactions.set",
        ReactionsCustom = "reactions.custom",
        PaidReact = "paid.react",
        ReactionsWho = "reactions.who",
        ReactionsEffects = "reactions.effects",
        ReactionsUnread = "reactions.unread",
        PollsCreate = "polls.create",
        PollsVote = "polls.vote",
        PollsClose = "polls.close",
        BotsCommands = "bots.commands",
        BotsButtons = "bots.buttons",
        ReadReceipts = "read_receipts",
        Typing = "typing",
        Presence = "presence",
        MentionsUnread = "mentions.unread",
        PinsList = "pins.list",
        DraftsSync = "drafts.sync",
        Scheduled = "scheduled",
        ScheduledEdit = "scheduled.edit",
        RepliesList = "replies.list",
        Topics = "topics",
        TopicsManage = "topics.manage",
        TopicsPin = "topics.pin",
        ChatSync = "chat.sync",
        ChatFlags = "chat.flags",
        ChatPin = "chat.pin",
        ChatMute = "chat.mute",
        ChatArchive = "chat.archive",
        ChatMarkUnread = "chat.mark_unread",
        ChatClear = "chat.clear",
        ChatLeave = "chat.leave",
        AutoDelete = "auto_delete",
        ChatReorderPinned = "chat.reorder_pinned",
        Folders = "folders",
        FoldersManage = "folders.manage",
        FoldersEdit = "folders.edit",
        FoldersRules = "folders.rules",
        GroupCreate = "group.create",
        GroupJoinByLink = "group.join_by_link",
        GroupInviteLink = "group.invite_link",
        GroupEdit = "group.edit",
        GroupPhoto = "group.photo",
        GroupPermissions = "group.permissions",
        GroupSlowMode = "group.slow_mode",
        Members = "members",
        MembersSearch = "members.search",
        MembersBan = "members.ban",
        MembersAdmin = "members.admin",
        MembersInvite = "members.invite",
        JoinRequests = "join_requests",
        LookupUsername = "lookup.username",
        LookupPhone = "lookup.phone",
        ContactsList = "contacts.list",
        ContactsEdit = "contacts.edit",
        ContactsInfo = "contacts.info",
        CommonChats = "common_chats",
        Blocked = "blocked",
        Profile = "profile",
        ProfileEdit = "profile.edit",
        ProfilePhotos = "profile.photos",
        Stories = "stories",
        Privacy = "privacy",
        TwoFactor = "two_factor",
        Sessions = "sessions",
        GhostMode = "ghost_mode",
        SignOut = "sign_out",
        AuthInteractive = "auth.interactive",
        Stickers = "stickers",
        StickersSend = "stickers.send",
        StickersSearch = "stickers.search",
        StickersManage = "stickers.manage",
        StickersFavorites = "stickers.favorites",
        StickersPlay = "stickers.play",
        Gifs = "gifs",
        GifsSend = "gifs.send",
        GifsSearch = "gifs.search",
        CustomEmoji = "custom_emoji",
        EmojiAnimated = "emoji.animated",
    }
}

/// Capability keys that take a parameter object, with the Rust type of that object.
pub const PARAMETERISED: &[(&str, &str)] = &[
    ("send.entities", "SendEntitiesParams"),
    ("send.file", "SendFileParams"),
    ("send.album", "SendAlbumParams"),
    ("edit", "WindowParams"),
    ("delete", "WindowParams"),
    ("media.search", "MediaSearchParams"),
    ("reactions.set", "ReactionsSetParams"),
    ("polls.create", "PollsCreateParams"),
    ("typing", "TypingParams"),
    ("chat.clear", "ChatClearParams"),
    ("auto_delete", "ChoicesParams"),
    ("group.slow_mode", "ChoicesParams"),
    ("group.create", "GroupCreateParams"),
    ("group.permissions", "KeysParams"),
    ("privacy", "KeysParams"),
];
