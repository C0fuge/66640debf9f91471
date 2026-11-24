//! Types generated once from `schemas/events.json` (API v1.0.0). The schemas are the contract; the
//! tests in `tests/` keep these types in step with them. `str_enum!` enums keep unknown values
//! as `Unknown(String)` (spec §18).
#![allow(clippy::struct_excessive_bools, missing_docs, unused_imports)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::common::*;
use crate::dto::*;
use crate::jsonrpc::RequestId;

// ---- events.json ----

str_enum! {
    EventName {
    Status = "status",
    ConversationsUpsert = "conversations.upsert",
    ConversationsSynced = "conversations.synced",
    MessagesUpsert = "messages.upsert",
    MessagesDeleted = "messages.deleted",
    MessageEdited = "message.edited",
    ReactionChanged = "reaction.changed",
    ContactsUpsert = "contacts.upsert",
    FoldersSynced = "folders.synced",
    TopicsUpsert = "topics.upsert",
    ReadOutbox = "read.outbox",
    ReadInbox = "read.inbox",
    Presence = "presence",
    DraftChanged = "draft.changed",
    AvatarReady = "avatar.ready",
    MediaReady = "media.ready",
    TransferProgress = "transfer.progress",
    AuthStep = "auth.step",
    }
}

str_enum! {
    AccountState {
    Disconnected = "disconnected",
    Connecting = "connecting",
    AuthRequired = "auth_required",
    Connected = "connected",
    Unavailable = "unavailable",
    Error = "error",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusEvent {
    pub account_id: AccountId,
    pub state: AccountState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Default: false.
    #[serde(default)]
    pub reconnecting: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub me: Option<Contact>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ConversationsUpsertEvent {
    pub account_id: AccountId,
    pub conversations: Vec<Conversation>,
}

/// Every chat the account currently has. Chats not listed are hidden by the core.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ConversationsSyncedEvent {
    pub account_id: AccountId,
    pub chat_ids: Vec<Id>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MessagesUpsertEvent {
    pub account_id: AccountId,
    pub messages: Vec<Message>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MessagesDeletedEvent {
    pub account_id: AccountId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<Id>,
    pub message_ids: Vec<Id>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageEditedEvent {
    pub account_id: AccountId,
    pub message: Message,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReactionChangedEvent {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub message_id: Id,
    pub reactions: Vec<Reaction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reactors: Option<Vec<Reactor>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ContactsUpsertEvent {
    pub account_id: AccountId,
    pub contacts: Vec<Contact>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct FoldersSyncedEvent {
    pub account_id: AccountId,
    pub folders: Vec<Folder>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TopicsUpsertEvent {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub topics: Vec<Topic>,
    /// true: this is every topic of the chat. Default: false.
    #[serde(default)]
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReadOutboxEvent {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    pub max_message_id: Id,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReadInboxEvent {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    pub max_message_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unread_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PresenceEvent {
    pub account_id: AccountId,
    pub user_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub online: Option<OnlineStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<PresenceAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DraftChangedEvent {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    pub draft: Draft,
}

str_enum! {
    AvatarKind {
    User = "user",
    Chat = "chat",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AvatarReadyEvent {
    pub account_id: AccountId,
    pub kind: AvatarKind,
    pub id: Id,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MediaReadyEvent {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub message_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thumb_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

str_enum! {
    TransferDirection {
    Download = "download",
    Upload = "upload",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransferProgressEvent {
    pub account_id: AccountId,
    /// The id of the core request that caused this transfer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<RequestId>,
    pub direction: TransferDirection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<Id>,
    pub done_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthStepEvent {
    pub account_id: AccountId,
    pub step: AuthStep,
}

/// One plugin → core event (spec §10): the JSON-RPC method name plus its params.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Status(StatusEvent),
    ConversationsUpsert(ConversationsUpsertEvent),
    ConversationsSynced(ConversationsSyncedEvent),
    MessagesUpsert(MessagesUpsertEvent),
    MessagesDeleted(MessagesDeletedEvent),
    MessageEdited(MessageEditedEvent),
    ReactionChanged(ReactionChangedEvent),
    ContactsUpsert(ContactsUpsertEvent),
    FoldersSynced(FoldersSyncedEvent),
    TopicsUpsert(TopicsUpsertEvent),
    ReadOutbox(ReadOutboxEvent),
    ReadInbox(ReadInboxEvent),
    Presence(PresenceEvent),
    DraftChanged(DraftChangedEvent),
    AvatarReady(AvatarReadyEvent),
    MediaReady(MediaReadyEvent),
    TransferProgress(TransferProgressEvent),
    AuthStep(AuthStepEvent),
}

impl Event {
    /// The JSON-RPC method name of this event.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Status(_) => "status",
            Self::ConversationsUpsert(_) => "conversations.upsert",
            Self::ConversationsSynced(_) => "conversations.synced",
            Self::MessagesUpsert(_) => "messages.upsert",
            Self::MessagesDeleted(_) => "messages.deleted",
            Self::MessageEdited(_) => "message.edited",
            Self::ReactionChanged(_) => "reaction.changed",
            Self::ContactsUpsert(_) => "contacts.upsert",
            Self::FoldersSynced(_) => "folders.synced",
            Self::TopicsUpsert(_) => "topics.upsert",
            Self::ReadOutbox(_) => "read.outbox",
            Self::ReadInbox(_) => "read.inbox",
            Self::Presence(_) => "presence",
            Self::DraftChanged(_) => "draft.changed",
            Self::AvatarReady(_) => "avatar.ready",
            Self::MediaReady(_) => "media.ready",
            Self::TransferProgress(_) => "transfer.progress",
            Self::AuthStep(_) => "auth.step",
        }
    }

    /// Build an event from a method name and its params. `None` when the name is not an event this version knows (§18: ignore it).
    pub fn from_params(
        name: &str,
        params: serde_json::Value,
    ) -> Option<Result<Self, serde_json::Error>> {
        Some(match name {
            "status" => serde_json::from_value(params).map(Self::Status),
            "conversations.upsert" => serde_json::from_value(params).map(Self::ConversationsUpsert),
            "conversations.synced" => serde_json::from_value(params).map(Self::ConversationsSynced),
            "messages.upsert" => serde_json::from_value(params).map(Self::MessagesUpsert),
            "messages.deleted" => serde_json::from_value(params).map(Self::MessagesDeleted),
            "message.edited" => serde_json::from_value(params).map(Self::MessageEdited),
            "reaction.changed" => serde_json::from_value(params).map(Self::ReactionChanged),
            "contacts.upsert" => serde_json::from_value(params).map(Self::ContactsUpsert),
            "folders.synced" => serde_json::from_value(params).map(Self::FoldersSynced),
            "topics.upsert" => serde_json::from_value(params).map(Self::TopicsUpsert),
            "read.outbox" => serde_json::from_value(params).map(Self::ReadOutbox),
            "read.inbox" => serde_json::from_value(params).map(Self::ReadInbox),
            "presence" => serde_json::from_value(params).map(Self::Presence),
            "draft.changed" => serde_json::from_value(params).map(Self::DraftChanged),
            "avatar.ready" => serde_json::from_value(params).map(Self::AvatarReady),
            "media.ready" => serde_json::from_value(params).map(Self::MediaReady),
            "transfer.progress" => serde_json::from_value(params).map(Self::TransferProgress),
            "auth.step" => serde_json::from_value(params).map(Self::AuthStep),
            _ => return None,
        })
    }

    /// Serialise the params object.
    pub fn to_params(&self) -> serde_json::Value {
        match self {
            Self::Status(e) => serde_json::to_value(e).expect("event params serialise"),
            Self::ConversationsUpsert(e) => {
                serde_json::to_value(e).expect("event params serialise")
            }
            Self::ConversationsSynced(e) => {
                serde_json::to_value(e).expect("event params serialise")
            }
            Self::MessagesUpsert(e) => serde_json::to_value(e).expect("event params serialise"),
            Self::MessagesDeleted(e) => serde_json::to_value(e).expect("event params serialise"),
            Self::MessageEdited(e) => serde_json::to_value(e).expect("event params serialise"),
            Self::ReactionChanged(e) => serde_json::to_value(e).expect("event params serialise"),
            Self::ContactsUpsert(e) => serde_json::to_value(e).expect("event params serialise"),
            Self::FoldersSynced(e) => serde_json::to_value(e).expect("event params serialise"),
            Self::TopicsUpsert(e) => serde_json::to_value(e).expect("event params serialise"),
            Self::ReadOutbox(e) => serde_json::to_value(e).expect("event params serialise"),
            Self::ReadInbox(e) => serde_json::to_value(e).expect("event params serialise"),
            Self::Presence(e) => serde_json::to_value(e).expect("event params serialise"),
            Self::DraftChanged(e) => serde_json::to_value(e).expect("event params serialise"),
            Self::AvatarReady(e) => serde_json::to_value(e).expect("event params serialise"),
            Self::MediaReady(e) => serde_json::to_value(e).expect("event params serialise"),
            Self::TransferProgress(e) => serde_json::to_value(e).expect("event params serialise"),
            Self::AuthStep(e) => serde_json::to_value(e).expect("event params serialise"),
        }
    }

    /// The `account_id` every event carries.
    pub fn account_id(&self) -> &AccountId {
        match self {
            Self::Status(e) => &e.account_id,
            Self::ConversationsUpsert(e) => &e.account_id,
            Self::ConversationsSynced(e) => &e.account_id,
            Self::MessagesUpsert(e) => &e.account_id,
            Self::MessagesDeleted(e) => &e.account_id,
            Self::MessageEdited(e) => &e.account_id,
            Self::ReactionChanged(e) => &e.account_id,
            Self::ContactsUpsert(e) => &e.account_id,
            Self::FoldersSynced(e) => &e.account_id,
            Self::TopicsUpsert(e) => &e.account_id,
            Self::ReadOutbox(e) => &e.account_id,
            Self::ReadInbox(e) => &e.account_id,
            Self::Presence(e) => &e.account_id,
            Self::DraftChanged(e) => &e.account_id,
            Self::AvatarReady(e) => &e.account_id,
            Self::MediaReady(e) => &e.account_id,
            Self::TransferProgress(e) => &e.account_id,
            Self::AuthStep(e) => &e.account_id,
        }
    }
}

/// Every event name, in spec order.
pub const EVENT_NAMES: &[&str] = &[
    "status",
    "conversations.upsert",
    "conversations.synced",
    "messages.upsert",
    "messages.deleted",
    "message.edited",
    "reaction.changed",
    "contacts.upsert",
    "folders.synced",
    "topics.upsert",
    "read.outbox",
    "read.inbox",
    "presence",
    "draft.changed",
    "avatar.ready",
    "media.ready",
    "transfer.progress",
    "auth.step",
];
