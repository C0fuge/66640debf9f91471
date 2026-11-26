//! Types generated once from `schemas/methods/*.json` (API v1.2.0). The schemas are the contract; the
//! tests in `tests/` keep these types in step with them. `str_enum!` enums keep unknown values
//! as `Unknown(String)` (spec §18).
#![allow(clippy::struct_excessive_bools, missing_docs, unused_imports)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::capabilities::Capabilities;
use crate::common::*;
use crate::dto::*;
use crate::events::AvatarKind;

// ---- methods/common.json ----

/// reply_to on sending methods. quote_* need the quote_reply capability.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReplyTarget {
    pub message_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote_text: Option<String>,
    /// UTF-16 units into the replied message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote_offset: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageResult {
    pub message: Message,
}

/// message is present when the platform echoed the changed message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct OptionalMessageResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<Message>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MessagesResult {
    pub messages: Vec<Message>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PagedMessagesResult {
    pub messages: Vec<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<Cursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MessageIdsResult {
    pub message_ids: Vec<Id>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PathResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// A request that takes only account_id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct AccountParams {
    pub account_id: AccountId,
}

/// A request that takes account_id and chat_id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ChatParams {
    pub account_id: AccountId,
    pub chat_id: Id,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ChatTopicParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
}

/// A request that addresses one message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ChatMessageParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub message_id: Id,
}

/// A request that addresses several messages of one chat.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ChatMessagesParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub message_ids: Vec<Id>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ChatUserParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub user_id: Id,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct UserParams {
    pub account_id: AccountId,
    pub user_id: Id,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ChatTopicPagedParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    pub limit: Limit,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Cursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ChatTopicLimitParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    pub limit: Limit,
}

/// Options shared by every sending method; each is gated by its capability.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SendOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<ReplyTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silent: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_at: Option<Timestamp>,
}

// ---- methods/auth.json ----

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct AuthBeginParams {
    pub account_id: AccountId,
    /// One of the manifest's auth_methods.
    pub method: String,
}

pub type AuthBeginResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct AuthSubmitParams {
    pub account_id: AccountId,
    pub flow: String,
    pub step_id: String,
    pub value: String,
}

pub type AuthSubmitResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct AuthCancelParams {
    pub account_id: AccountId,
    pub flow: String,
}

pub type AuthCancelResult = Empty;

// ---- methods/bots.json ----

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BotCommand {
    /// Without the slash.
    pub command: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BotsCommandsParams {
    pub account_id: AccountId,
    pub bot_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<Id>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BotsCommandsResult {
    pub commands: Vec<BotCommand>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BotsPressButtonParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub message_id: Id,
    pub button: InlineButton,
    pub row: u64,
    pub column: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BotsPressButtonResult {
    /// A toast to show.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// A URL to open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<Message>,
}

// ---- methods/chats.json ----

pub type ChatsGetParams = ChatParams;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChatsGetResult {
    pub conversation: Conversation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ChatsSyncParams {
    pub account_id: AccountId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_archived: Option<bool>,
}

pub type ChatsSyncResult = Empty;

/// Only present fields are changed; each is gated by chat.pin / chat.mute / chat.archive / chat.mark_unread.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ChatsSetFlagsParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub muted: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mute_until: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub marked_unread: Option<bool>,
}

pub type ChatsSetFlagsResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ChatsClearHistoryParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoke: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
}

pub type ChatsClearHistoryResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ChatsLeaveParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete: Option<bool>,
}

pub type ChatsLeaveResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ChatsSetAutoDeleteParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub seconds: u64,
}

pub type ChatsSetAutoDeleteResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ChatsReorderPinnedParams {
    pub account_id: AccountId,
    /// Top first.
    pub chat_ids: Vec<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder_id: Option<Id>,
}

pub type ChatsReorderPinnedResult = Empty;

pub type FoldersListParams = AccountParams;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct FoldersListResult {
    pub folders: Vec<Folder>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct FoldersCreateParams {
    pub account_id: AccountId,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emoticon: Option<String>,
    pub chat_ids: Vec<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rules: Option<FolderRules>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FoldersCreateResult {
    pub folder: Folder,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct FoldersRenameParams {
    pub account_id: AccountId,
    pub folder_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emoticon: Option<String>,
}

pub type FoldersRenameResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct FoldersDeleteParams {
    pub account_id: AccountId,
    pub folder_id: Id,
}

pub type FoldersDeleteResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct FoldersSetChatsParams {
    pub account_id: AccountId,
    pub folder_id: Id,
    pub add: Vec<Id>,
    pub remove: Vec<Id>,
}

pub type FoldersSetChatsResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FoldersSetRulesParams {
    pub account_id: AccountId,
    pub folder_id: Id,
    pub rules: FolderRules,
}

pub type FoldersSetRulesResult = Empty;

// ---- methods/contacts.json ----

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct LookupUsernameParams {
    pub account_id: AccountId,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct LookupUsernameResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact: Option<Contact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation: Option<Conversation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct LookupPhoneParams {
    pub account_id: AccountId,
    pub phone: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct LookupPhoneResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact: Option<Contact>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ContactsResult {
    pub contacts: Vec<Contact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<Cursor>,
}

pub type ContactsListParams = AccountParams;

pub type ContactsListResult = ContactsResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ContactsSetParams {
    pub account_id: AccountId,
    pub user_id: Id,
    pub add: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phone: Option<String>,
}

pub type ContactsSetResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProfileResult {
    pub profile: Profile,
}

pub type ContactsInfoParams = UserParams;

pub type ContactsInfoResult = ProfileResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ContactsCommonChatsParams {
    pub account_id: AccountId,
    pub user_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<Limit>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ContactsCommonChatsResult {
    pub conversations: Vec<Conversation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BlockedListParams {
    pub account_id: AccountId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<Limit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Cursor>,
}

pub type BlockedListResult = ContactsResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BlockedSetParams {
    pub account_id: AccountId,
    pub user_id: Id,
    pub blocked: bool,
}

pub type BlockedSetResult = Empty;

pub type ProfileGetParams = AccountParams;

pub type ProfileGetResult = ProfileResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ProfileUpdateParams {
    pub account_id: AccountId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bio: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub photo_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remove_photo: Option<bool>,
}

pub type ProfileUpdateResult = ProfileResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ProfilePhotosParams {
    pub account_id: AccountId,
    pub user_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<Limit>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ProfilePhotosResult {
    pub paths: Vec<String>,
}

pub type StoriesListParams = UserParams;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct StoriesListResult {
    pub stories: Vec<Story>,
}

pub type PrivacyGetParams = AccountParams;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PrivacyGetResult {
    pub rules: Vec<PrivacyRule>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrivacySetParams {
    pub account_id: AccountId,
    pub key: String,
    pub value: PrivacyValue,
}

pub type PrivacySetResult = Empty;

pub type TwoFactorGetParams = AccountParams;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TwoFactorGetResult {
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_recovery_email: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TwoFactorSetParams {
    pub account_id: AccountId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_password: Option<String>,
    /// Absent or empty = remove.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

pub type TwoFactorSetResult = Empty;

pub type SessionsListParams = AccountParams;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SessionsListResult {
    pub sessions: Vec<Session>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SessionsTerminateParams {
    pub account_id: AccountId,
    /// Absent = all other sessions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<Id>,
}

pub type SessionsTerminateResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GhostApplyParams {
    pub account_id: AccountId,
    pub enabled: bool,
}

pub type GhostApplyResult = Empty;

pub type AccountSignOutParams = AccountParams;

pub type AccountSignOutResult = Empty;

// ---- methods/core.json ----

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SecretsSetParams {
    pub account_id: AccountId,
    pub key: String,
    pub value: String,
}

pub type SecretsSetResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SecretsDeleteParams {
    pub account_id: AccountId,
    pub key: String,
}

pub type SecretsDeleteResult = Empty;

// ---- methods/drafts.json ----

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DraftsSaveParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    pub draft: Draft,
}

pub type DraftsSaveResult = Empty;

pub type ScheduledListParams = ChatParams;

pub type ScheduledListResult = MessagesResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ScheduledEditParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub message_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entities: Option<Vec<Entity>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_at: Option<Timestamp>,
}

pub type ScheduledEditResult = OptionalMessageResult;

pub type ScheduledDeleteParams = ChatMessagesParams;

pub type ScheduledDeleteResult = Empty;

pub type ScheduledSendNowParams = ChatMessagesParams;

pub type ScheduledSendNowResult = Empty;

// ---- methods/groups.json ----

str_enum! {
    GroupKind {
    Group = "group",
    Channel = "channel",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GroupCreateParams {
    pub account_id: AccountId,
    pub title: String,
    pub user_ids: Vec<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<GroupKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub about: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GroupCreateResult {
    pub chat_id: Id,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GroupJoinByLinkParams {
    pub account_id: AccountId,
    pub link: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GroupJoinByLinkResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<Id>,
    /// A join request was filed instead of joining.
    pub pending: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GroupInviteLinkParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoke: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GroupInviteLinkResult {
    pub link: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GroupEditTitleParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub title: String,
}

pub type GroupEditTitleResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GroupEditAboutParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub about: String,
}

pub type GroupEditAboutResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GroupEditPhotoParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    /// Absent = remove the photo.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

pub type GroupEditPhotoResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GroupSetPermissionsParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub permissions: BTreeMap<String, bool>,
}

pub type GroupSetPermissionsResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GroupSetSlowModeParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub seconds: u64,
}

pub type GroupSetSlowModeResult = Empty;

str_enum! {
    MemberFilter {
    All = "all",
    Admins = "admins",
    Banned = "banned",
    Restricted = "restricted",
    Bots = "bots",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MembersListParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    /// Needs members.search.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<MemberFilter>,
    pub limit: Limit,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Cursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MembersListResult {
    pub members: Vec<Member>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<Cursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MembersBanParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub user_id: Id,
    pub banned: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<Timestamp>,
}

pub type MembersBanResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MembersSetAdminParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub user_id: Id,
    pub admin: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rights: Option<BTreeMap<String, bool>>,
}

pub type MembersSetAdminResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MembersInviteParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub user_ids: Vec<Id>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MembersInviteResult {
    /// Users who could not be added.
    pub failed: Vec<Id>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct JoinRequestsListParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub limit: Limit,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Cursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct JoinRequestsListResult {
    pub requests: Vec<JoinRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<Cursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct JoinRequestsAnswerParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub user_id: Id,
    pub approve: bool,
}

pub type JoinRequestsAnswerResult = Empty;

// ---- methods/history.json ----

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoryAroundParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    pub at: Timestamp,
    pub limit: Limit,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct HistoryAroundResult {
    pub messages: Vec<Message>,
    /// For messages.fetch_recent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub older_cursor: Option<Cursor>,
    /// For history.fill_gap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newer_cursor: Option<Cursor>,
}

/// Messages newer than after_message_id (and older than before_message_id when given), oldest first.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct HistoryFillGapParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    pub after_message_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_message_id: Option<Id>,
    pub limit: Limit,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Cursor>,
}

pub type HistoryFillGapResult = PagedMessagesResult;

// ---- methods/lifecycle.json ----

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InitializeParams {
    /// The API version the core speaks.
    pub api_version: String,
    pub core_version: String,
    pub plugin_dir: String,
    /// BCP 47.
    pub locale: String,
    pub log_level: LogLevel,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InitializeResult {
    /// The API version the plugin implements.
    pub api_version: String,
    pub capabilities: Capabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_version: Option<String>,
}

pub type ShutdownParams = Empty;

pub type ShutdownResult = Empty;

pub type PingParams = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PingResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub busy: Option<bool>,
}

/// What the plugin may fetch without being asked. 0 bytes = never.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct AutoDownload {
    pub photos: bool,
    pub videos_max_bytes: u64,
    pub files_max_bytes: u64,
    pub voice: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountStartParams {
    pub account_id: AccountId,
    /// settings_schema values with secrets merged in.
    pub settings: serde_json::Map<String, serde_json::Value>,
    /// Values stored via secrets.set.
    pub secrets: BTreeMap<String, String>,
    pub state_dir: String,
    pub cache_dir: String,
    pub ghost: bool,
    pub auto_download: AutoDownload,
}

pub type AccountStartResult = Empty;

pub type AccountStopParams = AccountParams;

pub type AccountStopResult = Empty;

// ---- methods/media.json ----

str_enum! {
    MediaWhat {
    Full = "full",
    Thumb = "thumb",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MediaDownloadParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub message_id: Id,
    pub what: MediaWhat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MediaDownloadResult {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
}

str_enum! {
    MediaSearchKind {
    Photo = "photo",
    Video = "video",
    Music = "music",
    Voice = "voice",
    Document = "document",
    Link = "link",
    Gif = "gif",
    Round = "round",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MediaSearchParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    pub kind: MediaSearchKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    pub limit: Limit,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Cursor>,
}

pub type MediaSearchResult = PagedMessagesResult;

str_enum! {
    AvatarSize {
    Small = "small",
    Big = "big",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AvatarsDownloadParams {
    pub account_id: AccountId,
    pub kind: AvatarKind,
    pub id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<AvatarSize>,
}

pub type AvatarsDownloadResult = PathResult;

// ---- methods/messages.json ----

/// Required. Newest page, or the page before cursor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MessagesFetchRecentParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    pub limit: Limit,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Cursor>,
}

pub type MessagesFetchRecentResult = PagedMessagesResult;

pub type MessagesGetParams = ChatMessageParams;

pub type MessagesGetResult = OptionalMessageResult;

/// Required method; the optional fields are each gated by a capability.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MessagesSendParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entities: Option<Vec<Entity>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<ReplyTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment_to: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silent: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub no_webpage: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
}

pub type MessagesSendResult = MessageResult;

str_enum! {
    FileKind {
    Auto = "auto",
    Voice = "voice",
    VideoNote = "video_note",
    Photo = "photo",
    Video = "video",
    Document = "document",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MessagesSendFileParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caption: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entities: Option<Vec<Entity>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<ReplyTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_file: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spoiler: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<FileKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silent: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waveform: Option<Vec<u8>>,
}

pub type MessagesSendFileResult = MessageResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct AlbumItem {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caption: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entities: Option<Vec<Entity>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spoiler: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_file: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<FileKind>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MessagesSendAlbumParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub items: Vec<AlbumItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caption: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entities: Option<Vec<Entity>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<ReplyTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silent: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_at: Option<Timestamp>,
}

pub type MessagesSendAlbumResult = MessagesResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MessagesEditParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub message_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entities: Option<Vec<Entity>>,
    /// edit.media: replace the media with this file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub no_webpage: Option<bool>,
}

pub type MessagesEditResult = MessageResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MessagesDeleteParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub message_ids: Vec<Id>,
    /// true = for everyone (delete); false = only for me (delete.for_me).
    pub revoke: bool,
}

pub type MessagesDeleteResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MessagesForwardParams {
    pub account_id: AccountId,
    pub from_chat_id: Id,
    pub message_ids: Vec<Id>,
    pub to_chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drop_author: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drop_captions: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silent: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_at: Option<Timestamp>,
}

pub type MessagesForwardResult = MessagesResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MessagesPinParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub message_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silent: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub for_me_only: Option<bool>,
}

pub type MessagesPinResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MessagesUnpinParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    /// Absent = unpin all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<Id>,
}

pub type MessagesUnpinResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SeenByEntry {
    pub user_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<Timestamp>,
}

pub type MessagesSeenByParams = ChatMessageParams;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MessagesSeenByResult {
    pub users: Vec<SeenByEntry>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MessagesTranslateParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub message_id: Id,
    /// BCP 47.
    pub to_language: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MessagesTranslateResult {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entities: Option<Vec<Entity>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_language: Option<String>,
}

pub type MessagesTranscribeParams = ChatMessageParams;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MessagesTranscribeResult {
    pub text: String,
    /// true: still being worked on; call again later.
    pub pending: bool,
}

// ---- methods/polls.json ----

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PollsCreateParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub question: String,
    pub options: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multiple: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quiz: Option<bool>,
    /// Default true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anonymous: Option<bool>,
    /// Quiz: index into options.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correct_option: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub solution: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub solution_entities: Option<Vec<Entity>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_after_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<ReplyTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silent: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_at: Option<Timestamp>,
}

pub type PollsCreateResult = MessageResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PollsVoteParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub message_id: Id,
    /// [] retracts.
    pub option_ids: Vec<Id>,
}

pub type PollsVoteResult = OptionalMessageResult;

pub type PollsCloseParams = ChatMessageParams;

pub type PollsCloseResult = OptionalMessageResult;

// ---- methods/reactions.json ----

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReactionsAvailableParams {
    pub account_id: AccountId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<Id>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReactionsAvailableResult {
    /// Absent = any emoji; [] = none allowed here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reactions: Option<Vec<ReactionKey>>,
    pub allow_custom: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_per_message: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReactionsSetParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub message_id: Id,
    /// The full new set; [] clears.
    pub reactions: Vec<ReactionKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub big: Option<bool>,
}

pub type ReactionsSetResult = OptionalMessageResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReactionsPaidParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub message_id: Id,
    pub count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anonymous: Option<bool>,
}

pub type ReactionsPaidResult = OptionalMessageResult;

pub type ReactionsPaidBalanceParams = AccountParams;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReactionsPaidBalanceResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balance: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReactionsWhoParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub message_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<ReactionKey>,
    pub limit: Limit,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Cursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReactionsWhoResult {
    pub reactors: Vec<Reactor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<Cursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReactionsEffectParams {
    pub account_id: AccountId,
    pub key: ReactionKey,
}

pub type ReactionsEffectResult = PathResult;

pub type ReactionsListUnreadParams = ChatTopicLimitParams;

pub type ReactionsListUnreadResult = MessageIdsResult;

pub type ReactionsReadParams = ChatTopicParams;

pub type ReactionsReadResult = Empty;

// ---- methods/read.json ----

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReadSendReceiptParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    pub max_message_id: Id,
}

pub type ReadSendReceiptResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypingSendParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    pub action: PresenceAction,
}

pub type TypingSendResult = Empty;

pub type MentionsListUnreadParams = ChatTopicLimitParams;

pub type MentionsListUnreadResult = MessageIdsResult;

pub type MentionsReadParams = ChatTopicParams;

pub type MentionsReadResult = Empty;

pub type PinsListParams = ChatTopicPagedParams;

pub type PinsListResult = PagedMessagesResult;

// ---- methods/stickers.json ----

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct StickersResult {
    pub stickers: Vec<Sticker>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PacksResult {
    pub packs: Vec<StickerPack>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PackResult {
    pub pack: StickerPack,
    pub stickers: Vec<Sticker>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct AccountLimitParams {
    pub account_id: AccountId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<Limit>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PackParams {
    pub account_id: AccountId,
    pub pack_id: Id,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct StickerParams {
    pub account_id: AccountId,
    pub sticker_id: Id,
}

/// Shared by stickers.send and gifs.send; the item id field differs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SendItemParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<ReplyTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silent: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_at: Option<Timestamp>,
}

pub type StickersRecentParams = AccountLimitParams;

pub type StickersRecentResult = StickersResult;

pub type StickersFavoritesParams = AccountParams;

pub type StickersFavoritesResult = StickersResult;

pub type StickersPacksParams = AccountParams;

pub type StickersPacksResult = PacksResult;

pub type StickersPackParams = PackParams;

pub type StickersPackResult = PackResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct StickersSearchParams {
    pub account_id: AccountId,
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<Limit>,
}

pub type StickersSearchResult = PacksResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct StickersInstallParams {
    pub account_id: AccountId,
    pub pack_id: Id,
    pub installed: bool,
}

pub type StickersInstallResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct StickersSetFavoriteParams {
    pub account_id: AccountId,
    pub sticker_id: Id,
    pub favorite: bool,
}

pub type StickersSetFavoriteResult = Empty;

pub type StickersThumbParams = StickerParams;

pub type StickersThumbResult = PathResult;

pub type StickersClipParams = StickerParams;

pub type StickersClipResult = PathResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct StickersSendParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<ReplyTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silent: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_at: Option<Timestamp>,
    pub sticker_id: Id,
}

pub type StickersSendResult = MessageResult;

pub type GifsListParams = AccountLimitParams;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GifsResult {
    pub gifs: Vec<Gif>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<Cursor>,
}

pub type GifsListResult = GifsResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GifsSearchParams {
    pub account_id: AccountId,
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<Limit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Cursor>,
}

pub type GifsSearchResult = GifsResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GifParams {
    pub account_id: AccountId,
    pub gif_id: Id,
}

pub type GifsThumbParams = GifParams;

pub type GifsThumbResult = PathResult;

pub type GifsClipParams = GifParams;

pub type GifsClipResult = PathResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GifsSendParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<ReplyTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silent: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_at: Option<Timestamp>,
    pub gif_id: Id,
}

pub type GifsSendResult = MessageResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct EmojiCustomFilesParams {
    pub account_id: AccountId,
    pub ids: Vec<Id>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct EmojiCustomFilesResult {
    /// id -> path; missing keys are unavailable.
    pub files: BTreeMap<String, String>,
}

pub type EmojiPacksParams = AccountParams;

pub type EmojiPacksResult = PacksResult;

pub type EmojiPackParams = PackParams;

pub type EmojiPackResult = PackResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct EmojiAnimatedParams {
    pub account_id: AccountId,
    pub emoji: String,
}

pub type EmojiAnimatedResult = PathResult;

// ---- methods/threads.json ----

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct RepliesListParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub message_id: Id,
    pub limit: Limit,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Cursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct RepliesListResult {
    pub messages: Vec<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_chat_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<Cursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TopicsListParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<Limit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Cursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TopicsListResult {
    pub topics: Vec<Topic>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<Cursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TopicsCreateParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon_color: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon_custom_emoji_id: Option<Id>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopicsCreateResult {
    pub topic: Topic,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TopicsEditParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub topic_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hidden: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon_custom_emoji_id: Option<Id>,
}

pub type TopicsEditResult = Empty;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TopicsPinParams {
    pub account_id: AccountId,
    pub chat_id: Id,
    pub topic_id: Id,
    pub pinned: bool,
}

pub type TopicsPinResult = Empty;
