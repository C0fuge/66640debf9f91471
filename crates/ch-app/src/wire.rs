use ch_core::{
    history::ChatKey,
    history::HistoryError,
    host::{HostError, PluginState},
    outbox::OutboxError,
    runtime::RuntimeError,
};
use ch_protocol::{
    ErrorCode,
    dto::{Entity, EntityType},
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

pub type Object = Map<String, Value>;
pub(crate) type Result<T> = std::result::Result<T, Failure>;

#[derive(Debug, Clone, Copy)]
pub(crate) enum Failure {
    Invalid,
    Missing,
    Unsupported,
    Conflict,
    Unavailable,
    Internal,
    OutcomeUnknown,
}

impl Failure {
    pub(crate) fn reply(self) -> Value {
        let (code, message) = match self {
            Self::Invalid => ("invalid_request", "Invalid request or cursor."),
            Self::Missing => ("not_found", "The requested item was not found."),
            Self::Unsupported => (
                "unsupported",
                "This feature is unavailable for this account.",
            ),
            Self::Conflict => ("conflict", "The item changed. Reload its current state."),
            Self::Unavailable => ("unavailable", "The service is currently unavailable."),
            Self::Internal => ("internal", "The operation could not be completed."),
            Self::OutcomeUnknown => ("unavailable", "The action may have completed. Reload before trying again."),
        };
        let mut reply = json!({"ok":false,"error":{"code":code,"message":message}});
        if matches!(self, Self::OutcomeUnknown) {
            reply["error"]["outcomeUnknown"] = json!(true);
        }
        reply
    }
}

impl From<&HostError> for Failure {
    fn from(e: &HostError) -> Self {
        match e {
            HostError::UnknownAccount(_) | HostError::UnknownPlugin(_) => Self::Missing,
            HostError::AccountBusy(..)
            | HostError::AccountAlreadyStarted(_)
            | HostError::SingleAccount(_) => Self::Conflict,
            HostError::Rpc(err) => match err.code() {
                ErrorCode::InvalidParams | ErrorCode::InvalidRequest => Self::Invalid,
                ErrorCode::NotFound | ErrorCode::UnknownAccount => Self::Missing,
                ErrorCode::MethodNotFound | ErrorCode::NotSupported => Self::Unsupported,
                ErrorCode::AuthRequired | ErrorCode::Forbidden | ErrorCode::Cancelled => {
                    Self::Conflict
                }
                ErrorCode::Internal | ErrorCode::Unknown(_) => Self::Internal,
                _ => Self::Unavailable,
            },
            HostError::Json(_) | HostError::InvalidMethod(..) => Self::Internal,
            _ => Self::Unavailable,
        }
    }
}
impl From<RuntimeError> for Failure {
    fn from(e: RuntimeError) -> Self {
        match e {
            RuntimeError::UnknownAccount(_) | RuntimeError::UnknownPlugin(_) => Self::Missing,
            RuntimeError::InvalidSettings(_) => Self::Invalid,
            RuntimeError::AccountExists(_) => Self::Conflict,
            RuntimeError::PluginUnavailable { .. } | RuntimeError::Secret(_) => Self::Unavailable,
            RuntimeError::Host(e) => Self::from(&e),
            _ => Self::Internal,
        }
    }
}
impl From<HistoryError> for Failure {
    fn from(e: HistoryError) -> Self {
        match e {
            HistoryError::UnknownAccount(_) => Self::Missing,
            HistoryError::UnknownMessage(_) => Self::Invalid,
            HistoryError::Plugin(e) => Self::from(e.as_ref()),
            HistoryError::Storage(_) => Self::Internal,
            _ => Self::Unavailable,
        }
    }
}
impl From<OutboxError> for Failure {
    fn from(e: OutboxError) -> Self {
        match e {
            OutboxError::InvalidRequest(_) => Self::Invalid,
            OutboxError::NotFound(_) | OutboxError::UnknownAccount(_) => Self::Missing,
            OutboxError::WrongState { .. } => Self::Conflict,
            OutboxError::Stopped => Self::Unavailable,
            _ => Self::Internal,
        }
    }
}
impl From<ch_db::Error> for Failure {
    fn from(_: ch_db::Error) -> Self {
        Self::Internal
    }
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Thread {
    pub account_id: String,
    pub chat_id: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub topic_id: Option<String>,
}
impl Thread {
    pub fn chat_key(&self) -> ChatKey {
        ChatKey {
            account_id: self.account_id.clone(),
            chat_id: self.chat_id.clone(),
            topic_id: self.topic_id.clone(),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum Window {
    Latest,
    Refresh,
    Older { before: Option<String> },
    Newer { after: String },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Span {
    #[serde(rename = "type")]
    kind: EntityType,
    offset: u64,
    length: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    custom_emoji_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    collapsed: Option<bool>,
}

impl From<Span> for Entity {
    fn from(e: Span) -> Self {
        Self {
            kind: e.kind,
            offset: e.offset,
            length: e.length,
            url: e.url,
            user_id: e.user_id,
            language: e.language,
            custom_emoji_id: e.custom_emoji_id,
            collapsed: e.collapsed,
        }
    }
}
impl From<Entity> for Span {
    fn from(e: Entity) -> Self {
        Self {
            kind: e.kind,
            offset: e.offset,
            length: e.length,
            url: e.url,
            user_id: e.user_id,
            language: e.language,
            custom_emoji_id: e.custom_emoji_id,
            collapsed: e.collapsed,
        }
    }
}

#[derive(Deserialize)]
#[serde(
    tag = "method",
    content = "params",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum Request {
    Inventory {},
    ListChats {
        account_id: String,
        archived: Option<bool>,
    },
    History {
        thread: Thread,
        window: Window,
        limit: u32,
    },
    FillGap {
        thread: Thread,
        gap_id: String,
    },
    EnqueueText {
        thread: Thread,
        text: String,
        #[serde(default)]
        entities: Vec<Span>,
    },
    EditText { thread: Thread, message_id: String, text: String, entities: Vec<Span> },
    DeleteMessages { thread: Thread, message_ids: Vec<String>, revoke: bool },
    PinMessage { thread: Thread, message_id: String, silent: bool },
    UnpinMessage { thread: Thread, message_id: String },
    StartMediaDownload { thread: Thread, message_id: String, session_id: String, request_key: String },
    StartMediaSave { thread: Thread, message_id: String, session_id: String, request_key: String },
    ListMediaJobs { thread: Thread, #[serde(deserialize_with = "Option::deserialize")] cursor: Option<String>, #[serde(deserialize_with = "Option::deserialize")] request_key: Option<String> },
    CancelMediaJob { thread: Thread, session_id: String, job_id: String },
    VotePoll { thread: Thread, message_id: String, option_ids: Vec<String> },
    ClosePoll { thread: Thread, message_id: String },
    RefreshPoll { thread: Thread, message_id: String },
    ListOutbox {
        thread: Thread,
    },
    CancelOutbox {
        thread: Thread,
        id: String,
    },
    RetryOutbox {
        thread: Thread,
        id: String,
    },
    Search {
        thread: Thread,
        query: String,
        offset: u32,
        limit: u32,
        utc_offset_minutes: i32,
    },
    AddAccount {
        plugin_id: String,
        label: Option<String>,
        settings: Object,
    },
    GetSettings {
        account_id: String,
    },
    SaveSettings {
        account_id: String,
        values: Object,
    },
    GetAuthStep {
        account_id: String,
    },
    BeginAuth {
        account_id: String,
        method: String,
    },
    SubmitAuth {
        account_id: String,
        flow: String,
        step_id: String,
        value: String,
    },
    CancelAuth {
        account_id: String,
        flow: String,
    },
}

pub(crate) fn phase(s: &PluginState) -> &'static str {
    match s {
        PluginState::Unavailable(_) => "unavailable",
        PluginState::Stopped => "stopped",
        PluginState::Starting => "starting",
        PluginState::Running { .. } => "running",
        PluginState::Restarting { .. } => "restarting",
        PluginState::Failed { .. } => "failed",
        PluginState::Incompatible { .. } => "incompatible",
        PluginState::ShuttingDown => "shutting_down",
    }
}
