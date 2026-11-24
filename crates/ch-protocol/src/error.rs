//! Error codes and the error object (spec §13).

use serde::{Deserialize, Serialize};

str_enum! {
    /// The string error code in `error.data.code`. It is authoritative for behaviour; the
    /// numeric code mirrors it. Receivers treat an unknown code as `INTERNAL`.
    ErrorCode {
        /// -32700: the frame was not valid JSON.
        ParseError = "PARSE_ERROR",
        /// -32600: not a valid JSON-RPC object, or a batch array.
        InvalidRequest = "INVALID_REQUEST",
        /// -32601: unknown method.
        MethodNotFound = "METHOD_NOT_FOUND",
        /// -32602: params failed validation or are semantically wrong. `data.field?`.
        InvalidParams = "INVALID_PARAMS",
        /// -32603: anything unexpected.
        Internal = "INTERNAL",
        /// -32800: cancelled via `$/cancelRequest`.
        Cancelled = "CANCELLED",
        /// -32001: the platform or plugin cannot do this. `data.capability?`.
        NotSupported = "NOT_SUPPORTED",
        /// -32002: the account must sign in first.
        AuthRequired = "AUTH_REQUIRED",
        /// -32003: rate-limited by the platform. `data.seconds` is required.
        FloodWait = "FLOOD_WAIT",
        /// -32004: the chat, message, user, … does not exist or is not visible. `data.kind?`.
        NotFound = "NOT_FOUND",
        /// -32005: the platform refused. `data.reason?`.
        Forbidden = "FORBIDDEN",
        /// -32006: the platform is unreachable or the connection dropped. Retryable.
        Network = "NETWORK",
        /// -32007: the request exceeded its timeout.
        Timeout = "TIMEOUT",
        /// -32008: `account_id` is not started on this plugin.
        UnknownAccount = "UNKNOWN_ACCOUNT",
        /// -32009: the account is `unavailable` or `connecting`; try later.
        Unavailable = "UNAVAILABLE",
    }
}

impl ErrorCode {
    /// The fixed numeric code for this string code (§13). Unknown codes map to `INTERNAL`'s.
    pub fn numeric(&self) -> i64 {
        match self {
            Self::ParseError => -32700,
            Self::InvalidRequest => -32600,
            Self::MethodNotFound => -32601,
            Self::InvalidParams => -32602,
            Self::Internal | Self::Unknown(_) => -32603,
            Self::Cancelled => -32800,
            Self::NotSupported => -32001,
            Self::AuthRequired => -32002,
            Self::FloodWait => -32003,
            Self::NotFound => -32004,
            Self::Forbidden => -32005,
            Self::Network => -32006,
            Self::Timeout => -32007,
            Self::UnknownAccount => -32008,
            Self::Unavailable => -32009,
        }
    }

    /// The string code for a numeric code, if the table has it.
    pub fn from_numeric(n: i64) -> Option<Self> {
        Self::KNOWN
            .iter()
            .map(|s| Self::from(*s))
            .find(|c| c.numeric() == n)
    }

    /// The behaviour code: this one, or `Internal` for an unknown value (§13).
    pub fn effective(&self) -> Self {
        if self.is_known() {
            self.clone()
        } else {
            Self::Internal
        }
    }

    /// Is a retry worth it? (`NETWORK`, `TIMEOUT`, `UNAVAILABLE`, and `FLOOD_WAIT` after its delay.)
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Network | Self::Timeout | Self::Unavailable | Self::FloodWait
        )
    }
}

/// `error.data`: the string code plus code-specific extras. Unknown extras are kept in `extra`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorData {
    /// The authoritative code.
    pub code: ErrorCode,
    /// `FLOOD_WAIT`: how long to wait. Required there.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seconds: Option<u64>,
    /// `NOT_SUPPORTED`: the missing capability key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability: Option<String>,
    /// `INVALID_PARAMS`: the offending param.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// `NOT_FOUND`: what kind of thing (`chat`, `message`, …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// `FORBIDDEN`: why.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Anything else the sender put in `data`.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl ErrorData {
    /// Data with just a code.
    pub fn new(code: ErrorCode) -> Self {
        Self {
            code,
            seconds: None,
            capability: None,
            field: None,
            kind: None,
            reason: None,
            extra: Default::default(),
        }
    }
}

/// A JSON-RPC error object (§13).
///
/// `data` is required by the spec; it is `Option` here so that a bare JSON-RPC error from a
/// generic library still parses — [`ErrorObject::code`] then reports `INTERNAL`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[error("{message} [{}]", self.code())]
pub struct ErrorObject {
    /// The numeric code; mirrors `data.code`.
    pub code: i64,
    /// English, for logs. The UI shows its own text keyed by `data.code`.
    pub message: String,
    /// The string code and extras.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<ErrorData>,
}

impl ErrorObject {
    /// Build an error with the numeric code derived from the string code.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code: code.numeric(),
            message: message.into(),
            data: Some(ErrorData::new(code)),
        }
    }

    /// Set an extra on `data` (`seconds`, `capability`, …) by mutating it.
    pub fn with_data(mut self, f: impl FnOnce(&mut ErrorData)) -> Self {
        f(self
            .data
            .get_or_insert_with(|| ErrorData::new(ErrorCode::Internal)));
        self
    }

    /// `FLOOD_WAIT` with its mandatory `seconds`.
    pub fn flood_wait(seconds: u64) -> Self {
        Self::new(ErrorCode::FloodWait, "Too many requests")
            .with_data(|d| d.seconds = Some(seconds))
    }

    /// `NOT_SUPPORTED` naming the missing capability.
    pub fn not_supported(capability: impl Into<String>) -> Self {
        let capability = capability.into();
        Self::new(
            ErrorCode::NotSupported,
            format!("not supported: needs capability {capability}"),
        )
        .with_data(|d| d.capability = Some(capability))
    }

    /// The behaviour code: `data.code` when known, else `INTERNAL` (§13). The numeric code is
    /// consulted only when `data` is missing entirely.
    pub fn code(&self) -> ErrorCode {
        match &self.data {
            Some(d) => d.code.effective(),
            None => ErrorCode::from_numeric(self.code).unwrap_or(ErrorCode::Internal),
        }
    }
}
