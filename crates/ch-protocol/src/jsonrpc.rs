//! The JSON-RPC 2.0 envelope (spec §2.3) and the `$/cancelRequest` notification (§11.0).
//!
//! Framing (NDJSON, §2.2) is the transport's job; this module only describes one frame's
//! JSON value.

use serde::{Deserialize, Serialize};

use crate::error::ErrorObject;

/// The only accepted `jsonrpc` value.
pub const JSONRPC_VERSION: &str = "2.0";
/// Maximum frame length in bytes, including the trailing newline (§2.2).
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
/// The cancellation notification (§11.0).
pub const CANCEL_METHOD: &str = "$/cancelRequest";

/// A request id: a number or a string, never null (§2.3). The core uses integers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    /// Integer id (the core's choice).
    Number(i64),
    /// String id (plugins may use these).
    String(String),
}

impl From<i64> for RequestId {
    fn from(n: i64) -> Self {
        Self::Number(n)
    }
}
impl From<String> for RequestId {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}
impl From<&str> for RequestId {
    fn from(s: &str) -> Self {
        Self::String(s.to_owned())
    }
}
impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Number(n) => write!(f, "{n}"),
            Self::String(s) => f.write_str(s),
        }
    }
}

fn jsonrpc_version() -> String {
    JSONRPC_VERSION.to_owned()
}

/// A request: has an `id` and expects exactly one response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Request<P = serde_json::Value> {
    /// Always `"2.0"`.
    #[serde(default = "jsonrpc_version")]
    pub jsonrpc: String,
    /// The sender's id for this request.
    pub id: RequestId,
    /// `namespace.verb`, or `$/cancelRequest`.
    pub method: String,
    /// Named parameters. Always an object when present; `{}` and absent are equivalent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<P>,
}

impl<P> Request<P> {
    /// A request with params.
    pub fn new(id: impl Into<RequestId>, method: impl Into<String>, params: P) -> Self {
        Self {
            jsonrpc: jsonrpc_version(),
            id: id.into(),
            method: method.into(),
            params: Some(params),
        }
    }
}

/// A notification: no `id`, never answered. Events (§10) and `$/cancelRequest` use it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Notification<P = serde_json::Value> {
    /// Always `"2.0"`.
    #[serde(default = "jsonrpc_version")]
    pub jsonrpc: String,
    /// The event name or `$/cancelRequest`.
    pub method: String,
    /// Named parameters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<P>,
}

impl<P> Notification<P> {
    /// A notification with params.
    pub fn new(method: impl Into<String>, params: P) -> Self {
        Self {
            jsonrpc: jsonrpc_version(),
            method: method.into(),
            params: Some(params),
        }
    }
}

/// A successful response. `result` is always an object, `{}` when there is nothing to say.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SuccessResponse<R = serde_json::Value> {
    /// Always `"2.0"`.
    #[serde(default = "jsonrpc_version")]
    pub jsonrpc: String,
    /// Echo of the request id.
    pub id: RequestId,
    /// The result object.
    pub result: R,
}

/// An error response (§13). `id` is `None` only when the request's id could not be read
/// (`PARSE_ERROR`, `INVALID_REQUEST`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorResponse {
    /// Always `"2.0"`.
    #[serde(default = "jsonrpc_version")]
    pub jsonrpc: String,
    /// Echo of the request id, or null.
    pub id: Option<RequestId>,
    /// What went wrong.
    pub error: ErrorObject,
}

/// Either kind of response to a request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Response<R = serde_json::Value> {
    /// The request succeeded.
    Success(SuccessResponse<R>),
    /// The request failed.
    Error(ErrorResponse),
}

impl<R> Response<R> {
    /// A success response.
    pub fn ok(id: impl Into<RequestId>, result: R) -> Self {
        Self::Success(SuccessResponse {
            jsonrpc: jsonrpc_version(),
            id: id.into(),
            result,
        })
    }
    /// An error response.
    pub fn err(id: impl Into<Option<RequestId>>, error: ErrorObject) -> Self {
        Self::Error(ErrorResponse {
            jsonrpc: jsonrpc_version(),
            id: id.into(),
            error,
        })
    }
    /// The id this response answers, if any.
    pub fn id(&self) -> Option<&RequestId> {
        match self {
            Self::Success(s) => Some(&s.id),
            Self::Error(e) => e.id.as_ref(),
        }
    }
    /// Into a `Result`.
    pub fn into_result(self) -> Result<R, ErrorObject> {
        match self {
            Self::Success(s) => Ok(s.result),
            Self::Error(e) => Err(e.error),
        }
    }
}

/// Any one frame, as received. Params and result are left as raw JSON for a second step.
///
/// The variant order matters for `untagged`: a request has `id` + `method`, a notification
/// `method` only, a success `id` + `result`, an error `id` + `error`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Envelope {
    /// A request (has `id` and `method`).
    Request(Request),
    /// A notification (has `method`, no `id`).
    Notification(Notification),
    /// A success response (has `id` and `result`).
    Success(SuccessResponse),
    /// An error response (has `id` and `error`).
    Error(ErrorResponse),
}

impl Envelope {
    /// The method name, for requests and notifications.
    pub fn method(&self) -> Option<&str> {
        match self {
            Self::Request(r) => Some(&r.method),
            Self::Notification(n) => Some(&n.method),
            _ => None,
        }
    }
    /// The id, for requests and responses.
    pub fn id(&self) -> Option<&RequestId> {
        match self {
            Self::Request(r) => Some(&r.id),
            Self::Success(s) => Some(&s.id),
            Self::Error(e) => e.id.as_ref(),
            Self::Notification(_) => None,
        }
    }
}

/// Params of `$/cancelRequest` (§11.0).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelParams {
    /// The id of the request to stop working on.
    pub id: RequestId,
}

impl CancelParams {
    /// The notification that cancels `id`.
    pub fn notification(id: impl Into<RequestId>) -> Notification<CancelParams> {
        Notification::new(CANCEL_METHOD, CancelParams { id: id.into() })
    }
}
