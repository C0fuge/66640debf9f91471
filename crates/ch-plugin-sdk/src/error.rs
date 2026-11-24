//! Errors: constructors for the spec's error objects (§13), and the SDK's own error types.
//!
//! Handlers return [`PluginError`], a boxed spec [`ErrorObject`] (boxed so that `Result`s stay
//! small and plugin crates need no `clippy::result_large_err` allowance). The functions here
//! build the common ones with the right `data` extras; [`ResultExt`] turns any other error
//! into `INTERNAL`; `?` converts an [`ErrorObject`] or an [`EmitError`].

use std::ops::{Deref, DerefMut};

use ch_protocol::{AccountId, ErrorCode, ErrorObject};

/// What a handler returns: a result, or a spec error object.
pub type HandlerResult<T> = Result<T, PluginError>;

/// A spec error object (§13) on the heap: what handlers, hooks and the [`crate::Settings`]
/// accessors fail with. Derefs to [`ErrorObject`] (`e.code`, `e.message`, `e.data`,
/// `e.code()` for the string code).
#[derive(Debug, Clone, PartialEq)]
pub struct PluginError(Box<ErrorObject>);

impl PluginError {
    /// Wrap an error object.
    pub fn new(e: ErrorObject) -> Self {
        Self(Box::new(e))
    }

    /// The error object.
    pub fn into_inner(self) -> ErrorObject {
        *self.0
    }

    /// The error object, by reference.
    pub fn object(&self) -> &ErrorObject {
        &self.0
    }
}

impl std::fmt::Display for PluginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for PluginError {}

impl Deref for PluginError {
    type Target = ErrorObject;
    fn deref(&self) -> &ErrorObject {
        &self.0
    }
}

impl DerefMut for PluginError {
    fn deref_mut(&mut self) -> &mut ErrorObject {
        &mut self.0
    }
}

impl From<ErrorObject> for PluginError {
    fn from(e: ErrorObject) -> Self {
        Self::new(e)
    }
}

impl From<Box<ErrorObject>> for PluginError {
    fn from(e: Box<ErrorObject>) -> Self {
        Self(e)
    }
}

impl From<PluginError> for ErrorObject {
    fn from(e: PluginError) -> Self {
        e.into_inner()
    }
}

fn err(code: ErrorCode, message: impl Into<String>) -> PluginError {
    PluginError::new(ErrorObject::new(code, message))
}

/// `INVALID_PARAMS`, naming the offending field. The message is shown to the user for
/// `account.start` (§11.1), so make it readable.
pub fn invalid_params(field: impl Into<String>, message: impl Into<String>) -> PluginError {
    let field = field.into();
    ErrorObject::new(ErrorCode::InvalidParams, message)
        .with_data(|d| d.field = Some(field))
        .into()
}

/// `INTERNAL`: anything unexpected.
pub fn internal(message: impl Into<String>) -> PluginError {
    err(ErrorCode::Internal, message)
}

/// `NOT_SUPPORTED` for a missing capability (§13, §14.2). Same as
/// [`ErrorObject::not_supported`].
pub fn not_supported(capability: impl Into<String>) -> PluginError {
    ErrorObject::not_supported(capability).into()
}

/// `FLOOD_WAIT` with its mandatory `seconds` (§13). Same as [`ErrorObject::flood_wait`].
pub fn flood_wait(seconds: u64) -> PluginError {
    ErrorObject::flood_wait(seconds).into()
}

/// `NOT_FOUND`, naming the kind of object (`chat`, `message`, `user`, …).
pub fn not_found(kind: impl Into<String>, message: impl Into<String>) -> PluginError {
    let kind = kind.into();
    ErrorObject::new(ErrorCode::NotFound, message)
        .with_data(|d| d.kind = Some(kind))
        .into()
}

/// `FORBIDDEN`: the platform refused, with a reason.
pub fn forbidden(reason: impl Into<String>) -> PluginError {
    let reason = reason.into();
    ErrorObject::new(ErrorCode::Forbidden, format!("forbidden: {reason}"))
        .with_data(|d| d.reason = Some(reason))
        .into()
}

/// `NETWORK`: the platform is unreachable or the connection dropped. Retryable.
pub fn network(message: impl Into<String>) -> PluginError {
    err(ErrorCode::Network, message)
}

/// `AUTH_REQUIRED`: the account must sign in first.
pub fn auth_required() -> PluginError {
    err(ErrorCode::AuthRequired, "the account must sign in first")
}

/// `UNAVAILABLE`: the account is started but `connecting` or `unavailable`.
pub fn unavailable(message: impl Into<String>) -> PluginError {
    err(ErrorCode::Unavailable, message)
}

/// `TIMEOUT`: a platform-side timeout.
pub fn timeout(message: impl Into<String>) -> PluginError {
    err(ErrorCode::Timeout, message)
}

/// `CANCELLED`: the request was cancelled via `$/cancelRequest` (§11.0).
pub fn cancelled() -> PluginError {
    err(ErrorCode::Cancelled, "request cancelled")
}

/// `UNKNOWN_ACCOUNT`: `account_id` is not started on this plugin (§5).
pub fn unknown_account(account_id: &str) -> PluginError {
    err(
        ErrorCode::UnknownAccount,
        format!("account {account_id:?} is not started"),
    )
}

/// `INVALID_REQUEST`: not a valid request in this state (§2.3, §4.1).
pub(crate) fn invalid_request(message: impl Into<String>) -> PluginError {
    err(ErrorCode::InvalidRequest, message)
}

/// Map any displayable error to `INTERNAL`, so `?` works on library errors in handlers.
pub trait ResultExt<T> {
    /// `Err(e)` becomes `INTERNAL` with `e`'s text as the message.
    fn or_internal(self) -> Result<T, PluginError>;
    /// `Err(e)` becomes the error `f(e)` builds (`.or_error(|e| network(e.to_string()))`).
    fn or_error(self, f: impl FnOnce(String) -> PluginError) -> Result<T, PluginError>;
}

impl<T, E: std::fmt::Display> ResultExt<T> for Result<T, E> {
    fn or_internal(self) -> Result<T, PluginError> {
        self.map_err(|e| internal(e.to_string()))
    }
    fn or_error(self, f: impl FnOnce(String) -> PluginError) -> Result<T, PluginError> {
        self.map_err(|e| f(e.to_string()))
    }
}

/// Make an error object well-formed before it goes on the wire: `data` present, the numeric
/// code mirroring `data.code` (§13: the mapping is fixed; an unknown string code is sent with
/// `INTERNAL`'s number, its string kept), and `FLOOD_WAIT` carrying `seconds`.
pub(crate) fn normalize(e: PluginError) -> ErrorObject {
    let mut e = e.into_inner();
    let data = e.data.get_or_insert_with(|| {
        ch_protocol::ErrorData::new(ErrorCode::from_numeric(e.code).unwrap_or(ErrorCode::Internal))
    });
    e.code = data.code.numeric();
    if data.code == ErrorCode::FloodWait && data.seconds.is_none() {
        tracing::warn!("FLOOD_WAIT without seconds; sending seconds = 1 (spec §13 requires it)");
        data.seconds = Some(1);
    }
    if e.message.is_empty() {
        e.message = data.code.to_string();
    }
    e
}

/// Why an event or a plugin → core request could not be sent.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum EmitError {
    /// The account is not started, or `account.stop` has run for it (§4.2: no events after
    /// stop). Background tasks can treat this as "stop now".
    #[error("account {0:?} is not started")]
    AccountNotStarted(AccountId),
    /// The transport is closed (shutdown, or the core went away).
    #[error("transport closed")]
    Closed,
    /// The serialised frame exceeds the 16 MiB limit (§2.2). Split the batch (§16).
    #[error("frame of {0} bytes exceeds the 16 MiB limit")]
    FrameTooLarge(usize),
    /// The core answered the request with an error, kept as received (an unknown `data.code`
    /// included), or it timed out: `TIMEOUT`.
    #[error("core rejected the request: {0}")]
    Rejected(PluginError),
}

impl From<EmitError> for PluginError {
    fn from(e: EmitError) -> Self {
        match e {
            EmitError::Rejected(o) => o,
            other => internal(other.to_string()),
        }
    }
}

/// Why [`crate::PluginBuilder::build`] or [`crate::Plugin::run`] failed.
#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    /// The plugin definition breaks a spec rule (a required method missing, a declared
    /// capability without all its methods, a handler for a built-in method, a vendor method
    /// not named `x-<plugin id>.<name>`).
    #[error("invalid plugin definition: {0}")]
    Definition(String),
    /// A frame longer than 16 MiB arrived (§2.2); the stream was closed.
    #[error("incoming frame exceeds the 16 MiB limit")]
    FrameTooLarge,
    /// Reading or writing the transport failed.
    #[error("transport I/O: {0}")]
    Io(#[from] std::io::Error),
}
