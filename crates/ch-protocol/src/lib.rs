//! Contact Hub plugin API, version 1.1.0 — the wire contract between the core and a plugin.
//!
//! This crate is the Rust rendering of `docs/plugin-api.md` and `schemas/`: the JSON-RPC
//! envelope, the manifest, every DTO, event, method params/result, the error codes, the
//! capability registry and the versioning rule. It contains no I/O; the plugin host and the
//! Rust plugin SDK build on it.
//!
//! Tolerance rules (spec §8.4, §18) are baked in: unknown fields are ignored and dropped,
//! `null` and absent are the same for optional fields, unknown enum values are kept as
//! `Unknown(String)`, and `ext` is carried verbatim.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
// Wire enums (Event, Response) wrap whole DTOs; boxing them would cost every SDK user more
// than the few hundred bytes of padding it saves.
#![allow(clippy::large_enum_variant, clippy::result_large_err)]

use serde::{Deserialize, Serialize};

/// Declares a string enum whose unknown values survive as `Unknown(String)` (spec §18).
macro_rules! str_enum {
    ($(#[$m:meta])* $name:ident { $($(#[$vm:meta])* $variant:ident = $s:literal,)* }) => {
        $(#[$m])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        #[allow(missing_docs)]
        pub enum $name {
            $($(#[$vm])* $variant,)*
            /// A value this version of the API does not know. Treat it as "other" and keep going.
            Unknown(String),
        }
        impl $name {
            /// Every value this version knows, as wire strings.
            pub const KNOWN: &'static [&'static str] = &[$($s),*];
            /// The wire string.
            pub fn as_str(&self) -> &str {
                match self { $(Self::$variant => $s,)* Self::Unknown(s) => s }
            }
            /// `true` unless this is an `Unknown` value.
            pub fn is_known(&self) -> bool { !matches!(self, Self::Unknown(_)) }
        }
        impl From<&str> for $name {
            fn from(s: &str) -> Self { match s { $($s => Self::$variant,)* _ => Self::Unknown(s.to_owned()) } }
        }
        impl From<String> for $name {
            fn from(s: String) -> Self { Self::from(s.as_str()) }
        }
        impl From<$name> for String {
            fn from(v: $name) -> String { v.as_str().to_owned() }
        }
        impl ::std::str::FromStr for $name {
            type Err = ::std::convert::Infallible;
            fn from_str(s: &str) -> Result<Self, Self::Err> { Ok(Self::from(s)) }
        }
        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result { f.write_str(self.as_str()) }
        }
        impl ::serde::Serialize for $name {
            fn serialize<S: ::serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> { s.serialize_str(self.as_str()) }
        }
        impl<'de> ::serde::Deserialize<'de> for $name {
            fn deserialize<D: ::serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                String::deserialize(d).map(Self::from)
            }
        }
    };
}

pub mod capabilities;
pub mod common;
pub mod dto;
pub mod error;
pub mod events;
pub mod jsonrpc;
pub mod manifest;
pub mod methods;
pub mod registry;

pub use capabilities::{Capabilities, CapabilityKey, Flag};
pub use common::{AccountId, Cursor, Empty, Ext, Id, Limit, Timestamp};
pub use error::{ErrorCode, ErrorData, ErrorObject};
pub use events::{EVENT_NAMES, Event};
pub use jsonrpc::{
    CANCEL_METHOD, CancelParams, Envelope, ErrorResponse, JSONRPC_VERSION, MAX_FRAME_BYTES,
    Notification, Request, RequestId, Response, SuccessResponse,
};
pub use manifest::{AuthMethod, Manifest};
pub use registry::{GatedParam, GatedValue, METHODS, MethodInfo};

/// The API version this crate implements (spec §18).
pub const API_VERSION: &str = "1.1.0";

/// The major version of [`API_VERSION`]; the compatibility rule compares only this.
pub const API_MAJOR: u64 = 1;

/// The methods every plugin must implement (spec §18); fixed for v1.
pub const REQUIRED_METHODS: &[&str] = &[
    "initialize",
    "shutdown",
    "ping",
    "account.start",
    "account.stop",
    "messages.fetch_recent",
    "messages.send",
];

/// Interval between `ping`s per plugin process, in seconds (spec §4.3).
pub const PING_INTERVAL_SECONDS: u64 = 15;
/// Consecutive missed pings that count as a hang (spec §4.3).
pub const PING_MISSES_BEFORE_RESTART: u32 = 3;
/// Timeout for any method the registry does not list explicitly (spec §4.6).
pub const DEFAULT_TIMEOUT_SECONDS: u32 = 60;
/// Grace after `shutdown` before SIGTERM (spec §4.5).
pub const SHUTDOWN_GRACE_SECONDS: u64 = 10;
/// Seconds a plugin has to exit after EOF on stdin or SIGTERM (spec §2.5, §4.5).
pub const EXIT_GRACE_SECONDS: u64 = 5;

/// Environment variables the core sets for the plugin process (spec §3.3).
pub mod env {
    /// The manifest `id`.
    pub const PLUGIN_ID: &str = "CH_PLUGIN_ID";
    /// The API version the core speaks.
    pub const API_VERSION: &str = "CH_API_VERSION";
    /// Log-level hint: `error`, `warn`, `info`, `debug`, `trace`.
    pub const LOG_LEVEL: &str = "CH_LOG_LEVEL";
}

/// Who sends a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallDirection {
    /// The core calls the plugin (spec §11).
    CoreToPlugin,
    /// The plugin calls the core (spec §12).
    PluginToCore,
}

/// A parsed semver `MAJOR.MINOR.PATCH` (pre-release and build metadata are accepted and kept).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ApiVersion {
    /// Major: the compatibility unit.
    pub major: u64,
    /// Minor: additive changes.
    pub minor: u64,
    /// Patch: text and schema clarifications.
    pub patch: u64,
    /// Anything after the patch (`-rc.1`, `+build`), verbatim.
    pub rest: String,
}

impl ApiVersion {
    /// The version of this crate.
    pub fn current() -> Self {
        Self::parse(API_VERSION).expect("API_VERSION is valid semver")
    }

    /// Parse `MAJOR.MINOR.PATCH[-pre][+build]`.
    pub fn parse(s: &str) -> Result<Self, ParseVersionError> {
        let core_end = s.find(['-', '+']).unwrap_or(s.len());
        let (core, rest) = s.split_at(core_end);
        let mut parts = core.split('.');
        let mut num = |what: &'static str| -> Result<u64, ParseVersionError> {
            let p = parts.next().ok_or(ParseVersionError::Missing(what))?;
            if p.is_empty()
                || (p.len() > 1 && p.starts_with('0'))
                || !p.bytes().all(|b| b.is_ascii_digit())
            {
                return Err(ParseVersionError::Invalid(what));
            }
            p.parse().map_err(|_| ParseVersionError::Invalid(what))
        };
        let major = num("major")?;
        let minor = num("minor")?;
        let patch = num("patch")?;
        if parts.next().is_some() {
            return Err(ParseVersionError::Invalid("extra component"));
        }
        Ok(Self {
            major,
            minor,
            patch,
            rest: rest.to_owned(),
        })
    }

    /// The spec's rule (§18): two versions are compatible when their majors are equal.
    pub fn is_compatible_with(&self, other: &Self) -> bool {
        self.major == other.major
    }
}

impl std::fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}{}",
            self.major, self.minor, self.patch, self.rest
        )
    }
}

/// Why a version string did not parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ParseVersionError {
    /// A component is missing.
    #[error("semver: missing {0}")]
    Missing(&'static str),
    /// A component is not a plain non-negative integer, or there are too many.
    #[error("semver: invalid {0}")]
    Invalid(&'static str),
}

/// Are two `api_version` strings compatible (§18)? Unparseable versions are not.
pub fn versions_compatible(a: &str, b: &str) -> bool {
    match (ApiVersion::parse(a), ApiVersion::parse(b)) {
        (Ok(a), Ok(b)) => a.is_compatible_with(&b),
        _ => false,
    }
}

/// Look up a method in the registry by its wire name.
pub fn method(name: &str) -> Option<&'static MethodInfo> {
    METHODS.iter().find(|m| m.name == name)
}
