//! Shared scalar types (spec §8): opaque ids, timestamps, `ext`, the empty result.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;

/// An opaque id (§8.1): a non-empty UTF-8 string of at most 256 bytes. Never parse it.
pub type Id = String;
/// The core-assigned account id (§5). Opaque; never parse it.
pub type AccountId = Id;
/// An opaque paging cursor (§11, "Common parameter conventions").
pub type Cursor = String;
/// A page size: 1..=200.
pub type Limit = u32;
/// Platform-specific extras keyed by plugin id (§8.5). Stored and returned verbatim.
pub type Ext = serde_json::Map<String, serde_json::Value>;

str_enum! {
    /// Log-level hint passed in `initialize` and `CH_LOG_LEVEL` (§3.3).
    LogLevel {
        Error = "error",
        Warn = "warn",
        Info = "info",
        Debug = "debug",
        Trace = "trace",
    }
}

/// Maximum byte length of an id (§8.1).
pub const MAX_ID_BYTES: usize = 256;

/// Is `s` a well-formed id (§8.1)?
pub fn is_valid_id(s: &str) -> bool {
    !s.is_empty() && s.len() <= MAX_ID_BYTES
}

/// `{}`: the result of methods that have nothing to say. Accepts any object on input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Empty {}

/// An instant with millisecond precision (§8.2).
///
/// Serialises as RFC 3339 UTC with exactly three fractional digits and a `Z`
/// (`2026-10-19T14:03:27.512Z`). Deserialises from any RFC 3339 `date-time`, normalising
/// to UTC and truncating to milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Timestamp(OffsetDateTime);

const WIRE: &[time::format_description::BorrowedFormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z");

impl Timestamp {
    /// Build from any `OffsetDateTime`; it is converted to UTC and truncated to milliseconds.
    pub fn new(t: OffsetDateTime) -> Self {
        let t = t.to_offset(time::UtcOffset::UTC);
        let ms = t.millisecond();
        Self(t.replace_millisecond(ms).expect("millisecond in range"))
    }

    /// Now, to the millisecond.
    pub fn now() -> Self {
        Self::new(OffsetDateTime::now_utc())
    }

    /// From unix milliseconds.
    pub fn from_unix_millis(ms: i64) -> Option<Self> {
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
            .ok()
            .map(Self::new)
    }

    /// Unix milliseconds.
    pub fn unix_millis(&self) -> i64 {
        (self.0.unix_timestamp_nanos() / 1_000_000) as i64
    }

    /// The underlying UTC instant.
    pub fn as_datetime(&self) -> OffsetDateTime {
        self.0
    }

    /// Parse any RFC 3339 `date-time`.
    pub fn parse(s: &str) -> Result<Self, time::error::Parse> {
        OffsetDateTime::parse(s, &Rfc3339).map(Self::new)
    }
}

impl From<OffsetDateTime> for Timestamp {
    fn from(t: OffsetDateTime) -> Self {
        Self::new(t)
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.format(WIRE).map_err(|_| std::fmt::Error)?)
    }
}

impl std::str::FromStr for Timestamp {
    type Err = time::error::Parse;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for Timestamp {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = <std::borrow::Cow<'de, str>>::deserialize(d)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}
