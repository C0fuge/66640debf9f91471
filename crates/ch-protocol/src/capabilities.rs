//! Capabilities (spec §14): the map a plugin returns from `initialize`, the key registry and
//! the typed parameter objects.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[path = "capabilities_gen.rs"]
mod generated;
pub use generated::*;

/// One capability value: `true`/`false`, or a parameter object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Flag {
    /// `true` = supported with defaults; `false` = not supported (same as absent).
    Bool(bool),
    /// Supported with these parameters.
    Params(serde_json::Map<String, serde_json::Value>),
}

impl Flag {
    /// Is the capability present?
    pub fn is_on(&self) -> bool {
        !matches!(self, Self::Bool(false))
    }
}

impl From<bool> for Flag {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

/// The capability map. Keys outside [`CapabilityKey`] (vendor `x-<plugin-id>.<name>` keys,
/// or keys from a newer minor version) are kept and ignored.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Capabilities(pub BTreeMap<String, Flag>);

impl Capabilities {
    /// An empty map (only the required methods).
    pub fn new() -> Self {
        Self::default()
    }

    /// Declare `key` with default parameters.
    pub fn enable(&mut self, key: impl AsRef<str>) -> &mut Self {
        self.0.insert(key.as_ref().to_owned(), Flag::Bool(true));
        self
    }

    /// Declare `key` with a parameter object.
    pub fn enable_with<T: Serialize>(&mut self, key: impl AsRef<str>, params: &T) -> &mut Self {
        let v = serde_json::to_value(params).expect("capability params serialise");
        let flag = match v {
            serde_json::Value::Object(m) => Flag::Params(m),
            serde_json::Value::Bool(b) => Flag::Bool(b),
            _ => Flag::Bool(true),
        };
        self.0.insert(key.as_ref().to_owned(), flag);
        self
    }

    /// Is `key` declared (`true` or an object)?
    pub fn has(&self, key: impl AsRef<str>) -> bool {
        self.0.get(key.as_ref()).is_some_and(Flag::is_on)
    }

    /// The API 1.2 send-file source-lifetime promise, independently of upload support.
    /// This is never sufficient cleanup permission without that attempt's validated success
    /// and the core's own source/reference/lease checks. Unknown/malformed values grant none.
    pub fn file_source_released_on_success(&self) -> bool {
        match self.get("send.file") {
            Some(Flag::Params(params)) => {
                params.get("source_release").and_then(serde_json::Value::as_str)
                    == Some("on_success")
            }
            _ => false,
        }
    }

    /// The raw flag.
    pub fn get(&self, key: impl AsRef<str>) -> Option<&Flag> {
        self.0.get(key.as_ref())
    }

    /// The typed parameters of `key`. `None` when the key is absent or `false`; `true`
    /// yields `T::default()` (every parameter has a default, §14.1).
    pub fn params<T: DeserializeOwned + Default>(
        &self,
        key: impl AsRef<str>,
    ) -> Option<Result<T, serde_json::Error>> {
        match self.0.get(key.as_ref())? {
            Flag::Bool(false) => None,
            Flag::Bool(true) => Some(Ok(T::default())),
            Flag::Params(m) => Some(serde_json::from_value(serde_json::Value::Object(m.clone()))),
        }
    }

    /// Keys this version does not know and that are not vendor keys.
    pub fn unknown_keys(&self) -> impl Iterator<Item = &str> {
        self.0
            .keys()
            .map(String::as_str)
            .filter(|k| !k.starts_with("x-") && !CapabilityKey::from(*k).is_known())
    }
}

impl<K: Into<String>> FromIterator<(K, Flag)> for Capabilities {
    fn from_iter<I: IntoIterator<Item = (K, Flag)>>(iter: I) -> Self {
        Self(iter.into_iter().map(|(k, v)| (k.into(), v)).collect())
    }
}
