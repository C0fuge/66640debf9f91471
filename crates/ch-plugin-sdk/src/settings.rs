//! Typed access to an account's `settings` (spec §6).
//!
//! The core validates user input against the manifest's `settings_schema`, fills in the
//! top-level `default`s, merges the `x-secret` values back in, and hands one flat object to
//! `account.start` (§6.1). The SDK fills the same defaults from `plugin_dir/plugin.json`, so a
//! plugin sees the same settings with or without a core. These helpers read it; every failure is an `INVALID_PARAMS` error naming the field, which is what
//! `account.start` should return for unusable settings (§11.1: the message is shown to the
//! user).

use std::path::Path;

use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use crate::error::{PluginError, invalid_params};

/// An account's settings object, as received in `account.start`.
///
/// Absent and `null` are the same thing (§8.4). Keys the user never set carry the
/// `settings_schema` `default` (§6.1), so [`Settings::get`] returns it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Settings(Map<String, Value>);

impl Settings {
    /// Wrap a raw settings object.
    pub fn new(raw: Map<String, Value>) -> Self {
        Self(raw)
    }

    /// The raw object.
    pub fn raw(&self) -> &Map<String, Value> {
        &self.0
    }

    /// `key` as `T`; `Ok(None)` when absent or `null`; `INVALID_PARAMS` when it has the wrong
    /// type.
    pub fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, PluginError> {
        match self.0.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => serde_json::from_value(v.clone())
                .map(Some)
                .map_err(|e| invalid_params(key, format!("setting {key:?}: {e}"))),
        }
    }

    /// `key` as `T`, or `default` when absent or `null`.
    pub fn get_or<T: DeserializeOwned>(&self, key: &str, default: T) -> Result<T, PluginError> {
        Ok(self.get(key)?.unwrap_or(default))
    }

    /// `key` as `T`; `INVALID_PARAMS` when absent, `null` or an empty string (§8.4: required
    /// strings are never empty).
    pub fn require<T: DeserializeOwned>(&self, key: &str) -> Result<T, PluginError> {
        if matches!(self.0.get(key), Some(Value::String(s)) if s.is_empty()) {
            return Err(invalid_params(key, format!("setting {key:?} is required")));
        }
        self.get(key)?
            .ok_or_else(|| invalid_params(key, format!("setting {key:?} is required")))
    }

    /// `key` as a string slice, if it is a string.
    pub fn str(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(Value::as_str)
    }

    /// The whole object as a typed struct (use `#[serde(default)]` for optional settings).
    pub fn parse<T: DeserializeOwned>(&self) -> Result<T, PluginError> {
        serde_json::from_value(Value::Object(self.0.clone()))
            .map_err(|e| invalid_params("settings", format!("settings: {e}")))
    }
}

/// The top-level `settings_schema.properties[*].default`s of the manifest in `plugin_dir`
/// (§6.1). A missing or unreadable manifest yields none (logged).
pub(crate) fn manifest_defaults(plugin_dir: &Path) -> Map<String, Value> {
    let path = plugin_dir.join("plugin.json");
    let manifest: Value = match std::fs::read(&path) {
        Ok(bytes) => match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("{}: {e}; no settings defaults", path.display());
                return Map::new();
            }
        },
        Err(e) => {
            tracing::debug!("{}: {e}; no settings defaults", path.display());
            return Map::new();
        }
    };
    manifest
        .pointer("/settings_schema/properties")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .filter_map(|(k, prop)| Some((k.clone(), prop.get("default")?.clone())))
        .collect()
}

/// `settings` with every key that is absent or `null` filled from `defaults`.
pub(crate) fn with_defaults(
    mut settings: Map<String, Value>,
    defaults: &Map<String, Value>,
) -> Map<String, Value> {
    for (k, v) in defaults {
        match settings.get(k) {
            None | Some(Value::Null) => {
                settings.insert(k.clone(), v.clone());
            }
            Some(_) => {}
        }
    }
    settings
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn manifest_defaults_fill_absent_and_null_settings() {
        let dir = std::env::temp_dir().join(format!("ch-sdk-settings-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let manifest = json!({"settings_schema": {"type": "object", "properties": {
            "proxy": {"type": "string", "default": ""},
            "port": {"type": "integer", "default": 8080},
            "name": {"type": "string"}}}});
        std::fs::write(dir.join("plugin.json"), manifest.to_string()).unwrap();
        let defaults = manifest_defaults(&dir);
        std::fs::remove_dir_all(&dir).unwrap();
        let given = json!({"port": 1, "name": null});
        let s = Settings::new(with_defaults(given.as_object().unwrap().clone(), &defaults));
        assert_eq!(s.get::<String>("proxy").unwrap(), Some(String::new()));
        assert_eq!(s.get::<u16>("port").unwrap(), Some(1));
        assert_eq!(s.get::<String>("name").unwrap(), None);
        assert!(manifest_defaults(Path::new("/nonexistent")).is_empty());
    }
}
