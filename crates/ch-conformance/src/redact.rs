//! Keep sensitive fixture values private until every reader has finished.
use crate::Options;
use serde_json::Value;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

const REDACTED: &str = "[redacted]";

#[derive(Clone, Default)]
pub(crate) struct Redactions(Arc<Mutex<Inventory>>);
#[derive(Default)]
struct Inventory {
    values: BTreeSet<String>,
    bytes: usize,
    suppress: bool,
}

impl Redactions {
    pub fn new(options: &Options) -> Self {
        let this = Self::default();
        for value in options.secrets.values() {
            this.value(value);
        }
        for answer in options.auth_answers.values() {
            this.add(answer);
        }
        this
    }

    pub fn settings(&self, manifest: &Value, options: &Options) {
        if let Some(props) = manifest
            .pointer("/settings_schema/properties")
            .and_then(Value::as_object)
        {
            for (key, schema) in props {
                if schema["x-secret"] == true {
                    if let Some(v) = schema.get("default") {
                        self.value(v);
                    }
                    if let Some(v) = options.settings.get(key) {
                        self.value(v);
                    }
                }
            }
        }
    }

    fn value(&self, v: &Value) {
        if let Some(s) = v.as_str() {
            self.add(s);
        } else {
            self.add(&v.to_string());
        }
    }

    pub fn add(&self, text: &str) {
        if text.is_empty() {
            return;
        }
        let mut inv = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if inv.suppress || inv.values.contains(text) {
            return;
        }
        if text.len() > 65536
            || inv.bytes.saturating_add(text.len() * 7) > 262144
            || inv.values.len() >= 512
        {
            inv.suppress = true;
            inv.values.clear();
            return;
        }
        let quoted = serde_json::to_string(text).expect("string");
        let escaped = &quoted[1..quoted.len() - 1];
        for s in [text, escaped] {
            if inv.values.insert(s.to_owned()) {
                inv.bytes += s.len();
            }
        }
    }

    /// A truncated log or schema diagnostic may contain only a prefix of a secret.
    /// With sensitive input, withhold truncated diagnostics as a unit.
    pub fn clean(&self, source: &str) -> String {
        let inv = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if inv.suppress || (!inv.values.is_empty() && source.contains('…')) {
            return REDACTED.into();
        }
        let mut values: Vec<_> = inv.values.iter().collect();
        values.sort_unstable_by_key(|v| std::cmp::Reverse(v.len()));
        let mut text = source.to_owned();
        for value in values {
            text = text.replace(value, REDACTED);
        }
        text
    }

    pub fn clean_log(&self, source: &str) -> String {
        if source.is_empty() {
            return String::new();
        }
        let inv = self.0.lock().unwrap_or_else(|e| e.into_inner());
        // The stderr ring may start in the middle of a multiline secret.
        if inv.suppress || !inv.values.is_empty() {
            REDACTED.into()
        } else {
            source.into()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn protects_configured_rotated_and_escaped_secrets() {
        let mut opts = Options::default();
        opts.secrets.insert("session".into(), json!("seed-token"));
        opts.settings
            .insert("password".into(), json!("fixture-password"));
        opts.auth_answers.insert("code".into(), "test-code".into());
        let r = Redactions::new(&opts);
        r.settings(&json!({"settings_schema":{"properties":{"password":{"x-secret":true,"default":"default-password"}}}}), &opts);
        r.add("rotated\nvalue");
        assert_eq!(
            r.clean("seed-token fixture-password default-password test-code rotated\\nvalue"),
            "[redacted] [redacted] [redacted] [redacted] [redacted]"
        );
        assert_eq!(r.clean_log("ken"), REDACTED);
        assert_eq!(r.clean("first few letters…"), REDACTED);
        r.add(&"z".repeat(65537));
        assert_eq!(r.clean("remaining diagnostic"), REDACTED);
    }
}
