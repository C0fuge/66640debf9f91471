//! `settings_schema` defaults (spec §6.1): the core fills in the `default` of every top-level
//! property the user has not set, so `account.start.settings` arrives complete.

use serde_json::{Map, Value};

/// Insert `properties.<k>.default` for every top-level property missing from `settings`.
/// Properties without a `default` are left absent. Non-object schemas are ignored.
pub fn fill_defaults(schema: Option<&Map<String, Value>>, settings: &mut Map<String, Value>) {
    let Some(props) = schema
        .and_then(|s| s.get("properties"))
        .and_then(Value::as_object)
    else {
        return;
    };
    for (key, prop) in props {
        if settings.contains_key(key) {
            continue;
        }
        if let Some(default) = prop.get("default") {
            settings.insert(key.clone(), default.clone());
        }
    }
}

/// The keys of top-level properties flagged `x-secret: true` (§6.2). The settings service
/// stores their values in the keyring and merges them back before `account.start`.
pub fn secret_keys(schema: Option<&Map<String, Value>>) -> Vec<String> {
    schema
        .and_then(|s| s.get("properties"))
        .and_then(Value::as_object)
        .map(|props| {
            props
                .iter()
                .filter(|(_, p)| p.get("x-secret").and_then(Value::as_bool) == Some(true))
                .map(|(k, _)| k.clone())
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn fills_only_missing_defaults() {
        let schema = json!({"type": "object", "properties": {
            "proxy": {"type": "string", "default": ""},
            "seed": {"type": "integer", "default": 1},
            "token": {"type": "string", "x-secret": true}
        }});
        let mut settings = json!({"seed": 7}).as_object().unwrap().clone();
        fill_defaults(schema.as_object(), &mut settings);
        assert_eq!(Value::Object(settings), json!({"seed": 7, "proxy": ""}));
        assert_eq!(secret_keys(schema.as_object()), vec!["token".to_owned()]);
    }

    #[test]
    fn no_schema_is_a_noop() {
        let mut settings = Map::new();
        fill_defaults(None, &mut settings);
        assert!(settings.is_empty());
    }
}
