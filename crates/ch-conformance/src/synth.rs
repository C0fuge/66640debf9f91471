//! Synthesises schema-valid request params so the suite can call every method a plugin
//! declares without knowing anything about the platform.
//!
//! Values are deliberately bogus (`ch-conformance-missing` ids) so that probes never act on
//! real data: a conformant plugin answers `NOT_FOUND`, `INVALID_PARAMS`, `AUTH_REQUIRED`, …,
//! and any of those proves that the method is wired up. Only a handful of read-only methods
//! are given a real chat id (see `checks::methods`).

use serde_json::{Value, json};

use crate::schemas::Schemas;

/// The id used wherever a probe needs an id that must not exist.
pub const MISSING_ID: &str = "ch-conformance-missing";

/// Per-call hints: the account to address and, for read-only probes, a real chat.
#[derive(Debug, Clone, Default)]
pub struct Hints {
    /// Value for `account_id`.
    pub account_id: String,
    /// Value for `chat_id` (bogus by default).
    pub chat_id: Option<String>,
    /// An absolute path to an existing file, for `path` params.
    pub file_path: Option<String>,
}

/// A schema plus the file it lives in (for relative `$ref`s).
#[derive(Clone, Copy)]
pub struct Located<'a> {
    pub file: &'a str,
    pub schema: &'a Value,
}

/// Constraints that apply together to one candidate, each in its defining document.
#[derive(Clone)]
struct Shape<'a>(Vec<Located<'a>>);

impl<'a> Shape<'a> {
    fn keyword(&self, key: &str) -> Option<Located<'a>> {
        self.0.iter().find_map(|loc| {
            Some(Located {
                file: loc.file,
                schema: loc.schema.get(key)?,
            })
        })
    }

    fn property(&self, name: &str) -> Option<Located<'a>> {
        self.0.iter().find_map(|loc| {
            Some(Located {
                file: loc.file,
                schema: loc.schema.get("properties")?.get(name)?,
            })
        })
    }

    fn ty(&self) -> &str {
        self.0
            .iter()
            .find_map(|loc| primary_type(loc.schema))
            .unwrap_or_else(|| {
                if self.keyword("properties").is_some() || self.keyword("required").is_some() {
                    "object"
                } else {
                    "string"
                }
            })
    }
}

/// Keep sibling constraints alongside references and allOf members. A union yields
/// separate candidates so a gated path can select the branch that declares it.
fn shapes<'a>(s: &'a Schemas, loc: Located<'a>, depth: u32) -> Vec<Shape<'a>> {
    if depth > 16 {
        return vec![];
    }
    let mut candidates = vec![Shape(vec![loc])];
    let referenced = loc.schema["$ref"]
        .as_str()
        .and_then(|r| s.resolve(loc.file, r))
        .map(|(file, schema)| Located { file, schema });
    let members = loc.schema["allOf"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|schema| Located {
            file: loc.file,
            schema,
        });
    for member in referenced.into_iter().chain(members) {
        candidates = combine(&candidates, shapes(s, member, depth + 1));
    }
    for union in ["oneOf", "anyOf"] {
        if let Some(branches) = loc.schema[union].as_array() {
            let mut choices = branches
                .iter()
                .flat_map(|schema| {
                    shapes(
                        s,
                        Located {
                            file: loc.file,
                            schema,
                        },
                        depth + 1,
                    )
                })
                .collect::<Vec<_>>();
            choices.sort_by_key(|shape| shape.ty() == "null");
            candidates = combine(&candidates, choices);
        }
    }
    candidates
}

fn combine<'a>(bases: &[Shape<'a>], choices: Vec<Shape<'a>>) -> Vec<Shape<'a>> {
    let mut result = Vec::new();
    for base in bases {
        for choice in &choices {
            let mut candidate = base.clone();
            candidate.0.extend_from_slice(&choice.0);
            result.push(candidate);
        }
    }
    result
}

/// The JSON type a schema asks for (first non-null entry of `type`).
fn primary_type(schema: &Value) -> Option<&str> {
    match schema.get("type") {
        Some(Value::String(t)) => Some(t),
        Some(Value::Array(ts)) => ts
            .iter()
            .filter_map(Value::as_str)
            .find(|t| *t != "null")
            .or_else(|| ts.first().and_then(Value::as_str)),
        _ => None,
    }
}

#[derive(Clone, Copy)]
struct Gate<'a> {
    path: &'a [&'a str],
    value: Option<&'a Value>,
}

impl<'a> Gate<'a> {
    fn next(self) -> Option<(&'a str, Self)> {
        let (&key, path) = self.path.split_first()?;
        Some((key, Self { path, ..self }))
    }
}

/// Generate a required value, or walk a gated path while preserving supplied values.
fn generate(
    s: &Schemas,
    loc: Located<'_>,
    name: &str,
    hints: &Hints,
    gate: Option<Gate<'_>>,
    existing: Option<&Value>,
    depth: u32,
) -> Option<Value> {
    if depth > 12 {
        return None;
    }
    let candidates = shapes(s, loc, 0);
    let present = gate.is_some_and(|g| g.path.is_empty());
    if present && let Some(value) = gate.and_then(|g| g.value) {
        return Some(value.clone());
    }
    let step = gate.and_then(Gate::next);
    for shape in &candidates {
        match shape.ty() {
            "object" => {
                let update = if let Some((key, rest)) = step {
                    let Some(prop) = shape.property(key) else {
                        continue;
                    };
                    let child = generate(
                        s,
                        prop,
                        key,
                        hints,
                        Some(rest),
                        existing.and_then(|obj| obj.get(key)),
                        depth + 1,
                    );
                    let Some(child) = child else { continue };
                    Some((key, child))
                } else {
                    None
                };
                let mut obj = existing
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                // Switching tagged alternatives must also remove the previous variant's keys.
                obj.retain(|key, _| {
                    shape.property(key).is_some()
                        || !candidates
                            .iter()
                            .any(|candidate| candidate.property(key).is_some())
                });
                if let Some((key, child)) = update {
                    obj.insert(key.to_owned(), child);
                }
                for required in shape
                    .0
                    .iter()
                    .filter_map(|loc| loc.schema["required"].as_array())
                    .flatten()
                {
                    let key = required.as_str()?;
                    if !obj.contains_key(key) {
                        let prop = shape.property(key).unwrap_or(Located {
                            file: loc.file,
                            schema: &Value::Null,
                        });
                        obj.insert(
                            key.to_owned(),
                            generate(s, prop, key, hints, None, None, depth + 1)?,
                        );
                    }
                }
                return Some(Value::Object(obj));
            }
            "array" => {
                let mut items = existing
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let min = shape
                    .keyword("minItems")
                    .and_then(|k| k.schema.as_u64())
                    .unwrap_or(0);
                let item = shape.keyword("items").unwrap_or(Located {
                    file: loc.file,
                    schema: &Value::Null,
                });
                if let Some((segment, rest)) = step {
                    if segment != "[]" {
                        continue;
                    }
                    let Some(child) =
                        generate(s, item, name, hints, Some(rest), items.first(), depth + 1)
                    else {
                        continue;
                    };
                    if items.is_empty() {
                        items.push(child);
                    } else {
                        items[0] = child;
                    }
                }
                while (items.len() as u64) < min.max(u64::from(present)) {
                    items.push(generate(s, item, name, hints, None, None, depth + 1)?);
                }
                return Some(Value::Array(items));
            }
            _ if step.is_none() => return Some(scalar(shape, name, hints, present)),
            _ => {}
        }
    }
    None
}

fn scalar(shape: &Shape<'_>, name: &str, hints: &Hints, prefer_present: bool) -> Value {
    if let Some(c) = shape.keyword("const") {
        return c.schema.clone();
    }
    if let Some(first) = shape
        .keyword("enum")
        .and_then(|e| e.schema.as_array()?.first())
    {
        return first.clone();
    }
    let keyword = |key| shape.keyword(key).map(|k| k.schema);
    match shape.ty() {
        "boolean" => Value::Bool(prefer_present),
        "integer" => {
            let min = keyword("minimum").and_then(Value::as_i64);
            let max = keyword("maximum").and_then(Value::as_i64);
            let v = match (min, max) {
                (Some(lo), _) if prefer_present && lo < 1 => 1.min(max.unwrap_or(1)),
                (Some(lo), _) => lo,
                (None, Some(hi)) => hi.min(1),
                (None, None) => {
                    if prefer_present {
                        1
                    } else {
                        0
                    }
                }
            };
            json!(v)
        }
        "number" => json!(keyword("minimum").and_then(Value::as_f64).unwrap_or(0.0)),
        "null" => Value::Null,
        _ => Value::String(string_for(shape, name, hints)),
    }
}

fn string_for(shape: &Shape<'_>, name: &str, hints: &Hints) -> String {
    let pattern = shape
        .keyword("pattern")
        .and_then(|k| k.schema.as_str())
        .unwrap_or("");
    let format = shape
        .keyword("format")
        .and_then(|k| k.schema.as_str())
        .unwrap_or("");
    if name == "account_id" {
        return hints.account_id.clone();
    }
    if name == "chat_id" {
        return hints
            .chat_id
            .clone()
            .unwrap_or_else(|| MISSING_ID.to_owned());
    }
    if format == "date-time" || pattern.contains("\\d{4}-\\d{2}-\\d{2}T") {
        return "2026-01-01T00:00:00.000Z".to_owned();
    }
    if pattern.starts_with("^#[0-9A-Fa-f]{6}$") {
        return "#000000".to_owned();
    }
    if pattern.starts_with("^(0|[1-9]") {
        return "1.0.0".to_owned();
    }
    if name == "path" || name.ends_with("_path") {
        return hints
            .file_path
            .clone()
            .unwrap_or_else(|| "/nonexistent/ch-conformance-missing".to_owned());
    }
    if name == "id" || name.ends_with("_id") || name.ends_with("_ids") {
        return MISSING_ID.to_owned();
    }
    match name {
        "text" | "title" | "query" | "about" | "value" | "name" | "question" | "emoji"
        | "username" => "conformance".to_owned(),
        "phone" => "+15550100".to_owned(),
        "method" => "none".to_owned(),
        "locale" | "to_language" => "en".to_owned(),
        "cursor" => "0".to_owned(),
        _ => "x".to_owned(),
    }
}

/// Params for `method`: every required field of `<schema>#/$defs/<method>.params`.
pub fn params_for(s: &Schemas, method: &str, schema_file: &str, hints: &Hints) -> Option<Value> {
    let file = format!("methods/{schema_file}");
    let (file, schema) = s.resolve(&file, &format!("#/$defs/{method}.params"))?;
    generate(s, Located { file, schema }, "", hints, None, None, 0)
}

/// Set a gated parameter: walk `path` (`reply_to.quote_text`, `reactions[].custom_emoji_id`)
/// into `params`, creating containers (objects get their required fields, arrays one
/// element) and placing `value` (or a synthesised present value) at the leaf.
pub fn set_gated(
    s: &Schemas,
    method: &str,
    schema_file: &str,
    params: &mut Value,
    path: &str,
    value: Option<Value>,
    hints: &Hints,
) -> bool {
    let file = format!("methods/{schema_file}");
    let Some((file, schema)) = s.resolve(&file, &format!("#/$defs/{method}.params")) else {
        return false;
    };
    let loc = Located { file, schema };
    let mut segments = Vec::new();
    for part in path.split('.') {
        if let Some(key) = part.strip_suffix("[]") {
            segments.extend([key, "[]"]);
        } else {
            segments.push(part);
        }
    }
    let Some(updated) = generate(
        s,
        loc,
        "",
        hints,
        Some(Gate {
            path: &segments,
            value: value.as_ref(),
        }),
        Some(params),
        0,
    ) else {
        return false;
    };
    *params = updated;
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hints() -> Hints {
        Hints {
            account_id: "acc".into(),
            chat_id: None,
            file_path: None,
        }
    }

    #[test]
    fn synthesises_required_params() {
        let s = Schemas::embedded();
        let p = params_for(s, "messages.send", "messages.json", &hints()).unwrap();
        assert_eq!(p["account_id"], "acc");
        assert_eq!(p["chat_id"], MISSING_ID);
        assert_eq!(p["text"], "conformance");
        assert!(p.get("silent").is_none());
        assert!(
            s.validate("methods/messages.json#/$defs/messages.send.params", &p)
                .is_empty()
        );
        let p = params_for(s, "messages.fetch_recent", "messages.json", &hints()).unwrap();
        assert_eq!(p["limit"], 1);
        assert!(
            s.validate(
                "methods/messages.json#/$defs/messages.fetch_recent.params",
                &p
            )
            .is_empty()
        );
    }

    #[test]
    fn every_registry_method_synthesises_valid_params() {
        let s = Schemas::embedded();
        for m in s.registry()["methods"].as_array().unwrap() {
            let name = m["name"].as_str().unwrap();
            let file = m["schema"].as_str().unwrap();
            let p = params_for(s, name, file, &hints()).unwrap_or_else(|| panic!("{name}"));
            let errs = s.validate(&format!("methods/{file}#/$defs/{name}.params"), &p);
            assert!(errs.is_empty(), "{name}: {errs:?} for {p}");
        }
    }

    #[test]
    fn gated_edit_preserves_existing_values_and_replaces_exclusive_branch() {
        let s = Schemas::embedded();
        let mut p = json!({
            "account_id": "acc", "chat_id": MISSING_ID, "message_id": MISSING_ID,
            "reactions": [{"emoji": "test"}, {"paid": true}], "big": false
        });
        assert!(set_gated(
            s,
            "reactions.set",
            "reactions.json",
            &mut p,
            "reactions[].custom_emoji_id",
            None,
            &hints()
        ));
        assert_eq!(
            p["reactions"],
            json!([{"custom_emoji_id": MISSING_ID}, {"paid": true}])
        );
        assert_eq!(p["big"], false);
        assert!(
            s.validate("methods/reactions.json#/$defs/reactions.set.params", &p)
                .is_empty()
        );
    }

    #[test]
    fn gated_edit_follows_inherited_properties_and_their_relative_references() {
        let s = Schemas::embedded();
        let mut p = params_for(s, "stickers.send", "stickers.json", &hints()).unwrap();
        assert!(set_gated(
            s,
            "stickers.send",
            "stickers.json",
            &mut p,
            "reply_to.quote_text",
            None,
            &hints()
        ));
        assert_eq!(p["account_id"], "acc");
        assert_eq!(p["chat_id"], MISSING_ID);
        assert_eq!(p["sticker_id"], MISSING_ID);
        assert_eq!(p["reply_to"]["message_id"], MISSING_ID);
        assert!(p["reply_to"]["quote_text"].is_string());
        assert!(
            s.validate("methods/stickers.json#/$defs/stickers.send.params", &p)
                .is_empty()
        );
    }

    #[test]
    fn sets_gated_paths() {
        let s = Schemas::embedded();
        for m in s.registry()["methods"].as_array().unwrap() {
            let name = m["name"].as_str().unwrap();
            let file = m["schema"].as_str().unwrap();
            for g in m
                .get("gated_params")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let mut p = params_for(s, name, file, &hints()).unwrap();
                let path = g["param"].as_str().unwrap();
                let ok = set_gated(
                    s,
                    name,
                    file,
                    &mut p,
                    path,
                    g.get("equals").cloned(),
                    &hints(),
                );
                assert!(ok, "{name}.{path} could not be set");
                let errs = s.validate(&format!("methods/{file}#/$defs/{name}.params"), &p);
                assert!(errs.is_empty(), "{name}.{path}: {errs:?} for {p}");
                let info = ch_protocol::method(name).unwrap();
                let missing: Vec<_> = info.missing_capabilities(&p, |_| false).collect();
                assert!(
                    missing.iter().any(|g| g.param == path),
                    "{name}.{path} not seen as present in {p}"
                );
            }
        }
    }
}
