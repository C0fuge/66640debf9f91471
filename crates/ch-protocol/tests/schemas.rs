//! Keeps `schemas/` and the Rust types in step:
//! - every example in `schemas/examples/` validates against its schema and round-trips
//!   through the typed structs byte-for-byte (modulo `null` ≡ absent);
//! - the registry, capability keys, event names and error codes exported by the crate equal
//!   the machine-readable lists in `schemas/`.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use ch_protocol::methods::*;
use ch_protocol::*;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

const BASE: &str = "https://contacthub.dev/schemas/v1/";

fn schemas_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../schemas")
}

fn read_json(p: &Path) -> Value {
    serde_json::from_str(&fs::read_to_string(p).unwrap_or_else(|e| panic!("{}: {e}", p.display())))
        .unwrap_or_else(|e| panic!("{}: {e}", p.display()))
}

/// Every schema file, keyed by `$id`.
struct Schemas(BTreeMap<String, Value>);

impl Schemas {
    fn load() -> Self {
        let mut map = BTreeMap::new();
        fn walk(dir: &Path, map: &mut BTreeMap<String, Value>) {
            for e in fs::read_dir(dir).unwrap() {
                let p = e.unwrap().path();
                if p.is_dir() {
                    if p.file_name().unwrap() != "examples" {
                        walk(&p, map);
                    }
                } else if p.extension().is_some_and(|x| x == "json") {
                    let v = read_json(&p);
                    let id = v["$id"]
                        .as_str()
                        .unwrap_or_else(|| panic!("{} has no $id", p.display()))
                        .to_owned();
                    assert!(id.starts_with(BASE), "{id} is not under {BASE}");
                    map.insert(id, v);
                }
            }
        }
        walk(&schemas_dir(), &mut map);
        Self(map)
    }

    fn validator(&self, reference: &str) -> jsonschema::Validator {
        let root = json!({"$schema": "https://json-schema.org/draft/2020-12/schema", "$ref": format!("{BASE}{reference}")});
        jsonschema::options()
            .with_retriever(Retriever(self.0.clone()))
            .build(&root)
            .unwrap_or_else(|e| panic!("building validator for {reference}: {e}"))
    }

    fn check(&self, reference: &str, instance: &Value, what: &str) {
        let v = self.validator(reference);
        let errors: Vec<String> = v
            .iter_errors(instance)
            .map(|e| format!("{} at {}", e, e.instance_path()))
            .collect();
        assert!(
            errors.is_empty(),
            "{what} does not match {reference}:\n  {}",
            errors.join("\n  ")
        );
    }

    fn get(&self, file: &str) -> &Value {
        &self.0[&format!("{BASE}{file}")]
    }
}

struct Retriever(BTreeMap<String, Value>);

impl jsonschema::Retrieve for Retriever {
    fn retrieve(
        &self,
        uri: &jsonschema::Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let key = uri.as_str().split('#').next().unwrap();
        self.0
            .get(key)
            .cloned()
            .ok_or_else(|| format!("unknown schema {key}").into())
    }
}

/// `null` and absent are equivalent (§8.4); emitters omit, so strip nulls before comparing.
fn strip_nulls(v: &Value) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k.clone(), strip_nulls(v)))
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.iter().map(strip_nulls).collect()),
        other => other.clone(),
    }
}

/// `back` must carry every value of `original` unchanged; the only keys it may add are the
/// spec's non-nullable defaults (`false` / `0`), which an emitter may write or omit (§8.4).
fn assert_same_modulo_defaults(original: &Value, back: &Value, what: &str, path: &str) {
    match (original, back) {
        (Value::Object(o), Value::Object(b)) => {
            for (k, ov) in o {
                let bv = b
                    .get(k)
                    .unwrap_or_else(|| panic!("{what}: {path}/{k} was dropped"));
                assert_same_modulo_defaults(ov, bv, what, &format!("{path}/{k}"));
            }
            for (k, bv) in b {
                if !o.contains_key(k) {
                    assert!(
                        *bv == Value::Bool(false) || *bv == json!(0),
                        "{what}: {path}/{k} = {bv} appeared and is not a spec default"
                    );
                }
            }
        }
        (Value::Array(o), Value::Array(b)) => {
            assert_eq!(o.len(), b.len(), "{what}: {path} length");
            for (i, (ov, bv)) in o.iter().zip(b).enumerate() {
                assert_same_modulo_defaults(ov, bv, what, &format!("{path}/{i}"));
            }
        }
        _ => assert_eq!(original, back, "{what}: {path}"),
    }
}

fn roundtrip<T: DeserializeOwned + Serialize + PartialEq + std::fmt::Debug>(
    original: &Value,
    what: &str,
) -> Value {
    let typed: T = serde_json::from_value(original.clone()).unwrap_or_else(|e| {
        panic!(
            "{what}: deserialising into {}: {e}",
            std::any::type_name::<T>()
        )
    });
    let back = serde_json::to_value(&typed).unwrap();
    assert_same_modulo_defaults(&strip_nulls(original), &back, what, "");
    let again: T = serde_json::from_value(back.clone()).unwrap();
    assert_eq!(typed, again, "{what}: second pass differs");
    back
}

/// Round-trips a frame whose params are an event: Notification/Request<Value> + Event.
fn roundtrip_event_frame(original: &Value, what: &str) -> Value {
    let env: Envelope = serde_json::from_value(original.clone()).unwrap();
    let (method, params) = match &env {
        Envelope::Notification(n) => (n.method.clone(), n.params.clone().unwrap()),
        Envelope::Request(r) => (r.method.clone(), r.params.clone().unwrap()),
        other => panic!("{what}: not an event frame: {other:?}"),
    };
    let event = Event::from_params(&method, params)
        .unwrap_or_else(|| panic!("{what}: unknown event {method}"))
        .unwrap();
    assert_eq!(event.name(), method);
    let mut back = original.clone();
    back["params"] = event.to_params();
    assert_same_modulo_defaults(&strip_nulls(original), &back, what, "");
    back
}

#[test]
fn examples_validate_and_round_trip() {
    let schemas = Schemas::load();
    let dir = schemas_dir().join("examples");
    let index = read_json(&dir.join("index.json"));
    let index = index.as_array().unwrap();
    assert!(index.len() >= 10);
    let mut seen = 0;
    for entry in index {
        let file = entry["file"].as_str().unwrap();
        let original = read_json(&dir.join(file));
        schemas.check(entry["schema"].as_str().unwrap(), &original, file);
        if let Some(s) = entry["params_schema"].as_str() {
            schemas.check(s, &original["params"], &format!("{file} params"));
        }
        if let Some(s) = entry["result_schema"].as_str() {
            schemas.check(s, &original["result"], &format!("{file} result"));
        }
        let back = match entry["type"].as_str().unwrap() {
            "Manifest" => {
                let m: Manifest = serde_json::from_value(original.clone()).unwrap();
                m.validate().unwrap();
                assert!(m.is_compatible());
                roundtrip::<Manifest>(&original, file)
            }
            "Request<InitializeParams>" => roundtrip::<Request<InitializeParams>>(&original, file),
            "Response<InitializeResult>" => {
                let r: Response<InitializeResult> =
                    serde_json::from_value(original.clone()).unwrap();
                let init = r.into_result().unwrap();
                assert!(init.capabilities.has("edit"));
                assert!(!init.capabilities.has("sign_out"));
                assert_eq!(
                    init.capabilities
                        .params::<capabilities::WindowParams>("edit")
                        .unwrap()
                        .unwrap()
                        .window_seconds,
                    Some(172_800)
                );
                assert_eq!(
                    init.capabilities
                        .params::<capabilities::ReactionsSetParams>("reactions.who")
                        .unwrap()
                        .unwrap()
                        .max,
                    None
                );
                assert_eq!(
                    init.capabilities.unknown_keys().count(),
                    0,
                    "vendor x- keys are not 'unknown'"
                );
                roundtrip::<Response<InitializeResult>>(&original, file)
            }
            "Request<AccountStartParams>" => {
                roundtrip::<Request<AccountStartParams>>(&original, file)
            }
            "Request<MessagesSendParams>" => {
                roundtrip::<Request<MessagesSendParams>>(&original, file)
            }
            "Response<MessageResult>" => roundtrip::<Response<MessageResult>>(&original, file),
            "Message" => roundtrip::<dto::Message>(&original, file),
            "Conversation" => roundtrip::<dto::Conversation>(&original, file),
            "Notification<Event>" | "Request<Event>" => roundtrip_event_frame(&original, file),
            "Response<Empty>" => {
                let r: Response<Empty> = serde_json::from_value(original.clone()).unwrap();
                let err = r.into_result().unwrap_err();
                assert_eq!(
                    err.code().numeric(),
                    err.code,
                    "{file}: numeric code mirrors data.code"
                );
                roundtrip::<Response<Empty>>(&original, file)
            }
            "Notification<CancelParams>" => {
                roundtrip::<Notification<CancelParams>>(&original, file)
            }
            other => panic!("{file}: no round-trip arm for type {other}"),
        };
        // What we emit must be valid too.
        schemas.check(
            entry["schema"].as_str().unwrap(),
            &back,
            &format!("{file} (re-serialised)"),
        );
        // And every frame parses as an Envelope.
        if original.get("jsonrpc").is_some() {
            let env: Envelope = serde_json::from_value(original.clone()).unwrap();
            let expect = entry["schema"]
                .as_str()
                .unwrap()
                .rsplit('/')
                .next()
                .unwrap();
            let got = match env {
                Envelope::Request(_) => "Request",
                Envelope::Notification(_) => "Notification",
                Envelope::Success(_) => "SuccessResponse",
                Envelope::Error(_) => "ErrorResponse",
            };
            assert_eq!(got, expect, "{file}: envelope variant");
        }
        seen += 1;
    }
    // Every example file is indexed.
    let files = fs::read_dir(&dir)
        .unwrap()
        .filter(|e| e.as_ref().unwrap().file_name() != "index.json")
        .count();
    assert_eq!(
        files, seen,
        "every file in schemas/examples/ must be listed in index.json"
    );
}

#[test]
fn registry_matches_schema() {
    let schemas = Schemas::load();
    let reg = schemas.get("methods/registry.json");
    assert_eq!(reg["api_version"], API_VERSION);
    assert_eq!(reg["cancel"], CANCEL_METHOD);
    assert_eq!(
        reg["default_timeout_seconds"].as_u64().unwrap() as u32,
        DEFAULT_TIMEOUT_SECONDS
    );
    let rows = reg["methods"].as_array().unwrap();
    assert_eq!(rows.len(), METHODS.len());
    for (row, m) in rows.iter().zip(METHODS) {
        assert_eq!(row["name"], m.name);
        assert_eq!(row["schema"], m.schema);
        assert_eq!(
            serde_json::to_value(m.direction).unwrap(),
            row["direction"],
            "{}",
            m.name
        );
        let cap = row["capability"].as_str().unwrap();
        assert_eq!(
            m.capability,
            (cap != "required").then_some(cap),
            "{}",
            m.name
        );
        assert_eq!(
            m.timeout_seconds,
            row.get("timeout_seconds")
                .and_then(Value::as_u64)
                .map_or(DEFAULT_TIMEOUT_SECONDS, |t| t as u32),
            "{}",
            m.name
        );
        assert_eq!(
            m.progress,
            row.get("progress")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            "{}",
            m.name
        );
        // Gated parameters: same rows, same order, every capability known, every path a
        // real parameter (the first segment is a property of `<method>.params`).
        let gated: Vec<Value> = m
            .gated_params
            .iter()
            .map(|g| {
                let mut row = json!({"param": g.param, "capability": g.capability});
                match g.equals {
                    Some(GatedValue::Str(v)) => row["equals"] = json!(v),
                    Some(GatedValue::Bool(v)) => row["equals"] = json!(v),
                    None => {}
                }
                assert!(
                    CapabilityKey::from(g.capability).is_known(),
                    "{}: gated capability {} unknown",
                    m.name,
                    g.capability
                );
                row
            })
            .collect();
        assert_eq!(
            Value::Array(gated),
            row.get("gated_params").cloned().unwrap_or(json!([])),
            "{} gated_params",
            m.name
        );
        // params + result schemas exist for the method.
        let file = schemas.get(&format!("methods/{}", m.schema));
        for g in m.gated_params {
            let first = g.param.split('.').next().unwrap().trim_end_matches("[]");
            assert!(
                file["$defs"][format!("{}.params", m.name)]["properties"]
                    .get(first)
                    .is_some(),
                "{}: gated param {} is not a parameter",
                m.name,
                g.param
            );
        }
        assert!(
            file["$defs"].get(format!("{}.params", m.name)).is_some(),
            "{}.params missing in {}",
            m.name,
            m.schema
        );
        assert!(
            file["$defs"].get(format!("{}.result", m.name)).is_some(),
            "{}.result missing in {}",
            m.name,
            m.schema
        );
        // The gating capability is a known key.
        if let Some(c) = m.capability {
            assert!(
                CapabilityKey::from(c).is_known(),
                "{}: capability {c} is not in CapabilityKey",
                m.name
            );
        }
        assert_eq!(method(m.name), Some(m));
    }
    let required: Vec<&str> = METHODS
        .iter()
        .filter(|m| m.capability.is_none() && m.direction == CallDirection::CoreToPlugin)
        .map(|m| m.name)
        .collect();
    assert_eq!(required, REQUIRED_METHODS);
    let events: Vec<&str> = reg["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(events, EVENT_NAMES);
    let event_enum: Vec<&str> = schemas.get("events.json")["$defs"]["EventName"]["enum"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(event_enum, EVENT_NAMES);
    for e in EVENT_NAMES {
        assert!(
            schemas.get("events.json")["$defs"].get(e).is_some(),
            "events.json has no $defs/{e}"
        );
    }
}

#[test]
fn capability_keys_match_schema() {
    let schemas = Schemas::load();
    let caps = schemas.get("capabilities.json");
    let keys: Vec<&str> = caps["$defs"]["CapabilityKey"]["enum"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(keys, CapabilityKey::KNOWN);
    let parameterised: Vec<&str> = caps["$defs"]["Capabilities"]["properties"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    let ours: Vec<&str> = capabilities::PARAMETERISED
        .iter()
        .map(|(k, _)| *k)
        .collect();
    assert_eq!(parameterised, ours);
    // Every value in `true` form is valid, and every parameter struct's default is valid.
    let mut all = Capabilities::new();
    for k in CapabilityKey::KNOWN {
        all.enable(k);
    }
    schemas.check(
        "capabilities.json#/$defs/Capabilities",
        &serde_json::to_value(&all).unwrap(),
        "all-true capabilities",
    );
    let mut with_params = Capabilities::new();
    with_params.enable_with("edit", &capabilities::WindowParams::default());
    with_params.enable_with(
        "send.file",
        &capabilities::SendFileParams {
            max_bytes: Some(1),
            source_release: Some("on_success".into()),
        },
    );
    with_params.enable_with("polls.create", &capabilities::PollsCreateParams::default());
    with_params.enable_with("x-fake.thing", &json!({"a": 1}));
    schemas.check(
        "capabilities.json#/$defs/Capabilities",
        &serde_json::to_value(&with_params).unwrap(),
        "parameterised capabilities",
    );
    assert!(with_params.has("x-fake.thing"));
}

#[test]
fn error_codes_match_schema() {
    let schemas = Schemas::load();
    let errs = &schemas.get("errors.json")["$defs"];
    let codes: Vec<&str> = errs["KnownErrorCode"]["enum"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(codes, ErrorCode::KNOWN);
    let numeric: Vec<i64> = errs["KnownNumericCode"]["enum"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_i64().unwrap())
        .collect();
    let ours: Vec<i64> = ErrorCode::KNOWN
        .iter()
        .map(|c| ErrorCode::from(*c).numeric())
        .collect();
    assert_eq!(numeric, ours, "numeric codes in table order");
    for c in ErrorCode::KNOWN {
        let code = ErrorCode::from(*c);
        assert_eq!(ErrorCode::from_numeric(code.numeric()), Some(code.clone()));
        let obj = if code == ErrorCode::FloodWait {
            ErrorObject::flood_wait(1)
        } else {
            ErrorObject::new(code, "x")
        };
        schemas.check(
            "errors.json#/$defs/ErrorObject",
            &serde_json::to_value(&obj).unwrap(),
            c,
        );
    }
    schemas.check(
        "errors.json#/$defs/ErrorObject",
        &serde_json::to_value(ErrorObject::flood_wait(3)).unwrap(),
        "flood_wait",
    );
    schemas.check(
        "errors.json#/$defs/ErrorObject",
        &serde_json::to_value(ErrorObject::not_supported("send.ttl")).unwrap(),
        "not_supported",
    );
    // The enum is open (§13, §18): a code from a later minor version validates, and so does
    // what this crate emits for it (numeric INTERNAL, string kept).
    let newer = json!({"code": -32010, "message": "later", "data": {"code": "SOMETHING_NEW"}});
    schemas.check("errors.json#/$defs/ErrorObject", &newer, "unknown code validates");
    let e: ErrorObject = serde_json::from_value(newer).unwrap();
    assert_eq!(e.code(), ErrorCode::Internal);
    schemas.check(
        "errors.json#/$defs/ErrorObject",
        &serde_json::to_value(ErrorObject::new(ErrorCode::from("SOMETHING_NEW"), "x")).unwrap(),
        "unknown code emitted",
    );
    assert!(
        schemas
            .validator("errors.json#/$defs/ErrorData")
            .validate(&json!({"code": ""}))
            .is_err(),
        "an empty code is not a code"
    );
}

#[test]
fn partial_updates_add_nothing() {
    // §8.4: Conversation, Topic and Message are merge-updated; a partial update must come
    // out of the typed structs exactly as it went in — no `false`/`0` for absent fields.
    let schemas = Schemas::load();
    let cases = [
        (
            "dto/conversation.json#/$defs/Conversation",
            json!({"id": "c1", "kind": "group", "unread_count": 3}),
            json!({"id": "c1", "kind": "group", "archived": true, "pinned": false}),
        ),
        (
            "dto/topic.json#/$defs/Topic",
            json!({"id": "t1", "chat_id": "c1", "title": "General", "closed": true}),
            json!({"id": "t1", "chat_id": "c1", "title": "General", "unread_count": 0}),
        ),
        (
            "dto/message.json#/$defs/Message",
            json!({"id": "m1", "chat_id": "c1", "direction": "in", "sent_at": "2026-01-01T00:00:00.000Z", "is_pinned": true}),
            json!({"id": "m1", "chat_id": "c1", "direction": "in", "sent_at": "2026-01-01T00:00:00.000Z", "edited": true, "text": "v2"}),
        ),
    ];
    for (schema, a, b) in cases {
        for v in [a, b] {
            schemas.check(schema, &v, "partial update validates");
            let back = match schema {
                s if s.contains("conversation") => {
                    serde_json::to_value(serde_json::from_value::<dto::Conversation>(v.clone()).unwrap())
                }
                s if s.contains("topic") => {
                    serde_json::to_value(serde_json::from_value::<dto::Topic>(v.clone()).unwrap())
                }
                _ => serde_json::to_value(serde_json::from_value::<dto::Message>(v.clone()).unwrap()),
            }
            .unwrap();
            assert_eq!(back, v, "{schema}: a partial update round-trips unchanged");
        }
    }
}

#[test]
fn every_schema_builds() {
    // Each $defs entry compiles as a validator (catches broken $refs anywhere in the tree).
    let schemas = Schemas::load();
    for (id, schema) in &schemas.0 {
        let file = id.strip_prefix(BASE).unwrap();
        schemas.validator(file);
        if let Some(defs) = schema.get("$defs").and_then(Value::as_object) {
            for name in defs.keys() {
                schemas.validator(&format!("{file}#/$defs/{name}"));
            }
        }
    }
}

#[test]
fn dto_defaults_validate() {
    // A minimal Message/Conversation built in Rust is valid on the wire.
    let schemas = Schemas::load();
    let m = dto::Message {
        id: "1".into(),
        chat_id: "c".into(),
        direction: dto::Direction::Out,
        sent_at: Timestamp::parse("2026-10-19T14:03:27.512Z").unwrap(),
        text: Some("hi".into()),
        ..minimal_message()
    };
    schemas.check(
        "dto/message.json#/$defs/Message",
        &serde_json::to_value(&m).unwrap(),
        "minimal message",
    );
}

fn minimal_message() -> dto::Message {
    serde_json::from_value(
        json!({"id": "x", "chat_id": "y", "direction": "in", "sent_at": "2026-01-01T00:00:00Z"}),
    )
    .unwrap()
}

#[test]
fn envelope_method_names_include_the_v1_vendor_namespace() {
    let validator = Schemas::load().validator("jsonrpc.json#/$defs/Message");
    let valid = |name: &str| validator.is_valid(&json!({"jsonrpc":"2.0","method":name}));
    for name in [
        "x-demo_chat.export_chat",
        "x-demo.events.ready",
        "ping",
        "$/cancelRequest",
    ] {
        assert!(valid(name), "{name}");
    }
    for name in [
        "x-a.foo",
        "x-2demo.ready",
        "x-demo.",
        "x-demo.bad-name",
        "x-demo..ready",
    ] {
        assert!(!valid(name), "{name}");
    }
}

#[test]
fn source_release_schema_is_open_but_does_not_accept_malformed_known_shapes() {
    let validator = Schemas::load().validator("capabilities.json#/$defs/Capabilities");
    for value in [
        json!(true),
        json!(false),
        json!({}),
        json!({"source_release":"on_success"}),
        json!({"source_release":"future", "future_param":true}),
    ] {
        assert!(validator.is_valid(&json!({"send.file":value})), "{value}");
    }
    for malformed in [json!(null), json!(1), json!(false), json!([]), json!({})] {
        let wire = json!({"send.file":{"source_release":malformed}});
        assert!(
            !validator.is_valid(&wire),
            "known non-string promise shapes fail conformance"
        );
        let runtime: Capabilities = serde_json::from_value(wire).unwrap();
        assert!(runtime.has("send.file"));
        assert!(!runtime.file_source_released_on_success());
    }
}
