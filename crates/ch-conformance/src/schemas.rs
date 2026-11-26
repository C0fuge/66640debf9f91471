//! The JSON Schemas of the API, embedded at compile time so that `ch plugin test` works
//! outside a source checkout, plus `$ref` resolution for the parameter synthesiser.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::Value;

/// `$id` prefix of every schema file (spec §18).
pub const BASE: &str = "https://contacthub.dev/schemas/v1/";

macro_rules! embed {
    ($($file:literal),* $(,)?) => {
        /// Every schema file, by path relative to `schemas/`.
        pub const FILES: &[(&str, &str)] = &[
            $(($file, include_str!(concat!("../../../schemas/", $file))),)*
        ];
    };
}

embed!(
    "capabilities.json",
    "common.json",
    "dto/auth.json",
    "dto/contact.json",
    "dto/conversation.json",
    "dto/draft.json",
    "dto/folder.json",
    "dto/member.json",
    "dto/message.json",
    "dto/presence.json",
    "dto/privacy.json",
    "dto/profile.json",
    "dto/session.json",
    "dto/sticker.json",
    "dto/story.json",
    "dto/topic.json",
    "errors.json",
    "events.json",
    "jsonrpc.json",
    "manifest.json",
    "methods/auth.json",
    "methods/bots.json",
    "methods/chats.json",
    "methods/common.json",
    "methods/contacts.json",
    "methods/core.json",
    "methods/drafts.json",
    "methods/groups.json",
    "methods/history.json",
    "methods/lifecycle.json",
    "methods/media.json",
    "methods/messages.json",
    "methods/polls.json",
    "methods/reactions.json",
    "methods/read.json",
    "methods/registry.json",
    "methods/stickers.json",
    "methods/threads.json",
);

/// The schema set: documents keyed by `$id`, with a validator cache.
pub struct Schemas {
    docs: BTreeMap<String, Value>,
    cache: Mutex<HashMap<String, Arc<jsonschema::Validator>>>,
}

struct Retriever(BTreeMap<String, Value>);

impl jsonschema::Retrieve for Retriever {
    fn retrieve(
        &self,
        uri: &jsonschema::Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let key = uri.as_str().split('#').next().unwrap_or_default();
        self.0
            .get(key)
            .cloned()
            .ok_or_else(|| format!("unknown schema {key}").into())
    }
}

impl Schemas {
    /// The embedded schema set (built once).
    pub fn embedded() -> &'static Schemas {
        static S: OnceLock<Schemas> = OnceLock::new();
        S.get_or_init(|| {
            let docs = FILES
                .iter()
                .map(|(file, text)| {
                    let v: Value = serde_json::from_str(text)
                        .unwrap_or_else(|e| panic!("embedded schema {file}: {e}"));
                    (format!("{BASE}{file}"), v)
                })
                .collect();
            Schemas {
                docs,
                cache: Mutex::new(HashMap::new()),
            }
        })
    }

    /// The parsed document for a path relative to `schemas/` (`methods/registry.json`).
    pub fn doc(&self, file: &str) -> &Value {
        &self.docs[&format!("{BASE}{file}")]
    }

    /// The method registry (`schemas/methods/registry.json`).
    pub fn registry(&self) -> &Value {
        self.doc("methods/registry.json")
    }

    fn validator(&self, reference: &str) -> Arc<jsonschema::Validator> {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(v) = cache.get(reference) {
            return v.clone();
        }
        let root = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$ref": format!("{BASE}{reference}")
        });
        let v = jsonschema::options()
            .with_retriever(Retriever(self.docs.clone()))
            .build(&root)
            .unwrap_or_else(|e| panic!("building validator for {reference}: {e}"));
        let v = Arc::new(v);
        cache.insert(reference.to_owned(), v.clone());
        v
    }

    /// Validate `instance` against `reference` (`events.json#/$defs/status`). Returns the
    /// problems, empty when it validates.
    pub fn validate(&self, reference: &str, instance: &Value) -> Vec<String> {
        let v = self.validator(reference);
        v.iter_errors(instance)
            .take(8)
            .map(|e| format!("value at {} violates {reference}", e.instance_path()))
            .collect()
    }

    /// Resolve a `$ref` written inside document `doc_file` (a path relative to `schemas/`).
    /// Returns the document the target lives in and the target schema.
    pub fn resolve<'a>(
        &'a self,
        doc_file: &str,
        reference: &str,
    ) -> Option<(&'static str, &'a Value)> {
        let (file, pointer) = match reference.split_once('#') {
            Some((f, p)) => (f, p),
            None => (reference, ""),
        };
        let file = if file.is_empty() {
            doc_file.to_owned()
        } else {
            join_relative(doc_file, file)
        };
        let doc = self.docs.get(&format!("{BASE}{file}"))?;
        let mut cur = doc;
        for seg in pointer.split('/').filter(|s| !s.is_empty()) {
            let seg = seg.replace("~1", "/").replace("~0", "~");
            cur = cur.get(&seg)?;
        }
        let (file, _) = FILES.iter().find(|(name, _)| *name == file)?;
        Some((*file, cur))
    }
}

/// Join `rel` (`../common.json`, `common.json`) onto the directory of `base` (`methods/x.json`).
fn join_relative(base: &str, rel: &str) -> String {
    let mut parts: Vec<&str> = base
        .rsplit_once('/')
        .map(|(d, _)| d)
        .unwrap_or("")
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    for seg in rel.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            s => parts.push(s),
        }
    }
    parts.join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joins_relative_paths() {
        assert_eq!(
            join_relative("methods/messages.json", "../common.json"),
            "common.json"
        );
        assert_eq!(
            join_relative("methods/messages.json", "common.json"),
            "methods/common.json"
        );
        assert_eq!(
            join_relative("events.json", "dto/contact.json"),
            "dto/contact.json"
        );
    }

    #[test]
    fn embedded_list_matches_the_directory() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../schemas");
        let mut on_disk = vec![];
        fn walk(dir: &std::path::Path, root: &std::path::Path, out: &mut Vec<String>) {
            for e in std::fs::read_dir(dir).unwrap() {
                let p = e.unwrap().path();
                if p.is_dir() {
                    if p.file_name().unwrap() != "examples" {
                        walk(&p, root, out);
                    }
                } else if p.extension().is_some_and(|x| x == "json") {
                    out.push(p.strip_prefix(root).unwrap().to_string_lossy().into_owned());
                }
            }
        }
        walk(&root, &root, &mut on_disk);
        on_disk.sort();
        let embedded: Vec<String> = FILES.iter().map(|(f, _)| f.to_string()).collect();
        assert_eq!(
            embedded, on_disk,
            "schemas.rs FILES must list every schema file"
        );
    }

    #[test]
    fn validates_and_resolves() {
        let s = Schemas::embedded();
        assert!(
            s.validate("common.json#/$defs/Id", &Value::String("x".into()))
                .is_empty()
        );
        assert!(
            !s.validate("common.json#/$defs/Id", &Value::String(String::new()))
                .is_empty()
        );
        let (file, v) = s
            .resolve("methods/messages.json", "../common.json#/$defs/Id")
            .unwrap();
        assert_eq!(file, "common.json");
        assert_eq!(v["type"], "string");
    }

    #[test]
    fn source_release_declaration_is_open_but_not_implicitly_trusted() {
        let schemas = Schemas::embedded();
        let reference = "capabilities.json#/$defs/Capabilities";
        for flag in [
            serde_json::json!(true),
            serde_json::json!(false),
            serde_json::json!({}),
            serde_json::json!({"source_release": "future"}),
            serde_json::json!({"source_release": "on_success"}),
        ] {
            let value = serde_json::json!({"send.file": flag});
            assert!(schemas.validate(reference, &value).is_empty());
            let caps: ch_protocol::Capabilities = serde_json::from_value(value).unwrap();
            assert_eq!(
                caps.file_source_released_on_success(),
                flag == serde_json::json!({"source_release": "on_success"})
            );
        }
        for malformed in [
            serde_json::json!(null),
            serde_json::json!(true),
            serde_json::json!(1),
            serde_json::json!([]),
            serde_json::json!({}),
        ] {
            let value = serde_json::json!({"send.file": {"source_release": malformed}});
            assert!(!schemas.validate(reference, &value).is_empty());
            // Runtime raw capability tolerance is deliberately independent of conformance.
            let caps: ch_protocol::Capabilities = serde_json::from_value(value).unwrap();
            assert!(caps.has("send.file"));
            assert!(!caps.file_source_released_on_success());
        }
    }
}
