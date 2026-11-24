//! Static checks on `plugin.json` (§3.2, §3.4, §6.1, §18).

use std::path::PathBuf;

use ch_protocol::{AuthMethod, Manifest};
use serde_json::Value;

use super::Ctx;
use crate::report::Outcome;

/// Returns `false` when the manifest or the entry is unusable (nothing can be run).
pub fn run(ctx: &mut Ctx<'_>) -> bool {
    let path = ctx.dir.join("plugin.json");
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => {
            ctx.fail(
                "manifest.schema",
                format!("cannot read {}: {e}", path.display()),
            );
            ctx.skip_rest("manifest.api_version", "no manifest");
            return false;
        }
    };
    let value: Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(e) => {
            ctx.fail(
                "manifest.schema",
                format!("plugin.json is not valid JSON: {e}"),
            );
            ctx.skip_rest("manifest.api_version", "no manifest");
            return false;
        }
    };
    ctx.redactions.settings(&value, ctx.opts);
    let errors = ctx.schemas.validate("manifest.json", &value);
    let manifest: Option<Manifest> = serde_json::from_value(value.clone()).ok();
    let structural = manifest.as_ref().map(Manifest::validate);
    match (errors.is_empty(), structural) {
        (true, Some(Ok(()))) => ctx.pass(
            "manifest.schema",
            format!("id {}, version {}", value["id"], value["version"]),
        ),
        (true, Some(Err(e))) => ctx.fail("manifest.schema", e),
        (true, None) => ctx.fail(
            "manifest.schema",
            "plugin.json validates but does not deserialise into Manifest",
        ),
        (false, _) => ctx.fail("manifest.schema", errors.join("\n")),
    }
    let Some(manifest) = manifest.filter(|m| m.validate().is_ok() && errors.is_empty()) else {
        ctx.skip_rest("manifest.api_version", "the manifest is invalid");
        return false;
    };

    // api_version
    match ch_protocol::ApiVersion::parse(&manifest.api_version) {
        Ok(v) if v.major == ch_protocol::API_MAJOR => {
            ctx.pass(
                "manifest.api_version",
                format!("api_version {} (suite: {})", v, ch_protocol::API_VERSION),
            );
        }
        Ok(v) => ctx.fail(
            "manifest.api_version",
            format!(
                "api_version {v} has major {}, this suite implements {}",
                v.major,
                ch_protocol::API_VERSION
            ),
        ),
        Err(e) => ctx.fail(
            "manifest.api_version",
            format!("api_version {:?}: {e}", manifest.api_version),
        ),
    }

    // entry
    let exe = resolve_entry(&ctx.dir, &manifest.entry[0]);
    match &exe {
        Ok(p) => ctx.pass("manifest.entry", format!("{} -> {}", manifest.entry[0], p.display())),
        Err(p) => ctx.fail(
            "manifest.entry",
            format!(
                "entry[0] {:?} is not an executable file ({}); the core would show \"not installed\" (did you run uv sync / cargo build --release?)",
                manifest.entry[0],
                p.display()
            ),
        ),
    }

    // icon
    match &manifest.icon {
        None => ctx.skip("manifest.icon", "no icon declared"),
        Some(icon) => {
            let p = ctx.dir.join(icon);
            let ext_ok = p
                .extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| matches!(e.to_ascii_lowercase().as_str(), "svg" | "png"));
            let inside = std::fs::canonicalize(&p).is_ok_and(|c| c.starts_with(&ctx.dir));
            if p.is_file() && ext_ok && inside {
                ctx.pass("manifest.icon", icon.clone());
            } else {
                ctx.fail(
                    "manifest.icon",
                    format!(
                        "icon {icon:?}: {}",
                        if !p.is_file() {
                            "file does not exist"
                        } else if !ext_ok {
                            "not .svg or .png"
                        } else {
                            "not inside the plugin directory"
                        }
                    ),
                );
            }
        }
    }

    // settings_schema
    match value.get("settings_schema") {
        None => ctx.skip("manifest.settings_schema", "no settings_schema"),
        Some(s) => {
            let problems = settings_schema_problems(s);
            if problems.is_empty() {
                let n = s
                    .get("properties")
                    .and_then(Value::as_object)
                    .map(|p| p.len())
                    .unwrap_or(0);
                ctx.pass(
                    "manifest.settings_schema",
                    format!("{n} top-level properties"),
                );
            } else {
                ctx.fail("manifest.settings_schema", problems.join("\n"));
            }
        }
    }

    // auth_methods
    let methods = &manifest.auth_methods;
    let unknown: Vec<&str> = methods
        .iter()
        .filter(|m| !m.is_known())
        .map(|m| m.as_str())
        .collect();
    let has_none = methods.contains(&AuthMethod::None);
    if methods.is_empty() {
        ctx.fail(
            "manifest.auth_methods",
            "auth_methods is empty; use [\"none\"] for a plugin that signs in from its settings",
        );
    } else if has_none && methods.len() > 1 {
        ctx.fail(
            "manifest.auth_methods",
            format!("\"none\" combined with other methods: {methods:?}"),
        );
    } else if !unknown.is_empty() {
        // Unknown values are allowed (shown verbatim), but worth a note.
        ctx.record(
            "manifest.auth_methods",
            Outcome::Pass,
            format!(
                "unknown auth method names {unknown:?} are shown verbatim by the core; known: {:?}",
                AuthMethod::KNOWN
            ),
        );
    } else {
        ctx.pass(
            "manifest.auth_methods",
            format!(
                "{:?}",
                methods.iter().map(|m| m.as_str()).collect::<Vec<_>>()
            ),
        );
    }

    let compatible = manifest.is_compatible();
    ctx.manifest = Some(manifest);
    if !compatible {
        return false;
    }
    match exe {
        Ok(p) => {
            ctx.exe = Some(p);
            true
        }
        Err(_) => false,
    }
}

/// `entry[0]` starting with `./` is relative to the plugin directory; otherwise it is looked
/// up on `PATH` (an absolute path passes that lookup as it is) (§3.2).
pub fn resolve_entry(dir: &std::path::Path, entry0: &str) -> Result<PathBuf, PathBuf> {
    let candidate = if entry0.starts_with("./") || entry0.starts_with("../") {
        dir.join(entry0)
    } else if std::path::Path::new(entry0).is_absolute() {
        PathBuf::from(entry0)
    } else if entry0.contains('/') {
        dir.join(entry0)
    } else {
        let path = std::env::var_os("PATH").unwrap_or_default();
        let found = std::env::split_paths(&path)
            .map(|p| p.join(entry0))
            .find(|p| is_executable(p));
        return found.ok_or_else(|| PathBuf::from(entry0));
    };
    if is_executable(&candidate) {
        Ok(candidate)
    } else {
        Err(candidate)
    }
}

#[cfg(unix)]
fn is_executable(p: &std::path::Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(p).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable(p: &std::path::Path) -> bool {
    p.is_file()
}

fn settings_schema_problems(s: &Value) -> Vec<String> {
    let mut out = vec![];
    if s.get("type")
        .and_then(Value::as_str)
        .is_some_and(|t| t != "object")
    {
        out.push(format!(
            "settings_schema.type must be \"object\", found {}",
            s["type"]
        ));
    }
    let Some(props) = s.get("properties").and_then(Value::as_object) else {
        if s.get("properties").is_some() {
            out.push("settings_schema.properties is not an object".into());
        }
        return out;
    };
    for (name, p) in props {
        let ty = p.get("type");
        let ty_str = ty.and_then(Value::as_str);
        if let Some(d) = p.get("default") {
            let ok = match ty_str {
                Some("string") => d.is_string(),
                Some("integer") => d.is_i64() || d.is_u64(),
                Some("number") => d.is_number(),
                Some("boolean") => d.is_boolean(),
                Some("object") => d.is_object(),
                Some("array") => d.is_array(),
                _ => true,
            };
            if !ok {
                out.push(format!(
                    "property {name:?}: default {d} does not match type {}",
                    ty.unwrap_or(&Value::Null)
                ));
            }
            if let Some(e) = p.get("enum").and_then(Value::as_array)
                && !e.contains(d)
            {
                out.push(format!(
                    "property {name:?}: default {d} is not one of its enum values"
                ));
            }
        }
        if p.get("x-secret") == Some(&Value::Bool(true)) && ty_str != Some("string") {
            out.push(format!(
                "property {name:?}: x-secret is only meaningful on a string property"
            ));
        }
        if let Some(w) = p.get("x-widget").and_then(Value::as_str)
            && !matches!(w, "password" | "textarea" | "file" | "directory")
        {
            out.push(format!("property {name:?}: unknown x-widget {w:?}"));
        }
        if p.get("x-order").is_some_and(|o| !o.is_i64() && !o.is_u64()) {
            out.push(format!("property {name:?}: x-order must be an integer"));
        }
    }
    out
}
