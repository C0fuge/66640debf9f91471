//! Validate the actual plugin output in order, including events sent as requests.
use super::Ctx;
use crate::session::Kind;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

#[derive(Default)]
struct Observations {
    samples: usize,
    failures: Vec<String>,
}

impl Observations {
    fn sample(&mut self, errors: Vec<String>) {
        self.samples += 1;
        self.failures.extend(
            errors
                .into_iter()
                .take(3.min(10usize.saturating_sub(self.failures.len()))),
        );
    }
    fn condition(&mut self, condition: bool, detail: impl Into<String>) {
        self.samples += 1;
        if !condition && self.failures.len() < 10 {
            self.failures.push(detail.into());
        }
    }
    fn finish(self, ctx: &mut Ctx<'_>, id: &str) {
        if self.samples == 0 {
            ctx.skip(id, "no applicable frames observed");
        } else if self.failures.is_empty() {
            ctx.pass(id, format!("{} observations", self.samples));
        } else {
            ctx.fail(
                id,
                self.failures
                    .into_iter()
                    .take(10)
                    .collect::<Vec<_>>()
                    .join("\n"),
            );
        }
    }
}

pub fn run(ctx: &mut Ctx<'_>) {
    let mut checks: BTreeMap<&'static str, Observations> = BTreeMap::new();
    let mut seen = BTreeSet::new();
    let mut required = BTreeSet::new();
    let recording = ctx.session().take_recording();
    let sent = recording.sent;
    let prefix = format!("x-{}.", ctx.manifest().id);
    for line in recording.lines {
        let seq = line.seq;
        let desc = format!("stdout frame {seq}");
        checks
            .entry("transport.stdout_clean")
            .or_default()
            .condition(line.value.is_some(), &desc);
        let Some(v) = &line.value else {
            continue;
        };
        checks
            .entry("transport.frames_valid")
            .or_default()
            .sample(ctx.schemas.validate("jsonrpc.json#/$defs/Message", v));
        let mut data = None;
        let mut account = None;
        match &line.kind {
            Kind::Error { id, .. } | Kind::Success { id, .. } => {
                if let Kind::Error { error, .. } = &line.kind {
                    let mut errs = ctx
                        .schemas
                        .validate("errors.json#/$defs/ErrorObject", error);
                    if let Some(code) = error.pointer("/data/code").and_then(Value::as_str) {
                        let code = ch_protocol::ErrorCode::from(code);
                        if code.is_known() && Some(code.numeric()) != error["code"].as_i64() {
                            errs.push(format!("{desc}: expected {} for {code}", code.numeric()));
                        }
                    }
                    checks.entry("errors.shape").or_default().sample(errs);
                }
                let key = id.to_string();
                if let Some(call) = sent.get(&key) {
                    let unique = seen.insert(key.clone());
                    checks
                        .entry("transport.frames_valid")
                        .or_default()
                        .condition(unique, format!("duplicate response id {id}"));
                    let method = call.method.as_str();
                    let in_time = line.at.saturating_duration_since(call.at) <= call.timeout;
                    checks
                        .entry("timing.within_timeouts")
                        .or_default()
                        .condition(in_time, format!("{method} exceeded its timeout"));
                    if ch_protocol::REQUIRED_METHODS.contains(&method)
                        && (matches!(line.kind, Kind::Success { .. })
                            || v.pointer("/error/data/code").is_some_and(|c| {
                                ![
                                    "NOT_SUPPORTED",
                                    "METHOD_NOT_FOUND",
                                    "INVALID_REQUEST",
                                    "UNKNOWN_ACCOUNT",
                                ]
                                .contains(&c.as_str().unwrap_or(""))
                            }))
                    {
                        required.insert(method.to_owned());
                    }
                    if let (Kind::Success { result, .. }, Some(info)) =
                        (&line.kind, ch_protocol::method(method))
                    {
                        let reference = format!("methods/{}#/$defs/{method}.result", info.schema);
                        checks
                            .entry("results.schema")
                            .or_default()
                            .sample(ctx.schemas.validate(&reference, result));
                        data = Some(result);
                        account = call.params.as_ref().and_then(|p| p["account_id"].as_str());
                    }
                } else {
                    let allowed = id.is_null()
                        && matches!(
                            v.pointer("/error/data/code").and_then(Value::as_str),
                            Some("PARSE_ERROR" | "INVALID_REQUEST")
                        );
                    checks
                        .entry("transport.frames_valid")
                        .or_default()
                        .condition(allowed, format!("unsolicited response {id}"));
                }
            }
            Kind::Notification { method, params } | Kind::Request { method, params, .. } => {
                if method == ch_protocol::CANCEL_METHOD {
                    checks.entry("requests.schema").or_default().sample(
                        ctx.schemas
                            .validate("jsonrpc.json#/$defs/CancelParams", params),
                    );
                } else if matches!(method.as_str(), "secrets.set" | "secrets.delete") {
                    let mut errors = ctx
                        .schemas
                        .validate(&format!("methods/core.json#/$defs/{method}.params"), params);
                    if !matches!(line.kind, Kind::Request { .. }) {
                        errors.push(format!("{method} must be a request"));
                    }
                    checks.entry("requests.schema").or_default().sample(errors);
                } else {
                    let known = ch_protocol::EVENT_NAMES.contains(&method.as_str());
                    checks.entry("events.vendor_names").or_default().condition(
                        known || method.starts_with(&prefix),
                        format!("unknown event {method}"),
                    );
                    if known {
                        checks.entry("events.schema").or_default().sample(
                            ctx.schemas
                                .validate(&format!("events.json#/$defs/{method}"), params),
                        );
                    }
                    account = params["account_id"].as_str();
                    let started = ctx.lifecycles.iter().any(|(a, start, stop)| {
                        account == Some(a.as_str()) && seq > *start && stop.is_none_or(|n| seq < n)
                    });
                    checks
                        .entry("events.started_accounts_only")
                        .or_default()
                        .condition(
                            started,
                            format!("{method} outside an account's run at frame {seq}"),
                        );
                    if method == "status" {
                        let ok = params["reconnecting"] != true
                            || matches!(
                                params["state"].as_str(),
                                Some("connecting" | "unavailable")
                            );
                        checks
                            .entry("events.status_flags")
                            .or_default()
                            .condition(ok, format!("reconnecting with state {}", params["state"]));
                    }
                    if known {
                        data = Some(params);
                    }
                }
            }
            _ => {}
        }
        if let Some(payload) = data {
            inspect_fields(payload, account, ctx, &mut checks);
        }
    }
    for (id, request) in sent {
        if !seen.contains(&id) {
            checks
                .entry("timing.within_timeouts")
                .or_default()
                .condition(false, format!("{}: no reply", request.method));
        }
    }
    let missing: Vec<_> = ch_protocol::REQUIRED_METHODS
        .iter()
        .filter(|m| !required.contains(**m))
        .collect();
    if missing.is_empty() {
        ctx.pass(
            "handshake.required_methods",
            "all required methods answered",
        );
    } else {
        ctx.skip(
            "handshake.required_methods",
            format!("no handler verified for {missing:?}; see the individual method results"),
        );
    }
    for (id, obs) in checks {
        obs.finish(ctx, id);
    }
}

fn inspect_fields(
    value: &Value,
    account: Option<&str>,
    ctx: &Ctx<'_>,
    checks: &mut BTreeMap<&'static str, Observations>,
) {
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                if key == "ext" {
                    continue;
                }
                if let Some(s) = value.as_str() {
                    if key.ends_with("_at") {
                        let digits = s.as_bytes();
                        let canonical = digits.len() == 24
                            && digits
                                .iter()
                                .zip(b"0000-00-00T00:00:00.000Z")
                                .all(|(a, b)| {
                                    if *b == b'0' {
                                        a.is_ascii_digit()
                                    } else {
                                        a == b
                                    }
                                });
                        checks
                            .entry("timestamps.format")
                            .or_default()
                            .condition(canonical, format!("non-canonical timestamp at {key}"));
                    }
                    if key == "path" || key.ends_with("_path") {
                        let path = Path::new(s);
                        let valid = account.is_some_and(|id| {
                            let base = ctx.isolate.join("accounts").join(super::sanitise(id));
                            // Resolve the deepest existing ancestor too: a missing file can still
                            // escape the account via an existing symlink in its parent path.
                            path.is_absolute()
                                && !path
                                    .components()
                                    .any(|c| c == std::path::Component::ParentDir)
                                && ["state", "cache"].iter().any(|name| {
                                    let root = base.join(name);
                                    path.starts_with(&root)
                                        && path
                                            .ancestors()
                                            .find_map(|p| p.canonicalize().ok())
                                            .is_some_and(|p| p.starts_with(&root))
                                })
                        });
                        checks.entry("paths.inside_dirs").or_default().condition(
                            valid,
                            format!("path outside emitting account directories at {key}"),
                        );
                    }
                } else {
                    inspect_fields(value, account, ctx, checks);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                inspect_fields(item, account, ctx, checks);
            }
        }
        _ => {}
    }
}
