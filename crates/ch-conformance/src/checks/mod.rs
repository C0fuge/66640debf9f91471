//! The checks, grouped by area, and the context they share.
//!
//! Every check records exactly one [`CheckResult`] under a stable id. Static ids are listed
//! in [`CATALOGUE`]; `cap.<capability>` and `gated.<method>` are generated from the registry.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ch_protocol::{ApiVersion, Capabilities, Manifest};
use serde_json::{Map, Value};

use crate::Options;
use crate::report::{CheckResult, Outcome};
use crate::schemas::Schemas;
use crate::session::Session;

pub mod accounts;
pub mod exit;
pub mod lifecycle;
pub mod manifest;
pub mod methods;
pub mod recording;
pub mod transport;

/// A check's id, one-line title and spec section.
#[derive(Debug, Clone, Copy)]
pub struct CheckDef {
    pub id: &'static str,
    pub title: &'static str,
    pub spec: &'static str,
}

macro_rules! catalogue {
    ($($id:literal => $spec:literal, $title:literal;)*) => {
        /// Every static check, in execution order.
        pub const CATALOGUE: &[CheckDef] = &[$(CheckDef { id: $id, title: $title, spec: $spec },)*];
    };
}

catalogue! {
    "manifest.schema" => "§3.2", "plugin.json parses and validates against schemas/manifest.json";
    "manifest.api_version" => "§18", "the manifest's api_version has the suite's major version";
    "manifest.entry" => "§3.4", "entry[0] resolves to an existing executable (./ relative, else on PATH)";
    "manifest.icon" => "§3.2", "icon, when set, is an existing .svg or .png inside the plugin directory";
    "manifest.settings_schema" => "§6.1", "settings_schema is an object schema; top-level defaults match their types; x-secret only on strings";
    "manifest.auth_methods" => "§3.2", "auth_methods uses known values and 'none' stands alone";
    "process.spawn" => "§3.3", "the process starts with the plugin directory as cwd and CH_* in the environment";
    "transport.silent_before_initialize" => "§4.1", "nothing is written to stdout before initialize";
    "lifecycle.request_before_initialize" => "§4.1", "a request before initialize is answered with INVALID_REQUEST";
    "lifecycle.notification_before_initialize" => "§4.1", "a notification before initialize is ignored";
    "handshake.initialize" => "§4.1", "initialize is answered with a valid result and a compatible api_version";
    "handshake.capabilities" => "§14.1", "capability keys are known or x-<id>.<name>; values validate";
    "handshake.required_methods" => "§18", "the required methods answer (not METHOD_NOT_FOUND)";
    "lifecycle.second_initialize" => "§4.1", "a second initialize is answered with INVALID_REQUEST";
    "lifecycle.ping" => "§4.3", "ping (with and without params) is answered within 10 s with {} or {busy}";
    "transport.string_id" => "§2.3", "a request with a string id is answered with the same string id";
    "transport.crlf" => "§2.2", "a frame ending in \\r\\n is accepted";
    "transport.empty_lines" => "§2.2", "empty lines are ignored";
    "transport.batch_rejected" => "§2.2", "a batch array frame is answered with INVALID_REQUEST";
    "transport.parse_error" => "§2.2", "a non-JSON frame costs one line: PARSE_ERROR or nothing, and the plugin keeps working";
    "transport.unknown_method" => "§2.3", "unknown and plugin-to-core methods are answered with METHOD_NOT_FOUND";
    "transport.unknown_notification" => "§2.3", "an unknown notification is ignored";
    "account.start" => "§4.2", "account.start with the settings_schema defaults returns {} within 30 s";
    "account.events_before_reply" => "§4.2", "no event for the account precedes the account.start reply";
    "account.first_status" => "§4.2", "the first status event follows the account.start reply promptly";
    "account.settled" => "§4.4", "the account reaches connected, auth_required, unavailable or error";
    "auth.consistency" => "§7.2", "auth_required is only reported by plugins with auth.interactive; 'none' plugins never ask";
    "auth.flow" => "§7", "an interactive sign-in driven through auth.begin / auth.step / auth.submit reaches connected";
    "account.duplicate_start" => "§4.2", "account.start for a started account is INVALID_PARAMS (field account_id) and leaves it running";
    "account.unknown_request" => "§5", "a request for an unknown account_id is UNKNOWN_ACCOUNT";
    "account.stop_unknown" => "§4.2", "account.stop for an unknown account_id is UNKNOWN_ACCOUNT";
    "sync.conversation_ids" => "§8.1", "repeated completed chat sync on a stationary fixture produces the same distinct ids";
    "methods.fetch_recent" => "§11.3", "messages.fetch_recent answers; a successful page validates";
    "methods.fetch_recent_idempotent" => "§8.1", "messages.fetch_recent returns the same message ids when asked twice";
    "methods.send" => "§11.3", "messages.send answers (not METHOD_NOT_FOUND / NOT_SUPPORTED)";
    "cap.undeclared" => "§14.2", "methods of undeclared capabilities answer NOT_SUPPORTED";
    "cancel.honoured" => "§11.0", "a cancelled request is still answered (result or CANCELLED) within the grace";
    "cancel.unknown_ignored" => "§11.0", "a $/cancelRequest for an unknown id is ignored";
    "account.multi" => "§5", "with multi_account, a second account starts, reports its own status and stops";
    "account.stop" => "§4.2", "account.stop returns {} within 30 s, after status: disconnected";
    "account.restart" => "§4.2", "the same account_id can be started again after a stop";
    "shutdown.answers" => "§4.5", "shutdown is answered with {} within 10 s";
    "shutdown.exits" => "§4.5", "the process exits with status 0 shortly after answering shutdown";
    "transport.stdout_clean" => "§2.1", "nothing but JSON-RPC frames on stdout";
    "transport.frames_valid" => "§2.3", "every frame validates against jsonrpc.json";
    "events.schema" => "§10", "every event validates against events.json and carries account_id";
    "events.vendor_names" => "§12.3", "every event name is in the registry or x-<plugin-id>.<name>";
    "events.started_accounts_only" => "§4.2", "events are only emitted for accounts between start reply and stop reply";
    "events.status_flags" => "§4.4", "reconnecting rides only on connecting or unavailable";
    "results.schema" => "§11", "every result validates against the method's result schema";
    "errors.shape" => "§13", "every error has data.code, a mirroring numeric code, and FLOOD_WAIT carries seconds";
    "timing.within_timeouts" => "§4.6", "every request was answered within its registry timeout";
    "timestamps.format" => "§8.2", "every *_at timestamp is RFC 3339 UTC with three fractional digits and Z";
    "paths.inside_dirs" => "§15", "every path the plugin emits is absolute and inside its cache_dir or state_dir";
    "partial.merge_defaults" => "§8.4", "merge-updated DTOs do not gain on-create defaults on later updates";
    "requests.schema" => "§12", "plugin-to-core requests (secrets.*) validate";
    "eof.exits" => "§2.5", "EOF on stdin makes the plugin exit 0 within 5 s";
    "eof.inflight_answered" => "§2.5", "a shutdown in flight at EOF is still answered and the plugin exits 0";
}

/// Look up a static check.
pub fn def(id: &str) -> Option<&'static CheckDef> {
    CATALOGUE.iter().find(|d| d.id == id)
}

/// The account the suite starts and what it learned about it.
#[derive(Debug, Clone, Default)]
pub struct AccountCtx {
    pub id: String,
    /// Last settled state.
    pub state: Option<String>,
    /// Is the account currently started (from the suite's point of view)?
    pub started: bool,
}

/// Shared state of one run.
pub struct Ctx<'a> {
    pub opts: &'a Options,
    pub capture: crate::capture::CaptureBudget,
    pub redactions: crate::redact::Redactions,
    pub dir: PathBuf,
    pub schemas: &'static Schemas,
    pub results: Vec<CheckResult>,
    pub isolate: PathBuf,
    pub manifest: Option<Manifest>,
    pub exe: Option<PathBuf>,
    pub session: Option<Session>,
    pub caps: Capabilities,
    pub plugin_api_version: Option<ApiVersion>,
    pub account: AccountCtx,
    /// (account_id, start reply seq, stop reply seq) for every lifecycle the suite ran.
    pub lifecycles: Vec<(String, usize, Option<usize>)>,
    pub stderr_tail: String,
}

impl<'a> Ctx<'a> {
    pub fn new(dir: &Path, opts: &'a Options, isolate: PathBuf) -> Self {
        let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
        Ctx {
            opts,
            capture: Default::default(),
            redactions: crate::redact::Redactions::new(opts),
            dir,
            schemas: Schemas::embedded(),
            results: Vec::new(),
            isolate,
            manifest: None,
            exe: None,
            session: None,
            caps: Capabilities::default(),
            plugin_api_version: None,
            account: AccountCtx::default(),
            lifecycles: Vec::new(),
            stderr_tail: String::new(),
        }
    }

    /// Record a static check.
    pub fn record(&mut self, id: &str, outcome: Outcome, message: impl Into<String>) {
        let d = def(id).unwrap_or_else(|| panic!("unknown check id {id}"));
        self.record_dyn(id, d.title, d.spec, outcome, message);
    }

    /// Record a generated check.
    pub fn record_dyn(
        &mut self,
        id: &str,
        title: &str,
        spec: &str,
        outcome: Outcome,
        message: impl Into<String>,
    ) {
        if let Some(existing) = self.results.iter_mut().find(|r| r.id == id) {
            // A check is recorded once; a later verdict replaces an earlier one only if worse.
            if outcome == Outcome::Fail && existing.outcome != Outcome::Fail {
                existing.outcome = outcome;
                existing.message = message.into();
            }
            return;
        }
        self.results.push(CheckResult {
            id: id.to_owned(),
            title: title.to_owned(),
            spec: spec.to_owned(),
            outcome,
            message: message.into(),
        });
    }

    pub fn pass(&mut self, id: &str, message: impl Into<String>) {
        self.record(id, Outcome::Pass, message);
    }
    pub fn fail(&mut self, id: &str, message: impl Into<String>) {
        self.record(id, Outcome::Fail, message);
    }
    pub fn skip(&mut self, id: &str, message: impl Into<String>) {
        self.record(id, Outcome::Skip, message);
    }

    /// Mark every static check from `from_id` onwards (in catalogue order) as skipped.
    pub fn skip_rest(&mut self, from_id: &str, reason: &str) {
        let mut on = false;
        for d in CATALOGUE {
            if d.id == from_id {
                on = true;
            }
            if on && !self.results.iter().any(|r| r.id == d.id) {
                self.record(d.id, Outcome::Skip, reason);
            }
        }
    }

    /// Record a verdict from a boolean.
    pub fn verdict(&mut self, id: &str, ok: bool, message: impl Into<String>) {
        self.record(id, if ok { Outcome::Pass } else { Outcome::Fail }, message);
    }

    pub fn manifest(&self) -> &Manifest {
        self.manifest.as_ref().expect("manifest checked before use")
    }

    pub fn session(&mut self) -> &mut Session {
        self.session.as_mut().expect("session running")
    }

    /// The registry timeout for `method`, as a duration.
    pub fn timeout_for(method: &str) -> Duration {
        let secs = ch_protocol::method(method)
            .map(|m| m.timeout_seconds)
            .unwrap_or(ch_protocol::DEFAULT_TIMEOUT_SECONDS);
        Duration::from_secs(secs as u64)
    }

    /// Settings for `account.start`: schema defaults, then `--setting` overrides (§6.1).
    pub fn settings(&self) -> Map<String, Value> {
        let mut out = Map::new();
        if let Some(schema) = self.manifest().settings_schema.as_ref()
            && let Some(props) = schema.get("properties").and_then(Value::as_object)
        {
            for (k, p) in props {
                if let Some(d) = p.get("default") {
                    out.insert(k.clone(), d.clone());
                }
            }
        }
        for (k, v) in &self.opts.settings {
            out.insert(k.clone(), v.clone());
        }
        out
    }

    /// Params for `account.start` of `account_id`, with the secrets the plugin stored so far.
    pub fn start_params(&mut self, account_id: &str) -> Value {
        let base = self.isolate.join("accounts").join(sanitise(account_id));
        let state_dir = base.join("state");
        let cache_dir = base.join("cache");
        let _ = std::fs::create_dir_all(&state_dir);
        let _ = std::fs::create_dir_all(&cache_dir);
        let empty = Map::new();
        let seed = if account_id == self.opts.account_id {
            &self.opts.secrets
        } else {
            &empty
        };
        let secrets = self
            .session
            .as_ref()
            .expect("session started")
            .account_secrets(account_id, seed);
        serde_json::json!({
            "account_id": account_id,
            "settings": self.settings(),
            "secrets": secrets,
            "state_dir": state_dir,
            "cache_dir": cache_dir,
            "ghost": false,
            "auto_download": {"photos": false, "videos_max_bytes": 0, "files_max_bytes": 0, "voice": false}
        })
    }

    /// Declared capability keys that are on.
    pub fn declared(&self) -> Vec<String> {
        self.caps
            .0
            .iter()
            .filter(|(_, f)| f.is_on())
            .map(|(k, _)| k.clone())
            .collect()
    }

    pub fn has_cap(&self, key: &str) -> bool {
        self.caps.has(key)
    }

    /// Registry rows (from the embedded registry.json) as a map name → row.
    pub fn registry_methods(&self) -> BTreeMap<String, Value> {
        self.schemas.registry()["methods"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|m| m["name"].as_str().map(|n| (n.to_owned(), m.clone())))
                    .collect()
            })
            .unwrap_or_default()
    }
}

fn sanitise(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// The whole flow: static checks, the main session, the post-session checks, the EOF sessions.
pub async fn run_all(ctx: &mut Ctx<'_>) {
    if !manifest::run(ctx) {
        ctx.skip_rest(
            "process.spawn",
            "the manifest or entry is unusable; nothing was run",
        );
        return;
    }
    if !lifecycle::spawn(ctx).await {
        ctx.skip_rest(
            "transport.silent_before_initialize",
            "the process did not start",
        );
        return;
    }
    transport::before_initialize(ctx).await;
    if !lifecycle::handshake(ctx).await {
        lifecycle::abandon(ctx).await;
        ctx.skip_rest(
            "lifecycle.second_initialize",
            "initialize failed; nothing else was run",
        );
        return;
    }
    lifecycle::after_handshake(ctx).await;
    transport::framing(ctx).await;
    accounts::start_and_settle(ctx).await;
    accounts::auth(ctx).await;
    accounts::edge_cases(ctx).await;
    methods::run(ctx).await;
    accounts::multi(ctx).await;
    accounts::stop_and_restart(ctx).await;
    lifecycle::shutdown(ctx).await;
    recording::run(ctx);
    if let Some(s) = ctx.session.take() {
        ctx.stderr_tail = tail(&s.stderr_text(), 40);
        drop(s);
    }
    exit::run(ctx).await;
}

/// The last `n` lines of `s`.
pub fn tail(s: &str, n: usize) -> String {
    let lines: Vec<&str> = s.lines().collect();
    let start = lines.len().saturating_sub(n);
    lines[start..].join("\n")
}
