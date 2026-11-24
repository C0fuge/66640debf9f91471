//! Process start, handshake, ping, unknown methods, shutdown (§4.1, §4.3, §4.5, §2.3).

use std::time::Duration;

use ch_protocol::Capabilities;
use serde_json::{Value, json};

use super::Ctx;
use crate::session::Session;

/// Spawn the plugin into `ctx.session`. `false` when it could not be started.
pub async fn spawn(ctx: &mut Ctx<'_>) -> bool {
    let exe = ctx.exe.clone().expect("entry resolved");
    let manifest = ctx.manifest().clone();
    let isolate = ctx.isolate.clone();
    match Session::spawn(
        &ctx.dir,
        &manifest,
        &exe,
        &isolate,
        ctx.redactions.clone(),
        ctx.capture.clone(),
    )
    .await
    {
        Ok(mut s) => {
            // Give a crashing process a moment to show it.
            tokio::time::sleep(Duration::from_millis(150)).await;
            if let Some(st) = s.try_exit() {
                s.kill().await;
                ctx.fail(
                    "process.spawn",
                    format!("the process exited immediately with {st}"),
                );
                ctx.stderr_tail = super::tail(&s.stderr_text(), 40);
                return false;
            }
            ctx.pass(
                "process.spawn",
                format!("{} {}", exe.display(), manifest.entry[1..].join(" ")),
            );
            ctx.session = Some(s);
            true
        }
        Err(e) => {
            ctx.fail("process.spawn", format!("spawn {}: {e}", exe.display()));
            false
        }
    }
}

/// `initialize`: result shape, version compatibility, capabilities. `false` on failure.
pub async fn handshake(ctx: &mut Ctx<'_>) -> bool {
    let params = json!({
        "api_version": ch_protocol::API_VERSION,
        "core_version": format!("ch-conformance {}", env!("CARGO_PKG_VERSION")),
        "plugin_dir": ctx.dir,
        "locale": "en-US",
        "log_level": "debug",
    });
    let reply = ctx
        .session()
        .call("initialize", Some(params), Ctx::timeout_for("initialize"))
        .await;
    let result = match &reply.response {
        Some(Ok(r)) => r.clone(),
        _ => {
            ctx.fail("handshake.initialize", reply.describe());
            ctx.skip("handshake.capabilities", "initialize failed");
            return false;
        }
    };
    let errors = ctx
        .schemas
        .validate("methods/lifecycle.json#/$defs/initialize.result", &result);
    if !errors.is_empty() {
        ctx.fail(
            "handshake.initialize",
            format!("result does not validate: {}", errors.join("; ")),
        );
        ctx.skip("handshake.capabilities", "initialize result invalid");
        return false;
    }
    let api = result["api_version"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    match ch_protocol::ApiVersion::parse(&api) {
        Ok(v) if v.major == ch_protocol::API_MAJOR => {
            let manifest_v = &ctx.manifest().api_version;
            let note = if *manifest_v != api {
                format!(" (manifest says {manifest_v})")
            } else {
                String::new()
            };
            ctx.pass(
                "handshake.initialize",
                format!("api_version {api}{note}, {:.0?}", reply.elapsed),
            );
            ctx.plugin_api_version = Some(v);
        }
        Ok(v) => {
            ctx.fail(
                "handshake.initialize",
                format!(
                    "plugin api_version {v} is not major {}",
                    ch_protocol::API_MAJOR
                ),
            );
            return false;
        }
        Err(e) => {
            ctx.fail("handshake.initialize", format!("api_version {api:?}: {e}"));
            return false;
        }
    }

    // capabilities
    let caps_value = result["capabilities"].clone();
    let mut problems = vec![];
    let errors = ctx
        .schemas
        .validate("capabilities.json#/$defs/Capabilities", &caps_value);
    problems.extend(errors);
    let known: Vec<&str> = ctx.schemas.doc("capabilities.json")["$defs"]["CapabilityKey"]["enum"]
        .as_array()
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let id = ctx.manifest().id.clone();
    let newer_minor = ctx
        .plugin_api_version
        .as_ref()
        .is_some_and(|v| v.minor > ch_protocol::ApiVersion::current().minor);
    if let Some(obj) = caps_value.as_object() {
        for (k, v) in obj {
            if !known.contains(&k.as_str()) {
                if k.starts_with("x-") {
                    if !k.starts_with(&format!("x-{id}.")) {
                        problems.push(format!(
                            "vendor capability {k:?} does not use this plugin's prefix x-{id}."
                        ));
                    }
                } else if !newer_minor {
                    problems.push(format!("unknown capability key {k:?} (un-prefixed names are reserved; vendor keys are x-{id}.<name>)"));
                }
            }
            if !(v.is_boolean() || v.is_object()) {
                problems.push(format!(
                    "capability {k:?} must be true/false or a parameter object, found {v}"
                ));
            }
        }
    }
    let caps: Capabilities = serde_json::from_value(caps_value).unwrap_or_default();
    ctx.caps = caps;
    let declared = ctx.declared();
    if problems.is_empty() {
        ctx.pass(
            "handshake.capabilities",
            format!("{} capabilities declared", declared.len()),
        );
    } else {
        ctx.fail("handshake.capabilities", problems.join("\n"));
    }
    true
}

/// Second initialize, ping, unknown methods and notifications.
pub async fn after_handshake(ctx: &mut Ctx<'_>) {
    // second initialize
    let params = json!({
        "api_version": ch_protocol::API_VERSION, "core_version": "ch-conformance", "plugin_dir": ctx.dir,
        "locale": "en-US", "log_level": "debug",
    });
    let r = ctx
        .session()
        .call("initialize", Some(params), Duration::from_secs(10))
        .await;
    ctx.verdict(
        "lifecycle.second_initialize",
        r.is_code("INVALID_REQUEST") && r.numeric() == Some(-32600),
        format!("second initialize: {}", r.describe()),
    );

    // ping, with and without params
    let r1 = ctx
        .session()
        .call("ping", None, Ctx::timeout_for("ping"))
        .await;
    let r2 = ctx
        .session()
        .call("ping", Some(json!({})), Ctx::timeout_for("ping"))
        .await;
    let mut msgs = vec![];
    let mut ok = true;
    for (what, r) in [("without params", &r1), ("with {}", &r2)] {
        match &r.response {
            Some(Ok(res)) => {
                let errs = ctx
                    .schemas
                    .validate("methods/lifecycle.json#/$defs/ping.result", res);
                if errs.is_empty() {
                    msgs.push(format!("ping {what}: {} in {:.0?}", res, r.elapsed));
                } else {
                    ok = false;
                    msgs.push(format!(
                        "ping {what}: result {res} invalid: {}",
                        errs.join("; ")
                    ));
                }
            }
            _ => {
                ok = false;
                msgs.push(format!("ping {what}: {}", r.describe()));
            }
        }
    }
    ctx.verdict("lifecycle.ping", ok, msgs.join("\n"));

    // unknown methods
    let id = ctx.manifest().id.clone();
    let probes = [
        ("conformance.nonexistent", "an unknown un-prefixed method"),
        (
            &*format!("x-{id}.conformance_nonexistent"),
            "an unknown vendor method",
        ),
        ("secrets.set", "a plugin-to-core method sent to the plugin"),
    ];
    let mut msgs = vec![];
    let mut ok = true;
    for (m, what) in probes {
        let params = if m == "secrets.set" {
            json!({"account_id": "x", "key": "k", "value": "v"})
        } else {
            json!({"account_id": "x"})
        };
        let r = ctx
            .session()
            .call(m, Some(params), Duration::from_secs(10))
            .await;
        let good = r.is_code("METHOD_NOT_FOUND") && r.numeric() == Some(-32601);
        ok &= good;
        msgs.push(format!("{m} ({what}): {}", r.describe()));
    }
    ctx.verdict("transport.unknown_method", ok, msgs.join("\n"));

    let from = ctx.session().cursor();
    ctx.session()
        .notify("conformance.nonexistent", Some(json!({"account_id": "x"})))
        .await;
    let stray = ctx
        .session()
        .wait_for(from, Duration::from_millis(300), |_| true)
        .await;
    match stray {
        None => ctx.pass(
            "transport.unknown_notification",
            "no frame was written in reply",
        ),
        Some(l) => ctx.fail(
            "transport.unknown_notification",
            format!(
                "the plugin wrote a frame in reply: {}",
                crate::session::short(&l.raw)
            ),
        ),
    }
}

/// `shutdown`, then exit status and timing (§4.5).
pub async fn shutdown(ctx: &mut Ctx<'_>) {
    let grace = Duration::from_secs(ch_protocol::SHUTDOWN_GRACE_SECONDS);
    let r = ctx.session().call("shutdown", None, grace).await;
    match &r.response {
        Some(Ok(res)) if res.is_object() => ctx.pass(
            "shutdown.answers",
            format!("answered {res} in {:.0?}", r.elapsed),
        ),
        Some(Ok(res)) => ctx.fail(
            "shutdown.answers",
            format!("result must be an object, got {res}"),
        ),
        Some(Err(_)) => ctx.fail("shutdown.answers", r.describe()),
        None => ctx.fail(
            "shutdown.answers",
            format!("no reply to shutdown within {grace:?}"),
        ),
    }
    let answered = matches!(r.response, Some(Ok(_)));
    let budget = Duration::from_secs(ch_protocol::EXIT_GRACE_SECONDS);
    let started = std::time::Instant::now();
    let exit = ctx.session().wait_exit(budget).await;
    match exit {
        Some(st) if st.success() && answered => ctx.pass(
            "shutdown.exits",
            format!("exited 0 {:.0?} after the reply", started.elapsed()),
        ),
        Some(st) if answered => ctx.fail(
            "shutdown.exits",
            format!("exited with {st} (expected status 0)"),
        ),
        Some(st) => ctx.fail(
            "shutdown.exits",
            format!("exited with {st} without answering shutdown"),
        ),
        None => {
            ctx.fail(
                "shutdown.exits",
                format!(
                    "still running {budget:?} after {}; the core would SIGTERM then SIGKILL it",
                    if answered {
                        "the reply"
                    } else {
                        "the shutdown grace"
                    }
                ),
            );
            ctx.session().kill().await;
        }
    }
    ctx.session().kill().await;
}

/// Kill the process after a failed handshake, keeping its stderr.
pub async fn abandon(ctx: &mut Ctx<'_>) {
    if let Some(mut s) = ctx.session.take() {
        s.kill().await;
        ctx.stderr_tail = super::tail(&s.stderr_text(), 40);
    }
}
