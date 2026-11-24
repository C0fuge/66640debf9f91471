//! Account lifecycle: start, status, interactive sign-in, duplicates, multi-account, stop,
//! restart (§4.2, §4.4, §5, §7).

use std::time::Duration;

use ch_protocol::AuthMethod;
use serde_json::{Value, json};

use super::Ctx;
use crate::session::{Line, short};

const SETTLED: &[&str] = &["connected", "auth_required", "unavailable", "error"];

/// A status event's state, when `l` is one for `account_id`.
fn status_state<'a>(l: &'a Line, account_id: &str) -> Option<&'a str> {
    let (name, p) = l.event()?;
    if name != "status" || p.get("account_id").and_then(Value::as_str) != Some(account_id) {
        return None;
    }
    p.get("state").and_then(Value::as_str)
}

/// `account.start`, event ordering, first status, settled state.
pub async fn start_and_settle(ctx: &mut Ctx<'_>) {
    let id = ctx.opts.account_id.clone();
    ctx.account.id = id.clone();
    let params = ctx.start_params(&id);
    let setting_count = params["settings"].as_object().map_or(0, |m| m.len());
    let r = ctx
        .session()
        .call(
            "account.start",
            Some(params),
            Ctx::timeout_for("account.start"),
        )
        .await;
    let reply_seq = match &r.response {
        Some(Ok(res)) if res.is_object() => {
            ctx.pass(
                "account.start",
                format!("accepted {setting_count} settings in {:.0?}", r.elapsed),
            );
            r.seq.unwrap_or(0)
        }
        Some(Ok(res)) => {
            ctx.fail(
                "account.start",
                format!("result must be an object, got {res}"),
            );
            return not_started(ctx, "account.start did not return an object");
        }
        Some(Err(_)) => {
            ctx.fail(
                "account.start",
                format!(
                    "{} — with {setting_count} test settings; pass --setting key=value / --secret key=value if the plugin needs more",
                    r.describe(),
                ),
            );
            return not_started(ctx, "account.start failed");
        }
        None => {
            ctx.fail("account.start", r.describe());
            return not_started(ctx, "account.start did not answer");
        }
    };
    ctx.account.started = true;
    ctx.lifecycles.push((id.clone(), reply_seq, None));

    // Events before the reply.
    let early = ctx.session().with_recording(|r| {
        r.lines
            .iter()
            .take(reply_seq)
            .find(|l| l.account_id() == Some(&id))
            .map(|l| short(&l.raw))
    });
    if early.is_none() {
        ctx.pass(
            "account.events_before_reply",
            "no event for the account preceded the reply",
        );
    } else {
        ctx.fail(
            "account.events_before_reply",
            format!(
                "event for {id} before account.start reply: {}",
                early.unwrap_or_default()
            ),
        );
    }

    // First status.
    let reply_at = ctx.session().with_recording(|r| r.lines[reply_seq].at);
    let first = ctx
        .session()
        .wait_event(reply_seq, Duration::from_secs(2), "status", Some(&id))
        .await;
    match first {
        Some(l) => {
            let delay = l.at.saturating_duration_since(reply_at);
            let state = status_state(&l, &id).unwrap_or("?").to_owned();
            if delay <= Duration::from_secs(1) {
                ctx.pass(
                    "account.first_status",
                    format!("status {state} after {delay:.0?}"),
                );
            } else {
                ctx.skip(
                    "account.first_status",
                    format!("status {state} after {delay:.0?}; the one-second target is advisory"),
                );
            }
        }
        None => ctx.skip(
            "account.first_status",
            "no status observed within the advisory interval",
        ),
    }

    // Settled state.
    let timeout = ctx.opts.state_timeout;
    let settled = ctx
        .session()
        .wait_for(reply_seq, timeout, |l| {
            status_state(l, &id).is_some_and(|s| SETTLED.contains(&s))
        })
        .await;
    match settled {
        Some(l) => {
            let state = status_state(&l, &id).unwrap_or("?").to_owned();
            let detail = l
                .event()
                .and_then(|(_, p)| p.get("detail"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned();
            ctx.account.state = Some(state.clone());
            let msg = if detail.is_empty() {
                state.clone()
            } else {
                format!("{state}: {detail}")
            };
            ctx.pass("account.settled", msg);
        }
        None => {
            let last = ctx.session().with_recording(|r| {
                r.lines
                    .iter()
                    .rev()
                    .find_map(|l| status_state(l, &id).map(String::from))
            });
            ctx.skip(
                "account.settled",
                format!(
                    "no connected/auth_required/unavailable/error status within {timeout:?} (last status: {}); pass --state-timeout to wait longer",
                    last.as_deref().unwrap_or("none")
                ),
            );
        }
    }
}

fn not_started(ctx: &mut Ctx<'_>, why: &str) {
    for id in [
        "account.events_before_reply",
        "account.first_status",
        "account.settled",
    ] {
        ctx.skip(id, why);
    }
}

/// Interactive sign-in, when the account asked for it (§7).
pub async fn auth(ctx: &mut Ctx<'_>) {
    let id = ctx.account.id.clone();
    let state = ctx.account.state.clone();
    let has_auth = ctx.has_cap("auth.interactive");
    let methods = ctx.manifest().auth_methods.clone();
    let only_none = methods == [AuthMethod::None];

    // Consistency between the manifest, the capability and the reported state.
    if !ctx.account.started {
        ctx.skip("auth.consistency", "no started account");
        ctx.skip("auth.flow", "no started account");
        return;
    }
    let mut problems = vec![];
    if has_auth && only_none {
        problems.push("auth.interactive is declared but auth_methods is [\"none\"]".to_owned());
    }
    if state.as_deref() == Some("auth_required") {
        if only_none {
            problems.push(
                "the account reported auth_required although auth_methods is [\"none\"]".to_owned(),
            );
        }
        if !has_auth {
            problems.push("the account reported auth_required but the plugin does not declare auth.interactive, so the core could not sign it in".to_owned());
        }
    }
    if problems.is_empty() {
        ctx.pass(
            "auth.consistency",
            format!(
                "auth_methods {:?}, auth.interactive {}, state {}",
                methods.iter().map(|m| m.as_str()).collect::<Vec<_>>(),
                if has_auth { "declared" } else { "not declared" },
                state.as_deref().unwrap_or("?")
            ),
        );
    } else {
        ctx.fail("auth.consistency", problems.join("\n"));
    }

    if state.as_deref() != Some("auth_required") {
        ctx.skip(
            "auth.flow",
            format!(
                "the account did not ask for sign-in (state {})",
                state.as_deref().unwrap_or("?")
            ),
        );
        return;
    }
    if !ctx.opts.fixture {
        ctx.skip(
            "auth.flow",
            "interactive sign-in needs --fixture-account with an offline fake or recording",
        );
        return;
    }
    if !has_auth {
        ctx.skip("auth.flow", "auth.interactive is not declared");
        return;
    }
    // Pick a method the suite can drive.
    const DRIVABLE: &[&str] = &[
        "phone_code",
        "username_password",
        "token",
        "email_code",
        "password_2fa",
    ];
    let method = DRIVABLE
        .iter()
        .find(|m| methods.iter().any(|x| x.as_str() == **m))
        .map(|m| (*m).to_owned())
        .or_else(|| {
            methods
                .iter()
                .map(|m| m.as_str().to_owned())
                .find(|m| !matches!(m.as_str(), "qr" | "oauth" | "none"))
        });
    let Some(method) = method else {
        ctx.skip(
            "auth.flow",
            format!(
                "sign-in methods {:?} need a QR scan or a browser; the suite cannot drive them",
                methods.iter().map(|m| m.as_str()).collect::<Vec<_>>()
            ),
        );
        return;
    };

    let from = ctx.session().cursor();
    let r = ctx
        .session()
        .call(
            "auth.begin",
            Some(json!({"account_id": id, "method": method})),
            Ctx::timeout_for("auth.begin"),
        )
        .await;
    if !matches!(&r.response, Some(Ok(res)) if res.is_object()) {
        ctx.fail(
            "auth.flow",
            format!("auth.begin({method}): {}", r.describe()),
        );
        return;
    }
    let mut cursor = from;
    let mut log = vec![format!("auth.begin({method}) -> {{}}")];
    let mut answered: Vec<(String, String)> = vec![];
    for _ in 0..12 {
        let next = ctx
            .session()
            .wait_for(cursor, Duration::from_secs(10), |l| {
                l.account_id() == Some(&id)
                    && (l.event().is_some_and(|(n, _)| n == "auth.step")
                        || status_state(l, &id)
                            .is_some_and(|s| s != "auth_required" && s != "connecting"))
            })
            .await;
        let Some(l) = next else {
            ctx.fail(
                "auth.flow",
                format!(
                    "{}\nno auth.step or status event within 10 s",
                    log.join("\n")
                ),
            );
            return;
        };
        cursor = l.seq + 1;
        if let Some(state) = status_state(&l, &id) {
            if state == "connected" {
                ctx.account.state = Some("connected".into());
                log.push("status connected".into());
                ctx.pass("auth.flow", log.join("\n"));
            } else {
                ctx.fail(
                    "auth.flow",
                    format!("{}\nstatus {state} during sign-in", log.join("\n")),
                );
            }
            return;
        }
        let step = l
            .event()
            .map(|(_, p)| p.get("step").cloned().unwrap_or(Value::Null))
            .unwrap_or(Value::Null);
        let kind = step
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let flow = step
            .get("flow")
            .and_then(Value::as_str)
            .unwrap_or("sign_in")
            .to_owned();
        let step_id = step
            .get("step_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let error = step.get("error").and_then(Value::as_str).map(String::from);
        let title = step
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        match kind.as_str() {
            "phone" | "code" | "password" | "email" | "email_code" | "text" => {
                let value = ctx
                    .opts
                    .auth_answers
                    .get(&kind)
                    .cloned()
                    .unwrap_or_else(|| default_answer(&kind).to_owned());
                if let Some(e) = &error
                    && answered.iter().any(|(k, v)| *k == kind && *v == value)
                {
                    ctx.skip(
                            "auth.flow",
                            format!(
                                "{}\nthe plugin rejected the {kind} test answer: {e}. Pass --auth {kind}=<value> with a test credential the plugin accepts",
                                log.join("\n")
                            ),
                        );
                    return;
                }
                ctx.redactions.add(&value);
                let r = ctx
                    .session()
                    .call(
                        "auth.submit",
                        Some(json!({"account_id": id, "flow": flow, "step_id": step_id, "value": value})),
                        Ctx::timeout_for("auth.submit"),
                    )
                    .await;
                log.push(format!(
                    "step {kind} {title:?}{} -> auth.submit(<redacted>) -> {}",
                    error.map(|e| format!(" error={e:?}")).unwrap_or_default(),
                    r.describe()
                ));
                if !matches!(&r.response, Some(Ok(res)) if res.is_object()) {
                    ctx.fail("auth.flow", format!("{}\nauth.submit must answer {{}} (a wrong value is reported by re-emitting the step with error)", log.join("\n")));
                    return;
                }
                answered.push((kind, value));
            }
            "info" => log.push(format!("step info {title:?}")),
            "done" => {
                log.push("step done".into());
                let timeout = ctx.opts.state_timeout;
                let connected = ctx
                    .session()
                    .wait_for(cursor, timeout, |l| {
                        status_state(l, &id) == Some("connected")
                    })
                    .await;
                if connected.is_some() {
                    ctx.account.state = Some("connected".into());
                    log.push("status connected".into());
                    ctx.pass("auth.flow", log.join("\n"));
                } else {
                    ctx.fail(
                        "auth.flow",
                        format!(
                            "{}\nno status connected after the done step",
                            log.join("\n")
                        ),
                    );
                }
                return;
            }
            "qr" => {
                ctx.skip(
                    "auth.flow",
                    format!(
                        "{}\nstep qr: the suite cannot scan a QR code",
                        log.join("\n")
                    ),
                );
                return;
            }
            other => {
                ctx.fail(
                    "auth.flow",
                    format!("{}\nunknown step kind {other:?}", log.join("\n")),
                );
                return;
            }
        }
    }
    ctx.fail(
        "auth.flow",
        format!(
            "{}\nmore than 12 steps without reaching connected",
            log.join("\n")
        ),
    );
}

fn default_answer(kind: &str) -> &'static str {
    match kind {
        "phone" => "+15550100",
        "code" | "email_code" => "12345",
        "password" => "fake",
        "email" => "conformance@example.org",
        _ => "conformance",
    }
}

/// Duplicate start, unknown account, stop of an unknown account.
pub async fn edge_cases(ctx: &mut Ctx<'_>) {
    if !ctx.account.started {
        for id in [
            "account.duplicate_start",
            "account.unknown_request",
            "account.stop_unknown",
        ] {
            ctx.skip(id, "no started account");
        }
        return;
    }
    let id = ctx.account.id.clone();
    let params = ctx.start_params(&id);
    let from = ctx.session().cursor();
    let r = ctx
        .session()
        .call(
            "account.start",
            Some(params),
            Ctx::timeout_for("account.start"),
        )
        .await;
    let field = match &r.response {
        Some(Err(e)) => e
            .get("data")
            .and_then(|d| d.get("field"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
        _ => String::new(),
    };
    let disconnected = ctx
        .session()
        .wait_for(from, Duration::from_millis(400), |l| {
            status_state(l, &id) == Some("disconnected")
        })
        .await;
    let ok = r.is_code("INVALID_PARAMS") && field == "account_id" && disconnected.is_none();
    ctx.verdict(
        "account.duplicate_start",
        ok,
        format!(
            "second account.start: {}{}{}",
            r.describe(),
            if r.is_code("INVALID_PARAMS") && field != "account_id" {
                format!(" (data.field is {field:?}, expected \"account_id\")")
            } else {
                String::new()
            },
            if disconnected.is_some() {
                "; the running account was disconnected"
            } else {
                ""
            }
        ),
    );

    let r = ctx
        .session()
        .call(
            "messages.fetch_recent",
            Some(json!({"account_id": "ch-conformance-unknown-account", "chat_id": crate::synth::MISSING_ID, "limit": 1})),
            Duration::from_secs(10),
        )
        .await;
    ctx.verdict(
        "account.unknown_request",
        r.is_code("UNKNOWN_ACCOUNT") && r.numeric() == Some(-32008),
        format!(
            "messages.fetch_recent for an unknown account: {}",
            r.describe()
        ),
    );

    let r = ctx
        .session()
        .call(
            "account.stop",
            Some(json!({"account_id": "ch-conformance-unknown-account"})),
            Duration::from_secs(10),
        )
        .await;
    ctx.verdict(
        "account.stop_unknown",
        r.is_code("UNKNOWN_ACCOUNT") && r.numeric() == Some(-32008),
        format!("account.stop for an unknown account: {}", r.describe()),
    );
}

/// A second account on a multi-account plugin (§5).
pub async fn multi(ctx: &mut Ctx<'_>) {
    if !ctx.manifest().multi_account {
        ctx.skip("account.multi", "multi_account is false");
        return;
    }
    if !ctx.account.started {
        ctx.skip("account.multi", "no started account");
        return;
    }
    let b = format!("{}-second", ctx.account.id);
    let params = ctx.start_params(&b);
    let r = ctx
        .session()
        .call(
            "account.start",
            Some(params),
            Ctx::timeout_for("account.start"),
        )
        .await;
    let Some(start_seq) = r
        .seq
        .filter(|_| matches!(&r.response, Some(Ok(res)) if res.is_object()))
    else {
        ctx.fail(
            "account.multi",
            format!("account.start({b}): {}", r.describe()),
        );
        return;
    };
    let status = ctx
        .session()
        .wait_event(start_seq, Duration::from_secs(3), "status", Some(&b))
        .await;
    let status_state_b = status
        .as_ref()
        .and_then(|l| status_state(l, &b).map(String::from));
    let stop_from = ctx.session().cursor();
    let r = ctx
        .session()
        .call(
            "account.stop",
            Some(json!({"account_id": b})),
            Ctx::timeout_for("account.stop"),
        )
        .await;
    let stop_seq = r.seq;
    ctx.lifecycles.push((b.clone(), start_seq, stop_seq));
    let stopped = matches!(&r.response, Some(Ok(res)) if res.is_object());
    let disconnected_before = stop_seq.is_some_and(|s| {
        ctx.session().with_recording(|r| {
            r.lines.iter().any(|l| {
                l.seq >= stop_from && l.seq < s && status_state(l, &b) == Some("disconnected")
            })
        })
    });
    let ok = status_state_b.is_some() && stopped;
    ctx.verdict(
        "account.multi",
        ok,
        format!(
            "second account {b}: start {{}}; first status {}; stop {}; disconnected before the stop reply: {}",
            status_state_b.as_deref().unwrap_or("MISSING within 3 s"),
            r.describe(),
            if disconnected_before { "yes" } else { "no" }
        ),
    );
}

/// `account.stop` with `disconnected` first, then a restart of the same id.
pub async fn stop_and_restart(ctx: &mut Ctx<'_>) {
    if !ctx.account.started {
        ctx.skip("account.stop", "no started account");
        ctx.skip("account.restart", "no started account");
        return;
    }
    let id = ctx.account.id.clone();
    let from = ctx.session().cursor();
    let r = ctx
        .session()
        .call(
            "account.stop",
            Some(json!({"account_id": id})),
            Ctx::timeout_for("account.stop"),
        )
        .await;
    let stop_seq = r.seq;
    if let Some(l) = ctx
        .lifecycles
        .iter_mut()
        .find(|l| l.0 == id && l.2.is_none())
    {
        l.2 = stop_seq;
    }
    ctx.account.started = false;
    let stopped = matches!(&r.response, Some(Ok(res)) if res.is_object());
    let disconnected_before = stop_seq.is_some_and(|s| {
        ctx.session().with_recording(|r| {
            r.lines
                .iter()
                .any(|l| l.seq >= from && l.seq < s && status_state(l, &id) == Some("disconnected"))
        })
    });
    if !stopped {
        ctx.fail("account.stop", format!("account.stop: {}", r.describe()));
        ctx.skip("account.restart", "account.stop failed");
        return;
    }
    ctx.verdict(
        "account.stop",
        stopped,
        if disconnected_before {
            format!("status disconnected, then {{}} in {:.0?}", r.elapsed)
        } else {
            "answered {} without a preceding status: disconnected for the account".to_owned()
        },
    );

    // Restart.
    let params = ctx.start_params(&id);
    let r = ctx
        .session()
        .call(
            "account.start",
            Some(params),
            Ctx::timeout_for("account.start"),
        )
        .await;
    let Some(start_seq) = r
        .seq
        .filter(|_| matches!(&r.response, Some(Ok(res)) if res.is_object()))
    else {
        ctx.fail(
            "account.restart",
            format!("account.start after a stop: {}", r.describe()),
        );
        return;
    };
    let status = ctx
        .session()
        .wait_event(start_seq, Duration::from_secs(3), "status", Some(&id))
        .await;
    let state = status
        .as_ref()
        .and_then(|l| status_state(l, &id).map(String::from));
    let r2 = ctx
        .session()
        .call(
            "account.stop",
            Some(json!({"account_id": id})),
            Ctx::timeout_for("account.stop"),
        )
        .await;
    ctx.lifecycles.push((id.clone(), start_seq, r2.seq));
    let ok = state.is_some() && matches!(&r2.response, Some(Ok(res)) if res.is_object());
    ctx.verdict(
        "account.restart",
        ok,
        format!(
            "restart: start {{}}, first status {}, stop {}",
            state.as_deref().unwrap_or("MISSING within 3 s"),
            r2.describe()
        ),
    );
}
