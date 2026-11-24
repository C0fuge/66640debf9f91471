use std::collections::BTreeSet;
use std::time::Duration;

use super::Ctx;
use crate::report::Outcome;
use crate::session::{Kind, Reply};
use crate::synth::{Hints, params_for, set_gated};
use serde_json::{Value, json};

/// An operational error cannot demonstrate a working feature. Missing handlers are failures.
fn observed(reply: &Reply) -> Outcome {
    match &reply.response {
        None => Outcome::Fail,
        Some(Ok(_)) => Outcome::Pass,
        Some(Err(_)) if reply.is_code("METHOD_NOT_FOUND") || reply.is_code("NOT_SUPPORTED") => {
            Outcome::Fail
        }
        Some(Err(_)) => Outcome::Skip,
    }
}

pub async fn run(ctx: &mut Ctx<'_>) {
    let fixture = ctx.opts.fixture && ctx.account.state.as_deref() == Some("connected");
    let rows = ctx.registry_methods();
    let account_id = ctx.account.id.clone();
    let chat = ctx.session().with_recording(|r| {
        r.lines.iter().find_map(|l| {
            let (event, p) = l.event()?;
            if l.account_id() != Some(&account_id) {
                return None;
            }
            match event {
                "conversations.upsert" => p.pointer("/conversations/0/id"),
                "conversations.synced" => p.pointer("/chat_ids/0"),
                _ => None,
            }
            .and_then(Value::as_str)
            .map(str::to_owned)
        })
    });
    let hints = Hints {
        account_id: ctx.account.id.clone(),
        chat_id: None,
        file_path: None,
    };
    if fixture && ctx.has_cap("chat.sync") {
        repeat_sync(ctx).await;
    }
    let mut disabled = 0;
    let mut bad_disabled = Vec::new();
    for (name, row) in rows {
        if row["direction"] != "core_to_plugin" || row["schema"] == "lifecycle.json" {
            continue;
        }
        let file = row["schema"].as_str().unwrap();
        let cap = row["capability"].as_str().unwrap();
        let id = format!("method.{name}");
        if !fixture {
            ctx.record_dyn(
                &id,
                &name,
                "§14.2",
                Outcome::Skip,
                "requires a connected offline --fixture-account",
            );
            continue;
        }
        let mut hints = hints.clone();
        if matches!(name.as_str(), "messages.fetch_recent" | "messages.send") {
            hints.chat_id = chat.clone();
        }
        let Some(mut params) = params_for(ctx.schemas, &name, file, &hints) else {
            ctx.record_dyn(&id, &name, "§11", Outcome::Fail, "request schema missing");
            continue;
        };
        let info = ch_protocol::method(&name).unwrap();
        let enabled = cap == "required" || ctx.has_cap(cap);
        let is_auth = name.starts_with("auth.");
        if enabled && is_auth {
            // Re-opening sign-in would change the account used by the remaining checks.
            let replies = ctx.session().with_recording(|r| {
                r.lines.iter().any(|line| {
                    let crate::session::Kind::Success { id, .. } = &line.kind else {
                        return false;
                    };
                    r.sent
                        .get(&id.to_string())
                        .is_some_and(|s| s.method == name)
                })
            });
            let outcome = if replies {
                Outcome::Pass
            } else {
                Outcome::Skip
            };
            ctx.record_dyn(
                &id,
                &name,
                "§7",
                outcome,
                if replies {
                    "called during the sign-in flow (see auth.flow)"
                } else {
                    "not used by this fixture's sign-in flow"
                },
            );
            continue;
        }
        if enabled && name == "account.sign_out" {
            ctx.record_dyn(
                &id,
                &name,
                "§11",
                Outcome::Skip,
                "sign-out would invalidate the restart fixture",
            );
            continue;
        }
        // For an enabled method choose a base shape without a missing parameter capability.
        let first_gate = info
            .missing_capabilities(&params, |c| ctx.has_cap(c))
            .next()
            .copied();
        if let Some(gate) = first_gate
            && let Some(eq) = gate.equals
            && let ch_protocol::GatedValue::Bool(v) = eq
        {
            params[gate.param] = json!(!v);
        }
        let param_ref = format!("methods/{file}#/$defs/{name}.params");
        let errs = ctx.schemas.validate(&param_ref, &params);
        if !errs.is_empty() {
            ctx.record_dyn(
                &id,
                &name,
                "§11",
                Outcome::Fail,
                format!("invalid suite probe: {}", errs.join(", ")),
            );
            continue;
        }
        let r = ctx
            .session()
            .call(&name, Some(params.clone()), Ctx::timeout_for(&name))
            .await;
        let outcome = if enabled {
            observed(&r)
        } else {
            disabled += 1;
            if r.is_code("NOT_SUPPORTED") {
                Outcome::Pass
            } else {
                bad_disabled.push(name.clone());
                Outcome::Fail
            }
        };
        ctx.record_dyn(&id, &name, "§14.2", outcome, r.describe());
        if name == "messages.fetch_recent" {
            ctx.record("methods.fetch_recent", outcome, r.describe());
            repeat_page(ctx, &params, &r, chat.is_some()).await;
        } else if name == "messages.send" {
            ctx.record("methods.send", outcome, r.describe());
        }
        if !enabled {
            continue;
        }
        for g in row["gated_params"].as_array().into_iter().flatten() {
            let path = g["param"].as_str().unwrap();
            let capability = g["capability"].as_str().unwrap();
            let id = format!("gated.{name}.{path}.{capability}");
            let mut p = params.clone();
            if !set_gated(
                ctx.schemas,
                &name,
                file,
                &mut p,
                path,
                g.get("equals").cloned(),
                &hints,
            ) || !ctx.schemas.validate(&param_ref, &p).is_empty()
            {
                ctx.record_dyn(
                    &id,
                    path,
                    "§14.2",
                    Outcome::Fail,
                    "suite could not generate a valid probe",
                );
                continue;
            }
            let result = ctx
                .session()
                .call(&name, Some(p.clone()), Ctx::timeout_for(&name))
                .await;
            let missing: Vec<_> = info.missing_capabilities(&p, |c| ctx.has_cap(c)).collect();
            let outcome = if missing.is_empty() {
                observed(&result)
            } else {
                let label = match &result.response {
                    Some(Err(e)) => e.pointer("/data/capability").and_then(Value::as_str),
                    _ => None,
                };
                if result.is_code("NOT_SUPPORTED")
                    && missing.iter().any(|g| Some(g.capability) == label)
                {
                    Outcome::Pass
                } else {
                    Outcome::Fail
                }
            };
            ctx.record_dyn(&id, path, "§14.2", outcome, result.describe());
        }
    }
    if disabled == 0 {
        ctx.skip(
            "cap.undeclared",
            "no undeclared-capability method exercised",
        );
    } else {
        ctx.verdict(
            "cap.undeclared",
            bad_disabled.is_empty(),
            format!("{disabled} probes, incorrect answers: {bad_disabled:?}"),
        );
    }
    ctx.skip(
        "partial.merge_defaults",
        "a black-box frame cannot establish which optional fields its producer intended to change",
    );
    cancel(ctx).await;
}

async fn repeat_page(ctx: &mut Ctx<'_>, params: &Value, first: &Reply, known_chat: bool) {
    let id = "methods.fetch_recent_idempotent";
    let Some(Ok(page)) = &first.response else {
        ctx.skip(id, "first fetch did not produce a page");
        return;
    };
    let ids = |v: &Value| -> Option<Vec<String>> {
        v.get("messages")?
            .as_array()?
            .iter()
            .map(|m| m["id"].as_str().map(String::from))
            .collect()
    };
    let Some(a) = ids(page).filter(|v| !v.is_empty() && known_chat) else {
        ctx.skip(id, "no known chat with a non-empty replayable page");
        return;
    };
    let r = ctx
        .session()
        .call(
            "messages.fetch_recent",
            Some(params.clone()),
            Ctx::timeout_for("messages.fetch_recent"),
        )
        .await;
    let b = r
        .response
        .as_ref()
        .and_then(|r| r.as_ref().ok())
        .and_then(ids);
    let unique = a.iter().collect::<BTreeSet<_>>().len() == a.len();
    ctx.verdict(id, unique && b.as_ref() == Some(&a), format!("first page {} message ids, second page {:?}; repeated static fixture expects identical ordered ids", a.len(), b.map(|v| v.len())));
}

async fn cancel(ctx: &mut Ctx<'_>) {
    let from = ctx.session().cursor();
    let unknown_id = json!("unknown-cancel-id");
    let sent = ctx
        .session()
        .notify(ch_protocol::CANCEL_METHOD, Some(json!({"id":unknown_id})))
        .await;
    let ping = ctx
        .session()
        .call("ping", None, Ctx::timeout_for("ping"))
        .await;
    let responsive = matches!(ping.response, Some(Ok(_)));
    // A ping is a harmless real request that may race the cancellation.
    let r = ctx
        .session()
        .call_and_cancel("ping", Ctx::timeout_for("ping"))
        .await;
    let answered = matches!(&r.response, Some(Ok(_))) || r.is_code("CANCELLED");
    ctx.verdict("cancel.honoured", answered, r.describe());
    let stray = ctx.session().with_recording(|r| {
        r.lines.iter().skip(from).any(|l|
            matches!(&l.kind, Kind::Success { id, .. } | Kind::Error { id, .. } if *id == unknown_id)
        )
    });
    ctx.verdict(
        "cancel.unknown_ignored",
        sent && responsive && !stray,
        format!("unknown cancellation sent: {sent}, responsive: {responsive}, unsolicited reply: {stray}"),
    );
}

async fn repeat_sync(ctx: &mut Ctx<'_>) {
    let id = "sync.conversation_ids";
    let account = ctx.account.id.clone();
    let mut previous = None;
    for _ in 0..2 {
        let from = ctx.session().cursor();
        let r = ctx
            .session()
            .call(
                "chats.sync",
                Some(json!({"account_id":account})),
                Ctx::timeout_for("chats.sync"),
            )
            .await;
        if !matches!(r.response, Some(Ok(_))) {
            ctx.record(id, observed(&r), r.describe());
            return;
        }
        let event = ctx
            .session()
            .wait_event(
                from,
                Duration::from_secs(5),
                "conversations.synced",
                Some(&account),
            )
            .await;
        let Some(event) = event else {
            ctx.skip(
                id,
                "no completed sync snapshot observed within five seconds",
            );
            return;
        };
        let ids = event
            .event()
            .and_then(|(_, p)| p["chat_ids"].as_array())
            .and_then(|a| {
                a.iter()
                    .map(|v| v.as_str().map(String::from))
                    .collect::<Option<Vec<_>>>()
            });
        let Some(ids) = ids else {
            ctx.fail(id, "snapshot contains invalid chat ids");
            return;
        };
        let count = ids.len();
        let unique: BTreeSet<_> = ids.into_iter().collect();
        if unique.len() != count {
            ctx.fail(id, "duplicate chat ids in completed snapshot");
            return;
        }
        if let Some(prev) = previous {
            if unique.is_empty() && unique == prev {
                ctx.skip(id, "both snapshots were empty");
            } else {
                ctx.verdict(id, unique == prev, format!("compared two completed snapshots for a stationary fixture ({count} ids on second sync)"));
            }
            return;
        }
        previous = Some(unique);
    }
}
