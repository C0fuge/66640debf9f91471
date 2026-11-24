//! Framing and envelope checks (§2.2, §2.3, §4.1).

use std::time::Duration;

use serde_json::{Value, json};

use super::Ctx;
use crate::session::{Kind, short};

/// Checks that run before `initialize` is sent.
pub async fn before_initialize(ctx: &mut Ctx<'_>) {
    // Nothing on stdout yet.
    let early = ctx
        .session()
        .wait_for(0, Duration::from_millis(300), |_| true)
        .await;
    match early {
        None => ctx.pass(
            "transport.silent_before_initialize",
            "stdout stayed empty for 300 ms",
        ),
        Some(l) => ctx.fail(
            "transport.silent_before_initialize",
            format!("stdout before initialize: {}", short(&l.raw)),
        ),
    }

    // A request before initialize.
    let r = ctx
        .session()
        .call("ping", None, Duration::from_secs(5))
        .await;
    ctx.verdict(
        "lifecycle.request_before_initialize",
        r.is_code("INVALID_REQUEST") && r.numeric() == Some(-32600),
        format!("ping before initialize: {}", r.describe()),
    );

    // A notification before initialize.
    let from = ctx.session().cursor();
    ctx.session()
        .notify(ch_protocol::CANCEL_METHOD, Some(json!({"id": 424242})))
        .await;
    let stray = ctx
        .session()
        .wait_for(from, Duration::from_millis(300), |_| true)
        .await;
    match stray {
        None => ctx.pass(
            "lifecycle.notification_before_initialize",
            "$/cancelRequest before initialize produced no frame",
        ),
        Some(l) => ctx.fail(
            "lifecycle.notification_before_initialize",
            format!("a frame was written in reply: {}", short(&l.raw)),
        ),
    }
}

/// Framing checks that need an initialized plugin.
pub async fn framing(ctx: &mut Ctx<'_>) {
    let ping_t = Duration::from_secs(10);

    // string id
    let id = json!("conformance-string-id");
    let r = ctx
        .session()
        .call_with_id(id.clone(), "ping", None, ping_t, b"\n")
        .await;
    ctx.verdict(
        "transport.string_id",
        matches!(&r.response, Some(Ok(_))),
        format!("ping with id {id}: {}", r.describe()),
    );

    // CRLF
    let r = ctx.session().call("ping", Some(json!({})), ping_t).await;
    let crlf_ok = matches!(&r.response, Some(Ok(_)));
    let id = json!(900_001);
    let r = ctx
        .session()
        .call_with_id(id, "ping", None, ping_t, b"\r\n")
        .await;
    ctx.verdict(
        "transport.crlf",
        crlf_ok && matches!(&r.response, Some(Ok(_))),
        format!("ping terminated by \\r\\n: {}", r.describe()),
    );

    // empty lines
    ctx.session().write_raw(b"\n\n   \n\r\n").await;
    let r = ctx.session().call("ping", None, ping_t).await;
    ctx.verdict(
        "transport.empty_lines",
        matches!(&r.response, Some(Ok(_))),
        format!("ping after blank lines: {}", r.describe()),
    );

    // batch array
    let from = ctx.session().cursor();
    ctx.session().write_raw(b"[]\n").await;
    let ans = ctx
        .session()
        .wait_for(from, Duration::from_secs(3), |l| {
            matches!(l.kind, Kind::Error { .. })
        })
        .await;
    match ans {
        Some(l) => {
            let (id_null, code) = match &l.kind {
                Kind::Error { id, error } => (
                    id.is_null(),
                    error
                        .get("data")
                        .and_then(|d| d.get("code"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned(),
                ),
                _ => (false, String::new()),
            };
            ctx.verdict(
                "transport.batch_rejected",
                id_null && code == "INVALID_REQUEST",
                format!(
                    "[] was answered with id {} and {}",
                    if id_null { "null" } else { "non-null" },
                    short(&l.raw)
                ),
            );
        }
        None => ctx.fail(
            "transport.batch_rejected",
            "a batch array frame produced no INVALID_REQUEST error within 3 s",
        ),
    }

    // parse error
    let from = ctx.session().cursor();
    ctx.session().write_raw(b"{this is not json\n").await;
    let ans = ctx
        .session()
        .wait_for(from, Duration::from_millis(700), |l| {
            matches!(l.kind, Kind::Error { .. })
        })
        .await;
    let r = ctx.session().call("ping", None, ping_t).await;
    let alive = matches!(&r.response, Some(Ok(_)));
    match ans {
        Some(l) => {
            let good = match &l.kind {
                Kind::Error { id, error } => {
                    id.is_null()
                        && error
                            .get("data")
                            .and_then(|d| d.get("code"))
                            .and_then(Value::as_str)
                            == Some("PARSE_ERROR")
                        && error.get("code").and_then(Value::as_i64) == Some(-32700)
                }
                _ => false,
            };
            ctx.verdict(
                "transport.parse_error",
                good && alive,
                format!(
                    "answered {}; ping afterwards: {}",
                    short(&l.raw),
                    r.describe()
                ),
            );
        }
        None => ctx.verdict(
            "transport.parse_error",
            alive,
            format!(
                "the bad line was ignored; ping afterwards: {}",
                r.describe()
            ),
        ),
    }
}
