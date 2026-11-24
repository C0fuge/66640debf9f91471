use super::Ctx;
use crate::session::{Session, frame};
use serde_json::json;
use std::time::{Duration, Instant};

/// Separate processes keep EOF independent of the normal shutdown checks.
pub async fn run(ctx: &mut Ctx<'_>) {
    for (id, inflight) in [("eof.exits", false), ("eof.inflight_answered", true)] {
        let mut s = match Session::spawn(
            &ctx.dir,
            ctx.manifest(),
            ctx.exe.as_ref().unwrap(),
            &ctx.isolate,
            ctx.redactions.clone(),
            ctx.capture.clone(),
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                ctx.fail(id, format!("could not start another process: {e}"));
                continue;
            }
        };
        let grace = Duration::from_secs(ch_protocol::EXIT_GRACE_SECONDS);
        if inflight {
            let params = json!({"api_version":ch_protocol::API_VERSION, "core_version":"conformance",
                "plugin_dir":ctx.dir,"locale":"en","log_level":"warn"});
            let r = s
                .call("initialize", Some(params), Ctx::timeout_for("initialize"))
                .await;
            if !matches!(r.response, Some(Ok(_))) {
                ctx.fail(id, format!("initialize: {}", r.describe()));
                s.kill().await;
                continue;
            }
            let from = s.cursor();
            let at = Instant::now();
            s.write_raw(&frame(
                &json!({"jsonrpc":"2.0","id":"exit","method":"shutdown"}),
            ))
            .await;
            s.close_stdin().await;
            let reply = s.wait_reply(json!("exit"), from, at, grace).await;
            let answered = matches!(reply.response, Some(Ok(r)) if r.is_object());
            let status = s.wait_exit(grace.saturating_sub(at.elapsed())).await;
            ctx.verdict(
                id,
                answered && status.is_some_and(|st| st.success()),
                format!("shutdown answered: {answered}, exit: {status:?}"),
            );
        } else {
            s.close_stdin().await;
            let status = s.wait_exit(grace).await;
            ctx.verdict(
                id,
                status.is_some_and(|st| st.success()),
                format!("exit: {status:?}"),
            );
        }
        s.kill().await;
    }
}
