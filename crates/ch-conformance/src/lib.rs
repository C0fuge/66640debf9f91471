//! Exercise a plugin through its actual stdio entry point under a disposable profile.
mod capture;
mod checks;
mod redact;
mod report;
mod schemas;
mod session;
mod synth;

pub use report::{CheckResult, Outcome, Report};
use serde_json::{Map, Value};
use std::{collections::BTreeMap, path::Path, time::Duration};

pub struct Options {
    pub account_id: String,
    /// Enable platform method probes only on an explicitly supplied offline fixture.
    pub fixture: bool,
    pub state_timeout: Duration,
    pub settings: Map<String, Value>,
    pub secrets: Map<String, Value>,
    pub auth_answers: BTreeMap<String, String>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            fixture: false,
            account_id: "ch-conformance".into(),
            state_timeout: Duration::from_secs(15),
            settings: Map::new(),
            secrets: Map::new(),
            auth_answers: BTreeMap::new(),
        }
    }
}

pub async fn run(dir: &Path, options: &Options) -> std::io::Result<Report> {
    let tmp = tempfile::Builder::new()
        .prefix("ch-conformance-")
        .tempdir()?;
    let root = tmp.path().canonicalize()?;
    let mut ctx = checks::Ctx::new(dir, options, root);
    if options.account_id.is_empty()
        || options.account_id.len() > 249
        || options.account_id == "ch-conformance-unknown-account"
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid fixture account id (1–249 bytes, reserve space for the second account)",
        ));
    }
    let budget = Duration::from_secs(300).saturating_add(options.state_timeout);
    let mut capture_failure = ctx.capture.subscribe();
    let timeout = tokio::select! {
        biased;
        _ = capture_failure.changed() => false,
        run = tokio::time::timeout(budget, checks::run_all(&mut ctx)) => run.is_err(),
    };
    if let Some(reason) = ctx.capture.failure() {
        ctx.record_dyn(
            "suite.capture",
            "bounded stdout recording",
            "§2.2",
            Outcome::Fail,
            reason,
        );
    } else if timeout {
        ctx.record_dyn(
            "suite.timeout",
            "suite execution deadline",
            "§4.6",
            Outcome::Fail,
            format!("suite exceeded {budget:?}"),
        );
    }
    if let Some(mut s) = ctx.session.take() {
        s.kill().await;
        ctx.stderr_tail = checks::tail(&s.stderr_text(), 40);
    }
    ctx.skip_rest(
        "manifest.schema",
        "no applicable observation during this run",
    );
    for check in &mut ctx.results {
        check.message = ctx.redactions.clean(&check.message);
    }
    let stderr_tail = ctx.redactions.clean_log(&ctx.stderr_tail);
    Ok(Report {
        plugin_dir: ctx.dir,
        plugin_id: ctx.manifest.map(|m| ctx.redactions.clean(&m.id)),
        suite_api_version: ch_protocol::API_VERSION.to_owned(),
        checks: ctx.results,
        stderr_tail,
    })
}
