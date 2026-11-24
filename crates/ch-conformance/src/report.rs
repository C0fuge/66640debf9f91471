//! The structured result of a run: one [`CheckResult`] per check, with the spec section it
//! comes from and a concrete message.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// How a check ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    /// The plugin does what the spec says.
    Pass,
    /// It does not; `message` says what was observed.
    Fail,
    /// Could not be run (no account reached `connected`, nothing to check, …); `message`
    /// says why. Never counts as a failure.
    Skip,
}

/// One check's result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    /// Stable id (`account.stop`, `cap.send.file`), usable with `--only`.
    pub id: String,
    /// One-line description of what is checked.
    pub title: String,
    /// The spec section (`§4.2`) the rule comes from.
    pub spec: String,
    /// Pass, fail or skip.
    pub outcome: Outcome,
    /// What was observed, concretely.
    pub message: String,
}

/// A complete run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    /// The plugin directory that was tested.
    pub plugin_dir: PathBuf,
    /// The manifest id, when the manifest could be read.
    pub plugin_id: Option<String>,
    /// The API version this suite implements.
    pub suite_api_version: String,
    /// Every check, in execution order.
    pub checks: Vec<CheckResult>,
    /// The last lines of the plugin's stderr, for context.
    pub stderr_tail: String,
}

impl Report {
    /// `true` when no check failed.
    pub fn ok(&self) -> bool {
        !self.checks.iter().any(|c| c.outcome == Outcome::Fail)
    }

    /// Counts of (passed, failed, skipped).
    pub fn counts(&self) -> (usize, usize, usize) {
        let n = |o| self.checks.iter().filter(|c| c.outcome == o).count();
        (n(Outcome::Pass), n(Outcome::Fail), n(Outcome::Skip))
    }

    /// Results with a given outcome.
    pub fn with_outcome(&self, outcome: Outcome) -> impl Iterator<Item = &CheckResult> {
        self.checks.iter().filter(move |c| c.outcome == outcome)
    }

    /// The result of one check by id.
    pub fn check(&self, id: &str) -> Option<&CheckResult> {
        self.checks.iter().find(|c| c.id == id)
    }

    /// Human-readable rendering. `verbose` also lists passed checks.
    pub fn render(&self, verbose: bool) -> String {
        let mut out = String::new();
        let id = self.plugin_id.as_deref().unwrap_or("?");
        out.push_str(&format!(
            "Contact Hub plugin conformance (API {}): {} ({})\n\n",
            self.suite_api_version,
            id,
            self.plugin_dir.display()
        ));
        let width = self.checks.iter().map(|c| c.id.len()).max().unwrap_or(0);
        for c in &self.checks {
            if c.outcome == Outcome::Pass && !verbose {
                continue;
            }
            let tag = match c.outcome {
                Outcome::Pass => "PASS",
                Outcome::Fail => "FAIL",
                Outcome::Skip => "SKIP",
            };
            out.push_str(&format!(
                "{tag} {:<width$}  {} [{}]\n",
                c.id, c.title, c.spec
            ));
            if !c.message.is_empty() && (c.outcome != Outcome::Pass || verbose) {
                for line in c.message.lines() {
                    out.push_str(&format!("     {:<width$}  {line}\n", ""));
                }
            }
        }
        let (p, f, s) = self.counts();
        out.push_str(&format!(
            "\n{p} passed, {f} failed, {s} skipped ({} checks)\n",
            self.checks.len()
        ));
        if f == 0 {
            out.push_str("Result: no failures in executed checks\n");
        } else {
            out.push_str("Result: NOT conformant\n");
        }
        if !self.stderr_tail.is_empty() && (f > 0 || verbose) {
            out.push_str("\nplugin stderr (tail):\n");
            for line in self.stderr_tail.lines() {
                out.push_str(&format!("  | {line}\n"));
            }
        }
        out
    }
}
