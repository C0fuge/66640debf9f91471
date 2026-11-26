# Owner resumed

Development resumed on explicit owner instruction. Verify is active again.
Development runs locally; Actions builds/tests only. The verified source below
remains the baseline. Next candidate is the frozen Telegram dialog pin/manual
unread subset; no new application operation has been approved yet.

# Verification handoff

Latest verified source: `db11338a2b54cb4bc820fe6b130925c5e8fb6ffb`.
[Successful workflow](https://github.com/C0fuge/66640debf9f91471/actions/runs/45807435282).
Artifacts contain logs, synthetic browser screenshots and Linux debug binaries.
These are build/test results, not a production deployment or live-account test.

`Verify` runs on code pushes, manual dispatch and a configured daily schedule at
00:23UTC (03:23 Moscow). Push/manual execution and scheduled run45580094701 were observed (the latter
failed on an older candidate). Verify was re-enabled after an explicit owner resume. Both jobs must be successful for the exact source
SHA. A short SHA is not a valid prefix filter for GitHub's `head_sha` API query;
resolve it to the full commit before matching a run. Do not infer completion from
an older green run or a green job whose peer job failed.

Rust gates include workspace/no-default tests, two explicit fake integrations,
an opt-in native portal test on a fresh private D-Bus session, Clippy, fake-plugin
conformance and desktop/CLI builds. UI/Python gates include dependency-locked
suites and actual headless Chromium with synthetic account/action/media/poll/reaction data.

`Resolve dependency lock` is a separate manually triggered metadata-only workflow.
Its artifact is a proposed Cargo lockfile and source hash, not build verification.
Review exact dependency/version/feature changes before applying it.

This public mirror accepts only reviewed sanitized source snapshots. Never push
private history, account databases, messenger sessions, environment files, model
credentials or subscription authentication. Actions builds/tests require no model
API key. No cloud coding/model session is running or scheduled here.

The verified app now has34 typed operations, including explicit single-file
selection/review/admission, scoped recovery and independent copy/upload progress.
Telegram remains39/119 methods. Retained-ID core admission is idempotent only
while the outbox row survives; absent rows never authorize replay. Keep the
16-file retention ceiling and pending reference-safe reclamation explicit.
Migration, real-account and actual desktop chooser verification remain pending.
