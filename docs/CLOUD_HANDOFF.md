# Verification handoff

Latest verified source: `2c0959483edb9edae6f4f54e27987cee640c7e99`.
[Successful workflow](https://github.com/C0fuge/66640debf9f91471/actions/runs/45575407031).
Artifacts contain logs, synthetic browser screenshots and Linux debug binaries.
These are build/test results, not a production deployment or live-account test.

`Verify` runs on code pushes, manual dispatch and a configured daily schedule at
00:23UTC (03:23 Moscow). Push execution was observed; scheduled execution has not
been independently confirmed. Both jobs must be successful for the exact source
SHA. A short SHA is not a valid prefix filter for GitHub's `head_sha` API query;
resolve it to the full commit before matching a run. Do not infer completion from
an older green run or a green job whose peer job failed.

Rust gates include workspace/no-default tests, two explicit fake integrations,
an opt-in native portal test on a fresh private D-Bus session, Clippy, fake-plugin
conformance and desktop/CLI builds. UI/Python gates include dependency-locked
suites and actual headless Chromium with synthetic account/action/media/poll data.

`Resolve dependency lock` is a separate manually triggered metadata-only workflow.
Its artifact is a proposed Cargo lockfile and source hash, not build verification.
Review exact dependency/version/feature changes before applying it.

This public mirror accepts only reviewed sanitized source snapshots. Never push
private history, account databases, messenger sessions, environment files, model
credentials or subscription authentication. Actions builds/tests require no model
API key. No cloud coding/model session is running or scheduled here.

Next work is local development of ADR0008's app reactions and a read-only Telegram
sticker catalog; neither candidate is included in the verified27-operation app.
Migration and live-account verification remain pending. Preserve the existing app
and its data until a separately verified switch-over.
