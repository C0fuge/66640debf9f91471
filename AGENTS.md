# Contact Hub cloud development

Read docs/CLOUD_HANDOFF.md, .github/cloud-tasks.md and docs/cloud-progress.md.
Work only on the first ready unfinished task. Finish a useful bounded item and
record evidence and the next step. A scheduled job is finite; do not wait in an
idle loop or provision unrelated services.

The app uses a Rust core, Tauri and React. Telegram/MAX are Python plugins;
WhatsApp is a later Rust plugin. Keep platform/account threads separate. Both
incoming and outgoing message bubbles are left-aligned. Accent selection changes
accent tokens only, never surfaces, backgrounds or bubble colours.

No live account access, real sessions, personal chat data, credentials, host data
or messenger network actions. Tests use temporary HOME/CH_DATA_DIR/XDG paths and
MemorySecretStore. Never read or print CI secrets. Never put tokens, auth.json,
.env files, databases, transcripts or generated caches in a patch or artifact.

Frozen interfaces: docs/plugin-api.md, schemas/**, crates/ch-protocol/**, DB
schema/migrations and app/ui/src/core/index.ts. If a feature needs an amendment,
write an evidence-backed proposal first and mark it for review. Do not alter
frozen contracts to get an isolated feature working.

Do not modify .github/**, tools/cloud/**, AGENTS.md, Git remotes, CI policy, or
repository permissions. Do not push, publish, send messages or create accounts.
The trusted publisher handles reviewed file paths after the agent exits.

Use precise primary-source citations for platform API research. A helper/library
method, declared capability or passing fake test does not prove live parity.
Keep unsupported, partially implemented and unverified behavior explicit.

Run checks appropriate to changed code; build only coherent runnable batches.
Do not repeatedly run full suites after every edit. Keep changes focused and
simple. Add tests for meaningful risky behavior, not implementation mirrors.
No extra agents unless a task explicitly requires them; the default is one worker.

Write a concise progress entry in docs/cloud-progress.md: changes, commands and
outcomes, limitations, next task. Never claim tests passed if they were not run.
