# Source mirror and build checkpoint

This is a clean source snapshot without private Git history. It combines the
Rust/Tauri text demo, rich text, generic account settings/auth UI, and partial
Telegram/MAX foundations and typed message actions. No real database, messenger login, .env or Anvil transcript
is present.

Development runs locally. GitHub Actions provides builds and tests only, with no
API key, ChatGPT OAuth credentials, cloud coding job or publishing agent. Reports
and packaged synthetic-demo artifacts are retained for later inspection.

Latest verified source snapshot: `45343fc`, [Verify run45503307759](https://github.com/C0fuge/66640debf9f91471/actions/runs/45503307759), 2026-11-25.

- Workspace Rust199 passed; core without native keyring113; Clippy passed.
- UI39; Python SDK48; fake8; MAX45; Telegram60 offline tests passed.
- Actual headless Chromium account, message-action, attachment geometry/spoiler and poll scenarios passed.
- Fake integrations and CLI conformance passed (144 passed, zero failed,71 skipped).
- TypeScript/Vite and Linux desktop/CLI development builds passed.

These counts describe that source snapshot; check the workflow for any later
source revision. Telegram implements30 core-to-plugin operations; the registry
contains119 such operations and2 host services. Full Telegram parity is
incomplete. MAX has a secure partial auth/session/text foundation and bounded
chat continuation upserts; required history continuation is explicitly
unsupported. WhatsApp is pending. The app displays safe attachment metadata;
native media access and upload selection remain pending. The app supports stored
poll display, explicit vote/retract/close and targeted refresh (23 application
operations). Each fresh poll panel requires a scoped read before writes; unknown
choices remain unknown and lost writes are not automatically replayed.

Recommended next work reviews bounded media downloads/native cache access and
durable upload/poll-creation outbox designs, then remaining platform methods and
matching UI. Poll creation UI, full MAX history and platform parity remain
incomplete. Full platform API evidence and remaining contract
gaps are described in platform-coverage.md and api-contract-gaps.md.
.github/cloud-tasks.md is a development backlog, not an automated agent
schedule. Real legacy import and deletion of the old app remain a later verified
stage outside this public source repository.
