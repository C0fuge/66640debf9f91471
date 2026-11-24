# Source mirror and build checkpoint

This is a clean source snapshot without private Git history. It combines the
Rust/Tauri text demo, rich text, generic account settings/auth UI, and partial
Telegram/MAX foundations and typed message actions. No real database, messenger login, .env or Anvil transcript
is present.

Development runs locally. GitHub Actions provides builds and tests only, with no
API key, ChatGPT OAuth credentials, cloud coding job or publishing agent. Reports
and packaged synthetic-demo artifacts are retained for later inspection.

Latest verified source snapshot: `7b949fc`, [Verify run45493731509](https://github.com/C0fuge/66640debf9f91471/actions/runs/45493731509), 2026-11-25.

- Workspace Rust189 passed; core without native keyring113; Clippy passed.
- UI28; Python SDK48; fake8; MAX18; Telegram44 offline tests passed.
- Actual headless Chromium account and message-action scenarios passed.
- Fake integrations and CLI conformance passed (144 passed, zero failed,71 skipped).
- TypeScript/Vite and Linux desktop/CLI development builds passed.

These counts describe that source snapshot; check the workflow for any later
source revision. Telegram implements27 core-to-plugin operations; the registry
contains119 such operations and2 host services. Full Telegram parity is
incomplete. MAX has a secure partial auth/session/text foundation; required
history continuation is explicitly unsupported. WhatsApp is pending.

The next local batch adds attachment metadata to the app view and researches
safe MAX chat continuation. Full platform API evidence and remaining contract
gaps are described in platform-coverage.md and api-contract-gaps.md.
.github/cloud-tasks.md is a development backlog, not an automated agent
schedule. Real legacy import and deletion of the old app remain a later verified
stage outside this public source repository.
