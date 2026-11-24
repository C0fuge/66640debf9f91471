# Source mirror and build checkpoint

This is a clean source snapshot without private Git history. It combines the
Rust/Tauri text demo, rich text, generic account settings/auth UI, and partial
Telegram foundation. No real database, messenger login, .env or Anvil transcript
is present.

Development runs locally. GitHub Actions provides builds and tests only, with no
API key, ChatGPT OAuth credentials, cloud coding job or publishing agent. Reports
and packaged synthetic-demo artifacts are retained for later inspection.

Historical local baseline before publication:
- Workspace Rust176 passed; core without native keyring108; Clippy passed.
- UI20 passed; TypeScript/Vite build passed.
- Python SDK41; Telegram foundation18 offline tests passed.
- Fake-plugin CLI conformance and tutorial/template gates passed.
- Browser send/account switching/drafts and native startup/close passed.

These counts do not prove the current cloud commit passed. Check its workflow.
Telegram supports14 core-to-plugin operations in the imported foundation; the
existing registry contains119 such operations and2 host services. Full Telegram
parity is incomplete; MAX and WhatsApp plugins are pending.

The next local batch continues Telegram actions/lifecycle and the full platform
API audit. .github/cloud-tasks.md is a development backlog, not an automated agent
schedule. Real legacy import and deletion of the old app remain a later verified
stage outside this public source repository.
