# Cloud continuation checkpoint

This public repository is a clean source snapshot without private Git history.
It combines the verified Rust/Tauri text demo, rich text and generic account
settings/auth UI, and an unfinished Telegram foundation. It contains no user
database, messenger login, .env or Anvil transcript.

Verified local baseline before publication:
- Workspace Rust176 passed, core without native keyring108; Clippy passed.
- UI integration20 passed, TypeScript/Vite build passed.
- Python SDK41; Telegram foundation18 offline tests passed.
- Actual fake-plugin conformance and Python/Rust tutorial/template gates passed.
- Real browser text send, account switching and isolated drafts passed.
- Native Tauri window opened and closed normally.

These are historical results. The cloud workflows must report their own checks.
The Telegram foundation supports14 core-to-plugin operations;105 remain pending
in the existing121-method registry (the other2 are host secret-store services).
Full platform parity is not complete. MAX and WhatsApp have no plugin yet.

Use AGENTS.md and .github/cloud-tasks.md as current execution instructions.
Older orchestration/resume notes describe a laptop environment and are reference
material only. Follow docs/cloud-progress.md for the latest cloud state.

Cloud runs use an API key through the official Codex action proxy. A local
ChatGPT OAuth login is deliberately not copied into this public repository.
With no OPENAI_API_KEY secret, the coding job must say it is blocked; CI checks
can still run. No idle server, tunnel or continuous desktop is required.

The laptop checkout remains paused and unchanged. Never overwrite local branches
blindly when retrieving cloud/progress: fetch into a separate worktree and review.
