# Contact Hub development

Development runs locally. This public repository is a sanitized source mirror;
GitHub Actions runs only builds, tests and artifact packaging. No API key,
provider login, cloud coding job or Anvil transcript is part of CI.

Read docs/CLOUD_HANDOFF.md, .github/cloud-tasks.md and docs/cloud-progress.md for
the source checkpoint and remaining work. Do not claim a test passed from a
historical local note; use the workflow result for the tested commit.

Keep platform/account threads separate. Incoming and outgoing bubbles are both
left-aligned. An accent recolors accent tokens only, never backgrounds or bubbles.
Use synthetic data and temporary CH_DATA_DIR/HOME/XDG paths with MemorySecretStore.
No real account access, messenger sessions, .env, databases, credentials or host
keyring state may be published or used in CI.

The plugin protocol, schemas, generated protocol types, DB schema/migrations and
CoreClient are frozen. An evidence-backed extension proposal and review precede
contract changes. Keep unsupported and unverified platform features explicit.
Primary API sources and exact pinned library versions are required for audits.

Run meaningful checks for coherent source batches; avoid repeating full suites
after every edit. Heavy builds normally run in GitHub Actions. Any explicitly
authorized local fallback must keep power-saver unchanged and use one compile
job, CPUQuota=40%, MemoryMax=4G, low CPU/I/O priority. Never change power profile.
