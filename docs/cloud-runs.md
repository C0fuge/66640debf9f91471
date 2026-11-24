# CI builds and tests

**Verify** runs on pushes to `main`, every night at 00:23 UTC (03:23 Moscow),
and manual dispatch. Select a branch in GitHub's **Run workflow** menu to verify
that branch. Schedules use the default branch and can be delayed by GitHub.
Documentation-only pushes are skipped; manual dispatch can verify any branch.
Runs queue for the same branch; a newer run does not cancel an active run.

There is no cloud coding agent or publisher. No API key, ChatGPT login, OAuth
session, messenger account or repository secret is needed. The workflow has only
read access to repository contents and does not push commits or create PRs.

Two jobs run independently on disposable Ubuntu 24.04 runners:

- UI tests and TypeScript/Vite build; Python SDK and every Python plugin suite
  (currently fake, MAX and Telegram); real headless Chromium account/action flows.
- Rust workspace tests, core tests without default features, Clippy, ignored fake
  integrations, actual CLI fake conformance and desktop/CLI binary builds. The
  UI is built first because Tauri embeds its output. GTK/WebKit/D-Bus development
  packages are installed for the Linux desktop build.

Verified snapshot `3655669` passed UI32, Python SDK48, fake8, MAX45, Telegram44,
Rust workspace192 and core without default features113 tests. Each run's logs
are authoritative; counts may grow with new tests. See CLOUD_HANDOFF.md for the
linked run and the explicit partial-platform limitations.

Toolchains are Node 22, Python 3.13, uv 0.13.9 and Rust 1.99.0. Rust's published
manifest confirms the required 1.99.0 toolchain; an installation failure fails
verification rather than silently testing another version. Rust uses two build
jobs, no incremental compilation and no debug symbols. Job limits are 25 minutes
for UI/Python and 55 minutes for Rust.

Caches contain npm downloads, Cargo registry/git dependencies and selected Rust
compilation directories only. They do not include HOME, application data, XDG
state, Python virtual environments, logs or credentials. Tests use synthetic data,
a temporary CH_DATA_DIR and XDG directories, and unavailable session/system
D-Bus addresses. No real user data or messenger sessions are present.

Each job uploads a result summary and console log for 14 days, including on
failure. A successful Rust job also uploads `contact-hub-linux-x86_64.tar.gz`
containing the Linux desktop executable and `ch` CLI. These are development
binaries, without debug symbols, built for Ubuntu 24.04 x86_64; the desktop needs
GTK 3, WebKitGTK 4.1 and related system runtime libraries. This is not an installer
or a signed release. Logs and artifacts contain only this public source snapshot
and synthetic CI execution. Open the **Actions → Verify** run for the actual
results and downloadable artifacts.
