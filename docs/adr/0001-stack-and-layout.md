# ADR 0001: Stack, repository layout and data locations

Status: accepted (2026-10-19)

## Context

`docs/REBUILD_PROMPT.md` is the product spec. The owner asked for the rebuild to be
**primarily Rust** for speed. The prompt allows a Tauri UI over the core if justified.
The old app (`/legacy-reference`, Python + PySide6) is reference material only
and is never modified. It is deleted once this app replaces it **and** its data
has been imported (see "Migration").

## Decision

### Core and app: Rust
- Cargo workspace at the repo root. All core logic is Rust (edition 2024, stable):
  - `crates/ch-protocol`: plugin API types (DTOs, JSON-RPC envelopes, error codes,
    capability keys) with serde. JSON Schemas in `schemas/` are the source of truth;
    Rust types must round-trip them (tested).
  - `crates/ch-db`: SQLite via `rusqlite` (feature `bundled`, FTS5 on), WAL,
    idempotent schema evolution.
  - `crates/ch-core`: ingest and identity, plugin host, services, search, outbox,
    notification policy, ghost-mode enforcement, presence log, backup and restore,
    secrets (`keyring` crate, Secret Service on Linux).
  - `crates/ch-plugin-sdk`: Rust plugin SDK.
  - `crates/ch-cli`: the `ch` binary (`ch plugin new`, `ch plugin test` conformance
    runner, `ch import-legacy`).
  - `app/`: the Tauri 2 desktop shell (`app/src-tauri`, crate `contact-hub`) and the
    UI (`app/ui`).
- Async runtime: tokio. Logging: `tracing`.

### UI: Tauri 2 + React + TypeScript + Vite
Why not native Rust widgets (GTK4, Slint, iced, egui)? The UI inventory (§6) is a
Telegram Desktop-class chat client: rich text with entities, spoilers, animated
emoji, Lottie, video, virtualised variable-height lists and a lot of animation.
The web platform does all of this natively, and it's where subagents produce the
best results fastest. The heavy work (DB, ingest, search, plugin I/O) stays in Rust,
so the UI only renders. WebKitGTK is installed on the target machine.

- UI talks to the core only through one `CoreClient` interface (`app/ui/src/core/`).
  It has two implementations: Tauri `invoke` and events in the app, and a WebSocket
  bridge (`ch devserver`, localhost plus token) for browser dev and Playwright e2e.
  Headless UI tests run in Chromium against `ch devserver` plus the fake plugin, with
  no WebKitGTK and no real accounts.
- State: lightweight stores (zustand). Lists: virtualised (`@tanstack/react-virtual`).
- Theme system: CSS custom properties. The palettes are ported from the old
  `gui/theme.py` and `gui/palettes.py`.

### Plugins: separate processes, any language
- Transport: JSON-RPC 2.0 over stdio. The exact framing is defined in
  `docs/plugin-api.md`.
- `plugins/fake`: Python, built on the Python SDK (`sdk/python`). It is deterministic.
- `plugins/telegram`: Python + Telethon. It ports the old
  `backend/connectors/telegram/connector.py` (about 4.9k lines of proven feature
  code) and **reuses the existing `.session` file**, so you don't sign in again.
  Telegram traffic is network-bound, so Rust buys nothing there, and Telethon has the
  widest MTProto coverage.
- `plugins/max`: Python + pymax (`maxapi-python`). It ports the old MAX connector and
  reuses its session.
- `plugins/whatsapp`: Rust, crate `whatsapp-rust = "=0.7.0"` with `default-features = false`
  (its default `simd` feature needs nightly). It was evaluated in `notes/whatsapp-eval.md`:
  it builds on stable, prints a real QR, and has full feature coverage including poll vote and decrypt. The fallback
  is neonize (Python, bundled whatsmeow).
- `examples/discord-plugin`: Rust, on `ch-plugin-sdk`. This is the non-Python example.
- Each plugin manages its own runtime. Python plugins use a `uv`-managed venv in their
  folder.

### Data locations (XDG)
- Data: `$XDG_DATA_HOME/contact-hub-ng/` (default `~/.local/share/contact-hub-ng/`),
  holding `hub.db`, `media/`, `accounts/<account-id>/` (plugin-private state such as
  session files) and `logs/`.
- User plugins: `$XDG_DATA_HOME/contact-hub-ng/plugins/`. Built-in plugins ship in
  `<install>/plugins/` and are discovered the same way.
- Config: none outside the DB `settings` table. Secrets go in the keyring.
- `-ng` keeps us clear of the old app's `~/.config/contact-hub/` and any old
  installed copy. Override everything with `CH_DATA_DIR`. **Every test and smoke run
  sets `CH_DATA_DIR` to a temp dir.**

### Migration from the old app
`ch import-legacy <old-repo-or-data-dir>` and a first-run prompt import the
following from the old app's `data/` (`hub.db`, `telegram*.session`, `max*/`)
plus its `.env` (api_id and api_hash go to the keyring):
- persons, categories, person-category links and handles;
- conversations, messages, edit history and the presence log;
- settings that map to new settings;
- Telegram and MAX sessions.

The import only reads the old data, never writes it, and is idempotent.

## Consequences
- Python is still needed at runtime for the Telegram and MAX plugins. Each plugin has
  its own venv, so there are no dependency conflicts.
- Disk is tight (about 60 GB free). Cargo target dirs are big, so subagents remove
  their worktree's `target/` after merge.
