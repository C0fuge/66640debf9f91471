# Build plan

Current execution instructions: [AGENTS.md](../AGENTS.md),
[cloud task queue](../.github/cloud-tasks.md), and [cloud progress](cloud-progress.md).
Product requirements: [REBUILD_PROMPT.md](REBUILD_PROMPT.md).

| Phase | Scope | Snapshot status |
|---|---|---|
| 1a | Plugin API, schemas and Rust protocol types | implemented and tested |
| 1b | Python/Rust SDKs, fake plugin, templates, tutorial | implemented and tested |
| 2 | Rust DB, ingest, plugin host, services, outbox, search/history, CLI | implemented and tested |
| 3 | Tauri/React text demo on the fake plugin | implemented and tested |
| 4 | Telegram parity and matching UI | partial foundation, generic auth and rich text |
| 5/6 | MAX, then WhatsApp according to current product priority | not implemented |
| 7 | Themes, settings, notifications, lock, backup, packaging, i18n | largely pending |
| 8 | Safe legacy import and verified switch-over | pending; no real data in cloud |

The current contract is frozen. API inventories identify gaps; an evidence-backed
amendment proposal precedes protocol changes. Preserve independent platform chats,
left-aligned bubbles, accent-only recoloring and isolated temporary test data.

Completion means storage, plugin events/commands, core bridge, UI and meaningful
tests agree. A library wrapper or capability flag alone does not establish parity.
