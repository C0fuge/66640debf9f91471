# Verification handoff

Source `d34cc06be018c20d769aa61b857114ab938348d1` passed
[Verify45847181736](https://github.com/C0fuge/66640debf9f91471/actions/runs/45847181736).
Both jobs passed: 320 Rust workspace tests (including doctests), no-default core
126, isolated native portal transport, explicit fake integrations, Clippy,
CLI/desktop builds and conformance 144/0/72. UI 83, Python SDK 63, fake 8, MAX 77,
Telegram 125 and all eight Chromium scenarios passed. Desktop and mobile chat
preference screenshots were inspected.

The app exposes 36 typed operations. Explicit targeted observation authorizes
one pin/unpin or manual-unread change on the same account run. Unknown fields
cannot authorize writes; lost replies never replay a setter. UI uncertainty is
bounded without eviction. API 1.1 adds optional chats.get; Telegram implements
41 of 120 core-to-plugin methods, with 79 pending. This is partial platform coverage.

Development resumed on owner instruction; Verify is active. Heavy checks stay
in Actions, with local power-saver preserved. These CI runs use synthetic data;
real accounts, native desktop chooser use and legacy migration remain unverified.
File retention is still bounded to 16 files and needs reference-safe reclamation.
MAX edit/history continuation and broader Telegram/MAX parity remain unfinished.

The public mirror includes no private source history, production account data,
credentials or cloud coding sessions. Docs-only commits do not rerun CI; use the
exact verified source SHA above. Keep the existing app/data until verified import
and switch-over. Platform threads remain separate, bubbles left-aligned, and
accent changes affect only accents.
