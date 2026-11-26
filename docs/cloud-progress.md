# Verified Telegram mentions; text-admission safety candidate

Verified source `b222cde3450377babab7f19d9dd7bbd859357ff9` passed
[Verify45854288324](https://github.com/C0fuge/66640debf9f91471/actions/runs/45854288324).
Rust workspace 323, no-default core 126, native/private-bus and fake integrations,
Clippy/builds/conformance 145/0/71, UI 84, SDK 63, fake 8, MAX 177, Telegram 222
and all eight Chromium scenarios passed. Telegram implements 43/120 methods,
with 77 pending; the app remains at 36 typed operations. The paired mentions
methods are verified plugin behavior; a matched mentions UI remains pending.

Archive/unarchive and bounded MAX literal text edits remain verified. Platform
threads stay separate, bubbles left-aligned, and accents recolor accents only.

Development continues. The current candidate fixes text admission uncertainty:
both transports classify enqueueText as a write, a validated exact-thread text
receipt is required before clearing its draft, and explicit outcomeUnknown wins
regardless of error code. Reconciliation never causes automatic re-enqueue.
The candidate is pending its own complete verification.

Further source-lifetime API 1.2, useful indexed local-copy removal and ordinary
text-reply work are staged review/implementation tasks, not enabled cleanup or
claimed reply UI. Mute and MAX history-tie continuation remain unsupported.
All data and files used by CI are synthetic. No real accounts, credentials,
private history, cloud coding session, desktop chooser or legacy migration was
used. Heavy checks stay in Actions with local power-saver preserved. Use exact
verified source SHA/results; docs-only commits may have no separate CI run.
