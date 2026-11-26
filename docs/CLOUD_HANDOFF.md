# Verification handoff

Last fully verified source: `7e5dc01d420070f7127a530a3eb135e6a8e44967`,
[Verify45852695182](https://github.com/C0fuge/66640debf9f91471/actions/runs/45852695182).
Both jobs passed: 323 Rust workspace tests including doctests, no-default core
126, native private-bus and fake integrations, Clippy, CLI/desktop builds,
conformance 144/0/72; UI 84, SDK 63, fake 8, MAX 177, Telegram 169 and eight
Chromium scenarios. Desktop/mobile archive screenshots were inspected.

The verified app has 36 typed operations. Exact observed archive/unarchive now
shares one-use account-run authorization with pin/manual unread controls. Rows
move between tabs from committed data; unknown outcomes never replay. Telegram
validates raw inbox/archive folders consistently across targeted and full reads.
MAX plain-text edits support Saved Messages and explicitly writable ordinary
member groups, with fresh message/chat/policy checks and no media/format changes.
Unsupported peer kinds, expired/unknown rights and old runs fail safely.

Development continues. The new candidate adds paired Telegram unread-mention
list/read methods: exact groups/topics, one bounded oldest batch, no invented
counters or cursors, bounded acknowledged read continuation and ghost refusal.
These are under verification; the verified baseline remains 41/120 Telegram
methods until that exact candidate passes. No matched mentions UI is claimed.

Heavy gates remain in Actions with power-saver preserved locally. All test data
is synthetic. Real accounts/server behavior, actual desktop chooser use and
legacy migration remain unverified. Mute representation/settings preservation,
file-copy reclamation, MAX timestamp-tie history continuation and broader parity
remain unfinished. Keep platform threads separate, bubbles left-aligned and
accent changes limited to accents. No private history, production account data,
credentials or cloud model sessions are included. Docs-only commits may lack a
CI run; use exact source SHAs and results.
