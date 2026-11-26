# Verified API 1.2 foundation; owned text admission candidate

Verified source `b2f1e833b66109e700ca93a10c9760d84954d788` passed
[Verify45858802319](https://github.com/C0fuge/66640debf9f91471/actions/runs/45858802319).
Both jobs passed: UI 89, SDK 89, fake 9, MAX 177, Telegram 223, Rust workspace/
no-default/native/Clippy/build gates, conformance 144/0/72 and eight Chromium
scenarios. API 1.2 adds an optional open-string send.file.source_release promise;
only exact on_success grants it. Telegram/fake closure and serialization tests
pass; same-major older peers and ordinary uploads remain supported. No cleanup
or DB migration is enabled. App 36 operations; Telegram 43/120 methods.

The current candidate owns four bounded text-admission jobs independently of IPC
waiters. Exact account owner and storage generation are captured together. The
DB writer rechecks existing chat/topic/rights before atomically inserting an echo
and outbox entry; rejection cannot recreate missing destinations. Actual blocking
work owns directory, permit and owner guards through settlement. Shutdown drains
it before Outbox/Core. Literal offline/stopped/disabled queue behavior is retained.
Independent source review found no blocker; complete exact-source CI is pending.
No schema, app operation, database migration or automatic retry is added.

Useful Remove local copy remains under staged implementation: durable unique
attempt settlement, indexed references, reader exclusion and journaled recovery
must pass together before any action is enabled. Unknown external readers remain
barriers across Core restart. Basic text replies and mute remain pending.
All fixtures use synthetic data/files/accounts; real credentials, private history,
chooser, legacy migration and source cleanup are excluded. Heavy gates stay remote
with power-saver unchanged. Platform threads stay separate, bubbles left-aligned,
accents limited to accents. Docs-only commits may lack their own CI run.
