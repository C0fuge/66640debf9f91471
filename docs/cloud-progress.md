# Source ownership and text-reply confirmation candidate

Last verified source `50705de51f323b66a882df8db8c6ebe823d816d2` passed
[Verify45863782700](https://github.com/C0fuge/66640debf9f91471/actions/runs/45863782700).
Rust workspace350 including doctests, native/private-bus, no-default/Clippy/build,
UI89, SDK89, fake9, MAX177, Telegram223, conformance145/0/71 and eight Chromium
scenarios passed. App36 operations; Telegram43/120 methods. API1.2 source promise
and Accounts-bound Core Ready-stage verification are verified.

Current reviewed batch contains three saved stages and their regression fixes:
- Migration0005 and conservative reverse-reference metadata. Snapshot comparison
  now validates actual live slots/owner/revision relationships, then normalizes
  surrogate keys and allocator revisions. Genuine thumb_path slots are recognized
  structurally without stripping JSON path quotes. Semantic state still compares.
- Core owns the actual durable Ready-to-Claimed transition and privately mints a
  non-Clone live claim. Consumed bookkeeping requires exact canonical retained
  admission; request paths, byte handles and original owner leases remain scoped.
- Bare text replies require observed exact relation/topic before Telegram/MAX
  correlation and Core adoption. Telegram sends once and reads at most one known
  sent ID; MAX validates new acknowledgments and old journal receipts. Missing or
  ambiguous relation remains unknown, with no resend or plain-text fallback.
  Echo pointer ownership prevents a reused SQL row ID from confirming or changing
  an unrelated message. Quote/comment/plain-send behavior remains unchanged.

Independent source reviews found no blocker. Full exact-source CI is pending;
no candidate stage is claimed verified from source review alone. Reply UI and
owner-bound history work are separate, unverified development.

No production managed-source registration, dispatch certificate, journaled unlink,
cleanup command/control or new operation is enabled. Literal refs are not inode/
alias/source-lifetime proof. All fixtures are synthetic in temporary storage;
no real account, credentials, history, chooser, original-source modification or
legacy migration is included. Heavy gates run remotely; power-saver unchanged.
Platform threads remain separate, bubbles left-aligned and accents limited to
accent tokens. Docs-only source commits may not have a dedicated CI run.
