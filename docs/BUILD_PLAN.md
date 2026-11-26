# Build status

Verified source snapshot: `dc808f3`.
[Verify45794077036](https://github.com/C0fuge/66640debf9f91471/actions/runs/45575407031)
passed both jobs: Rust workspace267, no-default core113, two explicit fake
integrations, isolated native-portal transport, Clippy, CLI conformance144/0/71,
Linux desktop/CLI builds; UI61, PythonSDK48, fake8, MAX77 and Telegram110.
Chromium account, action, metadata, attachment-job, poll and reaction scenarios passed.

| Phase | Status |
| --- | --- |
| 1a/1b: protocol, SDKs, fake plugin, templates/tutorial | Done |
| 2: Rust core, storage, ingest, host, outbox, search, CLI | Integrated and verified |
| 3: Tauri/React skeleton and three-pane text demo | Integrated and verified |
| 4: Telegram and matching UI | Partial:39 of119 plugin methods;80 pending |
| 5/6: WhatsApp and MAX | WhatsApp pending; MAX partial auth/text/chat upserts and bounded deletion |
| 7: remaining settings/themes/i18n, notifications, lock, backup, packaging/import | Pending |
| 8: migration, working-app verification, switch-over | Pending |

The app currently exposes30 typed operations. Media jobs add explicit bounded
download, cache-only native Save-copy, scoped receipt listing and cancellation.
Receipt keys are retained for the process lifetime; lost replies never cause
an automatic replay. Native save uses held file descriptors and no-overwrite
publication. Browser/demo save is unavailable without a native provider.
A private D-Bus test verifies portal transport; a real desktop chooser and live
messenger accounts have not been exercised by these CI runs.

Telegram ordinary reactions, scoped unread-reaction IDs/read receipts, poll
interactions and attachment APIs are tested offline. Paid/effect and remaining
families are not claimed. MAX history continuation and authoritative full chat
reconciliation remain unsupported; timestamp-tie safety is unproved. MAX delete
requires exact scoped acknowledgment; edit remains undeclared.

ADR0008's three ordinary reaction operations and matching explicit UI are verified.
The base Telegram sticker catalog (recent/packs/pack/static thumbnail) is tested;
sending, management, animations, favorites and catalog UI remain pending.
Next work: a reviewed single-file composer ingress contract and a caller-stable
local outbox admission seam. Current generic enqueue is not admission-idempotent.
No upload-root isolation or durable file staging is claimed to exist yet.
Uploads, poll creation and broader messenger parity remain pending. Message bubbles
stay left-aligned; accent changes affect accent tokens only.

Builds/tests run in Actions against synthetic data. Development remains outside
this mirror. No account state, credentials, private history or cloud model sessions
are included. See [CLOUD_HANDOFF.md](CLOUD_HANDOFF.md) for verification entry points.
