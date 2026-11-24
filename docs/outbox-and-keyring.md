# Outbox and keyring

Create the desktop secret store with `KeyringSecretStore::open().await` and pass it to
`CoreConfig::new`. On Linux the default core feature links the keyring crate’s synchronous
Secret Service backend with Rust cryptography. Building requires pkg-config and the libdbus-1
development files. The desktop session needs a D-Bus session bus and a Secret Service provider
whose collection the user can unlock. A locked or unavailable keyring returns an error.
Configuration values flagged x-secret and plugin runtime secrets occupy the runtime’s separate
namespaces. Each account/namespace has a single credential containing its key/value map. The
desktop core is its writer. Multiple store handles in that core serialize writes.

Construct one `Outbox::for_core(&core, policy)` per running database and clone that handle for
callers. Call `start()` after construction to resume queued work. New enqueues create workers
on demand. Subscribe to the outbox for item snapshots and to the core bus for local message
changes. Await `outbox.shutdown()` before shutting down the host. The service uses the
runtime’s main ingest connection and its current path and history settings.

Enqueue commits the local message bubble and queue entry together. Each attempt in the same
conversation runs after the current head. Other conversations can proceed. Upload progress is
accepted only with the live request’s exact wire ID, plugin and account. Due dates for flood
waits and retry backoff are stored as Unix milliseconds. Offline accounts wait for connection.
The v1 wire format carries the stable client ID on text sends only. Workers check the
persisted account owner at claim and dispatch. Late RPC results must match the original
queue entry’s stable client ID. Cancelled sends stop after a definitive refusal such as a
flood wait. A lost reply still requires reconciliation.

A timeout, disconnect during a send, missing reply, malformed result, or crash can leave delivery
unknown. Text sends wait for an outgoing platform echo carrying the client ID. If no matching
echo arrives within the online grace period the UI can show `outcome_unknown` and ask the user
to check the platform before choosing Retry. A late matching echo can confirm a failed item, including one already present under its
platform ID. For a channel comment in another chat, the cached source post’s discussion-chat
ID or a cross-chat reply to that exact post must establish the destination. Without that
evidence a lost reply remains ambiguous.
Files, albums and forwards have no guaranteed wire correlation after a lost reply. An explicit
retry in these cases may send a duplicate. Edit and delete can be applied again. Unavailable
accounts and pre-connection refusals use bounded backoff where safe.

## Verification

The outbox tests use an in-memory database, scripted host and paused Tokio clock. The keyring
tests inject an in-memory credential backend. The optional host integration test uses a temp
copy of the fake plugin. These tests can run with isolated XDG directories and CH_DATA_DIR.
Run `cargo test --locked -p ch-core`, `cargo test --locked -p ch-core --no-default-features`
and `cargo test --locked -p ch-core --test host_fake -- --ignored` when the Rust build gate
opens. The no-default-features variant exercises the explicit unavailable-OS path and injected
backend without linking Secret Service. Source checks cover Rust formatting, locked offline workspace metadata, and isolated SQLite
claim and rollback probes. Compilation, Rust behavior tests and a live Secret Service check remain unverified.
