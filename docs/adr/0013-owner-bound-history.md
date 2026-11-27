# ADR 0013: bind cached and fetched history to its original account owner

Status: accepted for isolated implementation and review, 2026-11-27.
This is an internal lifecycle correction, not an application or plugin wire
extension. Reply controls and owner-context DTOs require a separate decision.

## Problem

History currently identifies a fetch by account/chat/topic and plugin settings.
An account can be removed and recreated with the same identifier while a fetch
is pending. Matching a row's creation timestamp cannot prove the original owner;
timestamps can coincide. A response must not populate the replacement account,
nor may its request join the previous owner's in-flight work or rate-limit state.

## Decision

Accounts issues a lease-free observation of the exact persisted owner and its
published storage Gate. Fields and constructors remain private. Cloning the
observation retains identity and permanent revocation, not a filesystem lease,
file descriptor or write permission. Fresh storage acquisition must match this
exact original generation, not reconstruct authority from row/token fragments.

History captures the observation before network work, locks and deduplication.
Production constructors obtain authority from Core/Accounts; synthetic callers
use real isolated Core ownership with an injected plugin API. No unguarded
production fallback is added. Generation keys scope in-flight requests, thread
locks, flood waits and gap errors; revoked state is pruned.

Network awaits retain no account storage read guard. After a response, acquire
a fresh matching bounded lease outside Ingest/SQLite. The actual blocking writer
owns it through its transaction and final revocation check. Rejection rolls back
messages, conversation stubs, cursors and gaps together. If admission linearized
before revocation, removal waits for the writer then removes the old owner's
rows before replacement. This adds no global commit/revoke mutex.

Apply this to cached windows, refresh, older/newer paging, gap filling, around
and background work. A private Page observation carries the original generation
through application reaction projection; projection reacquires it rather than
attaching a replacement owner's identity to old rows. It is never serialized.

History closes new work and drains actual database permits in App's owned
shutdown before Core and directory guards are released. Dropping a request or
shutdown waiter cannot detach a writer from this lifetime. No network request
is allowed to hold up account removal through a storage lease.

## Gates and limits

Use only isolated synthetic accounts and temporary storage. Cover same-ID,
same-plugin, equal-timestamp replacement before/after network and during a
writer; old dedup cells and flood/gap state; stalled network removal; cached
reads/projection; caller loss and interrupted shutdown. Preserve existing gap,
cursor and merge semantics. Full Rust, Clippy, native and browser gates remain
remote. No real account access, migration, cleanup, new app operation or Reply
UI is enabled by this decision.
