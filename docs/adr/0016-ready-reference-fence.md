# ADR 0016: fence new reference slots during pending Ready retirement

Status: root reviewed for isolated metadata foundation, 2026-11-27. Physical
retirement, journal writers and recovery remain unactivated. No new RPC.

## Evidence and problem

ADR0015's journal excludes Source creation and linkage. The actual Core Ready
precheck now also refuses existing literal/inode index hits, including dirty and
NULL-source extension slots. Sourcea183919 passed Verify46049654726 (538 workspace
tests plus the full native/Clippy/build/Python/UI/browser gate). Actual directory
and cache lifetime binding source102938f passed Verify46050366340.

An earlier reference census does not exclude a later writer adding a path while
the future worker performs unlink and sync. Discovering a new revision after
unlink cannot preserve those bytes. The old App discard reserves an IMMEDIATE
writer across its filesystem work, but has an incomplete raw-field census and a
global row limit. A new journal worker cannot retain that separate SQLite writer
while committing its FULL phase transactions. Filesystem work inside the FULL
callback would also violate its existing boundary.

## Reviewed narrow metadata change

Append migration0008. Do not edit0005/0006/0007. Reuse the existing managed_ref
extraction, keys, coverage and revision system; add no second reference ledger.
While ANY managed_ready_retirement row exists, regardless of its phase:

- Refuse INSERT or UPDATE of a managed_ref slot. This includes unrelated paths,
  aliases and dirty/NULL-source slots; path equality is not an alias proof.
- Refuse a managed_ref_row becoming blocked, including INSERT and repeated
  blocked-state publication. Ambiguous/unindexable payloads cannot slip through
  merely because extraction produced no literal slot.
- Let deletes remain negative evidence only. Neither removal of a slot/journal,
  a receipt nor a parsed phase certifies closure, absence or quota settlement.

These are negative SQL guards. Normalized reference extraction runs in the same
transaction as the actual consumer write, so trigger refusal rolls back that
write, its old-reference deletion and revision change together. They also cover
direct SQL through the existing bounded extraction triggers. The pending table's
existence, not a caller-vouched phase or source label, controls refusal. Compact
receipts do not lift the fence while a journal still exists.

Existing flows create no journals and retain their behavior. Ordinary no-path
text must remain a positive control even in a metadata test with a pending
journal. Account metadata cascade and exact retained admission lookup remain
coherent. No new factory accepts a path list, closure result or serialized grant.

## Runtime policy still required before activation

The durable guard intentionally refuses path-bearing writes globally during the
short physical window. Before any actual journal writer exists, normal consumer
writers need an owned local scheduling gate that queues their real DB work,
including after waiter loss; native events must not be dropped or converted into
unnecessary uncertain delivery because of this backstop. Audit ingest, history,
outbox/native completion, avatars/plugin metadata and backfill. No helper stops,
network waits with storage leases, or registry/FS mutex inside FULL callbacks.

An actual independent alias census, stable writer observations/revisions,
original locked namespace/owner, real all-reader exclusion, every owned FD drop,
durable journal/absence/restart and exact catalog/quota settlement remain
mandatory. Persistent outside classifications alone do not certify current
targets. Catalog's global inode set also needs bounded origin accounting before
an actual settled worker can retire only its own negatives. This migration does
not solve or activate any of those operations.

## Gates

Test actual SQLite consumer writes racing an already committed pending journal,
all journal phases, canonical/unrelated/extension path slots, unsupported payloads
without an extracted path, rollback of original row/references/revision, no-path
text, path removal, account cascade, upgrade/reapply/reopen and whole-migration
rollback. Seeded metadata never stands for actual filesystem absence or closure.
Migration7's own replay/rollback tests must explicitly select version7 after8 is
appended. Run the complete existing cloud gate before considering this foundation
verified. No production promotion or physical cleanup follows from a green gate.
