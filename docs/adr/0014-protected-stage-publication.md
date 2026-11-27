# ADR 0014: protected stage publication before managed admission

Status: accepted for isolated staged implementation after root source review,
2026-11-27. This extends ADR0012's storage plan, not the plugin or application RPC
contract. It does not enable local-copy removal or authorize real-data cleanup.

## Evidence and required behavior

The actual Core creator now exclusively allocates the new byte inode, copies,
seals, syncs and publishes Ready v1. Its production App caller passes an authentic
Accounts binding; a matching label cannot substitute another Core issuer. The
creator foundation passed Verify46025496113. Native managed dispatch, independent
late settlement and actual admission wake passed Verify46024596897. Older native
peers without a release promise passed Verify46026511928: successful delivery does
not mark physical closure, and the source stays retained.

Those facts do not certify a protected birth yet. Core still preserves the
unregistered generic-input baseline. A v1 Ready file may already have been read
through a generic file/album/edit/photo/vendor input. A catalog scan, source row
absence, process exit, stop acknowledgment or a new positive reply cannot close
that older reader. App's old Ready discard checks references rather than this
exposure history. These are coherent activation requirements.

## Publication fact and schema review

Add migration0006 with a separate `managed_stage_publication` table. A Ready file
must not become a standalone `managed_source` registration. The table records a
fact captured only by the actual exclusive Core creator, after byte/parent sync
and before usable Ready publication:

- immutable stage ID, birth nonce and client ID, each exactly32 lowercase hex;
- original account/plugin/birth, exact configured path and bounded version1
  normalized Ready/namespace/byte identity, with a64KiB JSON ceiling;
- an optional unique source ID, set once in the same FULL transaction as initial
  source/outbox/echo/reference admission;
- diagnostic creation time, never a freshness or lifetime permission.

A source link must never silently return to NULL on source deletion. Use a deferred
NO ACTION foreign key, so linked source/publication deletion must settle together
at commit while ordinary verified account metadata cascades can still complete.
The write-once trigger rejects unlinking or substituting an established source ID.
The link also checks exact scalar stage/client/account/plugin/birth/path ownership.
The publication identity is immutable. SQL metadata checks are not filesystem proof.

The publication table is descriptive history, not a new active reference consumer:
no generic method may acquire source bytes through it. A parsed row cannot mint a
LiveStageClaim, native producer, byte read, closure or unlink permission. The real
one-use claim and independently verified current owner/namespace/hash remain
mandatory. Exact retained admission lookup still precedes all fresh checks.
Historical FULL database facts use the existing managed-dispatch trust model;
this is not a scheme for trusting arbitrary hostile same-UID database edits.

Migration review requires upgrade/reapply/reopen, identity/link immutability,
foreign-source rejection, rollback of the whole migration on failure, and preserving
migration0005's existing revision/replay checks rather than accidentally rerunning
only the newest migration. No shipped migration is edited.

## Coherent activation sequence

Core must install strict private-input enforcement before Host discovery/autostart
and before the creator can record protected publication. This is immutable Core
composition, not a setting, caller boolean, timestamp, raw path or RPC bypass.
All five known file-input routes require independent external classification;
unknown vendor inputs cannot escape it. Text and absent optional photo paths keep
their behavior. Only the canonical managed native route may read a private source.

The actual creator captures publication identity from its held new inode and exact
Accounts-issued binding. No API accepts caller-vouched provenance. Its FULL writer
performs bounded DB-only work; original input checks and all FS work remain outside
the writer. Caller loss cannot release the actual local/DB job's drain accounting.
Unknown commit or Ready publication retains charged bytes and conservative recovery
state. It never restores a spent Claimed stage to reenqueuable Ready.

App must transfer the actual unspent Core claim into managed admission, replacing
its generic private-path enqueue. Validated delivery remains available to older
file-capable peers; only the original native on_success promise can certify source
closure. No generic private-path fallback or automatic resend is added.

Ready discard and account removal require the same typed publication evidence,
original owner/namespace, actual stage flock and exclusion. A legacy Ready without
protected birth remains explicitly unproved and retained. Never retrofit its
history from a scan, missing source row or current response. Genuine unadmitted
Ready recovery may complete local publication only, never send a file.

## Capacity, recovery and release gates

Bound active/unresolved metadata and each per-stage historical exposure query.
Do not load every historical JSON blob on startup. Preserve every unresolved
exposure independently of current outbox pointer and source/dispatch labels.
Fully proven release history needs indexed compact receipts and proof-based
compaction; a256-row safety budget must not become a lifetime upload/startup limit.
Unknown history is never pruned to make room.

The first metadata foundation remains unactivated: any publication record refuses
startup before Host discovery/autostart, and its owner metadata cannot be removed
through the unproved ordinary removal path. No runtime publication writer is added
in that foundation. Strict input composition,
actual protected creation/restore, admission and App transfer/discard must pass as
a coherent batch before promotion. Physical release additionally needs ADR0012's
all-reader proof, indexed owned-reference detachment, journaled exact unlink/fsync,
durable absence and quota settlement. No cleanup command/UI is enabled by this ADR.
