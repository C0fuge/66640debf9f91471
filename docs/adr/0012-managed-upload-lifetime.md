# ADR 0012: managed upload lifetime and explicit local-copy removal

Status: accepted by root for staged implementation, 2026-11-26, after review of
the normal sent/adopted-file path and independent protocol/settlement/index
analysis. The current 16-file retention limit remains until the complete release
flow passes its gates. This decision authorizes no cleanup of real user data,
legacy migration or platform-side deletion. Heavy tests remain in Actions.

## Useful outcome and boundaries

An explicit Remove local copy action must work for a normal newly sent file,
including its adopted message, same-message versions and own media index entry.
Preserve message IDs, body, remote attachment metadata and the canonical terminal
Sent outbox record. Detach only positively owned local path slots, then remove
only the exact private staged byte inode. No resend, new echo, automatic download,
chooser or original user-source modification follows cleanup. Shared, foreign,
live or unexplained references refuse removal.

A current-process-only helper which refuses every adopted file or databases with
more than 256 unrelated rows is not the product feature. Implement a complete
indexed managed-source/reference journal and bounded resumable backfill, maintained
atomically by every actual writer. Per-file work remains bounded independently
of unrelated history size. An exact-string index alone is not an inode/alias proof:
preserve held-root/fd identity, no-follow ancestry, single-link and fingerprint
checks. Ordinary user-file uploads remain supported; unsupported generic album,
edit or raw-host use of a registered managed source may be rejected pre-dispatch
in this first slice rather than escaping source ownership.

## Protocol foundation: API 1.2

Existing upload semantics do not guarantee source lifetime after a successful
response. Section 15's cache eviction rule applies to cache_dir, not upload input
files. Add the optional parameter `source_release: "on_success"` to the existing
send.file capability object. A plugin opting in promises that a successful
messages.send_file response is sent only after all reads of that request's source
file have completed and all its source handles/tasks have settled. It retains no
future source access authority. Failure, cancellation, timeout, progress or a
separate message event provides no such promise.

This is an additive API 1.2 amendment, with same-major compatibility and no new
method, DTO field or RPC parameter. Boolean true, an empty object or missing
source_release permits existing uploads but provides no cleanup authority.
Older peers continue working. Unknown/malformed promise values must never grant
release. The parameter is an open string, with only the exact case-sensitive known
value `on_success` granting the promise. Unknown strings remain valid upload
parameters; malformed non-string values fail schema/conformance, while tolerated
raw runtime capabilities keep upload support without release authority. Do not
globally tighten the handshake or unrelated object parameters. Coordinate schema,
capability metadata, both SDKs, API versions, examples,
goldens and conformance; test mixed versions and absent/false/parameter variants.
Enable the promise only in built-ins whose source closure has been inspected and
tested. Do not hardcode any messenger in core. This protocol-only stage must pass
before the dependent storage/app work is enabled.

## Durable source and attempt ownership

Use one reviewed DB migration for the managed source, reverse references,
backfill coverage and retirement journal. A registration is not admission or
settlement proof. A bare Sent row, adopted echo, progress, error, cancellation,
missing row or restart cannot authorize unlink. New dispatches need a persisted
unique attempt identity; the existing retry-reset attempts counter is insufficient.
Capture the exact canonical request, source identity and live handshake promise
for that attempt. Record positive settlement only from its validated successful
response, even when an earlier event already marked delivery Sent. Keep a source
read lease through actual response processing and proof commit. A cleanup writer
excludes readers and new references.

Only new positively settled SendFile stages, including recovered durable proofs,
are eligible initially. Old unproved consumed/claimed stages remain retained.
Account removal, replacement, caller cancellation and shutdown must preserve and
drain the actual source/DB/FS leases; a late callback cannot recreate roots.

## Atomic detachment and journaled removal

Under exact owner/stage/source exclusion and the DB writer reservation, require
complete reference coverage, the matching canonical Sent row, positive settlement
and only detachable references owned by its adopted/result message. Clear only
those indexed local path slots and the matching own media_file entry. Mark the
journal retiring in the same transaction, retain Sent intent, and reject new
source acquisitions or reattachment after that commit.

Then unlink the verified named inode using held directory fds, sync the required
bindings and persist released. Quota is freed only from durable proven absence.
Failures retain conservative accounting and a recoverable cleanup status. The
FS inventory must recognize byte absence only through the exact durable journal;
old version-1 Consumed manifests remain admission evidence, never reenqueuable
Ready files. Recovery completes only cleanup, never source recreation or sends.
Bound metadata compaction and inventory work as well as byte retention.

## Application and release sequencing

The eventual matched command is a typed, account/thread/session/stage-scoped
releaseFileStage operation with an explicit local-copy projection; it is separate
from cancelFileSelection and retryOutbox. The exact app result/projection and
core/FS interface plan must be reviewed before those contracts are edited. Do not
enable a UI control from an optimistic Sent display or today's filesystem scan.

Implement/review the protocol foundation first. Then review concrete core DB,
writer/lease coverage and FS/app interfaces, implement them in isolated lanes,
and verify the coherent flow before merging or enabling removal.

Required gates include real normal-send/adoption/version removal, retained-row
admission dedupe, early Sent with delayed response, durable attempt recovery,
100000 unrelated synthetic messages with indexed lookup, bounded backfill with
concurrent writes, aliases/shared refs, every detach/unlink/fsync crash boundary,
caller drop, account recreation and shutdown. No real accounts or files are used.
