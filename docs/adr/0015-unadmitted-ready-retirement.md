# ADR 0015: journal unadmitted protected Ready retirement

Status: root reviewed for isolated metadata foundation, 2026-11-27. Runtime
retirement and physical cleanup remain unactivated. No new application/plugin RPC.

## Evidence and scope

ADR0014's actual protected birth, strict Core Host inputs, atomic source linkage,
unspent App claim transfer and owned local recovery passed Verify46038977659:
522 workspace tests, native/no-default/Clippy/build, Python/UI/browser and fake
conformance. The exact source is e950ae5. Existing Ready discard now requires an
actual live Core verifier and the matching unlinked birth before checking stored
references. Legacy Ready without protected birth stays retained.

That discard still leaves conservative birth/catalog metadata behind after local
unlink. Its old global256 consumer-row census also fails to scale with ordinary
unrelated history. Neither keeping every active fact forever nor deleting facts
from source/outbox absence is a complete quota or account-removal solution.

This ADR concerns only never-admitted protected copies. An admitted Source needs
its separate all-reader/native-exposure proof, reference detachment and journal.
A successful reply, dropped verifier, process exit, source label or parsed receipt
never proves physical closure. Shared plugins/helpers must not be stopped to
manufacture a shortcut.

## Reviewed metadata foundation

Append migration0007; never edit migrations0005/0006. Add:

- `managed_ready_retirement`: immutable journal nonce, actual birth nonce,
  stage/client/original account/plugin/birth/path and bounded normalized Ready
  identity. Its stage references the original publication using deferred NO ACTION;
  no silent unlink of the birth. It accepts only a matching unlinked publication
  with no matching Source or canonical outbox receipt. Persist monotonic phases
  `intent`, `marked`, `bytes_absent`, `stage_absent`; labels remain descriptive.
- `managed_ready_receipt`: immutable compact original stage/client/path/owner,
  birth and retirement nonces, byte and identity digests, version and diagnostic
  times. It contains no byte/read/claim/replay/cleanup grant. A future actual worker
  may write it only after durable absence and quota settlement. Indexed receipts
  veto reuse of an old stage/client/path without loading historical JSON at boot.

Matching checks are negative guards, not caller-vouched provenance. A pending
journal must reject source insertion/linkage and publication recovery; an exact
retained admission lookup remains before every fresh check. A receipt never clears
active birth/source/reader barriers merely because it exists. Account metadata
cascades stay coherent; actual ordinary removal keeps its original owner/anchor
and lifetime vetoes. Identity fields are immutable, and journal phase regression
or skipping a required step is rejected. Repeating the same phase is harmless.

The first foundation has no journal/receipt runtime writer. Any active journal,
including an orphan left by a foreign-key-disabled writer, refuses startup before
Host discovery/autostart. Its original owner is refused by the metadata removal
guard. Compact receipt metadata alone does not require a byte scan or grant IO;
all active facts remain independently checked. Existing flows have no records in
the new tables and keep their behavior.

## Runtime order still required before activation

1. The actual bounded Core worker retains authentic current/exclusive owner,
   independently verified original Ready hash, byte descriptor and actual flock.
   The creator's real output handles have already closed. A held claim excludes
   retirement; a held retirement excludes another actual Ready claim.
2. Use the existing general `managed_ref` ledger, including NULL-source literal
   slots, global monotonic revision and bounded coverage cursors. Do not duplicate
   its extraction with a publication-reference table or fake a staged Source.
   Classify aliases outside FULL, recheck exact row revisions/ownership inside
   FULL, and use indexed per-stage candidates. Unknown/blocked or incomplete
   evidence retains. No FS or owner/catalog registry lock inside the FULL writer.
3. Commit intent before destructive work. Every fresh managed admission rejects
   pending intent, including after restart. Publish local discarded state without
   reversing Claimed/Consumed, then exact identity-bound unlink/fsync of each entry
   and parent. Lost/failed sync retains charge and journal state. Never infer
   durable absence from an unanchored path NotFound.
4. Only a nonserializable result of actual owned settlement may retire that birth's
   exact catalog negatives and settle quota. Account rearm, scan completeness,
   cleared SQL pointers and parsed journals cannot construct that result.
5. Compact only proven settled metadata into indexed receipts. The256 unresolved
   safety budget must not become a lifetime selection limit; never prune unknown
   history. Preserve exact outbox/echo receipts and no automatic file resend.

Old no-release native peers continue to deliver files and remain retained. Local
recovery may complete Ready only after independent actual verification; it cannot
publish over Claimed/Consumed/Discarded or enqueue. Do not relabel an unknown
incomplete copy Ready just to reuse the existing UI actions. Any UI integration
needs a separate honest state/intent contract.

## Gates

Before runtime activation: source/discard race, lost callers and interrupted drains
at every journal/unlink/sync edge, every restart phase without replay/grants,
changed owner/namespace/hash, aliases and unsupported payloads, actual closure of
all local readers, exact durable absence and quota settlement. Ordinary text and
independent outside files stay positive controls. More than256 fully retired
selections must work without discarding unresolved evidence.

For this schema foundation: immutable/matching/monotonic guards, source/admission
exclusion, upgrade/reapply/reopen/account cascades and whole-migration rollback.
Migration0005/0006 replay tests must select their own explicit version after0007
is appended. A green metadata gate does not authorize physical cleanup/promotion.
