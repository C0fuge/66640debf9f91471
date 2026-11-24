# 0004: Draft snapshots and freshness

Status: accepted
Date: 2026-11-25

## Context

Draft ingest compared only text, losing formatting, reply target and extension changes
when text stayed the same. It also discarded a newer timestamp for identical content,
allowing an intermediate older conflicting snapshot to overwrite the draft later.
The plugin spec's equal-text echo shortcut obscured the distinction between a
merge-updated parent row and its replaced nested Draft value.

## Decision

Draft is a whole nested snapshot, not another merge-updated row. Compare its text,
entities, reply target and extension object. Omitted/null optional metadata is unknown
in the replacement; an empty entity array is explicitly empty. Omitted/null
Conversation.draft or Topic.draft still preserves the existing draft. Extension
objects are stored verbatim, without deep merging.

Reject older timestamps before comparing content. Identical content with a newer
explicit timestamp advances the stored watermark without emitting DraftChanged.
An exact duplicate or identical undated snapshot performs no write and emits no
content event. Equal-timestamp snapshots with different content retain the existing
arrival-order behavior. Changed snapshots emit DraftChanged. Empty text keeps the
existing row-deletion behavior.

Correct plugin spec §8.4's array-preservation wording to apply to merge-updated rows,
and §10.1's draft echo wording to compare full content instead of text alone. This is
an accepted specification erratum and implementation correction: API 1.0.0, wire
shapes, generated SDKs and database schema stay unchanged.

## Consequences

Focused ingest regressions cover independent same-text formatting/reply/extension
changes, nested replacement versus sparse-parent preservation, explicit freshness,
stale writes and clears, duplicate events, account/chat/topic isolation and clearing.
Rust test execution and formatting remain CI gates; no local heavy build is required.

No draft bridge, cloud-sync queue, compare-and-set revision, ghost policy or migration
is added. Deleting an empty draft also deletes its timestamp: a delayed old nonempty
snapshot can still resurrect a cleared draft. Preventing that requires a separately
reviewed tombstone/revision design and is not claimed as fixed here.
