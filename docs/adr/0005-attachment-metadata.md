# ADR 0005: attachment metadata in the application message view

Status: accepted for implementation, 2026-11-25.

## Decision

Implement only the metadata projection from the private media review as the next
small application batch. Add optional `Message.attachment: Attachment | null`,
retaining `mediaKind` for existing callers. `Attachment` contains `kind: string`,
nullable `name: string`, `mime: string`, `bytes: string` (unsigned decimal),
`width: number`, `height: number`, `durationSeconds: number`, and
`hasSpoiler: boolean`. Missing/unknown values remain null. Numeric projections
must remain within JavaScript's safe range.

Project only confirmed stored message media. Never expose local paths, remote
URLs, upload source paths, file descriptors, or arbitrary platform extensions.
The renderer treats all names, MIME values and unknown kinds as untrusted text.
Show a readable attachment summary with the existing caption. Spoiler metadata
is concealed until an explicit local reveal for that exact message; deleted
messages and provisional echoes do not expose attachment details.

Preserve per-platform/account threads, left-aligned incoming/outgoing bubbles,
and accent-only colour behavior. No automatic fetches, thumbnails, file access,
upload controls, new CoreOperations, plugin/schema/SDK changes or DB migrations
are included. Download jobs, native open/reveal/save and durable upload selection
remain separate reviewed batches; the metadata view must not display nonworking
controls for them.

## Verification

Add focused synthetic projection/UI checks for ordinary photo/document/voice
metadata, unknown/absent fields, escaping, safe byte counts, hidden/deleted/local
echo paths, spoiler reveal isolation and preserved captions. Run UI/Rust checks
in the normal cloud integration gate. No live accounts or user files are fixtures.
