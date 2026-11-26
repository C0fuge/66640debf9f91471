# Managed upload metadata foundation

Migration 0005 and `queries::managed` are metadata infrastructure only. Nothing
registers a production source, dispatches it, creates a settlement certificate,
detaches paths or removes a file. No filesystem independence, ownership or
release authority follows from a literal reference or completed SQL cursor.

`managed_source`, `managed_producer`, `managed_dispatch` and
`managed_retirement` reserve the reviewed immutable source/attempt identities
and journal. There is no public insert/proof constructor. The current-attempt
binding is `managed_outbox_dispatch(outbox_id, dispatch_id)`, rather than an
ALTERed outbox column: this preserves migration SQL rerun idempotence. Deleting
the current outbox row cascades only that binding, not historical dispatches or
their unknown exposures. Retry-reset counters do not identify a dispatch.

Sources/dispatches use AUTOINCREMENT IDs and unique 128-bit lowercase hex
nonces. Producer PID/start/host/boot/scope fields may remain absent, meaning
unprovable; an exited leader or restart cannot populate positive settlement.
The later Core authority layer must validate the exact request, all-reader
`on_success` promise, response and owner before constructing any certificate.

## Atomic writer matrix

Each path-bearing table has INSERT/UPDATE/DELETE triggers. They update literal
refs and a dirty revision in the same SQLite statement, on every connection.
Only reviewed typed helpers acknowledge their own exact dirty revision inside
the same SAVEPOINT as their write. Helpers work both standalone and under an
existing transaction; failure rolls back data and index together. A direct SQL
write stays dirty until bounded revalidation.

| Consumer | Production writers integrated through the typed query helper | Indexed fields |
| --- | --- | --- |
| message | insert/update used by local echo, adoption/merge, sent results, edits and ordinary ingest | media, link preview; conservative path-like slots in extension/other structured message payloads |
| message_version | insert_message_version during edits/merge snapshots | media |
| outbox | insert_outbox used by ordinary and checked admission | full canonical params, including nested file/album/edit paths |
| media_file | upsert_media_file / delete_media_file / account cleanup | path, account/message hints |
| conversation | insert_conversation / update_conversation | avatar, local wallpaper and conservative extension paths |
| platform_handle | insert_handle / update_handle / set_handle_avatar / set_handle_status | avatar, conservative extension/status paths |
| plugin | upsert_plugin, including Accounts' separate SQLite connection | icon |

Deletes remove refs/control rows atomically. FK SET NULL changes to media-file
message hints or outbox echo hints also trigger conservative dirty reindexing;
the bounded dirty pass acknowledges these changes, not an unrelated writer.
Tombstones, delivery/progress, flags, notification preferences and text-only
Outbox state updates do not create a new path. FTS updates are avoided during
backfill by updating only a path-bearing column to itself.

The production SQL inventory found no path creation bypass outside these ch-db
helpers. Core's direct worker/retry SQL changes only state/error/progress fields.
Other SQL in App source is isolated test setup. The metadata-only current
manifest/install directory is not treated as an attachment file reader; arbitrary
extension strings are not permissions. Future path-reading APIs must join the
Core source gate before the registration/dispatch/release feature is enabled.

## Bounds, coverage and revision semantics

Triggers use CASE to replace null/invalid/over-64-KiB JSON with `null` **before**
calling json_tree. Extraction visits only named `path`/`*_path` slots, not text
bodies or arbitrary string values. Scalar paths, literal paths and JSON slots
are bounded at 4096 bytes. Duplicate keys, unsupported path fields, invalid
shapes and oversized values stay stored, with a blocked consumer row. Typed
helper acknowledgement cannot turn a blocked row clean. Unknown nonpath JSON
fields are never rewritten by extraction/backfill.
Oversized consumer keys and owner hints also block extraction rather than being
loaded into reference results. Malformed long text cursors remain entirely
inside SQLite, so backfill can advance past them without copying unbounded keys
into Rust. Candidate byte budgets include owner hints.

The singleton `managed_ref_clock` allocates a durable global revision for each
path write, so deleting/reusing a consumer row ID cannot reuse its old revision.
Normal write acknowledgement is private and occurs while SQLite's writer is
held. Migration rerun preserves that counter, revisions and coverage cursors.

`backfill_managed_references` reserves the SQLite writer before reading a page.
It scans primary-key ranges (integer IDs, binary text paths/plugin IDs), at most
128 rows / 1 MiB charged input per call. Oversized rows advance the cursor and
remain blocked. Changes behind a cursor and reused IDs enter the dirty index;
after cursor coverage, bounded dirty work revalidates them. There is no OFFSET
or nullable-OR predicate that scans all preceding history on each page. No
history is scanned by migration or database open.

`managed_references(source_id)` uses source/path indices, returns at most 256
actual source candidates / 1 MiB, and explicitly marks a truncated result. A
database with 100,000 unrelated messages does not enlarge this query. Retained
canonical outbox params remain intent metadata; future removal must not delete
them or infer resend permission from their path.

## Boundary for the next Core/FS/Host milestone

IDs have private constructors and are read from persisted rows. They convey
identity only, not authority. Public seams in this milestone are:

- `managed_source_id` and `managed_current_dispatch` metadata lookups;
- `managed_reference_coverage` and `backfill_managed_references`;
- bounded `managed_references`, with consumer/key/column/exact SQLite JSON slot,
  revision and denormalized owner hints. The actual owner must be re-read.

Every newly extracted ref is `unresolved`; its optional dev/ino/identity fields
are NULL. A symlink, hardlink, missing file or different pathname is never
classified independent by this layer. Later anchored FS classification must
validate all relevant identities and writer/source leases; nullable resolution
columns/index are reserved for that real proof-backed work, with no setter yet.

The next lane must also coordinate every live reader/new ref writer with source
retirement exclusion. The SQL guard rejects bounded recognized literal
reattachment to retiring/released paths; unsupported/oversized payloads are
blockers, not trusted source-free writes. That guard alone is not a substitute
for Core writer/source gating, alias proof or the retirement journal. Source
registration and cleanup remain disabled until the whole chain is reviewed.

Required runtime gates remain: fused owner+Gate capture, no account storage guard
across a network wait, unique dispatch nonce before possible exposure, durable
unknown whole-holder barriers across restart, positive all-reader settlement,
atomic owned-path detachment, exact FD unlink/fsync and durable absence. No API,
App command, platform hardcode or filesystem cleanup is added here.
