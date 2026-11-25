# MAX plugin foundation (partial)

This is an **offline-testable foundation, not a complete API 1.0 plugin or MAX
parity implementation**. In particular, required `messages.fetch_recent` supports
only the first server page; its continuation fails explicitly. Do not ship this
as a fully conformant messenger plugin until that limitation is resolved.

## Install and test

From this directory, prepare the interpreter before plugin discovery:

```sh
uv sync --locked
uv run --locked pytest
```

The manifest invokes the prepared `.venv/bin/python`; the host does not install
packages. Runtime dependency is exactly `maxapi-python==2.5.0`, resolved with all
transitive artifacts in `uv.lock`. MIT license and the upstream copyright notice
are included in `THIRD_PARTY_LICENSES.txt`. The SDK is the repository path
`../../sdk/python`. The private file locking currently requires POSIX.

Tests use synthetic temporary directories, the actual pinned models/payloads,
injected adapters/transport, and the SDK's schema-checked FakeCore. They do not
connect, authenticate to MAX, discover environment tokens, access a keyring, or
import the legacy backend. The wire injection entry point is test-only; the
production entry point always uses `PyMaxAdapter`.

Local quiet-night validation initially performed source AST parsing, JSON parsing,
`git diff --check`, and `uv lock`. Dependencies/test tools were not installed on the
host; pytest execution belongs to CI. No real-server behavior has been verified.

## Implemented and deliberately bounded

| Area | Behavior | Remaining limitation |
|---|---|---|
| SDK lifecycle | Fast account acceptance, ordered initial status, responsive ping, bounded cleanup on stop/shutdown/EOF; saved-session reconnect with exponential backoff capped at 60 seconds | No server/network authentication exercised |
| Account isolation | Explicit per-account `state_dir`, account owner binding, exclusive lifetime lock, separate device and instance IDs | POSIX file locking; no legacy data import |
| Session storage | Custom PyMax `StoreProtocol`; atomic private JSON, directory 0700/files 0600, durable fsync, no symlink/hardlink session reads | Session tokens are stored unencrypted in the private directory, like the upstream SQLite session |
| Device identity | Persist device ID, instance ID, selected phone/QR transport and generated user agent before connection; preserve session sync markers and rotated token | No device/transport migration UI |
| Auth | Generic SDK phone, code, optional server-requested password challenge (three attempts), QR link/expiry/poll/confirmation; cancel/restart and rejected-input steps; no console providers | Existing accounts only; no registration, logout/settings for 2FA, email recovery or imported sessions |
| Initial data | Emit login contacts/chats/messages and additional validated chat pages seeded by a present LOGIN `chatMarker`; buffer live events until discovery finishes | Absent login marker remains unknown/partial; no `conversations.synced` deletion/reconciliation signal or claim of exhaustive membership |
| Live data | New/edited message upserts, chat updates, scoped message deletions | No durable event replay, read/presence/reaction/contact-change projection or gap closing |
| Text send | One literal-text request using pinned payload models; basic same-chat reply; confirm only the returned outgoing platform message; emit the confirmed upsert | Media, entities, silent/scheduled/topic/comment/effect sends are not advertised |
| Local send journal | A supplied `client_id` binds to the exact chat/text/reply payload. Confirmed retries return and re-emit the stored message. Authoritative results/upserts preserve the exact ID in `ext.max.client_id`, including an empty ID. Pending/unknown retries fail without sending again, across restart | Does **not** claim server deduplication or exactly-once delivery. No `client_id` means no durable local replay protection. Journal is capped at 10,000 entries; every private state file has a 64 MiB limit. New keyed sends are refused when full |
| Delete | Fresh chat-scoped message preflight, one delete request, exact chat/ID acknowledgment, then scoped `messages.deleted` and `{}` success. `revoke=false` supports incoming messages; `revoke=true` requires outgoing sender identity | At most 100 input IDs, duplicates normalized; regular `USER` messages only. Comments, scheduled/service messages, admin deletion of others' messages and automatic replay are unsupported. Server permissions still apply |
| Recent history | Request a newest page, deduplicate IDs and sort by timestamp/numeric ID descending; bounded limit 1–200 | Continuation is unsupported; even short/empty pages cannot falsely close history |

Only `auth.interactive`, `delete` and `delete.for_me` are advertised. Optional `chat.sync`, `contacts.list`,
`history.fill_gap`, `history.jump_to_date`, attachments, search, editing and other
capabilities remain absent. Account ghost mode and unsupported request parameters
are rejected explicitly. Auto-download has no effect while media is unsupported.
Full chat/contact reconciliation needs a separately proven enumeration strategy.

`request_timeout` is a validated integer (5–60 seconds). Optional `proxy` is typed
and marked `x-secret`; no credentials are accepted through environment discovery.
The library's raw logger is suppressed independently of the core log level,
because upstream debug frames can contain tokens and message text. Errors exposed
to the core are fixed descriptions, with no raw API exception payloads. Stdout is
reserved for SDK JSON-RPC frames.

## Bounded chat discovery

The pinned public `on_raw` hook observes LOGIN response metadata that `App.start`
does not expose through `client.chats`. A present integer `chatMarker` seeds
command-53 requests. The adapter follows each exact returned marker, with a
cancellable two-second pause before each request. Explicit zero makes no request;
absent LOGIN marker remains unknown and preserves the original partial upserts.
Present null/string/bool/float markers, missing/invalid page fields, invalid chat
models, repeated/cyclic cursors, or budget exhaustion fail startup. Markers have
no invented monotonic direction. Budgets are 128 pages, 10,000 received chat rows
(including login and duplicates), and 300 seconds; these are local safety limits.
No partially drained result is published as a successful discovery. A fatal live
event-buffer overflow cancels pending discovery, and account stop owns its tasks.

This behavior is grounded in the actual request/ack schema and page-drain call
sites of the [public MAX web bundle](https://web.max.ru/_app/immutable/chunks/D0OzoqHp.js)
captured 2026-11-25 Moscow, SHA-256
`3d300974d8508a3674a5f56e4be618249fb4dea8661044398fbc96f91b32a181`.
Only original adapter logic is included. A terminal marker proves that this page
chain ended; it does not establish deletion-authoritative full snapshot semantics.
No `chat.sync` capability or `conversations.synced` event is added. Required
message-history continuation remains explicitly unsupported.

## Send uncertainty

Before a keyed send, a durable `unknown` record is written. Once a platform message
confirms success, the record becomes `confirmed`. A network failure, cancellation,
malformed response, process death, or a crash between the response and journal
commit leaves uncertainty. It must be reconciled manually; it is never retried as
a known-safe send. Error replies use `NETWORK` with `outcome: "unknown"` and
`retry_safe: false`, never the safe pre-send `UNAVAILABLE` code.

The upstream `send_message` generates its own `cid` and parses Markdown. The adapter
instead uses `SendMessagePayload`/`SendMessagePayloadMessage` with empty elements,
then the same `App.invoke(MSG_SEND)` and response model path. This preserves literal
text and sends once. A persisted monotonic `cid` preserves per-device sequence
continuity; it is correlation metadata, **not proof of server idempotency**.

## Confirmed deletion and editing boundary

Deletion first sends pinned `GetMessagesPayload` through `App.invoke(MSG_GET)`.
Every selected ID must be present exactly once, in the requested chat context,
with a sender, regular message type and no scheduled/comment identity. Explicit
envelope/message chat contradictions are rejected. An absent message `chatId`
uses its request parent, as the pinned library does. For everyone, sender must
match this account's authenticated self; for me, incoming messages are allowed.
The account connection generation is checked again before the mutation. A failed
preflight is a safe pre-dispatch failure; it does not report an uncertain write.

`DeleteMessagePayload` sends `forMe = !revoke`. The pinned convenience method
discards its response and unconditionally returns true, so it is not used.
The actual first-party web command-66 binding instead requires acknowledgment
`chatId` and `messageIds`, with optional comment `postId`. Its deletion caller
maps `forAll` to inverse `forMe`, uses only returned IDs and rolls back missing
ones. This adapter requires the exact whole selected ID set after normalizing
duplicate/order differences, the same chat, and no comment field. Partial/empty,
foreign, malformed or lost acknowledgments never become `{}` success or a
fabricated full deletion event. Independent live notifications can still arrive.

Exactly one mutation request is made; batches are never split or automatically
retried. Cancellation after dispatch and any uncertain response use `NETWORK`,
`outcome: "unknown"`, `retry_safe: false`. The frozen delete method has no request
key; no durable replay/idempotency guarantee is added. A later explicit repeat
must pass a fresh preflight and may return not-found. This is not a promise that
the entire batch was unchanged when the result is unknown.

These response and flag semantics come from the same hashed first-party bundle
linked above: command schemas/bindings at decoded-character offsets 205230 and
216323, actual deletion loop at 784470. Request models and the lossy wrapper are
in the pinned `api/messages/payloads.py` and `api/messages/service.py`. All adapter
logic is original; no minified source was copied. Live server behavior remains
unverified.

Editing remains undeclared and rejected before dispatch. The pinned edit helper
parses Markdown; a direct literal-text payload is possible, but that alone does
not prove editability. The first-party client also checks a no-edit bit, server
`edit-timeout`, attachment/link category and chat write rights. The current
adapter does not retain/reconcile that configuration or implement those chat
permission rules. No timeout, edit window or rights fallback is guessed here.

## History safety and contract gap

The pinned `ChatHistoryPayload` has `from` as a timestamp and unconstrained integer
`backward`/`forward`. `fetch_history` returns message models only. Neither it nor the
upstream tests establish an authoritative maximum page size, inclusive/exclusive
same-millisecond boundary, or server continuation/exhaustion marker. Opcode names
and a short page are not evidence of completeness.

The first page always returns an opaque, versioned account/chat-scoped cursor.
Using that cursor produces `NOT_SUPPORTED` before another server request or any
history mutation. Repeated calls and restarts fail in the same way. A cursor from
another account/chat produces `INVALID_PARAMS`. No cache grows with requests.
This is intentionally partial required-method coverage, not pagination support.

Future timestamp/ID pagination must first prove the server boundary semantics and
count cap, preserve overlaps, and fail while keeping the gap open when same-ms
saturation cannot progress. It must never turn an ambiguous/repeated page into
`exhausted=true`, drop the cursor to signal completion, or subtract one millisecond
and silently skip messages. Date jumping and gap filling remain undeclared.

## Pinned primary source evidence

The implementation was checked against PyPI `maxapi_python-2.5.0` source and
[PyMax commit c4cb917d354a1179be625925530d72c8aaa969ff](https://github.com/MaxApiTeam/PyMax/tree/c4cb917d354a1179be625925530d72c8aaa969ff).
The inspected `app.py`, `base.py`, auth service, message service/payloads,
session models, and LICENSE are byte-identical between those two source snapshots.

- `base.py`: `connect()` connects once; `start()` owns a separate reconnect/relogin
  loop. This adapter uses `connect()`, `wait_closed()` and its own SDK account task.
- `app.py`: injected store loading/saving, handshake device identity, token rotation,
  login snapshots and `invoke`'s single connection request. Library reconnect and
  automatic token deletion/relogin are disabled.
- `api/auth/service.py`, `auth/sms.py`, `auth/qr.py`: actual phone/code/password and
  QR request/check/confirm service calls and millisecond QR interval/expiry.
- `api/messages/service.py`, `api/messages/payloads.py`: actual literal payload,
  response model, timestamp-only history request and absent page-cap evidence.
- `dispatch/mapping.py`, `base.py`: real message/edit/delete/chat callback routing.
- `session/models.py`: token, phone, device/user agent, instance ID and sync state
  preserved by the custom store. Legacy `Auth` versus newer `sessions` migration
  is a separate future synthetic workflow; no old database is opened here.

No GPL code or third-party history implementation was copied. All MAX server
acceptance, cursor semantics, source completeness, device acceptance, QR behavior,
and send deduplication remain unverified against a real account.
