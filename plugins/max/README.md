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
| Initial data | Emit contacts, conversations and text messages actually returned by login; buffer live events until the snapshot is published | No claim that login enumerates every chat/contact; no `conversations.synced` deletion/reconciliation signal |
| Live data | New/edited message upserts, chat updates, scoped message deletions | No durable event replay, read/presence/reaction/contact-change projection or gap closing |
| Text send | One literal-text request using pinned payload models; basic same-chat reply; confirm only the returned outgoing platform message; emit the confirmed upsert | Media, entities, silent/scheduled/topic/comment/effect sends are not advertised |
| Local send journal | A supplied `client_id` binds to the exact chat/text/reply payload. Confirmed retries return the stored message. Pending/unknown retries fail without sending again, across restart | Does **not** claim server deduplication or exactly-once delivery. No `client_id` means no durable local replay protection. Journal is capped at 10,000 entries; every private state file has a 64 MiB limit. New keyed sends are refused when full |
| Recent history | Request a newest page, deduplicate IDs and sort by timestamp/numeric ID descending; bounded limit 1–200 | Continuation is unsupported; even short/empty pages cannot falsely close history |

Only `auth.interactive` is advertised. Optional `chat.sync`, `contacts.list`,
`history.fill_gap`, `history.jump_to_date`, attachments, search, mutation and other
capabilities remain absent. Account ghost mode and unsupported request parameters
are rejected explicitly. Auto-download has no effect while media is unsupported.
Full chat/contact reconciliation needs a separately proven enumeration strategy.

`request_timeout` is a validated integer (5–60 seconds). Optional `proxy` is typed
and marked `x-secret`; no credentials are accepted through environment discovery.
The library's raw logger is suppressed independently of the core log level,
because upstream debug frames can contain tokens and message text. Errors exposed
to the core are fixed descriptions, with no raw API exception payloads. Stdout is
reserved for SDK JSON-RPC frames.

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
