# Contact Hub plugin API, version 1.0.0

This is the public contract between the Contact Hub core (the "host") and a
messenger plugin. It is self-contained: with this document and the JSON Schemas in
`schemas/` you can write a plugin for any platform (Discord, Signal, Slack, Matrix,
…) in any language, without reading core code.

The words MUST, SHOULD and MAY are used as in RFC 2119.

Contents

1. Model and vocabulary
2. Transport and framing
3. Manifest and discovery
4. Lifecycle
5. Multi-account addressing
6. Settings and secrets
7. Interactive auth
8. Conventions: ids, timestamps, text offsets, nullability, extensions
9. Data model (DTOs)
10. Events (plugin → core)
11. Requests (core → plugin)
12. Requests (plugin → core)
13. Errors
14. Capabilities
15. Media, files and transfers
16. Backpressure and batching
17. Ghost mode
18. Versioning and stability
19. Trust model
20. Checklist for a new plugin

Changes since the first draft (still 1.0.0: nothing had shipped). Merge-updated DTOs
(`Conversation`, `Topic`, `Message`) lost their on-wire defaults: every non-required
field is nullable and absent means *unchanged* (§8.4). `ErrorCode` and the numeric
code are open enums (§13). The method registry carries the capability-gated
parameters (`gated_params`) and `default_timeout_seconds` (§4.6, §12, §14.2).
Lifecycle corner cases are written down: version mismatch and requests before
`initialize` (§4.1), duplicate `account.start`, event ordering around the
`account.start` reply, `disconnected` before the `account.stop` reply (§4.2), EOF with
requests in flight (§2.5), echoes of core-initiated changes (§10.1), settings
defaults (§6.1), installation (§3.4), vendor methods (§12.3), local search (§11.4).

---

## 1. Model and vocabulary

- **Core / host**: the Contact Hub process. It spawns plugins, owns the database
  and the UI, and is the only thing a plugin talks to.
- **Plugin**: one executable per platform (`telegram`, `whatsapp`, `discord`, …),
  running as a child process of the core. One plugin process serves every account
  of its platform.
- **Account**: one signed-in identity on a platform. Identified by an
  `account_id` that the core assigns (§5).
- **Platform**: the messenger itself. The plugin's `id` names it.
- **DTO**: a JSON object a plugin emits or returns (`Message`, `Conversation`, …).
  DTOs are *raw platform truth*: the core normalises them and never asks a plugin to
  remember what it already sent.
- **Event**: a JSON-RPC notification from plugin to core.
- **Request**: a JSON-RPC request. Most go core → plugin; a few go the other way.
- **Capability**: a string key a plugin declares in `initialize`. The UI shows a
  control only when its capability is present; the core calls a method only when its
  capability is present.

The core is the single source of truth for the UI. A plugin never has to serve
reads from a local cache: it pushes data as it learns it, and answers the core's
requests from the platform.

## 2. Transport and framing

### 2.1 Channel

The transport is **JSON-RPC 2.0 over the plugin's standard streams**:

- the core writes to the plugin's **stdin**;
- the plugin writes to its **stdout**;
- the plugin's **stderr** is free-form text for logs. The core captures it to
  `logs/plugins/<plugin-id>.log`, line-buffered, and never parses it. Write anything
  you like there; write **nothing but protocol frames to stdout**. A stray
  `print()` on stdout is a protocol error and gets the plugin restarted.

Both directions carry requests, responses and notifications, so the same framing
applies to both.

### 2.2 Framing: NDJSON

One JSON-RPC message per line:

- UTF-8, no BOM.
- Exactly one JSON value per line, terminated by a single `\n` (0x0A). A `\r`
  before the `\n` is tolerated on input and never emitted.
- The JSON text itself MUST NOT contain raw newline characters; the JSON string
  escape `\n` is what carries newlines inside text. Any compliant JSON encoder does
  this already.
- Maximum frame length is **16 MiB** (16 777 216 bytes, including the newline). A
  longer line is a fatal protocol error: the receiver closes the stream. Keep event
  batches well under this (§16).
- Empty lines are ignored.
- **JSON-RPC batch arrays are not supported.** A frame whose top-level value is an
  array is answered with `INVALID_REQUEST` (-32600). Every message is its own frame.
  (Batches buy nothing here: NDJSON already streams, and one-message frames keep
  cancellation and progress correlation trivial.)
- Frames are processed in order of arrival. Requests MAY be answered out of order
  (both sides MUST tolerate that); notifications are delivered in order.

Why NDJSON rather than `Content-Length` headers (LSP style)? Every language can
produce it with "serialise, append newline, flush"; it is greppable in logs; and a
corrupt frame costs one line, not the rest of the stream.

### 2.3 JSON-RPC envelope

Standard JSON-RPC 2.0. Three message shapes:

```json
{"jsonrpc": "2.0", "id": 7, "method": "messages.send", "params": {…}}
{"jsonrpc": "2.0", "id": 7, "result": {…}}
{"jsonrpc": "2.0", "id": 7, "error": {"code": -32001, "message": "…", "data": {"code": "NOT_SUPPORTED"}}}
{"jsonrpc": "2.0", "method": "messages.upsert", "params": {…}}
```

- `id` is a number or a string, never `null`. Each side picks ids for the requests
  it sends; they only need to be unique among that side's outstanding requests. The
  core uses integers; plugins MAY use strings.
- `params` is always a JSON **object** (named parameters), never an array. It MAY be
  omitted for methods that take no parameters (`ping`, `shutdown`), but `{}` is
  equally valid.
- `result` is always a JSON object, `{}` when there is nothing to say. Never `null`.
- `error` follows §13.
- A notification has no `id` and never gets a reply.
- Any message carrying an unknown `method` is answered with `METHOD_NOT_FOUND`
  (-32601) if it is a request, or ignored if it is a notification.

### 2.4 Flow control on the pipe

The core reads stdout continuously and never stalls on it, so a plugin's writes do
not block for long. The plugin MUST likewise keep reading stdin while it is busy;
run the reader on its own task or thread. §16 describes the cooperative
backpressure built on top of this.

### 2.5 Shutdown and EOF

EOF on stdin means the core is gone: the plugin MUST exit promptly (within 5 s),
flushing nothing more than it already has. The orderly path is §4.5.

Requests still in flight at EOF: the plugin SHOULD still write the responses of
those that finish within its exit budget (the core may still be reading stdout — it
closes stdin first and reads until the plugin exits), and MAY drop the rest; it
MUST NOT wait for them. The core never closes stdin before it has read the
`shutdown` response or the shutdown grace has expired.

## 3. Manifest and discovery

### 3.1 Discovery

A plugin is a **directory** containing `plugin.json` at its top level. The core
scans, in order:

1. `<install>/plugins/*/plugin.json` (built-in plugins);
2. `$XDG_DATA_HOME/contact-hub-ng/plugins/*/plugin.json` (user plugins; default
   `~/.local/share/contact-hub-ng/plugins/`). `contact-hub-ng` is the data directory
   fixed by ADR 0001 and is the same for every platform-specific location.

A user plugin with the same `id` as a built-in one shadows it. Built-in plugins use
this same mechanism, with no private shortcuts.

### 3.2 `plugin.json`

```json
{
  "id": "discord",
  "name": "Discord",
  "version": "0.3.1",
  "api_version": "1.0.0",
  "entry": ["./venv/bin/python", "-m", "discord_plugin"],
  "icon": "icon.svg",
  "brand_color": "#5865F2",
  "multi_account": true,
  "auth_methods": ["token", "qr"],
  "settings_schema": {
    "type": "object",
    "properties": {
      "proxy": {"type": "string", "title": "SOCKS5 proxy", "default": ""},
      "token": {"type": "string", "title": "User token", "x-secret": true}
    }
  },
  "description": "Discord DMs, servers and group DMs through a user token.",
  "homepage": "https://example.org/ch-discord",
  "author": "Jane Doe",
  "license": "MIT"
}
```

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `id` | string, `^[a-z][a-z0-9_]{1,31}$` | yes | The platform id. Appears in `account_id`s, log names and `ext` namespaces. Immutable once published. |
| `name` | string | yes | Human name of the platform ("Discord"). |
| `version` | semver string | yes | The plugin's own version. |
| `api_version` | semver string | yes | The API this plugin implements (this document: `1.0.0`). Compared by major version (§18). |
| `entry` | array of strings, ≥1 | yes | argv to spawn. `entry[0]` that starts with `./` is resolved against the plugin directory; otherwise it is looked up on `PATH`. The working directory is the plugin directory. |
| `icon` | string | no | Path, relative to the plugin directory, of an SVG or PNG. |
| `brand_color` | `#RRGGBB` | yes | Used for the platform badge. |
| `multi_account` | boolean | yes | Whether the core may start more than one account on this plugin process. |
| `auth_methods` | array of strings | yes | The sign-in methods the user may pick from in `auth.begin`. Known values: `phone_code`, `password_2fa`, `qr`, `email_code`, `token`, `oauth`, `username_password`, `none`. Unknown values are shown verbatim. |
| `settings_schema` | JSON Schema object | no | The per-account settings form (§6). Omit it when there is nothing to configure. |
| `description`, `homepage`, `author`, `license` | string | no | Shown in Settings → Accounts. |
| `env` | object of string → string | no | Extra environment variables for the process. |

The schema is `schemas/manifest.json`. Unknown fields are ignored.

### 3.3 Process environment

The core spawns `entry` with the plugin directory as the working directory, the
parent's environment plus `env`, plus:

- `CH_PLUGIN_ID`: the manifest `id`;
- `CH_API_VERSION`: the API version the core speaks;
- `CH_LOG_LEVEL`: one of `error`, `warn`, `info`, `debug`, `trace` (a hint).

Nothing else is passed through the environment. Everything the plugin needs to know
arrives in `initialize` and `account.start`.

### 3.4 Installation

`entry` MUST be runnable as it stands when the core finds the manifest: the core
only spawns it, it never builds, fetches or installs anything. Whoever puts the
directory in place prepares it — the app's build for built-in plugins, `ch plugin
install` for user plugins, the developer during development (for a Python plugin
that is `uv sync`, which creates the `.venv` that `entry` points at; for Rust it is
`cargo build --release`). A plugin whose `entry[0]` does not exist or is not
executable is shown as `error` with "not installed" and the path; the core does not
retry it until the directory changes.

## 4. Lifecycle

```
core                                   plugin
 │  spawn ───────────────────────────▶  │
 │  initialize ─────────────────────▶  │
 │  ◀──────────── {api_version, capabilities}
 │  account.start(acc A) ───────────▶  │
 │  ◀──── {}            (returns fast)  │
 │  ◀──── status(A, connecting) …       │  (events stream from here on)
 │  ◀──── status(A, connected)          │
 │  ◀──── conversations.upsert(A) …     │
 │  messages.fetch_recent(A, …) ────▶  │
 │  ◀──── {messages, next_cursor}       │
 │  ping ───────────────────────────▶  │   every 15 s
 │  ◀──── {}                            │
 │  account.stop(A) ────────────────▶  │
 │  shutdown ───────────────────────▶  │
 │  ◀──── {}   then process exits       │
```

### 4.1 `initialize`

The first message on the wire, always from the core. The plugin MUST NOT send
anything before answering it (stderr logs excepted). Params and result are in §11.1.
The plugin returns the `api_version` it implements and its capabilities (§14).

Version check. The plugin answers `initialize` normally whatever `api_version` the
core states, with its own `api_version`; it MUST NOT refuse because of the version.
The core decides: if the major versions differ it sends `shutdown` and marks the
plugin incompatible in the UI (§18). An SDK MAY log a warning.

Before `initialize` has been answered the plugin answers every other *request* with
`INVALID_REQUEST` and ignores every *notification* (including `$/cancelRequest`),
logging it to stderr. A second `initialize` on the same process is also
`INVALID_REQUEST`.

### 4.2 `account.start` / `account.stop`

Per account. `account.start` carries everything the account needs: its settings,
its secrets, its private state directory and its media cache directory (§11.1).

`account.start` MUST return within its timeout **without waiting for the network**.
Connecting happens in the background; the plugin reports progress through `status`
events. Returning `{}` means "I have accepted this account and will emit `status`
for it". The first `status` event SHOULD follow within a second, even if it is just
`connecting`.

Ordering around the reply. The plugin MUST NOT send any event for an account before
its `account.start` response is on the wire; events produced while starting are
queued and sent right after the response, in order (SDKs do this). The core ignores,
with a log line, events for an `account_id` that is not started on that process.

`account.start` for an `account_id` that is already started on this process is an
error: `INVALID_PARAMS` with `data.field = "account_id"`, and the running account is
left untouched. The core never does this on purpose: to restart an account it sends
`account.stop`, waits for the response, then `account.start`.

`account.stop` tears the account down (close sockets, flush its session). The
plugin SHOULD emit `status: disconnected` for the account *before* answering, and
the core marks the account `disconnected` itself when the response arrives, so a
plugin that omits the event loses nothing. After the response the plugin MUST emit
no more events for that `account_id`; the core drops any that still arrive. Starting
the same `account_id` again later is allowed. `account.stop` for an account that is
not started answers `UNKNOWN_ACCOUNT`; the core treats that as "already stopped".

### 4.3 `ping`

The core sends `ping` every **15 s** per plugin process. The plugin MUST answer
`{}` within **10 s**, from a task that is not blocked by platform I/O. Three
consecutive misses count as a hang: the core kills and restarts the process
(§4.6). A plugin MAY include `{"busy": true}` in the result when it is in the
middle of a long operation; the core shows a spinner but treats it as alive.

### 4.4 Account states

Every account is in exactly one state, reported through the `status` event:

| state | meaning |
| --- | --- |
| `disconnected` | not started, or stopped |
| `connecting` | trying to reach the platform |
| `auth_required` | reachable, but the account must sign in (§7) |
| `connected` | live |
| `unavailable` | the platform cannot be reached at all (e.g. blocked without a VPN). The plugin keeps retrying on its own with backoff and reports `connected` when it succeeds. Never an error. |
| `error` | something the plugin cannot recover from alone; `detail` says what. The core offers "Retry", which is `account.stop` + `account.start`. |

plus a `reconnecting` flag that rides on `connecting` or `unavailable` and means
"this account was live and I am bringing it back by myself".

A plugin MUST NOT fail `account.start` just because the platform is unreachable;
that is `status: unavailable`.

### 4.5 `shutdown`

The core sends `shutdown` (no params) when the app quits or the plugin is being
disabled. The plugin stops every account, answers `{}`, and exits with status 0.
Timeline: `shutdown` → 10 s grace → `SIGTERM` → 5 s → `SIGKILL`. Answering `shutdown`
and then taking a few hundred milliseconds to exit is fine.

### 4.6 Timeouts and restart

Every core → plugin request has a timeout. If it expires the core answers its own
caller with `TIMEOUT` and sends `$/cancelRequest` (§11.0) to the plugin for that id.

| request | timeout |
| --- | --- |
| `initialize` | 30 s |
| `ping` | 10 s |
| `account.start`, `account.stop` | 30 s |
| `shutdown` | 10 s |
| `media.download`, `messages.send_file`, `messages.send_album`, `group.edit_photo`, `profile.update` with a photo | 10 min, **reset by each `transfer.progress` for that request** |
| `messages.fetch_recent`, `history.*`, `media.search`, `replies.list`, `members.list`, `chats.sync` | 120 s |
| everything else | 60 s (`default_timeout_seconds` in the registry) |

Plugin → core requests (§12) have the same default: `secrets.*` and events sent as
requests time out on the plugin's side after 60 s; the plugin then treats the call
as `TIMEOUT` and SHOULD send `$/cancelRequest`. The registry
(`schemas/methods/registry.json`, `ch_protocol::METHODS`) is the machine-readable
form of this table; the SDKs read their timeouts from it.

Restart policy. The core restarts the plugin process when it exits unexpectedly,
hangs (§4.3), or breaks framing (§2.2). Backoff is exponential: 1 s, 2 s, 4 s, …
capped at 60 s, reset after the process has been healthy for 5 min. After ten
failures in a row the core stops trying and shows the plugin as `error` with a
"Restart" button. On every restart the core re-runs `initialize` and then
`account.start` for each account that was started before the crash. Capabilities
are re-read on each `initialize`.

Requests that were in flight when the process died are answered to their callers
with `INTERNAL` ("plugin restarted"). The plugin is expected to be idempotent for
the operations the core retries (sync, fetch); the core never retries a `messages.send`
by itself.

## 5. Multi-account addressing

A plugin with `multi_account: true` may receive several `account.start` calls with
different `account_id`s and MUST serve them concurrently and independently (separate
sessions, separate state directories, separate `status`). With `multi_account:
false` the core starts at most one.

`account_id` is an **opaque, non-empty string chosen by the core**, stable for the
account's lifetime (across restarts of the app) and unique within the core. Plugins
MUST NOT parse it. (In practice it looks like `telegram` or `telegram#2`; do not
rely on that.)

**Every account-scoped request and every event carries `account_id`** as a
top-level field of `params`. The only exceptions are the process-scoped messages:
`initialize`, `shutdown`, `ping` and `$/cancelRequest`.

A request for an `account_id` that is not started is answered with
`UNKNOWN_ACCOUNT`.

## 6. Settings and secrets

### 6.1 `settings_schema`

A plugin that needs configuration describes it as **JSON Schema (draft 2020-12)**
in the manifest's `settings_schema`. The core renders a form from it with no
platform knowledge, validates the user's input against the schema, stores the
values, and hands them to the plugin in `account.start` as `settings`.

Defaults. The core fills in the `default` of every top-level property the user has
not set, so `settings` arrives complete: a plugin can read `settings.proxy` without a
fallback. Properties without a `default` that the user left empty are absent. (SDKs
apply the same defaults again from `plugin.json`, so a plugin behaves the same
under a test harness that does not.)

Supported subset for rendering: `type` ∈ `string | integer | number | boolean`,
`enum`, `title`, `description`, `default`, `minimum`, `maximum`, `minLength`,
`maxLength`, `pattern`, `format` (`uri`, `hostname`, `email`), `required`. Nested
objects and arrays are stored and passed through but rendered as a JSON text box.

Extension keywords (all optional, all prefixed `x-`):

| keyword | on | meaning |
| --- | --- | --- |
| `x-secret: true` | a string property | The value is a secret (§6.2). Rendered as a password field. |
| `x-widget` | a property | Rendering hint: `password`, `textarea`, `file`, `directory`. |
| `x-order` | a property | Integer sort key for the form. |
| `x-group` | a property | Section title to group fields under. |

### 6.2 Secrets

Secrets never live in plain settings storage:

1. Values of properties flagged `x-secret: true` are written to the **OS keyring**
   (or the core's encrypted store) by the core when the user saves the form. The
   plain settings row holds no copy.
2. At `account.start`, the core reads them back and merges them into `settings`,
   so the plugin sees one flat object with every value present. The plugin MUST NOT
   persist those values anywhere itself (not in `state_dir`, not in logs).
3. Secrets a plugin *obtains at runtime* (a session token after QR login, device
   keys, refresh tokens) go to the core through `secrets.set` (§12.1) and come back
   at every `account.start` in the separate `secrets` object. Keys are plugin-chosen
   strings; values are strings (base64 anything binary). Use `secrets.delete` on
   sign-out.
4. Session *files* that are large or that a library insists on owning (a Telethon
   `.session`) MAY live in `state_dir`, which the core never reads and never
   purges. Prefer `secrets.*` for anything that is a credential by itself.

Settings and secrets are per account. Changing settings in the UI results in
`account.stop` followed by `account.start` with the new values.

## 7. Interactive auth

Sign-in is **driven by the plugin and rendered by the core**. The core has no
per-platform sign-in UI; it draws whatever step the plugin asks for.

```
plugin ──── status(auth_required) ────────▶ core shows "Sign in" on the badge
core   ──── auth.begin{method:"phone_code"} ▶ plugin
plugin ──── auth.step{kind:"phone"} ───────▶ core renders a phone field
core   ──── auth.submit{step_id, value} ───▶ plugin calls the platform
plugin ──── auth.step{kind:"code", hint} ──▶ …
core   ──── auth.submit{…} ────────────────▶
plugin ──── auth.step{kind:"password", hint:"2FA"} ▶ …
plugin ──── status(connected) ─────────────▶ core closes the dialog
```

### 7.1 `AuthStep`

```json
{
  "flow": "sign_in",
  "step_id": "3",
  "kind": "code",
  "title": "Enter the code",
  "hint": "We sent a code to the Telegram app on your other device.",
  "placeholder": "12345",
  "can_resend": true,
  "error": null
}
```

| field | type | meaning |
| --- | --- | --- |
| `flow` | string | Which conversation this step belongs to. `sign_in` for login; `two_factor` for a password change that needs an e-mail code (`two_factor.set`); plugins MAY define others. |
| `step_id` | string | Opaque; echoed in `auth.submit`. |
| `kind` | enum | `phone`, `code`, `password`, `qr`, `email`, `email_code`, `text`, `info`, `done`. |
| `title`, `hint`, `placeholder` | string? | Human text; the core shows them verbatim. |
| `data` | string? | For `qr`: the payload to render as a QR code. |
| `expires_at` | timestamp? | For `qr` and `code`: when the core should ask for a new one (via `auth.begin` again). |
| `can_resend` | boolean | For `code`/`email_code`: the core shows "Resend", which is `auth.begin` with the same method. |
| `min_length`, `max_length` | integer? | Input constraints for `code`/`text`. |
| `error` | string? | The previous `auth.submit` for this flow was rejected; this is why. The step is being asked again. |

Kinds:

- `phone`, `code`, `password`, `email`, `email_code`, `text`: the core collects one
  string and sends `auth.submit`. `password` is masked.
- `qr`: the core renders `data`; there is nothing to submit. The plugin emits the
  next step (or `status: connected`) when the platform confirms the scan, and a new
  `qr` step when the old one expires.
- `info`: a message to show ("Confirm on your phone"); nothing to submit.
- `done`: the flow finished. For `sign_in` the `status` event does the same job,
  but `done` is what closes a `two_factor` flow.

### 7.2 Rules

- A plugin emits at most one *pending* input step per flow at a time. Emitting a new
  step replaces the one on screen.
- `auth.submit` returns `{}` when the value was accepted *for processing*. A wrong
  code is reported by re-emitting the step with `error`, not by failing the request.
  Only transport-level problems fail `auth.submit` (`NETWORK`, `FLOOD_WAIT`,
  `INTERNAL`).
- `auth.cancel` aborts the flow; the plugin returns the account to
  `auth_required` (sign-in) or does nothing further (other flows).
- Calling `auth.begin` while a flow is running restarts the flow. Plugins SHOULD
  reuse what they already know (the phone number) and resend the code.
- A plugin that signs in with nothing but its settings (a bot token) declares
  `auth_methods: ["none"]`, never emits steps, and never reports `auth_required`
  unless the token is rejected.

## 8. Conventions

### 8.1 Ids are opaque strings

Every id (`account_id`, `chat_id`, `message_id`, `user_id`, `topic_id`,
`folder_id`, `sticker_id`, `pack_id`, `session_id`, `poll` option ids, cursors) is a
**non-empty UTF-8 string of at most 256 bytes**. Consumers MUST NOT assume numeric
form, ordering or structure. Uniqueness:

- `chat_id`, `user_id`, `folder_id`, `sticker_id`, `pack_id`, `session_id`: unique
  within an account.
- `message_id`: unique within a chat. The core keys messages by
  `(account_id, chat_id, message_id)`.
- `topic_id`: unique within a chat.

Ids are case-sensitive and compared byte for byte. A plugin MUST emit the same id
for the same object every time (that is what makes re-sync idempotent).

### 8.2 Timestamps

Every timestamp is a string in **RFC 3339 UTC with exactly three fractional digits
and a `Z` suffix**: `2026-10-19T14:03:27.512Z`. Producers MUST emit exactly this
form. Consumers MUST accept any RFC 3339 `date-time` (other offsets, other
precisions) and normalise it to UTC milliseconds. Precision finer than a millisecond
is discarded. There are no unix integers anywhere in the API.

Field names ending in `_at` are timestamps. Durations are integers in seconds
(`duration`, `ttl_seconds`, `*_seconds`) unless the name says otherwise
(`*_ms`).

### 8.3 Text offsets are UTF-16 code units

`Entity.offset` and `Entity.length`, `quote_offset`, and every other position into
message text count **UTF-16 code units**, the way Telegram, JavaScript and Qt do.
An emoji outside the BMP is two units. Plugins written in languages with UTF-8 or
code-point strings (Python, Go, Rust) MUST convert. Offsets are relative to the
message's `text` (or the caption when the text is a caption).

### 8.4 Nullability

- A field that is **absent or `null` means "not reported / unknown"**. The two are
  equivalent everywhere; emitters SHOULD omit rather than write `null`.
- Required fields are listed per DTO. Everything else is optional.
- Booleans the platform cannot tell are left out (unknown), not defaulted to
  `false`. The schema marks each such field as nullable. Where a boolean is
  non-nullable the default is `false`.
- **Merge-updated DTOs: absent = unchanged.** `Conversation`, `Topic` and `Message`
  are rows the core keeps and that arrive repeatedly (`conversations.upsert`,
  `topics.upsert`, `messages.upsert`, `message.edited`, method results). For them
  every non-required field — scalars included — is nullable on the wire, and an
  absent or `null` field means "leave what you have". A partial update
  `{"id": "c1", "kind": "group", "unread_count": 3}` changes only the unread count.
  The "on create" value in the DTO tables (`archived: false`, `unread_count: 0`,
  `edited: false`, …) is what the core uses when it *creates* the row from an
  update that does not mention the field; it is never implied by a later update.
  To reset a field, send the value explicitly (`"unread_count": 0`). Emitters
  (and SDKs) MUST NOT write such defaults for fields they did not set. Nested value
  objects (`Media`, `Poll`, `Location`, `FolderRules`, …) and non-merged DTOs
  (`Folder`, `Sticker`, `AuthStep`) are replaced wholesale and keep their plain
  defaults.
- An **empty array** means "none" (the message has no reactions); an absent array
  means "not reported". The core treats an absent array as "leave what I have".
- `Conversation.draft`: a `Draft` with empty `text` means "no draft"; absent means
  "the platform did not say". This is how a platform without drafts never wipes a
  draft held locally.
- Strings are never empty where they are required; emit nothing instead of `""`.

### 8.5 Extensions (`ext`)

Every DTO has an optional `ext` object keyed by plugin id:

```json
"ext": {"telegram": {"peer_color": 3, "is_scam": false}}
```

The core stores `ext` verbatim, returns it to the UI untouched, and never
interprets it. Use it for platform-specific facts that have no neutral field. It is
not an excuse to skip neutral fields that exist.

### 8.6 Naming

Fields are `snake_case`. Methods and events are lowercase dotted
`namespace.verb`. Enum values are lowercase `snake_case` strings.

## 9. Data model (DTOs)

Schemas live in `schemas/dto/*.json`; the `$defs` name is the DTO name. Field tables
give `type`, `R` for required, and meaning. `?` after a type marks it nullable.
`ts` is a timestamp (§8.2), `id` an opaque id (§8.1).

### 9.1 `Contact`

A user on the platform as seen by this account.

| field | type | R | meaning |
| --- | --- | --- | --- |
| `id` | id | R | `user_id` |
| `display_name` | string? | | What to show. Plugins SHOULD always fill it. |
| `first_name`, `last_name` | string? | | Where the platform splits names. |
| `username` | string? | | Without the `@`. |
| `phone` | string? | | E.164 where possible (`+4915…`). Used for cross-platform person linking. |
| `avatar_path` | string? | | Local file (§15). |
| `is_bot` | bool? | | |
| `is_contact` | bool? | | In this account's address book. |
| `is_premium`, `is_verified`, `is_deleted` | bool? | | |
| `bio` | string? | | |
| `status` | `OnlineStatus`? | | Snapshot of presence at emit time. |
| `ext` | object | | §8.5 |

### 9.2 `Conversation`

Merge-updated (§8.4): absent = unchanged; "on create" values apply only when the
core first sees the chat.

| field | type | R | meaning |
| --- | --- | --- | --- |
| `id` | id | R | `chat_id` |
| `kind` | `dm` \| `group` \| `channel` | R | |
| `title` | string? | | For a DM the plugin SHOULD fill the peer's display name. |
| `peer_user_id` | id? | | DM only: the other party. |
| `parent_chat_id` | id? | | A channel inside another chat (Discord channels in a server). Forum topics are `Topic`s instead. |
| `username` | string? | | Public handle, if any. |
| `about` | string? | | Description. |
| `archived` | bool? | | on create false |
| `muted` | bool? | | Unknown on platforms without mute. |
| `mute_until` | ts? | | When `muted` ends; absent with `muted: true` means forever. |
| `pinned` | bool? | | |
| `pin_order` | integer? | | Position among pinned chats, 0 = top, when the platform orders them. |
| `folder_ids` | [id] | | Platform folders this chat is in. Absent = not reported. |
| `unread_count` | integer? | | on create 0 |
| `unread_mentions` | integer? | | on create 0 |
| `unread_reactions` | integer? | | on create 0 |
| `marked_unread` | bool? | | The "mark as unread" dot. |
| `read_inbox_max_id` | id? | | Highest incoming message id this account has read, on any device. |
| `read_outbox_max_id` | id? | | Highest outgoing message id the peer has read. |
| `last_message_at` | ts? | | For ordering the chat list before any message is fetched. |
| `is_self_chat` | bool? | | Saved Messages. on create false |
| `avatar_path` | string? | | |
| `member_count` | integer? | | |
| `can_post` | bool? | | False for a broadcast channel you only read. Unknown is treated as true. |
| `is_forum` | bool? | | Has topics (§9.6). |
| `draft` | `Draft`? | | §8.4 for the tri-state rule. |
| `color_index` | integer? | | Peer-chosen palette index for the name/placeholder avatar. |
| `scheduled_count` | integer? | | Messages waiting in the scheduled queue. |
| `auto_delete_seconds` | integer? | | 0 = off. |
| `slow_mode_seconds` | integer? | | |
| `permissions` | object of string → bool? | | Default member permissions, keys as in `group.permissions` (§14.3). |
| `is_verified`, `is_scam`, `is_restricted` | bool? | | |
| `ext` | object | | |

### 9.3 `Message`

Merge-updated (§8.4), including its arrays.

| field | type | R | meaning |
| --- | --- | --- | --- |
| `id` | id | R | |
| `chat_id` | id | R | |
| `topic_id` | id? | | Forum topic. |
| `direction` | `in` \| `out` | R | `out` = sent by this account. |
| `sent_at` | ts | R | For a scheduled message, when it will go out. |
| `sender_id` | id? | | Absent for anonymous channel posts. |
| `sender_name` | string? | | Display name at send time. |
| `sender_title` | string? | | "owner", "admin", or a custom rank, shown beside the name. |
| `text` | string? | | The text, or the caption when `media` is set. Absent for a service message. |
| `entities` | [`Entity`] | | Formatting spans over `text` (§8.3). |
| `media` | `Media`? | | |
| `reply_to` | `ReplyRef`? | | |
| `forward_from` | `ForwardInfo`? | | |
| `grouped_id` | id? | | Album: messages sharing it are drawn as one. |
| `reactions` | [`Reaction`] | | Empty = none. |
| `reactors` | [`Reactor`] | | Newest first, never this account; typically only on live `reaction.changed`. |
| `poll` | `Poll`? | | |
| `location` | `Location`? | | |
| `contact_card` | `ContactCard`? | | |
| `dice` | `Dice`? | | |
| `link_preview` | `LinkPreview`? | | |
| `buttons` | [[`InlineButton`]] | | Rows of a bot's inline keyboard. |
| `views` | integer? | | |
| `replies_count` | integer? | | Comments under a channel post; `replies.list` opens them. |
| `replies_chat_id` | id? | | The discussion chat those comments live in. |
| `effect_id` | id? | | Animation effect attached to the message. |
| `action` | `ServiceAction`? | | Set ⇒ this is a service message. |
| `mentioned` | bool? | | Mentions or replies to this account. |
| `edited` | bool? | | on create false |
| `edited_at` | ts? | | |
| `delivery` | `sending` \| `sent` \| `delivered` \| `read` \| `failed`? | | For `out` messages. |
| `read_by` | [id] | | Per-member read receipts where the platform gives them (WhatsApp). |
| `is_scheduled` | bool? | | on create false; `sent_at` is then the fire time. |
| `is_pinned` | bool? | | |
| `silent` | bool? | | Sent without notification. |
| `ttl_seconds` | integer? | | Self-destruct timer. |
| `via_bot_id` | id? | | |
| `ext` | object | | |

#### `Entity`

| field | type | R | meaning |
| --- | --- | --- | --- |
| `type` | enum | R | `bold`, `italic`, `underline`, `strike`, `spoiler`, `code`, `pre`, `blockquote`, `url`, `text_url`, `mention`, `mention_name`, `hashtag`, `cashtag`, `bot_command`, `email`, `phone`, `custom_emoji`, `bank_card` |
| `offset`, `length` | integer | R | UTF-16 units (§8.3). |
| `url` | string? | | `text_url` |
| `user_id` | id? | | `mention_name` |
| `language` | string? | | `pre` |
| `custom_emoji_id` | id? | | `custom_emoji`; the file comes from `emoji.custom_files`. |
| `collapsed` | bool? | | `blockquote` that starts collapsed. |

Entities MAY overlap and nest. The core renders unknown `type`s as plain text.

#### `Media`

| field | type | R | meaning |
| --- | --- | --- | --- |
| `kind` | enum | R | `photo`, `video`, `gif`, `sticker`, `voice`, `video_note`, `music`, `document`, `other` |
| `mime` | string? | | |
| `bytes` | integer? | | Size. |
| `filename` | string? | | |
| `width`, `height` | integer? | | |
| `duration` | integer? | | Seconds (audio, video). |
| `thumb_path` | string? | | Local thumbnail (§15). |
| `path` | string? | | Local full file, once downloaded. Absent ⇒ `media.download`. |
| `waveform` | [integer] | | Voice: amplitude bars, each 0–31. |
| `title`, `performer` | string? | | Music. |
| `has_spoiler` | bool | | default false |
| `sticker` | `StickerRef`? | | For `kind: sticker`: `{emoji?, pack_id?, format}`. |

#### `ReplyRef`

`{message_id: id (R), chat_id?: id (when the replied-to message is in another
chat), quote_text?: string, quote_entities?: [Entity], quote_offset?: integer,
sender_id?: id, preview?: string}`. `quote_*` is set when the sender quoted part of
the message; `preview` is a short text of the replied message for platforms that
embed it (so the core can render before it has the original).

#### `ForwardInfo`

`{name?: string, sender_id?: id, chat_id?: id, message_id?: id, at?: ts,
signature?: string}`. `name` is what to show when the source cannot be clicked.

#### `Reaction`

`{key: ReactionKey (R), count: integer (R), mine: bool (R), my_count?: integer}`.
`my_count` is for paid reactions (how many this account gave).

#### `ReactionKey`

Exactly one of: `{"emoji": "❤"}`, `{"custom_emoji_id": "…"}`, `{"paid": true}`.

#### `Reactor`

`{user_id: id (R), key: ReactionKey (R), name?: string, at?: ts, unread?: bool}`.

#### `Poll`

`{id?: id, question: string (R), options: [PollOption] (R), total_voters?: integer,
closed: bool, multiple: bool, quiz: bool, anonymous: bool, solution?: string,
solution_entities?: [Entity], close_at?: ts}`; booleans default false.
`PollOption`: `{id: id (R), text: string (R), votes?: integer, chosen?: bool,
correct?: bool}`.

#### `Location`

`{lat: number (R), lon: number (R), title?: string, address?: string, live: bool,
live_until?: ts, heading?: integer, accuracy?: number}`.

#### `ContactCard`

`{name: string (R), phone?: string, user_id?: id, vcard?: string}`.

#### `Dice`

`{emoticon: string (R), value: integer (R)}`; `value` 0 while rolling.

#### `LinkPreview`

`{url?: string, site?: string, title?: string, description?: string,
thumb_path?: string, media?: Media}`.

#### `InlineButton`

`{text: string (R), kind: enum (R), data?: string, url?: string}`; `kind` ∈
`callback`, `url`, `login_url`, `switch_inline`, `web_app`, `buy`, `other`. `data`
is opaque (echoed in `bots.press_button`).

#### `ServiceAction`

| field | type | R | meaning |
| --- | --- | --- | --- |
| `kind` | enum | R | `joined`, `left`, `pinned`, `title_changed`, `photo_changed`, `photo_removed`, `created`, `call`, `ttl_changed`, `topic_created`, `topic_edited`, `history_cleared`, `contact_joined`, `screenshot`, `other` |
| `actor_id` | id? | | Who did it. |
| `target_ids` | [id] | | Who it was done to. |
| `title` | string? | | `title_changed`, `created`, `topic_*` |
| `message_id` | id? | | `pinned` |
| `seconds` | integer? | | `ttl_changed`; `call` duration |
| `video`, `missed` | bool? | | `call` |
| `text` | string? | | `other`: a ready-made line to show. |

### 9.4 `Draft`

`{text: string (R), entities?: [Entity], reply_to?: id, updated_at?: ts}`. Empty
`text` = no draft.

### 9.5 `Folder`

| field | type | R | meaning |
| --- | --- | --- | --- |
| `id` | id | R | |
| `title` | string | R | |
| `emoticon` | string? | | |
| `position` | integer? | | Order in the folder bar. |
| `chat_ids` | [id] | | Explicitly included chats. default `[]` |
| `rules` | `FolderRules`? | | Absent ⇒ the folder is only its list. |
| `is_shared` | bool | | Joined via a shareable folder link; rules are read-only. |

`FolderRules`: `{include_kinds: [contacts | non_contacts | groups | channels |
bots], exclude_muted: bool, exclude_read: bool, exclude_archived: bool,
exclude_chat_ids: [id], pinned_chat_ids: [id]}`. A chat is in the folder when it is
in `chat_ids` or its kind matches `include_kinds`, and it is not excluded.

### 9.6 `Topic`

Merge-updated (§8.4).

| field | type | R | meaning |
| --- | --- | --- | --- |
| `id` | id | R | |
| `chat_id` | id | R | |
| `title` | string | R | |
| `icon_emoji` | string? | | |
| `icon_custom_emoji_id` | id? | | |
| `icon_color` | integer? | | 24-bit RGB. |
| `is_general` | bool? | | on create false |
| `unread_count`, `unread_mentions`, `unread_reactions` | integer? | | on create 0 |
| `pinned`, `closed`, `hidden` | bool? | | on create false |
| `last_message_at` | ts? | | |
| `last_message_preview` | string? | | |
| `draft` | `Draft`? | | |
| `read_inbox_max_id` | id? | | |

### 9.7 `Profile`

This account's own profile, or another user's full card.

`{user_id: id (R), display_name: string (R), first_name?, last_name?, username?,
phone?, bio?, avatar_path?, birthday?: string ("YYYY-MM-DD" or "--MM-DD" when the
year is hidden), is_blocked?: bool, is_contact?: bool, is_premium?: bool,
common_chats_count?: integer, status?: OnlineStatus, ext?}`.

### 9.8 `Session`

A logged-in device. `{id: id (R), device: string (R), platform: string (R) (OS),
app: string (R), ip?, country?, location?, created_at?: ts, active_at?: ts,
current: bool (R), official?: bool}`. `current` sessions cannot be terminated.

### 9.9 `Sticker`, `StickerPack`, `Gif`

`Sticker`: `{id: id (R), pack_id?: id, emoji?: string, format: static | animated |
video (R), thumb_path?, path?, width?, height?, premium: bool}`. `animated` is
Lottie (`.tgs` or `.json`), `video` is WebM/VP9 with alpha. `thumb_path` is filled
lazily by `stickers.thumb`.

`StickerPack`: `{id: id (R), title: string (R), short_name?, count: integer,
kind: stickers | emoji (R), installed?: bool, thumb_path?}`.

`Gif`: `{id: id (R), thumb_path?, path?, width?, height?, duration?: integer,
mime?: string}`.

### 9.10 `Story`

`{id: id (R), user_id: id (R), media: Media (R), caption?: string, entities?,
posted_at?: ts, expires_at?: ts}`.

### 9.11 `Member`

`{user: Contact (R), role: owner | admin | member | restricted | banned | left (R),
rank?: string, joined_at?: ts, invited_by?: id}`.

### 9.12 `OnlineStatus` and `PresenceAction`

`OnlineStatus`: `{state: online | offline | recently | within_week | within_month |
long_ago | hidden (R), last_seen_at?: ts, until?: ts}`. `until` for `online`: when
the platform will flip it to offline without news.

`PresenceAction` (what someone is doing in a chat): `typing`, `recording_voice`,
`recording_video`, `uploading`, `choosing_sticker`, `playing`, `speaking`,
`cancel`. `cancel` clears whatever was showing.

### 9.13 `PrivacyRule`

`{key: string (R), label: string (R), value: everybody | contacts | nobody (R),
choices: [everybody | contacts | nobody] (R)}`. Keys are plugin-defined
(`last_seen`, `phone`, `photo`, …); `label` is what the UI shows.

### 9.14 `AuthStep`

See §7.1.

## 10. Events (plugin → core)

Notifications; the method name is the event name. Every event's params carry
`account_id`. Schemas: `schemas/events.json#/$defs/<event>`.

Any event MAY be sent as a *request* (with an `id`) instead of a notification; the
core then answers `{}` once the event has been ingested. Use that for bulk pushes
(§16).

| event | params (besides `account_id`) | when |
| --- | --- | --- |
| `status` | `state` (§4.4), `detail?: string`, `reconnecting: bool`, `me?: Contact` | On every state change, and when `me` becomes known. |
| `conversations.upsert` | `conversations: [Conversation]` | Chats discovered or changed (title, unread, pin, …). Partial updates are the norm: emit the fields you know; absent = unchanged (§8.4). |
| `conversations.synced` | `chat_ids: [id]` | After a **complete** chat-list sync: every chat the account currently has. The core hides chats not listed (left groups). Never send a partial list here. |
| `messages.upsert` | `messages: [Message]` | New or re-fetched messages. Idempotent by `(chat_id, id)`; an existing row is merged field by field (§8.4). |
| `messages.deleted` | `chat_id?: id`, `message_ids: [id]` | Some platforms report deletions without the chat (Telegram private chats); then omit `chat_id` and the core matches by id across DMs. |
| `message.edited` | `message: Message` | The message changed (text, media, pin…). The core snapshots the previous version when `text`, `entities` or `media` actually differ from what it holds, then merges (§8.4). |
| `reaction.changed` | `chat_id`, `message_id`, `reactions: [Reaction]`, `reactors?: [Reactor]` | Someone reacted or un-reacted, live. The full current reaction list. |
| `contacts.upsert` | `contacts: [Contact]` | |
| `folders.synced` | `folders: [Folder]` | The **whole** folder set; the core deletes folders not listed. |
| `topics.upsert` | `chat_id`, `topics: [Topic]`, `complete: bool` | `complete: true` ⇒ this is every topic of the chat (the core hides the rest). Topics are merged field by field (§8.4). |
| `read.outbox` | `chat_id`, `topic_id?`, `max_message_id` | The peer read this account's messages up to here. |
| `read.inbox` | `chat_id`, `topic_id?`, `max_message_id`, `unread_count?: integer` | This account read up to here on another device. |
| `presence` | `user_id`, `chat_id?`, `topic_id?`, `online?: OnlineStatus`, `action?: PresenceAction`, `expires_at?: ts` | At least one of `online` and `action`. `chat_id` names the group the action happens in; absent for a DM or a plain online change. Actions expire after `expires_at` or 6 s. |
| `draft.changed` | `chat_id`, `topic_id?`, `draft: Draft` | Draft changed on another device. Empty text = cleared. |
| `avatar.ready` | `kind: user \| chat`, `id`, `path` | An avatar file is on disk. |
| `media.ready` | `chat_id`, `message_id`, `thumb_path?`, `path?` | A thumbnail or full file for a message is on disk (auto-download, or a download the plugin finished on its own). |
| `transfer.progress` | `request_id?: number \| string`, `direction: download \| upload`, `chat_id?`, `message_id?`, `done_bytes`, `total_bytes?` | Progress of a transfer. `request_id` is the id of the core request that caused it (`media.download`, `messages.send_file`, …). |
| `auth.step` | `step: AuthStep` (§7.1) | |

A plugin SHOULD emit events only for started accounts and MUST stop emitting for an
account once `account.stop` has returned.

### 10.1 Echoes of core-initiated changes

When the core changes something through a request (`messages.pin`,
`chats.set_flags`, `read.send_receipt`, `drafts.save`, `messages.edit`, `messages.send`,
`reactions.set`, …) and the request succeeds, **the core applies the result itself**:
it owns the state and does not wait for an event. Most platforms then push the same
change back as an update; the plugin MAY forward it as the matching event and SHOULD
NOT try to suppress it. The core is idempotent for these echoes: a `messages.upsert`
or `message.edited` whose content equals what it holds changes nothing and creates
no edit version; a `conversations.upsert` with the same flags is a no-op; a
`draft.changed` that is older than the core's own draft (by `updated_at`, or equal
in text) is ignored. A plugin MUST NOT echo *more* than the platform reports (no
synthetic events "to be safe").

## 11. Requests (core → plugin)

Every method lists its params (always including `account_id` unless process-scoped),
its result, the capability that gates it (§14) or `required`, and the errors it
typically returns besides the universal ones (`INVALID_PARAMS`, `UNKNOWN_ACCOUNT`,
`AUTH_REQUIRED`, `NETWORK`, `FLOOD_WAIT`, `INTERNAL`, `CANCELLED`). Schemas:
`schemas/methods/<area>.json#/$defs/<method>.params` and `….result`. The
machine-readable list of methods and their capability is exported by `ch-protocol`
(`METHODS`) and by `schemas/methods/registry.json`.

Common parameter conventions:

- `reply_to?: ReplyTarget` on sending methods: `{message_id: id (R), quote_text?:
  string, quote_offset?: integer}` (quote fields need `quote_reply`).
- `topic_id?` on sending methods routes into a forum topic.
- `silent?: bool` (`send.silent`), `schedule_at?: ts` (`send.schedule`).
- Paged lists take `limit: integer` and `cursor?: string`, and return `next_cursor?:
  string` (absent = no more). Cursors are opaque and plugin-defined; a plugin that
  pages by id puts the id in the cursor, one that pages by time puts a time in it.
- Methods that change a message return the fresh `Message` when the platform echoes
  it, so the core can ingest it without another fetch; `message` is nullable where
  the platform does not.

### 11.0 `$/cancelRequest` (both directions, notification)

`{"jsonrpc": "2.0", "method": "$/cancelRequest", "params": {"id": 42}}`

Asks the receiver to stop working on request `id`. The receiver SHOULD stop as
soon as practical and MUST still send a response for `id`: either the normal result
if it had already finished, or an error with `CANCELLED` (-32800). A cancel for an
unknown or already-answered id is ignored. This is the one cancellation mechanism;
there is no per-method cancel. Transfers are cancelled by cancelling the request
that started them.

### 11.1 Lifecycle (`required`)

**`initialize`** (process-scoped)
- params: `api_version: string` (the core's), `core_version: string`,
  `plugin_dir: string`, `locale: string` (BCP 47, e.g. `ru-RU`), `log_level: string`.
- result: `api_version: string` (the plugin's), `capabilities: Capabilities`
  (§14), `plugin_version?: string`.

**`shutdown`** (process-scoped) — params: none. result: `{}`.

**`ping`** (process-scoped) — params: none. result: `{busy?: bool}`.

**`account.start`**
- params: `account_id`, `settings: object` (the `settings_schema` values with
  secrets merged in), `secrets: object of string → string` (from `secrets.set`),
  `state_dir: string` (private, persistent, never touched by the core),
  `cache_dir: string` (for media; the core may purge it, §15), `ghost: bool`
  (§17), `auto_download: {photos: bool, videos_max_bytes: integer, files_max_bytes:
  integer, voice: bool}` (what the plugin may fetch without being asked; 0 = never).
- result: `{}`.
- errors: `INVALID_PARAMS` when settings are unusable (the message is shown to the
  user), `INTERNAL`.

**`account.stop`** — params: `account_id`. result: `{}`.

### 11.2 Auth (`auth.interactive`)

**`auth.begin`** — params: `account_id`, `method: string` (one of the manifest's
`auth_methods`). result: `{}`. The plugin then emits `auth.step`.

**`auth.submit`** — params: `account_id`, `flow: string`, `step_id: string`,
`value: string`. result: `{}`. §7.2 for the acceptance rule.

**`auth.cancel`** — params: `account_id`, `flow: string`. result: `{}`.

### 11.3 Messages

**`messages.fetch_recent`** (`required`) — the one history method every plugin has.
- params: `account_id`, `chat_id`, `topic_id?`, `limit: integer (1–200)`,
  `cursor?: string` (absent = newest page).
- result: `messages: [Message]` (newest first), `next_cursor?: string` (for the
  page *older* than this one).
- errors: `NOT_FOUND` (no such chat), `FORBIDDEN`.
- The core also ingests the returned messages, so the plugin need not emit them
  as `messages.upsert` too.

**`messages.get`** (`messages.get`) — params: `account_id`, `chat_id`,
`message_id`. result: `message: Message?` (absent = gone).

**`messages.send`** (`required`)
- params: `account_id`, `chat_id`, `text: string`, `entities?: [Entity]`
  (`send.entities`), `reply_to?: ReplyTarget`, `topic_id?`, `comment_to?: id`
  (`comments.send`: post as a comment under channel post `comment_to`; the result
  then belongs to the discussion chat), `silent?`, `schedule_at?`, `no_webpage?: bool`
  (`send.no_webpage`), `effect_id?: id` (`send.effects`), `client_id?: string` (an
  id the plugin MAY echo in `Message.ext` for de-duplication; optional).
- result: `message: Message`.
- errors: `FORBIDDEN` (cannot post here), `NOT_FOUND`, `NOT_SUPPORTED` (an option
  whose capability is missing was set).

**`messages.send_file`** (`send.file`)
- params: `account_id`, `chat_id`, `path: string` (absolute, readable by the
  plugin), `caption?: string`, `entities?`, `reply_to?`, `topic_id?`, `as_file?: bool`
  (`send.as_file`: skip compression), `spoiler?: bool` (`send.spoiler`), `kind?:
  auto | voice | video_note | photo | video | document` (`voice` needs `send.voice`,
  `video_note` needs `send.video_note`), `silent?`, `schedule_at?`, `ttl_seconds?`
  (`send.ttl`), `mime?: string`, `filename?: string`, `duration?: integer`,
  `waveform?: [integer]`.
- result: `message: Message`.
- Progress via `transfer.progress` with this request's id; cancellable.

**`messages.send_album`** (`send.album`)
- params: `account_id`, `chat_id`, `items: [{path, caption?, entities?, spoiler?,
  as_file?, kind?}]` (2 to `send.album.max`), `caption?: string` (album caption),
  `entities?`, `reply_to?`, `topic_id?`, `silent?`, `schedule_at?`.
- result: `messages: [Message]` (one per item, or one for all, as the platform does).

**`messages.edit`** (`edit`) — params: `account_id`, `chat_id`, `message_id`,
`text?: string`, `entities?`, `path?: string` (`edit.media`: replace the media),
`no_webpage?: bool`. `text` on a media message edits the caption (`edit.caption`).
result: `message: Message`. errors: `FORBIDDEN` (too old, not mine).

**`messages.delete`** (`delete`) — params: `account_id`, `chat_id`, `message_ids:
[id]`, `revoke: bool` (true = for everyone; false needs `delete.for_me`).
result: `{}`.

**`messages.forward`** (`forward`) — params: `account_id`, `from_chat_id`,
`message_ids: [id]`, `to_chat_id`, `topic_id?`, `drop_author?: bool`
(`forward.drop_author`), `drop_captions?: bool`, `silent?`, `schedule_at?`.
result: `messages: [Message]`.

**`messages.pin`** (`pin`) — params: `account_id`, `chat_id`, `message_id`,
`silent?: bool`, `for_me_only?: bool`. result: `{}`.

**`messages.unpin`** (`unpin`) — params: `account_id`, `chat_id`, `message_id?`
(absent = unpin all). result: `{}`.

**`messages.seen_by`** (`seen_by`) — params: `account_id`, `chat_id`,
`message_id`. result: `users: [{user_id, name?, at?}]`.

**`messages.translate`** (`translate`) — params: `account_id`, `chat_id`,
`message_id`, `to_language: string` (BCP 47). result: `text: string`,
`entities?`, `from_language?: string`.

**`messages.transcribe`** (`transcribe`) — params: `account_id`, `chat_id`,
`message_id`. result: `text: string`, `pending: bool` (true = still being worked
on; call again later). errors: `FORBIDDEN` (needs a paid tier).

### 11.4 History

**`history.around`** (`history.jump_to_date`) — params: `account_id`, `chat_id`,
`topic_id?`, `at: ts`, `limit`. result: `messages: [Message]` (newest first, the
page containing or nearest to `at`), `older_cursor?`, `newer_cursor?` (for
`messages.fetch_recent` and `history.fill_gap` respectively).

**`history.fill_gap`** (`history.fill_gap`) — messages *newer* than a known point.
params: `account_id`, `chat_id`, `topic_id?`, `after_message_id: id`,
`before_message_id?: id`, `limit`, `cursor?` (the `next_cursor` of the previous
page; absent = start right after `after_message_id`). result: `messages: [Message]`
(oldest first), `next_cursor?` (more in the same direction). The core uses it to close holes left by
missed live updates; a plugin that cannot page forwards omits the capability and the
core falls back to `messages.fetch_recent` paging.

There is deliberately no `messages.search`: the core indexes locally (full-text)
everything it has ingested and searches that, so results are instant and identical
across platforms; `media.search` (§11.5) is the one platform-side search. A later
minor version may add platform search for history the core has not fetched.

### 11.5 Media

**`media.download`** (`media.download`) — params: `account_id`, `chat_id`,
`message_id`, `what: full | thumb`, `priority?: integer`. result: `path: string`,
`mime?`, `bytes?`. Progress via `transfer.progress`; cancellable (§11.0). errors:
`NOT_FOUND` (media gone), `FLOOD_WAIT`.

**`media.search`** (`media.search`) — params: `account_id`, `chat_id`, `topic_id?`,
`kind: photo | video | music | voice | document | link | gif | round`, `query?:
string`, `limit`, `cursor?`. result: `messages: [Message]`, `next_cursor?`.

**`avatars.download`** (`avatars`) — params: `account_id`, `kind: user | chat`,
`id`, `size?: small | big`. result: `path?: string` (absent = no avatar).

### 11.6 Reactions

**`reactions.available`** (`reactions.set`) — params: `account_id`, `chat_id?`.
result: `reactions?: [ReactionKey]` (absent = any emoji is allowed; `[]` = none
allowed), `allow_custom: bool`, `max_per_message?: integer`.

**`reactions.set`** (`reactions.set`) — params: `account_id`, `chat_id`,
`message_id`, `reactions: [ReactionKey]` (the full new set of this account's
reactions; `[]` clears), `big?: bool`. result: `message?: Message`.

**`reactions.paid`** (`paid.react`) — params: `account_id`, `chat_id`,
`message_id`, `count: integer ≥ 1`, `anonymous?: bool`. result: `message?: Message`.
errors: `FORBIDDEN` (not enabled here, or insufficient balance).

**`reactions.paid_balance`** (`paid.react`) — params: `account_id`. result:
`balance?: integer`, `currency?: string`.

**`reactions.who`** (`reactions.who`) — params: `account_id`, `chat_id`,
`message_id`, `key?: ReactionKey`, `limit`, `cursor?`. result: `reactors:
[Reactor]`, `next_cursor?`.

**`reactions.effect`** (`reactions.effects`) — params: `account_id`, `key:
ReactionKey`. result: `path?: string` (a Lottie or video to play around the
reaction; absent = none).

**`reactions.list_unread`** (`reactions.unread`) — params: `account_id`, `chat_id`,
`topic_id?`, `limit`. result: `message_ids: [id]` (oldest first).

**`reactions.read`** (`reactions.unread`) — params: `account_id`, `chat_id`,
`topic_id?`. result: `{}`.

### 11.7 Polls

**`polls.create`** (`polls.create`) — params: `account_id`, `chat_id`, `question:
string`, `options: [string]` (2..`max_options`), `multiple?: bool`, `quiz?: bool`,
`anonymous?: bool` (default true), `correct_option?: integer` (quiz; index into
`options`), `solution?: string`, `solution_entities?`, `close_after_seconds?:
integer`, `reply_to?`, `topic_id?`, `silent?`, `schedule_at?`. result: `message:
Message`.

**`polls.vote`** (`polls.vote`) — params: `account_id`, `chat_id`, `message_id`,
`option_ids: [id]` (`[]` retracts). result: `message?: Message`.

**`polls.close`** (`polls.close`) — params: `account_id`, `chat_id`,
`message_id`. result: `message?: Message`.

### 11.8 Bots

**`bots.commands`** (`bots.commands`) — params: `account_id`, `bot_id`, `chat_id?`.
result: `commands: [{command: string, description: string}]`.

**`bots.press_button`** (`bots.buttons`) — params: `account_id`, `chat_id`,
`message_id`, `button: InlineButton`, `row: integer`, `column: integer`. result:
`text?: string` (a toast to show), `url?: string` (to open), `message?: Message`
(the message if the bot edited it).

### 11.9 Read state and typing

**`read.send_receipt`** (`read_receipts`) — params: `account_id`, `chat_id`,
`topic_id?`, `max_message_id`. result: `{}`. Never called while ghost mode is on.

**`typing.send`** (`typing`) — params: `account_id`, `chat_id`, `topic_id?`,
`action: PresenceAction`. result: `{}`. The core repeats it every 5 s while the
user types and sends `cancel` when they stop. Never called while ghost mode is on.

**`mentions.list_unread`** (`mentions.unread`) — params: `account_id`, `chat_id`,
`topic_id?`, `limit`. result: `message_ids: [id]` (oldest first).

**`mentions.read`** (`mentions.unread`) — params: `account_id`, `chat_id`,
`topic_id?`. result: `{}`.

### 11.10 Pins

**`pins.list`** (`pins.list`) — params: `account_id`, `chat_id`, `topic_id?`,
`limit`, `cursor?`. result: `messages: [Message]` (newest first), `next_cursor?`.

### 11.11 Drafts

**`drafts.save`** (`drafts.sync`) — params: `account_id`, `chat_id`, `topic_id?`,
`draft: Draft` (empty text clears). result: `{}`.

### 11.12 Scheduled messages

**`scheduled.list`** (`scheduled`) — params: `account_id`, `chat_id`. result:
`messages: [Message]` (soonest first, `is_scheduled: true`).

**`scheduled.edit`** (`scheduled.edit`) — params: `account_id`, `chat_id`,
`message_id`, `text?`, `entities?`, `schedule_at?: ts` (absent keeps the time).
result: `message?: Message`.

**`scheduled.delete`** (`scheduled`) — params: `account_id`, `chat_id`,
`message_ids: [id]`. result: `{}`.

**`scheduled.send_now`** (`scheduled`) — params: `account_id`, `chat_id`,
`message_ids: [id]`. result: `{}`.

### 11.13 Threads and comments

**`replies.list`** (`replies.list`) — params: `account_id`, `chat_id`,
`message_id`, `limit`, `cursor?`. result: `messages: [Message]` (newest first;
their `chat_id` is the discussion chat's), `thread_chat_id?: id`, `next_cursor?`.

### 11.14 Forum topics

**`topics.list`** (`topics`) — params: `account_id`, `chat_id`, `limit?`, `cursor?`.
result: `topics: [Topic]`, `next_cursor?`.

**`topics.create`** (`topics.manage`) — params: `account_id`, `chat_id`, `title`,
`icon_color?: integer`, `icon_custom_emoji_id?: id`. result: `topic: Topic`.

**`topics.edit`** (`topics.manage`) — params: `account_id`, `chat_id`, `topic_id`,
`title?`, `closed?: bool`, `hidden?: bool`, `icon_custom_emoji_id?`. result: `{}`.

**`topics.pin`** (`topics.pin`) — params: `account_id`, `chat_id`, `topic_id`,
`pinned: bool`. result: `{}`.

### 11.15 Chats

**`chats.sync`** (`chat.sync`) — params: `account_id`, `include_archived?: bool`
(default true). result: `{}`. The plugin re-emits `conversations.upsert` for every
chat and finishes with `conversations.synced`. (Plugins also do this unprompted
after connecting.)

**`chats.set_flags`** (`chat.flags`) — params: `account_id`, `chat_id`, `pinned?:
bool` (`chat.pin`), `muted?: bool` (`chat.mute`), `mute_until?: ts` (with `muted:
true`; absent = forever), `archived?: bool` (`chat.archive`), `marked_unread?: bool`
(`chat.mark_unread`). Only present fields are changed. result: `{}`. A present field
whose capability is missing ⇒ `NOT_SUPPORTED`, nothing changed.

**`chats.clear_history`** (`chat.clear`) — params: `account_id`, `chat_id`,
`revoke?: bool` (for everyone), `topic_id?`. result: `{}`.

**`chats.leave`** (`chat.leave`) — params: `account_id`, `chat_id`, `delete?: bool`
(also delete the conversation locally on the platform). result: `{}`.

**`chats.set_auto_delete`** (`auto_delete`) — params: `account_id`, `chat_id`,
`seconds: integer` (0 = off). result: `{}`.

**`chats.reorder_pinned`** (`chat.reorder_pinned`) — params: `account_id`,
`chat_ids: [id]` (top first), `folder_id?`. result: `{}`.

### 11.16 Folders

**`folders.list`** (`folders`) — params: `account_id`. result: `folders: [Folder]`.

**`folders.create`** (`folders.manage`) — params: `account_id`, `title`,
`emoticon?`, `chat_ids: [id]`, `rules?: FolderRules`. result: `folder: Folder`.

**`folders.rename`** (`folders.manage`) — params: `account_id`, `folder_id`,
`title?`, `emoticon?`. result: `{}`.

**`folders.delete`** (`folders.manage`) — params: `account_id`, `folder_id`.
result: `{}`.

**`folders.set_chats`** (`folders.edit`) — params: `account_id`, `folder_id`, `add:
[id]`, `remove: [id]`. result: `{}`.

**`folders.set_rules`** (`folders.rules`) — params: `account_id`, `folder_id`,
`rules: FolderRules`. result: `{}`. errors: `FORBIDDEN` for a shared folder.

### 11.17 Groups

**`group.create`** (`group.create`) — params: `account_id`, `title`, `user_ids:
[id]`, `kind?: group | channel` (default group), `about?`. result: `chat_id: id`.

**`group.join_by_link`** (`group.join_by_link`) — params: `account_id`, `link:
string`. result: `chat_id?: id`, `pending: bool` (a join request was filed
instead).

**`group.invite_link`** (`group.invite_link`) — params: `account_id`, `chat_id`,
`revoke?: bool` (make a new one). result: `link: string`.

**`group.edit_title`** (`group.edit`) — params: `account_id`, `chat_id`, `title`.
result: `{}`.

**`group.edit_about`** (`group.edit`) — params: `account_id`, `chat_id`, `about`.
result: `{}`.

**`group.edit_photo`** (`group.photo`) — params: `account_id`, `chat_id`, `path?:
string` (absent = remove). result: `{}`.

**`group.set_permissions`** (`group.permissions`) — params: `account_id`,
`chat_id`, `permissions: object of string → bool` (keys from the capability's
`keys` param; true = allowed). result: `{}`.

**`group.set_slow_mode`** (`group.slow_mode`) — params: `account_id`, `chat_id`,
`seconds: integer` (0 = off; one of the capability's `choices`). result: `{}`.

### 11.18 Members

**`members.list`** (`members`) — params: `account_id`, `chat_id`, `query?: string`
(`members.search`), `filter?: all | admins | banned | restricted | bots`, `limit`,
`cursor?`. result: `members: [Member]`, `next_cursor?`, `total?: integer`.

**`members.ban`** (`members.ban`) — params: `account_id`, `chat_id`, `user_id`,
`banned: bool`, `until?: ts`. result: `{}`.

**`members.set_admin`** (`members.admin`) — params: `account_id`, `chat_id`,
`user_id`, `admin: bool`, `rank?: string`, `rights?: object of string → bool`.
result: `{}`.

**`members.invite`** (`members.invite`) — params: `account_id`, `chat_id`,
`user_ids: [id]`. result: `failed: [id]` (users who could not be added, e.g.
privacy).

**`join_requests.list`** (`join_requests`) — params: `account_id`, `chat_id`,
`limit`, `cursor?`. result: `requests: [{user: Contact, requested_at?: ts, about?:
string}]`, `next_cursor?`.

**`join_requests.answer`** (`join_requests`) — params: `account_id`, `chat_id`,
`user_id`, `approve: bool`. result: `{}`.

### 11.19 Contacts

**`lookup.username`** (`lookup.username`) — params: `account_id`, `username`.
result: `contact?: Contact`, `conversation?: Conversation` (when the handle is a
group or channel). Both absent = nothing found.

**`lookup.phone`** (`lookup.phone`) — params: `account_id`, `phone`. result:
`contact?: Contact`.

**`contacts.list`** (`contacts.list`) — params: `account_id`. result: `contacts:
[Contact]`.

**`contacts.set`** (`contacts.edit`) — params: `account_id`, `user_id`, `add:
bool`, `first_name?`, `last_name?`, `phone?`. result: `{}`.

**`contacts.info`** (`contacts.info`) — params: `account_id`, `user_id`. result:
`profile: Profile`.

**`contacts.common_chats`** (`common_chats`) — params: `account_id`, `user_id`,
`limit?`. result: `conversations: [Conversation]`.

**`blocked.list`** (`blocked`) — params: `account_id`, `limit?`, `cursor?`. result:
`contacts: [Contact]`, `next_cursor?`.

**`blocked.set`** (`blocked`) — params: `account_id`, `user_id`, `blocked: bool`.
result: `{}`.

### 11.20 Profile

**`profile.get`** (`profile`) — params: `account_id`. result: `profile: Profile`.

**`profile.update`** (`profile.edit`) — params: `account_id`, `first_name?`,
`last_name?`, `bio?`, `username?`, `photo_path?: string`, `remove_photo?: bool`.
result: `profile: Profile`.

**`profile.photos`** (`profile.photos`) — params: `account_id`, `user_id`,
`limit?`. result: `paths: [string]` (newest first).

**`stories.list`** (`stories`) — params: `account_id`, `user_id`. result:
`stories: [Story]`.

### 11.21 Privacy and security

**`privacy.get`** (`privacy`) — params: `account_id`. result: `rules:
[PrivacyRule]`.

**`privacy.set`** (`privacy`) — params: `account_id`, `key`, `value: everybody |
contacts | nobody`. result: `{}`.

**`two_factor.get`** (`two_factor`) — params: `account_id`. result: `enabled:
bool`, `hint?: string`, `has_recovery_email?: bool`.

**`two_factor.set`** (`two_factor`) — params: `account_id`, `current_password?`,
`new_password?` (absent or empty = remove), `hint?`, `email?`. result: `{}`. May
emit `auth.step` with `flow: "two_factor"`, `kind: "email_code"` and finish with
`kind: "done"`.

### 11.22 Devices

**`sessions.list`** (`sessions`) — params: `account_id`. result: `sessions:
[Session]`.

**`sessions.terminate`** (`sessions`) — params: `account_id`, `session_id?: id`
(absent = all others). result: `{}`.

### 11.23 Account

**`ghost.apply`** (`ghost_mode`) — params: `account_id`, `enabled: bool`. result:
`{}`. §17.

**`account.sign_out`** (`sign_out`) — params: `account_id`. result: `{}`. Ends the
session on the platform, deletes the plugin's local session (and calls
`secrets.delete` for what it stored), then reports `status: auth_required`.

### 11.24 Stickers

**`stickers.recent`** (`stickers`) — params: `account_id`, `limit?`. result:
`stickers: [Sticker]`.

**`stickers.favorites`** (`stickers.favorites`) — params: `account_id`. result:
`stickers: [Sticker]`.

**`stickers.packs`** (`stickers`) — params: `account_id`. result: `packs:
[StickerPack]` (installed, in the account's order).

**`stickers.pack`** (`stickers`) — params: `account_id`, `pack_id`. result: `pack:
StickerPack`, `stickers: [Sticker]`.

**`stickers.search`** (`stickers.search`) — params: `account_id`, `query`,
`limit?`. result: `packs: [StickerPack]`.

**`stickers.install`** (`stickers.manage`) — params: `account_id`, `pack_id`,
`installed: bool`. result: `{}`.

**`stickers.set_favorite`** (`stickers.favorites`) — params: `account_id`,
`sticker_id`, `favorite: bool`. result: `{}`.

**`stickers.thumb`** (`stickers`) — params: `account_id`, `sticker_id`. result:
`path?: string`.

**`stickers.clip`** (`stickers.play`) — params: `account_id`, `sticker_id`. result:
`path?: string` (the full animated/video file).

**`stickers.send`** (`stickers.send`) — params: `account_id`, `chat_id`,
`sticker_id`, `reply_to?`, `topic_id?`, `silent?`, `schedule_at?`. result:
`message: Message`.

### 11.25 GIFs

**`gifs.list`** (`gifs`) — params: `account_id`, `limit?`. result: `gifs: [Gif]`
(saved/recent).

**`gifs.search`** (`gifs.search`) — params: `account_id`, `query`, `limit?`,
`cursor?`. result: `gifs: [Gif]`, `next_cursor?`.

**`gifs.thumb`** (`gifs`) — params: `account_id`, `gif_id`. result: `path?`.

**`gifs.clip`** (`gifs`) — params: `account_id`, `gif_id`. result: `path?`.

**`gifs.send`** (`gifs.send`) — params: `account_id`, `chat_id`, `gif_id`,
`reply_to?`, `topic_id?`, `silent?`, `schedule_at?`. result: `message: Message`.

### 11.26 Custom emoji

**`emoji.custom_files`** (`custom_emoji`) — params: `account_id`, `ids: [id]`.
result: `files: object of id → string` (path; missing keys = unavailable).

**`emoji.packs`** (`custom_emoji`) — params: `account_id`. result: `packs:
[StickerPack]` (`kind: emoji`).

**`emoji.pack`** (`custom_emoji`) — params: `account_id`, `pack_id`. result:
`pack: StickerPack`, `stickers: [Sticker]`.

**`emoji.animated`** (`emoji.animated`) — params: `account_id`, `emoji: string`.
result: `path?: string` (the big animated version of one standard emoji).

## 12. Requests (plugin → core)

The core implements these; a plugin may call them any time after `initialize`. They
time out on the plugin's side after `default_timeout_seconds` (60 s, §4.6); the
plugin then sends `$/cancelRequest` and treats the call as `TIMEOUT`.

### 12.1 Secrets

**`secrets.set`** — params: `account_id`, `key: string`, `value: string`. result:
`{}`. Stored in the keyring; returned in `account.start.secrets` from then on.

**`secrets.delete`** — params: `account_id`, `key: string`. result: `{}`.

### 12.2 Events as requests

Any event name (§10) may be sent as a request; the core replies `{}` when the
event has been ingested, or an error if the params were invalid
(`INVALID_PARAMS`). See §16.

### 12.3 Vendor methods and events

Every method and event name not in the registry MUST be of the form
`x-<plugin-id>.<name>` (`x-telegram.export_chat`); the un-prefixed namespace is
reserved for future minor versions, and a receiver answers an unknown un-prefixed
method with `METHOD_NOT_FOUND`. In v1 the core calls no vendor methods on its own and
ignores (logs) vendor events; vendor names exist so that a plugin, its tests and a
future "plugin actions" UI have a collision-free space. The same prefix rule holds
for capability keys (§14.3) and `ext` (§8.5).

## 13. Errors

A JSON-RPC error object with a **numeric `code`**, a human `message` (English, for
logs; the UI shows its own text keyed by `data.code`), and a `data` object with a
**string `code`** and code-specific fields:

```json
{"code": -32003, "message": "Too many requests", "data": {"code": "FLOOD_WAIT", "seconds": 37}}
```

| numeric | `data.code` | meaning | `data` extras |
| --- | --- | --- | --- |
| -32700 | `PARSE_ERROR` | The frame was not valid JSON. | |
| -32600 | `INVALID_REQUEST` | Not a valid JSON-RPC object (or a batch array). | |
| -32601 | `METHOD_NOT_FOUND` | Unknown method. | |
| -32602 | `INVALID_PARAMS` | Params failed validation or are semantically wrong. | `field?: string` |
| -32603 | `INTERNAL` | Anything unexpected. | |
| -32800 | `CANCELLED` | The request was cancelled via `$/cancelRequest`. | |
| -32001 | `NOT_SUPPORTED` | The platform or this plugin cannot do this; the capability is missing or a param needs a missing capability. | `capability?: string` |
| -32002 | `AUTH_REQUIRED` | The account must sign in first. | |
| -32003 | `FLOOD_WAIT` | Rate-limited by the platform. | `seconds: integer` (R) |
| -32004 | `NOT_FOUND` | The chat, message, user, … does not exist or is not visible. | `kind?: string` |
| -32005 | `FORBIDDEN` | The platform refused: no rights, too old to edit, paid tier needed, blocked. | `reason?: string` |
| -32006 | `NETWORK` | The platform is unreachable or the connection dropped mid-call. Retryable. | |
| -32007 | `TIMEOUT` | The request exceeded its timeout (raised by the core; a plugin may raise it for a platform-side timeout). | |
| -32008 | `UNKNOWN_ACCOUNT` | `account_id` is not started on this plugin. | |
| -32009 | `UNAVAILABLE` | The account is started but currently `unavailable` or `connecting`; try later. | |

Rules:

- `data.code` is authoritative for behaviour; the numeric code mirrors it (the
  mapping is fixed by this table). Both are **open enums**: a later minor version
  may add codes, and `schemas/errors.json` accepts any non-empty string and any
  integer (`KnownErrorCode`/`KnownNumericCode` list the ones defined here).
  Receivers MUST accept an unknown `data.code`, treat it as `INTERNAL` and keep the
  string for logs. Emitters only use codes from this table within a major version;
  platform detail goes into extra `data` fields, not into new codes.
- `FLOOD_WAIT` MUST carry `seconds`. The core shows a countdown and does not retry
  before it elapses.
- A plugin MUST NOT answer a missing feature with an empty success result. Missing
  feature ⇒ `NOT_SUPPORTED`. The core never calls a method whose capability the
  plugin did not declare, so a plugin only sees this for param-level capabilities.
- Errors in notifications cannot be reported; log them to stderr.

## 14. Capabilities

### 14.1 Shape

`initialize` returns `capabilities`, an object mapping **capability key → `true` or
a parameter object**:

```json
{
  "capabilities": {
    "edit": {"window_seconds": 172800},
    "send.album": {"max": 10},
    "polls.create": {"quiz": true, "multiple": true, "anonymous": true, "max_options": 10},
    "reactions.set": true,
    "typing": {"actions": ["typing", "recording_voice", "uploading"]}
  }
}
```

- A key that is absent (or `false`) is **not supported**.
- `true` means supported with default parameters.
- An object means supported with these parameters. Each parameterised key lists its
  parameters below; every parameter has a default so `true` is always a valid value.
- Capabilities are per plugin process and static between `initialize` calls.
  Facts that vary per account at runtime (a premium tier) belong in `Profile`
  (`is_premium`) or in `reactions.available`, not here.

### 14.2 The rule

**If the core calls a method whose capability is missing, the plugin MUST fail
with `NOT_SUPPORTED`.** The same holds for a *parameter* gated by a capability (a
`schedule_at` without `send.schedule`). There is no silent no-op. The UI hides or
greys out the control, so a user never sees that error in normal use; the
conformance suite checks it.

The gated parameters are machine-readable: each registry row
(`schemas/methods/registry.json`, `ch_protocol::METHODS`) lists them as
`gated_params: [{param, capability, equals?}]`. `param` is a dotted path into the
params (`reply_to.quote_text`; a `[]` segment means any element,
`reactions[].custom_emoji_id`); the parameter is "present" when the value at that
path is neither absent nor `null`, and — when `equals` is set — equals it (`kind =
"voice"` needs `send.voice`, `revoke = false` needs `delete.for_me`). A present gated
parameter without its capability ⇒ `NOT_SUPPORTED` with `data.capability` = the key,
before any work is done. Both SDKs and the core enforce from this one table; the
prose in §11 is informative. Gates that depend on state rather than on a parameter
(`edit.caption`: the message has media) stay with the plugin.

A plugin that declares a capability MUST implement every method listed for it. The
conformance suite calls each one.

### 14.3 Registry

`schemas/capabilities.json` is the machine-readable list. Parameters are shown as
`{param: default}`.

**Sending and messages**

| key | gates | params |
| --- | --- | --- |
| `messages.get` | `messages.get` | |
| `send.entities` | `entities` on `messages.send`/`send_file`/`edit` | `{types: [all Entity types]}` — which entity types can be *sent* |
| `send.schedule` | `schedule_at` | |
| `send.silent` | `silent` | |
| `send.no_webpage` | `no_webpage` | |
| `send.effects` | `effect_id` | |
| `quote_reply` | `reply_to.quote_*` | |
| `comments.send` | `comment_to` | |
| `send.file` | `messages.send_file` | `{max_bytes: 2147483648}` |
| `send.voice`, `send.video_note`, `send.spoiler`, `send.as_file`, `send.ttl` | the matching `send_file` options | |
| `send.album` | `messages.send_album` | `{max: 10}` |
| `edit` | `messages.edit` (text) | `{window_seconds: null}` (null = no limit) |
| `edit.caption` | editing a media caption | |
| `edit.media` | `path` on `messages.edit` | |
| `delete` | `messages.delete` with `revoke: true` | `{window_seconds: null}` |
| `delete.for_me` | `messages.delete` with `revoke: false` | |
| `forward` | `messages.forward` | |
| `forward.drop_author` | `drop_author` | |
| `pin`, `unpin` | `messages.pin`, `messages.unpin` | |
| `seen_by` | `messages.seen_by` | |
| `translate` | `messages.translate` | |
| `transcribe` | `messages.transcribe` | |
| `history.jump_to_date` | `history.around` | |
| `history.fill_gap` | `history.fill_gap` | |
| `media.download` | `media.download` | |
| `media.search` | `media.search` | `{kinds: [photo, video, music, voice, document, link]}` |
| `avatars` | `avatars.download` | |

**Reactions, polls, bots**

| key | gates | params |
| --- | --- | --- |
| `reactions.set` | `reactions.available`, `reactions.set` | `{max: 1}` reactions per message by this account |
| `reactions.custom` | custom-emoji reaction keys | |
| `paid.react` | `reactions.paid`, `reactions.paid_balance` | |
| `reactions.who` | `reactions.who` | |
| `reactions.effects` | `reactions.effect` | |
| `reactions.unread` | `reactions.list_unread`, `reactions.read` | |
| `polls.create` | `polls.create` | `{quiz: false, multiple: false, anonymous: false, max_options: 10}` |
| `polls.vote` | `polls.vote` | |
| `polls.close` | `polls.close` | |
| `bots.commands` | `bots.commands` | |
| `bots.buttons` | `bots.press_button` | |

**Read state, presence, drafts, scheduling, threads**

| key | gates | params |
| --- | --- | --- |
| `read_receipts` | `read.send_receipt` | |
| `typing` | `typing.send` | `{actions: [typing]}` |
| `presence` | the plugin emits `presence` events | |
| `mentions.unread` | `mentions.list_unread`, `mentions.read` | |
| `pins.list` | `pins.list` | |
| `drafts.sync` | `drafts.save`, `draft.changed` | |
| `scheduled` | `scheduled.list`, `scheduled.delete`, `scheduled.send_now` | |
| `scheduled.edit` | `scheduled.edit` | |
| `replies.list` | `replies.list` | |
| `topics` | `topics.list`, `topics.upsert` | |
| `topics.manage` | `topics.create`, `topics.edit` | |
| `topics.pin` | `topics.pin` | |

**Chats, folders, groups, members**

| key | gates | params |
| --- | --- | --- |
| `chat.sync` | `chats.sync` | |
| `chat.flags` | `chats.set_flags` | |
| `chat.pin`, `chat.mute`, `chat.archive`, `chat.mark_unread` | the matching flag | |
| `chat.clear` | `chats.clear_history` | `{revoke: false}` |
| `chat.leave` | `chats.leave` | |
| `auto_delete` | `chats.set_auto_delete` | `{choices: [0, 86400, 604800, 2678400]}` |
| `chat.reorder_pinned` | `chats.reorder_pinned` | |
| `folders` | `folders.list`, `folders.synced` | |
| `folders.manage` | `folders.create`, `folders.rename`, `folders.delete` | |
| `folders.edit` | `folders.set_chats` | |
| `folders.rules` | `folders.set_rules` | |
| `group.create` | `group.create` | `{kinds: [group]}` |
| `group.join_by_link` | `group.join_by_link` | |
| `group.invite_link` | `group.invite_link` | |
| `group.edit` | `group.edit_title`, `group.edit_about` | |
| `group.photo` | `group.edit_photo` | |
| `group.permissions` | `group.set_permissions` | `{keys: [send_messages, send_media, send_stickers, send_polls, embed_links, invite_users, pin_messages, change_info]}` |
| `group.slow_mode` | `group.set_slow_mode` | `{choices: [0, 10, 30, 60, 300, 900, 3600]}` |
| `members` | `members.list` | |
| `members.search` | `query` on `members.list` | |
| `members.ban` | `members.ban` | |
| `members.admin` | `members.set_admin` | |
| `members.invite` | `members.invite` | |
| `join_requests` | `join_requests.list`, `join_requests.answer` | |

**Contacts, profile, privacy, account**

| key | gates | params |
| --- | --- | --- |
| `lookup.username` | `lookup.username` | |
| `lookup.phone` | `lookup.phone` | |
| `contacts.list` | `contacts.list` | |
| `contacts.edit` | `contacts.set` | |
| `contacts.info` | `contacts.info` | |
| `common_chats` | `contacts.common_chats` | |
| `blocked` | `blocked.list`, `blocked.set` | |
| `profile` | `profile.get` | |
| `profile.edit` | `profile.update` | |
| `profile.photos` | `profile.photos` | |
| `stories` | `stories.list` | |
| `privacy` | `privacy.get`, `privacy.set` | `{keys: []}` |
| `two_factor` | `two_factor.get`, `two_factor.set` | |
| `sessions` | `sessions.list`, `sessions.terminate` | |
| `ghost_mode` | `ghost.apply` | |
| `sign_out` | `account.sign_out` | |
| `auth.interactive` | `auth.begin`, `auth.submit`, `auth.cancel` | |

**Stickers, GIFs, emoji**

| key | gates | params |
| --- | --- | --- |
| `stickers` | `stickers.recent`, `stickers.packs`, `stickers.pack`, `stickers.thumb` | |
| `stickers.send` | `stickers.send` | |
| `stickers.search` | `stickers.search` | |
| `stickers.manage` | `stickers.install` | |
| `stickers.favorites` | `stickers.favorites`, `stickers.set_favorite` | |
| `stickers.play` | `stickers.clip` | |
| `gifs` | `gifs.list`, `gifs.thumb`, `gifs.clip` | |
| `gifs.send` | `gifs.send` | |
| `gifs.search` | `gifs.search` | |
| `custom_emoji` | `emoji.custom_files`, `emoji.packs`, `emoji.pack`, `custom_emoji` entities | |
| `emoji.animated` | `emoji.animated` | |

Unknown capability keys returned by a plugin are ignored (a newer plugin on an
older core). A plugin MAY declare vendor keys as `x-<plugin-id>.<name>`; the core
stores them for the UI's `ext` consumers and never acts on them.

## 15. Media, files and transfers

- **No bytes on the wire.** Files move through the filesystem. The plugin writes
  into the account's `cache_dir` (from `account.start`) and passes **absolute
  paths**; the core reads them. The core gives the plugin absolute paths of the
  user's files for uploads.
- Every path a plugin emits MUST be inside its `cache_dir` or `state_dir`. The core
  rejects anything else (the DTO is dropped and logged). This is the one thing that
  keeps a misbehaving plugin from pointing the UI at `~/.ssh`.
- `cache_dir` belongs to the core's media cache: it enforces size limits and the
  user's "clear cache". **Files there can disappear at any time.** A plugin MUST
  tolerate a missing file and re-download on the next `media.download`; it MUST NOT
  keep anything irreplaceable there. `state_dir` is never purged.
- Layout inside `cache_dir` is the plugin's business; `<chat_id>/<message_id>.<ext>`
  is a fine convention. File names MUST NOT be derived from user-controlled text
  unsanitised.
- Thumbnails SHOULD be fetched eagerly (they are small) and reported through
  `Media.thumb_path` or `media.ready`. Full files are fetched on `media.download`,
  or eagerly within the `auto_download` limits from `account.start`, reported via
  `media.ready`.
- Progress: for every long `media.download` / `messages.send_file` /
  `messages.send_album`, emit `transfer.progress` with `request_id` set to the
  request's id at least every 500 ms while bytes move. The core draws the ring and
  resets the request's timeout on each one.
- Cancellation is `$/cancelRequest` for the request (§11.0). On cancel, delete any
  partial file.
- Stickers, GIFs and emoji files are cache content too (`stickers.thumb`,
  `gifs.clip`, `emoji.custom_files`, `reactions.effect`, `emoji.animated`): same
  rules, same directory.

## 16. Backpressure and batching

- Batch bulk events: `messages.upsert`, `conversations.upsert`, `contacts.upsert`
  carry arrays. Aim for **≤ 100 items or ≤ 1 MiB per frame**, whichever comes
  first; the hard cap is the 16 MiB frame limit.
- During initial sync (thousands of chats, many pages of history) send bulk events
  **as requests** (§12.2) and keep **at most 4 in flight**; wait for the core's
  `{}` before sending more. The core answers when the batch is in the database, so
  this paces the plugin to what the core can ingest. Live updates can stay
  notifications.
- Prefer breadth first: emit every conversation (with `last_message_at`) before
  any history, so the chat list is usable within seconds, then `conversations.synced`,
  then recent messages per chat as the platform hands them out. Do not pre-fetch
  deep history unprompted; the core asks with `messages.fetch_recent` when a chat is
  opened.
- Coalesce `presence`: never more than one event per user per 500 ms.
- Respect the platform's rate limits yourself and surface them as `FLOOD_WAIT`;
  the core will not hammer a method that just returned one.
- The core processes one plugin's frames in order, so the `conversations.synced`
  that follows a batch of `conversations.upsert` is applied after them.

## 17. Ghost mode

Ghost mode is a global switch in the core: the user is never shown online, sends no
read receipts and no typing indicators, on every account at once.

The core enforces it: while ghost is on it **does not call** `read.send_receipt` or
`typing.send`, on any plugin, and it hides "online" from outgoing presence. A plugin
bug therefore cannot leak a receipt.

Plugins participate too, through `ghost.apply` (`ghost_mode` capability) and the
`ghost` flag in `account.start`, because some platforms flip an account online as a
side effect of *any* activity (sending a message, fetching history). A plugin with
`ghost_mode` MUST, while ghost is on:

- keep the account's status reported to the platform as offline, re-asserting it
  after every operation that could flip it (sending, reacting, editing);
- not send read receipts or typing even if asked (defence in depth; answer `{}`);
- not mark stories or media as viewed where the platform tracks that.

A plugin without `ghost_mode` still gets the core-side protection; the UI tells the
user that "{platform} may still show you online".

## 18. Versioning and stability

- `api_version` is **semver**. This document is `1.0.0`. The core and the plugin
  each state theirs in `initialize`; they are compatible when the **major** versions
  are equal. (The amendments listed under "Changes since the first draft" were made
  before anything shipped and did not bump the version.)
- Within a major version every change is **additive**: new optional methods,
  events, capability keys, enum values and fields. Nothing is removed, renamed or
  retyped; no required field is added; defaults never change. A plugin written
  against 1.0 keeps working on a 1.7 core and vice versa.
- **Unknown fields are ignored on both sides**, in params, results, DTOs and the
  manifest. Schemas say `additionalProperties: true` wherever that applies. Never
  fail on a field you do not know.
- **Unknown enum values**: a receiver MUST treat an unknown `kind`/`type`/`state`
  value as `other`/unknown and keep going (render the message as text, skip the
  entity). Emitters SHOULD only use values from this document within a major
  version; new values arrive with a minor version.
- Methods are never required retroactively. The required set is fixed for v1:
  `initialize`, `shutdown`, `ping`, `account.start`, `account.stop`,
  `messages.fetch_recent`, `messages.send`.
- A minor bump (1.1.0) means something was added; a plugin can detect it from the
  core's `api_version` in `initialize` and use the addition when present. A patch
  bump clarifies text or schemas without changing the wire.
- **Deprecation**: a feature slated for removal in the next major is marked
  deprecated in the spec and schemas (`"deprecated": true`) at least one minor
  version earlier; the core keeps **supporting the previous major version for at
  least 12 months** after the next major ships, and announces the end date in the
  release notes.
- Every schema file carries `$id` under `https://contacthub.dev/schemas/v1/…`. A
  new major gets a new path.

## 19. Trust model

- A plugin runs **as the user**, with the user's privileges, as a child process of
  the core. There is no sandbox in v1. Anything the user can do, a plugin can do.
  Therefore: before a third-party plugin is enabled, the app shows who published it
  and warns that it will have full access to that account's messages, contacts and
  media, and to the user's files.
- Isolation that *is* enforced: a plugin never touches the database, the keyring
  or another plugin's directories through the API. It gets its own `state_dir`,
  `cache_dir`, settings and secrets, and nothing else from the core. Paths it emits
  are validated (§15). Frames are size-limited; requests are timed out; a hung or
  crashing plugin is restarted and never takes the app down.
- Secrets flow only through `account.start` (in) and `secrets.set` (out), never
  through logs. Plugins MUST NOT write secrets to stderr.
- The core identifies plugins by directory and manifest `id`; there is no
  signature verification in v1. Install only plugins you trust, from sources you
  trust. Built-in plugins get no privileges that third-party plugins lack.
- Network: the plugin owns its connection to its platform (and its proxy
  settings). The core does not proxy traffic and does not know the plugin's
  credentials beyond what the settings form holds.

## 20. Checklist for a new plugin

1. Write `plugin.json` (§3.2). Pick an `id`, a brand colour, the `auth_methods` and
   a `settings_schema` with `x-secret` on tokens.
2. Implement NDJSON JSON-RPC on stdio (§2). Read stdin on its own thread; write
   stdout under a lock; log to stderr.
3. Answer `initialize` with your capabilities (§14): start small and honest.
4. Implement `account.start`/`stop`, `ping`, `shutdown`.
5. Emit `status`. If sign-in is interactive, drive `auth.step` (§7).
6. On connect: `conversations.upsert` (all chats) → `conversations.synced`;
   `contacts.upsert`; then live `messages.upsert`, `messages.deleted`,
   `message.edited`, `read.*`, `presence`.
7. Implement `messages.fetch_recent` with cursor paging and `messages.send`.
8. Add capabilities one at a time, each with every method it gates.
9. Run `ch plugin test ./my-plugin`: handshake, schemas, declared capabilities,
   error codes, idempotent sync.
10. Write a README with your capability table and known gaps.
