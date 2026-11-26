# UI to core API

The TypeScript declarations in `app/ui/src/core/index.ts` define the Phase 3 wire types and pane interface. A pane receives a `CoreClient`. The native application owns one Rust runtime, History and Outbox for its database. Every send has exactly one account and chat destination.

## Transport

The Tauri adapter invokes `core_request` with one argument named `request`, containing a `CoreRequest`. The Rust dispatcher allowlists the methods in `CoreOperations`. Return the matching `CoreReply`. For example, `{"request":{"method":"inventory","params":{}}}` returns `{"ok":true,"value":{"plugins":[],"accounts":[],"mediaAccess":{"saveCopy":false},"composerAccess":{"chooseFile":false}}}` on an empty browser installation. Client methods resolve to the value or reject with a `CoreFailure`. Adapter transport failures reject with `unavailable`. Never automatically replay a mutation after a lost reply.

`subscribe` installs a listener on `core-change` and resolves once it is active. Its return value removes that listener idempotently. The future loopback, token-authenticated development server uses the same requests, replies and invalidations, with transport-level request correlation. Plugin RPC, SQL, native paths, arbitrary invoke names and secret-store operations are private to Rust.

The development server accepts WebSocket upgrades only at `ws://127.0.0.1:43127/core` from the exact Origin `http://127.0.0.1:5173`, with subprotocol `ch-core-v1`. Its first text frame must be `{"token":"…"}`. Upon success the server subscribes to changes and sends `{"ready":true}`. Subsequent frames carry `{"id":"…","request":CoreRequest}`, corresponding `{"id":"…","reply":CoreReply}`, or unsolicited `{"change":CoreChange}`. Allow up to 16 concurrent requests per connection, using unique string IDs up to 128 bytes. The server caps inbound messages at 256 KiB and closes malformed or unauthenticated connections. The token is a fresh 256-bit capability from a private file for that process. Supply it to the browser via a URL fragment. Keep it out of URLs sent to the server, logs and persistent storage.

DTO member names use camelCase. Required nullable members are present with JSON null. Optional members can be omitted. All IDs and cursors are opaque strings. An account ID identifies its plugin through inventory. A thread is the tuple (accountId, chatId, topicId). A null topic selects the whole chat for reads and the main chat for sends. Never combine messages from two accounts or chats through person identity.

`Millis` is a signed base-10 integer string of Unix milliseconds, including negative times and zero. Convert protocol timestamp values at the Rust boundary. The UI can convert a valid JavaScript Date range for display and fall back when out of range. Counts and range offsets are nonnegative safe JSON integers. Clamp display counters above the safe range. Reject invalid numeric request parameters.

## Reads and invalidation

- `inventory` combines discovered plugin snapshots and persisted accounts in account display order. For a removed plugin with remaining accounts, return an unavailable summary using its persisted name if available, otherwise its ID. Capabilities come from the current handshake. Null means no live handshake. A capability value of true or an object enables its key. Basic text send is a required method. Present runtime account state from the host, falling back to stopped/disconnected with reconnecting false when absent. Treat unknown platform status values as displayable text. Project manifest auth methods and settings schema for use in generic forms.
- `listChats` projects `ch_db::list_conversations` through a Rust-owned read connection. Preserve its ordering and archive filter. Each row has a whole-chat key and core-derived preview, dates and unread state. Display title fallback is the chat ID. Unknown post permission is null.
- `history` maps latest, refresh, older and newer to the corresponding History operations (latest uses `open`). Latest uses cached data. Refresh explicitly requests a platform update. A limit of zero selects the service default, otherwise cap at 200. Return the service's oldest-first order, including its gap markers. The before/after cursor is the corresponding boundary message’s **messageId**, exclusive and scoped to the requested thread. Older with null starts at the latest edge and may fetch. An empty cached page can have moreOlder. A stale or foreign anchor produces a typed error. Re-open the thread after a stale cursor.
- Within a thread use **rowKey** for a rendered row, also for matching the outbox echo. It is the string form of the local message row ID. **messageId** identifies the current platform message or provisional local echo. Echo adoption can replace the provisional ID or coalesce rows. Replace the old page on reload. A gap ID is scoped to the exact thread. `fillGap` returns whether it closed. Re-read the visible window after each requested fill, even when no message changed.
- `search` delegates to local Search using chat/topic scope, newest order and the supplied UTC offset in minutes east of UTC. Use the returned nextOffset with the same query, scope and offset from UTC. Search replies are individual SQLite snapshots. Concurrent edits may shift subsequent pages. Each snippet's highlights are relative to that snippet. Message entities refer to message text. Both use half-open UTF-16 code-unit ranges (in `A😀B`, the emoji has offset 1 and length 2). Render text as text, ignore unsupported entity types and malformed spans. Message direction controls delivery details and bubble styling. Both directions remain left-aligned. Theme accent affects accent elements only.
- `CoreChange` is an invalidation. Subscribe before initial reads. Inventory events invalidate inventory. Account events invalidate that account's inventory row, auth, settings, chats, visible history, search and outbox. Reset invalidates every active snapshot. The bridge watches both the core bus and outbox subscription. Unknown scope or receiver lag on either stream produces reset. A reconnect also emits reset after the new listener is installed. Group redundant invalidations, discard stale responses when the selection changes, and re-read once more if a change arrived during a read. A notification causes a cached latest read, never an automatic platform refresh. Other visible history windows may be discarded and reopened at latest on reset. Separate calls have no shared transaction.
- Run synchronous DB, outbox and search work off UI threads. Chat rows, search hits and outbox items come from the core. The UI never generates demo records or speculative echo rows.

## Attachment metadata view

ADR 0005 adds optional `Message.attachment: Attachment | null` alongside the existing
`mediaKind` fallback. The eight fields are `kind`, nullable `name`, `mime`, `bytes`,
`width`, `height`, `durationSeconds`, and `hasSpoiler`. Byte counts are exact unsigned
decimal strings; numeric dimensions/duration above JavaScript's safe integer range
become null. Unknown and absent values stay unknown rather than receiving guesses.
Only confirmed stored media receives the projection; deleted messages, `local:` IDs
and unconfirmed sending/failed echoes receive null. No paths, URLs, cache flags,
platform extensions or upload source locations are projected.

The thread renders metadata as escaped text with the existing caption. Spoilers
withhold metadata from the DOM until a local reveal for the exact account, chat,
topic, row, message ID and attachment metadata. A changed attachment requires a new
reveal; switching conversations can reset reveals. Deleted/provisional messages
never reveal metadata, and old `mediaKind`-only messages keep a generic summary.
The metadata projection itself adds no operations or network fetches. Its separate
attachment-job controls are defined below; no upload controls are added.

## Attachment jobs

ADR 0007 adds four short operations, bringing `CoreOperations` to 27:

- `startMediaDownload({ thread, messageId, sessionId, requestKey })` reserves an explicit full-attachment download.
- `startMediaSave({ thread, messageId, sessionId, requestKey })` reserves an explicit native Save-copy from the account cache. It never downloads implicitly.
- `listMediaJobs({ thread, cursor, requestKey })` reads at most 16 receipts, newest creation first. Both nullable parameters are required; at most one can be non-null. Exact-key recovery returns zero or one job and no continuation.
- `cancelMediaJob({ thread, sessionId, jobId })` requests cancellation promptly and returns the actual receipt. Completion can win a late cancellation.

`Inventory.mediaAccess: { saveCopy: boolean }` is required. False means no reviewed
native Save provider; browser production uses false. It says nothing about whether
a particular attachment remains cached. No renderer path, URL, bytes, native command
or file handle is accepted or returned. Save has no open/reveal/upload counterpart.

`MediaStart` carries the exact stored message topic, including from whole-chat
history, plus a session ID learned through listing and one fresh ASCII random
request key for that explicit intent. A null-topic jobs list means exact null-topic
receipts, not a union of topic jobs. List and cancel do not require the old message
or chat row to remain present; account ownership still applies. UI recovery must
retain observed exact topic scopes when switching views or deleting a message.

Each `MediaJob` exposes only `id`, `sessionId`, `requestKey`, `thread`, `messageId`,
`kind` (`download` or `save`), `state`, `cancelRequested`, nullable unsigned decimal
`doneBytes`/`totalBytes`, and nullable `error`. State is one of `queued`, `running`,
`choosing`, `committing`, `cancelling`, `succeeded`, `failed`, `cancelled`, `unknown`.
`MediaJobError` has a normal `CoreFailure.code`, boolean `outcomeUnknown` and a safe
reason enum: `notCached`, `unsafeCache`, `changed`, `tooLarge`, `destinationExists`,
`unsupportedDestination`, `unavailable`, `internal`. It contains no raw error text.
`MediaJobPage` carries `sessionId`, `jobs`, nullable `nextCursor` and
`remainingRequestKeys` (0–4096). This quota counts accepted keys, not running slots.

The core retains accepted keys and exact receipts for its entire session, up to
4096, without eviction or replay. Starts and cancel are side effects in both
transports; lost or malformed replies are outcome-unknown, while list is a read.
A lost start retains its original key/session and is recovered only through a
same-key list, never an automatic restart or a fresh key. A zero-result lookup may
race admission and is still unresolved. A process-session change cannot prove
that an earlier Save did not publish a copy. A new copy requires explicit review
and another user intent. Existing files are never replaced in this batch.

The UI subscribes before reading one bounded jobs page and rereads on the captured
account's invalidations/reset. Earlier-page navigation remains an explicit local
read and never changes history cursors or gaps. Starts occur only from explicit
Download/Save-copy clicks after learning the session; mounting, reconnecting,
revealing a spoiler or listing jobs never starts a transfer. Pending intent keys
survive panel closure/account switches in a bounded per-client memory store.
Returned late results cannot update an unmounted or different-account panel.
Unknown cancellation requires a fresh receipt read before another cancellation.
The attachment's exact-topic actions stay behind its local spoiler reveal, while
header jobs access remains available for recovery after deletion. A changed
visible attachment blocks new starts until its actions are reopened. Native Save
can use an enabled account's safe cache while the plugin is stopped; a false
provider hides Save. No cached-existence flag or future cache guarantee is shown.

## Ordinary reaction interactions

ADR0008 adds exactly three operations, bringing `CoreOperations` to 30:

- `refreshMessageReactions({ thread, messageId })` explicitly reads that exact
  stored target and its account/chat policy, returning a `ReactionContext`.
- `setMessageReactions({ thread, messageId, sessionId, contextToken, keys })`
  replaces the complete ordinary key membership, including `[]` to clear.
- `listMessageReactors({ thread, messageId, sessionId, contextToken, key, cursor })`
  explicitly reads one bounded page; both nullable filter/cursor fields are required.

`OrdinaryReactionKey` is exactly `{ kind: "emoji", emoji }` or `{ kind: "custom", id }`.
Literal sequences and opaque IDs retain exact identity; paid input and chosen-order
claims are outside this slice. `ReactionItem` contains `key`, nullable unsigned
decimal `count`, and nullable `mine`; `ReactionSnapshot` contains `items`. Optional
`Message.reactions` is read-only cached metadata: absent/zero stored rows are unknown,
negative stored counts become null, and cached personal choice stays null. It is
never a write proof or permission to infer a chip-toggle from missing state.

`ReactionContext` carries its own reaction-service `sessionId`, nullable
`contextToken`/`expiresAt`, nullable `snapshot`/`availability`, and `canSet`.
`ReactionAvailability` contains nullable `keys`, `allowCustom`, and nullable decimal
`maxPerMessage`. A null catalog permits literal ordinary emoji; a finite catalog
remains authoritative even if `allowCustom` is true. Listed custom keys are allowed
with the live custom capability even when `allowCustom` is false; arbitrary custom
ID entry and assets are deferred. The frozen handshake default for `reactions.set: true` or `{}` is a known plugin bound of 1; a valid explicit integer `max >= 1` is respected. A malformed explicit maximum, including null, disables writes rather than becoming a default or an unbounded allowance. Runtime unknown `maxPerMessage` never becomes 0, 1 or unlimited.

Refresh is explicit, never a mount, invalidation or checkbox effect. Only the newest
matching local generation can install a proof. A token expires after 60 seconds,
belongs to the exact account/chat/message topic, and is unrelated to media-job
sessions. A write consumes the proof before dispatch, invalidating reactor cursors;
every subsequent write requires another explicit refresh. The result is
`{ snapshot: ReactionSnapshot | null }`: a missing reported snapshot stays unknown,
while an explicit empty array is known-empty. Lost/malformed writes use
`outcomeUnknown`; both transports classify replacement as a write and the other two
operations as reads. Neither automatically replays work.

Each reactor page contains `items` and nullable `nextCursor`. A `ReactorItem` exposes
only `userId`, ordinary `key`, nullable escaped `name`, and nullable `at: Millis`.
Backend pages use 32 rows, at most 8 RPC attempts/256 rows per context and 128 global
cursor/page receipts. Changing a filter does not reset its context budget. A short
or empty page with a continuation remains pageable; absence alone ends traversal.
No paged data is written into `Message.reactors`, and no exhaustive stable-list
claim, asset URL, avatar or unread/paid control is added.

The thread shows at most eight cached ordinary chips, labels them a partial view,
and offers explicit View/refresh. Opening the panel, selecting checkboxes/changing
the reactor filter, history invalidation and reconnect send no reaction RPC. Initial
choices are unselected; cached mine/count never grants a proof. Finite catalogs use
checkboxes and unrestricted ordinary catalogs use bounded literal emoji input;
there is no custom-ID text input. Confirmations list the entire new ordinary set
and every current choice that will be removed, including custom keys. No-op clears
and order-only proposals are disabled. Local ceiling16 and all known plugin/runtime
limits are enforced independently; empty/limit0 policy still permits an explicit
clear of a known nonempty selection. Read-only custom reactor filters may use
reported keys without the separate custom WRITE capability.

Panel proofs/generations never survive unmount/account switch. The UI evaluates
expiry using current `Date.now` on rendering/actions and repaints expiry locally;
it never refreshes automatically. A consumed/lost write remains blocked until a
new explicit refresh. Each context retains at most eight local page receipts;
filter changes do not restore its request/row budget, cached pages can be reviewed
without another RPC, and a reached local limit preserves the continuation rather
than presenting a false end. Late pages or refresh replies cannot replace a newer
filter, account, target or expired context.

## Sends

`enqueueText` validates a nonblank body and thread ownership, checks known posting restrictions and delegates to `Outbox::enqueue(SendText)`. Preserve the original text. An empty entity list is omitted when constructing the plugin request. Formatted sends require `send.entities`. Keep the original draft until acceptance. An ambiguous transport error requires reloading outbox and history before another user-initiated send.

`listOutbox` includes text and file sends. Whole-chat reads include topics, selected-topic reads are exact. Each receipt includes its actual thread, opaque client ID, `kind` (`sendText` or `sendFile`), nullable decimal `doneBytes`/`totalBytes`, and nullable echoRowKey. File transfer counters require a matching core upload observation. Cancel a queued/sending item using its exact receipt thread; delivery can win the race. Only failed text items support manual retry. A text row with an unknown delivery outcome requires explicit confirmation before retry. File delivery uncertainty stays visible through the existing outcomeUnknown projection.

## Single-file composer

The four operations and exact shapes are in [ADR0009](adr/0009-single-file-composer-ingress.md). Subscribe to account invalidations and read `listFileStages({thread, cursor:null, requestKey:null})` for the current file-ingress session. Other subsystems' tokens cannot authorize these operations.

- `startFileSelection({thread,sessionId,requestKey})` captures one explicit user choice. A same-key replay returns its existing process receipt. Linux desktop uses the native portal, sharing one reservation with Save. The browser demo has no provider unless its isolated devserver starts with `--file-fixture`.
- `listFileStages` reads up to 16 receipts for the exact thread. Supply either a current-process request key or a continuation token. Recovered durable stages have a null requestKey and the new session ID.
- `enqueueFile({thread,sessionId,stageId,caption,asFile,spoiler})` starts verification and returns a FileStage. The literal caption is limited to 1024 Unicode scalars and 4096 UTF-8 bytes. Rust rechecks live permissions and options, verifies the immutable staged bytes, and durably claims this stage before its single initial outbox admission.
- `cancelFileSelection({thread,sessionId,stageId})` requests local cancellation or explicit ready-file discard. Once admission has started, use `cancelOutbox` on an eligible retained delivery receipt. Failure to prove a ready file unreferenced retains its bytes.

`Inventory.composerAccess.chooseFile` indicates native provider setup. Selection/copy, local admission and network transfer have independent states. UI receives bounded metadata and opaque identifiers. Recover a lost start through its key, then read its stage; a lost enqueue never warrants a new file send. A consumed stage with a missing outbox row still proves local admission. A recovered claim with a missing row stays admissionUnknown. Neither permits resubmission. All file mutations can return transport outcomeUnknown; list is a read.

Retained bytes are limited to 2 GiB/file, 4 GiB/account, 8 GiB total and 16 byte stages. The service keeps consumed and uncertain files. Reaching a byte or stage ceiling can prevent future selections until reference-safe reclamation is implemented. A process accepts 4096 selection keys, at most four owned jobs and one active copy/hash scan. Explicit discard and reads remain available at the key limit.

## Message actions

ADR 0003 adds exactly four typed calls to the bundled `ch-core-v1` application API:

- `editText({ thread, messageId, text, entities })` replaces confirmed outgoing text. `entities` is required, uses UTF-16 spans, and `[]` explicitly clears formatting. Both `edit` and `send.entities` must be live, including for an empty entity list. Media captions, service messages, scheduled messages and incoming messages cannot be edited in this batch.
- `deleteMessages({ thread, messageIds, revoke })` accepts 1–100 distinct confirmed IDs in the exact thread. `revoke` is explicit: true requests everyone, false requests only the current user and requires `delete.for_me` in addition to `delete`. After confirmation the existing local user-deletion effect hard-deletes rows; remote deletions retain the existing tombstone policy.
- `pinMessage({ thread, messageId, silent })` pins one stored message and requires `pin`. Both boolean values of `silent` are carried explicitly. Personal-only pins are not exposed.
- `unpinMessage({ thread, messageId })` unpins one stored message and requires `unpin`. Unpin-all is not exposed.

Each call resolves to null only after its local effect commits. The bridge resolves the persisted account owner, verifies exact account/chat/topic ownership and rejects provisional `local:` IDs and unconfirmed outbox echoes before RPC. A whole-chat history can display topic messages, but mutations must target the displayed message's exact topic. Closed-topic posting restrictions do not prohibit these actions. The backend checks the current method and parameter capabilities, independently of UI visibility. The UI offers keyboard-accessible controls, confirmation dialogs, explicit deletion scope and disabled controls for provisional messages. Editing has its own dialog and never changes composer drafts.

The frozen edit response requires `MessageResult.message`; its ID, chat and topic must exactly match the requested target. Missing or foreign confirmation fails without invented local content. Confirmed responses use `Ingest::apply_edited`, `apply_deleted` and `apply_pinned`, publishing normal account invalidations. Event echoes before or after the reply converge through the same ingest rules. Application `Message.isPinned?: boolean` projects the stored pin state.

Application `CoreFailure.outcomeUnknown?: boolean` marks a mutation that may have executed, including malformed/mismatched confirmations and failed local persistence after platform success. The bridge invalidates the affected account. Both transports preserve this flag while replacing raw plugin messages with safe text; a lost transport reply is also unknown for these four actions. They never automatically replay a mutation. The UI blocks another action until a successful history reload, then requires another explicit user action after reviewing the result. A definite rejection leaves the database unchanged.

UI and backend must come from the same bundled version. Runtime method presence and current plugin handshake capabilities gate the controls; backend validation remains authoritative. This application amendment does not alter plugin API 1.0.0, schemas, SDKs or database migrations. Read receipts and synchronized drafts remain deferred.

## Accounts and sign-in

`addAccount` delegates to Accounts with a generated ID and starts the account. The integration fixture may provision an isolated fake account with the fake plugin's connected start setting. The generic add path uses the selected manifest and supplied settings. Use Accounts' settings projection. The response exposes nonsecret values and names of configured secret fields. On save, values replace ordinary stored settings. For a secret field, omission keeps it, null clears it, and a string replaces it.

The Rust bridge owns a transient current sign-in step per account. Subscribe to applied auth changes before starting accounts (CoreConfig supports deferred autostart). Set or replace the step on auth.step, clear it on completion, cancellation, account stop/removal or restart, and then publish an invalidation. Arm the listener before beginAuth. Forward begin/submit/cancel to typed auth operations for the account's verified owner. Begin accepts manifest methods when auth.interactive is active. Submit must match the current flow and step ID. Resend repeats beginAuth for the selected method. QR data is rendered as a payload. Keep submitted values and step data out of logs and persistent storage. If the bridge itself misses an ephemeral step, clear the affected cached flow, emit reset and let the user begin again.

Map validation and bad cursors to invalid_request, missing targets to not_found, missing capabilities to unsupported, rejected state transitions to conflict, transient host/service failures to unavailable, and unexpected failures to internal. Error messages shown to the UI must omit plugin dumps, credentials, local paths and request bodies.

## Pane integration

The shell passes the same CoreClient instance to named components ChatListPane, ThreadPane and Composer using the exported props. It owns selected account and selected chat, clears the chat when switching accounts, and mounts Composer once beneath ThreadPane. ChatListPane reports a selected ChatSummary through onSelectChat. ThreadPane and Composer receive that same chat, or null for the empty state. On receipt of updated chat rows the shell keeps the selected summary current. A successful enqueue appears through the committed outbox and history invalidations.

## Stored polls

ADR 0006 adds optional `Message.poll: PollSnapshot | null`, projected from confirmed
stored polls. A snapshot contains nullable `id`, literal `question`, `options`
(`id`, `text`, nullable unsigned decimal `votes`, nullable `chosen` and `correct`),
nullable unsigned decimal `totalVoters`, `closed`, `multiple`, `quiz`, `anonymous`,
nullable `closeAt: Millis`, nullable `solution`, and `solutionEntities`. Sparse values
stay null. Correct answers and solutions are redacted until a quiz is closed or a
server-reported personal choice is true. Deleted and provisional polls are omitted.

Exactly three typed operations return `{ poll: PollSnapshot | null }`:

- `votePoll({ thread, messageId, optionIds })` submits distinct opaque stored option
  IDs. Single-choice polls accept at most one. `[]` explicitly retracts a known
  normal-poll choice; quiz retraction and answered or unknown-choice quiz voting
  are rejected. No vote is sent while displaying or selecting an answer.
- `closePoll({ thread, messageId })` requests closing a confirmed outgoing,
  nonforwarded poll. The server decides actual rights.
- `refreshPoll({ thread, messageId })` reads that exact stored target through
  `messages.get`, including an older message outside the latest history page.
  It changes no history cursor or gap and never implies deletion for a missing result.

All three enforce exact account/chat/topic ownership and a running account. Writes
share the existing action lock and live method capability checks and also require
`messages.get` for recovery. Returned real messages must match the target and any
known poll ID, and use typed ingest. An absent message or absent returned poll is
legal and returns null without fabricating choices or closure. Ambiguous writes
use `outcomeUnknown`; both transports and development worker failures classify
vote/close as writes and refresh as a read. Nothing automatically replays a vote.

The poll panel captures its target independently of latest-history invalidations.
Every freshly opened panel requires an explicit targeted refresh before writes,
including after a chat/account switch or closing and reopening. It retains no
unbounded session registry and never reads on render. Missing confirmations and unknown outcomes block further writes until an explicit
targeted refresh reports a poll. Cached reloads cannot release that block. The user
reviews the reported state before choosing another action. Pending responses cannot
update a different account or a panel that has unmounted. Creation controls and a
new outbox representation are outside this slice.
