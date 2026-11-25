# ADR 0008: ordinary reaction membership and scoped reactor views

Status: accepted for implementation by root, 2026-11-25. This records contract approval
within the owner's ongoing full-development authorization. Baseline: integration
`b018c12`, with 27 CoreOperations after ADR0007.

## Decision

Accept cached ordinary reaction metadata, explicit scoped refresh, full-set membership
replacement/clear, and explicitly paged reactor metadata. Exactly three new
CoreOperations bring the bundled application API to 30. No frozen plugin API, schema,
SDK, database migration or dependency changes are included. Paid writes, effects/big
animations, unread controls, custom/sticker asset downloads, uploads and poll creation
remain outside this slice.

Accept **ordinary key membership only**. Telethon chosen_order=0 means mine=true, but
the plugin maps chosen_order to booleans and the frozen Reaction DTO has no
personal-order field. The aggregate array order is not personal chosen order. Do not add
order badges/reordering or claim order preservation/acknowledgment. Durable native
chosen-order parity remains pending a separately reviewed DTO extension; this ADR does
not silently remove that requirement from future work.

## Exact application types and methods

```ts
type OrdinaryReactionKey =
  | { kind: "emoji"; emoji: string }
  | { kind: "custom"; id: string };
interface ReactionItem {
  key: OrdinaryReactionKey;
  count: string | null;          // exact unsigned decimal; null for unprovable cached value
  mine: boolean | null;          // unknown is never false
}
interface ReactionSnapshot { items: ReactionItem[] }
interface ReactionAvailability {
  keys: OrdinaryReactionKey[] | null; // null = any ordinary emoji, per frozen contract
  allowCustom: boolean;
  maxPerMessage: string | null;       // reported runtime limit; null is unknown
}
interface ReactionContext {
  sessionId: string;
  contextToken: string | null;
  expiresAt: Millis | null;
  snapshot: ReactionSnapshot | null;
  availability: ReactionAvailability | null; // policy read failed/unsupported = unknown
  canSet: boolean;                          // backend permission/proof, rechecked on write
}
interface ReactorItem {
  userId: string;
  key: OrdinaryReactionKey;
  name: string | null;
  at: Millis | null;
}
refreshMessageReactions(p: { thread: ThreadKey; messageId: string }): Promise<ReactionContext>;
setMessageReactions(p: { thread: ThreadKey; messageId: string; sessionId: string;
  contextToken: string; keys: OrdinaryReactionKey[] }): Promise<{ snapshot: ReactionSnapshot | null }>;
listMessageReactors(p: { thread: ThreadKey; messageId: string; sessionId: string;
  contextToken: string; key: OrdinaryReactionKey | null; cursor: string | null }):
  Promise<{ items: ReactorItem[]; nextCursor: string | null }>;
// Optional read-only Message.reactions?: ReactionSnapshot | null.
```

Application keys are strict tagged unions, mapped to frozen emoji/custom_emoji_id
objects in Rust. Paid is unrepresentable in the app input. Keep literal emoji sequences,
variation selectors and ZWJ; compare exact key identity, never display text or
normalization. Require nonempty emoji of at most 128 UTF-8 bytes, custom IDs to existing Id limits. No
user-supplied plugin IDs, method names, asset URLs or native paths.

## Refresh proof and bounded state

- All three methods verify persisted account→plugin, enabled/running account, confirmed
  stored account/chat/topic/message and reject deleted/scheduled/local/ unconfirmed
  echoes. A whole-chat view passes the displayed message's real topic. Refresh and write
  share the existing thread action lock with text/poll actions.
- Refresh explicitly calls messages.get and reactions.available(account,chat)
  concurrently, avoiding a sequential two-timeout request. Require messages.get;
  availability additionally needs reactions.set. This is one user-triggered refresh,
  never a render/invalidation/reconnect effect. A failed policy read can return a
  matching message snapshot with availability=null and canSet=false. It never receives
  an invented unrestricted policy. No automatic hydration retry in app.
- Validate returned id/chat/explicit topic before ingest; preserve a known stored topic
  when omitted as in polls. Apply the real Message through typed ingest, but project
  this operation's snapshot from its ORIGINAL reported reactions, not the merged stored
  rows. Missing Message or absent reactions remains snapshot=null; it is not deletion,
  known-empty or personal mine=false. Reported [] is known empty ordinary membership.
  Paid metadata may still be ingested, while the app explicitly displays only ordinary
  reactions.
- Issue a random server-owned context token bound to app session, plugin/account, exact
  thread/message row identity, full reported state and current stored reaction-row
  baseline. A matching Message with absent reactions may get a read-only context for
  who; canSet requires a complete snapshot and policy. Missing Message gets no token.
  Expiry 60 seconds, at most 64 contexts globally; prune expired idle contexts,
  otherwise refuse new capacity. Do not evict an in-flight context to admit more work.
  Server mints tokens; unknown/expired/consumed tokens ALWAYS reject, never become new
  intents. Thus discarded spent tokens cannot become replay opportunities. No
  request-key ledger is necessary for a one-use write proof.
- A successful refresh supersedes all older unused contexts/cursors for that exact
  account/chat/topic/message target. A panel clears its current proof when a new refresh
  starts and uses a monotonically increasing local request generation: only its newest
  matching reply can install state/proof. Older/different-target replies are ignored,
  even when they arrive later. A failed refresh leaves the panel write-disabled.
  Switching/unmounting never resurrects a proof.
- Write consumes/removes the context atomically BEFORE dispatch, also invalidating its
  reactor cursors. It checks current live capabilities, row identity, deleted/ enabled
  state, expiry and that the DB reaction baseline has not changed since refresh. A
  mismatch is a pre-dispatch conflict, requiring another explicit read. This is an
  immediate local validation/proof, not a server-atomic compare-and-swap; remote
  state/rights can change after the read and the server remains authoritative.
  Account/plugin disable/remove invalidates contexts. Cached latest reads never mint a
  proof. Fresh panels start without one; remount/account switch cannot grant writes. No
  unbounded per-panel map or automatic retry.

Use at most 4 in-flight reaction operations globally (a refresh has at most 2 host calls,
so at most 8 pending host RPCs). On slow/lost browser replies the underlying owned
operation retains its action lock/quota until host settlement or shutdown; normal
reaction methods have the default 30-second host timeout, not media's 600. No automatic
resubmit on the browser's 30-second boundary. Reads fail ordinarily; write
transport/worker failures are outcomeUnknown. Stop new work on shutdown and cancel/drain
owned calls without leaving replacement workers or locks behind.

The implementation seam acquires the existing OwnedMutexGuard and operation quota,
validates/preloads the scoped target, then moves those guards plus owned Core/DB/
change/state clones into an owned task with a oneshot reply. Dropping a caller's
receiver must not drop the native RPC future, guard or permit. Four total owned tasks
remain the hard admission cap. Shutdown cancels/drains them without an Arc<App> cycle.
No public App::request receiver change or broad action refactor is required for this
slice.

## Ordinary policy and full-set write semantics

reactions.available and reactions.set BOTH require capability reactions.set;
reactions.who separately requires reactions.who. Every custom key also requires
reactions.custom through the registry parameter gate. Backend gates are authoritative.
An object-valued reactions.set.max is a declared upper bound; the per-account
max_per_message is separately reported and may be unknown. Never turn unknown into
0/1/unlimited. Known bounds and local selection ceiling 16 are checked independently;
server rights/entitlement remain authoritative. Explicit [] can clear a known selection
even when the currently offered policy is empty/limit 0; server decides whether clearing
is permitted. The UI disables no-op clear for known-empty state.

Offer server-listed ordinary keys, including listed custom IDs only when the custom
capability is live. For reactions=null offer bounded literal emoji input (not a fake
exhaustive catalog). Arbitrary custom-ID entry/assets are deferred. Preserve existing
custom choices as visible current metadata. The selector builds an EXPLICIT complete new
set, never a chip-toggle inferred from incomplete mine data; the confirmation states
that it replaces all current ordinary choices and lists removed choices, including
custom ones. No hidden clearing of an unshown selection. Paid reactions are not part of
this ordinary input set: never construct a paid key, call reactions.paid or try to
clear/restore paid counts. Apply the original confirmed Message through ingest without
deleting its unrelated paid metadata. The ordinary membership comparison filters paid
keys; it never treats paid state as a selected ordinary key. The accepted app
interpretation of allowCustom is permission for arbitrary custom choices outside a
finite catalog. It does not veto an explicitly listed custom key when reactions.custom
is live. This matches Telegram ChatReactionsSome, which can return listed custom keys
with allow_custom=false. Finite availability keys remain authoritative membership
bounds: do not add outside-catalog keys even if allowCustom=true. This batch has no
arbitrary custom-ID entry in either mode.

Require distinct keys, <=16 and any known lower maximum, allowed policy and current
caps. Preserve the submitted vector literally into ReactionsSetParams; hardcode
big=false. Do not promise chosen order. Reject order-only/no-membership-change requests
in this batch instead of quietly interpreting them as reorder commands.

The response is frozen OptionalMessageResult. Absent Message or absent reactions is
legal and returns snapshot=null, leaving the consumed proof unusable. Identity mismatch,
malformed reply, failed local persistence or a reported ordinary mine SET differing from
the requested SET is outcomeUnknown; never invent selection, counts or a local success.
Validate full membership before local application. A matching full confirmation uses
existing typed ingest, returns the exact reported snapshot and account invalidation, and
grants NO replacement write token. Another write requires explicit refresh/review.
Events before/after reply converge through ingest. A late response cannot act on a new
account/panel. No automatic write replay.

## Reactor paging and rendering

Who uses an existing fresh read context even if it is read-only for writes. Backend sets
limit 32; no user-controlled limit or unbounded fetch. Context/key/cursor must match
exact account/chat/topic/message. Map native next_cursor to an opaque app cursor bound
to context+filter, never forward an arbitrary renderer cursor. Cache at most 128
cursor/page receipts globally, expire/remove with their context; refuse new cursor
capacity rather than retaining an unbounded map. Max 8 who RPC attempts and 256 returned
rows per context across filters. A changed filter resets the visible traversal, not that
context's total budget. Re-reading a cached app cursor returns that page without another
RPC or budget charge. Reject repeated/cyclic native continuation. Empty/short pages WITH
a new cursor still permit an explicit Load more; absence alone ends this traversal. No
automatic page loops, and never claim a paged view is a transactionally stable
exhaustive list of all reactors. On reaching the local budget while a continuation
exists, show the limit and require an explicit fresh view; never turn it into a
fabricated end cursor. Who completion must recheck context liveness before installing
page/cursor receipts, so a consumed/expired context cannot be recreated by a late read.
Removed message, disabled account, stale context or foreign cursor fails safely and
clears controls.

Project only userId, ordinary key, optional literal name and optional timestamp. No
unread control, paid UI, avatar, asset fetch or person/chat merge. Who pages may include
this account; Message.reactors specifically excludes it, so do NOT persist paged who
data into that field. Names are display strings capped at 256 UTF-8 bytes at a character
boundary, with explicit ellipsis when shortened; IDs/keys are exact and validated.
Custom keys display 'Custom emoji <id>' as metadata, not guessed Unicode or an image
URL. All text is escaped; no HTML/asset protocol.

Bound a complete fresh snapshot to 128 entries and availability to 256 ordinary keys. An
oversized fresh reaction array is an ordinary unsupported read, without issuing a proof.
An oversized policy is unavailable (null), leaving the matching snapshot read-only.
Bound every new app operation result to 128KiB serialized; if the combined result
exceeds it, fail unsupported and issue no token. A write confirmation exceeding bounds
is post-dispatch outcomeUnknown. Never truncate an array into a full-set proof. Cached
message chips can show at most 8 ordinary entries, always with a 'View/refresh
reactions' affordance rather than a false claim about how many omitted reactions exist.
First panel render and selecting a checkbox send no RPC/write; only explicit Refresh,
Apply complete set, Clear and Load more do. Left alignment/accent policy remains.

## Existing storage limitations and verification

SQLite has no reactions-known-empty marker. Zero cached rows therefore projects null,
not []. Cached mine is conservatively null until a scoped full read, never false by
omission. The DB stores counts as signed i64 using a u64 cast; negative stored counts
must project unknown, not reactions_dto's clamped0. Fresh reads and confirmations
project raw u64 counts to decimal strings. This app batch does not claim durable exact
counts above i64::MAX; that needs separate core/DB review.

Source evidence: [frozen reaction methods](../../schemas/methods/reactions.json),
[capability/parameter gates](../../schemas/methods/registry.json), [ReactionKey/Reaction
DTOs](../../schemas/dto/message.json), [plugin API](../plugin-api.md) sections
Reaction,11.6,14.2; [stored reaction
queries](../../crates/ch-db/src/queries/messages.rs) and [ReactionRow
conversion](../../crates/ch-db/src/types.rs); [Telegram
mapping](../../plugins/telegram/telegram_plugin/reaction_info.py) and
[available/set/who/hydration](../../plugins/telegram/telegram_plugin/reactions.py).

Implementation and verification must update Rust wire/dispatch/projections, both
transports, fixtures, real UI and app API docs together. Read operations do not receive
outcomeUnknown solely for a read failure; set and its development worker/transport
failures do. Test with isolated fake data; no live-account or native GUI parity is
claimed by this ADR.

Required fake/regression checks: absent vs [] vs explicit false; min/unknown never
preselected; chosen_order0 and lack of order claims; duplicate/wrong keys/caps and
custom catalog semantics; unknown paid metadata never changes ordinary input; full-set
mismatch, missing optional reply and lost response never replay; expiry/
consumption/capacity and account-switch/newest-refresh proof guards; exact topics and
removed messages; stale DB reaction baseline; scoped older refresh without history gaps;
uint64/raw precision vs corrupt cached negative counts; empty/short/cyclic reactor
pages, foreign filter/cursors, bounded names and 128 KiB budget; no asset/download
calls; real-App explicit replacement/clear and late-panel responses. Heavy checks remain
remote. This ADR accepts the bounded contract; implementation ownership is assigned
separately so parallel builders do not edit each other's files.
