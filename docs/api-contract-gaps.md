# API contract gaps and proposed amendments

Status: **reviewed subset of Amendment 1 accepted by the integration owner; remaining additions are proposals only**. Evidence baseline: `b14c48f` (2026-11-24). Telegram work after that commit must be checked separately. The frozen plugin contract remains API `1.0.0`. Integration review accepted only `editText`, `deleteMessages`, `pinMessage`, `unpinMessage`, optional `Message.isPinned` and optional `CoreFailure.outcomeUnknown`. `markRead` and all draft operations remain unaccepted pending separate review of ghost authority and draft revisions. See integration ADR 0003 when present.

The 121 registry entries are 119 core-to-plugin methods and two plugin-to-core secret-store services. They are a shared adapter vocabulary, not the full API of any platform. There are 18 frozen event names. A second, independent gap exists at the application boundary: `CoreOperations` currently has 16 calls for inventory, account/auth, cached history, local search and text outbox. It exposes no message edit/delete/pin, remote read receipt, synchronized draft, media, reaction, group, privacy or story operation. Implementing the plugin registry alone cannot deliver those features to a user.

Evidence: [plugin registry](../schemas/methods/registry.json), [events](../schemas/events.json), [CoreOperations](../app/ui/src/core/index.ts), [Rust request allowlist](../crates/ch-app/src/wire.rs), [dispatcher](../crates/ch-app/src/bridge.rs), [existing application contract](app-api.md).

## Amendment 1 — first runnable application actions

**Recommendation: accept an additive, typed application bridge batch, using the existing plugin API unchanged.** This is an application contract amendment, not a new plugin method set. Keep the existing `ch-core`/`ch-protocol`/SDK boundary. Add explicit `CoreOperations`, Rust request variants, projections, adapter methods and UI controls together.

### Proposed first batch

All targets use the existing `(accountId, chatId, topicId)` `ThreadKey`; IDs remain opaque. `messageId` is a confirmed platform ID, never an outbox provisional ID or row ID. Omitted entities preserve the stored entities only when text is unchanged; for a replacement text the bridge sends an explicit full replacement entity list, including `[]` to clear formatting. Validate UTF-16 spans against the new text.

```ts
// Additions to the application wire contract; proposed names, not implemented.
interface DraftSnapshot {
  text: string;
  entities: TextEntity[];
  replyTo: string | null;
  updatedAt: Millis | null;
}
interface CoreOperations {
  editText(params: { thread: ThreadKey; messageId: string;
    text: string; entities: TextEntity[] }): Promise<null>;
  deleteMessages(params: { thread: ThreadKey; messageIds: string[];
    revoke: boolean }): Promise<null>;
  pinMessage(params: { thread: ThreadKey; messageId: string;
    silent: boolean }): Promise<null>;
  unpinMessage(params: { thread: ThreadKey; messageId: string }): Promise<null>;
  markRead(params: { thread: ThreadKey; maxMessageId: string }): Promise<null>;
  getDraft(params: { thread: ThreadKey }): Promise<DraftSnapshot>;
  saveDraft(params: { thread: ThreadKey; draft: {
    text: string; entities: TextEntity[]; replyTo: string | null;
  }; expectedUpdatedAt: Millis | null }): Promise<DraftSnapshot>;
  syncDraft(params: { thread: ThreadKey; expectedUpdatedAt: Millis }): Promise<null>;
}
```

`expectedUpdatedAt` is a compare-and-set against the stored draft. On a missing draft, return empty text/entities and null reply/time. Check the precondition and write in one transaction. The implementation must make local update tokens strictly increase, including two saves in one millisecond; if the existing timestamp representation cannot do that, replace this proposal with an explicit opaque revision before implementation. Null means "still absent", not "ignore conflicts". An empty text intentionally clears the draft. Preserve a local clear revision long enough for `syncDraft`; if the existing empty-draft deletion loses it, an additive local revision/tombstone migration is required (see below). Never invent cloud success because the local draft was saved.

| Application operation | Existing plugin method / capability | Backend effect after confirmed success | Existing event / UI result |
| --- | --- | --- | --- |
| `editText` | `messages.edit` / `edit`; `send.entities` when nonempty | Validate stored target is outgoing text and belongs to exact thread; call with typed `MessagesEditParams`; require the typed `MessageResult.message` (required by the frozen schema), verify its exact chat/message identity and topic when supplied, then merge through `Ingest::apply_edited`. Missing/mismatched confirmation is a protocol failure with an uncertain outcome; refresh to reconcile. `messages.get` has a different, optional-message result and is not an edit-result fallback. | `message.edited` echo is idempotent; publish normal ingest invalidation; reread history. |
| `deleteMessages` | `messages.delete` / `delete`; `delete.for_me` for `revoke=false` | Resolve each target in exact thread; reject empty/oversize lists; after success `apply_deleted`. Existing local user-deletion semantics physically remove rows, unlike remote tombstones. Preserve that rule. | `messages.deleted` duplicate is a no-op; reread history/chat preview. UI explicitly offers the supported deletion scope. |
| `pinMessage`, `unpinMessage` | `messages.pin` / `pin`; `messages.unpin` / `unpin` | Exact stored target, then `apply_pinned`. First batch omits unpin-all and `for_me_only` to avoid guessing platform semantics. | Normal messages invalidation; add optional `isPinned` to application `Message` projection for visible state. |
| `markRead` | `read.send_receipt` / `read_receipts` | Read the authoritative core ghost setting. With ghost on, perform only `apply_read`; make no platform call. Otherwise require capability and confirmed RPC success, then `apply_read`. The watermark must be a stored message in the exact thread. | `read.inbox` echo; list unread counters and visible history invalidate. Mark only the visible active conversation through its displayed watermark. |
| `getDraft`, `saveDraft` | Local DB, no platform capability required | Read/write through the Rust-owned DB and `apply_draft`, preserving thread isolation and conflict checks. Local persistence works without `drafts.sync`. | Existing `draft.changed` ingest path / `DraftChanged` invalidation; composer reloads without overwriting unsaved newer typing. |
| `syncDraft` | `drafts.save` / `drafts.sync` | Read the exact saved revision, serialize cloud saves per thread, send typed frozen `Draft`, and leave a newer local draft intact. A stale revision returns `conflict`. No automatic durable retry is promised in this batch. | No new plugin event. After success mark only that revision acknowledged in transient state; a later `draft.changed` follows existing timestamp merge rules. |

Evidence for effects: [local ingest methods](../crates/ch-core/src/ingest/local.rs), [draft DTO](../schemas/dto/draft.json), [message methods](../schemas/methods/messages.json), [read methods](../schemas/methods/read.json), [draft methods](../schemas/methods/drafts.json), [DB draft query](../crates/ch-db/src/queries/chats.rs). The current composer keeps its drafts in memory; that is not synchronized-draft implementation ([composer store](../app/ui/src/components/composer/drafts.ts)).

### Validation, ownership and failure behavior

Resolve the account's plugin from persisted account ownership, never a browser-supplied `pluginId`. Resolve chat/topic/message ownership before a platform mutation. Do not reject a permitted delete merely because a topic is closed; operation permissions differ from text posting rights. Check method and parameter capabilities against the current handshake on the backend; UI hiding is supplementary. Keep schema typing, host timeouts, redaction and account lifecycle checks.

A platform RPC and SQLite transaction are separate operations. Mutations return only after the local effect commits; a lost response or failed local apply after platform success is an ambiguous outcome. Add optional `outcomeUnknown?: boolean` to `CoreFailure`, expose no raw error data, trigger an invalidation and have the UI reload before a user-initiated retry. Do not automatically replay edit/delete/pin calls through the browser/native adapter. Existing event echoes can reconcile a mutation; an empty result alone cannot synthesize server metadata.

Drafts are local-first. A cloud failure must never erase accepted local text. For the first batch, `saveDraft` commits locally and `syncDraft` reports cloud success/failure separately. Client-side debounce can call the latter; app restart does not imply queued cloud delivery. A later durable draft-sync queue needs a separate amendment. Keep a per-thread generation in the composer so incoming snapshots and delayed replies cannot overwrite newer typing or another selected thread. A remote draft update racing local editing must surface a conflict, preserve local text and let the user choose; timestamps alone do not establish causality between devices.

### Explicit methods versus a registry-gated generic call

| Choice | Advantage | Required restrictions / cost |
| --- | --- | --- |
| Explicit typed actions (**recommended**) | Eight small additions correspond to concrete UI actions and existing ingest effects; easy to verify account/thread ownership, redaction and local reconciliation. | Extend TS + Rust allowlists/projections/adapters together; reuse shared validation/effect helpers. No plugin protocol change. |
| `callPlugin({method, params})` gated only by registry | Less initial dispatcher syntax. | Insufficient: the registry permits file paths, lifecycle, auth, paid operations and secret services with different ownership/effect rules. Registry/capability presence does not prove safe UI access or DB effects. |
| Strict allowlisted discriminated `performAction` | Can reduce wrapper repetition once many actions exist. | Requires an additional app-only allowlist, typed params/results per action, injected account identity, target ownership validation, separate local effect handler, error projection, path/file-token mediation, and denial of vendor/lifecycle/secret calls. With eight actions this reconstructs explicit dispatch with a more generic name. |

Internal Rust `HostHandle::call`/`call_raw` remains useful behind typed handlers. Do not expose it directly to renderer code. The host currently permits vendor method prefixes; an application generic bridge must not inherit that permission. [Host call enforcement](../crates/ch-core/src/host/handle.rs).

### Version, migration, SDK and test impact

- **Plugin API:** remains `1.0.0`; no registry/capability/event change and no generated SDK change for Amendment 1. Telegram handlers can proceed independently against the existing frozen contract.
- **Application wire:** additive change to the current `ch-core-v1` contract. Update TS declarations, Rust request variants, both adapter maps and documentation in one commit. Keep old 16 calls unchanged. Document the new optional failure/message fields. If independently deployed UI/core versions are supported, add an explicit app feature-negotiation response before showing these controls; otherwise require the matching bundled build.
- **DTOs/events:** reuse frozen plugin DTOs; `DraftSnapshot`, optional `Message.isPinned`, optional `CoreFailure.outcomeUnknown` are application projections. Existing account/reset invalidations suffice for this small batch. Do not claim that a stored field is already exposed by `CoreClient`.
- **DB:** edits/deletes/pins/reads already have local effects. Draft compare-and-set and clear need verification: the existing draft row may be deleted for empty text. If so, add a local revision/tombstone table or keep an empty revision row with a reviewed additive migration; retain rollback/reopen compatibility and test older databases. This proposal does not approve a migration implicitly. A root-approved reduction can ship actions/read first and defer drafts until this rule is settled.
- **Backend tests:** isolated fake-plugin round trips for each action; missing capability and parameter gate rejected before RPC; foreign account/chat/topic/message rejected; ghost mode sends zero receipts; remote denial leaves DB unchanged; confirmed mutation commits the intended effect once; event-before-reply and event-after-reply are idempotent; response loss/local-apply failure is marked unknown. Draft tests must cover same-ms saves, stale saves, empty clear, restart persistence, older remote echo, typing while sync is in flight and account/topic switches.
- **UI/adapter tests:** both transports carry exact typed requests; no automatic mutation replay; controls follow live capabilities; edit/delete/pin effects refresh visible rows; incoming changes do not replace a newer composer draft; deletion scope is explicit; draft cloud failure keeps local text. Meaningful existing suites live in `crates/ch-app/tests`, `crates/ch-core/tests`, and `app/ui/tests`. Run targeted tests and required CI after implementation; this audit does not run a local heavy build.

The accepted first batch is exactly the first four action methods and the two optional application fields named above. It requires no plugin/SDK/schema/DB change. Read and draft operations require another explicit integration review, including the authoritative ghost setting and a draft revision design. The rest of this file records broader contract gaps; none are implemented by this proposal.

## Amendment 2 — authoritative read/ghost policy and durable draft revisions

**Unaccepted.** Split the deferred four read/draft operations out of Amendment 1. No plugin method change is necessary for a basic implementation; the application/core behavior needs review first.

The core must own one authoritative runtime privacy policy used by *all* outgoing receipt, typing, story-view and call/presence paths. `AccountSpec.ghost` is a start input, not by itself a universal call guard. Add a core service that reads policy and suppresses requests before dispatch; UI parameters must never bypass it. Define whether a local read without remote receipts is allowed when capability is absent, and how account restart/policy updates converge. Preserve the existing local `apply_read` effect without claiming a remote acknowledgment.

For drafts, `ch_db::set_draft` deletes empty rows and `Ingest::set_draft` currently treats equal text as a no-op even when entities/reply target differ. A timestamp alone is therefore not a safe local revision token. Proposed design: additive `draft_revision` records keyed by account/conversation/topic, with monotonically increasing opaque local revision, clear tombstone and cloud-ack revision. Compare full draft content; retain old timestamps only as remote merge hints. Local save/clear and revision advance must be atomic. Keep the original `draft` data readable by the current core; review migration ownership and downgrade behavior. An in-flight acknowledgment must not clobber a newer revision. A durable cloud-sync queue, if required, must be explicit; do not quietly promise retries with only an in-memory debounce.

Impacts: plugin API/SDK unchanged; add the deferred typed CoreOperations and local projections, a reviewed DB migration, core guard/sync services and conflict-aware composer. Tests: equal-text entity/reply changes, clear/restart, same-ms saves, stale local/remote writes, event/response reordering, account/topic isolation, policy changes and zero prohibited network calls under ghost mode. [DB draft deletion](../crates/ch-db/src/queries/chats.rs) · [draft merge](../crates/ch-core/src/ingest/mod.rs) · [account start defaults](../crates/ch-core/src/runtime/accounts.rs).

## Amendment 3 — remote search, saved dialogs, discussion identity and administration

**Unaccepted; design candidate.** Introduce optional domain methods in a minor plugin API release, provisionally `1.1.0`: paged `search.messages` (chat/global/sender/media/date scopes and opaque cursor), `saved_dialogs.list/history/set_flags/reorder`, saved-tag operations, missing forum delete/reorder/settings, admin-log queries and detailed invite/ownership workflows. Existing local UI `search` keeps its behavior and name. Pinned raw evidence includes Telegram `messages.searchGlobal/getSearchResultsCalendar/getSavedDialogs/getSavedHistory/getSavedReactionTags`, `channels.getAdminLog`, and MAX enum-only `PUBLIC_SEARCH/MSG_SEARCH`; MAX payloads remain unknown, so do not advertise a wrapper on that evidence alone.

Define `SearchPage/SearchHit`, explicit saved-peer and parent-post identities, admin-action/cursor DTOs and refresh/invalidation events. Saved dialogs are not forum topics; channel comments need their root/post identity through edit/delete/history, especially PyMax's explicit `post_id`. Reuse ordinary Message IDs only where the platform's identity rules actually agree. Add local indexing/cache tables only for accepted domains and never merge cross-platform threads through person identity.

Impacts: registry/capability/schema entries, generated Rust/Python SDKs, optional event dispatch, local cache/query migrations, typed core services and new CoreOperations/UI. Test cursor scope/reuse/expiry, rights restrictions, deleted parents, account isolation and old-plugin fallback. Keep unsupported native admin/search variants visible in the scope ledger until resolved. Existing frozen folders/groups/members methods can be implemented without this amendment; their incomplete coverage is not a reason to invent new methods prematurely.

## Amendment 4 — complete story/status lifecycle

**Unaccepted; design candidate for an additive plugin minor release.** Preserve existing `stories.list` and `Story`. Propose optional `stories.feed/get/publish/edit/delete/set_pinned/read/react/views/archive/albums` domains and story upsert/delete/read/reaction invalidations, with smaller capabilities per operation. A feed/list cursor, owner peer (including channels), publication/privacy, media areas, audience/view state, archive/expiry and view-stat DTOs require explicit schemas. Existing `Story.user_id` and required media are insufficient for all channel-owned or text-only WhatsApp Status cases; add a parallel richer DTO/method result rather than changing existing required fields in place.

Evidence: Telegram `stories.*` and story updates; PyMax's publish/edit/delete/read/reaction/stats wrappers plus story-preview notification; WhatsApp `Status::send_text/send_image/send_video/revoke`. Online presence is a separate concept. Story views are externally visible writes and need the authoritative core ghost policy before dispatch.

Impacts: schemas/registry/capability keys, SDK generation, event codec/ingest and expiry scheduling, story/media cache tables, typed CoreOperations, reader/composer/privacy controls. Tests: expired/deleted content, missing media, owner identity, cursor paging, privacy restrictions, read suppression, duplicate events and restart convergence. Until accepted, implement only honest existing list support and mark all writes/events unavailable.

## Amendment 5 — calls and a separate media plane

**Unaccepted; requires architecture review, not a list of signaling wrappers.** Telegram has personal/group/conference signaling; MAX confirms native calls while PyMax only names relevant opcodes; whatsapp-rust has signaling plus gated runtime/codec facades. No frozen method, capability, DTO or event models call sessions.

Propose optional session-oriented `calls.start/accept/reject/end/get`, group participant/join/presentation controls and `call.changed`/participant/signaling-control events. Define call ID/generation, state transitions, media kind, peer identities, reasons, device/permission state, timers and cancellation. Keep audio/video/encoded frames outside the NDJSON DTO stream; choose a bounded local media transport and native device ownership separately. Telethon provides signaling, so a Telegram call engine/codec implementation is a distinct dependency. MAX payload/media research is unresolved. WhatsApp feature/codec compatibility must be established in CI and controlled integration tests.

Impacts: plugin `1.1.0` optional domains or later minor after review; SDK session DTOs; core call coordinator, device/media service and scoped routing; call log migration; typed CoreOperations and call UI; no credentials/media bytes in logs. Test state-machine races, late callbacks from an old generation, multi-account isolation, incoming call while stopped, permission denial, loss/reconnect, hangup cleanup, privacy/ghost policy and media queue limits. Do not equate an incoming call service-message DTO with a working call session.

## Amendment 6 — Business, quick replies and native domain collections

**Unaccepted; additive domain proposal.** Telegram owner Business hours/location/intro/away/greeting, connected-bot rights and chat links need explicit operations/DTOs; quick replies need shortcut collections/messages and mutations. User-owner operations must be separated from bot-server events. WhatsApp business profile/labels, communities and newsletters need their own identities/topology/roles; ordinary group/Profile DTOs represent only subsets. MAX channel-comment moderation and native-service/WebApp trust flows need payload and product review.

Proposed domains: `business.profile/settings/links/connections`, `quick_replies.*`, `labels.*`, `communities.*`, and explicit newsletter management. Select exact method names only after parameter, pagination, permission and event review. Frozen profile/group/bot-button operations remain reusable where their semantics fit.

Impacts: optional plugin minor additions with generated SDKs; typed collection DTOs, updated/deleted refresh events and account-scoped storage; typed CoreOperations and owner/admin surfaces. Test role/tier refusal, rights narrowing, deleted collection items, reconnect snapshot reconciliation and older-plugin fallback. Public Business documentation and pinned wrapper/source references appear in [platform coverage](platform-coverage.md).

## Amendment 7 — Stars, gifts, checkout, Premium, boosts and analytics

**Unaccepted; separate read and mutation contracts required.** Paid reactions and their balance are a small frozen subset. Telegram Stars balance/transactions/subscriptions, gift catalog/inventory/transfer/upgrade/collections, checkout/shipping/receipt workflows, Premium gift codes/giveaways, boosts and admin analytics have no complete shared DTO/method/event chain. Do not classify personal-account financial/owner features as bot-only.

Propose distinct optional read capabilities and explicit user-initiated mutation capabilities. Use exact integer amount/currency representations with publisher-defined scale, immutable transaction identifiers, opaque cursors and server confirmation. Keep invoice/receipt, gift ownership and call/message/story metadata separate. Core must not retry a spend/transfer after uncertain completion without reconciliation. Role/tier/region constraints are returned runtime restrictions.

Impacts: reviewed optional plugin minor release, generated SDK financial/domain DTOs and event handling, account-scoped ledgers/caches, typed CoreOperations and review/confirmation screens appropriate to the actual action; tests for idempotency/uncertainty, double submission, amount precision, rights and account isolation. This audit proposes contracts only and performs no transactions.

## Amendment 8 — rich message/profile/security completeness and scope decisions

**Unaccepted; one reviewed family at a time.** The source ledgers retain todo lists, fact checks, suggested/paid posts/media, location/live location, link-preview controls, multiple usernames/profile music/collectibles, full notification/privacy exceptions/global settings, passkeys, recovery/phone/account deletion, shared folder links, sticker authoring, reports, bot interaction/inline/WebApps and secret chats. Message/Conversation/Profile `ext` can preserve platform detail but does not make a feature reachable or typed. Secret chats require a clear product decision plus E2E state and device/key ownership design; do not implicitly include or exclude them by analogy to a native desktop client.

For each accepted family, prefer additive optional fields when the old shape remains valid; add parallel DTOs/methods where required fields or meanings differ. Define explicit unsupported rendering for new message kinds and retain source identifiers for later hydration. App appearance/i18n/cache preferences can be local core features; cloud synchronization is a separately named platform feature. Mobile device push/SMS relay and bot-account server management can be named exclusions with evidence. Unknown wallet/communities/ephemeral/browser/onramp/AI fields remain unresolved, not an automatic parity commitment.

Impacts vary by family but must enumerate schema/SDK/event/storage/core/UI/tests before approval. Do not add required fields or alter existing defaults in plugin v1. A breaking reinterpretation requires a major version and the deprecation/previous-major support policy in the existing spec.

## Amendment 9 — versioned capability and coverage acceptance

**Unaccepted process proposal.** Keep the present 1.0.0 contract frozen while implementing existing methods and the accepted application actions. For any accepted protocol addition, record an ADR with exact optional methods, param gates, result DTOs, events, version and migration plan. Regenerate Rust/Python SDKs and conformance fixtures from the same schemas. A new capability is advertised only after all of its gated methods and required behavior are implemented; an upstream enum, stub or empty success cannot justify it.

The plugin handshake compares majors today. Before emitting new optional event names, define how an old core ignores or rejects them and gate emission using peer version/negotiation. Unknown-field tolerance does not prove unknown-event support. New optional method support is detected through capabilities. Bundle app/CoreOperations changes or negotiate application feature availability separately; plugin `api_version` does not version the renderer bridge automatically.

For each feature, the acceptance record must link raw source and access evidence → frozen or approved amended method/capability → exact DTO/event → plugin handler → core effect/DB migration → typed CoreOperations/UI → targeted offline tests → CI → separately authorized controlled live verification. The current audit establishes source/mapping evidence only. Review public API/layer pins at planned milestones so the scope remains current and bounded rather than an unlimited release race.
