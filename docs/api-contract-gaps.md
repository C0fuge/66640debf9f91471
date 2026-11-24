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
