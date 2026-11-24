# ADR 0003: typed message actions in the application bridge

Status: accepted for implementation, 2026-11-24.

## Decision

After reviewing Amendment 1 in `docs/api-contract-gaps.md`, accept only the first
four application operations: `editText`, `deleteMessages`, `pinMessage`, and
`unpinMessage`. The plugin API stays at 1.0.0. Schemas, generated protocol types,
SDKs and DB migrations do not change in this batch.

Use the proposed typed parameters with the existing account/chat/topic ThreadKey.
Add optional `isPinned` to the application Message projection and optional
`outcomeUnknown` to CoreFailure. Update Rust wire dispatch, both TS adapters,
documentation, UI controls and meaningful regression checks together. Bundled UI
and backend versions must match. No generic renderer-to-plugin RPC is exposed.

Validate account ownership, exact chat/topic/message identity and current method
and parameter capabilities before any platform call. Edit only confirmed outgoing
text messages in this batch. Deletion supports an explicit revoke scope, a
nonempty bounded list, and rejects provisional/outbox IDs. Pin/unpin targets one
stored message; unpin-all and personal-only pin modes remain outside this batch.
Closed-topic posting restrictions must not automatically forbid other actions.

The frozen `messages.edit` result REQUIRES `MessageResult.message`. Validate its
confirmed ID/chat/topic before applying it through `Ingest::apply_edited`.
An absent or mismatched confirmation is an error, never locally invented text.
After confirmed delete/pin/unpin responses, use existing local ingest effects and
normal change invalidations. Both event-before-response and event-after-response
must converge without duplicate snapshots or rows.

RPC and SQLite effects cannot be atomic. If the call may have reached the plugin,
or local persistence fails after confirmed platform success, report an ambiguous
outcome, invalidate the affected view, and require reload before an explicit user
retry. Neither transport automatically replays a mutation. Preserve error
redaction; do not expose raw plugin errors or sensitive settings.

## Deferred decisions

Read receipts require a reviewed authoritative ghost-mode setting and policy.
Draft synchronization requires a reviewed local revision/clear design. These
operations remain proposals. No draft migration, timestamp-as-revision shortcut,
or new plugin method is implicitly approved by this ADR.

## Verification

Use isolated fake-plugin backend round trips and adapter/UI checks for the four
actions. Cover foreign account/chat/topic targets, absent capabilities and gated
parameters, denied calls preserving the DB, exact edit confirmation, duplicate
echo convergence, ambiguous outcomes and no automatic replay. Use synthetic data.
Run Rust/Tauri builds and the required suites in GitHub Actions. A quiet local
fallback may use one job, CPUQuota=40% and MemoryMax=4G without changing power mode.
