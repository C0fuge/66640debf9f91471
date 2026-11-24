# Messenger API coverage

This audit is a dated feature snapshot, not a promise to follow every future
upstream release. The product scope is the current Telegram/MAX client feature
set requested in `REBUILD_PROMPT.md`, followed by WhatsApp. Chats remain separate
by account and platform.

## Two different inventories

1. The upstream inventory lists every method, update/event and protocol opcode
   exposed by a pinned source release. Protocol plumbing, bot-only operations,
   transport helpers and user-facing features must have separate classifications.
2. The product matrix maps user-visible behavior to the plugin contract, storage,
   CoreClient operation, UI and tests. A library method or declared capability
   alone does not establish completed product support.

The current plugin contract has 121 methods: 119 core-to-plugin operations and
two plugin-to-core secret-store operations. It also has 18 event names. These
counts describe the contract, not platform implementation coverage.

## Audit evidence

Working inventories and primary-source provenance are kept in the sibling notes
directory, `CH-notes/api-audit-2026-11-24/{telegram,max,whatsapp}/`. Each audit must
record its exact package version, source checksum or commit, extraction procedure
and fetch date. Documentation can lag the pinned source and must be labelled
when it does. Historical `CH-notes/api-inventory` is comparison input, not proof
that an old classification remains correct.

For each platform the audit retains the complete mechanical inventory and a
separate feature-family matrix. Every missing mapping stays explicit; an unknown
opcode is not evidence that the client implements the feature its name suggests.

## Completion rules

- Identify the actual personal-account API or protocol. A bot/business API does
  not establish personal-client feature coverage.
- Map each product feature to exact upstream methods and our method, event,
  capability, DTO and parameters. Include inbound updates as well as commands.
- Record implementation, persisted fields, CoreClient access, UI and tests
  independently. Use partial, missing or unverified rather than treating a
  working text-send slice as platform parity.
- Review authorization, pagination boundaries, update ordering, retries,
  idempotency, sparse-field semantics, errors and account isolation.
- Account for admin and premium restrictions, platform differences and features
  whose library wrapper is absent. State the evidence for an exclusion.
- Keep offline contract tests separate from server-side verification. No live
  account or legacy-session access is part of this audit.

## Contract gaps

A missing UI implementation, a missing plugin handler and an unrepresentable
feature are different problems. The last requires a reviewed contract amendment
before feature work fans out. Amendments must include schemas, capabilities,
events/DTOs, generated Rust types, both SDKs and conformance checks; a UI-facing
change also needs the CoreClient contract and adapters. Do not silently encode
essential fields into unrelated parameters or claim support by setting a flag.

The API audit supplies extension candidates with primary-source evidence. It does
not itself modify the frozen API, and no candidate is complete until the full
storage-to-UI behavior is verified.
