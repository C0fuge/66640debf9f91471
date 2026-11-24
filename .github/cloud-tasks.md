# Cloud task queue

Work in order. Progress persists on cloud/progress; update docs/cloud-progress.md.
A task may span runs. Do not spend a run only rereading finished work.

1. Verify the imported Telegram foundation and finish lifecycle/error/pagination
   review. Its existing 18 isolated tests passed locally, but the cloud result
   must be checked independently. Never sign into a real account.
2. Finish the API audit summaries in docs/api-audit for Telegram, MAX and WhatsApp.
   Retain full method/update/opcode inventories. Distinguish pinned library API,
   official platform API, current plugin handlers, core/UI and tested behavior.
   Telegram pin1.46.0/layer233; PyMax/maxapi-python2.5.0; whatsapp-rust0.7.0.
   Download primary sources into temporary directories, not the published tree.
3. Write docs/api-contract-gaps.md: consolidate required user-facing features
   unrepresentable by the frozen contract, with proposed methods/DTOs/events,
   exact primary sources and migration/testing impact. Do not amend contracts.
4. Implement Telegram message actions supported by the existing registry:
   messages.get/edit/delete/forward/pin/unpin, read receipts, typing and drafts.
   Do one coherent group per run, declare only supported capabilities, pair with
   injected-client and schema-checked wire tests. Preserve sparse update fields.
5. Implement Telegram media upload/download and albums with cancellation, bounded
   progress and scoped paths; preserve the existing protocol semantics.
6. Add corresponding UI actions only when representable by frozen CoreClient.
   Otherwise propose the bridge extension and continue another independent task.
7. Start an isolated MAX foundation on PyMax2.5.0: settings/auth/reconnect, chats,
   text/history and offline fixtures. Import compatibility must use synthetic
   databases and preserve original sources. No legacy files exist in the cloud.
8. Continue current-contract Telegram/MAX features in product order, with honest
   capability matrices: reactions/polls, folders/topics, members/privacy/sessions,
   stickers/GIFs, media UI. Contract blockers remain visible for review.
9. WhatsApp, product polish and verified legacy import remain later work.

The complete desired feature inventory is docs/REBUILD_PROMPT.md sections4–7.
Use its current feature snapshot, not a forever-growing upstream parity target.
