# ADR 0011: observed whole-chat archive controls

Implementation verified on 2026-11-26: exact public source `7e5dc01` passed
[Verify45852695182](https://github.com/C0fuge/66640debf9f91471/actions/runs/45852695182),
including App and plugin lifecycle/ownership tests, native transports, eight
Chromium scenarios and inspected desktop/mobile screenshots. The app remains at
36 operations. The original acceptance and release requirements follow.

Status: accepted for implementation by root, 2026-11-26, after reviewing the
Telegram folder API and the pinned Telethon 1.46.0 archive path. The owner's
continued Telegram/MAX build authorizes this existing client feature. Verification
and independent review must pass before merging. No database migration, new app
operation, plugin method or SDK/schema version change.

## Semantics and scope

[Telegram peer folders](https://core.telegram.org/api/folders#peer-folders) assign
archive to folder 1 and the ordinary inbox to folder 0. These are distinct from
custom dialog filters. The pinned custom Dialog.archived tests non-null folder_id
and would misclassify an explicit zero. Use the validated raw folder value in
targeted reads and full synchronization: absent/0 is false, 1 is true; other or
malformed values cannot establish an ordinary archive observation.

The plugin's existing chats.set_flags gains its declared chat.archive capability.
One folders.editPeerFolders request selects one resolved account-scoped peer and
folder 1/0. Its Updates response is not exact-dialog confirmation: read back that
peer once. Archive can affect pin state; publish only observed archived/pinned
and other explicitly requested flags. Preserve ambiguous/partial error outcomes,
no replay or compensation. Invalid folder observations cannot publish a complete
inventory. Other-device folder updates remain reconciled by explicit targeted
read or synchronization; do not fabricate a guessed kind or fan out network reads.

## Matched application amendment

Extend ADR0010's ChatFlag with archived and its observation result with a required
nullable archived field. Existing plugins may omit the DTO field; return null and
grant no authority for it. Unknown archive state never becomes false merely from
the list's default inbox placement. The existing database archive boolean and
ordinary list projection remain unchanged.

refreshChatFlags commits only the observed archive value through the same guarded
existing-owner/row transaction. Its short-lived one-use proof binds archived just
as it binds pinned/markedUnread. setChatFlag sends exactly one boolean archived,
requiring chat.get, chat.flags and chat.archive from the same live handshake and
account run. False has the same gates as true. Preserve all task/context bounds,
one-use consumption, safe uncertainty and both transports' no-replay behavior.

The panel labels the observed field Archived, with Archive chat and Move to inbox
commands. A new panel requires explicit refresh. Never optimistically move a row
or infer success from an Empty result. A committed observation/event can move the
row out of the active tab and unmount its panel. The field's pending/unknown ledger
survives that unmount, filtering and account switches; only a matching fresh
observation can reconcile it. Capabilities disable this action independently.

## Verification

Exercise exact 1/0 peer requests, absent/zero/one/malformed folders, full sync and
targeted consistency, mixed pin/mark/archive partial failures and pin side effects.
App tests cover true/false, sparse/foreign reads, one-use/old-run rejection,
capability gates, no local fabrication from Empty, unrelated-field preservation
and moving between tabs. Chromium must archive/unarchive actual virtual rows from
committed fixtures, lose a setter response after a move, reconcile in the other
tab, reject duplicate clicks/late account replies, and verify mobile geometry.
All data is synthetic. Actual server/account behavior remains unverified.
