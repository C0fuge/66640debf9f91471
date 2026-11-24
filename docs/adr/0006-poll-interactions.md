# ADR 0006: stored polls, explicit voting and targeted refresh

Status: accepted for implementation, 2026-11-25.

## Decision

Accept one application batch: stored poll display and exactly three typed
CoreOperations: `votePoll({thread,messageId,optionIds})`,
`closePoll({thread,messageId})`, and `refreshPoll({thread,messageId})`.
Each returns `{poll: PollSnapshot | null}`. Add optional `Message.poll` using
the same snapshot. No generic plugin RPC, poll creation UI, new plugin API,
schema, SDK or DB migration is included. Creation needs a separately reviewed
durable outbox design.

PollSnapshot contains nullable `id`, literal `question`, `options`, nullable
`totalVoters`, `closed`, `multiple`, `quiz`, `anonymous`, nullable `closeAt`
(Millis), nullable `solution`, and `solutionEntities` (TextEntity[]). Each option
contains opaque `id`, literal `text`, nullable decimal-string `votes`, nullable
`chosen` and nullable `correct`. Counts remain exact unsigned decimal strings;
unknown is never false/zero. Copy only these fields, never platform extensions.
Suppress deleted/provisional poll details. Keep quiz correctness/solution fields
redacted in Rust unless the quiz is closed or a personal choice is reported true;
even then reveal only values actually reported.

## Ownership and mutation

Reuse existing action locks, account/plugin ownership, exact stored chat/topic/
message checks, handshake capabilities, typed plugin calls and ingest effects.
Validate distinct selected option IDs against the stored poll before dispatch.
Single-choice polls allow at most one choice. Retraction is an explicit action
for known selected normal polls; reject quiz retraction. Restrict this initial
close slice to confirmed outgoing, non-forwarded polls. Server rights and native
revoting restrictions still apply. Closed-topic posting rules do not by
themselves forbid vote/close. Require messages.get for the recovery control.

polls.vote/close legally return OptionalMessageResult. A confirmed matching
Message may be applied via existing apply_edited without inventing selected,
closed or count fields. An absent Message/Poll returns null and leaves the view
stale; it is not a fabricated current snapshot. Validate returned identity,
explicit topic and known poll ID before any local effect. Malformed/foreign
post-dispatch replies or lost transport/apply results are outcomeUnknown. Never
automatically replay vote/close. Read errors remain ordinary read errors.

## Targeted read and UI

refreshPoll calls messages.get for the exact validated target and applies only
confirmed reported fields, with no history cursor/gap/completion changes. Missing
Message/Poll is unknown, not deletion. Cached latest and newest-page refresh
cannot reconcile an older poll. A lost mutation needs a matching scoped read
plus explicit review before another user write; unknown personal choices stay
unknown. An answered/unknown-choice quiz is read-only in this first slice.

Keep tentative choices separate from confirmed choices. Never vote on render,
selection, account switch or reconnect. A mutation success without a snapshot
may trigger one bounded targeted read, never another write. Capture exact target
ownership in the interaction panel and discard stale responses after switches.
Preserve left alignment, per-platform threads and accent-only colouring.

## Verification

Update Rust wire/dispatch/projections, both TS adapters, documentation and real
UI together. Cover null/false/zero and count precision, quiz redaction, inert
text, foreign/duplicate IDs and topics, both optional result forms, event/reply
ordering, older-message refresh without gap mutation, lost replies/no replay,
unknown choices, explicit retraction and stale panels. Use isolated fake data
and meaningful browser tests. Full compilation/tests run in GitHub Actions;
the accepted design does not claim live-server or full platform poll parity.
