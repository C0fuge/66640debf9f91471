# Build progress

Development runs locally. GitHub Actions supplies build/test workers only, with
no API key, ChatGPT OAuth or cloud coding job.

## 2026-11-24 night batch

Telegram source now adds nine existing-contract handlers: get/edit/delete/forward,
pin/unpin, read receipt, typing and drafts. It also fixes startup cleanup, exact
send confirmation and history boundaries, and preserves service-event metadata.
The source declares23 core-to-plugin operations;96 remain pending.

The updated batch is awaiting GitHub verification. Do not infer success from
the first bootstrap run: it failed to propagate pipeline errors. The corrected
CI uses pipefail; required public auth schemas were restored after an overly
broad ignore rule excluded them from the initial snapshot.

Next local work: typed application edit/delete/pin/unpin controls (ADR0003),
platform API audit and further Telegram/MAX implementation. No real account
or server-side delivery has been verified in CI.
