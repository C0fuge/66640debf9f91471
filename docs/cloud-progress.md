# Build progress

Development runs locally. GitHub Actions runs builds/tests only, with no API key
or provider login. All test accounts and messages are synthetic.

## Verified first night batch

Run45485427483 passed UI20, SDK41, fake8, Telegram28 and the Rust workspace,
no-default core, Clippy, fake integrations/conformance and debug builds. It
verifies the23-handler Telegram source snapshot, not full platform parity.

## Next candidate, awaiting verification

- Four typed app actions: edit/delete/pin/unpin, scoped validation, ingest effects,
  uncertain-outcome projection, UI dialogs and browser fixtures.
- Correct Draft snapshot content/freshness comparison, spec erratum and regressions.
- Partial MAX foundation: private state/auth/events/text and durable send journal.
  Required history continuation is intentionally unsupported pending verified
  server boundaries; real accounts/server behavior remain unverified.
- Full platform source audit and proposals in docs/platform-coverage.md and
  docs/api-contract-gaps.md.

The next workflow also runs headless account/action UI checks with screenshots.
A published candidate is not a claim that its tests have passed; inspect Actions.
