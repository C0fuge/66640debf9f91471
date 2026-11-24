# ADR 0002. Snapshot reads and invalidations for the application UI

Accepted for the Phase 3 fake-plugin desktop skeleton.

ADR 0001 places React panes behind one CoreClient. The existing core already owns history gap filling, local echo adoption, outbox reconciliation and account lifecycle. The UI needs current views of these services across process restarts and broadcast lag.

Use the JSON operations and snapshots in `docs/app-api.md` and `app/ui/src/core/index.ts`. Both desktop and development adapters expose the same typed methods. Rust projects service data into UI DTOs and publishes invalidations. Re-reading the affected views recovers from skipped or coalesced events. Auth challenges are ephemeral and the Rust bridge retains the current one while its account is live.

The first slice uses TypeScript declarations and a documented wire contract. Its native dispatcher can live in the desktop shell. A second Rust transport can share the dispatcher when the development server is added. Plugin protocol types and the database schema remain owned by their current crates.
