# Plugin conformance

Build the CLI with `cargo build -p ch-cli`. Run `ch plugin test ./my-plugin` against a prepared manifest entry. Use `--json` for a machine-readable report or `--verbose` for every successful check. A failed check sets the exit status to 1.

The suite starts the plugin in its own directory with fresh temporary HOME, XDG, data, cache and state directories. It answers secret requests in memory. The default checks cover the manifest, transport, lifecycle and observed frames. Plugins execute with the caller's filesystem and network permissions.

Full platform-method probes require an **offline, disposable fake or recording**. Supply its test account id using `--fixture-account`. This enables interactive auth and mutating requests, including sending to a chat announced by the fixture. Use a stationary dataset to compare consecutive sync snapshots and history pages. Use only synthetic settings and auth answers. A typed operational error proves the error shape and leaves feature behavior unverified. SKIP results explain missing observations.

With each example's manifest entry already built, run the following from the checkout.

```sh
ch plugin test plugins/fake --fixture-account fixture-fake
ch plugin test docs/tutorial/example/python --fixture-account fixture-parrot --auth code=1234
ch plugin test docs/tutorial/example/rust --fixture-account fixture-parrot --auth code=1234
```

Generate a scaffold with `ch plugin new demo_chat --lang python` or `--lang rust`. Use `--no-install` to skip dependency setup and tests. Rust scaffolds require a release build for the manifest entry. After setup, run `ch plugin test ./demo_chat --fixture-account fixture-demo`. The same command accepts third-party fixture plugins that implement the published stdio protocol.
