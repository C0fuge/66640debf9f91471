# Contact Hub desktop and browser demo

Build and run these commands from the rebuild repository once the application source is ready. The native shell embeds the UI bundle. The browser demo serves it through Vite. Both use the Rust dispatcher in `ch-app`.

## Desktop

Linux dependencies include Rust 1.99+, Node 22, npm, pkg-config, GTK 3, WebKitGTK 4.1, librsvg, Ayatana AppIndicator and the D-Bus development library. Python 3.12+ runs the bundled fake plugin using the SDK source tree. The verification image recipe in `tools/app/Containerfile` includes these tools plus Chromium and Xvfb.

```sh
npm --prefix app/ui ci
npm --prefix app/ui run build
cargo build --locked -p contact-hub
./target/debug/contact-hub --demo "$PWD"
```

The explicitly requested demo creates a fresh temporary data directory, loads only the fake plugin and gets its connected demo account through Accounts. The plugin uses the system Python interpreter with the repository SDK on its process-local import path. Its session secrets stay in memory. It requires no plugin installation, desktop keyring, live login or network. A regular shutdown releases the fake process and temporary directory.

Starting the desktop binary without arguments opens the production XDG `contact-hub-ng` directory (or `CH_DATA_DIR`) and the system Secret Service. Install production plugins under its `plugins` directory or under the packaged resource directory's `plugins`. Each running process holds an exclusive OS file lock in the selected data directory for its lifetime.

## Browser

Run the Vite server on the exact allowed origin and the development server in a separate terminal.

```sh
npm --prefix app/ui ci
npm --prefix app/ui run dev -- --host 127.0.0.1 --port 5173 --strictPort
```

```sh
demo_dir=$(mktemp -d)
cargo run --locked -p ch-app --bin ch-devserver -- --demo "$PWD" --token-file "$demo_dir/token"
```

Open `http://127.0.0.1:5173/#token=<contents of token file>` in the browser. The private file must be a new path. Each launch generates a new token. Keep the file within the temporary directory and remove it when finished. Press Ctrl+C to shut down the service. For automated browser verification, read the token file directly into the browser navigation URL and omit that URL from logs and traces.

## Verification container

From the repository root, build the task image when the readiness gate is open.

```sh
podman build -t localhost/contact-hub-app-check -f tools/app/Containerfile tools/app
```

Use a disposable workspace copy, fresh CH_DATA_DIR and XDG directories. The image includes Chromium at `/usr/bin/chromium`. Run the Vite server, Rust devserver and browser in the same network namespace so that the loopback policy stays intact. Run the native demo in Xvfb. Production startup requires a real Secret Service. Fake verification uses the injected memory store and needs no host D-Bus mount.

For the Rust gates, build the frontend bundle first so the Tauri context can embed it, then run the workspace tests and existing verification suite with their documented isolated environments. `cargo test --locked -p ch-app` exercises the shared dispatcher against the real Python fake plugin and checks the development handshake scope.
