#!/usr/bin/env bash
set -euo pipefail
export CARGO_BUILD_JOBS=2 RUST_TEST_THREADS=2 CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring
export UV_PYTHON=3.13 UV_LINK_MODE=copy
export UV_CACHE_DIR="$RUNNER_TEMP/ci-uv-cache"
export CH_DATA_DIR="$RUNNER_TEMP/ci-test-data"
export XDG_DATA_HOME="$RUNNER_TEMP/ci-xdg/data"
export XDG_CONFIG_HOME="$RUNNER_TEMP/ci-xdg/config"
export XDG_CACHE_HOME="$RUNNER_TEMP/ci-xdg/cache"
export XDG_STATE_HOME="$RUNNER_TEMP/ci-xdg/state"
export XDG_RUNTIME_DIR="$RUNNER_TEMP/ci-xdg/runtime"
export DBUS_SESSION_BUS_ADDRESS="unix:path=$RUNNER_TEMP/no-session-bus"
export DBUS_SYSTEM_BUS_ADDRESS="unix:path=$RUNNER_TEMP/no-system-bus"
mkdir -p "$CH_DATA_DIR" "$XDG_RUNTIME_DIR"
chmod 700 "$CH_DATA_DIR" "$XDG_RUNTIME_DIR"
case "${1:-}" in
  quick)
    result=0
    (cd app/ui && npm ci --no-audit --no-fund && npm test && npm run build) || result=1
    for package in sdk/python plugins/*; do
      [ -f "$package/pyproject.toml" ] || continue
      (cd "$package" && uv sync --locked && uv run --no-sync pytest -q) || result=1
    done
    exit "$result"
    ;;
  rust)
    (cd app/ui && npm ci --no-audit --no-fund && npm run build)
    cargo metadata --locked --no-deps --format-version 1 >/dev/null
    cargo build --locked -p ch-plugin-sdk --example echo
    cargo test --locked --workspace
    cargo test --locked -p ch-core --no-default-features
    cargo clippy --locked --workspace --all-targets -- -D warnings
    # The CLI's fake conformance needs the isolated synthetic Python environment.
    (cd plugins/fake && uv sync --locked)
    cargo test --locked -p ch-core --test host_fake --test runtime_fake -- --ignored
    cargo build --locked -p ch-cli -p contact-hub
    target/debug/ch plugin test plugins/fake --fixture-account fake-ci \
      --setting start_state=connected --setting live_interval_ms=0 --setting transfer_chunk_ms=0
    mkdir -p "$RUNNER_TEMP/ci-result"
    tar -czf "$RUNNER_TEMP/ci-result/contact-hub-linux-x86_64.tar.gz" \
      -C target/debug contact-hub ch
    ;;
  *) echo 'Expected quick or rust' >&2; exit 2 ;;
esac
