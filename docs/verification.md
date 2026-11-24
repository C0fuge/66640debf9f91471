# Phase 2 verification

`tools/verification/run.py` verifies an **explicit, committed integration checkout**.
It never merges task branches. Integrate/review the outbox, history, conformance,
and API changes first, including their Cargo manifest/lockfile changes. The full
runner rejects a checkout missing the default keyring feature or CLI scaffold.
No feature branch is treated as evidence that the integrated build passes.

The host needs Python 3.12+, Git, Bash, and Podman (default) or Docker. Host Cargo,
Python packages, and existing virtual environments are not required or used.

```bash
python3 tools/verification/run.py --source /path/to/integration --dry-run
python3 tools/verification/run.py --source /path/to/integration --source-only
python3 tools/verification/self_check.py

# Full gate, only once runnable app sources are ready and the power gate permits:
python3 tools/verification/run.py --source /path/to/integration --app-ready
# Docker alternative:
python3 tools/verification/run.py --source /path/to/integration --app-ready --engine docker
```

The full invocation requires an explicit `--app-ready` acknowledgement before any
heavy work. A performance power profile alone never grants readiness; there is
no file-existence heuristic or automatic readiness override. Without this flag,
it defers work with exit **75**, even if `--allow-low-power` was supplied.

After readiness is acknowledged, it checks `/sys/firmware/acpi/platform_profile` before building
an image, then again before starting the suite. An active `low-power`,
`power-saver`, or `quiet` profile defers heavy work with exit **75**. An unreadable
profile also defers work; an absent interface is reported and permitted. The
runner never changes the profile. Only an explicitly requested
`--allow-low-power` flag bypasses this gate. Dry-run and source-only are safe
while heavy verification is deferred; neither is a passing Phase 2 test gate.

Source-only exports `git archive HEAD`, checks actual Python/JSON/TOML syntax,
renders synthetic values for Python template syntax, and checks the template
shell script with `bash -n`. It does not install dependencies, resolve Cargo
metadata, build images, compile Rust, or run tests. Tracked edits must be committed;
untracked and ignored files are excluded. This prevents copying a developer's
venv, targets, credentials, or sessions into the verification environment.

## Isolation and resources

Each full run creates a uniquely named image/container, exports committed source
into its own temporary directory, mounts that snapshot read-only, and copies it
into a fresh container `/work`. It does not mount the host home, checkout, old
application, D-Bus socket, keyring, or any real account data. Only the disposable
snapshot and these verification scripts are mounted. `HOME`, `CH_DATA_DIR`, all
`XDG_*` homes/runtime, Cargo and uv caches, and test temp directories live under
`/work`; D-Bus addresses point to nonexistent private sockets. Existing fixtures
use an in-memory SecretStore. Native keyring dependencies are compiled, with no
desktop keyring service or user secrets available to tests.

The container has a read-only root filesystem, no extra capabilities, a PID cap,
two CPUs, 8 GiB memory (no additional swap), and an aggregate 8 GiB `/work` tmpfs
cap. A separate 64 MiB `/tmp` is also capped; test `TMPDIR` is under `/work`.
Use `--cpus`, `--memory-gib`, and `--disk-gib` to request different positive limits.
Tmpfs consumes memory, so exhausting the memory/disk cap is a failure, not a
reason to rerun automatically. Builds disable incremental artifacts and share
only this run's Cargo target/cache. There are no host target/cache volumes.

The recipe pins the inspected official Rust 1.99.0 image by digest, installs
clippy/rustfmt, Python 3.12+ and the native build dependencies (`pkg-config`,
`libdbus-1-dev`, compiler toolchain). uv is copied from its official **0.13.9**
image as documented in the [official uv Docker guide](https://docs.astral.sh/uv/guides/integration/docker/).
The Rust base comes from the [official Rust image](https://hub.docker.com/_/rust).
The runner asserts Rust/uv versions during image build. OS packages, generated
template dependency resolution, and tutorial pytest are not fully locked; the
recipe provides repeatable gates, not a byte-identical offline build. Building
the image and fetching crates/Python dependencies require network access and
remain deferred while the power gate is active.

The container is removed by its own `--rm` lifecycle. Only the runner's own
temporary snapshot is cleaned. Existing containers, images, caches, worktrees,
venvs, and targets are never reused or deleted. The uniquely tagged image and
engine build layers remain in engine storage; their size is **outside** the
runtime tmpfs cap. No automatic image/container prune runs.

## Full gate order

The dry-run prints every command and working directory. Gates stop at the first
failure and do not retry or rerun a suite to conceal failures:

1. Locked workspace metadata and the Rust SDK echo example needed by host tests.
2. `cargo test --locked --workspace`, with default native keyring enabled.
3. `cargo test --locked -p ch-core --no-default-features` as a separate feature
   configuration, followed by workspace/all-target clippy with denied warnings.
4. Only the ignored `host_fake` and `runtime_fake` tests. The normal workspace
   tests skip them, so these integrations execute once.
5. Locked Python SDK/fake dependency sync and their pytest suites.
6. Python tutorial venv plus local editable SDK and pytest; Python tutorial tests.
   Rust tutorial locked tests and all-target clippy.
7. Build the actual `ch` CLI. Run `ch plugin test` against the fake plugin and
   both tutorial manifests, using synthetic offline accounts/secrets/settings.
   The Rust tutorial release binary is copied into its manifest's local
   `target/release` path from the run's shared target directory.
8. Generate both templates through the actual `ch plugin new --no-install`,
   explicitly selecting this snapshot's SDK and synthetic author/name values.
   Run each generated SDK suite once, Rust clippy, prepare manifest executables,
   and run the actual `ch plugin test` on both. Enable their documented auth
   examples and check Python capabilities / Rust clippy afterwards.

`templates/test_templates.sh` is syntax-checked but is not also executed: the
generated-template gates perform its SDK/auth checks using the CLI scaffolder,
avoiding duplicate suite runs. Rust release builds are required by the shipped
manifest entries. All existing locked projects use their lockfiles; generated
projects resolve fresh dependencies inside the disposable container.

## Results and logs

Exit **0** means all requested gates passed (or the explicitly selected dry-run /
source-only completed). Exit **2** reports runner/setup/source syntax errors;
**75** means readiness or power deferral; **130** means interruption. Subprocess failure codes
are preserved, including image build and test failures. Each gate announces its
name before executing; a failed suite reports the exact gate and stops.

The runner prints the integration commit and a fresh `ch-phase2-logs-*` directory
outside its disposable snapshot. `build.log` and `suite.log` stream both stdout
and stderr to the terminal and host files, so failed-stage output survives
container removal without an output mount. These synthetic verification logs
are retained on success or failure; remove only the reported log directory when
it is no longer needed. Readiness/power deferral before image build creates no log folder.

The lightweight self-check uses a synthetic Git checkout and fake container
engine to prove that an image-build failure returns its original exit status,
retains output, skips the suite, and cleans only its own snapshot. It also checks
that readiness is required and low-power requires an explicit override without
changing the profile.
