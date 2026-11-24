#!/usr/bin/env python3
"""Run Phase 2 gates on a committed integration checkout, never on live data."""
from __future__ import annotations

import argparse
import ast
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import uuid

HERE = Path(__file__).resolve().parent
PROFILE = Path("/sys/firmware/acpi/platform_profile")
DEFERRED = 75


def run(command: list[str], **kwargs) -> subprocess.CompletedProcess:
    print("+ " + shlex.join(map(str, command)), flush=True)
    return subprocess.run(command, check=True, **kwargs)


def run_logged(stage: str, command: list[str], log: Path) -> None:
    print(f"== {stage}; log: {log}", flush=True)
    with log.open("w") as stream:
        stream.write("+ " + shlex.join(command) + "\n")
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, errors="replace") as process:
            assert process.stdout is not None
            for line in process.stdout:
                print(line, end="", flush=True)
                stream.write(line)
                stream.flush()
            status = process.wait()
    if status:
        print(f"FAILED {stage}; retained log: {log}", file=sys.stderr)
        raise subprocess.CalledProcessError(status, command)


def revision(source: Path) -> str:
    # A revision and checkout are explicit inputs; this tool never checks out or merges.
    return subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True
    ).strip()


def power_gate(override: bool, profile: Path = PROFILE) -> bool:
    try:
        value = profile.read_text().strip()
    except FileNotFoundError:
        print("Power profile interface absent; no low-power setting detected.")
        return True
    except OSError as exc:
        action = "Explicit override requested." if override else "Heavy stage deferred."
        print(f"Cannot read power profile: {exc}. {action}", file=sys.stderr)
        return override
    print(f"Power profile: {value}")
    if value in {"low-power", "power-saver", "quiet"} and not override:
        print(
            "Heavy verification deferred: intentional low-power profile is active. "
            "Use --source-only or --dry-run now; rerun after the power gate lifts. "
            "--allow-low-power is an explicit caller override and never changes the profile.",
            file=sys.stderr,
        )
        return False
    if override:
        print("Explicit --allow-low-power override requested; profile is unchanged.")
    return True


def export(source: Path, dest: Path) -> None:
    for diff in ([], ["--cached"]):
        run(["git", "-C", str(source), "diff", "--exit-code", "--no-ext-diff", "--no-textconv",
             *diff, "HEAD", "--"],
            stdout=subprocess.DEVNULL)
    archive = dest.parent / "source.tar"
    with archive.open("wb") as stream:
        run(["git", "-C", str(source), "archive", "--format=tar", "HEAD"], stdout=stream)
    dest.mkdir()
    with tarfile.open(archive) as stream:
        stream.extractall(dest, filter="data")
    archive.unlink()


def source_checks(source: Path) -> None:
    counts = {"Python": 0, "JSON": 0, "TOML": 0}
    rendered = 0
    for path in sorted(source.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix == ".py":
            contents = path.read_text()
            if "templates" in path.relative_to(source).parts and "{{" in contents:
                for key, value in {"plugin_id": "verify_plugin", "display_name": "Verification",
                                   "author": "Test Author", "module_name": "verify_plugin",
                                   "crate_name": "verify-plugin"}.items():
                    contents = contents.replace("{{" + key + "}}", value)
                rendered += 1
            ast.parse(contents, filename=str(path))
            counts["Python"] += 1
        elif path.suffix == ".json":
            json.loads(path.read_text())
            counts["JSON"] += 1
        elif path.suffix == ".toml":
            tomllib.loads(path.read_text())
            counts["TOML"] += 1
    run(["bash", "-n", str(source / "templates/test_templates.sh")])
    print(f"Source syntax checks passed: {counts}; {rendered} Python templates rendered "
          "with synthetic values. No builds or test suites ran.")


def integration_checks(source: Path) -> None:
    core = tomllib.loads((source / "crates/ch-core/Cargo.toml").read_text())
    if "keyring" not in core.get("features", {}).get("default", []):
        raise ValueError("integrated source must include the default keyring feature")
    if not (source / "crates/ch-cli/src/scaffold.rs").is_file():
        raise ValueError("integrated source must include ch plugin new/test, not the CLI stub")


def container_command(engine: str, image: str, name: str, source: Path,
                      cpus: int, memory: int, disk: int) -> list[str]:
    return [
        engine, "run", "--rm", "--name", name, "--read-only",
        "--cap-drop=ALL", "--security-opt=no-new-privileges",
        f"--cpus={cpus}", f"--memory={memory}g", f"--memory-swap={memory}g",
        "--pids-limit=512", "--tmpfs", f"/work:rw,size={disk}g,mode=1777",
        "--tmpfs", "/tmp:rw,size=64m,mode=1777",
        "--mount", f"type=bind,src={source},dst=/input,ro=true",
        "--mount", f"type=bind,src={HERE},dst=/verification,ro=true",
        "--env", f"CARGO_BUILD_JOBS={cpus}",
        "--env", f"RUST_TEST_THREADS={cpus}", image,
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True,
                        help="explicit committed integration checkout; no auto-merges")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--dry-run", action="store_true", help="show plan; start no containers")
    modes.add_argument("--source-only", action="store_true", help="host syntax checks only")
    parser.add_argument("--engine", choices=("podman", "docker"), default="podman")
    parser.add_argument("--allow-low-power", action="store_true")
    parser.add_argument("--app-ready", action="store_true",
                        help="explicit caller acknowledgement that runnable app sources are ready")
    parser.add_argument("--cpus", type=int, default=2)
    parser.add_argument("--memory-gib", type=int, default=8)
    parser.add_argument("--disk-gib", type=int, default=8,
                        help="aggregate /work tmpfs cap, including targets/cache/temp copies")
    args = parser.parse_args()
    if min(args.cpus, args.memory_gib, args.disk_gib) < 1:
        parser.error("resource limits must be positive integers")
    source = args.source.resolve(strict=True)
    print(f"Integration commit: {revision(source)}", flush=True)
    name = "ch-phase2-" + uuid.uuid4().hex[:12]
    image = "localhost/" + name + ":verification"
    build = [args.engine, "build", "--tag", image, "--file", str(HERE / "Containerfile"), str(HERE)]
    if args.dry_run:
        print("Dry run: git archive HEAD exports committed tracked files only.")
        print("A full run requires --app-ready and checks the host power profile BEFORE exporting/building.")
        print("+ " + shlex.join(build))
        print("+ " + shlex.join(container_command(args.engine, image, name, Path("/TASK-TEMP/source"),
                                                 args.cpus, args.memory_gib, args.disk_gib)))
        run([sys.executable, str(HERE / "suite.py"), "--plan"])
        return 0
    if not args.source_only:
        if not args.app_ready:
            print("Heavy verification deferred: app readiness has not been acknowledged. "
                  "Use --app-ready only once runnable app sources are ready; "
                  "--source-only and --dry-run remain available.", file=sys.stderr)
            return DEFERRED
        if not power_gate(args.allow_low_power):
            return DEFERRED
    if not args.source_only and shutil.which(args.engine) is None:
        raise ValueError(f"container engine {args.engine!r} is unavailable")
    # This directory is the only host path removed by this program.
    with tempfile.TemporaryDirectory(prefix="ch-phase2-") as temp:
        snapshot = Path(temp) / "source"
        export(source, snapshot)
        source_checks(snapshot)
        if args.source_only:
            return 0
        integration_checks(snapshot)
        logs = Path(tempfile.mkdtemp(prefix="ch-phase2-logs-"))
        print(f"Logs retained outside the disposable snapshot/container: {logs}", flush=True)
        run_logged("verification image build", build, logs / "build.log")
        if not power_gate(args.allow_low_power):
            return DEFERRED
        run_logged("verification suite", container_command(args.engine, image, name, snapshot,
                                                         args.cpus, args.memory_gib, args.disk_gib),
                   logs / "suite.log")
    print("All Phase 2 verification gates passed.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except subprocess.CalledProcessError as exc:
        print(f"Verification stopped: command exited {exc.returncode}.", file=sys.stderr)
        sys.exit(exc.returncode if exc.returncode > 0 else 128 - exc.returncode)
    except (OSError, ValueError, tarfile.TarError, SyntaxError) as exc:
        print(f"Verification setup/source check failed: {exc}", file=sys.stderr)
        sys.exit(2)
    except KeyboardInterrupt:
        sys.exit(130)
