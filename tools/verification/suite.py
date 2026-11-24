#!/usr/bin/env python3
"""Container-only Phase 2 gates; --plan is safe to run on the host."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys

ROOT = Path("/work/source")
TARGET = Path("/work/target")


@dataclass
class Step:
    name: str
    command: list[str]
    directory: Path = ROOT


def steps() -> list[Step]:
    sdk = ROOT / "sdk/python"
    fake = ROOT / "plugins/fake"
    py_parrot = ROOT / "docs/tutorial/example/python"
    rs_parrot = ROOT / "docs/tutorial/example/rust"
    ch = str(TARGET / "debug/ch")
    result = [
        Step("workspace metadata", ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"]),
        # Avoid racing nested on-demand cargo builds in host_echo integration tests.
        Step("host echo prerequisite", ["cargo", "build", "--locked", "-p", "ch-plugin-sdk", "--example", "echo"]),
        Step("workspace tests, including default native keyring", ["cargo", "test", "--locked", "--workspace"]),
        Step("core without default features", ["cargo", "test", "--locked", "-p", "ch-core", "--no-default-features"]),
        Step("workspace clippy", ["cargo", "clippy", "--locked", "--workspace", "--all-targets", "--", "-D", "warnings"]),
        Step("ignored fake host/runtime integrations", ["cargo", "test", "--locked", "-p", "ch-core", "--test", "host_fake", "--test", "runtime_fake", "--", "--ignored"]),
        Step("Python SDK dependencies", ["uv", "sync", "--locked"], sdk),
        Step("Python SDK tests", ["uv", "run", "--no-sync", "pytest", "-q"], sdk),
        Step("fake plugin dependencies", ["uv", "sync", "--locked"], fake),
        Step("fake plugin tests", ["uv", "run", "--no-sync", "pytest", "-q"], fake),
        Step("Python tutorial venv", ["uv", "venv", "--python", sys.executable, ".venv"], py_parrot),
        Step("Python tutorial dependencies", ["uv", "pip", "install", "--python", ".venv/bin/python", "-e", str(sdk), "pytest>=8"], py_parrot),
        Step("Python tutorial tests", [".venv/bin/python", "-m", "pytest", "-q"], py_parrot),
        Step("Rust tutorial tests", ["cargo", "test", "--locked"], rs_parrot),
        Step("Rust tutorial clippy", ["cargo", "clippy", "--locked", "--all-targets", "--", "-D", "warnings"], rs_parrot),
        Step("CLI build", ["cargo", "build", "--locked", "-p", "ch-cli"]),
        Step("fake plugin conformance via actual CLI", [ch, "plugin", "test", str(fake), "--fixture-account", "fake-verification", "--setting", "start_state=connected", "--setting", "live_interval_ms=0", "--setting", "transfer_chunk_ms=0"]),
        Step("Python tutorial conformance via actual CLI", [ch, "plugin", "test", str(py_parrot), "--fixture-account", "parrot-verification", "--secret", "session=verification-only", "--setting", "max_per_minute=100"]),
        Step("Rust tutorial release entry", ["cargo", "build", "--locked", "--release"], rs_parrot),
        Step("stage Rust tutorial manifest entry", ["@stage-entry", str(rs_parrot), "parrot"]),
        Step("Rust tutorial conformance via actual CLI", [ch, "plugin", "test", str(rs_parrot), "--fixture-account", "parrot-verification", "--secret", "session=verification-only", "--setting", "max_per_minute=100"]),
    ]
    for lang in ("python", "rust"):
        ident = "verify_" + lang
        directory = Path("/work/generated") / ident
        result.append(Step(f"generate {lang} template via actual CLI", [
            ch, "plugin", "new", ident, "--lang", lang, "--dir", str(directory),
            "--sdk-path", str(ROOT), "--name", "Verification Plugin", "--author", "Test Author", "--no-install",
        ]))
        if lang == "python":
            result.extend([
                Step("Python template dependencies", ["uv", "sync"], directory),
                Step("Python template SDK tests", ["uv", "run", "--no-sync", "pytest", "-q"], directory),
            ])
        else:
            result.extend([
                Step("Rust template SDK tests", ["cargo", "test"], directory),
                Step("Rust template clippy", ["cargo", "clippy", "--all-targets", "--", "-D", "warnings"], directory),
                Step("Rust template release entry", ["cargo", "build", "--release"], directory),
                Step("stage Rust template manifest entry", ["@stage-entry", str(directory), "verify-rust-plugin"]),
            ])
        result.append(Step(f"{lang} template conformance via actual CLI", [
            ch, "plugin", "test", str(directory), "--fixture-account", "template-verification",
        ]))
        # Same auth-example check as templates/test_templates.sh, applied to the
        # actual CLI-generated scaffold. Its SDK suite was already run exactly once.
        file = directory / ("src/verify_python_plugin/plugin.py" if lang == "python" else "src/main.rs")
        result.append(Step(f"enable {lang} template auth example", ["@enable-auth", str(file), "#" if lang == "python" else "//"]))
        if lang == "python":
            result.append(Step("Python template auth example", ["uv", "run", "--no-sync", "python", "-c", "from verify_python_plugin import PlatformPlugin; assert PlatformPlugin().get_capabilities().get('auth.interactive')"], directory))
        else:
            result.append(Step("Rust template auth example", ["cargo", "clippy", "--", "-D", "warnings"], directory))
    return result


def environment() -> dict[str, str]:
    env = os.environ.copy()
    for key in ("HOME", "CH_DATA_DIR", "XDG_DATA_HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME",
                "XDG_STATE_HOME", "XDG_RUNTIME_DIR", "CARGO_HOME", "UV_CACHE_DIR", "TMPDIR"):
        directory = Path("/work/env") / key.lower()
        directory.mkdir(parents=True, exist_ok=True)
        directory.chmod(0o700)
        env[key] = str(directory)
    env.update({
        "CARGO_TARGET_DIR": str(TARGET), "CARGO_INCREMENTAL": "0",
        "UV_PYTHON_DOWNLOADS": "never", "UV_LINK_MODE": "copy",
        "PYTHONDONTWRITEBYTECODE": "1", "PYTHON_KEYRING_BACKEND": "keyring.backends.null.Keyring",
        "DBUS_SESSION_BUS_ADDRESS": "unix:path=/work/env/no-session-bus",
        "DBUS_SYSTEM_BUS_ADDRESS": "unix:path=/work/env/no-system-bus",
    })
    return env


def execute(step: Step, env: dict[str, str]) -> None:
    command = step.command
    print(f"\n== {step.name}\n({step.directory}) $ {shlex.join(command)}", flush=True)
    if command[0] == "@stage-entry":
        # Manifests use ./target/release, while all Cargo builds share /work/target.
        directory, binary = Path(command[1]), command[2]
        destination = directory / "target/release" / binary
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(TARGET / "release" / binary, destination)
    elif command[0] == "@enable-auth":
        file, prefix = Path(command[1]), command[2]
        active = False
        found = False
        lines = []
        for line in file.read_text().splitlines(keepends=True):
            if "BEGIN auth example" in line:
                active = found = True
            elif "END auth example" in line:
                active = False
            elif active and line.lstrip().startswith(prefix):
                indent = line[:len(line) - len(line.lstrip())]
                body = line.lstrip()[len(prefix):]
                line = indent + (body[1:] if body.startswith(" ") else body)
            lines.append(line)
        if not found or active:
            raise ValueError(f"auth example markers missing or unbalanced: {file}")
        file.write_text("".join(lines))
    else:
        subprocess.run(command, cwd=step.directory, env=env, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", action="store_true")
    args = parser.parse_args()
    if args.plan:
        for step in steps():
            print(f"{step.name}: ({step.directory}) {shlex.join(step.command)}")
        return 0
    if not Path("/input/Cargo.toml").is_file() or ROOT.exists():
        raise ValueError("run through run.py in a new dedicated container")
    shutil.copytree("/input", ROOT)
    env = environment()
    for step in steps():
        try:
            execute(step, env)
        except (subprocess.CalledProcessError, OSError, ValueError):
            print(f"FAILED gate: {step.name}", file=sys.stderr, flush=True)
            raise
    print("\nAll container gates passed.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except subprocess.CalledProcessError as exc:
        print(f"Gate failed; later gates were not run (exit {exc.returncode}).", file=sys.stderr)
        sys.exit(exc.returncode if exc.returncode > 0 else 128 - exc.returncode)
    except (OSError, ValueError) as exc:
        print(f"Gate setup failed: {exc}", file=sys.stderr)
        sys.exit(2)
    except KeyboardInterrupt:
        sys.exit(130)
