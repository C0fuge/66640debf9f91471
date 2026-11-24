#!/usr/bin/env python3
"""Lightweight runner checks. No images, Cargo, dependencies, or real test data."""
from __future__ import annotations

import contextlib
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
import run as runner


class RunnerChecks(unittest.TestCase):
    def test_low_power_requires_explicit_override(self):
        with tempfile.TemporaryDirectory(prefix="ch-verification-self-check-") as temp:
            profile = Path(temp) / "profile"
            profile.write_text("low-power\n")
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertFalse(runner.power_gate(False, profile))
                self.assertTrue(runner.power_gate(True, profile))
            self.assertEqual(profile.read_text(), "low-power\n")

    def test_fake_engine_failure_is_returned_and_logged(self):
        # Run the real entry point against a synthetic committed checkout and a
        # fake engine. The explicit override can never start an actual container.
        with tempfile.TemporaryDirectory(prefix="ch-verification-self-check-") as temp:
            root = Path(temp)
            git_env = {**os.environ, "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"}
            source = root / "integration"
            source.mkdir()
            files = {
                "Cargo.toml": "[workspace]\nmembers = []\n",
                "crates/ch-core/Cargo.toml": '[features]\ndefault = ["keyring"]\nkeyring = []\n',
                "crates/ch-cli/src/scaffold.rs": "// synthetic fixture\n",
                "templates/test_templates.sh": "#!/bin/bash\nexit 0\n",
            }
            for name, contents in files.items():
                path = source / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(contents)
            for command in (["git", "init", "-q", str(source)],
                            ["git", "-C", str(source), "add", "."],
                            ["git", "-C", str(source), "-c", "user.name=Test Author",
                             "-c", "user.email=test@example.invalid", "commit", "-qm", "fixture"]):
                subprocess.run(command, env=git_env, check=True)
            binary_dir = root / "bin"
            binary_dir.mkdir()
            engine = binary_dir / "podman"
            calls = root / "calls"
            engine.write_text(
                f"#!{sys.executable}\nimport sys\nfrom pathlib import Path\n"
                f"with Path({str(calls)!r}).open('a') as stream: stream.write(sys.argv[1] + '\\n')\n"
                "print('synthetic engine failure', flush=True)\nsys.exit(23)\n"
            )
            engine.chmod(0o700)
            env = {**os.environ, "PATH": str(binary_dir) + os.pathsep + os.environ["PATH"],
                   "TMPDIR": str(root), "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"}
            command = [sys.executable, str(runner.HERE / "run.py"), "--source", str(source),
                       "--allow-low-power"]
            deferred = subprocess.run(command, env=env, text=True, stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT)
            self.assertEqual(deferred.returncode, runner.DEFERRED, deferred.stdout)
            self.assertIn("app readiness", deferred.stdout)
            self.assertFalse(calls.exists())
            result = subprocess.run([*command, "--app-ready"],
                                    env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            self.assertEqual(result.returncode, 23, result.stdout)
            self.assertEqual(calls.read_text(), "build\n")
            logs = list(root.glob("ch-phase2-logs-*/build.log"))
            self.assertEqual(len(logs), 1)
            self.assertIn("synthetic engine failure", logs[0].read_text())
            self.assertIn("FAILED verification image build", result.stdout)
            self.assertFalse(list(root.glob("ch-phase2-logs-*/suite.log")))
            self.assertFalse(list(root.glob("ch-phase2-*/source")))


if __name__ == "__main__":
    unittest.main()
