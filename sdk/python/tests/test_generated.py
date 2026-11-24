"""The generated modules match schemas/ (regenerate with tools/gen_types.py)."""

import subprocess
import sys
from pathlib import Path

TOOLS = Path(__file__).resolve().parents[1] / "tools"


def test_generated_modules_are_current():
    r = subprocess.run([sys.executable, str(TOOLS / "gen_types.py"), "--check"], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
