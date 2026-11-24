"""Test harness: ``contact_hub_sdk.testing.FakeCore`` plays the core against the fake
plugin as a child process over stdio.

Every frame the plugin writes is checked against ``schemas/`` (NDJSON JSON-RPC, events,
results, errors), and so are the requests the harness sends. Every run is isolated from
real data (the helper points CH_DATA_DIR / XDG_* into the test's temp dir).
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from functools import cache
from pathlib import Path

import pytest

from contact_hub_sdk.testing import ISOLATED_ENV_VARS
from contact_hub_sdk.testing import FakeCore as SdkFakeCore
from contact_hub_sdk.testing import schema_check

PLUGIN_DIR = Path(__file__).resolve().parents[1]
ROOT = PLUGIN_DIR.parents[1]
SCHEMAS = ROOT / "schemas"

_ISOLATED = tempfile.mkdtemp(prefix="ch-fake-tests-")
for var in ISOLATED_ENV_VARS:
    os.environ[var] = os.path.join(_ISOLATED, var.lower())

validate = schema_check(SCHEMAS)


@cache
def method_caps() -> dict[str, str]:
    reg = json.loads((SCHEMAS / "methods/registry.json").read_text())
    return {m["name"]: m["capability"] for m in reg["methods"] if m["direction"] == "core_to_plugin"}


class FakeCore(SdkFakeCore):
    """The SDK's fake core, running ``python -m fake_plugin``, plus a sync helper."""

    def __init__(self, env: dict[str, str], tmp: Path):
        super().__init__([sys.executable, "-m", "fake_plugin"], plugin_dir=PLUGIN_DIR, tmp=tmp, env=env,
                         check=validate)

    def wait_synced(self, account_id: str = "fake", timeout=20.0) -> dict[str, list]:
        """Wait for the initial sync to finish; return its events by name."""
        self.event("status", account_id=account_id, state="connected", timeout=timeout)
        got: dict[str, list] = {}
        n_chats = None
        deadline = time.monotonic() + timeout
        while True:
            with self._cond:
                for e in list(self.events):
                    if e["params"]["account_id"] != account_id:
                        continue
                    self.events.remove(e)
                    got.setdefault(e["method"], []).append(e["params"])
                    if e["method"] == "conversations.synced":
                        n_chats = len(e["params"]["chat_ids"])
            last = sum(len(p["messages"]) for p in got.get("messages.upsert", []))
            if n_chats is not None and last >= n_chats and "folders.synced" in got:
                return got
            assert time.monotonic() < deadline, f"sync of {account_id} did not finish: {sorted(got)}"
            self.drain(0.05)


@pytest.fixture
def isolated_env(tmp_path):
    env = dict(os.environ)
    for var in ISOLATED_ENV_VARS:
        env[var] = str(tmp_path / var.lower())
    return env


@pytest.fixture
def core(isolated_env, tmp_path):
    c = FakeCore(isolated_env, tmp_path)
    yield c
    c.close()
    sys.stderr.write(c.stderr_text[-3000:])  # shown by pytest on failure
