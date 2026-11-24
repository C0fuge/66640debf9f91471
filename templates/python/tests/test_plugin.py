"""Run the plugin as a child process over stdio with the SDK's FakeCore playing the core:
initialize -> account.start -> messages.fetch_recent -> messages.send -> shutdown."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from contact_hub_sdk import API_VERSION
from contact_hub_sdk.testing import FakeCore

PLUGIN_DIR = Path(__file__).resolve().parents[1]


@pytest.fixture
def core(tmp_path):
    # CH_DATA_DIR and XDG_* point into tmp_path: a test never touches a real profile.
    with FakeCore([sys.executable, "-m", "{{module_name}}"], plugin_dir=PLUGIN_DIR, tmp=tmp_path) as c:
        yield c


def test_session(core):
    res = core.initialize()
    assert res["api_version"].split(".")[0] == API_VERSION.split(".")[0]
    assert res["plugin_version"] == json.loads((PLUGIN_DIR / "plugin.json").read_text())["version"]
    assert core.call("ping") == {}

    acc = "{{plugin_id}}#1"
    core.start(acc)  # settings get plugin.json's defaults, as from the real core
    assert core.event("status", account_id=acc, state="connected")

    res = core.call("messages.fetch_recent", {"account_id": acc, "chat_id": "c1", "limit": 20})
    assert isinstance(res["messages"], list)
    res = core.call("messages.send", {"account_id": acc, "chat_id": "c1", "text": "hello"})
    assert res["message"]["chat_id"] == "c1" and res["message"]["text"] == "hello"
    assert res["message"]["direction"] == "out"

    assert core.shutdown() == 0
