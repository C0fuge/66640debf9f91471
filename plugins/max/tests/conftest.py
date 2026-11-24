from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from contact_hub_sdk.testing import FakeCore, schema_check

PLUGIN = Path(__file__).resolve().parents[1]
ROOT = PLUGIN.parents[1]


@pytest.fixture
def core_factory(tmp_path):
    cores = []

    def make(scenario="normal", directory=None):
        env = {"PYTHONPATH": os.pathsep.join([str(PLUGIN), str(ROOT / "sdk/python/src")]),
               "CH_MAX_TEST_SCENARIO": scenario}
        core = FakeCore([sys.executable, str(PLUGIN / "tests/offline_plugin.py")],
                        plugin_dir=PLUGIN, tmp=directory or tmp_path, env=env,
                        check=schema_check(ROOT / "schemas"), timeout=5)
        cores.append(core)
        return core

    yield make
    for core in cores:
        core.close()


@pytest.fixture(autouse=True)
def deny_network(monkeypatch):
    import socket

    def denied(*args, **kwargs):
        raise AssertionError("MAX foundation tests must remain offline")

    monkeypatch.setattr(socket.socket, "connect", denied)
    monkeypatch.setattr(socket.socket, "connect_ex", denied)
    monkeypatch.setattr(socket, "create_connection", denied)
