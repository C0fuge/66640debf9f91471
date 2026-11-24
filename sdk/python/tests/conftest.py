"""Shared test helpers. Every test runs with CH_DATA_DIR / XDG_* in a temp dir."""

from __future__ import annotations

import json
import os
import tempfile
from functools import cache
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
SCHEMAS = ROOT / "schemas"
BASE = "https://contacthub.dev/schemas/v1/"

_ISOLATED = tempfile.mkdtemp(prefix="ch-sdk-tests-")
for var in ("CH_DATA_DIR", "XDG_DATA_HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME"):
    os.environ[var] = os.path.join(_ISOLATED, var.lower())


@cache
def registry():
    from referencing import Registry, Resource

    resources = []
    for p in SCHEMAS.rglob("*.json"):
        if "examples" in p.parts:
            continue
        doc = json.loads(p.read_text())
        resources.append((doc["$id"], Resource.from_contents(doc)))
    return Registry().with_resources(resources)


def validate(instance, ref: str) -> None:
    """Validate against e.g. ``dto/message.json#/$defs/Message`` (relative to schemas/)."""
    from jsonschema import Draft202012Validator

    v = Draft202012Validator({"$ref": BASE + ref}, registry=registry(), format_checker=Draft202012Validator.FORMAT_CHECKER)
    errors = sorted(v.iter_errors(instance), key=lambda e: list(e.path))
    assert not errors, f"{ref}: " + "; ".join(f"{list(e.path)}: {e.message}" for e in errors[:5])


@pytest.fixture
def isolated_env(tmp_path):
    env = dict(os.environ)
    for var in ("CH_DATA_DIR", "XDG_DATA_HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME"):
        env[var] = str(tmp_path / var.lower())
    return env
