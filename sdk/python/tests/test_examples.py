"""Every file in schemas/examples/ validates, parses into the SDK types and re-serialises
equivalently (modulo null == absent and written non-nullable defaults), and the
re-serialised form validates too."""

from __future__ import annotations

import json

import pytest

from conftest import SCHEMAS, validate
from contact_hub_sdk import EVENTS, METHODS, PluginError, from_wire, to_wire, types as T

INDEX = json.loads((SCHEMAS / "examples" / "index.json").read_text())


def strip_nulls(v):
    if isinstance(v, dict):
        return {k: strip_nulls(x) for k, x in v.items() if x is not None}
    if isinstance(v, list):
        return [strip_nulls(x) for x in v]
    return v


def same_modulo_defaults(orig, back, path=""):
    if isinstance(orig, dict) and isinstance(back, dict):
        for k, ov in orig.items():
            assert k in back, f"{path}/{k} was dropped"
            same_modulo_defaults(ov, back[k], f"{path}/{k}")
        for k, bv in back.items():
            if k not in orig:
                assert bv is False or bv == 0 and bv is not True, f"{path}/{k} = {bv!r} appeared"
    elif isinstance(orig, list) and isinstance(back, list):
        assert len(orig) == len(back), path
        for i, (o, b) in enumerate(zip(orig, back)):
            same_modulo_defaults(o, b, f"{path}/{i}")
    else:
        assert orig == back and type(orig) is type(back), f"{path}: {orig!r} != {back!r}"


def params_type(method: str):
    if method in METHODS:
        return METHODS[method].params
    if method in EVENTS:
        return EVENTS[method]
    assert method == "$/cancelRequest"
    return T.CancelParams


def roundtrip(entry: dict, doc: dict) -> dict:
    kind = entry["type"]
    if "<" not in kind:  # a bare DTO
        cls = getattr(T, kind)
        return to_wire(from_wire(cls, doc))
    out = dict(doc)
    if "params" in doc:
        out["params"] = to_wire(from_wire(params_type(doc["method"]), doc["params"]))
    if "result" in doc:
        method = entry["result_schema"].split("#/$defs/")[1].removesuffix(".result")
        out["result"] = to_wire(from_wire(METHODS[method].result, doc["result"]))
    if "error" in doc:
        out["error"] = PluginError.from_wire(doc["error"]).to_wire()
    return out


def test_index_covers_every_example():
    files = {p.name for p in (SCHEMAS / "examples").glob("*.json")} - {"index.json"}
    assert files == {e["file"] for e in INDEX}


@pytest.mark.parametrize("entry", INDEX, ids=[e["file"] for e in INDEX])
def test_example(entry):
    doc = json.loads((SCHEMAS / "examples" / entry["file"]).read_text())
    validate(doc, entry["schema"])
    back = roundtrip(entry, doc)
    same_modulo_defaults(strip_nulls(doc), back)
    assert strip_nulls(back) == back, "emitted a null"
    validate(back, entry["schema"])
    for key in ("params", "result"):
        if f"{key}_schema" in entry:
            validate(doc[key], entry[f"{key}_schema"])
            validate(back[key], entry[f"{key}_schema"])
