"""Record the fake plugin's event stream into fake-events.ndjson (a test fixture for ch-core's ingest).

Run from the repo root with the fake plugin's venv:
    plugins/fake/.venv/bin/python crates/ch-core/tests/fixtures/record_fake.py

Every line is {"kind": "event", "method": ..., "params": ...} for a plugin -> core event, or
{"kind": "result", "method": ..., "params": <request params>, "result": ...} for a core -> plugin
request the core would apply locally (messages.fetch_recent, messages.send, ...). The run is
isolated: CH_DATA_DIR and XDG_* point into a temp dir. Deterministic apart from timestamps.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
sys.path.insert(0, str(ROOT / "plugins" / "fake" / "tests"))
sys.path.insert(0, str(ROOT / "plugins" / "fake"))

import conftest  # noqa: E402  (sets CH_DATA_DIR / XDG_* to a temp dir)

A = {"account_id": "fake"}
out: list[dict] = []


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="ch-record-"))
    env = dict(os.environ)
    for var in conftest.ENV_VARS:
        env[var] = str(tmp / var.lower())
    core = conftest.FakeCore(env, tmp)
    try:
        core.initialize()
        core.start(start_state="connected", chats=8, messages_per_chat=12, live_interval_ms=150, seed=7)
        got = core.wait_synced()
        for name, items in got.items():
            for p in items:
                out.append({"kind": "event", "method": name, "params": p})

        def rec_result(method, params, timeout=10.0):
            r = core.result(method, params, timeout)
            out.append({"kind": "result", "method": method, "params": params, "result": r})
            flush_events()
            return r

        def flush_events():
            for e in list(core.events):
                core.events.remove(e)
                out.append({"kind": "event", "method": e["method"], "params": e["params"]})

        # history pages for a few chats (the core ingests these results itself)
        for chat in ("c0", "c1", "c2", "c3"):
            page = rec_result("messages.fetch_recent", {**A, "chat_id": chat, "limit": 5})
            if page.get("next_cursor"):
                rec_result("messages.fetch_recent", {**A, "chat_id": chat, "limit": 5, "cursor": page["next_cursor"]})
        # topics of the forum chat, if any
        forum = next((c for p in got.get("conversations.upsert", []) for c in p["conversations"] if c.get("is_forum")), None)
        if forum:
            rec_result("topics.list", {**A, "chat_id": forum["id"]})
        # core-initiated changes whose effect the core applies itself; echoes follow
        sent = rec_result("messages.send", {**A, "chat_id": "c2", "text": "hello from the core", "client_id": "cid-1"})
        mid = sent["message"]["id"]
        core.drain(0.5); flush_events()
        rec_result("messages.edit", {**A, "chat_id": "c2", "message_id": mid, "text": "hello from the core (edited)"})
        core.drain(0.5); flush_events()
        rec_result("reactions.set", {**A, "chat_id": "c2", "message_id": mid, "reactions": [{"emoji": "❤"}]})
        core.drain(0.5); flush_events()
        rec_result("messages.pin", {**A, "chat_id": "c2", "message_id": mid})
        core.drain(0.5); flush_events()
        rec_result("drafts.save", {**A, "chat_id": "c2", "draft": {"text": "a draft"}})
        core.drain(0.5); flush_events()
        rec_result("read.send_receipt", {**A, "chat_id": "c2", "max_message_id": mid})
        core.drain(0.5); flush_events()
        rec_result("messages.delete", {**A, "chat_id": "c2", "message_ids": [mid], "revoke": True})
        core.drain(0.5); flush_events()
        # live traffic
        core.drain(4.0)
        flush_events()
        core.shutdown()
    finally:
        core.close()
    path = HERE / "fake-events.ndjson"
    with path.open("w") as f:
        for line in out:
            f.write(json.dumps(line, ensure_ascii=False) + "\n")
    kinds = {}
    for l in out:
        kinds[l["method"]] = kinds.get(l["method"], 0) + 1
    print(f"wrote {len(out)} lines to {path}: {kinds}")


if __name__ == "__main__":
    main()
