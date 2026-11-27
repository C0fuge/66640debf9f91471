"""Synthetic real-host peer; source reads require an explicit isolated test mode."""
import json
import os
from pathlib import Path
import sys
import hashlib
import sqlite3
import threading
import time

_output = threading.Lock()
_source_mode = os.environ.get("CH_TEST_SOURCE_MODE")


def write(value):
    with _output:
        print(json.dumps(value), flush=True)


def managed_file(frame):
    params = frame["params"]
    wire = Path(os.environ["CH_TEST_MANAGED_WIRE"])
    with sqlite3.connect(wire.parent / "hub.db") as db:
        exposure = db.execute("SELECT d.state,p.native_pid,d.source_release,d.nonce "
                              "FROM managed_dispatch d JOIN managed_producer p ON p.id=d.producer_id "
                              "ORDER BY d.id DESC LIMIT 1").fetchone()
    with open(params["path"], "rb") as stream:
        content = stream.read()
        wire.write_text(json.dumps({"request": frame, "exposure": exposure, "pid": os.getpid(),
                                    "sha256": hashlib.sha256(content).hexdigest()}))
        if _source_mode == "exit-after-read":
            os._exit(17)
        if _source_mode in ("late", "early", "never"):
            while not wire.with_suffix(".release").exists():
                time.sleep(0.01)
    wire.with_suffix(".closed").write_text(str(stream.closed))
    message = {"id": "remote-file", "chat_id": params["chat_id"], "topic_id": params.get("topic_id"),
               "direction": "out", "sent_at": "2023-11-14T22:13:20.000Z", "text": params.get("caption"),
               "media": {"kind": params.get("kind", "document"), "bytes": len(content),
                         "filename": params.get("filename")}}
    if _source_mode == "wrong-topic":
        message["topic_id"] = "other-topic"
    result = {} if _source_mode == "malformed" else {"message": message}
    write({"jsonrpc": "2.0", "id": frame["id"], "result": result})


for line in sys.stdin:
    frame = json.loads(line)
    if "id" not in frame:
        continue
    method = frame["method"]
    params = frame.get("params", {})
    result = {}
    if method == "initialize":
        result = {"api_version": "1.2.0", "capabilities": {
            "send.file": {"max_bytes": 1024}, "send.as_file": True, "send.spoiler": True,
            "send.album": True, "edit": True, "edit.media": True,
            "group.photo": True, "profile.edit": True,
        }}
        if _source_mode == "unknown-release":
            result["capabilities"]["send.file"]["source_release"] = "future"
        elif _source_mode == "malformed-release":
            result["capabilities"]["send.file"]["source_release"] = True
        elif _source_mode and _source_mode not in ("old-peer", "old-bool-peer"):
            result["capabilities"]["send.file"]["source_release"] = "on_success"
        if _source_mode == "old-peer":
            result["api_version"] = "1.1.0"
        elif _source_mode == "old-bool-peer":
            result["api_version"] = "1.0.0"
            result["capabilities"]["send.file"] = True
    elif method == "messages.fetch_recent" and params.get("chat_id") == "disconnect":
        write({"jsonrpc": "2.0", "method": "status", "params": {
            "account_id": params["account_id"], "state": "disconnected", "reconnecting": False,
        }})
        result = {"messages": []}
    elif method == "messages.send_file" and _source_mode:
        threading.Thread(target=managed_file, args=(frame,), daemon=True).start()
        continue
    elif method.startswith("messages.") or method.startswith("x-fixture.") or method in (
        "group.edit_photo", "profile.update",
    ):
        Path(os.environ["CH_TEST_MANAGED_WIRE"]).write_text(method)
        write({"jsonrpc": "2.0", "id": frame["id"], "error": {
            "code": -32601, "message": "unexpected raw request", "data": {"code": "METHOD_NOT_FOUND"},
        }})
        continue
    write({"jsonrpc": "2.0", "id": frame["id"], "result": result})
    if method == "account.start":
        if marker := os.environ.get("CH_TEST_LIFETIME_START_MARKER"):
            Path(marker).write_text("started")
        write({"jsonrpc": "2.0", "method": "status", "params": {
            "account_id": params["account_id"], "state": "connected", "reconnecting": False,
        }})
    if method == "shutdown":
        break
