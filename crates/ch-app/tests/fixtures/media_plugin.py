"""Isolated stdlib media fixture: no SDK, platform network, sessions or credentials."""
import json
import os
from pathlib import Path
import sys

mode = sys.argv[1]
accounts = {}
pending = {}
sequence = 0


def write(frame):
    print(json.dumps({"jsonrpc": "2.0", **frame}), flush=True)


def record(value):
    with open(os.environ["CH_MEDIA_TRACE"], "a", encoding="utf-8") as trace:
        trace.write(json.dumps(value) + "\n")


def event(method, params):
    global sequence
    sequence += 1
    key = f"event-{sequence}"
    write({"id": key, "method": method, "params": params})
    for raw in sys.stdin:
        frame = json.loads(raw)
        if frame.get("id") == key and "method" not in frame:
            assert "error" not in frame, frame
            return
        handle(frame)


def finish(frame):
    params, key = frame["params"], frame["id"]
    cache, state = accounts[params["account_id"]]
    path = (state if mode == "state-path" else cache) / (params["message_id"] + ".bin")
    path.write_bytes(b"isolated attachment")
    ready = {"account_id": params["account_id"], "chat_id": params["chat_id"], "message_id": params["message_id"], "path": str(path)}
    if mode == "before":
        event("media.ready", ready)
    write({"id": key, "result": {"path": 42} if mode == "malformed" else {"path": str(path), "bytes": 19}})
    if mode == "after":
        event("media.ready", ready)


def handle(frame):
    method, params, key = frame.get("method"), frame.get("params", {}), frame.get("id")
    if method == "$/cancelRequest":
        record({"cancel": params["id"]})
        if mode == "ignore-cancel":
            return
        request = pending.pop(params["id"], None)
        if request is not None:
            if mode == "completion-wins":
                finish(request)
            else:
                write({"id": request["id"], "error": {"code": -32000, "message": "fixture cancelled", "data": {"code": "CANCELLED"}}})
        return
    if method is None or key is None:
        return
    if method == "initialize":
        write({"id": key, "result": {"api_version": "1.0.0", "plugin_version": "0.0.1", "capabilities": {"media.download": mode != "unsupported"}}})
    elif method == "account.start":
        accounts[params["account_id"]] = (Path(params["cache_dir"]), Path(params["state_dir"]))
        write({"id": key, "result": {}})
    elif method in ("ping", "account.stop", "shutdown"):
        write({"id": key, "result": {}})
    elif method == "x-media_fixture.release":
        for request in list(pending.values()):
            pending.pop(request["id"], None)
            finish(request)
        write({"id": key, "result": {}})
    elif method == "media.download":
        record({"method": method, "params": params, "id": key})
        if mode in ("hold", "ignore-cancel", "completion-wins", "progress-hold", "event-hold"):
            pending[key] = frame
            if mode == "progress-hold":
                base = {"account_id": params["account_id"], "direction": "download", "chat_id": params["chat_id"], "message_id": params["message_id"], "done_bytes": 99}
                event("transfer.progress", {**base, "request_id": key + 999})
                event("transfer.progress", base)
                event("transfer.progress", {**base, "request_id": key, "direction": "upload"})
                event("transfer.progress", {**base, "request_id": key, "chat_id": "foreign"})
                event("transfer.progress", {**base, "request_id": key, "done_bytes": 3, "total_bytes": 9})
                event("transfer.progress", {**base, "request_id": key, "done_bytes": 2, "total_bytes": 1})
                event("transfer.progress", {**base, "request_id": key, "done_bytes": 4, "total_bytes": 1})
            elif mode == "event-hold":
                cache, _ = accounts[params["account_id"]]
                path = cache / (params["message_id"] + ".bin")
                path.write_bytes(b"isolated attachment")
                event("media.ready", {"account_id": params["account_id"], "chat_id": params["chat_id"], "message_id": params["message_id"], "path": str(path)})
        else:
            finish(frame)
    else:
        raise AssertionError(method)


for line in sys.stdin:
    handle(json.loads(line))
