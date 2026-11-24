"""Synthetic NDJSON plugin for application-action ownership and failure tests.

No SDK, platform network, credentials, real sessions or external state.
"""
import json
import os
import sys

mode = sys.argv[1]
caps = {key: True for key in ("edit", "delete", "delete.for_me", "pin", "unpin", "send.entities")}
if mode.startswith("without-"):
    caps[mode.removeprefix("without-")] = False
sequence = 0


def write(frame):
    print(json.dumps({"jsonrpc": "2.0", **frame}), flush=True)


def echo(method, params):
    global sequence
    sequence += 1
    key = f"event-{sequence}"
    write({"id": key, "method": method, "params": params})
    for raw in sys.stdin:
        frame = json.loads(raw)
        if frame.get("id") == key and "method" not in frame:
            assert "error" not in frame, frame
            with open(os.environ["CH_ACTION_TRACE"], "a", encoding="utf-8") as trace:
                trace.write(json.dumps({"echo": sequence}) + "\n")
            return
        handle(frame)


def handle(frame):
    method, params, key = frame.get("method"), frame.get("params", {}), frame.get("id")
    if method is None or key is None:
        return
    if method == "initialize":
        write({"id": key, "result": {"api_version": "1.0.0", "plugin_version": "0.0.1", "capabilities": caps}})
        return
    if method in ("ping", "account.start", "account.stop", "shutdown"):
        write({"id": key, "result": {}})
        return
    with open(os.environ["CH_ACTION_TRACE"], "a", encoding="utf-8") as trace:
        trace.write(json.dumps({"method": method, "params": params}) + "\n")
    if mode in ("denied", "unknown"):
        write({"id": key, "error": {"code": -32000, "message": "sensitive fixture payload", "data": {
            "code": "FORBIDDEN" if mode == "denied" else "NETWORK", "reason": "sensitive fixture payload"}}})
        return
    if method == "messages.edit":
        message = {"id": params["message_id"], "chat_id": params["chat_id"], "direction": "out",
                   "sent_at": "2026-11-01T00:00:00.000Z", "text": params["text"], "entities": params["entities"],
                   "edited": True, "edited_at": "2026-11-02T00:00:00.000Z"}
        if params["message_id"] == "topic-message":
            message["topic_id"] = "closed"
        result = {"message": message}
        event = ("message.edited", {"account_id": params["account_id"], "message": message})
        if mode == "missing":
            result = {}
        elif mode.startswith("wrong-"):
            message[{"wrong-id": "id", "wrong-chat": "chat_id", "wrong-topic": "topic_id"}[mode]] = "foreign"
    elif method == "messages.delete":
        result = {}
        event = ("messages.deleted", {"account_id": params["account_id"], "chat_id": params["chat_id"], "message_ids": params["message_ids"]})
    elif method in ("messages.pin", "messages.unpin"):
        result = {}
        message = {"id": params["message_id"], "chat_id": params["chat_id"], "direction": "out",
                   "sent_at": "2026-11-01T00:00:00.000Z", "is_pinned": method == "messages.pin"}
        if params["message_id"] == "topic-message":
            message["topic_id"] = "closed"
        event = ("messages.upsert", {"account_id": params["account_id"], "messages": [message]})
    else:
        raise AssertionError(method)
    if mode == "before":
        echo(*event)
    write({"id": key, "result": result})
    if mode == "after":
        echo(*event)


for line in sys.stdin:
    handle(json.loads(line))
