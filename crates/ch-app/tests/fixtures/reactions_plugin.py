"""Stdlib-only NDJSON reaction fixture. No messenger/network/credentials/SDK."""
import json
import os
import sys

MODE = sys.argv[1]
HEART = {"emoji": "❤️"}
UP = {"emoji": "👍"}
CUSTOM = {"custom_emoji_id": "9007199254740993"}
PAID = {"paid": True}
selection = {}
pending = []
get_missing = False


def write(value):
    print(json.dumps({"jsonrpc": "2.0", **value}), flush=True)


def trace(frame):
    with open(os.environ["CH_REACTION_TRACE"], "a", encoding="utf-8") as stream:
        stream.write(json.dumps({"method": frame["method"], "params": frame.get("params", {})}) + "\n")


def state(p):
    key = (p["account_id"], p["chat_id"], p["message_id"])
    selected = selection.get(key, [HEART] if MODE == "policy-empty" else [])
    return [{"key": item, "count": 18446744073709551615 if MODE == "precision" else 3, "mine": item in selected}
            for item in (HEART, UP, CUSTOM)] + [{"key": PAID, "count": 5, "mine": False, "my_count": 0}]


def message(p, *, write_reply=False):
    value = {"id": p["message_id"], "chat_id": p["chat_id"], "direction": "in", "sent_at": "2026-11-01T00:00:00.000Z", "reactions": state(p)}
    if p["message_id"] == "topic-message":
        value["topic_id"] = "topic"
    if MODE == "empty":
        value["reactions"] = []
    if MODE == "sparse" or write_reply and MODE == "write-sparse":
        value.pop("reactions")
    if MODE == "read-wrong-chat" or write_reply and MODE == "write-wrong-chat":
        value["chat_id"] = "foreign"
    if write_reply and MODE == "bad-key":
        value["reactions"][0]["key"] = {**HEART, **CUSTOM}
    return value


def complete(frame):
    p = frame["params"]
    if MODE != "mismatch":
        selection[(p["account_id"], p["chat_id"], p["message_id"])] = p["reactions"]
    if MODE == "unknown":
        write({"id": frame["id"], "error": {"code": -32000, "message": "sensitive-fixture-payload", "data": {"code": "NETWORK"}}})
    else:
        write({"id": frame["id"], "result": {} if MODE == "write-missing" else {"message": message(p, write_reply=True)}})


def handle(frame):
    global get_missing
    method, p, key = frame.get("method"), frame.get("params", {}), frame.get("id")
    if method is None or key is None:
        return
    if method == "initialize":
        caps = {"messages.get": True, "reactions.set": {"max": 16}, "reactions.custom": True, "reactions.who": True}
        if MODE.startswith("without-"):
            caps[MODE.removeprefix("without-")] = False
        if MODE == "max-default":
            caps["reactions.set"] = True
        if MODE in ("cap-null", "cap-zero"):
            caps["reactions.set"] = {"max": None if MODE == "cap-null" else 0}
        write({"id": key, "result": {"api_version": "1.0.0", "plugin_version": "0.0.1", "capabilities": caps}})
        return
    if method in ("ping", "account.start", "account.stop", "shutdown"):
        write({"id": key, "result": {}})
        if method == "shutdown":
            raise SystemExit(0)
        return
    if method == "x-reaction_fixture.missing":
        get_missing = True
        write({"id": key, "result": {}})
        return
    if method == "x-reaction_fixture.release":
        for item in list(pending):
            pending.remove(item)
            complete(item)
        write({"id": key, "result": {}})
        return
    trace(frame)
    if method == "messages.get":
        write({"id": key, "result": {} if MODE == "missing" or get_missing else {"message": message(p)}})
    elif method == "reactions.available":
        if MODE == "policy-unavailable":
            write({"id": key, "error": {"code": -32000, "message": "sensitive-policy", "data": {"code": "NETWORK"}}})
            return
        result = {"reactions": [] if MODE == "policy-empty" else [HEART, UP, CUSTOM], "allow_custom": False, "max_per_message": 0 if MODE == "policy-empty" else 16}
        if MODE == "max-default":
            result.pop("max_per_message")
        if MODE == "unrestricted":
            result.pop("reactions")
            result["allow_custom"] = True
        write({"id": key, "result": result})
    elif method == "reactions.set":
        assert p["big"] is False
        assert all(not k.get("paid") for k in p["reactions"])
        if MODE == "hold-set":
            pending.append(frame)
        else:
            complete(frame)
    elif method == "reactions.who":
        cursor = p.get("cursor")
        if MODE == "infinite":
            result = {"reactors": [], "next_cursor": str(int(cursor or "0") + 1)}
        elif cursor is None:
            result = {"reactors": [{"user_id": "peer", "key": p.get("key") or HEART, "name": "<literal>" + "x" * 1000}], "next_cursor": "A"}
        elif cursor == "A":
            result = {"reactors": [], "next_cursor": "B"}
        else:
            result = {"reactors": [{"user_id": "other-peer", "key": p.get("key") or CUSTOM}]}
            if MODE == "cycle":
                result["next_cursor"] = "A"
        write({"id": key, "result": result})
    else:
        raise AssertionError(method)


for line in sys.stdin:
    handle(json.loads(line))
