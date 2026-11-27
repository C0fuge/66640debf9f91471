"""Synthetic real-host local-admission peer. Never reads an upload source."""
import json
import os
from pathlib import Path
import sys


def write(value):
    print(json.dumps(value), flush=True)


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
        }}
    elif method == "messages.fetch_recent" and params.get("chat_id") == "disconnect":
        write({"jsonrpc": "2.0", "method": "status", "params": {
            "account_id": params["account_id"], "state": "disconnected", "reconnecting": False,
        }})
        result = {"messages": []}
    elif method.startswith("messages."):
        Path(os.environ["CH_TEST_MANAGED_WIRE"]).write_text(method)
        write({"jsonrpc": "2.0", "id": frame["id"], "error": {
            "code": -32601, "message": "unexpected raw request", "data": {"code": "METHOD_NOT_FOUND"},
        }})
        continue
    write({"jsonrpc": "2.0", "id": frame["id"], "result": result})
    if method == "account.start":
        write({"jsonrpc": "2.0", "method": "status", "params": {
            "account_id": params["account_id"], "state": "connected", "reconnecting": False,
        }})
    if method == "shutdown":
        break
