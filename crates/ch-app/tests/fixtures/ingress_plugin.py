"""Isolated stdlib file admission fixture using only its temporary account and trace."""
import json
import os
from pathlib import Path
import sys

roots = {}
sequence = 0


def write(frame):
    print(json.dumps({"jsonrpc": "2.0", **frame}), flush=True)


for line in sys.stdin:
    frame = json.loads(line)
    method = frame.get("method")
    if method is None:
        continue
    key = frame.get("id")
    p = frame.get("params", {})
    if method == "initialize":
        write({"id": key, "result": {"api_version": "1.0.0", "plugin_version": "0.0.1",
               "capabilities": json.loads(os.environ["CH_FIXTURE_CAPS"])}})
    elif method == "account.start":
        roots[p["account_id"]] = Path(p["state_dir"]).parent / "uploads"
        write({"id": key, "result": {}})
        write({"id": f"status-{key}", "method": "status",
               "params": {"account_id": p["account_id"], "state": "connected"}})
    elif method == "messages.send_file":
        path = Path(p["path"])
        assert path.name == "bytes" and path.parent.parent == roots[p["account_id"]]
        with path.open("rb") as data:
            content = data.read(4097)
        assert len(content) <= 4096
        sequence += 1
        trace = {k: v for k, v in p.items() if k != "path"}
        trace["body"] = content.decode("ascii")
        with open(os.environ["CH_FIXTURE_TRACE"], "a", encoding="utf-8") as out:
            out.write(json.dumps(trace) + "\n")
        write({"id": key, "result": {"message": {"id": f"fixture-{sequence}", "chat_id": p["chat_id"],
               "topic_id": p.get("topic_id"), "direction": "out", "sent_at": "2026-01-01T00:00:00.000Z",
               "text": p.get("caption", "")}}})
    elif method in ("ping", "shutdown", "account.stop"):
        write({"id": key, "result": {}})
    elif key is not None:
        write({"id": key, "error": {"code": -32601, "message": "unknown fixture method"}})
