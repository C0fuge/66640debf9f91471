"""Isolated metadata peer. The control file contains only synthetic status DTOs."""
import json
import os
from pathlib import Path
import sys
import threading
import time

root = Path(os.environ["CH_TEST_METADATA_ROOT"])
output = threading.Lock()
state = threading.Lock()
active = None


def send(frame):
    with output:
        print(json.dumps(frame), flush=True)


def pulses():
    previous = None
    while True:
        try:
            value = (root / "pulse.json").read_text()
            payload = json.loads(value)
        except (OSError, ValueError):
            time.sleep(0.005)
            continue
        if value != previous:
            with state:
                if active is not None:
                    payload["account_id"] = active
                    send({"jsonrpc": "2.0", "id": f"pulse-{payload['me']['id']}",
                          "method": "status", "params": payload})
                    previous = value
        time.sleep(0.005)


threading.Thread(target=pulses, daemon=True).start()
for line in sys.stdin:
    frame = json.loads(line)
    if "id" not in frame:
        continue
    if "method" not in frame:
        (root / f"ack-{frame['id']}").touch()
        continue
    method = frame["method"]
    result = {"api_version": "1.2.0", "capabilities": {}} if method == "initialize" else {}
    with state:
        if method == "account.start":
            active = frame["params"]["account_id"]
        if method in ("account.stop", "shutdown"):
            active = None
        # Source status is serialized after start ACK and before stop ACK.
        send({"jsonrpc": "2.0", "id": frame["id"], "result": result})
    if method == "shutdown":
        break
