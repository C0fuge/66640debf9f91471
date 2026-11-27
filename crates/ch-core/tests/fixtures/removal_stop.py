"""Isolated logical lifecycle peer. Does not read payloads or real sessions."""
import json
import os
from pathlib import Path
import sys
import threading
import time

root = Path(os.environ["CH_TEST_ACCOUNT_STARTS"]).parent
mode = os.environ["CH_TEST_REMOVAL_MODE"]
lock = threading.Lock()


def answer(frame, error=None):
    response = {"jsonrpc": "2.0", "id": frame["id"]}
    if error is None:
        response["result"] = ({"api_version": "1.2.0", "capabilities": {}}
                              if frame["method"] == "initialize" else {})
        if (mode == "stop-malformed" and frame["method"] == "account.stop"
                or mode == "shutdown-malformed" and frame["method"] == "shutdown"):
            response["result"] = None
    else:
        code, name = error
        response["error"] = {"code": code, "message": name,
                             "data": {"code": name}}
    with lock:
        print(json.dumps(response), flush=True)


def delayed_answer(frame, kind):
    (root / f"{kind}-waiting").touch()
    deadline = time.monotonic() + 4
    while not (root / f"release-{kind}").exists() and time.monotonic() < deadline:
        time.sleep(0.005)
    answer(frame)
    (root / f"{kind}-answered").touch()


for line in sys.stdin:
    frame = json.loads(line)
    if "id" not in frame:
        continue
    method = frame["method"]
    with (root / "requests.log").open("a") as record:
        record.write(method + "\n")
    if method == "account.start":
        with Path(os.environ["CH_TEST_ACCOUNT_STARTS"]).open("a") as record:
            record.write(frame["params"]["account_id"] + "\n")
        if mode == "delayed-start":
            threading.Thread(target=delayed_answer, args=(frame, "start"), daemon=True).start()
            continue
    if method == "account.stop":
        if mode == "delayed-stop":
            threading.Thread(target=delayed_answer, args=(frame, "stop"), daemon=True).start()
            continue
        if mode == "stop-timeout":
            continue
        if mode == "stop-error":
            answer(frame, (-32603, "INTERNAL"))
            continue
        if mode == "stop-unknown":
            answer(frame, (-32008, "UNKNOWN_ACCOUNT"))
            continue
    if method == "shutdown" and mode == "shutdown-error":
        answer(frame, (-32603, "INTERNAL"))
        break
    answer(frame)
    if method == "shutdown":
        break
