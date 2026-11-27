"""Synthetic account-start recorder; never reads upload bytes or real accounts."""
import json
import os
from pathlib import Path
import sys


for line in sys.stdin:
    frame = json.loads(line)
    if "id" not in frame:
        continue
    method = frame["method"]
    result = {}
    if method == "initialize":
        result = {"api_version": "1.2.0", "capabilities": {}}
    if method == "account.start":
        with Path(os.environ["CH_TEST_ACCOUNT_STARTS"]).open("a") as record:
            record.write(frame["params"]["account_id"] + "\n")
    print(json.dumps({"jsonrpc": "2.0", "id": frame["id"], "result": result}), flush=True)
    if method == "shutdown":
        break
