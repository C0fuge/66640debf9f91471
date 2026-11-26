#!/usr/bin/env python3
"""A tiny raw-NDJSON Contact Hub plugin for host tests; no SDK.

`--mode` picks the misbehaviour:
  good           well-behaved; extra test hooks via `messages.fetch_recent` `chat_id`:
                   "slow"      never answers until $/cancelRequest, then CANCELLED
                   "hang"      never answers at all (even after cancel)
                   "progress"  emits transfer.progress every `step_ms` for `steps`, then answers
                   "crash"     os._exit(3) immediately
                   "flood"     emits `count` messages.upsert events (notifications, or requests
                               when `acked` is true), then answers with next_cursor "sent=<n>"
                   "stray"     emits an event for an account that is not started, as a request
                   "malformed" writes a batch array and a shapeless object; reports the reply code
  hold_stop      waits for a synthetic marker in state_dir before replying to account.stop
  crash_on_start exits 1 before reading anything
  old_api        answers initialize with api_version 0.9.0
  garbage        prints a non-JSON line to stdout after initialize
  slow_shutdown  answers shutdown after 300 ms; if stdin closes first, writes the marker
                 file given by CH_TEST_MARKER and exits 3
  ignore_shutdown never answers shutdown and never exits on EOF (SIGTERM kills it)
  immortal       like ignore_shutdown but ignores SIGTERM too (needs SIGKILL)
  hang           stops answering anything after account.start (ping misses -> restart)
"""
import argparse
import json
import os
import queue
import signal
import sys
import threading
import time

parser = argparse.ArgumentParser()
parser.add_argument("--mode", default="good")
MODE = parser.parse_args().mode

if MODE == "crash_on_start":
    sys.exit(1)
if MODE == "immortal":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

OUT = sys.stdout.buffer
OUT_LOCK = threading.Lock()
INBOX: "queue.Queue[object]" = queue.Queue()
EOF = object()
NEXT_ID = 1
WAITERS: dict = {}
STARTED: dict = {}
START_COUNT: dict = {}
SHUTDOWN_ANSWERED = False
STDERR_LOCK = threading.Lock()


def log(msg):
    with STDERR_LOCK:
        sys.stderr.write(f"[{MODE}] {msg}\n")
        sys.stderr.flush()


def write(obj):
    data = json.dumps(obj, separators=(",", ":")).encode() + b"\n"
    with OUT_LOCK:
        OUT.write(data)
        OUT.flush()


def reply(req_id, result):
    write({"jsonrpc": "2.0", "id": req_id, "result": result})


def error(req_id, numeric, code, message, **extra):
    data = {"code": code}
    data.update(extra)
    write({"jsonrpc": "2.0", "id": req_id, "error": {"code": numeric, "message": message, "data": data}})


def notify(method, params):
    write({"jsonrpc": "2.0", "method": method, "params": params})


def request(method, params, timeout=10.0):
    """Send a plugin -> core request and wait for its reply (processed by the main loop)."""
    global NEXT_ID
    rid = f"p{NEXT_ID}"
    NEXT_ID += 1
    ev = threading.Event()
    WAITERS[rid] = [ev, None]
    write({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
    return rid, ev


def read_loop():
    for raw in sys.stdin.buffer:
        line = raw.strip()
        if not line:
            continue
        try:
            INBOX.put(json.loads(line))
        except ValueError:
            log(f"bad json from core: {line[:80]!r}")
    INBOX.put(EOF)


threading.Thread(target=read_loop, daemon=True).start()

PENDING_CANCELS: dict = {}  # core request id -> thread.Event


def handle_request(req_id, method, params):
    global SHUTDOWN_ANSWERED
    if method == "initialize":
        api = "0.9.0" if MODE == "old_api" else "1.0.0"
        reply(req_id, {"api_version": api, "capabilities": {"send.silent": True}, "plugin_version": "0.0.1"})
        if MODE == "garbage":
            with OUT_LOCK:
                OUT.write(b"hello from a stray print\n")
                OUT.flush()
        return
    if method == "ping":
        reply(req_id, {})
        return
    if method == "shutdown":
        if MODE in ("ignore_shutdown", "immortal"):
            log("ignoring shutdown")
            return
        if MODE == "slow_shutdown":
            time.sleep(0.3)
        for acc in list(STARTED):
            notify("status", {"account_id": acc, "state": "disconnected", "reconnecting": False})
        STARTED.clear()
        SHUTDOWN_ANSWERED = True
        reply(req_id, {})
        time.sleep(0.05)
        os._exit(0)
    if method == "account.start":
        acc = params["account_id"]
        if acc in STARTED:
            error(req_id, -32602, "INVALID_PARAMS", "already started", field="account_id")
            return
        STARTED[acc] = params
        START_COUNT[acc] = START_COUNT.get(acc, 0) + 1
        reply(req_id, {})
        detail = json.dumps({"start": START_COUNT[acc], "settings": params["settings"], "secrets": params["secrets"],
                             "state_dir": params["state_dir"], "ghost": params["ghost"]}, sort_keys=True)
        notify("status", {"account_id": acc, "state": "connected", "reconnecting": False, "detail": detail})
        # Store a runtime secret the first time; the host must hand it back on the next start.
        if "session" not in params["secrets"]:
            rid, ev = request("secrets.set", {"account_id": acc, "key": "session", "value": f"session-for-{acc}"})
            threading.Thread(target=await_secret, args=(acc, rid, ev), daemon=True).start()
        if MODE == "hang":
            log("hanging now")
            while True:
                time.sleep(3600)
        return
    if method == "account.stop":
        acc = params["account_id"]
        if acc not in STARTED:
            error(req_id, -32008, "UNKNOWN_ACCOUNT", "not started")
            return
        if MODE == "hold_stop":
            marker = os.path.join(STARTED[acc]["state_dir"], "release-stop")
            deadline = time.monotonic() + 5
            while not os.path.exists(marker) and time.monotonic() < deadline:
                time.sleep(0.005)
        del STARTED[acc]
        notify("status", {"account_id": acc, "state": "disconnected", "reconnecting": False})
        reply(req_id, {})
        return
    if method == "messages.fetch_recent":
        acc = params.get("account_id")
        if acc not in STARTED:
            error(req_id, -32008, "UNKNOWN_ACCOUNT", "not started")
            return
        chat = params.get("chat_id", "")
        if chat == "run_status":
            notify("status", {"account_id": acc, "state": "connected", "detail": "updated", "reconnecting": False})
        if chat == "slow":
            ev = threading.Event()
            PENDING_CANCELS[req_id] = ev
            threading.Thread(target=slow_request, args=(req_id, ev), daemon=True).start()
            return
        if chat == "hang":
            return
        if chat == "crash":
            log("crashing on purpose")
            os._exit(3)
        if chat == "progress":
            threading.Thread(target=progress_request, args=(req_id, acc, params), daemon=True).start()
            return
        if chat == "flood":
            threading.Thread(target=flood, args=(req_id, acc, params), daemon=True).start()
            return
        if chat == "malformed":
            # A batch array (answered with id null) and an object that is no JSON-RPC shape.
            with OUT_LOCK:
                OUT.write(b"[1,2]\n")
                OUT.write(b'{"id":"q1","foo":1}\n')
                OUT.flush()
            ev = threading.Event()
            WAITERS["q1"] = [ev, None]
            threading.Thread(target=await_malformed, args=(req_id, ev), daemon=True).start()
            return
        if chat == "stray":
            rid, ev = request("status", {"account_id": "nobody", "state": "connected", "reconnecting": False})
            threading.Thread(target=await_stray, args=(req_id, rid, ev), daemon=True).start()
            return
        reply(req_id, {"messages": [], "next_cursor": f"seen_settings={json.dumps(STARTED[acc]['settings'], sort_keys=True)}"})
        return
    if method == "messages.send":
        acc = params.get("account_id")
        if acc not in STARTED:
            error(req_id, -32008, "UNKNOWN_ACCOUNT", "not started")
            return
        msg = {"id": "m1", "chat_id": params["chat_id"], "direction": "out",
               "sent_at": "2026-11-10T00:00:00.000Z", "text": params["text"]}
        if params.get("silent") is not None:
            msg["text"] = "silent:" + params["text"]
        reply(req_id, {"message": msg})
        return
    error(req_id, -32601, "METHOD_NOT_FOUND", f"unknown method {method}")


def slow_request(req_id, ev):
    ev.wait()
    error(req_id, -32800, "CANCELLED", "cancelled as asked")


def progress_request(req_id, acc, params):
    steps = int(params.get("steps", 5))
    step_ms = int(params.get("step_ms", 100))
    for i in range(steps):
        time.sleep(step_ms / 1000)
        notify("transfer.progress", {"account_id": acc, "request_id": req_id, "direction": "download",
                                     "done_bytes": i + 1, "total_bytes": steps})
    reply(req_id, {"messages": [], "next_cursor": f"progress={steps}"})


def flood(req_id, acc, params):
    count = int(params.get("count", 1000))
    acked = bool(params.get("acked", False))
    sent = 0
    for i in range(count):
        ev = {"account_id": acc, "messages": [{"id": str(i), "chat_id": "flood", "direction": "in",
                                                "sent_at": "2026-11-10T00:00:00.000Z", "text": f"event {i}"}]}
        if acked:
            rid, done = request("messages.upsert", ev)
            done.wait(30)
            res = WAITERS.pop(rid, [None, None])[1]
            if res is None or "error" in res:
                log(f"ack {i} failed: {res}")
                break
        else:
            notify("messages.upsert", ev)
        sent += 1
        if sent % 100 == 0:
            log(f"flood sent {sent}")
    reply(req_id, {"messages": [], "next_cursor": f"sent={sent}"})


def await_secret(acc, rid, ev):
    ev.wait(10)
    res = WAITERS.pop(rid, [None, None])[1]
    log(f"secrets.set for {acc} -> {res}")


def await_malformed(req_id, ev):
    ev.wait(10)
    res = WAITERS.pop("q1", [None, None])[1]
    code = (res or {}).get("error", {}).get("data", {}).get("code")
    reply(req_id, {"messages": [], "next_cursor": f"malformed={code}"})


def await_stray(req_id, rid, ev):
    ev.wait(10)
    res = WAITERS.pop(rid, [None, None])[1]
    code = (res or {}).get("error", {}).get("data", {}).get("code")
    reply(req_id, {"messages": [], "next_cursor": f"stray={code}"})


def main():
    while True:
        msg = INBOX.get()
        if msg is EOF:
            if MODE in ("ignore_shutdown", "immortal"):
                log("EOF ignored")
                while True:
                    time.sleep(3600)
            if MODE == "slow_shutdown" and not SHUTDOWN_ANSWERED:
                marker = os.environ.get("CH_TEST_MARKER")
                if marker:
                    with open(marker, "w") as f:
                        f.write("stdin closed before the shutdown reply\n")
                os._exit(3)
            log("EOF; exiting")
            os._exit(0)
        if not isinstance(msg, dict):
            continue
        if "method" in msg:
            method = msg["method"]
            params = msg.get("params") or {}
            if "id" in msg:
                handle_request(msg["id"], method, params)
            elif method == "$/cancelRequest":
                ev = PENDING_CANCELS.pop(params.get("id"), None)
                if ev:
                    ev.set()
        elif "id" in msg:
            w = WAITERS.get(msg["id"])
            if w:
                w[1] = msg
                w[0].set()


main()
