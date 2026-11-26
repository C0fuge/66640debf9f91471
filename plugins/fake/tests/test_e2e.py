"""End-to-end sessions against the fake plugin process: auth -> sync -> send -> paging ->
scripted errors -> cancel -> restart -> multi-account QR -> shutdown, plus the start
states, the live loop and random failures. Every frame is schema-checked by conftest."""

from __future__ import annotations

import time

from conftest import FakeCore

A = {"account_id": "fake"}


def code_of(err: dict) -> str:
    return err["data"]["code"]


def no_partials(core: FakeCore) -> bool:
    return not list((core.tmp / "accounts").rglob("*.part"))


def test_full_session(core: FakeCore):
    init = core.initialize()
    assert init["api_version"] == "1.2.0" and init["plugin_version"] == "0.1.0"
    assert init["capabilities"]["auth.interactive"] is True
    assert core.call("ping") == {}
    assert code_of(core.error("messages.fetch_recent", {**A, "chat_id": "c0", "limit": 1})) == "UNKNOWN_ACCOUNT"

    # unusable settings are rejected with the field name
    err = core.error("account.start", core.start_params(settings={"chats": 0}))
    assert err["data"] == {"code": "INVALID_PARAMS", "field": "chats"}
    err = core.error("account.start", core.start_params(settings={"fail_script": "messages.send=BOGUS"}))
    assert err["data"]["field"] == "fail_script"

    core.start(settings={"messages_per_chat": 50, "transfer_chunk_ms": 100,
                         "fail_script": "messages.send#2=FLOOD_WAIT:7"})
    assert core.event("status")["state"] == "connecting"
    core.event("status", lambda p: p["state"] == "auth_required")
    assert code_of(core.error("messages.fetch_recent", {**A, "chat_id": "c0", "limit": 1})) == "AUTH_REQUIRED"

    # interactive auth: phone (bad, good) -> code (bad, good) -> 2FA password (bad, good)
    assert core.call("auth.begin", {**A, "method": "phone_code"}) == {}
    expected = [("phone", None, "abc"), ("phone", "Invalid phone number", "+15550000001"),
                ("code", None, "11111"), ("code", "Wrong code", "12345"),
                ("password", None, "nope"), ("password", "Wrong password", "fake")]
    for kind, error, value in expected:
        step = core.event("auth.step")["step"]
        assert (step["kind"], step.get("error")) == (kind, error), step
        assert step["flow"] == "sign_in"
        if kind == "code":
            assert step["can_resend"] is True and "12345" in step["hint"]
        assert core.call("auth.submit", {**A, "flow": "sign_in", "step_id": step["step_id"], "value": value}) == {}
    sync = core.wait_synced()
    assert core.secrets["fake"] == {"session": "fake-session-1"}

    # the initial sync covers every kind of chat, folders, topics and contacts
    convs = {c["id"]: c for p in sync["conversations.upsert"] for c in p["conversations"]}
    assert sync["conversations.synced"][0]["chat_ids"] == list(convs)
    assert convs["c0"]["is_self_chat"] is True and convs["c0"]["pinned"] is True
    assert convs["c1"]["peer_user_id"] == "b1"
    assert convs["c3"]["kind"] == "channel" and convs["c3"]["can_post"] is False
    assert convs["c4"]["is_forum"] is True
    assert {c["kind"] for c in convs.values()} == {"dm", "group", "channel"}
    assert any(c.get("archived") for c in convs.values()) and any(c.get("draft") for c in convs.values())
    assert [f["id"] for f in sync["folders.synced"][0]["folders"]] == ["f1", "f2", "f3", "f4"]
    assert sync["topics.upsert"][0]["complete"] is True and len(sync["topics.upsert"][0]["topics"]) == 4
    contacts = [c for p in sync["contacts.upsert"] for c in p["contacts"]]
    assert any(c.get("is_bot") for c in contacts)
    # first emission: the merge fields are explicit (spec 8.4)
    assert all({"archived", "unread_count", "is_self_chat"} <= set(c) for c in convs.values())

    # a later update carries only what changed: absent = unchanged (spec 8.4)
    assert core.call("read.send_receipt", {**A, "chat_id": "c2", "max_message_id": "1000"}) == {}
    upd = core.event("conversations.upsert", lambda p: p["conversations"][0]["id"] == "c2")["conversations"][0]
    assert upd["unread_count"] == 0 and "archived" not in upd and "is_self_chat" not in upd and "title" not in upd

    # send; the second send hits the scripted FLOOD_WAIT; the in-band /fail hook
    m1 = core.call("messages.send", {**A, "chat_id": "c2", "text": "first"})["message"]
    err = core.error("messages.send", {**A, "chat_id": "c2", "text": "second"})
    assert err["code"] == -32003 and err["data"] == {"code": "FLOOD_WAIT", "seconds": 7}
    m3 = core.call("messages.send", {**A, "chat_id": "c2", "text": "third"})["message"]
    assert int(m3["id"]) == int(m1["id"]) + 1 and m3["sent_at"] > m1["sent_at"]
    err = core.error("messages.send", {**A, "chat_id": "c2", "text": "/fail NETWORK"})
    assert err["code"] == -32006 and code_of(err) == "NETWORK"
    err = core.error("messages.send", {**A, "chat_id": "c3", "text": "read-only"})
    assert code_of(err) == "FORBIDDEN"
    assert core.error("messages.fetch_recent", {**A, "chat_id": "nope", "limit": 1})["data"] == {
        "code": "NOT_FOUND", "kind": "chat"}
    assert code_of(core.error("scheduled.list", {**A, "chat_id": "c2"})) == "NOT_SUPPORTED"
    err = core.error("messages.send", {**A, "chat_id": "c2", "text": "x", "schedule_at": "2026-02-01T00:00:00.000Z"})
    assert err["data"] == {"code": "NOT_SUPPORTED", "capability": "send.schedule"}
    assert code_of(core.error("no.such_method", A)) == "METHOD_NOT_FOUND"

    # paging the whole history back, then forwards with fill_gap
    ids, cursor = [], None
    while True:
        p = {**A, "chat_id": "c2", "limit": 7}
        if cursor:
            p["cursor"] = cursor
        page = core.call("messages.fetch_recent", p)
        ids += [int(m["id"]) for m in page["messages"]]
        cursor = page.get("next_cursor")
        if not cursor:
            break
    assert ids == sorted(ids, reverse=True) and len(ids) == len(set(ids)) == 52
    fwd, cursor = [], None
    while True:
        p = {**A, "chat_id": "c2", "after_message_id": "40", "limit": 4}
        if cursor:
            p["cursor"] = cursor
        page = core.call("history.fill_gap", p)
        fwd += [int(m["id"]) for m in page["messages"]]
        cursor = page.get("next_cursor")
        if not cursor:
            break
    assert fwd == list(range(41, 53))

    # a finished download reports progress to the end
    hit = core.call("media.search", {**A, "chat_id": "c2", "kind": "photo", "limit": 1})["messages"][0]
    rid = core.send_request("media.download", {**A, "chat_id": "c2", "message_id": hit["id"], "what": "full"})
    res = core.response(rid)["result"]
    prog = core.event("transfer.progress", lambda p: p.get("request_id") == rid and p["done_bytes"] == res["bytes"])
    assert prog["direction"] == "download" and prog["total_bytes"] == res["bytes"]
    assert core.call("messages.get", {**A, "chat_id": "c2", "message_id": hit["id"]})["message"]["media"]["path"] \
        == res["path"]

    # cancel an upload mid-way: CANCELLED, no partial file, nothing committed
    big = core.tmp / "big.bin"
    big.write_bytes(bytes(range(256)) * 4096 * 2)  # 2 MiB = 32 chunks of 100 ms
    rid = core.send_request("messages.send_file", {**A, "chat_id": "c5", "path": str(big)})
    core.event("transfer.progress", lambda p: p.get("request_id") == rid and p["done_bytes"] > 0)
    core.cancel(rid)
    err = core.response(rid)["error"]
    assert err["code"] == -32800 and code_of(err) == "CANCELLED"
    assert no_partials(core)
    last = core.call("messages.fetch_recent", {**A, "chat_id": "c5", "limit": 1})["messages"][0]
    assert last.get("media", {}).get("filename") != "big.bin"

    # cancel a download of a big document (2 MiB, 32 chunks)
    for cid in convs:
        docs = core.call("media.search", {**A, "chat_id": cid, "kind": "document", "query": "dataset",
                                            "limit": 1})["messages"]
        if docs:
            rid = core.send_request("media.download", {**A, "chat_id": cid, "message_id": docs[0]["id"], "what": "full"})
            core.event("transfer.progress", lambda p: p.get("request_id") == rid and p["done_bytes"] > 0)
            core.cancel(rid)
            assert code_of(core.response(rid)["error"]) == "CANCELLED"
            assert no_partials(core)
            break
    else:
        raise AssertionError("seed 1 should have a dataset.bin document")

    # restart with the stored session: no sign-in this time
    assert core.call("account.stop", A) == {}
    core.event("status", lambda p: p["state"] == "disconnected")
    core.start(secrets=core.secrets["fake"], settings={"messages_per_chat": 50})
    core.wait_synced()

    # the reconnect test hook
    assert core.call("x-fake.reconnect", A) == {}
    assert core.event("status", lambda p: p["state"] == "connecting")["reconnecting"] is True
    core.event("status", lambda p: p["state"] == "connected")

    # a second account, signed in by QR, served concurrently
    core.start("fake#2", settings={"require_2fa": False, "qr_scan_ms": 100, "seed": 2})
    core.event("status", lambda p: p["account_id"] == "fake#2" and p["state"] == "auth_required")
    core.call("auth.begin", {"account_id": "fake#2", "method": "qr"})
    step = core.event("auth.step", lambda p: p["account_id"] == "fake#2")["step"]
    assert step["kind"] == "qr" and step["data"].startswith("ch-fake://login")
    core.wait_synced("fake#2")
    assert core.secrets["fake#2"] == {"session": "fake-session-2"}
    assert core.call("messages.fetch_recent", {**A, "chat_id": "c2", "limit": 1})["messages"]

    # sign out drops the session
    core.call("account.sign_out", {"account_id": "fake#2"})
    core.event("status", lambda p: p["account_id"] == "fake#2" and p["state"] == "auth_required")
    assert core.secrets["fake#2"] == {}

    assert core.shutdown() == 0
    

def test_start_states(core: FakeCore):
    core.initialize()
    core.start("down", settings={"start_state": "unavailable"})
    core.event("status", lambda p: p["account_id"] == "down" and p["state"] == "unavailable")
    assert code_of(core.error("messages.fetch_recent", {"account_id": "down", "chat_id": "c0", "limit": 1})) \
        == "UNAVAILABLE"
    core.start("broken", settings={"start_state": "error"})
    st = core.event("status", lambda p: p["account_id"] == "broken" and p["state"] == "error")
    assert st["detail"]
    assert core.shutdown() == 0


def test_live_loop(core: FakeCore):
    core.initialize()
    core.start(settings={"start_state": "connected", "live_interval_ms": 500, "messages_per_chat": 20})
    core.wait_synced()
    core.drain(5.0)
    names = {e["method"] for e in core.events}
    assert "messages.upsert" in names or "presence" in names, names
    assert core.call("account.stop", A) == {}
    core.events.clear()
    core.drain(1.2)
    assert not core.events, "events after account.stop returned"
    assert core.shutdown() == 0


def test_random_failures_are_seeded(core: FakeCore):
    core.initialize()
    core.start(settings={"start_state": "connected", "fail_rate": 1.0, "messages_per_chat": 5})
    core.wait_synced()
    codes = [code_of(core.error("messages.fetch_recent", {**A, "chat_id": "c2", "limit": 1})) for _ in range(12)]
    assert set(codes) == {"FLOOD_WAIT", "NETWORK"}
    core.call("account.stop", A)
    core.start(settings={"start_state": "connected", "fail_rate": 1.0, "messages_per_chat": 5})
    core.wait_synced()
    again = [code_of(core.error("messages.fetch_recent", {**A, "chat_id": "c2", "limit": 1})) for _ in range(12)]
    assert again == codes
    assert core.shutdown() == 0


def test_latency(core: FakeCore):
    core.initialize()
    core.start(settings={"start_state": "connected", "latency_ms": 300, "messages_per_chat": 5})
    core.wait_synced()
    t = time.monotonic()
    core.call("messages.fetch_recent", {**A, "chat_id": "c2", "limit": 1})
    assert time.monotonic() - t >= 0.3
    assert core.shutdown() == 0
