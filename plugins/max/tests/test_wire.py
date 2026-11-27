import json
import time

import pytest

from max_plugin.state import PrivateState

A = {"account_id": "max"}


def sign_in(core, account="max", mode="phone_code"):
    a = {"account_id": account}
    core.call("auth.begin", {**a, "method": mode})
    kinds = ["phone", "code", "password"] if mode == "phone_code" else ["qr", "password"]
    for kind in kinds:
        step = core.event("auth.step", account_id=account)["step"]
        assert step["kind"] == kind
        if kind != "qr":
            core.call("auth.submit", {**a, "flow": "sign_in", "step_id": step["step_id"],
                                       "value": "synthetic"})
    core.event("status", account_id=account, state="connected")
    core.event("auth.step", lambda p: p["step"]["kind"] == "done", account_id=account)


def test_auth_order_accounts_events_send_and_pagination(core_factory, tmp_path):
    core = core_factory()
    core.start()
    assert core.capabilities == {"auth.interactive": True, "delete": True, "delete.for_me": True,
                                 "edit": True, "send.entities": {"types": []}}
    core.event("status", state="auth_required")
    frames = [json.loads(line) for line in core.raw]
    start_reply = next(i for i, frame in enumerate(frames) if "result" in frame and frame["result"] == {})
    first_status = next(i for i, frame in enumerate(frames) if frame.get("method") == "status")
    assert first_status > start_reply
    assert core.call("ping") == {}
    sign_in(core)
    core.event("contacts.upsert")
    core.event("conversations.upsert")
    core.event("messages.upsert")
    assert core.error("auth.begin", {**A, "method": "phone_code"})["data"]["code"] == "INVALID_PARAMS"
    assert core.call("auth.cancel", {**A, "flow": "sign_in"}) == {}
    sent = core.call("messages.send", {**A, "chat_id": "10", "text": "**literal**", "entities": [], "client_id": "send-1"})
    assert sent["message"]["text"] == "**literal**"
    assert sent["message"]["ext"]["max"] == {
        "message_type": "USER", "has_attachments": False, "client_id": "send-1"}
    emitted = core.event("messages.upsert", lambda p: p["messages"][0]["id"] == sent["message"]["id"])
    assert emitted["messages"][0] == sent["message"]
    replay = core.call("messages.send", {**A, "chat_id": "10", "text": "**literal**", "client_id": "send-1"})
    assert replay == sent
    err = core.error("messages.send", {**A, "chat_id": "10", "text": "different", "client_id": "send-1"})
    assert err["data"]["code"] == "INVALID_PARAMS"
    for optional in ({"silent": False}, {"entities": [{"type": "bold", "offset": 0, "length": 1}]}, {"topic_id": "1"}, {"no_webpage": False}):
        assert core.error("messages.send", {**A, "chat_id": "10", "text": "x", **optional})["data"]["code"] == "NOT_SUPPORTED"
    core.call("messages.send", {**A, "chat_id": "10", "text": "live"})
    core.event("messages.upsert", lambda p: p["messages"][0]["id"] == "200")
    core.event("messages.deleted", chat_id="10")
    core.event("conversations.upsert", lambda p: p["conversations"][0].get("title") == "Updated")
    page = core.call("messages.fetch_recent", {**A, "chat_id": "10", "limit": 3})
    assert [m["id"] for m in page["messages"]] == ["22", "21", "20"]
    assert core.error("messages.fetch_recent", {**A, "chat_id": "10", "limit": 3,
                                                "cursor": page["next_cursor"]})["data"]["code"] == "NOT_SUPPORTED"
    assert core.error("messages.fetch_recent", {**A, "chat_id": "99", "limit": 3,
                                                "cursor": page["next_cursor"]})["data"]["code"] == "INVALID_PARAMS"
    empty = core.call("messages.fetch_recent", {**A, "chat_id": "99", "limit": 3})
    assert empty["messages"] == [] and empty["next_cursor"]
    assert core.error("history.fill_gap", {**A, "chat_id": "10", "after_message_id": "1", "limit": 3})["data"]["code"] == "NOT_SUPPORTED"
    core.start("max#2")
    core.event("status", account_id="max#2", state="auth_required")
    sign_in(core, "max#2", "qr")
    first = json.loads((tmp_path / "accounts/max/state/identity.json").read_text())
    second = json.loads((tmp_path / "accounts/max_2/state/identity.json").read_text())
    assert first["device_id"] != second["device_id"]
    assert core.error("messages.fetch_recent", {"account_id": "max#2", "chat_id": "10", "limit": 3,
                                                "cursor": page["next_cursor"]})["data"]["code"] == "INVALID_PARAMS"
    core.stop()
    assert core.error("messages.send", {**A, "chat_id": "10", "text": "x"})["data"]["code"] == "UNKNOWN_ACCOUNT"
    assert core.shutdown() == 0


def test_unknown_send_survives_process_restart_without_retry(core_factory, tmp_path):
    core = core_factory()
    core.start()
    core.event("status", state="auth_required")
    sign_in(core)
    confirmed = {**A, "chat_id": "10", "text": "confirmed", "client_id": "confirmed-once"}
    sent = core.call("messages.send", confirmed)
    params = {**A, "chat_id": "10", "text": "uncertain", "client_id": "never-retry"}
    error = core.error("messages.send", params)
    assert error["data"]["outcome"] == "unknown" and error["data"]["retry_safe"] is False
    assert core.shutdown() == 0
    again = core_factory()
    again.start()
    again.event("status", state="connected")
    assert again.error("messages.send", params)["data"]["outcome"] == "unknown"
    assert again.call("messages.send", confirmed) == sent
    replay_event = again.event("messages.upsert", lambda p: p["messages"][0]["id"] == sent["message"]["id"])
    assert replay_event["messages"][0] == sent["message"]
    assert replay_event["messages"][0]["ext"]["max"]["client_id"] == "confirmed-once"
    state = json.loads((tmp_path / "accounts/max/state/identity.json").read_text())
    assert state["send_count"] == 2
    assert "synthetic-private-token-do-not-log" not in core.stderr_text + again.stderr_text
    assert again.shutdown() == 0


@pytest.mark.parametrize("relation", ["valid", "missing", "wrong-id", "foreign-chat", "malformed-time"])
def test_confirmed_reply_journal_requires_observed_relation_before_reuse(core_factory, tmp_path, relation):
    core = core_factory()
    core.start()
    core.event("status", state="auth_required")
    sign_in(core)
    params = {**A, "chat_id": "10", "text": "literal", "client_id": "reply-once", "reply_to": {"message_id": "19"}}
    sent = core.call("messages.send", params)
    assert sent["message"]["reply_to"] == {"message_id": "19", "chat_id": "10"}
    assert core.shutdown() == 0
    path = tmp_path / "accounts/max/state/sends.json"
    journal = json.loads(path.read_text())
    entry = next(iter(journal.values()))
    if relation == "missing": entry["message"].pop("reply_to")
    elif relation == "wrong-id": entry["message"]["reply_to"]["message_id"] = "18"
    elif relation == "foreign-chat": entry["message"]["reply_to"]["chat_id"] = "99"
    elif relation == "malformed-time": entry["message"]["sent_at"] = "999999999999999999999-01-01T00:00:00Z"
    path.write_text(json.dumps(journal))
    again = core_factory()
    again.start()
    again.event("status", state="connected")
    if relation == "valid":
        assert again.call("messages.send", params) == sent
    else:
        assert again.error("messages.send", params)["data"] == {"code": "NETWORK", "outcome": "unknown", "retry_safe": False}
        assert not any(message.get("ext", {}).get("max", {}).get("client_id") == "reply-once"
                       for event in again.all_events if event["method"] == "messages.upsert"
                       for message in event["params"]["messages"])
    identity = json.loads((tmp_path / "accounts/max/state/identity.json").read_text())
    assert identity["send_count"] == 1
    assert again.shutdown() == 0


@pytest.mark.parametrize("scenario", ["reply-missing", "reply-foreign", "reply-pending"])
def test_unconfirmed_reply_never_gets_correlated_or_replayed(core_factory, tmp_path, scenario):
    core = core_factory(scenario)
    core.start()
    core.event("status", state="auth_required")
    sign_in(core)
    params = {**A, "chat_id": "10", "text": "literal", "client_id": "reply-unknown", "reply_to": {"message_id": "19"}}
    if scenario == "reply-pending":
        request = core.send_request("messages.send", params)
        core.event("conversations.upsert", lambda p: p["conversations"][0].get("title") == "Reply pending")
        core.cancel(request)
        error = core.response(request)["error"]
    else:
        error = core.error("messages.send", params)
    assert error["data"] == {"code": "NETWORK", "outcome": "unknown", "retry_safe": False}
    assert core.error("messages.send", params)["data"]["outcome"] == "unknown"
    assert not any(message.get("ext", {}).get("max", {}).get("client_id") == "reply-unknown"
                   for event in core.all_events if event["method"] == "messages.upsert"
                   for message in event["params"]["messages"])
    identity = json.loads((tmp_path / "accounts/max/state/identity.json").read_text())
    assert identity["send_count"] == 1
    assert core.shutdown() == 0


def test_slow_connection_fast_start_ping_stop_and_eof(core_factory, tmp_path):
    directory = tmp_path / "accounts/max/state"
    state = PrivateState(directory, "max")
    state.identity["mode"] = "qr"
    state.save_identity()
    state.write("session.json", {"token": "synthetic", "device_id": state.identity["device_id"]})
    state.release()
    core = core_factory("slow")
    before = time.monotonic()
    core.start()
    assert time.monotonic() - before < 2
    assert core.call("ping") == {}
    core.stop()
    core.event("status", state="disconnected")
    core.start()
    core.proc.stdin.close()
    assert core.wait_exit(5) == 0


def test_reconnect_and_cancel(core_factory, tmp_path):
    state = PrivateState(tmp_path / "accounts/max/state", "max")
    state.identity["mode"] = "qr"
    state.save_identity()
    state.write("session.json", {"token": "synthetic", "device_id": state.identity["device_id"]})
    state.release()
    core = core_factory("reconnect")
    core.start()
    core.event("status", state="unavailable")
    core.event("status", state="connected")
    assert core.shutdown() == 0
    assert "synthetic-private-token-do-not-log" not in core.stderr_text
    other = core_factory(directory=tmp_path / "fresh")
    other.start()
    other.event("status", state="auth_required")
    other.call("auth.begin", {**A, "method": "phone_code"})
    other.event("auth.step")
    other.call("auth.cancel", {**A, "flow": "sign_in"})
    other.event("status", state="auth_required")
    assert not (tmp_path / "fresh/accounts/max/state/session.json").exists()
    assert other.shutdown() == 0


def test_auth_restart_stale_submission_and_buffered_login_events(core_factory):
    core = core_factory("early-events")
    core.start()
    core.event("status", state="auth_required")
    core.call("auth.begin", {**A, "method": "phone_code"})
    old = core.event("auth.step")["step"]
    core.call("auth.begin", {**A, "method": "phone_code"})
    new = core.event("auth.step")["step"]
    assert new["step_id"] != old["step_id"]
    core.call("auth.submit", {**A, "flow": "sign_in", "step_id": old["step_id"], "value": "stale"})
    core.call("auth.submit", {**A, "flow": "sign_in", "step_id": new["step_id"], "value": "synthetic"})
    for kind in ["code", "password"]:
        step = core.event("auth.step")["step"]
        assert step["kind"] == kind
        core.call("auth.submit", {**A, "flow": "sign_in", "step_id": step["step_id"], "value": "synthetic"})
    core.event("auth.step", lambda p: p["step"]["kind"] == "done")
    messages = [frame["params"]["messages"][0]["id"] for frame in core.all_events
                if frame["method"] == "messages.upsert"]
    assert messages == ["20", "200"]
    assert core.shutdown() == 0


def test_opaque_client_id_empty_unicode_and_absent_are_distinct(core_factory, tmp_path):
    core = core_factory()
    core.start()
    core.event("status", state="auth_required")
    sign_in(core)
    params = {**A, "chat_id": "10", "text": "opaque ID"}
    empty = core.call("messages.send", {**params, "client_id": ""})
    assert empty["message"]["ext"]["max"]["client_id"] == ""
    emitted = core.event("messages.upsert", lambda p: p["messages"][0]["id"] == empty["message"]["id"])
    assert emitted["messages"][0] == empty["message"]
    assert core.call("messages.send", {**params, "client_id": ""}) == empty
    # An omitted/null ID has no journal key, and must not alias the empty ID.
    absent = core.call("messages.send", params)
    null = core.call("messages.send", {**params, "client_id": None})
    assert len({empty["message"]["id"], absent["message"]["id"], null["message"]["id"]}) == 3
    assert "client_id" not in absent["message"]["ext"]["max"]
    assert "client_id" not in null["message"]["ext"]["max"]
    opaque = "雪🙂" * 300
    unicode_sent = core.call("messages.send", {**params, "client_id": opaque})
    assert core.call("messages.send", {**params, "client_id": opaque}) == unicode_sent
    assert core.shutdown() == 0
    # The UTF-8 hash and empty-string key survive the process restart.
    again = core_factory()
    again.start()
    again.event("status", state="connected")
    assert again.call("messages.send", {**params, "client_id": ""}) == empty
    replay_event = again.event("messages.upsert", lambda p: p["messages"][0]["id"] == empty["message"]["id"])
    assert replay_event["messages"][0]["ext"]["max"]["client_id"] == ""
    assert again.call("messages.send", {**params, "client_id": opaque}) == unicode_sent
    assert unicode_sent["message"]["ext"]["max"]["client_id"] == opaque
    journal = json.loads((tmp_path / "accounts/max/state/sends.json").read_text())
    assert {row["message"]["ext"]["max"]["client_id"] for row in journal.values()} == {"", opaque}
    state = json.loads((tmp_path / "accounts/max/state/identity.json").read_text())
    assert state["send_count"] == 4
    assert again.shutdown() == 0


def test_snapshot_buffer_overflow_is_fatal_even_when_dispatcher_swallows_errors(core_factory, tmp_path):
    store = PrivateState(tmp_path / "accounts/max/state", "max")
    store.identity["mode"] = "qr"
    store.save_identity()
    store.write("session.json", {"token": "synthetic", "device_id": store.identity["device_id"]})
    store.release()
    core = core_factory("overflow-events")
    core.start()
    error = core.event("status", state="error")
    assert "event buffer overflow" in error["detail"]
    assert not any(event["method"] == "status" and event["params"]["state"] == "connected"
                   for event in core.all_events)
    assert not any(event["method"] == "messages.upsert" for event in core.all_events)
    identity = json.loads((tmp_path / "accounts/max/state/identity.json").read_text())
    assert identity["close_count"] == 1
    assert core.error("messages.send", {**A, "chat_id": "10", "text": "must not send"})["data"]["code"] == "UNAVAILABLE"
    assert core.shutdown() == 0


def test_event_overflow_cancels_owned_pending_discovery(core_factory, tmp_path):
    store = PrivateState(tmp_path / "accounts/max/state", "max")
    store.identity["mode"] = "qr"
    store.save_identity()
    store.write("session.json", {"token": "synthetic", "device_id": store.identity["device_id"]})
    store.release()
    core = core_factory("overflow-pending-discovery")
    core.start()
    core.event("status", state="error")
    identity = json.loads((tmp_path / "accounts/max/state/identity.json").read_text())
    assert identity["snapshot_cancelled"] is True and identity["close_count"] == 1
    assert not any(event["method"] == "status" and event["params"]["state"] == "connected"
                   for event in core.all_events)
    assert core.call("ping") == {}
    assert core.shutdown() == 0
