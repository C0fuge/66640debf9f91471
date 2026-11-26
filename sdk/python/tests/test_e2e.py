"""Drive tests/echo_plugin.py as a real child process over stdio with
``contact_hub_sdk.testing.FakeCore`` playing the core: handshake -> account.start ->
method calls -> error paths -> auth -> cancel -> stop -> shutdown, plus the lifecycle
rules of spec 2.5, 4.1, 4.2, 4.6, 12 and 14.2. Every frame the plugin writes is
schema-checked."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

from conftest import SCHEMAS
from contact_hub_sdk import API_VERSION, PluginError
from contact_hub_sdk.testing import FakeCore, schema_check

HERE = Path(__file__).resolve().parent
CHECK = schema_check(SCHEMAS)
ACC = {"account_id": "acc#1"}


@pytest.fixture
def core(tmp_path, request):
    plugin_dir = tmp_path / "echo"
    plugin_dir.mkdir()
    (plugin_dir / "plugin.json").write_text(json.dumps({
        "id": "echo", "name": "Echo", "version": "0.2.0", "api_version": API_VERSION,
        "entry": ["python", "echo_plugin.py"], "brand_color": "#123456", "multi_account": True,
        "auth_methods": ["phone_code"],
        "settings_schema": {"type": "object", "properties": {
            "name": {"type": "string", "default": "Echo Me"},
            "token": {"type": "string", "x-secret": True}}},
    }))
    c = FakeCore([sys.executable, str(HERE / "echo_plugin.py"), *getattr(request, "param", [])], plugin_dir=plugin_dir, tmp=tmp_path,
                 env={"PYTHONPATH": str(HERE)}, check=CHECK, timeout=5.0)
    yield c
    c.close()
    sys.stderr.write(c.stderr_text[-3000:])  # shown by pytest on failure


def frames(core) -> list[dict]:
    return [json.loads(line) for line in core.raw]


def position(core, pred) -> int:
    return next(i for i, f in enumerate(frames(core)) if pred(f))


def start(core, account_id="acc#1", **kw):
    core.start(account_id, **{"settings": {"token": "tok-very-secret"}, **kw})


def test_full_session(core):
    # Framing errors come back with id null; nothing is sent before initialize is answered.
    core.send(b"this is not json\n")
    r = core.response(None)
    assert r["error"]["code"] == -32700 and r["error"]["data"]["code"] == "PARSE_ERROR"
    core.send(b"[]\n")
    assert core.response(None)["error"]["data"]["code"] == "INVALID_REQUEST"
    core.send(b"\r\n\n")  # empty lines are ignored

    # Handshake.
    res = core.initialize()
    assert res["api_version"] == API_VERSION and res["plugin_version"] == "0.2.0"
    assert res["capabilities"] == {"media.download": True, "send.silent": True, "x-echo.flag": {"a": 1},
                                   "auth.interactive": True}
    assert core.call("ping") == {}
    assert core.call("ping", {}) == {}

    # Account scoping.
    err = core.error("messages.fetch_recent", {"account_id": "nope", "chat_id": "c", "limit": 1})
    assert err["data"]["code"] == "UNKNOWN_ACCOUNT" and err["code"] == -32008
    err = core.error("account.start", core.start_params("bad", settings={"fail": True}))
    assert err["data"] == {"code": "INVALID_PARAMS", "field": "fail"}

    # account.start returns {} before any event for the account (spec 4.2).
    rid = core.send_request("account.start", core.start_params("acc#1", settings={"token": "tok-very-secret"}))
    assert core.response(rid)["result"] == {}
    first = core.event("status", **ACC)["state"]
    assert first in ("connecting", "auth_required")
    if first == "connecting":  # the SDK's default when start itself reported nothing
        assert core.event("status", state="auth_required")
    reply = position(core, lambda f: f.get("id") == rid and "method" not in f)
    assert all(f.get("params", {}).get("account_id") != "acc#1" for f in frames(core)[:reply])

    # Interactive auth: phone -> code (wrong, re-asked with error) -> secrets.set -> connected.
    assert core.call("auth.begin", {**ACC, "method": "phone_code"}) == {}
    step = core.event("auth.step")["step"]
    assert step["kind"] == "phone" and step["flow"] == "sign_in"
    core.call("auth.submit", {**ACC, "flow": "sign_in", "step_id": step["step_id"], "value": "+4915"})
    step = core.event("auth.step")["step"]
    assert step["kind"] == "code" and step["hint"] == "sent to +4915" and step["can_resend"] is True
    core.call("auth.submit", {**ACC, "flow": "sign_in", "step_id": step["step_id"], "value": "0"})
    step = core.event("auth.step")["step"]
    assert step["error"] == "Wrong code"
    core.call("auth.submit", {**ACC, "flow": "sign_in", "step_id": "stale", "value": "x"})
    core.call("auth.submit", {**ACC, "flow": "sign_in", "step_id": step["step_id"], "value": "12345"})
    assert core.event("status", state="connected")
    assert core.secrets["acc#1"] == {"session": "sess-+4915"}

    # Typed method calls.
    res = core.call("messages.fetch_recent", {**ACC, "chat_id": "c1", "limit": 3})
    assert [m["id"] for m in res["messages"]] == ["0", "1", "2"] and res["next_cursor"] == "older"
    assert res["messages"][0]["sent_at"] == "2026-10-19T14:03:27.512Z"
    res = core.call("messages.send", {**ACC, "chat_id": "c1", "text": "hi", "silent": True})
    assert res["message"]["text"] == "hi" and res["message"]["silent"] is True
    assert "edited" not in res["message"]  # absent = unchanged (spec 8.4)
    upsert = [e for e in core.all_events if e["method"] == "messages.upsert"]
    assert upsert and "id" in upsert[0], "bulk event was sent as a request (ack=True)"
    rid = core.send_request("x-echo.whoami", ACC)
    assert core.response(rid)["result"] == {"account": "acc#1", "request": rid}

    # Error paths.
    err = core.error("messages.send", {**ACC, "chat_id": "c1", "text": "flood"})
    assert err == {"code": -32003, "message": "Too many requests", "data": {"code": "FLOOD_WAIT", "seconds": 37}}
    err = core.error("messages.send", {**ACC, "chat_id": "c1", "text": "boom"})
    assert err["data"]["code"] == "INTERNAL" and "kaboom" in err["message"]
    err = core.error("messages.edit", {**ACC, "chat_id": "c1", "message_id": "m1", "text": "x"})
    assert err["data"] == {"code": "NOT_SUPPORTED", "capability": "edit"} and err["code"] == -32001
    err = core.error("messages.send", {**ACC, "text": "x"}, check=False)
    assert err["data"] == {"code": "INVALID_PARAMS", "field": "chat_id"}
    err = core.error("no.such_method", {})
    assert err["code"] == -32601 and err["data"]["code"] == "METHOD_NOT_FOUND"
    core.send({"jsonrpc": "2.0", "method": "no.such_notification", "params": {}})  # ignored

    # Cancellation: a download reports progress, stays busy, ping still answers, cancel -> CANCELLED.
    rid = core.send_request("media.download", {**ACC, "chat_id": "c1", "message_id": "m1", "what": "full"})
    prog = core.event("transfer.progress")
    assert prog == {"account_id": "acc#1", "request_id": rid, "direction": "download", "chat_id": "c1",
                    "message_id": "m1", "done_bytes": 10, "total_bytes": 100}
    assert core.call("ping") == {}
    core.cancel(rid)
    r = core.response(rid)
    assert r["error"]["code"] == -32800 and r["error"]["data"]["code"] == "CANCELLED"
    core.cancel(rid)  # already answered: ignored
    assert core.event("status", detail="download aborted")

    # A second account runs independently; settings_schema defaults apply (spec 6.1).
    start(core, "acc#2", secrets={"session": "s"})
    me = core.event("status", account_id="acc#2", state="connected")["me"]
    assert me == {"id": "me", "display_name": "Echo Me"}

    # account.stop: status(disconnected) goes out before the reply, nothing after (spec 4.2).
    rid = core.send_request("account.stop", {"account_id": "acc#2"})
    core.response(rid)
    down = position(core, lambda f: f.get("method") == "status" and f["params"]["account_id"] == "acc#2"
                    and f["params"]["state"] == "disconnected")
    assert down < position(core, lambda f: f.get("id") == rid and "method" not in f)
    err = core.error("messages.send", {"account_id": "acc#2", "chat_id": "c", "text": "x"})
    assert err["data"]["code"] == "UNKNOWN_ACCOUNT"
    assert core.error("account.stop", {"account_id": "acc#2"})["data"]["code"] == "UNKNOWN_ACCOUNT"

    # Orderly shutdown: stops acc#1, answers {}, exits 0.
    rid = core.send_request("shutdown")
    assert core.event("status", **ACC)["state"] == "disconnected"
    assert core.response(rid)["result"] == {}
    assert core.wait_exit() == 0
    err_log = core.stderr_text
    assert "a stray print must land on stderr" in err_log
    assert "tok-very-secret" not in err_log and "sess-+4915" not in err_log


def test_eof_exits_promptly(core):
    start(core)
    core.close_stdin()
    t = time.monotonic()
    assert core.wait_exit() == 0
    assert time.monotonic() - t < 5


def test_eof_answers_inflight_requests(core):
    # Spec 2.5: responses of requests that finish within the exit budget still go out.
    start(core)
    slow = core.send_request("x-echo.slow", {**ACC, "seconds": 0.5})
    shut = core.send_request("shutdown")
    core.close_stdin()
    assert core.response(slow)["result"] == {"slept": 0.5}
    assert core.response(shut)["result"] == {}
    assert core.wait_exit() == 0


def test_eof_drops_requests_past_the_budget(core):
    start(core)
    slow = core.send_request("x-echo.slow", {**ACC, "seconds": 30})
    core.close_stdin()
    t = time.monotonic()
    assert core.wait_exit() == 0 and time.monotonic() - t < 5
    assert not any(f.get("id") == slow for f in frames(core))


@pytest.mark.parametrize("core", [["legacy"]], indirect=True)
def test_new_core_accepts_legacy_plugin_without_chat_get(core):
    result = core.initialize(api_version="1.1.0")
    assert result["api_version"] == "1.0.0" and "chat.get" not in result["capabilities"]
    start(core)
    assert core.call("ping") == {}
    error = core.error("chats.get", {**ACC, "chat_id": "c"})
    assert error["data"] == {"code": "NOT_SUPPORTED", "capability": "chat.get"}


@pytest.mark.parametrize("core", [["modern"]], indirect=True)
def test_legacy_core_can_use_common_methods_with_new_plugin(core):
    result = core.initialize(api_version="1.0.0")
    assert result["api_version"] == "1.1.0" and result["capabilities"]["chat.get"] == {}
    start(core)
    assert core.call("ping") == {}


@pytest.mark.parametrize("core", [["modern"]], indirect=True)
def test_chat_get_returns_only_observed_fields(core):
    core.initialize()
    start(core)
    assert core.call("chats.get", {**ACC, "chat_id": "c", "future_option": True}) == {
        "conversation": {"id": "c", "kind": "dm", "pinned": False}}


def test_major_version_mismatch_is_answered(core):
    # Spec 4.1: the plugin answers normally; the core decides.
    res = core.initialize(api_version="2.0.0")
    assert res["api_version"] == API_VERSION and "send.silent" in res["capabilities"]


def test_before_initialize_and_twice(core):
    assert core.error("ping")["code"] == -32600
    # Notifications before initialize are ignored, not processed.
    core.cancel(1)
    core.initialize()
    err = core.error("initialize", {"api_version": API_VERSION, "core_version": "0", "plugin_dir": ".",
                                    "locale": "en", "log_level": "info"})
    assert err["code"] == -32600 and err["data"]["code"] == "INVALID_REQUEST"
    assert core.shutdown() == 0
    assert "$/cancelRequest before initialize ignored" in core.stderr_text


def test_account_start_twice_is_invalid_params(core):
    start(core, secrets={"session": "s"})
    core.event("status", state="connected")
    err = core.error("account.start", core.start_params("acc#1"))
    assert err["code"] == -32602 and err["data"] == {"code": "INVALID_PARAMS", "field": "account_id"}
    # The running account is untouched: it still serves requests and emits events.
    core.call("messages.send", {**ACC, "chat_id": "c1", "text": "still here"})
    assert core.event("messages.upsert")["messages"][0]["text"] == "still here"


def test_gated_params_from_the_registry(core):
    start(core, secrets={"session": "s"})
    base = {**ACC, "chat_id": "c1", "text": "x"}
    assert core.call("messages.send", {**base, "silent": None}, check=False)["message"]["text"] == "x"
    err = core.error("messages.send", {**base, "reply_to": {"message_id": "1", "quote_text": "q"}})
    assert err["data"] == {"code": "NOT_SUPPORTED", "capability": "quote_reply"}
    err = core.error("messages.send", {**base, "entities": [{"type": "bold", "offset": 0, "length": 1}]})
    assert err["data"] == {"code": "NOT_SUPPORTED", "capability": "send.entities"}
    err = core.error("messages.send", {**base, "schedule_at": "2027-01-01T00:00:00.000Z"})
    assert err["data"] == {"code": "NOT_SUPPORTED", "capability": "send.schedule"}


def test_gated_param_check_runs_before_the_handler():
    from contact_hub_sdk import METHODS

    react = METHODS["reactions.set"]
    p = {"account_id": "a", "chat_id": "c", "message_id": "m",
         "reactions": [{"emoji": "👍"}, {"custom_emoji_id": "5"}]}
    assert [g.capability for g in react.missing_capabilities(p, lambda c: False)] == ["reactions.custom"]
    assert not react.missing_capabilities({**p, "reactions": [{"emoji": "👍"}]}, lambda c: False)
    edit = METHODS["messages.edit"]
    p = {"account_id": "a", "chat_id": "c", "message_id": "m", "text": "x",
         "entities": [{"type": "bold", "offset": 0, "length": 1}]}
    assert [g.capability for g in edit.missing_capabilities(p, lambda c: False)] == ["send.entities"]
    delete = METHODS["messages.delete"]
    p = {"account_id": "a", "chat_id": "c", "message_ids": ["1"]}
    assert not delete.missing_capabilities({**p, "revoke": True}, lambda c: False)
    assert not delete.missing_capabilities({**p, "revoke": 0}, lambda c: False)  # not the JSON value false
    assert [g.capability for g in delete.missing_capabilities({**p, "revoke": False}, lambda c: False)] \
        == ["delete.for_me"]
    send_file = METHODS["messages.send_file"]
    p = {"account_id": "a", "chat_id": "c", "path": "/x", "kind": "voice"}
    assert [g.capability for g in send_file.missing_capabilities(p, lambda c: c != "send.voice")] \
        == ["send.voice"]


def test_core_error_with_unknown_code_is_kept(core):
    # Spec 13: unknown codes are INTERNAL on the wire; the string is kept.
    core.auto_answer = False
    start(core)
    rid = core.send_request("x-echo.set_secret", ACC)
    req = core.request("secrets.set")
    core.answer(req, error={"code": -32010, "message": "new", "data": {"code": "SOMETHING_NEW", "x": 1}})
    err = core.response(rid)["error"]
    assert err["code"] == -32603 and err["data"] == {"code": "SOMETHING_NEW", "x": 1}
    e = PluginError.from_wire({"code": -32010, "message": "new", "data": {"code": "SOMETHING_NEW"}})
    assert e.code == "SOMETHING_NEW" and e.numeric == -32603 and "new" in str(e)
    assert PluginError.from_wire({"code": -32099, "message": "?"}).code == "INTERNAL"


def test_plugin_to_core_request_times_out(core):
    # Spec 4.6/12: on expiry the plugin cancels the request and the caller gets TIMEOUT.
    core.auto_answer = False
    start(core)
    rid = core.send_request("x-echo.set_secret", {**ACC, "timeout": 0.3})
    req = core.request("secrets.set")
    err = core.response(rid)["error"]
    assert err["data"]["code"] == "TIMEOUT"
    assert core.cancelled == [req["id"]]


def test_vendor_method_names_are_checked():
    from contact_hub_sdk import method

    with pytest.raises(ValueError, match="x-<plugin-id>"):
        method("foo.bar")
    with pytest.raises(ValueError):
        method("x-.bar")
    method("x-echo.ok")
    method("messages.send")


def test_default_timeout_comes_from_the_registry():
    from contact_hub_sdk import Plugin
    from contact_hub_sdk.registry import DEFAULT_TIMEOUT_SECONDS
    from contact_hub_sdk.transport import DEFAULT_OUTBOUND_TIMEOUT

    reg = json.loads((SCHEMAS / "methods" / "registry.json").read_text())
    assert DEFAULT_TIMEOUT_SECONDS == reg["default_timeout_seconds"] == DEFAULT_OUTBOUND_TIMEOUT
    assert Plugin.request_timeout == DEFAULT_TIMEOUT_SECONDS


def test_settings_defaults_without_a_core_filling_them(core):
    # Spec 6.1: the SDK applies plugin.json defaults itself, so `settings: {}` behaves the same.
    core.initialize()
    p = {**core.start_params("acc#3", secrets={"session": "s"}), "settings": {}}
    assert core.call("account.start", p) == {}
    assert core.event("status", state="connected")["me"]["display_name"] == "Echo Me"


def test_stop_right_after_start_flushes_the_start_events_first(core):
    core.initialize()
    start_id = core.send_request("account.start", core.start_params("acc#1", secrets={"session": "s"}))
    stop_id = core.send_request("account.stop", ACC)
    core.response(stop_id)
    order = [(f.get("id"), f.get("params", {}).get("state")) for f in frames(core)
             if f.get("id") in (start_id, stop_id) and "method" not in f or f.get("method") == "status"]
    assert order[0] == (start_id, None) and order[-1] == (stop_id, None)
    assert order[1][0] is None and order[1][1] in ("connecting", "connected")
    assert order[-2] == (None, "disconnected")
