"""Drive parrot.py as a real child process, with the SDK's FakeCore playing the core."""

import sys
from pathlib import Path

import pytest

from contact_hub_sdk.testing import FakeCore

HERE = Path(__file__).resolve().parent
A = {"account_id": "parrot"}  # FakeCore's default account id is the plugin id


@pytest.fixture
def core(tmp_path):
    # CH_DATA_DIR and XDG_* point into tmp_path: a test never touches a real profile.
    with FakeCore([sys.executable, "parrot.py"], plugin_dir=HERE, tmp=tmp_path) as c:
        yield c


def test_capabilities(core):
    core.start(secrets={"session": "s"})
    assert core.capabilities == {"messages.get": True, "send.silent": True, "auth.interactive": True}


def test_sign_in(core):
    core.start()
    core.event("status", state="auth_required")
    assert core.call("auth.begin", {**A, "method": "phone_code"}) == {}
    step = core.event("auth.step")["step"]
    assert step["kind"] == "phone"
    core.call("auth.submit", {**A, "flow": "sign_in", "step_id": step["step_id"], "value": "+15550100"})
    step = core.event("auth.step")["step"]
    assert step["kind"] == "code"
    core.call("auth.submit", {**A, "flow": "sign_in", "step_id": step["step_id"], "value": "0000"})
    step = core.event("auth.step")["step"]
    assert step["error"] == "Wrong code, try again."
    core.call("auth.submit", {**A, "flow": "sign_in", "step_id": step["step_id"], "value": "1234"})
    core.event("status", state="connected")
    assert core.secrets["parrot"] == {"session": "parrot:+15550100"}
    assert core.event("conversations.synced")["chat_ids"] == ["polly"]


def test_history_paging(core):
    core.start(secrets={"session": "s"})
    ids, cursor = [], None
    while True:
        page = core.call("messages.fetch_recent", {**A, "chat_id": "polly", "limit": 20,
                                                   **({"cursor": cursor} if cursor else {})})
        ids += [m["id"] for m in page["messages"]]
        cursor = page.get("next_cursor")
        if cursor is None:
            break
    assert ids == [str(i) for i in range(50, 0, -1)]  # newest first, no gaps, no repeats
    got = core.call("messages.get", {**A, "chat_id": "polly", "message_id": "7"})
    assert got["message"]["text"] == "Squawk #7!"


def test_send_and_errors(core):
    core.start(settings={"max_per_minute": 2}, secrets={"session": "s"})
    sent = core.call("messages.send", {**A, "chat_id": "polly", "text": "hi", "silent": True})
    assert sent["message"]["direction"] == "out" and sent["message"]["silent"] is True
    reply = core.event("messages.upsert")["messages"][0]
    assert reply["text"] == "Squawk! hi"

    err = core.error("messages.send", {**A, "chat_id": "nope", "text": "x"})
    assert err["data"]["code"] == "NOT_FOUND"
    err = core.error("messages.send", {**A, "chat_id": "polly", "text": "x",
                                       "schedule_at": "2030-01-01T00:00:00.000Z"})
    assert err["data"] == {"code": "NOT_SUPPORTED", "capability": "send.schedule"}
    err = core.error("messages.edit", {**A, "chat_id": "polly", "message_id": "1", "text": "x"})
    assert err["data"]["code"] == "NOT_SUPPORTED"

    core.call("messages.send", {**A, "chat_id": "polly", "text": "two"})
    err = core.error("messages.send", {**A, "chat_id": "polly", "text": "three"})
    assert err["data"]["code"] == "FLOOD_WAIT" and 0 < err["data"]["seconds"] <= 60


def test_shutdown(core):
    core.start(secrets={"session": "s"})
    assert core.shutdown() == 0
