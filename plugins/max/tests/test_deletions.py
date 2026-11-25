"""Synthetic pinned-model transport and schema-checked deletion wire coverage."""
import asyncio
import json
from types import SimpleNamespace

import pytest
from contact_hub_sdk import Forbidden, NotFound, NotSupported, PluginError
from pymax.exceptions import PyMaxError
from pymax.protocol import Opcode
from pymax.types.domain.message import Message

from max_plugin.adapter import PyMaxAdapter
from max_plugin.settings import Settings
from test_wire import sign_in


def raw(mid=21, **changes):
    data = dict(id=mid, chat_id=10, sender=1, time=1767225600123, type="USER", text="Synthetic")
    data.update(changes)
    return Message(**data).model_dump(by_alias=True, exclude_none=True)


def adapter(responses):
    calls = []

    async def invoke(opcode, payload):
        calls.append((opcode, payload))
        response = responses[len(calls) - 1]
        if isinstance(response, BaseException):
            raise response
        return SimpleNamespace(payload=response)

    result = PyMaxAdapter(None, None, Settings(30, None), None)
    result.me = 1
    result.client = SimpleNamespace(_app=SimpleNamespace(invoke=invoke))
    return result, calls


@pytest.mark.parametrize("revoke", [False, True])
def test_actual_delete_payload_and_exact_scoped_acknowledgment(revoke):
    async def run():
        client, calls = adapter([{"messages": [raw(22), raw()]},
                                 {"chatId": "10", "messageIds": ["22", "21", "22"]}])
        await client.deletion_preflight(10, [21, 22], revoke)
        await client.delete(10, [21, 22], revoke)
        assert calls == [(Opcode.MSG_GET, {"chatId": 10, "messageIds": [21, 22]}),
                         (Opcode.MSG_DELETE, {"chatId": 10, "messageIds": [21, 22], "forMe": not revoke})]
    asyncio.run(run())


def test_for_me_accepts_incoming_and_missing_chat_uses_requested_parent():
    async def run():
        client, calls = adapter([{"messages": [raw(sender=2, chat_id=None)]}])
        await client.deletion_preflight(10, [21], False)
        assert len(calls) == 1
    asyncio.run(run())


@pytest.mark.parametrize("extra", [{"chatId": 99}, {"postId": 9}])
def test_preflight_validates_envelope_context_as_well_as_message_context(extra):
    async def run():
        client, calls = adapter([{**extra, "messages": [raw(chat_id=None)]}])
        with pytest.raises(PluginError):
            await client.deletion_preflight(10, [21], False)
        assert len(calls) == 1 and calls[0][0] == Opcode.MSG_GET
    asyncio.run(run())


@pytest.mark.parametrize("rows,error", [
    ([raw(chat_id=99)], PluginError), ([raw(99)], PluginError),
    ([raw(), raw()], PluginError), ([], NotFound), ([raw(sender=None)], NotFound),
    ([raw(sender=2)], Forbidden), ([raw(status="REMOVED")], NotFound),
    ([raw(type="SYSTEM")], NotSupported), ([raw(post_id=9)], NotSupported),
    ([raw(delayed_attributes={"timeToFire": 1, "notifySender": True, "notifyOpponents": True})], NotSupported),
    ([{**raw(), "sender": True}], PluginError),
])
def test_preflight_rejects_missing_foreign_or_unsupported_targets_without_a_write(rows, error):
    async def run():
        client, calls = adapter([{"messages": rows}])
        with pytest.raises(error):
            await client.deletion_preflight(10, [21], True)
        assert [opcode for opcode, _ in calls] == [Opcode.MSG_GET]
    asyncio.run(run())


@pytest.mark.parametrize("payload", [
    {}, None, {"chatId": 99, "messageIds": [21, 22]}, {"chatId": 10, "messageIds": [21]},
    {"chatId": 10, "messageIds": [21, 22, 99]}, {"chatId": 10, "messageIds": []},
    {"chatId": 10, "messageIds": [21, True]}, {"chatId": 10, "messageIds": [21, "022"]},
    {"chatId": 10, "postId": 9, "messageIds": [21, 22]},
])
def test_empty_partial_or_cross_scope_ack_is_never_success(payload):
    async def run():
        client, calls = adapter([payload])
        with pytest.raises((PluginError, PyMaxError)):
            await client.delete(10, [21, 22], True)
        assert len(calls) == 1 and calls[0][0] == Opcode.MSG_DELETE
    asyncio.run(run())


def identity(tmp_path, account="max"):
    return json.loads((tmp_path / f"accounts/{account}/state/identity.json").read_text())


def test_delete_wire_scope_duplicate_normalization_and_for_me(core_factory, tmp_path):
    core = core_factory()
    core.start()
    sign_in(core)
    base = {"account_id": "max", "chat_id": "10", "revoke": True}
    assert core.call("messages.delete", {**base, "message_ids": ["21", "22", "21"]}) == {}
    event = core.event("messages.deleted", account_id="max", chat_id="10")
    assert event["message_ids"] == ["21", "22"]
    assert core.error("messages.delete", {**base, "message_ids": ["20"]})["data"]["code"] == "FORBIDDEN"
    assert core.call("messages.delete", {**base, "message_ids": ["20"], "revoke": False}) == {}
    core.event("messages.deleted", message_ids=["20"])
    for values in [[], ["bad"], ["021"], ["21"] * 101]:
        assert core.error("messages.delete", {**base, "message_ids": values})["data"]["code"] == "INVALID_PARAMS"
    assert core.error("messages.delete", {**base, "message_ids": ["21"], "revoke": 1}, check=False)["data"]["code"] == "INVALID_PARAMS"
    assert core.error("messages.delete", {**base, "chat_id": "99", "message_ids": ["21"]})["data"]["code"] == "NOT_FOUND"
    assert core.error("messages.edit", {"account_id": "max", "chat_id": "10", "message_id": "21", "text": "x"})["data"]["code"] == "NOT_SUPPORTED"
    assert identity(tmp_path)["delete_count"] == 2
    core.start("max#2")
    sign_in(core, "max#2", "qr")
    assert core.call("messages.delete", {**base, "account_id": "max#2", "message_ids": ["21"]}) == {}
    core.event("messages.deleted", account_id="max#2", message_ids=["21"])
    assert identity(tmp_path)["delete_count"] == 2
    assert identity(tmp_path, "max_2")["delete_count"] == 1
    assert core.shutdown() == 0


@pytest.mark.parametrize("scenario", ["delete-partial", "delete-uncertain", "delete-generation", "delete-read-error"])
def test_delete_wire_uncertainty_is_not_replayed_or_synthesized_as_deleted(core_factory, tmp_path, scenario):
    core = core_factory(scenario)
    core.start()
    sign_in(core)
    params = {"account_id": "max", "chat_id": "10", "message_ids": ["21", "22"], "revoke": True}
    error = core.error("messages.delete", params)
    if scenario in {"delete-generation", "delete-read-error"}:
        assert error["data"]["code"] == "UNAVAILABLE"
        assert identity(tmp_path).get("delete_count", 0) == 0
    else:
        assert error["data"] == {"code": "NETWORK", "outcome": "unknown", "retry_safe": False}
        assert identity(tmp_path)["delete_count"] == 1
    assert not any(event.get("method") == "messages.deleted" for event in core.all_events)
    assert "synthetic-private-delete-marker" not in core.stderr_text
    assert core.shutdown() == 0


@pytest.mark.parametrize("scenario,code", [("delete-read-pending", "CANCELLED"), ("delete-pending", "NETWORK")])
def test_cancel_read_is_safe_but_cancel_dispatched_delete_remains_unknown(core_factory, tmp_path, scenario, code):
    core = core_factory(scenario)
    core.start()
    sign_in(core)
    rid = core.send_request("messages.delete", {"account_id": "max", "chat_id": "10", "message_ids": ["21"], "revoke": True})
    core.event("conversations.upsert", lambda p: p["conversations"][0].get("title") in {"Preflight pending", "Deletion dispatched"})
    core.cancel(rid)
    error = core.response(rid)["error"]["data"]
    assert error["code"] == code
    assert identity(tmp_path).get("delete_count", 0) == (code == "NETWORK")
    if code == "NETWORK":
        assert error["outcome"] == "unknown" and error["retry_safe"] is False
    assert core.call("ping") == {}
    assert core.shutdown() == 0
