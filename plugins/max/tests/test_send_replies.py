"""Actual pinned acknowledgment models must independently confirm the reply."""
import asyncio
import copy
from types import SimpleNamespace

import pytest
from contact_hub_sdk import PluginError
from pymax.protocol import Opcode

from max_plugin.adapter import PyMaxAdapter
from max_plugin.settings import Settings
from max_plugin.state import PrivateState
from max_plugin import plugin
from test_adapter import raw_message


def ack():
    result = raw_message().model_dump(by_alias=True)
    result["link"] = {"type": "REPLY", "chatId": 10,
                      "message": raw_message().model_copy(update={"id": 19}).model_dump(by_alias=True)}
    return result


@pytest.mark.parametrize("change", ["none", "missing-parent", "foreign-parent", "foreign-sender", "missing-link", "forward",
    "foreign-link-chat", "foreign-source-chat", "wrong-source-id", "bool-id", "float-source-id", "bool-link-chat",
    "comment", "scheduled", "alias", "source-alias", "source-comment", "wrong-cid"])
def test_reply_ack_scope_and_raw_ids_are_not_coerced_or_filled_from_request(tmp_path, change):
    response = copy.deepcopy(ack())
    if change == "missing-parent": response.pop("chatId")
    elif change == "foreign-parent": response["chatId"] = 99
    elif change == "foreign-sender": response["sender"] = 2
    elif change == "missing-link": response.pop("link")
    elif change == "forward": response["link"]["type"] = "FORWARD"
    elif change == "foreign-link-chat": response["link"]["chatId"] = 99
    elif change == "foreign-source-chat": response["link"]["message"]["chatId"] = 99
    elif change == "wrong-source-id": response["link"]["message"]["id"] = 18
    elif change == "bool-id": response["id"] = True
    elif change == "float-source-id": response["link"]["message"]["id"] = 19.0
    elif change == "bool-link-chat": response["link"]["chatId"] = True
    elif change == "comment": response["postId"] = 5
    elif change == "scheduled": response["delayedAttributes"] = {"timeToFire": 7, "notifySender": True}
    elif change == "alias": response["chat_id"] = 10
    elif change == "source-alias": response["link"]["message"]["chat_id"] = 10
    elif change == "source-comment": response["link"]["message"]["postId"] = 5
    elif change == "wrong-cid": response["cid"] = -1

    async def exercise():
        store = PrivateState(tmp_path / "state", "synthetic")
        calls = []
        async def invoke(opcode, payload):
            calls.append((opcode, payload))
            return SimpleNamespace(payload=response)
        adapter = PyMaxAdapter(None, store, Settings(30, None), None)
        adapter.me = 1
        adapter.client = SimpleNamespace(_app=SimpleNamespace(invoke=invoke))
        try:
            if change in {"none", "missing-parent"}:
                message = await adapter.send(10, "literal", 19)
                assert (message.chat_id, message.reply_to.chat_id, message.reply_to.message_id) == ("10", "10", "19")
            else:
                with pytest.raises(PluginError):
                    await adapter.send(10, "literal", 19)
            assert len(calls) == 1 and calls[0][0] == Opcode.MSG_SEND
            assert calls[0][1]["message"]["link"] == {"type": "REPLY", "messageId": 19}
        finally:
            store.release()
    asyncio.run(exercise())


@pytest.mark.parametrize("failure", [OverflowError, OSError])
def test_cached_timestamp_decoder_failure_never_grants_reply_proof(monkeypatch, failure):
    malformed = {"id":"20", "chat_id":"10", "direction":"out", "sent_at":"synthetic-overflow-timestamp",
                 "reply_to":{"message_id":"19", "chat_id":"10"}}
    def decode(*args, **kwargs):
        raise failure("synthetic invalid historical timestamp")
    monkeypatch.setattr(plugin, "from_wire", decode)
    assert not plugin.reply_confirmation(malformed, "10", "19")
