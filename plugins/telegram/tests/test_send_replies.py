"""Reply confirmation uses only actual pinned TL observations after one send."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from contact_hub_sdk import errors as rpc, types as dto
from telethon import errors as tg, functions, types as tl

from stub import NOW, row
from telegram_plugin import send as sending
from telegram_plugin.session import platform_errors


def params(**values):
    return dto.MessagesSendParams(account_id="synthetic", chat_id="17", text="literal",
                                  reply_to=dto.ReplyTarget(message_id="7"), client_id="one", **values)


def observed(**values):
    values.setdefault("reply_to", tl.MessageReplyHeader(reply_to_msg_id=7))
    values.setdefault("out", True)
    return row(52, **values)


def client(result="short", raw=None):
    async def invoke(request):
        assert isinstance(request, functions.messages.SendMessageRequest)
        assert request.reply_to.reply_to_msg_id == 7
        assert bytes(request)
        if result == "short":
            return tl.UpdateShortSentMessage(52, 1, 1, NOW, out=True)
        updates = [tl.UpdateMessageID(52, request.random_id)]
        if result != "correlation-only":
            updates.append(tl.UpdateNewMessage(raw or observed(), 1, 1))
        if result == "combined":
            return tl.UpdatesCombined(updates, [], [], NOW, 1, 1)
        return tl.Updates(updates, [], [], NOW, 1)
    value = AsyncMock(side_effect=invoke)
    value.get_input_entity.return_value = tl.InputPeerUser(17, 0)
    value.get_messages.return_value = raw or observed()
    return value


def unknown(error):
    assert error.value.code == "NETWORK"
    assert error.value.data == {"outcome": "unknown", "retry_safe": False}


@pytest.mark.parametrize("kind,reads", [("short", 1), ("correlation-only", 1), ("full", 0), ("combined", 0)])
def test_exact_observed_reply_and_known_id_read_bound(kind, reads):
    value = client(kind)
    result = asyncio.run(sending.send(value, params())).message
    assert (result.id, result.reply_to.message_id, result.topic_id) == ("52", "7", None)
    assert result.ext == {"telegram": {"client_id": "one"}}
    assert value.await_count == 1 and value.get_messages.await_count == reads
    if reads:
        value.get_messages.assert_awaited_once_with(tl.InputPeerUser(17, 0), ids=52)


@pytest.mark.parametrize("raw", [None, tl.MessageEmpty(52, peer_id=None), observed(out=False), observed(reply_to=None),
    observed(reply_to=tl.MessageReplyHeader(reply_to_msg_id=8)),
    observed(reply_to=tl.MessageReplyHeader(reply_to_msg_id=True)),
    observed(reply_to=tl.MessageReplyHeader(reply_to_msg_id=7, reply_to_peer_id=tl.PeerUser(18))),
    observed(reply_to=tl.MessageReplyHeader(reply_to_msg_id=7, reply_to_scheduled=True)),
    observed(reply_to=tl.MessageReplyHeader(reply_to_msg_id=7, forum_topic=True, reply_to_top_id=1)),
    row(53, out=True, reply_to=tl.MessageReplyHeader(reply_to_msg_id=7)),
    row(52, tl.PeerUser(18), out=True, reply_to=tl.MessageReplyHeader(reply_to_msg_id=7))])
def test_short_reply_never_copies_or_accepts_missing_foreign_relation(raw):
    value = client()
    value.get_messages.return_value = raw
    with pytest.raises(rpc.PluginError) as error:
        asyncio.run(sending.send(value, params()))
    unknown(error)
    assert value.await_count == value.get_messages.await_count == 1


@pytest.mark.parametrize("failure", [tg.FloodWaitError(None, capture=23), tg.MessageIdInvalidError(None),
    tg.ChatAdminRequiredError(None), OSError("synthetic-private-reply-marker"), asyncio.CancelledError()])
def test_post_send_read_error_cannot_be_a_refusal_or_retry(failure):
    value = client()
    value.get_messages.side_effect = failure
    with pytest.raises(rpc.PluginError) as error, platform_errors():
        asyncio.run(sending.send(value, params()))
    unknown(error)
    assert value.await_count == value.get_messages.await_count == 1
    assert "synthetic-private" not in str(error.value)


def test_reply_read_deadline_is_unknown_and_does_not_spawn_more_work(monkeypatch):
    value = client()
    async def pending(*args, **kwargs):
        await asyncio.Future()
    value.get_messages.side_effect = pending
    monkeypatch.setattr(sending, "METHODS", {"messages.send": SimpleNamespace(timeout_seconds=0.001)})
    with pytest.raises(rpc.PluginError) as error:
        asyncio.run(sending.send(value, params()))
    unknown(error)
    assert value.await_count == value.get_messages.await_count == 1


@pytest.mark.parametrize("kind", ["missing", "conflicting", "malformed", "short-false", "send-cancel"])
def test_unproved_send_identity_never_reads_guessed_id(kind):
    value = client()
    async def invoke(request):
        if kind == "send-cancel":
            raise asyncio.CancelledError()
        if kind == "short-false":
            return tl.UpdateShortSentMessage(52, 1, 1, NOW, out=False)
        ids = [] if kind == "missing" else [tl.UpdateMessageID(52, request.random_id), tl.UpdateMessageID(53, request.random_id)]
        if kind == "malformed":
            ids = [tl.UpdateMessageID(True, request.random_id)]
        return tl.Updates(ids, [], [], NOW, 1)
    value.side_effect = invoke
    with pytest.raises(rpc.PluginError) as error:
        asyncio.run(sending.send(value, params()))
    unknown(error)
    assert value.await_count == 1 and value.get_messages.await_count == 0


def test_full_wrong_relation_is_not_repaired_by_a_second_observation():
    value = client("full", observed(reply_to=None))
    with pytest.raises(rpc.PluginError) as error:
        asyncio.run(sending.send(value, params()))
    unknown(error)
    assert value.await_count == 1 and value.get_messages.await_count == 0


@pytest.mark.parametrize("topic,header,accepted", [("4", tl.MessageReplyHeader(reply_to_msg_id=7, forum_topic=True, reply_to_top_id=4), True),
    ("4", tl.MessageReplyHeader(reply_to_msg_id=7, forum_topic=True, reply_to_top_id=5), False),
    ("1", tl.MessageReplyHeader(reply_to_msg_id=7), False),
    (None, tl.MessageReplyHeader(reply_to_msg_id=7, forum_topic=True, reply_to_top_id=1), False)])
def test_topics_are_observed_exactly_without_general_normalization(topic, header, accepted):
    value = client("full", observed(reply_to=header))
    if accepted:
        assert asyncio.run(sending.send(value, params(topic_id=topic))).message.topic_id == topic
    else:
        with pytest.raises(rpc.PluginError) as error:
            asyncio.run(sending.send(value, params(topic_id=topic)))
        unknown(error)
    assert value.await_count == 1 and value.get_messages.await_count == 0


def test_definite_send_refusal_keeps_existing_translation():
    value = client()
    value.side_effect = tg.FloodWaitError(None, capture=23)
    with pytest.raises(rpc.FloodWait) as error, platform_errors():
        asyncio.run(sending.send(value, params()))
    assert error.value.code == "FLOOD_WAIT" and error.value.data == {"seconds": 23}
    value.get_messages.assert_not_awaited()
