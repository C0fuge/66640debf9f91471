import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from contact_hub_sdk import errors as rpc, types as dto
from contact_hub_sdk.registry import METHODS
from contact_hub_sdk.wire import to_wire
from telethon import functions, types as tl
from telethon.extensions import BinaryReader

from stub import NOW, row
from telegram_plugin import mentions
from telegram_plugin.plugin import TelegramPlugin

CHAT = "-1000000000008"
SCOPE = functions.channels.GetChannelsRequest
LIST = functions.messages.GetUnreadMentionsRequest
READ = functions.messages.ReadMentionsRequest


def account(**kwargs):
    return SimpleNamespace(data={}, running=True, state="connected", ghost=False, **kwargs)


def group(**kwargs):
    return tl.Channel(8, "Mention group", tl.ChatPhotoEmpty(), NOW, megagroup=True, **kwargs)


def incoming(mid, topic=None, **kwargs):
    reply = tl.MessageReplyHeader(reply_to_msg_id=topic, forum_topic=True) if topic is not None else None
    return row(mid, tl.PeerChannel(8), reply_to=reply, out=False, mentioned=True, media_unread=True, **kwargs)


def params(topic=None, limit=2):
    return dto.ChatTopicLimitParams(account_id="a", chat_id=CHAT, topic_id=topic, limit=limit)


def client(replies, entity=None, peer=None):
    calls = []
    async def respond(request):
        decoded = BinaryReader(bytes(request)).tgread_object()
        calls.append(decoded)
        if isinstance(decoded, SCOPE):
            assert decoded.id == [tl.InputChannel(8, 0)]
            return tl.messages.Chats([entity or group(forum=True)])
        result = replies.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result
    value = AsyncMock(side_effect=respond)
    value.get_input_entity.return_value = peer or tl.InputPeerChannel(8, 0)
    return value, calls


def test_oldest_batch_is_exact_bounded_and_has_no_cursor_count_or_events():
    async def exercise():
        owner = account()
        value, calls = client([tl.messages.MessagesSlice(900, [incoming(10), incoming(2)], [], [], [], inexact=True),
                               tl.messages.MessagesSlice(900, [], [], [], [], inexact=True)])
        assert to_wire(await mentions.list_unread(owner, value, params(limit=3))) == {"message_ids": ["2", "10"]}
        assert list(map(type, calls)) == [SCOPE, LIST]
        req = calls[-1]
        assert (req.peer, req.top_msg_id, req.offset_id, req.add_offset, req.limit, req.max_id, req.min_id) == (
            tl.InputPeerChannel(8, 0), None, 1, -3, 3, 0, 0)
        assert to_wire(await mentions.list_unread(owner, value, params())) == {"message_ids": []}
        assert list(map(type, calls)) == [SCOPE, LIST, SCOPE, LIST]
        assert owner.data["mentions_work"]["active"] == 0
    asyncio.run(exercise())


def test_exact_forum_topic_and_general_without_header():
    async def exercise():
        owner = account()
        value, calls = client([tl.messages.Messages([incoming(45, 42)], [], [], []),
                               tl.messages.Messages([incoming(10)], [], [], []),
                               tl.messages.Messages([incoming(11, 43)], [], [], [])])
        assert (await mentions.list_unread(owner, value, params("42"))).message_ids == ["45"]
        assert calls[-1].top_msg_id == 42
        assert (await mentions.list_unread(owner, value, params("1"))).message_ids == ["10"]
        assert calls[-1].top_msg_id == 1
        with pytest.raises(rpc.Internal):
            await mentions.list_unread(owner, value, params("42"))
    asyncio.run(exercise())


def test_basic_group_scope_uses_the_exact_group_and_has_no_topics():
    async def exercise():
        calls = []
        async def respond(request):
            req = BinaryReader(bytes(request)).tgread_object()
            calls.append(req)
            if isinstance(req, functions.messages.GetChatsRequest):
                assert req.id == [8]
                return tl.messages.Chats([tl.Chat(8, "Basic", tl.ChatPhotoEmpty(), 1, NOW, 1)])
            assert req.peer == tl.InputPeerChat(8) and req.top_msg_id is None
            return tl.messages.AffectedHistory(1, 1, 0)
        value = AsyncMock(side_effect=respond)
        value.get_input_entity.return_value = tl.InputPeerChat(8)
        p = dto.ChatTopicParams(account_id="a", chat_id="-8")
        await mentions.read(account(), value, p)
        assert list(map(type, calls)) == [functions.messages.GetChatsRequest, READ]
        p.topic_id = "1"
        with pytest.raises(rpc.InvalidParams):
            await mentions.read(account(), value, p)
        assert list(map(type, calls)) == [functions.messages.GetChatsRequest, READ, functions.messages.GetChatsRequest]
    asyncio.run(exercise())


@pytest.mark.parametrize("topic", [None, "42"])
@pytest.mark.parametrize("kind", ["monoforum", "broadcast", "minimal", "foreign", "forbidden", "nonforum"])
def test_scope_rejects_unsupported_or_unverified_entity_before_read(topic, kind):
    async def exercise():
        entity = group(forum=kind != "nonforum")
        if kind == "monoforum": entity.monoforum = True
        elif kind == "broadcast": entity.megagroup = False
        elif kind == "minimal": entity.min = True
        elif kind == "foreign": entity.id = 9
        elif kind == "forbidden": entity = tl.ChannelForbidden(8, 0, "Unavailable")
        value, calls = client([], entity=entity)
        if topic is None and kind == "nonforum":
            value, calls = client([tl.messages.AffectedHistory(1, 1, 0)], entity=entity)
            await mentions.read(account(), value, params())
            assert list(map(type, calls)) == [SCOPE, READ]
            return  # Ordinary whole-group mentions do not require forum mode.
        with pytest.raises(rpc.PluginError):
            await mentions.read(account(), value, params(topic))
        assert list(map(type, calls)) == [SCOPE]
    asyncio.run(exercise())


@pytest.mark.parametrize("peer", [tl.InputPeerUser(17, 0), tl.InputPeerUser(18, 0)])
def test_ordinary_dm_and_bot_forum_are_unadvertised_scopes(peer):
    async def exercise():
        value, calls = client([], peer=peer)
        p = dto.ChatTopicParams(account_id="a", chat_id=str(peer.user_id))
        with pytest.raises(rpc.NotSupported):
            await mentions.read(account(), value, p)
        assert not calls
    asyncio.run(exercise())


@pytest.mark.parametrize("kind", ["outgoing", "not-mentioned", "already-read", "foreign", "duplicate", "empty", "zero", "oversized", "not-modified"])
def test_invalid_or_foreign_batches_never_become_partial_success(kind):
    async def exercise():
        sample = incoming(2)
        if kind == "outgoing": sample.out = True
        elif kind == "not-mentioned": sample.mentioned = False
        elif kind == "already-read": sample.media_unread = False
        elif kind == "foreign": sample.peer_id = tl.PeerChannel(9)
        elif kind == "zero": sample.id = 0
        rows = [sample]
        if kind == "duplicate": rows.append(sample)
        elif kind == "oversized": rows.extend([incoming(3), incoming(4)])
        elif kind == "empty": rows = [tl.MessageEmpty(id=2, peer_id=None)]
        result = tl.messages.MessagesNotModified(2) if kind == "not-modified" else tl.messages.Messages(rows, [], [], [])
        value, _ = client([result])
        with pytest.raises(rpc.Internal):
            await mentions.list_unread(account(), value, params())
    asyncio.run(exercise())


def test_service_mentions_are_valid_and_listing_never_fetches_media():
    async def exercise():
        service = tl.MessageService(id=2, peer_id=tl.PeerChannel(8), date=NOW, action=tl.MessageActionPinMessage(),
                                    mentioned=True, media_unread=True, out=False)
        value, calls = client([tl.messages.Messages([service], [], [], [])])
        assert (await mentions.list_unread(account(), value, params())).message_ids == ["2"]
        assert list(map(type, calls)) == [SCOPE, LIST]
    asyncio.run(exercise())


@pytest.mark.parametrize("field,value", [("limit", 0), ("limit", 201), ("limit", True), ("topic_id", "042"), ("chat_id", "-01000000000008")])
def test_invalid_parameters_do_not_dispatch(field, value):
    async def exercise():
        p = params(); setattr(p, field, value)
        transport, calls = client([])
        with pytest.raises(rpc.InvalidParams):
            await mentions.list_unread(account(), transport, p)
        assert not calls
    asyncio.run(exercise())


@pytest.mark.parametrize("offsets,success", [([5, 2, 0], True), ([0], True), ([3, 3], False), ([2, 4], False),
                                           ([-1], False), ([True], False), (list(range(127, -1, -1)), True),
                                           (list(range(129, 1, -1)), False)])
def test_read_continuation_is_acknowledged_bounded_and_not_a_history_receipt(offsets, success):
    async def exercise():
        replies = [tl.messages.AffectedHistory(1, 1, n) for n in offsets]
        value, calls = client(replies)
        owner = account()
        if success:
            assert await mentions.read(owner, value, params("42")) is None
        else:
            with pytest.raises(rpc.NetworkError) as err:
                await mentions.read(owner, value, params("42"))
            assert err.value.data == {"outcome": "unknown", "retry_safe": False}
        assert list(map(type, calls)) == [SCOPE, *[READ] * len(offsets)]
        assert all(req.top_msg_id == 42 for req in calls[1:])
        assert owner.data["mentions_work"]["active"] == 0 and not owner.data["mentions_work"]["lock"].locked()
    asyncio.run(exercise())


@pytest.mark.parametrize("failure", [True, tl.UpdatesTooLong(), OSError("private failure"), TimeoutError(), asyncio.CancelledError()])
def test_post_dispatch_failures_are_unknown_and_never_replayed(failure):
    async def exercise():
        value, calls = client([tl.messages.AffectedHistory(1, 1, 2), failure])
        with pytest.raises(rpc.NetworkError) as err:
            await mentions.read(account(), value, params())
        assert err.value.data == {"outcome": "unknown", "retry_safe": False}
        assert list(map(type, calls)) == [SCOPE, READ, READ]
    asyncio.run(exercise())


def test_ghost_initial_and_partial_switch_never_issue_an_extra_mutation():
    async def exercise():
        owner = account(); owner.ghost = True
        value, calls = client([])
        with pytest.raises(rpc.NotSupported):
            await mentions.read(owner, value, params())
        assert not calls
        # Read-only listing stays available; this does not promise online privacy.
        value, calls = client([tl.messages.Messages([], [], [], [])])
        assert (await mentions.list_unread(owner, value, params())).message_ids == []
        owner.ghost = False
        value, calls = client([tl.messages.AffectedHistory(1, 1, 2)])
        previous = value.side_effect
        async def respond(req):
            result = await previous(req)
            if isinstance(req, READ): owner.ghost = True
            return result
        value.side_effect = respond
        with pytest.raises(rpc.NetworkError) as err:
            await mentions.read(owner, value, params())
        assert err.value.data["outcome"] == "unknown"
        assert list(map(type, calls)) == [SCOPE, READ]
    asyncio.run(exercise())


def test_four_admitted_operations_bound_lock_waiters_and_cancellation_releases_them():
    async def exercise():
        owner = account()
        value, calls = client([])
        reached = asyncio.Event()
        previous = value.side_effect
        async def respond(req):
            if isinstance(req, READ):
                calls.append(req); reached.set(); await asyncio.Future()
            return await previous(req)
        value.side_effect = respond
        first = asyncio.create_task(mentions.read(owner, value, params()))
        await reached.wait()
        queued = [asyncio.create_task(mentions.read(owner, value, params(str(n)))) for n in (1, 2, 3)]
        await asyncio.sleep(0)
        assert owner.data["mentions_work"]["active"] == 4
        with pytest.raises(rpc.Unavailable):
            await mentions.list_unread(owner, value, params())
        for pending in queued: pending.cancel()
        assert all(isinstance(result, asyncio.CancelledError) for result in await asyncio.gather(*queued, return_exceptions=True))
        assert owner.data["mentions_work"]["active"] == 1
        first.cancel()
        with pytest.raises(rpc.NetworkError): await first
        assert owner.data["mentions_work"]["active"] == 0
        assert list(map(type, calls)) == [SCOPE, READ]
    asyncio.run(exercise())


def test_plugin_deadline_covers_queued_scope_and_dispatched_read(monkeypatch):
    async def exercise():
        registry = {**METHODS, "mentions.read": replace(METHODS["mentions.read"], timeout_seconds=0.05)}
        monkeypatch.setattr("telegram_plugin.plugin.METHODS", registry)
        plugin = TelegramPlugin()
        value, calls = client([])
        owner = account(); owner.data["client"] = value
        state = {"active": 0, "lock": asyncio.Lock()}; owner.data["mentions_work"] = state
        async with state["lock"]:
            with pytest.raises(rpc.RpcTimeout) as err:
                await plugin.read_mentions(owner, params())
            assert "outcome" not in err.value.data
        assert not calls and state["active"] == 0
        previous = value.side_effect
        async def respond(req):
            if isinstance(req, READ):
                calls.append(req); await asyncio.Future()
            return await previous(req)
        value.side_effect = respond
        with pytest.raises(rpc.NetworkError) as err:
            await plugin.read_mentions(owner, params())
        assert err.value.data["outcome"] == "unknown"
        assert list(map(type, calls)) == [SCOPE, READ] and state["active"] == 0
    asyncio.run(exercise())


def test_account_stop_during_read_only_scope_never_reaches_a_mutation():
    async def exercise():
        for writing in (False, True):
            owner = account()
            value, calls = client([])
            previous = value.side_effect
            async def respond(req):
                result = await previous(req)
                owner.running = False
                return result
            value.side_effect = respond
            with pytest.raises(rpc.Unavailable) as err:
                if writing:
                    await mentions.read(owner, value, params())
                else:
                    await mentions.list_unread(owner, value, params())
            assert "outcome" not in err.value.data
            assert list(map(type, calls)) == [SCOPE]
            assert owner.data["mentions_work"]["active"] == 0
    asyncio.run(exercise())
