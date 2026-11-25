import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from contact_hub_sdk import errors as rpc, types as dto
from contact_hub_sdk.wire import to_wire
from telethon import functions, types as tl
from telethon.extensions import BinaryReader

from stub import NOW, row
from telegram_plugin import unread_reactions
from telegram_plugin.mapping import message
from telegram_plugin.plugin import TelegramPlugin

CHAT = "-1000000000008"


def client(peer=None):
    value = AsyncMock()
    value.get_input_entity.return_value = peer or tl.InputPeerUser(17, 0)
    return value


def forum():
    return tl.Channel(id=8, title="Forum fixture", photo=tl.ChatPhotoEmpty(), date=NOW, megagroup=True, forum=True)


def in_topic(mid, topic=42, peer=None):
    return row(mid, peer or tl.PeerChannel(8), reply_to=tl.MessageReplyHeader(reply_to_msg_id=topic, forum_topic=True))


def test_oldest_batch_uses_pinned_offset_pair_and_does_not_guess_pagination():
    async def exercise():
        value = client()
        async def response(req):
            decoded = BinaryReader(bytes(req)).tgread_object()
            assert isinstance(decoded, functions.messages.GetUnreadReactionsRequest)
            assert decoded.peer == tl.InputPeerUser(17, 0)
            assert (decoded.offset_id, decoded.add_offset, decoded.limit, decoded.max_id, decoded.min_id) == (1, -3, 3, 0, 0)
            assert decoded.top_msg_id is None and decoded.saved_peer_id is None
            # A short slice and inexact count do not justify inventing another
            # page, a cursor, a total or a locally-cleared unread counter.
            return tl.messages.MessagesSlice(200, [row(10), row(2)], [], [], [], inexact=True)
        value.side_effect = response
        p = dto.ChatTopicLimitParams(account_id="a", chat_id="17", limit=3)
        result = await unread_reactions.list_unread(value, p)
        assert to_wire(result) == {"message_ids": ["2", "10"]}
        assert value.await_count == 1
        value.side_effect = lambda req: tl.messages.MessagesSlice(200, [], [], [], [], inexact=True)
        assert (await unread_reactions.list_unread(value, p)).message_ids == []
        assert value.await_count == 2, "empty batches do not trigger scans or imply exhaustion"
    asyncio.run(exercise())


def test_forum_and_general_topics_validate_full_peer_and_actual_message_scope():
    async def exercise():
        value = client(tl.InputPeerChannel(8, 0))
        p = dto.ChatTopicLimitParams(account_id="a", chat_id=CHAT, topic_id="42", limit=2)
        async def response(req):
            decoded = BinaryReader(bytes(req)).tgread_object()
            if isinstance(req, functions.channels.GetChannelsRequest):
                assert decoded.id == [tl.InputChannel(8, 0)]
                return tl.messages.Chats([forum()])
            assert decoded.top_msg_id == int(p.topic_id) and decoded.saved_peer_id is None
            rows = [in_topic(45), in_topic(42)] if p.topic_id == "42" else [row(10, tl.PeerChannel(8))]
            return tl.messages.ChannelMessages(1, 100, rows, [], [], [])
        value.side_effect = response
        assert (await unread_reactions.list_unread(value, p)).message_ids == ["42", "45"]
        p.topic_id = "1"
        assert (await unread_reactions.list_unread(value, p)).message_ids == ["10"], "verified General topic may omit its header"
        p.topic_id = "43"
        with pytest.raises(rpc.Internal):
            await unread_reactions.list_unread(value, p)
        assert value.await_count == 6
    asyncio.run(exercise())


def test_bot_forum_topics_use_exact_pinned_user_flag_and_reject_ordinary_dm_topics():
    async def exercise():
        value = client(tl.InputPeerUser(18, 0))
        bot = tl.User(18, first_name="Forum bot", bot=True, bot_info_version=1, bot_forum_view=True)
        async def response(req):
            decoded = BinaryReader(bytes(req)).tgread_object()
            if isinstance(req, functions.users.GetUsersRequest):
                assert decoded.id == [tl.InputUser(18, 0)]
                return [bot]
            assert isinstance(req, functions.messages.GetUnreadReactionsRequest) and req.top_msg_id == 7
            return tl.messages.Messages([in_topic(70, 7, tl.PeerUser(18))], [], [], [])
        value.side_effect = response
        p = dto.ChatTopicLimitParams(account_id="a", chat_id="18", topic_id="7", limit=2)
        assert (await unread_reactions.list_unread(value, p)).message_ids == ["70"]
        bot.bot_forum_view = False
        value.reset_mock()
        with pytest.raises(rpc.InvalidParams) as error:
            await unread_reactions.read(SimpleNamespace(data={}), value, p)
        assert error.value.data["field"] == "topic_id"
        assert value.await_count == 1 and isinstance(value.await_args.args[0], functions.users.GetUsersRequest)
    asyncio.run(exercise())


def test_unavailable_wrong_duplicate_or_oversized_unread_rows_are_not_partial_success():
    async def exercise():
        p = dto.ChatTopicLimitParams(account_id="a", chat_id="17", limit=2)
        value = client()
        for result in [tl.messages.MessagesNotModified(2), tl.messages.Messages([row(2), row(3, tl.PeerUser(18))], [], [], []),
                       tl.messages.Messages([row(2), row(2)], [], [], []), tl.messages.Messages([tl.MessageEmpty(2)], [], [], []),
                       tl.messages.Messages([row(0)], [], [], []), tl.messages.Messages([row(2), row(3), row(4)], [], [], [])]:
            value.return_value = result
            with pytest.raises(rpc.Internal):
                await unread_reactions.list_unread(value, p)
        for limit in [0, 201, True]:
            value.reset_mock(); p.limit = limit
            with pytest.raises(rpc.InvalidParams):
                await unread_reactions.list_unread(value, p)
            value.assert_not_awaited()
    asyncio.run(exercise())


def test_topic_preflight_rejects_foreign_minimal_nonforum_and_monoforum_without_read_write():
    async def exercise():
        p = dto.ChatTopicParams(account_id="a", chat_id=CHAT, topic_id="42")
        value = client(tl.InputPeerChannel(8, 0))
        for mode, error in [("foreign", rpc.Internal), ("minimal", rpc.Internal), ("nonforum", rpc.InvalidParams), ("monoforum", rpc.NotSupported)]:
            entity = forum()
            if mode == "foreign": entity.id = 9
            elif mode == "minimal": entity.min = True
            elif mode == "nonforum": entity.forum = False
            else: entity.monoforum = True
            value.return_value = tl.messages.Chats([entity]); value.reset_mock()
            with pytest.raises(error):
                await unread_reactions.read(SimpleNamespace(data={}), value, p)
            assert value.await_count == 1 and isinstance(value.await_args.args[0], functions.channels.GetChannelsRequest)
        value.reset_mock(); p.topic_id = "042"
        with pytest.raises(rpc.InvalidParams):
            await unread_reactions.read(SimpleNamespace(data={}), value, p)
        value.assert_not_awaited(); value.get_input_entity.assert_not_awaited()
    asyncio.run(exercise())


def test_read_continues_only_after_decreasing_positive_ack_and_never_resets_reactions():
    async def exercise():
        account, value = SimpleNamespace(data={}), client()
        p = dto.ChatTopicParams(account_id="a", chat_id="17")
        offsets = [5, 2, 0]
        async def response(req):
            decoded = BinaryReader(bytes(req)).tgread_object()
            assert isinstance(decoded, functions.messages.ReadReactionsRequest)
            assert decoded.peer == tl.InputPeerUser(17, 0) and decoded.top_msg_id is None and decoded.saved_peer_id is None
            return tl.messages.AffectedHistory(1, 1, offsets.pop(0))
        value.side_effect = response
        assert await unread_reactions.read(account, value, p) is None
        assert value.await_count == 3 and not account.data["reaction_lock"].locked()
        value.side_effect = [tl.messages.AffectedHistory(1, 1, 3), tl.messages.AffectedHistory(1, 1, 3)]
        with pytest.raises(rpc.NetworkError) as error:
            await unread_reactions.read(account, value, p)
        assert error.value.data == {"outcome": "unknown", "retry_safe": False}
        value.side_effect = [tl.messages.AffectedHistory(1, 1, 2), OSError("private unread failure")]
        value.reset_mock()
        with pytest.raises(rpc.NetworkError) as error:
            await unread_reactions.read(account, value, p)
        assert error.value.data == {"outcome": "unknown", "retry_safe": False}
        assert value.await_count == 2, "unknown continuation outcomes are not replayed"
        raw = row(4, reactions=tl.MessageReactions([tl.ReactionCount(tl.ReactionEmoji("❤️"), 2)], min=True))
        assert "reactions" not in to_wire(message(raw)), "unread methods do not invent a personal selection"
    asyncio.run(exercise())


def test_read_rejects_fake_ack_and_cancellation_releases_scope_lock_without_retry():
    async def exercise():
        account, value = SimpleNamespace(data={}), client()
        p = dto.ChatTopicParams(account_id="a", chat_id="17")
        for result in [True, tl.UpdatesTooLong(), tl.messages.AffectedHistory(1, 1, -1)]:
            value.return_value = result
            with pytest.raises(rpc.NetworkError) as error:
                await unread_reactions.read(account, value, p)
            assert error.value.data == {"outcome": "unknown", "retry_safe": False}
        waiting = asyncio.Event()
        async def response(req):
            waiting.set()
            await asyncio.Future()
        value.side_effect = response; value.reset_mock()
        task = asyncio.create_task(unread_reactions.read(account, value, p))
        await waiting.wait(); task.cancel()
        with pytest.raises(rpc.NetworkError) as error:
            await asyncio.wait_for(task, 1)
        assert error.value.data == {"outcome": "unknown", "retry_safe": False}
        assert value.await_count == 1 and not account.data["reaction_lock"].locked()
    asyncio.run(exercise())


def test_read_budget_is_128_total_rounds_and_zero_on_the_last_round_can_succeed():
    async def exercise():
        assert unread_reactions.MAX_READ_ROUNDS == 128
        account, value = SimpleNamespace(data={}), client()
        p = dto.ChatTopicParams(account_id="a", chat_id="17")
        value.side_effect = [tl.messages.AffectedHistory(1, 1, n) for n in range(129, 1, -1)]
        with pytest.raises(rpc.NetworkError) as error:
            await unread_reactions.read(account, value, p)
        assert value.await_count == 128 and error.value.data == {"outcome": "unknown", "retry_safe": False}
        assert not account.data["reaction_lock"].locked()
        value.reset_mock()
        value.side_effect = [tl.messages.AffectedHistory(1, 1, n) for n in range(127, -1, -1)]
        assert await unread_reactions.read(account, value, p) is None
        assert value.await_count == 128
    asyncio.run(exercise())


def test_real_plugin_handler_keeps_preflight_failures_distinct_and_postdispatch_unknown():
    async def exercise():
        value = client()
        account = SimpleNamespace(state="connected", data={"client": value})
        p = dto.ChatTopicParams(account_id="a", chat_id="17")
        value.return_value = True
        with pytest.raises(rpc.NetworkError) as error:
            await TelegramPlugin().read_reactions(account, p)
        assert error.value.to_wire()["data"] == {"code": "NETWORK", "outcome": "unknown", "retry_safe": False}
        value.reset_mock()
        value.get_input_entity.side_effect = OSError("private preflight error")
        with pytest.raises(rpc.NetworkError) as error:
            await TelegramPlugin().read_reactions(account, p)
        assert error.value.to_wire()["data"] == {"code": "NETWORK"}, "read-only lookup failure must not claim a dispatched write"
        value.assert_not_awaited()
        p.topic_id = "042"
        with pytest.raises(rpc.InvalidParams):
            await TelegramPlugin().read_reactions(account, p)
    asyncio.run(exercise())
