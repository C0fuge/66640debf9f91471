import asyncio
import copy
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from contact_hub_sdk import errors as rpc, types as dto
from contact_hub_sdk.wire import to_wire
from telethon import functions, types as tl
from telethon.extensions import BinaryReader

from stub import NOW, row
from telegram_plugin import reactions
from telegram_plugin.mapping import message
from telegram_plugin.plugin import TelegramPlugin
from telegram_plugin.sync import raw_update


def client(raw=None):
    value = AsyncMock()
    value.get_input_entity.return_value = tl.InputPeerUser(17, 0)
    value.get_messages.return_value = raw or row(4)
    value.get_me.return_value = tl.User(100, is_self=True)
    return value


def owner():
    return SimpleNamespace(data={}, reaction_changed=AsyncMock())


def state(*, minimum=False, selected=True):
    return tl.MessageReactions([tl.ReactionCount(tl.ReactionEmoji("❤️"), 3, chosen_order=0 if selected else None),
                               tl.ReactionCount(tl.ReactionCustomEmoji(9007199254740993), 1)], min=minimum,
          recent_reactions=[tl.MessagePeerReaction(tl.PeerUser(18), NOW, tl.ReactionEmoji("❤️"), unread=True),
                            tl.MessagePeerReaction(tl.PeerChannel(9), NOW + timedelta(seconds=1), tl.ReactionEmoji("❤️"), my=True),
                            tl.MessagePeerReaction(tl.PeerChannel(8), NOW + timedelta(seconds=2), tl.ReactionCustomEmoji(9007199254740993))])


def response(raw, result=None, *, topic=None):
    return tl.Updates([tl.UpdateMessageReactions(raw.peer_id, raw.id, result or raw.reactions, top_msg_id=topic)], [], [], NOW, 1)


def test_mapping_preserves_keys_zero_chosen_order_and_external_recent_peers():
    raw = row(4, reactions=state())
    mapped = message(raw)
    assert [(to_wire(value.key), value.count, value.mine) for value in mapped.reactions] == [
        ({"emoji": "❤️"}, 3, True), ({"custom_emoji_id": "9007199254740993"}, 1, False)]
    assert [value.user_id for value in mapped.reactors] == ["-1000000000008", "18"]
    assert mapped.reactors[1].unread and mapped.reactors[0].at > mapped.reactors[1].at
    assert mapped.text == "Row 4"
    raw.reactions.min = True
    assert "reactions" not in to_wire(message(raw)) and "reactors" not in to_wire(message(raw))
    assert to_wire(message(row(4, reactions=tl.MessageReactions([]))))["reactions"] == []


def test_unknown_reaction_or_paid_personal_state_does_not_create_partial_array():
    raw = row(4, reactions=state())
    raw.reactions.results.append(tl.ReactionCount(tl.ReactionEmpty(), 1))
    assert message(raw).reactions is None
    paid = tl.MessageReactions([tl.ReactionCount(tl.ReactionPaid(), 9)],
               top_reactors=[tl.MessageReactor(count=4, my=True, anonymous=True), tl.MessageReactor(count=5, top=True, anonymous=True)])
    value = message(row(4, reactions=paid)).reactions[0]
    assert value.key.paid and value.mine and value.my_count == 4
    paid.top_reactors = None
    assert message(row(4, reactions=paid)).reactions is None


def test_write_preserves_literal_sequence_flags_and_confirms_full_new_set():
    async def exercise():
        raw, value = row(4, reactions=state()), client()
        value.get_messages.return_value = raw
        async def reply(req):
            assert isinstance(req, functions.messages.SendReactionRequest)
            decoded = BinaryReader(bytes(req)).tgread_object()
            assert decoded.peer == tl.InputPeerUser(17, 0) and decoded.msg_id == 4
            assert decoded.big and not decoded.add_to_recent
            assert decoded.reaction == [tl.ReactionEmoji("❤️"), tl.ReactionCustomEmoji(9007199254740993)]
            full = state()
            full.results[1].chosen_order = 1
            return response(raw, full)
        value.side_effect = reply
        p = dto.ReactionsSetParams(account_id="a", chat_id="17", message_id="4", big=True,
              reactions=[dto.ReactionKey(emoji="❤️"), dto.ReactionKey(custom_emoji_id="9007199254740993")])
        result = await reactions.set_reactions(owner(), value, p)
        assert all(item.mine for item in result.message.reactions)
        assert value.await_count == 1
        assert raw.reactions.results[1].chosen_order is None, "combining a result does not mutate the fetched object"
    asyncio.run(exercise())


def test_clear_serializes_an_explicit_empty_vector_and_unknown_write_is_not_replayed():
    async def exercise():
        value, raw = client(), row(4, reactions=state())
        value.get_messages.return_value = raw
        async def cleared(req):
            decoded = BinaryReader(bytes(req)).tgread_object()
            assert decoded.reaction == [] and not decoded.big
            return response(raw, tl.MessageReactions([]))
        value.side_effect = cleared
        p = dto.ReactionsSetParams(account_id="a", chat_id="17", message_id="4", reactions=[], big=False)
        assert (await reactions.set_reactions(owner(), value, p)).message.reactions == []
        value.reset_mock()
        value.side_effect = OSError("synthetic-private-reaction-error")
        with pytest.raises(OSError):
            await reactions.set_reactions(owner(), value, p)
        assert value.await_count == 1 and value.get_messages.await_count == 1
    asyncio.run(exercise())


def test_write_rejects_wrong_chat_duplicates_and_paid_before_dispatch():
    async def exercise():
        p = dto.ReactionsSetParams(account_id="a", chat_id="17", message_id="4", reactions=[])
        value = client(row(4, tl.PeerUser(18)))
        with pytest.raises(rpc.NotFound):
            await reactions.set_reactions(owner(), value, p)
        value.assert_not_awaited()
        for keys in [[dto.ReactionKey(emoji="❤"), dto.ReactionKey(emoji="❤")],
                     [dto.ReactionKey(emoji="❤", custom_emoji_id="1")], [dto.ReactionKey(custom_emoji_id="01")]]:
            p.reactions = keys
            with pytest.raises(rpc.InvalidParams):
                await reactions.set_reactions(owner(), value, p)
        p.reactions = [dto.ReactionKey(paid=True)]
        with pytest.raises(rpc.NotSupported) as error:
            await reactions.set_reactions(owner(), value, p)
        assert error.value.data["capability"] == "paid.react"
        value.assert_not_awaited()
    asyncio.run(exercise())


def test_sparse_write_ack_hydrates_and_never_guesses_a_selection():
    async def exercise():
        raw = row(4, reactions=state(minimum=True))
        value = client(raw)
        p = dto.ReactionsSetParams(account_id="a", chat_id="17", message_id="4", reactions=[dto.ReactionKey(emoji="❤️")])
        async def reply(req):
            if isinstance(req, functions.messages.SendReactionRequest):
                return tl.UpdatesTooLong()
            assert isinstance(req, functions.messages.GetMessagesReactionsRequest) and req.id == [4]
            assert BinaryReader(bytes(req)).tgread_object().peer == tl.InputPeerUser(17, 0)
            return response(raw, state())
        value.side_effect = reply
        assert (await reactions.set_reactions(owner(), value, p)).message.reactions[0].mine
        assert value.await_count == 2 and value.get_messages.await_count == 2
        value.side_effect = lambda req: response(raw, state(minimum=True))
        with pytest.raises(rpc.Internal):
            await reactions.set_reactions(owner(), value, p)
        value.side_effect = lambda req: response(raw, state(selected=False))
        with pytest.raises(rpc.Internal):
            await reactions.set_reactions(owner(), value, p)
    asyncio.run(exercise())


def test_reactor_paging_preserves_explicit_short_and_empty_continuations():
    async def exercise():
        value = client()
        p = dto.ReactionsWhoParams(account_id="a", chat_id="17", message_id="4", limit=2, key=dto.ReactionKey(emoji="❤️"))
        pages = [tl.messages.MessageReactionsList(7, [tl.MessagePeerReaction(tl.PeerUser(18), NOW, tl.ReactionEmoji("❤️"))], [],
                                                 [tl.User(18, first_name="Sample reactor")], next_offset="opaque:a"),
                 tl.messages.MessageReactionsList(7, [], [], [], next_offset="opaque:b"),
                 tl.messages.MessageReactionsList(7, [], [], [])]
        async def reply(req):
            decoded = BinaryReader(bytes(req)).tgread_object()
            assert decoded.reaction.emoticon == "❤️" and decoded.limit == 2 and decoded.id == 4
            assert decoded.offset == [None, "opaque:a", "opaque:b"][3 - len(pages)]
            return pages.pop(0)
        value.side_effect = reply
        first = await reactions.who(value, p)
        assert first.reactors[0].name == "Sample reactor" and first.next_cursor
        p.cursor = first.next_cursor
        second = await reactions.who(value, p)
        assert second.reactors == [] and second.next_cursor
        p.cursor = second.next_cursor
        assert (await reactions.who(value, p)).next_cursor is None
        # Cursor identity includes account, chat, message and the exact key.
        for field, changed in [("account_id", "b"), ("chat_id", "18"), ("message_id", "5"), ("key", dto.ReactionKey(emoji="❤"))]:
            forged = copy.copy(p)
            setattr(forged, field, changed)
            with pytest.raises(rpc.InvalidParams):
                await reactions.who(value, forged)
        assert value.await_count == 3
    asyncio.run(exercise())


def test_reactor_page_rejects_nonadvancing_oversized_or_mismatched_results():
    async def exercise():
        p = dto.ReactionsWhoParams(account_id="a", chat_id="17", message_id="4", limit=1, key=dto.ReactionKey(emoji="❤"))
        value = client()
        for page in [tl.messages.MessageReactionsList(2, [], [], [], next_offset=""),
                     tl.messages.MessageReactionsList(2, [], [], [], next_offset="x" * 4096),
                     tl.messages.MessageReactionsList(2, [tl.MessagePeerReaction(tl.PeerUser(18), NOW, tl.ReactionEmoji("❤️"))], [], []),
                     tl.messages.MessageReactionsList(2, [tl.MessagePeerReaction(tl.PeerUser(18), NOW, tl.ReactionEmoji("❤")),
                                                          tl.MessagePeerReaction(tl.PeerUser(19), NOW, tl.ReactionEmoji("❤"))], [], [])]:
            value.return_value = page
            with pytest.raises(rpc.Internal):
                await reactions.who(value, p)
        p.cursor = reactions.cursor(p, "same")
        value.return_value = tl.messages.MessageReactionsList(2, [], [], [], next_offset="same")
        with pytest.raises(rpc.Internal):
            await reactions.who(value, p)
    asyncio.run(exercise())


def test_available_uses_account_limit_and_distinguishes_chat_policy():
    async def exercise():
        value = client()
        value.get_me.return_value.premium = True
        value.get_input_entity.return_value = tl.InputPeerChannel(8, 0)
        full = tl.ChannelFull(id=8, about="", read_inbox_max_id=0, read_outbox_max_id=0, unread_count=0,
                 chat_photo=tl.PhotoEmpty(0), notify_settings=tl.PeerNotifySettings(), bot_info=[], pts=0, reactions_limit=1)
        async def reply(req):
            assert bytes(req)
            if isinstance(req, functions.help.GetAppConfigRequest):
                return tl.help.AppConfig(1, tl.JsonObject([tl.JsonObjectValue("reactions_user_max_premium", tl.JsonNumber(3))]))
            assert isinstance(req, functions.channels.GetFullChannelRequest)
            return tl.messages.ChatFull(full, [], [])
        value.side_effect = reply
        p = dto.ReactionsAvailableParams(account_id="a", chat_id="-1000000000008")
        full.available_reactions = tl.ChatReactionsAll(allow_custom=True)
        result = await reactions.available(value, p)
        assert result.reactions is None and result.allow_custom and result.max_per_message == 3
        full.available_reactions = tl.ChatReactionsNone()
        result = await reactions.available(value, p)
        assert result.reactions == [] and not result.allow_custom
        full.available_reactions = tl.ChatReactionsSome([tl.ReactionEmoji("❤️"), tl.ReactionCustomEmoji(9007199254740993)])
        result = await reactions.available(value, p)
        assert [to_wire(key) for key in result.reactions] == [{"emoji": "❤️"}, {"custom_emoji_id": "9007199254740993"}]
        assert not result.allow_custom, "listed custom keys do not mean arbitrary custom emoji are allowed"
        full.available_reactions = None
        with pytest.raises(rpc.Internal):
            await reactions.available(value, p)
        full.id = 9
        with pytest.raises(rpc.Internal):
            await reactions.available(value, p)
    asyncio.run(exercise())


def test_live_minimum_refetch_validates_message_and_both_topic_envelopes():
    async def exercise():
        raw = row(4, tl.PeerChannel(8), reactions=state(minimum=True),
                  reply_to=tl.MessageReplyHeader(reply_to_msg_id=42, forum_topic=True))
        value, account = client(raw), owner()
        value.get_input_entity.return_value = tl.InputPeerChannel(8, 0)
        event = tl.UpdateMessageReactions(raw.peer_id, raw.id, raw.reactions, top_msg_id=42)
        value.return_value = response(raw, state(), topic=42)
        await raw_update(account, event, value)
        account.reaction_changed.assert_awaited_once()
        args = account.reaction_changed.await_args
        assert args.args[:2] == ("-1000000000008", "4") and args.args[2][0].mine
        assert [item.user_id for item in args.kwargs["reactors"]] == ["-1000000000008", "18"]
        for outer, inner in [(43, 42), (42, 43)]:
            event.top_msg_id = outer
            value.return_value = response(raw, state(), topic=inner)
            await raw_update(account, event, value)
        assert account.reaction_changed.await_count == 1
        assert raw.reactions.min, "a rejected or combined update never mutates the original envelope"
        value.reset_mock()
        value.return_value = response(raw, state(), topic=43)
        with pytest.raises(rpc.Internal):
            await reactions.set_reactions(account, value, dto.ReactionsSetParams(account_id="a", chat_id="-1000000000008",
                       message_id="4", reactions=[dto.ReactionKey(emoji="❤️")]))
        assert value.await_count == 1, "a contradictory write acknowledgement must not fall through or be replayed"
        event.top_msg_id, event.saved_peer_id = 42, tl.PeerUser(18)
        value.reset_mock()
        await raw_update(account, event, value)
        value.get_messages.assert_not_awaited()
        other = owner()
        value.get_messages.return_value = row(4, tl.PeerUser(18))
        event.saved_peer_id = None
        await raw_update(other, event, value)
        other.reaction_changed.assert_not_awaited()
    asyncio.run(exercise())


def test_messages_get_hydrates_only_minimal_reactions_without_changing_poll_read():
    async def exercise():
        raw = row(4, reactions=state(minimum=True))
        value = client(raw)
        value.return_value = response(raw, state())
        account = SimpleNamespace(state="connected", data={"client": value})
        p = dto.ChatMessageParams(account_id="a", chat_id="17", message_id="4")
        result = await TelegramPlugin().get(account, p)
        assert result.message.reactions[0].mine and value.await_count == 1
        value.reset_mock()
        raw.reactions = None
        assert (await TelegramPlugin().get(account, p)).message.reactions is None
        value.assert_not_awaited()
    asyncio.run(exercise())
