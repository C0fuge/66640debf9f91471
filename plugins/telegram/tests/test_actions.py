import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from contact_hub_sdk import errors as rpc, types as dto
from telethon import errors as tg, functions, types as tl

from stub import NOW, row
from telegram_plugin import actions, activity
from telegram_plugin.session import translated


def client(peer=None):
    value = AsyncMock()
    value.get_input_entity.return_value = peer or tl.InputPeerUser(17, 0)
    value.get_messages.return_value = row(4)
    return value


def test_delete_preflights_whole_batch():
    value = client()
    value.get_messages.side_effect = [row(4), row(5, tl.PeerUser(18))]
    with pytest.raises(rpc.NotFound):
        asyncio.run(actions.delete(value, dto.MessagesDeleteParams(account_id="a", chat_id="17", message_ids=["4", "5"], revoke=True)))
    value.assert_not_awaited()
    value = client(tl.InputPeerChannel(8, 0))
    with pytest.raises(rpc.NotSupported):
        asyncio.run(actions.delete(value, dto.MessagesDeleteParams(account_id="a", chat_id="-1000000000008", message_ids=["4"], revoke=False)))
    value.assert_not_awaited()


def test_activity_requests_preserve_scope_and_clear_drafts():
    async def exercise():
        value = client(tl.InputPeerChannel(8, 0))
        p = dto.ReadSendReceiptParams(account_id="a", chat_id="-1000000000008", max_message_id="20", topic_id="4")
        await activity.read(value, p)
        req = value.await_args.args[0]
        assert isinstance(req, functions.messages.ReadDiscussionRequest) and (req.msg_id, req.read_max_id) == (4, 20)
        await activity.typing(value, dto.TypingSendParams(account_id="a", chat_id=p.chat_id, topic_id="4", action="cancel"))
        req = value.await_args.args[0]
        assert req.top_msg_id == 4 and isinstance(req.action, tl.SendMessageCancelAction)
        value = client()
        await activity.save_draft(value, dto.DraftsSaveParams(account_id="a", chat_id="17", draft=dto.Draft(text="", reply_to="4")))
        req = value.await_args.args[0]
        assert req.message == "" and req.reply_to is None
    asyncio.run(exercise())


def test_forward_preserves_flags_and_correlates_order():
    async def response(req):
        assert req.id == [4, 5] and req.top_msg_id == 7
        assert req.silent is False and req.drop_author is True and req.drop_media_captions is True
        return tl.Updates([tl.UpdateMessageID(21, req.random_id[1]), tl.UpdateMessageID(20, req.random_id[0]),
                           tl.UpdateNewMessage(row(21, out=True), 1, 1), tl.UpdateNewMessage(row(20, out=True), 1, 1),
                           tl.UpdateNewMessage(row(30, out=True), 1, 1)], [], [], NOW, 1)
    value = client()
    value.get_messages.side_effect = [row(4), row(5)]
    value.side_effect = response
    p = dto.MessagesForwardParams(account_id="a", from_chat_id="17", to_chat_id="17", message_ids=["4", "5"],
                                  topic_id="7", drop_author=True, drop_captions=True, silent=False)
    assert [m.id for m in asyncio.run(actions.forward(value, p)).messages] == ["20", "21"]


def test_edit_without_text_preserves_original_entities():
    async def exercise():
        value = client()
        original = row(4, entities=[tl.MessageEntityMentionName(0, 3, 18)])
        value.get_messages.return_value = original
        value.return_value = tl.Updates([], [], [], NOW, 1)
        await actions.edit(value, dto.MessagesEditParams(account_id="a", chat_id="17", message_id="4", no_webpage=False))
        req = value.await_args.args[0]
        assert req.message == "Row 4" and req.entities == original.entities and req.no_webpage is False
    asyncio.run(exercise())


def test_unpin_all_repeats_affected_history():
    value = client()
    value.side_effect = [tl.messages.AffectedHistory(1, 1, 2), tl.messages.AffectedHistory(2, 1, 0)]
    asyncio.run(actions.unpin(value, dto.MessagesUnpinParams(account_id="a", chat_id="17")))
    assert value.await_count == 2


def test_platform_permissions_and_pin_retry():
    for error in (tg.ChatAdminRequiredError, tg.MessageEditTimeExpiredError, tg.PinRestrictedError, tg.UserBannedInChannelError):
        assert isinstance(translated(error(None)), rpc.Forbidden)
    assert isinstance(translated(tg.MessageIdInvalidError(None)), rpc.NotFound)
    value = client()
    value.side_effect = tg.ChatNotModifiedError(None)
    asyncio.run(actions.pin(value, dto.MessagesPinParams(account_id="a", chat_id="17", message_id="4")))
    asyncio.run(actions.unpin(value, dto.MessagesUnpinParams(account_id="a", chat_id="17")))
