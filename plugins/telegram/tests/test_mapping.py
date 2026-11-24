import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from contact_hub_sdk import errors as rpc
from contact_hub_sdk import types as dto
from contact_hub_sdk.wire import to_wire
from telethon import events, types as tl
from telethon.sessions import SQLiteSession

from stub import NOW, row
from telegram_plugin.auth import qr_login
from telegram_plugin.mapping import entities_from_tl, entities_to_tl, message, resolve_chat
from telegram_plugin.send import random_id, send
from telegram_plugin.session import disconnect, make_client, session_path
from telegram_plugin.sync import bind_updates, raw_update
from telegram_plugin.plugin import TelegramPlugin


def test_real_telethon_session_without_connect(tmp_path):
    async def check():
        path = session_path(tmp_path / "state")
        client = make_client(path, 1, "0" * 32, None)
        assert isinstance(client.session, SQLiteSession) and not client.is_connected()
        assert client.session.auth_key is None
        await disconnect(client)
        assert path.read_bytes().startswith(b"SQLite format 3")
    asyncio.run(check())


def test_session_refuses_symlink(tmp_path):
    directory = tmp_path / "state"
    directory.mkdir()
    other = tmp_path / "fixture"
    other.write_text("untouched")
    (directory / "telegram.session").symlink_to(other)
    with pytest.raises(OSError):
        session_path(directory)
    assert other.read_text() == "untouched"


def test_threads_entities_and_sparse_updates():
    header = tl.MessageReplyHeader(reply_to_msg_id=71, reply_to_top_id=42, forum_topic=True)
    sample = row(87, tl.PeerChannel(8), reply_to=header)
    sample.message = "🍍jump"
    sample.entities = [tl.MessageEntityBold(2, 4), tl.MessageEntityTextUrl(2, 4, "https://example.org"),
                       tl.MessageEntityPre(2, 4, "bash"), tl.MessageEntityBlockquote(0, 6, collapsed=True)]
    mapped = message(sample)
    assert (mapped.chat_id, mapped.topic_id, mapped.reply_to.message_id) == ("-1000000000008", "42", "71")
    assert [entity.to_dict() for entity in entities_to_tl(mapped.text, mapped.entities)] == [entity.to_dict() for entity in sample.entities]
    assert "media" not in to_wire(mapped) and "reactions" not in to_wire(mapped)
    assert message(row(1, tl.PeerChat(8))).chat_id == "-8"
    assert message(row(2, reply_to=tl.MessageReplyHeader(forum_topic=True))).topic_id == "1"
    assert message(row(2, reply_to=tl.MessageReplyHeader(reply_to_msg_id=1))).topic_id is None
    assert entities_from_tl([tl.MessageEntityMentionName(0, 2, 17)])[0].user_id == "17"
    with pytest.raises(rpc.InvalidParams):
        entities_to_tl("🌷", [dto.Entity(type="bold", offset=0, length=1)])


def test_live_updates_and_partial_payloads():
    async def exercise():
        account = AsyncMock()
        attached = []
        client = SimpleNamespace(add_event_handler=lambda handler, event: attached.append((handler, type(event))))
        bind_updates(account, client)
        handlers = {event: handler for handler, event in attached}
        await handlers[events.NewMessage](SimpleNamespace(message=row(1)))
        await handlers[events.MessageEdited](SimpleNamespace(message=row(1, edit_date=NOW)))
        assert account.messages_upsert.await_args.args[0][0].id == "1"
        assert account.message_edited.await_args.args[0].edited is True
        await raw_update(account, tl.UpdateDeleteMessages([1, 4], 1, 1))
        account.messages_deleted.assert_awaited_with(["1", "4"], chat_id=None)
        await raw_update(account, tl.UpdateDeleteChannelMessages(8, [1], 1, 1))
        account.messages_deleted.assert_awaited_with(["1"], chat_id="-1000000000008")
        await raw_update(account, tl.UpdateReadHistoryInbox(tl.PeerUser(17), 22, 3, 1, 1))
        account.read_inbox.assert_awaited_with("17", "22", topic_id=None, unread_count=3)
        await raw_update(account, tl.UpdateReadChannelDiscussionOutbox(8, 10, 25))
        account.read_outbox.assert_awaited_with("-1000000000008", "25", topic_id="10")
        await raw_update(account, tl.UpdateDraftMessage(tl.PeerUser(17), tl.DraftMessageEmpty(NOW)))
        assert account.draft_changed.await_args.args[1].text == ""
        await raw_update(account, tl.UpdateDraftMessage(tl.PeerChannel(8), tl.DraftMessage("ok", NOW), top_msg_id=18))
        assert account.draft_changed.await_args.kwargs == {"topic_id": "18"}
    asyncio.run(exercise())


def test_send_selects_only_correlated_update():
    params = dto.MessagesSendParams(account_id="a", chat_id="17", text="fixture", client_id="key",
                                    topic_id="4", reply_to=dto.ReplyTarget(message_id="7"))
    async def rpc_call(request):
        assert request.reply_to.reply_to_msg_id == 7 and request.reply_to.top_msg_id == 4
        assert request.random_id == random_id(params)
        return tl.Updates([tl.UpdateMessageID(52, request.random_id), tl.UpdateNewMessage(row(51), 1, 1),
                           tl.UpdateNewMessage(row(52, tl.PeerChannel(4)), 1, 1),
                           tl.UpdateNewMessage(row(52, out=True), 1, 1)], [], [], datetime.now(UTC), 1)
    client = AsyncMock(side_effect=rpc_call)
    client.get_input_entity.return_value = tl.InputPeerUser(17, 0)
    result = asyncio.run(send(client, params))
    assert (result.message.id, result.message.chat_id, result.message.ext) == ("52", "17", {"telegram": {"client_id": "key"}})

def test_qr_refresh_subscribes_before_display():
    async def exercise():
        started = []
        active = SimpleNamespace(url="first", expires=NOW)
        async def wait():
            started.append(active.url)
            if active.url == "first":
                raise TimeoutError()
        async def renew():
            active.url = "second"
        async def show(url, **fields):
            assert started[-1] == url
        active.wait, active.recreate = wait, renew
        client = SimpleNamespace(qr_login=AsyncMock(return_value=active))
        flow = SimpleNamespace(qr=AsyncMock(side_effect=show))
        await qr_login(client, flow)
        assert started == ["first", "second"]
    asyncio.run(exercise())


def test_overlapping_peer_ids_use_typed_session_lookup():
    cached = tl.InputPeerUser(8, 123)
    exact = tl.InputPeerChannel(8, 456)
    lookup = SimpleNamespace(get_input_entity=lambda peer: exact if peer == tl.PeerChannel(8) else cached)
    client = SimpleNamespace(get_input_entity=AsyncMock(return_value=cached), session=lookup)
    assert asyncio.run(resolve_chat(client, "-1000000000008")) == exact


def test_failed_start_closes_session(tmp_path, monkeypatch):
    client = SimpleNamespace(session=SimpleNamespace(close=lambda: closed.append(True)), disconnect=AsyncMock())
    closed = []
    account = SimpleNamespace(state_dir=tmp_path / "state", data={}, secrets={"api_hash": "0" * 32},
                              setting=lambda key, default=None: 1 if key == "api_id" else default)
    plugin = TelegramPlugin(factory=lambda *args: client)
    monkeypatch.setattr("telegram_plugin.plugin.sync.bind_updates", lambda *args: (_ for _ in ()).throw(ValueError()))
    with pytest.raises(rpc.PluginError):
        asyncio.run(plugin.on_account_start(account))
    client.disconnect.assert_awaited_once()
    assert closed == [True]


def test_send_rejects_wrong_fallback_confirmation():
    async def response(request):
        return tl.Updates([tl.UpdateMessageID(52, request.random_id)], [], [], NOW, 1)
    client = AsyncMock(side_effect=response)
    client.get_input_entity.return_value = tl.InputPeerUser(17, 0)
    client.get_messages.return_value = row(53)
    with pytest.raises(rpc.Internal):
        asyncio.run(send(client, dto.MessagesSendParams(account_id="a", chat_id="17", text="x", client_id="key")))


def test_service_events_forward_headers_and_sparse_metadata():
    header = tl.MessageReplyHeader(reply_to_msg_id=4, quote_text="Row", quote_entities=[tl.MessageEntityBold(0, 3)], quote_offset=0)
    service = tl.MessageService(id=8, peer_id=tl.PeerChannel(9), date=NOW, from_id=tl.PeerUser(17),
                                action=tl.MessageActionPinMessage(), reply_to=header)
    mapped = message(service)
    assert mapped.action.kind == "pinned" and mapped.action.message_id == "4"
    assert mapped.action.actor_id == "17"
    assert mapped.reply_to.quote_offset == 0 and mapped.reply_to.quote_entities[0].type == "bold"
    service.action = tl.MessageActionTopicCreate("Topic", 0)
    mapped = message(service)
    assert mapped.topic_id == "8" and mapped.action.title == "Topic"
    service.action = tl.MessageActionCustomAction("Sample custom event")
    assert message(service).action.text == "Sample custom event"
    forwarded = row(8, fwd_from=tl.MessageFwdHeader(NOW, from_id=tl.PeerChannel(9), channel_post=4, post_author="Author"),
                    pinned=True, silent=True, via_bot_id=21, effect=25, ttl_period=60)
    mapped = message(forwarded)
    assert (mapped.forward_from.chat_id, mapped.forward_from.message_id, mapped.forward_from.signature) == ("-1000000000009", "4", "Author")
    assert mapped.is_pinned and mapped.silent and mapped.via_bot_id == "21" and mapped.effect_id == "25" and mapped.ttl_seconds == 60
