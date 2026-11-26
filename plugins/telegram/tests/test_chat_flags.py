import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from contact_hub_sdk import errors as rpc, types as dto
from contact_hub_sdk.wire import to_wire
from telethon import functions, types as tl
from telethon.extensions import BinaryReader

from stub import NOW
from telegram_plugin.plugin import TelegramPlugin


READ = functions.messages.GetPeerDialogsRequest
PIN = functions.messages.ToggleDialogPinRequest
MARK = functions.messages.MarkDialogUnreadRequest
ARCHIVE = functions.folders.EditPeerFoldersRequest


def snapshot(peer=tl.PeerUser(17), entity=None, pinned=False, marked=False, folder=None):
    raw = tl.Dialog(peer, 10, 7, 9, 3, 2, 1, 0, tl.PeerNotifySettings(), pinned=pinned, unread_mark=marked,
                    folder_id=folder)
    entity = entity or tl.User(17)
    users, chats = ([entity], []) if isinstance(entity, tl.User) else ([], [entity])
    return tl.messages.PeerDialogs([raw], [], chats, users, tl.updates.State(0, 0, NOW, 0, 0))


def setup(replies, *, peer=None, chat_id="17", read_only=False, **fields):
    calls = []
    peer = peer or tl.InputPeerUser(17, 0)

    async def respond(request):
        req = BinaryReader(bytes(request)).tgread_object()
        calls.append(req)
        target = tl.InputDialogPeer(peer)
        if isinstance(req, READ):
            assert req.peers == [target]
        elif isinstance(req, ARCHIVE):
            assert len(req.folder_peers) == 1 and req.folder_peers[0].peer == peer
            assert req.folder_peers[0].folder_id in (0, 1)
        else:
            assert req.peer == target
            if isinstance(req, MARK):
                assert req.parent_peer is None
        result = replies[len(calls) - 1]
        if isinstance(result, BaseException):
            raise result
        return result

    client = AsyncMock(side_effect=respond)
    client.get_input_entity.return_value = peer
    account = SimpleNamespace(state="connected", data=dict(client=client, sync_lock=asyncio.Lock()),
                              conversations_upsert=AsyncMock())
    if read_only:
        params = dto.ChatsGetParams(account_id="a", chat_id=chat_id)
        return TelegramPlugin().chats_get(account, params), account, calls
    params = dto.ChatsSetFlagsParams(account_id="a", chat_id=chat_id, **fields)
    return TelegramPlugin().chats_set_flags(account, params), account, calls


def observed(account):
    args, kwargs = account.conversations_upsert.await_args
    assert kwargs == {"ack": True}
    return to_wire(args[0][0])


@pytest.mark.parametrize("failure,code", [(False, "NETWORK"), (1, "NETWORK"), (OSError(), "NETWORK"),
                                          (TimeoutError(), "TIMEOUT")])
def test_first_ambiguous_write_observes_once_and_stops(failure, code):
    async def exercise():
        task, account, calls = setup([snapshot(), failure, snapshot(pinned=True)], pinned=True, marked_unread=True)
        with pytest.raises(rpc.PluginError) as exc:
            await task
        assert exc.value.code == code and exc.value.data == {"reason": "outcome_unknown", "outcome": "unknown", "retry_safe": False}
        assert list(map(type, calls)) == [READ, PIN, READ] and calls[1].pinned is True
        assert observed(account) == {"id": "17", "kind": "dm", "pinned": True, "marked_unread": False}
    asyncio.run(exercise())


def test_definite_refusal_and_second_write_failure():
    async def exercise():
        task, account, calls = setup([snapshot(), rpc.FloodWait(9)], pinned=True)
        with pytest.raises(rpc.FloodWait) as exc:
            await task
        assert exc.value.seconds == 9 and list(map(type, calls)) == [READ, PIN]
        account.conversations_upsert.assert_not_awaited()
        for second in (TimeoutError(), rpc.Forbidden()):
            task, account, calls = setup([snapshot(pinned=True), True, second, snapshot()], pinned=False, marked_unread=True)
            with pytest.raises(rpc.PluginError) as exc:
                await task
            assert exc.value.code in ("TIMEOUT", "NETWORK") and exc.value.data["retry_safe"] is False
            assert list(map(type, calls)) == [READ, PIN, MARK, READ]
            assert calls[1].pinned is False and calls[2].unread is True
            assert observed(account) == {"id": "17", "kind": "dm", "pinned": False, "marked_unread": False}
    asyncio.run(exercise())


def test_postdispatch_cancel_core_ack_loss_and_racing_observations():
    async def exercise():
        task, account, calls = setup([snapshot(marked=True), asyncio.CancelledError()], marked_unread=False)
        with pytest.raises(rpc.PluginError) as exc:
            await task
        assert exc.value.data["retry_safe"] is False and calls[1].unread is False
        assert list(map(type, calls)) == [READ, MARK] and not account.data["sync_lock"].locked()
        for last, ack, code in [(snapshot(), None, "UNAVAILABLE"), (snapshot(pinned=True), TimeoutError(), "TIMEOUT"),
                                (snapshot(tl.PeerUser(19)), None, "NETWORK")]:
            task, account, calls = setup([snapshot(), True, last], pinned=True)
            account.conversations_upsert.side_effect = ack
            with pytest.raises(rpc.PluginError) as exc:
                await task
            assert exc.value.code == code and exc.value.data["retry_safe"] is False
            assert list(map(type, calls)) == [READ, PIN, READ]
            if code == "NETWORK":
                account.conversations_upsert.assert_not_awaited()
            else:
                assert observed(account) == {"id": "17", "kind": "dm", "pinned": ack is not None}
    asyncio.run(exercise())


def test_full_snapshot_lock_and_confirmed_noop():
    async def exercise():
        task, account, calls = setup([snapshot()], pinned=False)
        lock = account.data["sync_lock"]
        async with lock:
            pending = asyncio.create_task(task)
            await asyncio.sleep(0)
            account.data["client"].get_input_entity.assert_not_awaited()
        await pending
        assert list(map(type, calls)) == [READ]
        assert observed(account) == {"id": "17", "kind": "dm", "pinned": False}
    asyncio.run(exercise())


def test_exact_dialog_peer_and_entity_kind():
    async def exercise():
        channel = tl.PeerChannel(8)
        def entity(**kwargs):
            return tl.Channel(8, "Test", tl.ChatPhotoEmpty(), NOW, **kwargs)
        bad = [snapshot(tl.PeerUser(8)), snapshot(channel, tl.User(8)), snapshot(channel, entity(min=True)),
               snapshot(channel, entity(monoforum=True)), snapshot(channel, tl.ChannelForbidden(8, 0, "Test"))]
        empty = snapshot()
        empty.dialogs.clear()
        for wrong in [*bad, empty, False]:
            task, account, calls = setup([wrong], peer=tl.InputPeerChannel(8, 0), chat_id="-1000000000008", pinned=True)
            with pytest.raises(rpc.PluginError):
                await task
            assert list(map(type, calls)) == [READ]
            account.conversations_upsert.assert_not_awaited()
        for group in (True, False):
            task, account, calls = setup([snapshot(channel, entity(megagroup=group))], peer=tl.InputPeerChannel(8, 0),
                                         chat_id="-1000000000008", pinned=False)
            await task
            assert observed(account)["kind"] == ("group" if group else "channel")
    asyncio.run(exercise())


def test_chat_get_reads_one_peer_under_account_lock_without_emitting():
    async def exercise():
        for pin, mark in ((False, False), (True, False), (False, True)):
            task, account, calls = setup([snapshot(pinned=pin, marked=mark)], read_only=True)
            async with account.data["sync_lock"]:
                pending = asyncio.create_task(task)
                await asyncio.sleep(0)
                account.data["client"].get_input_entity.assert_not_awaited()
            assert to_wire(await pending) == {"conversation": {
                "id": "17", "kind": "dm", "pinned": pin, "marked_unread": mark, "archived": False}}
            assert list(map(type, calls)) == [READ]
            account.conversations_upsert.assert_not_awaited()
    asyncio.run(exercise())


def test_chat_get_validates_identity_and_propagates_read_failures():
    async def exercise():
        empty = snapshot()
        empty.dialogs = []
        for result, code in [(empty, "NOT_FOUND"), (snapshot(tl.PeerChannel(17)), "INTERNAL"),
                             (snapshot(entity=tl.User(18)), "INTERNAL"), (snapshot(pinned=None), "INTERNAL"),
                             (TimeoutError(), "TIMEOUT"), (OSError(), "NETWORK")]:
            task, account, calls = setup([result], read_only=True)
            with pytest.raises(rpc.PluginError) as err:
                await task
            assert err.value.code == code and "outcome" not in err.value.data
            assert list(map(type, calls)) == [READ]
            account.conversations_upsert.assert_not_awaited()
        task, account, calls = setup([asyncio.CancelledError()], read_only=True)
        with pytest.raises(asyncio.CancelledError):
            await task
        assert list(map(type, calls)) == [READ] and not account.data["sync_lock"].locked()
        account.conversations_upsert.assert_not_awaited()
    asyncio.run(exercise())
