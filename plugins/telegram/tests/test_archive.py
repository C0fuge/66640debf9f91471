"""Archive observations use the raw peer folder, never the custom truthy shortcut."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from contact_hub_sdk import errors as rpc
from contact_hub_sdk.wire import to_wire
from telethon import types as tl
from telethon.tl.custom.dialog import Dialog

from stub import NOW
from test_chat_flags import ARCHIVE, MARK, PIN, READ, observed, setup, snapshot
from telegram_plugin import sync
from telegram_plugin.mapping import archive_state, conversation


@pytest.mark.parametrize("folder,expected", [(None, False), (0, False), (1, True)])
def test_raw_folder_default_and_archive_are_consistent(folder, expected):
    raw = snapshot(folder=folder).dialogs[0]
    dialog = Dialog(None, raw, {17: tl.User(17, access_hash=0)}, None)
    assert archive_state(folder) is expected
    assert to_wire(conversation(dialog))["archived"] is expected
    if folder == 0:
        assert dialog.archived is True, "the pinned Telethon shortcut treats explicit zero as archived"


@pytest.mark.parametrize("folder", [True, False, -1, 2, "1", 1.0])
def test_unsupported_folder_is_never_guessed(folder):
    with pytest.raises(rpc.NotSupported):
        archive_state(folder)


@pytest.mark.parametrize("desired,folder", [(True, None), (True, 0), (False, 1)])
@pytest.mark.parametrize("updates", [tl.Updates([], [], [], NOW, 0), tl.UpdatesTooLong(),
                                    tl.UpdatesCombined([], [], [], NOW, 0, 0),
                                    tl.UpdateShort(tl.UpdateFolderPeers([], 1, 1), NOW)])
def test_archive_readback_observes_pin_side_effect_even_for_empty_updates(desired, folder, updates):
    async def exercise():
        task, account, calls = setup([snapshot(pinned=True, folder=folder), updates,
                                       snapshot(folder=1 if desired else 0)], archived=desired)
        await task
        assert list(map(type, calls)) == [READ, ARCHIVE, READ]
        assert calls[1].folder_peers[0].folder_id == (1 if desired else 0)
        assert observed(account) == {"id": "17", "kind": "dm", "archived": desired, "pinned": False}
    asyncio.run(exercise())


@pytest.mark.parametrize("folder,desired", [(None, False), (0, False), (1, True)])
def test_confirmed_archive_noop_emits_observed_pin_without_mutation(folder, desired):
    async def exercise():
        task, account, calls = setup([snapshot(pinned=True, folder=folder)], archived=desired)
        await task
        assert list(map(type, calls)) == [READ]
        assert observed(account) == {"id": "17", "kind": "dm", "archived": desired, "pinned": True}
    asyncio.run(exercise())


@pytest.mark.parametrize("failure", [False, True, {}, [], OSError(), TimeoutError(), asyncio.CancelledError()])
def test_lost_or_malformed_archive_reply_is_unknown_without_replay(failure):
    async def exercise():
        task, account, calls = setup([snapshot(), failure, snapshot(folder=1)], archived=True)
        with pytest.raises(rpc.PluginError) as err:
            await task
        assert err.value.data["outcome"] == "unknown" and err.value.data["retry_safe"] is False
        if isinstance(failure, asyncio.CancelledError):
            assert list(map(type, calls)) == [READ, ARCHIVE]
            account.conversations_upsert.assert_not_awaited()
        else:
            assert list(map(type, calls)) == [READ, ARCHIVE, READ]
            assert observed(account)["archived"] is True
    asyncio.run(exercise())


def test_mixed_write_partial_failure_preserves_observed_effects_and_stops():
    async def exercise():
        task, account, calls = setup([snapshot(), True, True, rpc.Forbidden(),
                                       snapshot(pinned=True, marked=True)],
                                      pinned=True, marked_unread=True, archived=True)
        with pytest.raises(rpc.PluginError) as err:
            await task
        assert err.value.data["outcome"] == "unknown"
        assert list(map(type, calls)) == [READ, PIN, MARK, ARCHIVE, READ]
        assert observed(account) == {"id": "17", "kind": "dm", "pinned": True,
                                     "marked_unread": True, "archived": False}
        task, account, calls = setup([snapshot(), OSError(), snapshot(pinned=True)], pinned=True, archived=True)
        with pytest.raises(rpc.PluginError):
            await task
        assert list(map(type, calls)) == [READ, PIN, READ]
        assert observed(account)["archived"] is False
    asyncio.run(exercise())


def test_archive_does_not_claim_requested_pin_survives_move():
    async def exercise():
        task, account, calls = setup([snapshot(), True, tl.UpdatesTooLong(), snapshot(folder=1)],
                                      pinned=True, archived=True)
        with pytest.raises(rpc.Unavailable):
            await task
        assert list(map(type, calls)) == [READ, PIN, ARCHIVE, READ]
        assert observed(account) == {"id": "17", "kind": "dm", "pinned": False, "archived": True}
    asyncio.run(exercise())


def test_archive_requires_valid_preflight_and_exact_readback():
    async def exercise():
        task, account, calls = setup([snapshot(folder=2)], pinned=True, archived=True)
        with pytest.raises(rpc.NotSupported):
            await task
        assert list(map(type, calls)) == [READ]
        account.conversations_upsert.assert_not_awaited()
        task, account, calls = setup([snapshot(), tl.UpdatesTooLong(), snapshot(tl.PeerUser(18))], archived=True)
        with pytest.raises(rpc.PluginError) as err:
            await task
        assert err.value.data["outcome"] == "unknown"
        assert list(map(type, calls)) == [READ, ARCHIVE, READ]
        account.conversations_upsert.assert_not_awaited()
    asyncio.run(exercise())


@pytest.mark.parametrize("folder,expected", [(None, False), (0, False), (1, True)])
def test_getter_exposes_exact_archive_state_without_events(folder, expected):
    async def exercise():
        task, account, calls = setup([snapshot(folder=folder)], read_only=True)
        assert to_wire(await task)["conversation"]["archived"] is expected
        assert list(map(type, calls)) == [READ]
        account.conversations_upsert.assert_not_awaited()
    asyncio.run(exercise())


def test_full_sync_uses_raw_zero_and_never_closes_invalid_folder_snapshot():
    async def exercise():
        for folder in (0, 1, 2):
            raw = snapshot(folder=folder).dialogs[0]
            item = Dialog(None, raw, {17: tl.User(17, access_hash=0)}, None)
            async def dialogs():
                yield item
            client = SimpleNamespace(iter_dialogs=dialogs)
            account = SimpleNamespace(conversations_upsert=AsyncMock(), conversations_synced=AsyncMock())
            if folder == 2:
                with pytest.raises(rpc.NotSupported):
                    await sync.dialogs(account, client, include_archived=False)
                account.conversations_synced.assert_not_awaited()
                account.conversations_upsert.assert_not_awaited()
                continue
            await sync.dialogs(account, client, include_archived=False)
            account.conversations_synced.assert_awaited_once_with(["17"], ack=True)
            if folder == 0:
                args = account.conversations_upsert.await_args.args
                assert to_wire(args[0][0])["archived"] is False
            else:
                args = account.conversations_upsert.await_args.args
                assert to_wire(args[0][0]) == {"id": "17", "kind": "dm", "archived": True, "pinned": False}
    asyncio.run(exercise())


def test_folder_updates_require_explicit_reconciliation_without_network_fanout():
    async def exercise():
        account, client = AsyncMock(), AsyncMock()
        await sync.raw_update(account, tl.UpdateFolderPeers([
            tl.FolderPeer(tl.PeerUser(17), 1), tl.FolderPeer(tl.PeerChannel(17), 0)], 1, 1), client)
        client.assert_not_awaited()
        assert not account.mock_calls
    asyncio.run(exercise())


def test_archive_core_ack_loss_keeps_unknown_effect_without_replay():
    async def exercise():
        task, account, calls = setup([snapshot(), tl.UpdatesTooLong(), snapshot(folder=1)], archived=True)
        account.conversations_upsert.side_effect = rpc.RpcTimeout()
        with pytest.raises(rpc.PluginError) as err:
            await task
        assert err.value.data["outcome"] == "unknown" and err.value.data["retry_safe"] is False
        assert list(map(type, calls)) == [READ, ARCHIVE, READ]
        assert observed(account) == {"id": "17", "kind": "dm", "archived": True, "pinned": False}
    asyncio.run(exercise())


@pytest.mark.parametrize("group", [True, False])
def test_archive_targets_the_exact_marked_channel_peer_and_kind(group):
    async def exercise():
        peer, entity = tl.PeerChannel(8), tl.Channel(8, "Test", tl.ChatPhotoEmpty(), NOW, megagroup=group)
        task, account, calls = setup([snapshot(peer, entity), tl.UpdatesTooLong(), snapshot(peer, entity, folder=1)],
                                      peer=tl.InputPeerChannel(8, 0), chat_id="-1000000000008", archived=True)
        await task
        assert list(map(type, calls)) == [READ, ARCHIVE, READ]
        assert calls[1].folder_peers[0].peer == tl.InputPeerChannel(8, 0)
        assert observed(account) == {"id": "-1000000000008", "kind": "group" if group else "channel",
                                     "archived": True, "pinned": False}
    asyncio.run(exercise())
