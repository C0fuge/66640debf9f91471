import asyncio
import os

import pytest
from contact_hub_sdk import InvalidParams, PluginError
from pymax.api.session.payloads import MobileUserAgentPayload
from pymax.session.models import SessionInfo
from pymax.types.domain.sync import SyncState

from max_plugin.state import PrivateState


def session(state, token="synthetic-token"):
    return SessionInfo(token=token, device_id=state.identity["device_id"], phone="+15550000001",
                       mt_instance_id=state.identity["mt_instance_id"],
                       user_agent=MobileUserAgentPayload(device_type="WEB", app_version="synthetic",
                           os_version="Linux", timezone="UTC", screen="100x100", locale="en",
                           device_name="Synthetic", device_locale="en"),
                       sync=SyncState(chats_sync=123, contacts_sync=456, drafts_sync=789,
                                      presence_sync=234, config_hash="synthetic"))


def test_pinned_session_round_trip_is_private_and_account_isolated(tmp_path):
    async def run():
        first = PrivateState(tmp_path / "a", "opaque/account/a")
        second = PrivateState(tmp_path / "b", "opaque/account/b")
        original = session(first)
        await first.save_session(original)
        await second.save_session(session(second, "synthetic-other"))
        identity = dict(first.identity)
        first.release()
        first = PrivateState(tmp_path / "a", "opaque/account/a")
        loaded = await first.load_session()
        assert loaded.model_dump() == original.model_dump()
        assert first.identity == identity
        assert (await second.load_session()).token == "synthetic-other"
        assert await first.load_session_by_phone("+15550000001") == loaded
        assert await first.load_session_by_device_id(second.identity["device_id"]) is None
        await first.update_token("synthetic-token", "synthetic-new-token")
        assert (await first.load_session()).token == "synthetic-new-token"
        with pytest.raises(PluginError):
            await first.update_token("synthetic-token", "stale")
        await first.delete_session("stale")
        assert first.has_session
        await first.delete_session("synthetic-new-token")
        assert not first.has_session
        assert first.directory.stat().st_mode & 0o777 == 0o700
        for path in list(first.directory.iterdir()) + list(second.directory.iterdir()):
            assert path.stat().st_mode & 0o777 == 0o600
        assert not list(tmp_path.rglob("*.db"))
        assert first.identity["device_id"] != second.identity["device_id"]
        first.release()
        second.release()
    asyncio.run(run())


def test_lock_owner_and_symlink_rejection(tmp_path):
    state = PrivateState(tmp_path / "a", "first")
    with pytest.raises(BlockingIOError):
        PrivateState(tmp_path / "a", "first")
    state.release()
    with pytest.raises(InvalidParams):
        PrivateState(tmp_path / "a", "another")
    external = tmp_path / "outside.json"
    external.write_text('{"untouched":true}')
    (tmp_path / "a/session.json").symlink_to(external)
    state = PrivateState(tmp_path / "a", "first")
    with pytest.raises(OSError):
        state.read("session.json")
    with pytest.raises(InvalidParams):
        state.write("session.json", {"bad": True})
    assert external.read_text() == '{"untouched":true}'
    state.release()
    (tmp_path / "linked").symlink_to(tmp_path / "a", target_is_directory=True)
    with pytest.raises(InvalidParams):
        PrivateState(tmp_path / "linked", "first")


def test_session_device_mismatch_is_rejected(tmp_path):
    async def run():
        state = PrivateState(tmp_path / "a", "a")
        wrong = session(state).model_copy(update={"device_id": "wrong"})
        with pytest.raises(PluginError):
            await state.save_session(wrong)
        state.write("session.json", wrong.model_dump(mode="json"))
        with pytest.raises(PluginError):
            await state.load_session()
        state.release()
    asyncio.run(run())


def test_no_legacy_session_import_or_environment_discovery(tmp_path, monkeypatch):
    legacy = tmp_path / "legacy-session.db"
    legacy.write_bytes(b"synthetic legacy snapshot; never open")
    monkeypatch.setenv("DB_URL", "sqlite:///" + str(legacy))
    monkeypatch.setenv("CONTACT_HUB_ENV_FILE", str(tmp_path / "missing.env"))
    state = PrivateState(tmp_path / "new", "new")
    assert not state.has_session
    assert legacy.read_bytes() == b"synthetic legacy snapshot; never open"
    state.release()


def test_non_regular_state_file_is_rejected_without_blocking(tmp_path):
    state = PrivateState(tmp_path / "a", "a")
    os.mkfifo(state.directory / "session.json", 0o600)
    with pytest.raises(InvalidParams):
        state.read("session.json")
    state.release()
