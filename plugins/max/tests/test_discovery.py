"""Synthetic server marker paths; no MAX account or network access."""
import asyncio
from types import SimpleNamespace

import pytest
from contact_hub_sdk import PluginError
from contact_hub_sdk.testing import schema_check
from contact_hub_sdk.wire import to_wire
from pymax.protocol import Command, InboundFrame, Opcode
from pymax.types.domain.chat import Chat
from pymax.types.domain.login import LoginResponse
from pymax.types.domain.user import User

from max_plugin.adapter import PyMaxAdapter
from max_plugin.discovery import drain_chat_pages
from max_plugin.settings import Settings
from max_plugin.state import PrivateState


def chat(chat_id, *, text=None):
    data = {"id": chat_id, "type": "DIALOG", "status": "ACTIVE", "owner": 1,
            "participants": {1: 0, 2: 0}}
    if text is not None:
        data["lastMessage"] = {"id": 500 + chat_id, "sender": 2, "time": 1767225600000,
                               "type": "USER", "text": text}
    return Chat.model_validate(data)


def page(chats, marker):
    return {"chats": [row.model_dump(mode="json", by_alias=True) for row in chats], "marker": marker}


def test_marker_chain_adds_chats_beyond_initial_forty_and_uses_exact_opaque_values():
    async def run():
        initial = [chat(index) for index in range(1, 41)]
        replies = iter([page([chat(41, text="page one")], 200), page([chat(42)], 0)])
        calls, delays = [], []

        async def invoke(opcode, payload):
            calls.append((opcode, payload))
            return SimpleNamespace(payload=next(replies))

        async def sleep(seconds):
            delays.append(seconds)

        # Increasing markers are opaque values, not rejected by an invented order.
        result = await drain_chat_pages(invoke, initial, 100, sleep=sleep)
        assert [row.id for row in result] == list(range(1, 43))
        assert calls == [(Opcode.CHATS_LIST, {"marker": 100}), (Opcode.CHATS_LIST, {"marker": 200})]
        assert delays == [2, 2]
    asyncio.run(run())


def test_explicit_zero_and_absent_unknown_do_not_issue_a_first_page_request():
    async def run():
        async def forbidden(*args):
            raise AssertionError("zero/unknown must not become marker-or-now")

        initial = [chat(1)]
        assert await drain_chat_pages(forbidden, initial, 0, sleep=forbidden) == initial
        assert await drain_chat_pages(forbidden, initial, None, sleep=forbidden) == initial
    asyncio.run(run())


@pytest.mark.parametrize("payload", [
    {}, {"chats": []}, {"marker": 0}, {"chats": None, "marker": 0},
    {"chats": [], "marker": None}, {"chats": [], "marker": True},
    {"chats": [], "marker": "0"}, {"chats": [], "marker": 0.0},
    {"chats": [{"id": 2}], "marker": 0},
])
def test_malformed_page_is_an_error_instead_of_exhaustion(payload):
    async def run():
        async def invoke(*args):
            return SimpleNamespace(payload=payload)

        async def sleep(_):
            pass

        with pytest.raises(PluginError):
            await drain_chat_pages(invoke, [chat(1)], 100, sleep=sleep)
    asyncio.run(run())


@pytest.mark.parametrize("markers", [[100], [200, 100]])
def test_repeated_or_cyclic_marker_fails_without_publishing_partial_result(markers):
    async def run():
        replies = iter(page([chat(2)], value) for value in markers)
        calls = []

        async def invoke(opcode, payload):
            calls.append(payload)
            return SimpleNamespace(payload=next(replies))

        async def sleep(_):
            pass

        initial = [chat(1)]
        with pytest.raises(PluginError, match="repeated"):
            await drain_chat_pages(invoke, initial, 100, sleep=sleep)
        assert [row.id for row in initial] == [1]
        assert len(calls) == len(markers)
    asyncio.run(run())


def test_page_and_record_budgets_are_explicit_failures(monkeypatch):
    from max_plugin import discovery

    async def run():
        calls = []

        async def invoke(opcode, payload):
            calls.append(payload)
            return SimpleNamespace(payload=page([chat(2)], 100 + len(calls)))

        async def sleep(_):
            pass

        monkeypatch.setattr(discovery, "MAX_CHAT_PAGES", 2)
        with pytest.raises(PluginError, match="page budget"):
            await drain_chat_pages(invoke, [chat(1)], 100, sleep=sleep)
        assert len(calls) == 2
        monkeypatch.setattr(discovery, "MAX_CHAT_RECORDS", 1)
        with pytest.raises(PluginError, match="record budget"):
            await drain_chat_pages(invoke, [chat(1)], 100, sleep=sleep)
    asyncio.run(run())


def test_cancel_during_pacing_never_issues_the_next_request():
    async def run():
        sleeping = asyncio.Event()
        calls = []

        async def sleep(_):
            sleeping.set()
            await asyncio.Event().wait()

        async def invoke(*args):
            calls.append(args)
            raise AssertionError("cancelled before the next server request")

        task = asyncio.create_task(drain_chat_pages(invoke, [chat(1)], 100, sleep=sleep))
        await sleeping.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert calls == []
    asyncio.run(run())


def test_cancel_during_page_read_does_not_return_a_partial_snapshot():
    async def run():
        reading = asyncio.Event()
        cancelled = asyncio.Event()

        async def sleep(_):
            pass

        async def invoke(*args):
            reading.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        task = asyncio.create_task(drain_chat_pages(invoke, [chat(1)], 100, sleep=sleep))
        await reading.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cancelled.is_set()
    asyncio.run(run())


def install_pinned_dispatch_client(monkeypatch, payload, replies, *, delayed=False):
    """Keep the actual pinned raw dispatcher; replace only the network client."""
    import pymax
    from pymax.dispatch import Router
    from pymax.dispatch.dispatcher import Dispatcher

    created = []

    class Client:
        def __init__(self, *, extra_config, **kwargs):
            created.append(self)
            self.closed = False
            self.calls = []
            self.tasks = []
            self.chats = [chat(index) for index in range(1, 41)]
            self.contacts = []
            self.messages = {}
            self.me = SimpleNamespace(contact=User(id=1))
            self._config = SimpleNamespace(device=SimpleNamespace(user_agent=extra_config.generate_web_user_agent()))
            self._app = SimpleNamespace(invoke=self.invoke)
            self.router = Router()
            self.dispatcher = Dispatcher(self._app, self.router)
            self.dispatcher.bind_client(self)

        async def _ensure_runtime(self):
            pass

        def on_raw(self):
            return self.router.on_raw()

        def on_message(self):
            return self.router.on_message()

        def on_message_edit(self):
            return self.router.on_message_edit()

        def on_chat_update(self):
            return self.router.on_chat_update()

        def on_message_delete(self):
            return self.router.on_message_delete()

        async def connect(self):
            async def deliver():
                if delayed:
                    await asyncio.sleep(0)
                # An unrelated response cannot satisfy LOGIN metadata capture.
                await self.dispatcher.dispatch(InboundFrame(opcode=Opcode.PING, cmd=Command.RESPONSE,
                                                            seq=1, payload={"chatMarker": 999}))
                await self.dispatcher.dispatch(InboundFrame(opcode=Opcode.LOGIN, cmd=Command.RESPONSE,
                                                            seq=2, payload=payload))
            if delayed:
                self.tasks.append(asyncio.create_task(deliver()))
            else:
                await deliver()

        async def invoke(self, opcode, payload):
            self.calls.append((opcode, payload))
            return SimpleNamespace(payload=replies.pop(0))

        async def close(self):
            self.closed = True
            for task in self.tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*self.tasks, return_exceptions=True)

    monkeypatch.setattr(pymax, "WebClient", Client)
    return created


@pytest.mark.parametrize("delayed", [False, True])
def test_public_raw_hook_retains_login_marker_across_response_timing(tmp_path, monkeypatch, delayed):
    from conftest import ROOT

    async def run():
        # Domain CamelModel retains unmodeled LOGIN metadata, but App.start does
        # not expose its response. The public raw hook needs no runtime override.
        assert LoginResponse.model_validate({"chatMarker": 100}).model_extra["chatMarker"] == 100
        created = install_pinned_dispatch_client(monkeypatch, {"chatMarker": 100},
                        [page([chat(41, text="beyond initial forty")], 0)], delayed=delayed)
        store = PrivateState(tmp_path / "state", "a")
        delays = []

        async def sleep(seconds):
            delays.append(seconds)

        async def event(*args):
            pass

        adapter = PyMaxAdapter(SimpleNamespace(), store, Settings(30, None), event, sleep=sleep)
        await adapter.connect("qr", True)
        me, contacts, chats, messages = await adapter.snapshot()
        assert len(chats) == 41 and chats[-1].id == "41"
        assert messages[0].text == "beyond initial forty" and messages[0].chat_id == "41"
        assert created[0].calls == [(Opcode.CHATS_LIST, {"marker": 100})] and delays == [2]
        check = schema_check(ROOT / "schemas")
        check(to_wire({"account_id": "max", "conversations": chats}), "events.json#/$defs/conversations.upsert")
        check(to_wire({"account_id": "max", "messages": messages}), "events.json#/$defs/messages.upsert")
        await adapter.close()
        store.release()
    asyncio.run(run())


@pytest.mark.parametrize("payload,invalid", [({}, False), ({"chatMarker": 0}, False),
    ({"chatMarker": None}, True), ({"chatMarker": False}, True), ({"chatMarker": "100"}, True)])
def test_absent_login_marker_is_unknown_but_present_invalid_fails(tmp_path, monkeypatch, payload, invalid):
    async def run():
        created = install_pinned_dispatch_client(monkeypatch, payload, [])
        store = PrivateState(tmp_path / "state", "a")
        adapter = PyMaxAdapter(SimpleNamespace(), store, Settings(30, None), None)
        try:
            if invalid:
                with pytest.raises(PluginError, match="invalid chat continuation marker"):
                    await adapter.connect("qr", True)
            else:
                await adapter.connect("qr", True)
                _, _, chats, _ = await adapter.snapshot()
                assert len(chats) == 40
            assert created[0].calls == []
        finally:
            await adapter.close()
            store.release()
    asyncio.run(run())


@pytest.mark.parametrize("source", ["page", "initial"])
def test_snapshot_rejects_message_with_contradicting_parent_chat(tmp_path, monkeypatch, source):
    async def run():
        row = chat(41, text="synthetic parent mismatch")
        row.last_message = row.last_message.model_copy(update={"chat_id": 99})
        replies = [page([row], 0)] if source == "page" else []
        created = install_pinned_dispatch_client(monkeypatch,
                    {"chatMarker": 100 if source == "page" else 0}, replies)
        store = PrivateState(tmp_path / "state", "a")

        async def sleep(_):
            pass

        adapter = PyMaxAdapter(SimpleNamespace(), store, Settings(30, None), None, sleep=sleep)
        try:
            await adapter.connect("qr", True)
            if source == "initial":
                created[0].messages = {41: [row.last_message]}
            with pytest.raises(PluginError, match="contradicts its parent conversation"):
                await adapter.snapshot()
        finally:
            await adapter.close()
            store.release()
    asyncio.run(run())
