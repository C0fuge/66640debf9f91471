"""Pinned models, single-invoke edit transport and schema-checked synthetic wire."""
import asyncio
from types import SimpleNamespace

import pytest
from contact_hub_sdk import Forbidden, NotSupported, PluginError, Unavailable
from pymax.exceptions import PyMaxError
from pymax.protocol import Command, InboundFrame, Opcode
from pymax.types.domain.message import Message

from max_plugin.adapter import PyMaxAdapter
from max_plugin.editing import EditPolicy
from max_plugin.plugin import MaxPlugin
from max_plugin.settings import Settings
from max_plugin.state import PrivateState
from test_deletions import identity
from test_discovery import install_pinned_dispatch_client
from test_wire import sign_in

NOW = 1767225601000
SENT = NOW - 1000


def message(**changes):
    row = {"id": 21, "chatId": 0, "sender": 1, "time": SENT, "type": "USER", "text": "old"}
    row.update(changes)
    return row


def chat(chat_id=0, **changes):
    row = {"id": chat_id, "type": "DIALOG" if chat_id == 0 else "CHAT", "status": "ACTIVE",
           "owner": 1, "participants": {"1": 0, "2": 0}, "restrictions": 0}
    row.update(changes)
    return row


def adapter(replies, *, clock=lambda: NOW / 1000, on_call=None):
    calls = []
    client = PyMaxAdapter(None, None, Settings(30, None), None, clock=clock)
    client.me = 1
    client._editing_active = True
    client.edit_policy.observe({"server": {"edit-timeout": 60}})

    async def invoke(opcode, payload):
        calls.append((opcode, payload))
        if on_call:
            on_call(client, len(calls))
        reply = replies[len(calls) - 1]
        if isinstance(reply, BaseException):
            raise reply
        return SimpleNamespace(payload=reply)

    client.client = SimpleNamespace(_app=SimpleNamespace(invoke=invoke))
    return client, calls


@pytest.mark.parametrize("chat_id", [0, 11])
def test_plain_text_edit_uses_fresh_pinned_reads_and_one_literal_payload(chat_id):
    async def run():
        text = "**literal** [text](https://example.invalid) `code`"
        old = message(chatId=chat_id)
        changed = message(chatId=chat_id, text=text, status="EDITED")
        client, calls = adapter([{"messages": [old]}, {"chats": [chat(chat_id)]}, {"message": changed}])
        target = await client.edit_preflight(chat_id, 21)
        result = await client.edit(target, text)
        assert isinstance(Message.model_validate(changed), Message)
        assert (result.id, result.chat_id, result.direction, result.text, result.edited) == (
            "21", str(chat_id), "out", text, True)
        assert calls == [(Opcode.MSG_GET, {"chatId": chat_id, "messageIds": [21]}),
                         (Opcode.CHAT_INFO, {"chatIds": [chat_id]}),
                         (Opcode.MSG_EDIT, {"chatId": chat_id, "messageId": 21, "text": text, "elements": []})]
    asyncio.run(run())


@pytest.mark.parametrize("change", [
    {"id": 99}, {"id": True}, {"chatId": 99}, {"chatId": 0.0}, {"sender": None},
    {"sender": 2}, {"sender": True}, {"type": "SYSTEM"}, {"postId": 9},
    {"delayedAttributes": {"timeToFire": NOW + 10000}}, {"status": "REMOVED"},
    {"attaches": [{"_type": "PHOTO"}]}, {"attaches": None}, {"text": ""},
    {"elements": [{"type": "STRONG", "from": 0, "length": 1}]},
    {"link": {"type": "FORWARD"}}, {"options": 32}, {"options": True},
    {"options": {}}, {"options": -1}, {"time": True}, {"time": SENT + 0.5},
    {"time": NOW + 1}, {"time": NOW - 60000},
    {"chat_id": 99}, {"post_id": 9}, {"delayed_attributes": {"timeToFire": NOW + 10000}},
])
def test_unsupported_or_unproved_messages_never_reach_mutation(change):
    async def run():
        client, calls = adapter([{"messages": [message(**change)]}, {"chats": [chat()]}])
        with pytest.raises((PluginError, ValueError)):
            await client.edit_preflight(0, 21)
        assert calls and all(opcode != Opcode.MSG_EDIT for opcode, _ in calls)
    asyncio.run(run())


@pytest.mark.parametrize("payload", [None, {}, {"messages": []}, {"messages": None},
    {"messages": [message(), message()]}, {"chatId": 99, "messages": [message()]},
    {"postId": 2, "messages": [message()]}])
def test_missing_duplicate_or_foreign_message_envelopes_never_write(payload):
    async def run():
        client, calls = adapter([payload])
        with pytest.raises((PluginError, ValueError, PyMaxError)):
            await client.edit_preflight(0, 21)
        assert [code for code, _ in calls] == [Opcode.MSG_GET]
    asyncio.run(run())


@pytest.mark.parametrize("change", [
    {"id": 99}, {"id": True}, {"type": "CHANNEL"}, {"type": "DIALOG"},
    {"status": "LEFT"}, {"status": "REMOVED"}, {"participants": {}},
    {"participants": {"2": 0}}, {"participants": {"1": True}},
    {"restrictions": None}, {"restrictions": True}, {"restrictions": 64},
    {"restrictions": "0"}, {"videoConversation": {"type": 0}},
    {"video_conversation": {"type": 0}},
])
def test_group_write_permission_requires_explicit_fresh_metadata(change):
    async def run():
        client, calls = adapter([{"messages": [message(chatId=11)]}, {"chats": [chat(11, **change)]}])
        with pytest.raises((PluginError, ValueError)):
            await client.edit_preflight(11, 21)
        assert [code for code, _ in calls] == [Opcode.MSG_GET, Opcode.CHAT_INFO]
    asyncio.run(run())


@pytest.mark.parametrize("payload", [{}, {"chats": []}, {"chats": None},
    {"chats": [chat(), chat()]}, {"chatId": 99, "chats": [chat()]},
    {"postId": 1, "chats": [chat()]}])
def test_chat_envelopes_cannot_silently_select_a_target(payload):
    async def run():
        client, calls = adapter([{"messages": [message()]}, payload])
        with pytest.raises((PluginError, ValueError)):
            await client.edit_preflight(0, 21)
        assert len(calls) == 2
    asyncio.run(run())


@pytest.mark.parametrize("value", [None, True, False, -1, 60.0, "60", {}, 2**31])
def test_invalid_policy_revokes_old_authority_instead_of_retaining_a_fallback(value):
    policy = EditPolicy(lambda: NOW / 1000)
    policy.observe({"server": {"edit-timeout": 60}})
    generation = policy.check(SENT)
    policy.observe({"server": {"edit-timeout": value}}, patch=True)
    with pytest.raises(Unavailable):
        policy.check(SENT, generation)
    with pytest.raises(Unavailable):
        policy.check(SENT)


def test_policy_full_vs_patch_and_strict_local_clock_boundaries():
    policy = EditPolicy(lambda: NOW / 1000)
    with pytest.raises(Unavailable):
        policy.check()
    policy.observe({"server": {"edit-timeout": 60}})
    generation = policy.check(NOW - 59999)
    policy.observe({"user": {"unrelated": True}})
    policy.observe({"server": {"unrelated": False}}, patch=True)
    assert policy.check(SENT, generation) == generation
    for when in [NOW - 60000, NOW + 1]:
        with pytest.raises(Forbidden):
            policy.check(when)
    policy.observe({"server": {}}, patch=False)
    with pytest.raises(Unavailable):
        policy.check(SENT)
    policy.observe({"server": {"edit-timeout": 0}})
    with pytest.raises(Forbidden):
        policy.check()


def test_policy_and_clock_change_during_preflight_or_preparation_remain_pre_dispatch():
    async def run():
        def revoke(client, count):
            if count == 2:
                client.edit_policy.reset()

        replies = [{"messages": [message()]}, {"chats": [chat()]}]
        client, calls = adapter(replies, on_call=revoke)
        with pytest.raises(Unavailable):
            await client.edit_preflight(0, 21)
        assert len(calls) == 2
        clock = [NOW / 1000]
        client, calls = adapter(replies, clock=lambda: clock[0])
        target = await client.edit_preflight(0, 21)
        clock[0] += 60
        with pytest.raises(Forbidden):
            client.edit(target, "new")
        assert len(calls) == 2
        clock[0] -= 60
        client.edit_policy.reset()
        with pytest.raises(Unavailable):
            client.edit(target, "new")
    asyncio.run(run())


def test_inactive_adapter_rejects_before_reads_and_revoked_prepared_target_cannot_write():
    async def run():
        client, calls = adapter([{"messages": [message()]}, {"chats": [chat()]}])
        client._editing_active = False
        with pytest.raises(Unavailable):
            await client.edit_preflight(0, 21)
        assert calls == []
        client._editing_active = True
        target = await client.edit_preflight(0, 21)
        client._editing_active = False
        client.edit_policy.reset()
        with pytest.raises(Unavailable):
            client.edit(target, "new")
        assert len(calls) == 2
    asyncio.run(run())


@pytest.mark.parametrize("replace_state", [False, True])
def test_queued_edit_cannot_follow_reconnect_or_runtime_replacement(replace_state):
    from contact_hub_sdk import types as T

    async def run():
        calls = []

        async def preflight(*args):
            calls.append(args)
            raise AssertionError("A previous run cannot preflight on its replacement")

        lock = asyncio.Lock()
        await lock.acquire()
        original = SimpleNamespace(ready=True, adapter=SimpleNamespace(edit_preflight=preflight),
                                   generation=1, mutation_lock=lock)
        account = SimpleNamespace(data={"max": original}, state="connected")
        plugin = object.__new__(MaxPlugin)
        pending = asyncio.create_task(plugin.edit(account, T.MessagesEditParams(
            account_id="max", chat_id="0", message_id="21", text="new")))
        await asyncio.sleep(0)
        assert not pending.done()
        if replace_state:
            account.data["max"] = SimpleNamespace(ready=True, adapter=SimpleNamespace(edit_preflight=preflight), generation=1)
        else:
            original.generation += 1
            original.adapter = SimpleNamespace(edit_preflight=preflight)
        lock.release()
        with pytest.raises(Unavailable):
            await pending
        assert calls == []
    asyncio.run(run())


@pytest.mark.parametrize("revoke", ["policy", "connection"])
def test_revocation_after_dispatch_never_confirms_a_previous_run(revoke):
    async def run():
        def changed(client, count):
            if count == 3:
                if revoke == "policy":
                    client.edit_policy.reset()
                else:
                    client._editing_active = False

        client, calls = adapter([{"messages": [message()]}, {"chats": [chat()]},
            {"message": message(text="new", status="EDITED")}], on_call=changed)
        target = await client.edit_preflight(0, 21)
        with pytest.raises(Unavailable):
            await client.edit(target, "new")
        assert len(calls) == 3
    asyncio.run(run())


@pytest.mark.parametrize("replace_at", ["preflight", "dispatch"])
def test_runtime_identity_is_rechecked_after_each_await(replace_at):
    from contact_hub_sdk import types as T
    from datetime import UTC, datetime

    async def run():
        calls = []

        def replace():
            # Deliberately leave the previous object's fields unchanged: only
            # identity distinguishes these two account lifecycle owners.
            account.data["max"] = SimpleNamespace(ready=True, adapter=client, generation=1)

        async def preflight(*args):
            calls.append("preflight")
            if replace_at == "preflight":
                replace()
            return SimpleNamespace()

        async def mutation():
            calls.append("dispatch")
            if replace_at == "dispatch":
                replace()
            return T.Message(id="21", chat_id="0", sender_id="1", direction="out",
                             sent_at=datetime(2026, 1, 1, tzinfo=UTC), text="new", edited=True)

        client = SimpleNamespace(edit_preflight=preflight, edit=lambda *args: mutation())
        state = SimpleNamespace(ready=True, adapter=client, generation=1, mutation_lock=asyncio.Lock())
        account = SimpleNamespace(data={"max": state}, state="connected")
        plugin = object.__new__(MaxPlugin)
        with pytest.raises(PluginError) as error:
            await plugin.edit(account, T.MessagesEditParams(account_id="max", chat_id="0", message_id="21", text="new"))
        assert error.value.code == ("UNAVAILABLE" if replace_at == "preflight" else "NETWORK")
        assert calls == (["preflight"] if replace_at == "preflight" else ["preflight", "dispatch"])
        if replace_at == "dispatch":
            assert error.value.data["outcome"] == "unknown" and error.value.data["retry_safe"] is False
    asyncio.run(run())


@pytest.mark.parametrize("payload", [None, {}, {"message": None},
    {"message": message(id=99, text="new", status="EDITED")},
    {"message": message(chatId=99, text="new", status="EDITED")},
    {"message": message(sender=2, text="new", status="EDITED")},
    {"message": message(text="wrong", status="EDITED")},
    {"message": message(text="new")}, {"message": message(text="new", status="REMOVED")},
    {"message": message(text="new", status="EDITED", time=SENT + 1)},
    {"message": message(text="new", status="EDITED", elements=[{"type": "STRONG"}])},
    {"chatId": 99, "message": message(text="new", status="EDITED")},
    {"message": message(text="new", status="EDITED", chat_id=99)},
    {"message": message(text="new", status="EDITED", post_id=9)},
    {"message": message(text="new", status="EDITED", delayed_attributes={"timeToFire": NOW + 10000})},
    ConnectionError("synthetic-private-edit-marker"),
])
def test_bad_or_lost_edit_ack_is_not_success_and_never_repeats(payload):
    async def run():
        client, calls = adapter([{"messages": [message()]}, {"chats": [chat()]}, payload])
        target = await client.edit_preflight(0, 21)
        with pytest.raises((PluginError, ValueError, PyMaxError, ConnectionError)):
            await client.edit(target, "new")
        assert [code for code, _ in calls] == [Opcode.MSG_GET, Opcode.CHAT_INFO, Opcode.MSG_EDIT]
    asyncio.run(run())


def test_raw_config_login2_patch_close_and_new_adapter_have_distinct_authority(tmp_path, monkeypatch):
    async def run():
        created = install_pinned_dispatch_client(monkeypatch,
            {"config": {"server": {"edit-timeout": 60}}}, [])
        store = PrivateState(tmp_path / "state", "a")
        client = PyMaxAdapter(SimpleNamespace(), store, Settings(30, None), None)
        await client.connect("qr", True)
        generation = client.edit_policy.check()

        async def emit(opcode, cmd, payload):
            await created[0].dispatcher.dispatch(InboundFrame(opcode=opcode, cmd=cmd, seq=9, payload=payload))

        await emit(Opcode.NOTIF_CONFIG, Command.REQUEST, {"config": {"server": {"unrelated": 1}}})
        assert client.edit_policy.check(generation=generation) == generation
        await emit(Opcode.NOTIF_CONFIG, Command.REQUEST, {"config": {"server": {"edit-timeout": 10}}})
        assert client.edit_policy.timeout == 10
        with pytest.raises(Unavailable):
            client.edit_policy.check(generation=generation)
        await emit(Opcode.LOGIN2, Command.RESPONSE, {"config": {"server": {"edit-timeout": 20}}})
        assert client.edit_policy.timeout == 20
        await emit(Opcode.NOTIF_CONFIG, Command.REQUEST, {"config": {"server": {"edit-timeout": "60"}}})
        with pytest.raises(Unavailable):
            client.edit_policy.check()
        await client.close()
        await emit(Opcode.NOTIF_CONFIG, Command.REQUEST, {"config": {"server": {"edit-timeout": 60}}})
        with pytest.raises(Unavailable):
            client.edit_policy.check()
        other = PyMaxAdapter(None, None, Settings(30, None), None)
        with pytest.raises(Unavailable):
            other.edit_policy.check()
        store.release()
    asyncio.run(run())


def test_edit_wire_accepts_only_plain_text_and_preserves_accounts(core_factory, tmp_path):
    core = core_factory()
    core.start()
    sign_in(core)
    params = {"account_id": "max", "chat_id": "0", "message_id": "21", "text": "**literal**", "entities": []}
    reply = core.call("messages.edit", params)["message"]
    assert (reply["chat_id"], reply["id"], reply["text"], reply["edited"]) == ("0", "21", "**literal**", True)
    assert identity(tmp_path)["edit_count"] == 1
    for change in [{"entities": [{"type": "bold", "offset": 0, "length": 1}]}, {"path": "/tmp/synthetic"},
                   {"no_webpage": False}, {"chat_id": "10"}, {"message_id": "20"}, {"text": " "}]:
        core.error("messages.edit", {**params, **change})
    assert identity(tmp_path)["edit_count"] == 1
    core.error("messages.send", {"account_id": "max", "chat_id": "0", "text": "x",
                               "entities": [{"type": "bold", "offset": 0, "length": 1}]})
    assert identity(tmp_path).get("send_count", 0) == 0
    core.start("max#2")
    sign_in(core, "max#2", "qr")
    core.call("messages.edit", {**params, "account_id": "max#2", "chat_id": "11"})
    assert identity(tmp_path)["edit_count"] == identity(tmp_path, "max_2")["edit_count"] == 1
    assert core.shutdown() == 0


@pytest.mark.parametrize("scenario,code,count", [
    ("edit-read-error", "UNAVAILABLE", 0), ("edit-generation", "UNAVAILABLE", 0),
    ("edit-prepare", "FORBIDDEN", 0), ("edit-unknown", "NETWORK", 1),
    ("edit-malformed", "NETWORK", 1), ("edit-post-generation", "NETWORK", 1),
])
def test_wire_failures_keep_preflight_definite_and_dispatched_edits_unknown(core_factory, tmp_path, scenario, code, count):
    core = core_factory(scenario)
    core.start()
    sign_in(core)
    params = {"account_id": "max", "chat_id": "0", "message_id": "21", "text": "new", "entities": []}
    error = core.error("messages.edit", params)["data"]
    assert error["code"] == code
    if count:
        assert error["outcome"] == "unknown" and error["retry_safe"] is False
    assert identity(tmp_path).get("edit_count", 0) == count
    assert "synthetic-private-edit-marker" not in core.stderr_text
    assert not any(e["method"] == "message.edited" for e in core.all_events)
    assert core.shutdown() == 0


@pytest.mark.parametrize("scenario,code,count", [("edit-read-pending", "CANCELLED", 0), ("edit-pending", "NETWORK", 1)])
def test_cancellation_before_and_after_edit_dispatch(core_factory, tmp_path, scenario, code, count):
    core = core_factory(scenario)
    core.start()
    sign_in(core)
    rid = core.send_request("messages.edit", {"account_id": "max", "chat_id": "0", "message_id": "21", "text": "new"})
    core.event("conversations.upsert", lambda p: p["conversations"][0].get("title") in {"Edit preflight pending", "Edit dispatched"})
    core.cancel(rid)
    error = core.response(rid)["error"]["data"]
    assert error["code"] == code and identity(tmp_path).get("edit_count", 0) == count
    if count:
        assert error["outcome"] == "unknown" and error["retry_safe"] is False
    assert core.call("ping") == {}
    assert core.shutdown() == 0
