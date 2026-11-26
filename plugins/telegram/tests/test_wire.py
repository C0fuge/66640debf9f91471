import os
import stat
import sys
from pathlib import Path

import pytest
from contact_hub_sdk.testing import FakeCore, schema_check

ROOT = Path(__file__).resolve().parents[1]
VALIDATE = schema_check(ROOT.parents[1] / "schemas")
CONFIG = {"api_id": 1}
SECRETS = {"api_hash": "fedcba98765432100123456789abcdef"}


@pytest.fixture
def core(tmp_path, request):
    # The fixture provides the client factory directly. Production exposes no
    # environment switch that can silently replace a real account with a stub.
    bootstrap = """
import os, sys
def no_network(event, args):
    if event == 'socket.connect':
        raise AssertionError('Network is disabled in Telegram tests.')
sys.addaudithook(no_network)
os.umask(0o077)
sys.path.insert(0, 'tests')
from stub import Stub
from telegram_plugin.plugin import TelegramPlugin
plugin = TelegramPlugin(factory=Stub)
if sys.argv[1] == 'legacy':
    plugin.api_version = '1.0.0'
    plugin.capabilities = {k: v for k, v in plugin.capabilities.items() if k != 'chat.get'}
elif sys.argv[1] == 'empty-object':
    plugin.capabilities = {**plugin.capabilities, 'chat.get': {}}
elif sys.argv[1] == 'without-archive':
    plugin.capabilities = {**plugin.capabilities, 'chat.archive': False}
elif sys.argv[1] == 'without-mentions':
    plugin.capabilities = {k: v for k, v in plugin.capabilities.items() if k != 'mentions.unread'}
elif sys.argv[1] == 'source-close':
    from contextlib import contextmanager
    from contact_hub_sdk import plugin as sdk_plugin, types as dto
    from telegram_plugin import uploads
    held = []
    original_file = uploads.local_file
    @contextmanager
    def tracked_file(path):
        with original_file(path) as opened:
            held.append(opened[0])
            yield opened
    uploads.local_file = tracked_file
    original_wire = sdk_plugin.to_wire
    def checked_wire(result):
        if isinstance(result, dto.MessageResult) and result.message.media is not None:
            assert held and all(source.closed for source in held), 'response preceded source closure'
        return original_wire(result)
    sdk_plugin.to_wire = checked_wire
plugin.run()
"""
    with FakeCore([sys.executable, "-c", bootstrap, getattr(request, "param", "default")], plugin_dir=ROOT, tmp=tmp_path,
                  env={**os.environ, "HOME": str(tmp_path / "home")}, check=VALIDATE) as harness:
        yield harness


def start(core, mode=1, name="a"):
    core.start(name, {"api_id": mode}, SECRETS)
    return core.event("status", account_id=name, state="auth_required" if mode == 2 else "connected")


def test_wire_and_snapshot(core):
    VALIDATE(core.manifest, "manifest.json")
    caps = core.initialize()["capabilities"]
    assert "auth.interactive" in caps and caps["edit"] is True
    start(core)
    assert core.event("conversations.synced")["chat_ids"] == ["17"]
    assert core.event("contacts.upsert")["contacts"][0]["display_name"] == "Sample"
    assert core.event("messages.upsert")["messages"][0]["id"] == "10"
    assert len(core.call("contacts.list", {"account_id": "a"})["contacts"]) == 1
    core.call("chats.sync", {"account_id": "a", "include_archived": False})
    assert sum(e["method"] == "conversations.synced" for e in core.all_events) == 2
    assert core.shutdown() == 0


def test_isolated_sessions_and_stop(core):
    start(core, name="first")
    start(core, name="second")
    for name in ["first", "second"]:
        directory = core.tmp / "accounts" / name / "state"
        assert stat.S_IMODE(directory.stat().st_mode) == 0o700
        path = directory / "telegram.session"
        assert path.read_bytes().startswith(b"SQLite format 3")
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    shared = core.start_params("third", CONFIG, SECRETS, state_dir=str(directory))
    assert core.error("account.start", shared)["data"]["field"] == "state_dir"
    core.stop("first")
    assert core.event("status", account_id="first", state="disconnected")
    assert core.call("messages.fetch_recent", {"account_id": "second", "chat_id": "17", "limit": 1})["messages"]
    start(core, name="first")
    assert core.shutdown() == 0


def test_invalid_settings(core):
    core.initialize()
    for settings, secrets, field in [({}, SECRETS, "api_id"), (CONFIG, {}, "api_hash"),
                                     ({**CONFIG, "api_hash": SECRETS["api_hash"]}, {}, "api_hash"),
                                     ({**CONFIG, "proxy_host": "auth@host"}, SECRETS, "proxy_host")]:
        assert core.error("account.start", core.start_params("a", settings, secrets))["data"]["field"] == field
    assert SECRETS["api_hash"] not in core.stderr_text
    assert core.shutdown() == 0


def test_auth(core):
    start(core, mode=2)
    a = {"account_id": "a"}
    core.call("auth.begin", {**a, "method": "phone_code"})

    def submit(kind, value, error=False):
        step = core.event("auth.step")["step"]
        assert step["kind"] == kind and bool(step.get("error")) == error
        core.call("auth.submit", {**a, "flow": "sign_in", "step_id": step["step_id"], "value": value})

    submit("phone", "invalid-phone")
    submit("phone", "+19995550000", True)
    submit("code", "00000")
    submit("code", "55881", True)
    submit("password", "wrong-password")
    submit("password", "unique-fixture-password", True)
    assert core.event("auth.step")["step"]["kind"] == "done"
    core.event("status", state="connected")
    assert core.shutdown() == 0
    assert all(value not in core.stderr_text for value in [SECRETS["api_hash"], "+19995550000", "55881", "unique-fixture-password"])


def test_auth_cancel_and_qr_2fa(core):
    start(core, mode=2)
    a = {"account_id": "a"}
    core.call("auth.begin", {**a, "method": "phone_code"})
    old = core.event("auth.step")["step"]["step_id"]
    core.call("auth.cancel", {**a, "flow": "sign_in"})
    core.call("auth.submit", {**a, "flow": "sign_in", "step_id": old, "value": "late-value"})
    core.call("auth.begin", {**a, "method": "qr"})
    qr = core.event("auth.step")["step"]
    assert qr["kind"] == "qr" and qr["data"].startswith("tg://login?")
    password = core.event("auth.step")["step"]
    assert password["kind"] == "password"
    core.call("auth.submit", {**a, "flow": "sign_in", "step_id": password["step_id"], "value": "unique-fixture-password"})
    core.event("status", state="connected")
    assert core.shutdown() == 0
    assert "synthetic-qr" not in core.stderr_text


@pytest.mark.parametrize("mode,reconnecting", [(3, False), (5, True)])
def test_recovery(core, mode, reconnecting):
    core.start("a", {"api_id": mode}, SECRETS)
    if reconnecting:
        core.event("status", state="connected")
    core.event("status", state="unavailable", reconnecting=reconnecting)
    assert core.call("ping") == {}
    core.event("status", state="connecting", reconnecting=reconnecting)
    core.event("status", state="connected")
    assert core.shutdown() == 0
    assert "synthetic-private-error" not in core.stderr_text


def test_failed_sync(core):
    core.start("a", {"api_id": 4}, SECRETS)
    assert core.event("status", state="error")["detail"] == "Telegram operation failed."
    assert not any(e["params"].get("state") == "connected" for e in core.all_events)
    assert core.shutdown() == 0
    assert "synthetic-private-error" not in core.stderr_text


def test_history(core):
    start(core)
    p = {"account_id": "a", "chat_id": "17", "limit": 3}
    def ids(page):
        return [int(m["id"]) for m in page["messages"]]
    page = core.call("messages.fetch_recent", p)
    assert ids(page) == [10, 9, 8]
    next_page = core.call("messages.fetch_recent", {**p, "cursor": page["next_cursor"]})
    assert ids(next_page) == [7, 6, 5]
    for bad in ["broken", "9" * 10000]:
        assert core.error("messages.fetch_recent", {**p, "cursor": bad}, check=False)["data"]["code"] == "INVALID_PARAMS"
    assert core.error("messages.fetch_recent", {**p, "topic_id": "42", "cursor": page["next_cursor"]})["data"]["field"] == "cursor"
    around = core.call("history.around", {**p, "at": "2025-01-01T00:00:05Z"})
    assert ids(around) == [6, 5, 4]
    assert ids(core.call("messages.fetch_recent", {**p, "cursor": around["older_cursor"]})) == [3, 2, 1]
    gap = {**p, "after_message_id": "6", "before_message_id": "10", "limit": 2}
    forward = core.call("history.fill_gap", {**gap, "cursor": around["newer_cursor"]})
    assert ids(forward) == [7, 8]
    end = core.call("history.fill_gap", {**gap, "cursor": forward["next_cursor"]})
    assert ids(end) == [9] and not end.get("next_cursor")
    assert core.error("history.fill_gap", {**gap, "before_message_id": "6"})["data"]["field"] == "before_message_id"
    assert ids(core.call("history.around", {**p, "at": "2000-01-01T00:00:00Z"})) == [3, 2, 1]
    assert ids(core.call("history.around", {**p, "at": "2050-01-01T00:00:00Z"})) == [10, 9, 8]
    assert core.shutdown() == 0


def test_send(core):
    start(core)
    p = {"account_id": "a", "chat_id": "17", "text": "😀x", "client_id": "fixture-key",
         "entities": [{"type": "bold", "offset": 2, "length": 1}]}
    sent = core.call("messages.send", p)["message"]
    assert sent["ext"]["telegram"]["client_id"] == "fixture-key"
    assert sent["entities"] == p["entities"]
    echo = core.event("messages.upsert", where=lambda e: e["messages"][0].get("ext") is not None)
    assert echo["messages"][0] == sent
    assert core.call("messages.send", p)["message"]["id"] == sent["id"]
    empty_key = core.call("messages.send", {**p, "client_id": ""})["message"]
    assert empty_key["ext"]["telegram"]["client_id"] == ""
    assert core.call("messages.send", {**p, "client_id": "another"})["message"]["id"] != sent["id"]
    assert core.error("messages.send", {**p, "entities": [{"type": "bold", "offset": 1, "length": 1}]})["data"]["field"] == "entities"
    assert core.error("messages.send", {**p, "schedule_at": "2025-04-03T12:00:00Z"})["data"]["code"] == "NOT_SUPPORTED"
    assert core.error("messages.send", {**p, "text": "wait"})["data"] == {"code": "FLOOD_WAIT", "seconds": 23}
    error = core.error("messages.send", {**p, "text": "explode"})
    assert error["data"]["code"] == "NETWORK"
    assert "synthetic-private-error" not in str(error) + core.stderr_text
    assert core.shutdown() == 0


def test_eof_during_sign_in(core):
    start(core, mode=2)
    core.call("auth.begin", {"account_id": "a", "method": "phone_code"})
    core.event("auth.step")
    core.close_stdin()
    assert core.wait_exit(timeout=5) == 0


def test_message_actions(core):
    start(core)
    p = {"account_id": "a", "chat_id": "17", "message_id": "4"}
    assert core.call("messages.get", p)["message"]["text"] == "Row 4"
    edited = core.call("messages.edit", {**p, "text": "😀new", "entities": [{"type": "bold", "offset": 2, "length": 3}],
                                           "no_webpage": True})["message"]
    assert edited["text"] == "😀new" and edited["edited"]
    assert core.call("messages.edit", {**p, "text": "😀new", "entities": edited["entities"]})["message"]["id"] == "4"
    assert core.error("messages.edit", {**p, "path": "/synthetic/file"})["data"]["code"] == "NOT_SUPPORTED"
    core.call("messages.pin", {**p, "silent": True, "for_me_only": True})
    core.call("messages.unpin", p)
    core.call("messages.unpin", {"account_id": "a", "chat_id": "17"})
    forwarded = core.call("messages.forward", {"account_id": "a", "from_chat_id": "17", "to_chat_id": "18",
                                                "message_ids": ["4", "5"], "drop_author": True,
                                                "drop_captions": True, "silent": True})
    assert len(forwarded["messages"]) == 2
    assert all(m["chat_id"] == "18" for m in forwarded["messages"])
    core.call("messages.delete", {"account_id": "a", "chat_id": "17", "message_ids": ["4"], "revoke": False})
    assert not core.call("messages.get", p).get("message")
    # An ID from another DM must not be edited, deleted, forwarded or pinned.
    wrong = {**p, "message_id": "99"}
    for method, params in [("messages.get", wrong), ("messages.edit", {**wrong, "text": "x"}),
                           ("messages.pin", wrong), ("messages.unpin", wrong),
                           ("messages.delete", {"account_id": "a", "chat_id": "17", "message_ids": ["5", "99"], "revoke": True}),
                           ("messages.forward", {"account_id": "a", "from_chat_id": "17", "to_chat_id": "18", "message_ids": ["99"]})]:
        assert core.error(method, params)["data"]["code"] == "NOT_FOUND"
    assert core.call("messages.get", {**p, "message_id": "5"})["message"]
    assert core.shutdown() == 0


def test_activity_and_drafts(core):
    start(core)
    p = {"account_id": "a", "chat_id": "17"}
    core.call("read.send_receipt", {**p, "max_message_id": "10"})
    core.call("read.send_receipt", {**p, "chat_id": "-1000000000008", "topic_id": "4", "max_message_id": "10"})
    core.call("typing.send", {**p, "action": "typing"})
    core.call("typing.send", {**p, "action": "cancel"})
    assert core.error("typing.send", {**p, "action": "speaking"})["data"]["code"] == "NOT_SUPPORTED"
    core.call("drafts.save", {**p, "draft": {"text": "😀note", "reply_to": "4",
                                            "entities": [{"type": "italic", "offset": 2, "length": 4}]}})
    core.call("drafts.save", {**p, "draft": {"text": "", "reply_to": "4"}})
    assert core.error("drafts.save", {**p, "topic_id": "4", "draft": {"text": "x"}})["data"]["code"] == "NOT_SUPPORTED"
    assert core.shutdown() == 0


def test_media_downloads_and_avatars(core):
    start(core)
    p = {"account_id": "a", "chat_id": "17", "message_id": "100", "what": "full", "priority": 3}
    info = core.call("messages.get", {k: v for k, v in p.items() if k not in ("what", "priority")})["message"]["media"]
    assert info["kind"] == "document" and info["filename"] == "../../remote.bin"
    result = core.call("media.download", p)
    path = Path(result["path"])
    assert path.is_relative_to(core.tmp / "accounts" / "a" / "cache") and path.read_bytes() == b"fixture"
    progress = core.event("transfer.progress", where=lambda e: e.get("done_bytes") == 7)
    assert progress["request_id"] is not None and progress["direction"] == "download"
    assert core.event("media.ready")["path"] == result["path"]
    thumb = core.call("media.download", {**p, "message_id": "101", "what": "thumb"})
    assert thumb["mime"] == "image/jpeg" and Path(thumb["path"]).read_bytes() == b"img"
    avatar = core.call("avatars.download", {"account_id": "a", "kind": "user", "id": "17", "size": "small"})
    assert Path(avatar["path"]).read_bytes() == b"avatar"
    assert core.event("avatar.ready")["path"] == avatar["path"]
    assert not core.call("avatars.download", {"account_id": "a", "kind": "user", "id": "18"}).get("path")
    assert core.shutdown() == 0


def test_file_and_album_upload_wire(core, tmp_path):
    start(core)
    path = tmp_path / "fixture.bin"
    path.write_bytes(b"fixture")
    p = {"account_id": "a", "chat_id": "17", "path": str(path), "caption": "**😀x**",
         "entities": [{"type": "bold", "offset": 2, "length": 3}], "filename": "custom.bin",
         "mime": "application/octet-stream", "silent": True, "as_file": True, "spoiler": True}
    result = core.call("messages.send_file", p)["message"]
    assert result["text"] == p["caption"] and result["entities"] == p["entities"]
    assert result["media"]["filename"] == "custom.bin" and result["media"]["has_spoiler"]
    assert core.event("transfer.progress", where=lambda e: e["direction"] == "upload")["request_id"] is not None
    image = tmp_path / "fixture.png"
    image.write_bytes(b"\x89PNG\r\n\x1a\nfixture")
    photo = core.call("messages.send_file", {"account_id": "a", "chat_id": "-1000000000008", "path": str(image),
                         "kind": "photo", "topic_id": "42", "reply_to": {"message_id": "7"}, "spoiler": True})["message"]
    assert photo["media"]["kind"] == "photo" and photo["topic_id"] == "42"
    album = core.call("messages.send_album", {"account_id": "a", "chat_id": "17", "caption": "Album",
              "items": [{"path": str(path), "as_file": True, "spoiler": True}, {"path": str(path), "caption": "Second"}]})["messages"]
    assert len(album) == 2 and [m["text"] for m in album] == ["Album", "Second"]
    assert album[0]["media"]["has_spoiler"] and album[0]["grouped_id"] == album[1]["grouped_id"]
    assert core.error("messages.send_file", {**p, "kind": "video_note"})["data"]["code"] == "NOT_SUPPORTED"
    assert core.error("messages.send_file", {**p, "kind": "video"})["data"]["code"] == "NOT_SUPPORTED"
    assert core.error("messages.send_file", {**p, "duration": 3})["data"]["field"] == "duration"
    assert core.shutdown() == 0


def test_poll_family_wire_and_account_scope(core):
    start(core)
    start(core, name="b")
    p = {"account_id": "a", "chat_id": "-1000000000008", "topic_id": "42", "reply_to": {"message_id": "7"},
         "question": "**😀Question**", "options": ["**A**", "B"], "quiz": True, "correct_option": 1,
         "anonymous": False, "multiple": False, "silent": False, "close_after_seconds": 30,
         "solution": "😀why", "solution_entities": [{"type": "bold", "offset": 2, "length": 3}]}
    created = core.call("polls.create", p)["message"]
    poll = created["poll"]
    assert poll["question"] == p["question"] and [a["text"] for a in poll["options"]] == p["options"]
    assert not poll["anonymous"] and poll["quiz"] and created["topic_id"] == "42"
    assert poll["close_at"] == "2025-01-01T00:00:30.000Z"
    target = {"account_id": "a", "chat_id": p["chat_id"], "message_id": created["id"]}
    voted = core.call("polls.vote", {**target, "option_ids": [poll["options"][1]["id"]]})["message"]["poll"]
    assert voted["options"][1]["chosen"] and voted["options"][1]["correct"]
    assert voted["solution"] == "😀why" and voted["solution_entities"] == p["solution_entities"]
    assert core.error("polls.vote", {**target, "account_id": "b", "option_ids": []})["data"]["code"] == "NOT_FOUND"
    closed = core.call("polls.close", target)["message"]["poll"]
    assert closed["closed"]
    assert core.call("polls.close", target)["message"]["poll"]["closed"]
    assert core.error("polls.vote", {**target, "option_ids": []})["data"]["code"] == "FORBIDDEN"
    assert core.error("polls.create", {**p, "schedule_at": "2027-01-01T00:00:00Z"})["data"]["code"] == "NOT_SUPPORTED"
    assert core.error("polls.create", {**p, "reply_to": {"message_id": "7", "quote_text": "Q"}})["data"]["code"] == "NOT_SUPPORTED"
    assert core.shutdown() == 0


def test_poll_retraction_and_uncertain_send_are_truthful(core):
    start(core)
    p = {"account_id": "a", "chat_id": "17", "question": "Plain", "options": ["A", "B"], "multiple": True}
    created = core.call("polls.create", p)["message"]
    target = {"account_id": "a", "chat_id": "17", "message_id": created["id"]}
    ids = [option["id"] for option in created["poll"]["options"]]
    assert all(option["chosen"] for option in core.call("polls.vote", {**target, "option_ids": ids})["message"]["poll"]["options"])
    assert all(not option["chosen"] for option in core.call("polls.vote", {**target, "option_ids": []})["message"]["poll"]["options"])
    error = core.error("polls.create", {**p, "question": "explode-poll"})
    assert error["data"]["code"] == "NETWORK"
    assert "synthetic-private-poll-error" not in str(error) + core.stderr_text
    assert core.shutdown() == 0


def test_messages_get_hydrates_sparse_personal_poll_results(core):
    start(core, mode=6)
    created = core.call("polls.create", {"account_id": "a", "chat_id": "17", "question": "Personal results", "options": ["A", "B"]})["message"]
    p = {"account_id": "a", "chat_id": "17", "message_id": created["id"]}
    core.call("polls.vote", {**p, "option_ids": [created["poll"]["options"][0]["id"]]})
    read = core.call("messages.get", p)["message"]["poll"]
    assert [value["chosen"] for value in read["options"]] == [True, False]
    assert read["total_voters"] == 1
    assert core.shutdown() == 0


def test_ordinary_reactions_wire_snapshot_paging_and_account_isolation(core):
    caps = core.initialize()["capabilities"]
    assert caps["reactions.set"]["max"] == 100 and caps["reactions.custom"] and caps["reactions.who"]
    assert caps["reactions.unread"] is True
    assert all(name not in caps for name in ("paid.react", "reactions.effects"))
    start(core, mode=7)
    start(core, name="b")
    available = core.call("reactions.available", {"account_id": "a", "chat_id": "17"})
    assert available == {"reactions": [{"emoji": "❤️"}, {"emoji": "👍"}], "allow_custom": False, "max_per_message": 1}
    p = {"account_id": "a", "chat_id": "17", "message_id": "4"}
    selected = core.call("reactions.set", {**p, "reactions": [{"emoji": "❤️"}], "big": True})["message"]
    assert selected["reactions"] == [{"key": {"emoji": "❤️"}, "count": 1, "mine": True}]
    assert core.call("messages.get", p)["message"]["reactions"] == selected["reactions"], "get hydrates the stub's minimal view"
    assert "reactions" not in core.call("messages.get", {**p, "account_id": "b"})["message"]
    custom = core.call("reactions.set", {**p, "reactions": [{"custom_emoji_id": "9007199254740993"}]})["message"]
    assert custom["reactions"][0]["key"] == {"custom_emoji_id": "9007199254740993"}
    assert core.call("reactions.set", {**p, "reactions": []})["message"]["reactions"] == []
    first = core.call("reactions.who", {**p, "limit": 2})
    assert first["reactors"][0]["user_id"] == "17" and first["reactors"][0]["name"] == "Sample"
    assert first["next_cursor"], "a short page with a server continuation is not exhausted"
    assert core.error("reactions.who", {**p, "account_id": "b", "limit": 2, "cursor": first["next_cursor"]})["data"]["field"] == "cursor"
    assert core.call("reactions.who", {**p, "limit": 2, "cursor": first["next_cursor"]}) == {"reactors": []}
    assert core.error("reactions.set", {**p, "reactions": [{"paid": True}]})["data"]["code"] == "NOT_SUPPORTED"
    assert core.error("reactions.set", {**p, "message_id": "99", "reactions": []})["data"]["code"] == "NOT_FOUND"
    error = core.error("reactions.set", {**p, "reactions": [{"emoji": "explode-reaction"}]})
    assert error["data"]["code"] == "NETWORK"
    assert "synthetic-private-reaction-error" not in str(error) + core.stderr_text
    assert core.shutdown() == 0


def test_unread_reactions_wire_oldest_batch_topic_read_and_account_isolation(core):
    start(core)
    start(core, name="b")
    p = {"account_id": "a", "chat_id": "17"}
    assert core.call("reactions.list_unread", {**p, "limit": 2}) == {"message_ids": ["2", "5"]}
    assert core.call("reactions.read", p) == {}, "positive server acknowledgements must be finished before success"
    assert core.call("reactions.list_unread", {**p, "limit": 3}) == {"message_ids": []}
    assert core.call("reactions.list_unread", {**p, "account_id": "b", "limit": 3}) == {"message_ids": ["2", "5", "9"]}
    topic = {"account_id": "a", "chat_id": "-1000000000008", "topic_id": "42"}
    assert core.call("reactions.list_unread", {**topic, "topic_id": None, "limit": 3}) == {"message_ids": ["11", "41", "45"]}
    assert core.call("reactions.list_unread", {**topic, "limit": 2}) == {"message_ids": ["41", "45"]}
    assert core.call("reactions.read", topic) == {}
    assert core.call("reactions.list_unread", {**topic, "limit": 2}) == {"message_ids": []}
    assert core.call("reactions.list_unread", {**topic, "topic_id": "1", "limit": 2}) == {"message_ids": ["11"]}, "reading topic42 must not clear General"
    assert core.call("reactions.read", {**topic, "topic_id": None}) == {}
    assert core.call("reactions.list_unread", {**topic, "topic_id": None, "limit": 3}) == {"message_ids": []}
    bot = {"account_id": "a", "chat_id": "18", "topic_id": "7"}
    assert core.call("reactions.list_unread", {**bot, "limit": 2}) == {"message_ids": ["70", "72"]}
    assert core.call("reactions.read", bot) == {}
    assert core.call("reactions.list_unread", {**bot, "limit": 2}) == {"message_ids": []}
    assert core.error("reactions.read", {**p, "topic_id": "42"})["data"]["field"] == "topic_id"
    assert core.shutdown() == 0


def test_unread_read_network_error_stays_unknown_and_redacted(core):
    start(core, mode=8)
    p = {"account_id": "a", "chat_id": "17"}
    error = core.error("reactions.read", p)
    assert error["data"] == {"code": "NETWORK", "outcome": "unknown", "retry_safe": False}
    assert core.call("reactions.list_unread", {**p, "limit": 2}) == {"message_ids": ["2", "5"]}
    assert "synthetic-private-unread-error" not in str(error) + core.stderr_text
    assert core.shutdown() == 0


def test_cancelling_dispatched_unread_write_preserves_unknown_through_sdk_wire(core):
    start(core, mode=9)
    p = {"account_id": "a", "chat_id": "17"}
    request_id = core.send_request("reactions.read", p)
    assert core.call("ping") == {}, "the later ping runs after the read handler has reached its client await"
    core.cancel(request_id)
    response = core.response(request_id)
    assert response["error"]["data"] == {"code": "NETWORK", "outcome": "unknown", "retry_safe": False}
    assert core.call("reactions.list_unread", {**p, "limit": 2}) == {"message_ids": ["2", "5"]}
    assert core.shutdown() == 0


def test_dialog_flags_capabilities_scope_and_sparse_ack(core):
    caps = core.initialize()["capabilities"]
    assert all(caps[key] is True for key in ("chat.flags", "chat.pin", "chat.mark_unread", "chat.archive"))
    assert not caps.get("chat.mute")
    start(core)
    start(core, name="b")
    p = {"account_id": "a", "chat_id": "17"}
    method = "chats.set_flags"
    start_events = len(core.all_events)
    for field, value, cap in [("muted", False, "chat.mute"),
                              ("mute_until", "2027-01-01T00:00:00Z", "chat.mute")]:
        error = core.error(method, {**p, "chat_id": "invalid", "pinned": True, field: value})
        assert error["data"] == {"code": "NOT_SUPPORTED", "capability": cap}
    assert core.call(method, {**p, "archived": None, "marked_unread": None}) == {}
    assert core.error(method, {**p, "chat_id": "017"})["data"]["field"] == "chat_id"
    assert len(core.all_events) == start_events

    for pin, mark in [(True, True), (False, True), (True, False), (False, False)]:
        assert core.call(method, {**p, "pinned": pin, "marked_unread": mark}) == {}
        event = core.all_events[-1]
        assert event["method"] == "conversations.upsert" and "id" in event
        assert event["params"]["conversations"] == [{"id": "17", "kind": "dm", "pinned": pin, "marked_unread": mark}]
    assert core.call(method, {**p, "pinned": True}) == {}
    assert len(core.all_events) == start_events + 5
    for owner, expected in [("a", True), ("b", False)]:
        before = len(core.all_events)
        assert core.call("chats.sync", {"account_id": owner}) == {}
        conversation = next(e["params"]["conversations"][0] for e in core.all_events[before:]
                            if e["method"] == "conversations.upsert")
        assert conversation["pinned"] is expected and conversation["marked_unread"] is False
        assert conversation["read_inbox_max_id"] == "2" and conversation["title"] == "Sample"


def test_dialog_flags_partial_refusal_and_lost_ack(core):
    start(core, mode=10)
    method = "chats.set_flags"
    params = {"account_id": "a", "chat_id": "17", "pinned": True, "marked_unread": True}
    before = len(core.all_events)
    error = core.error(method, params)
    assert error["data"] == {"code": "NETWORK", "reason": "outcome_unknown", "outcome": "unknown", "retry_safe": False}
    assert len(core.all_events) == before + 1
    assert core.all_events[-1]["params"]["conversations"] == [
        {"id": "17", "kind": "dm", "pinned": True, "marked_unread": False}]
    core.auto_answer = False
    for desired, cancel in [(False, True), (True, False)]:
        rid = core.send_request(method, {**params, "pinned": desired, "marked_unread": None})
        event = core.request("conversations.upsert")
        assert event["params"]["conversations"] == [{"id": "17", "kind": "dm", "pinned": desired}]
        if cancel:
            core.cancel(rid)
        else:
            core.answer(event, error={"code": -32007, "message": "Timed out", "data": {"code": "TIMEOUT"}})
        data = core.response(rid)["error"]["data"]
        assert data["outcome"] == "unknown" and data["retry_safe"] is False
    core.auto_answer = True


@pytest.mark.parametrize("core", ["default", "empty-object"], indirect=True)
def test_chat_get_observes_the_selected_account_without_events(core):
    result = core.initialize()
    assert result["api_version"] == "1.2.0" and result["capabilities"]["chat.get"] in (True, {})
    start(core, name="a")
    start(core, name="b")
    core.call("chats.set_flags", {"account_id": "a", "chat_id": "17", "marked_unread": True})
    before = len(core.all_events)
    for account, marked in [("a", True), ("b", False)]:
        assert core.call("chats.get", {"account_id": account, "chat_id": "17"}) == {"conversation": {
            "id": "17", "kind": "dm", "pinned": False, "marked_unread": marked, "archived": False}}
    for owner, chat, code in [("missing", "17", "UNKNOWN_ACCOUNT"), ("a", "017", "INVALID_PARAMS"),
                              ("a", "99", "NOT_FOUND")]:
        assert core.error("chats.get", {"account_id": owner, "chat_id": chat})["data"]["code"] == code
    assert len(core.all_events) == before


@pytest.mark.parametrize("core", ["legacy"], indirect=True)
def test_legacy_telegram_capability_gate_precedes_account_lookup(core):
    result = core.initialize()
    assert result["api_version"] == "1.0.0" and "chat.get" not in result["capabilities"]
    error = core.error("chats.get", {"account_id": "missing", "chat_id": "17"})
    assert error["data"] == {"code": "NOT_SUPPORTED", "capability": "chat.get"}


def test_archive_is_account_scoped_and_sparse_through_sdk_wire(core):
    start(core, name="a")
    start(core, name="b")
    p = {"account_id": "a", "chat_id": "17"}
    assert core.call("chats.set_flags", {**p, "pinned": True}) == {}
    for desired in (True, False):
        before = len(core.all_events)
        assert core.call("chats.set_flags", {**p, "archived": desired}) == {}
        events = core.all_events[before:]
        assert len(events) == 1 and events[0]["method"] == "conversations.upsert"
        assert events[0]["params"]["conversations"] == [
            {"id": "17", "kind": "dm", "archived": desired, "pinned": False}]
        assert core.call("chats.get", p)["conversation"]["archived"] is desired
        assert core.call("chats.get", {**p, "account_id": "b"})["conversation"]["archived"] is False
        before = len(core.all_events)
        assert core.call("chats.sync", {**p, "include_archived": True}) == {}
        full = next(e["params"]["conversations"][0] for e in core.all_events[before:]
                    if e["method"] == "conversations.upsert")
        assert full["archived"] is desired
        assert full["title"] == "Sample" and full["read_inbox_max_id"] == "2"
    assert core.shutdown() == 0


@pytest.mark.parametrize("core", ["without-archive"], indirect=True)
def test_archive_false_requires_the_same_capability_and_preflights_mixed_changes(core):
    assert core.initialize()["capabilities"].get("chat.archive") in (None, False)
    start(core, name="a")
    before = len(core.all_events)
    for desired in (True, False):
        error = core.error("chats.set_flags", {"account_id": "a", "chat_id": "invalid",
                                               "archived": desired, "pinned": True})
        assert error["data"] == {"code": "NOT_SUPPORTED", "capability": "chat.archive"}
    assert len(core.all_events) == before
    assert core.call("chats.get", {"account_id": "a", "chat_id": "17"})["conversation"]["pinned"] is False
    assert core.shutdown() == 0


def test_mentions_are_scoped_oldest_bounded_and_clear_only_the_requested_topic(core):
    assert core.initialize()["capabilities"]["mentions.unread"] is True
    start(core, name="a")
    start(core, name="b")
    p = {"account_id": "a", "chat_id": "-1000000000008", "topic_id": "42"}
    before = len(core.all_events)
    assert core.call("mentions.list_unread", {**p, "limit": 1}) == {"message_ids": ["41"]}
    assert core.call("mentions.list_unread", {**p, "limit": 200}) == {"message_ids": ["41", "45"]}
    assert core.call("mentions.read", p) == {}
    assert core.call("mentions.list_unread", {**p, "limit": 2}) == {"message_ids": []}
    assert core.call("mentions.list_unread", {**p, "topic_id": "43", "limit": 2}) == {"message_ids": ["50"]}
    assert core.call("mentions.list_unread", {**p, "topic_id": "1", "limit": 2}) == {"message_ids": ["11"]}
    assert core.call("mentions.list_unread", {**p, "account_id": "b", "limit": 2}) == {"message_ids": ["41", "45"]}
    assert core.call("mentions.read", {**p, "account_id": "b", "topic_id": None}) == {}
    assert core.call("mentions.list_unread", {**p, "account_id": "b", "topic_id": None, "limit": 200}) == {"message_ids": []}
    assert core.call("mentions.list_unread", {**p, "chat_id": "-8", "topic_id": None, "limit": 200}) == {"message_ids": ["2", "5"]}
    assert core.call("mentions.read", {**p, "chat_id": "-8", "topic_id": None}) == {}
    assert len(core.all_events) == before, "mention acknowledgements never fabricate counters or message changes"
    assert core.shutdown() == 0


@pytest.mark.parametrize("core", ["without-mentions"], indirect=True)
def test_missing_mentions_capability_rejects_before_owner_or_scope_lookup(core):
    assert not core.initialize()["capabilities"].get("mentions.unread")
    for method in ("mentions.list_unread", "mentions.read"):
        p = {"account_id": "missing", "chat_id": "invalid", "limit": 2} if method.endswith("list_unread") else {
            "account_id": "missing", "chat_id": "invalid"}
        assert core.error(method, p)["data"] == {"code": "NOT_SUPPORTED", "capability": "mentions.unread"}
    assert core.shutdown() == 0


def test_mention_writes_refuse_ghost_and_unavailable_scopes_without_success(core):
    core.start("ghost", CONFIG, SECRETS, ghost=True)
    core.event("status", account_id="ghost", state="connected")
    p = {"account_id": "ghost", "chat_id": "-1000000000008"}
    assert core.error("mentions.read", p)["data"] == {"code": "NOT_SUPPORTED", "capability": "mentions.unread"}
    assert core.call("mentions.list_unread", {**p, "topic_id": "42", "limit": 2}) == {"message_ids": ["41", "45"]}
    start(core, name="a")
    assert core.error("mentions.read", {**p, "account_id": "a", "chat_id": "17"})["data"]["code"] == "NOT_SUPPORTED"
    core.stop("a")
    assert core.error("mentions.read", {**p, "account_id": "a"})["data"]["code"] == "UNKNOWN_ACCOUNT"
    assert core.shutdown() == 0


def test_lost_and_cancelled_mention_writes_remain_unknown_through_sdk(core):
    start(core, mode=11, name="failed")
    p = {"account_id": "failed", "chat_id": "-1000000000008", "topic_id": "42"}
    error = core.error("mentions.read", p)
    assert error["data"] == {"code": "NETWORK", "outcome": "unknown", "retry_safe": False}
    assert "synthetic-private-mention-error" not in str(error) + core.stderr_text
    assert core.call("mentions.list_unread", {**p, "limit": 2}) == {"message_ids": ["41", "45"]}
    start(core, mode=12, name="cancelled")
    p["account_id"] = "cancelled"
    request = core.send_request("mentions.read", p)
    assert core.call("ping") == {}
    core.cancel(request)
    assert core.response(request)["error"]["data"] == {"code": "NETWORK", "outcome": "unknown", "retry_safe": False}
    assert core.call("mentions.list_unread", {**p, "limit": 2}) == {"message_ids": ["41", "45"]}
    assert core.shutdown() == 0


@pytest.mark.parametrize("core", ["source-close"], indirect=True)
def test_send_file_success_is_serialized_only_after_the_real_source_handle_closes(core, tmp_path):
    from contact_hub_sdk import file_source_released_on_success
    result = core.initialize()
    assert file_source_released_on_success(result["capabilities"])
    start(core)
    selected = tmp_path / "synthetic-source.bin"
    selected.write_bytes(b"fixture")
    reply = core.call("messages.send_file", {"account_id":"a", "chat_id":"17", "path":str(selected), "kind":"document"})
    assert reply["message"]["media"]["kind"] == "document"
    selected.unlink()
    assert core.call("ping") == {}
    assert core.shutdown() == 0
