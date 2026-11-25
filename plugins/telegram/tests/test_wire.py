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
def core(tmp_path):
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
TelegramPlugin(factory=Stub).run()
"""
    with FakeCore([sys.executable, "-c", bootstrap], plugin_dir=ROOT, tmp=tmp_path,
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
    assert all(name not in caps for name in ("paid.react", "reactions.effects", "reactions.unread"))
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
