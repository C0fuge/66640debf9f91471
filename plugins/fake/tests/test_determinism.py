"""Same seed => byte-identical frames and files. The session below also calls every method
of every declared capability once, so it doubles as a "declared means implemented" check."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from conftest import PLUGIN_DIR, FakeCore, method_caps, validate

SETTINGS = {"seed": 7, "chats": 14, "messages_per_chat": 80, "transfer_chunk_ms": 0}


def _find(core: FakeCore, pred, chats) -> tuple[str, dict] | None:
    for cid in chats:
        cursor = None
        while True:
            p = {"account_id": "fake", "chat_id": cid, "limit": 100}
            if cursor:
                p["cursor"] = cursor
            r = core.call("messages.fetch_recent", p)
            for m in r["messages"]:
                if pred(m):
                    return cid, m
            cursor = r.get("next_cursor")
            if not cursor:
                break
    return None


def session(core: FakeCore, upload: Path) -> None:
    A = {"account_id": "fake"}
    init = core.initialize()
    validate(init["capabilities"], "capabilities.json")
    core.start(settings=SETTINGS)
    core.event("status", lambda p: p["state"] == "auth_required")
    core.call("auth.begin", {**A, "method": "qr"})
    assert core.event("auth.step")["step"]["kind"] == "qr"
    core.call("auth.cancel", {**A, "flow": "sign_in"})
    core.event("status", lambda p: p["state"] == "auth_required")
    core.call("auth.begin", {**A, "method": "phone_code"})
    for value in ("+15550000001", "00000", "12345", "wrong", "fake"):
        step = core.event("auth.step")["step"]
        core.call("auth.submit", {**A, "flow": "sign_in", "step_id": step["step_id"], "value": value})
    sync = core.wait_synced()
    chats = sync["conversations.synced"][0]["chat_ids"]
    core.call("ping")

    # history paging, around, gap
    core.call("messages.fetch_recent", {**A, "chat_id": "c2", "limit": 30})
    r = core.call("messages.fetch_recent", {**A, "chat_id": "c2", "limit": 30})
    core.call("messages.fetch_recent", {**A, "chat_id": "c2", "limit": 30, "cursor": r["next_cursor"]})
    core.call("messages.fetch_recent", {**A, "chat_id": "c4", "topic_id": "2", "limit": 10})
    core.call("history.around", {**A, "chat_id": "c5", "at": "2025-12-31T00:00:00.000Z", "limit": 10})
    g = core.call("history.fill_gap", {**A, "chat_id": "c5", "after_message_id": "10", "limit": 5})
    core.call("history.fill_gap", {**A, "chat_id": "c5", "after_message_id": "10", "limit": 5,
                                     "cursor": g["next_cursor"]})
    core.call("messages.get", {**A, "chat_id": "c5", "message_id": "3"})
    core.call("pins.list", {**A, "chat_id": "c2", "limit": 10})
    for kind in ("photo", "video", "voice", "document", "music", "gif", "round", "link"):
        core.call("media.search", {**A, "chat_id": "c2", "kind": kind, "limit": 5})

    # media of every kind, thumb and full
    for kind in ("photo", "video", "voice", "video_note", "document", "music", "gif", "sticker"):
        hit = _find(core, lambda m, k=kind: m.get("media", {}).get("kind") == k
                    and m["media"].get("filename") != "dataset.bin", chats)
        if hit:
            cid, m = hit
            core.call("media.download", {**A, "chat_id": cid, "message_id": m["id"], "what": "full"})
            if kind in ("photo", "video", "gif", "sticker"):
                core.call("media.download", {**A, "chat_id": cid, "message_id": m["id"], "what": "thumb"})
    core.call("avatars.download", {**A, "kind": "user", "id": "u1", "size": "big"})
    core.call("avatars.download", {**A, "kind": "chat", "id": "c2"})

    # sending and changing
    sent = core.call("messages.send", {**A, "chat_id": "c5", "text": "Hello 👋 world",
                                         "entities": [{"type": "bold", "offset": 6, "length": 2}],
                                         "reply_to": {"message_id": "3", "quote_text": "Hi"}, "silent": True,
                                         "client_id": "cl-1"})["message"]
    core.call("messages.edit", {**A, "chat_id": "c5", "message_id": sent["id"], "text": "Hello again"})
    core.call("messages.forward", {**A, "from_chat_id": "c5", "message_ids": [sent["id"], "3"],
                                     "to_chat_id": "c0", "drop_author": False})
    core.call("messages.forward", {**A, "from_chat_id": "c3", "message_ids": ["2"], "to_chat_id": "c4",
                                     "topic_id": "3", "drop_author": True})
    core.call("messages.pin", {**A, "chat_id": "c5", "message_id": sent["id"]})
    core.call("messages.unpin", {**A, "chat_id": "c5", "message_id": sent["id"]})
    core.call("messages.unpin", {**A, "chat_id": "c2"})
    core.call("messages.send_file", {**A, "chat_id": "c6", "path": str(upload), "caption": "upload"})
    core.call("messages.send_file", {**A, "chat_id": "c6", "path": str(upload), "kind": "voice",
                                       "duration": 2, "waveform": [1, 5, 9]})
    core.call("messages.send_file", {**A, "chat_id": "c6", "path": str(upload), "as_file": True,
                                       "spoiler": True, "filename": "x.png"})
    core.call("messages.send_file", {**A, "chat_id": "c6", "path": str(upload), "kind": "video_note"})
    core.call("messages.send_album", {**A, "chat_id": "c6", "items": [{"path": str(upload)},
                                                                       {"path": str(upload), "caption": "2"}],
                                        "caption": "album"})
    core.call("messages.delete", {**A, "chat_id": "c5", "message_ids": [sent["id"]], "revoke": True})
    core.call("messages.delete", {**A, "chat_id": "c5", "message_ids": ["1"], "revoke": False})

    # bot: reply events after the response
    core.call("bots.commands", {**A, "bot_id": "b1"})
    b = core.call("messages.send", {**A, "chat_id": "c1", "text": "/buttons"})["message"]
    core.event("read.outbox", lambda p: p["chat_id"] == "c1" and p["max_message_id"] == b["id"])
    reply = next(e["params"]["messages"][0] for e in reversed(core.all_events) if e["method"] == "messages.upsert")
    for row, btns in enumerate(reply["buttons"]):
        for col, btn in enumerate(btns):
            core.call("bots.press_button", {**A, "chat_id": "c1", "message_id": reply["id"], "button": btn,
                                              "row": row, "column": col})

    # reactions and polls
    core.call("reactions.available", {**A, "chat_id": "c2"})
    core.call("reactions.set", {**A, "chat_id": "c2", "message_id": "5", "reactions": [{"emoji": "👍"},
                                                                                         {"emoji": "🔥"}]})
    core.call("reactions.who", {**A, "chat_id": "c2", "message_id": "5", "limit": 10})
    poll = core.call("polls.create", {**A, "chat_id": "c2", "question": "Tea or coffee?",
                                        "options": ["Tea", "Coffee", "Both"], "quiz": True, "correct_option": 1,
                                        "solution": "Coffee, obviously"})["message"]
    core.call("polls.vote", {**A, "chat_id": "c2", "message_id": poll["id"], "option_ids": ["1"]})
    core.call("polls.vote", {**A, "chat_id": "c2", "message_id": poll["id"], "option_ids": []})
    core.call("polls.close", {**A, "chat_id": "c2", "message_id": poll["id"]})

    # read state, typing, drafts
    core.call("read.send_receipt", {**A, "chat_id": "c2", "max_message_id": "80"})
    core.call("read.send_receipt", {**A, "chat_id": "c4", "topic_id": "2", "max_message_id": "80"})
    core.call("typing.send", {**A, "chat_id": "c5", "action": "typing"})
    core.call("drafts.save", {**A, "chat_id": "c5", "draft": {"text": "draft…"}})
    core.call("drafts.save", {**A, "chat_id": "c4", "topic_id": "3", "draft": {"text": ""}})

    # chats, topics, folders, members, contacts
    core.call("chats.set_flags", {**A, "chat_id": "c6", "pinned": True, "muted": True,
                                    "mute_until": "2026-02-01T00:00:00.000Z", "archived": False,
                                    "marked_unread": True})
    observed = core.call("chats.get", {**A, "chat_id": "c6"})["conversation"]
    assert observed == {"id": "c6", "kind": "group", "pinned": True, "marked_unread": True}
    core.call("topics.list", {**A, "chat_id": "c4"})
    core.call("folders.list", A)
    core.call("members.list", {**A, "chat_id": "c2", "limit": 3})
    core.call("members.list", {**A, "chat_id": "c2", "limit": 50, "filter": "admins"})
    core.call("members.list", {**A, "chat_id": "c4", "limit": 50, "query": "a"})
    core.call("contacts.list", A)
    core.call("contacts.info", {**A, "user_id": "u3"})
    core.call("profile.get", A)
    core.call("lookup.username", {**A, "username": "@fakenews"})
    core.call("lookup.username", {**A, "username": "fakehub_bot"})
    core.call("lookup.username", {**A, "username": "nobody_here"})

    # stickers and GIFs
    core.call("stickers.recent", {**A, "limit": 5})
    core.call("stickers.packs", A)
    core.call("stickers.pack", {**A, "pack_id": "p2"})
    core.call("stickers.thumb", {**A, "sticker_id": "s8"})
    core.call("stickers.send", {**A, "chat_id": "c5", "sticker_id": "s8"})
    core.call("gifs.list", A)
    core.call("gifs.thumb", {**A, "gif_id": "g2"})
    core.call("gifs.clip", {**A, "gif_id": "g2"})
    core.call("gifs.send", {**A, "chat_id": "c5", "gif_id": "g2"})

    # full re-sync, ghost mode, sign out
    core.call("chats.sync", {**A, "include_archived": True})
    core.event("folders.synced")
    core.call("ghost.apply", {**A, "enabled": True})
    core.call("account.sign_out", A)
    core.event("status", lambda p: p["state"] == "auth_required")
    core.call("account.stop", A)
    assert core.shutdown() == 0


def run_once(env: dict, tmp: Path) -> tuple[list[str], dict[str, str], set[str], dict]:
    upload = tmp / "upload.png"
    from fake_plugin import media

    upload.write_bytes(media.png(48, 32, "upload"))
    core = FakeCore(env, tmp)
    try:
        session(core, upload)
    finally:
        core.close()
    lines = [ln.decode().replace(str(tmp), "<TMP>") for ln in core.raw]
    cache = tmp / "accounts" / "fake" / "cache"
    files = {str(p.relative_to(cache)): hashlib.sha256(p.read_bytes()).hexdigest()
             for p in sorted(cache.rglob("*")) if p.is_file()}
    init = json.loads(lines[0])["result"]
    return lines, files, core.called, init


def test_same_seed_is_byte_identical(isolated_env, tmp_path):
    (a := tmp_path / "a").mkdir()
    (b := tmp_path / "b").mkdir()
    lines_a, files_a, called, init = run_once({**isolated_env, "CH_DATA_DIR": str(a / "data")}, a)
    lines_b, files_b, _, _ = run_once({**isolated_env, "CH_DATA_DIR": str(b / "data")}, b)
    assert len(lines_a) > 100 and len(files_a) > 15, (len(lines_a), len(files_a))
    for i, (x, y) in enumerate(zip(lines_a, lines_b)):
        assert x == y, f"frame {i} differs:\n{x}\n{y}"
    assert len(lines_a) == len(lines_b)
    assert files_a == files_b
    assert not any(name.endswith(".part") for name in files_a)

    # every method of every declared capability was exercised (and answered without error)
    caps = init["capabilities"]
    missing = sorted(m for m, cap in method_caps().items()
                     if cap != "required" and caps.get(cap) and m not in called)
    assert not missing, f"declared but not exercised: {missing}"
    errors = [json.loads(ln) for ln in lines_a if '"error"' in ln and '"id"' in ln]
    assert not [e for e in errors if "error" in e], errors


def test_other_seed_differs(isolated_env, tmp_path):
    from fake_plugin.world import World
    from contact_hub_sdk import to_wire

    def dump(seed):
        w = World(seed, 10, 30)
        return json.dumps([to_wire(m) for c in w.chats.values() for m in c.messages], ensure_ascii=False)

    assert dump(1) == dump(1)
    assert dump(1) != dump(2)


def test_manifest_validates():
    manifest = json.loads((PLUGIN_DIR / "plugin.json").read_text())
    validate(manifest, "manifest.json")
    from fake_plugin.plugin import DEFAULTS

    props = manifest["settings_schema"]["properties"]
    assert set(props) == set(DEFAULTS)
    assert {k: v["default"] for k, v in props.items()} == DEFAULTS
