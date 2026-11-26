"""The fake plugin: a deterministic messenger on contact_hub_sdk for UI and e2e tests.

Same settings (seed, sizes) and the same request sequence give byte-identical frames and
files. Wall-clock time is only used while the live loop (``live_interval_ms`` > 0) runs.
"""

from __future__ import annotations

import asyncio
import copy
import mimetypes
import random
import re
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from contact_hub_sdk import (
    Account,
    AuthRequired,
    FloodWait,
    Forbidden,
    InvalidParams,
    NetworkError,
    NotFound,
    NotSupported,
    Plugin,
    PluginError,
    Unavailable,
    current_request,
    method,
)
from contact_hub_sdk import types as T

from . import media as M
from .world import (
    BASE,
    BOT,
    BOT_CHAT,
    BOT_COMMANDS,
    EMOJI,
    ENTITY_TYPES,
    ME,
    PLUGIN,
    SENTENCES,
    Chat,
    World,
    bot_buttons,
    created,
    ms,
    preview,
    u16,
)

TEST_CODE = "12345"
TEST_PASSWORD = "fake"
CHUNK = 64 * 1024
MAX_UPLOAD = 50 * 1024 * 1024

DEFAULTS: dict[str, Any] = {
    "seed": 1, "chats": 12, "messages_per_chat": 60, "latency_ms": 0, "transfer_chunk_ms": 40,
    "fail_rate": 0.0, "fail_script": "", "live_interval_ms": 0, "start_state": "auto",
    "require_2fa": True, "qr_scan_ms": 3000,
}
LIMITS = {"seed": (0, 2**31 - 1), "chats": (1, 500), "messages_per_chat": (0, 2000), "latency_ms": (0, 60000),
          "transfer_chunk_ms": (0, 10000), "live_interval_ms": (0, 3600000), "qr_scan_ms": (0, 600000)}
ERROR_CODES = {"FLOOD_WAIT", "NETWORK", "INTERNAL", "TIMEOUT", "FORBIDDEN", "NOT_FOUND", "UNAVAILABLE",
               "AUTH_REQUIRED", "NOT_SUPPORTED", "INVALID_PARAMS"}
SEARCH_KINDS = {"photo": "photo", "video": "video", "music": "music", "voice": "voice", "document": "document",
                "gif": "gif", "round": "video_note"}


def scripted_error(code: str, arg: str | None) -> PluginError:
    if code == "FLOOD_WAIT":
        return FloodWait(int(arg or 5), "scripted FLOOD_WAIT")
    if code == "NETWORK":
        return NetworkError("scripted NETWORK failure")
    return PluginError(f"scripted {code}", code=code)


def parse_script(text: str) -> dict[str, tuple[str, str | None]]:
    """``method[#n]=CODE[:arg]`` entries separated by commas, semicolons or spaces.
    Without ``#n`` every call fails; with it only the n-th (1-based)."""
    out = {}
    for item in re.split(r"[,;\s]+", text.strip()):
        if not item:
            continue
        m = re.fullmatch(r"([a-z_.$/-]+)(?:#(\d+))?=([A-Z_]+)(?::(\d+))?", item)
        if m is None or m.group(3) not in ERROR_CODES:
            raise InvalidParams(f"bad fail_script entry {item!r} (want method[#n]=CODE[:seconds])",
                                field="fail_script")
        key = m.group(1) + (f"#{m.group(2)}" if m.group(2) else "")
        out[key] = (m.group(3), m.group(4))
    return out


class State:
    """Per-account state of the fake platform."""

    def __init__(self, account: Account, cfg: dict[str, Any]):
        self.account = account
        self.cfg = cfg
        self.seed: int = cfg["seed"]
        self.world: World | None = None
        self.online = False
        self.ghost = account.ghost
        self.tick = 0
        self.calls: Counter[str] = Counter()
        self.script = parse_script(cfg["fail_script"])
        self.fail_rng = random.Random(f"{self.seed}/fail")
        self.live_task: asyncio.Task[Any] | None = None
        self.presses = 0
        self.cache = account.cache_dir

    @property
    def live(self) -> bool:
        return self.cfg["live_interval_ms"] > 0

    def now(self) -> datetime:
        if self.live:
            return ms(datetime.now(UTC))
        self.tick += 1
        return BASE + timedelta(seconds=self.tick)

    async def gate(self, name: str, need_online: bool = True) -> None:
        """Latency, then scripted / random failures, then the connection state check."""
        self.calls[name] += 1
        if self.cfg["latency_ms"]:
            await asyncio.sleep(self.cfg["latency_ms"] / 1000)
        rule = self.script.get(f"{name}#{self.calls[name]}") or self.script.get(name)
        if rule is not None:
            raise scripted_error(*rule)
        if self.cfg["fail_rate"] and self.fail_rng.random() < self.cfg["fail_rate"]:
            if self.fail_rng.random() < 0.5:
                raise FloodWait(self.fail_rng.randint(1, 30), "random FLOOD_WAIT (fail_rate)")
            raise NetworkError("random NETWORK failure (fail_rate)")
        if need_online and not self.online:
            if self.account.state == "auth_required":
                raise AuthRequired()
            raise Unavailable()

    # ---- lookups -------------------------------------------------------------------------

    @property
    def w(self) -> World:
        assert self.world is not None
        return self.world

    def chat(self, chat_id: str) -> Chat:
        c = self.w.chats.get(chat_id)
        if c is None:
            raise NotFound(f"no chat {chat_id}", kind="chat")
        return c

    def msg(self, chat: Chat, message_id: str) -> T.Message:
        m = chat.find(message_id)
        if m is None:
            raise NotFound(f"no message {message_id} in {chat.id}", kind="message")
        return m

    # ---- files ---------------------------------------------------------------------------

    def media_key(self, chat: Chat, m: T.Message) -> str:
        return chat.media_keys.get(m.id, f"gen:{self.seed}/{chat.id}/{m.id}")

    def _full_spec(self, chat: Chat, m: T.Message) -> tuple[Path, str]:
        """(target path, media key) for the full file of ``m``."""
        assert m.media is not None
        key = self.media_key(chat, m)
        if key.startswith("file:"):
            return Path(key[5:]), key
        ext = Path(m.media.filename or "").suffix or (mimetypes.guess_extension(m.media.mime or "") or ".bin")
        if key.startswith("sticker:"):
            ext = ".json" if str(m.media.sticker.format if m.media.sticker else "") == "animated" else ".png"
        return self.cache / chat.id / f"{m.id}{ext}", key

    def full_bytes(self, key: str, media: T.Media) -> bytes:
        kind = str(media.kind)
        if key.startswith("file:"):
            p = Path(key[5:])
            if not p.is_file():
                raise NotFound("the uploaded file left the cache", kind="media")
            return p.read_bytes()
        if key.startswith("sticker:"):
            return self.sticker_bytes(key[8:])
        if key.startswith("gif:"):
            return M.gif(64, 64, f"gif/{self.seed}/{key[4:]}")
        if kind == "photo":
            return M.png(media.width or 320, media.height or 240, key)
        if kind in ("video", "video_note"):
            return M.CLIP.read_bytes()
        if kind in ("voice", "music"):
            return M.wav(media.duration or 1, key)
        if kind == "gif":
            return M.gif(64, 64, key)
        name = media.filename or ""
        if name.endswith(".pdf"):
            return M.pdf(f"Fake report {key}")
        if name.endswith(".bin"):
            return M.blob(key, media.bytes or CHUNK)
        return M.text_file(key)

    def sticker_bytes(self, sticker_id: str) -> bytes:
        s = self.w.sticker(sticker_id)
        if s is None:
            raise NotFound(f"no sticker {sticker_id}", kind="sticker")
        key = f"sticker/{self.seed}/{s.id}"
        return M.lottie(key) if str(s.format) == "animated" else M.png(256, 256, key)

    def thumb_path(self, chat: Chat, m: T.Message) -> Path | None:
        if m.media is None or str(m.media.kind) not in ("photo", "video", "video_note", "gif", "sticker"):
            return None
        key = self.media_key(chat, m)
        if key.startswith("file:") and str(m.media.kind) != "photo":
            return None
        path = self.cache / chat.id / f"{m.id}_thumb.png"
        if not path.is_file():
            if key.startswith("file:"):
                return None  # uploads: no thumbnail of our own
            w, h = (max(8, (m.media.width or 64) // 10), max(8, (m.media.height or 64) // 10))
            write_atomic(path, M.png(w, h, key + "/thumb"))
        return path

    def out(self, chat: Chat, m: T.Message) -> T.Message:
        """The wire copy of a stored message: thumbnails ensured, ``path`` once downloaded."""
        if m.media is None:
            return m
        o = copy.copy(m)
        o.media = copy.copy(m.media)
        thumb = self.thumb_path(chat, m)
        o.media.thumb_path = str(thumb) if thumb else None
        full, _ = self._full_spec(chat, m)
        o.media.path = str(full) if full.is_file() else None
        return o

    def outs(self, chat: Chat, msgs: list[T.Message]) -> list[T.Message]:
        return [self.out(chat, m) for m in msgs]

    def conv(self, chat: Chat) -> T.Conversation:
        c = copy.copy(chat.conv)
        c.folder_ids = self.w.folder_ids(chat.conv)
        return c


def write_atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".part")
    tmp.write_bytes(data)
    tmp.replace(path)


def rpc(name: str, *, online: bool = True):
    """A handler for ``name`` that goes through :meth:`State.gate` first."""

    def deco(fn):
        async def wrapper(self, account, p):
            st: State = account.data["fake"]
            await st.gate(name, online)
            return await fn(self, account, st, p)

        wrapper.__name__ = fn.__name__
        wrapper.__doc__ = fn.__doc__
        return method(name)(wrapper)

    return deco


def page(items: list[Any], limit: int, cursor: str | None) -> tuple[list[Any], str | None]:
    """Offset paging with an opaque ``o:<n>`` cursor."""
    start = int(cursor[2:]) if cursor and cursor.startswith("o:") and cursor[2:].isdigit() else 0
    chunk = items[start:start + max(1, limit)]
    nxt = f"o:{start + len(chunk)}" if start + len(chunk) < len(items) else None
    return chunk, nxt


def id_cursor(cursor: str | None) -> int | None:
    if cursor is None:
        return None
    if not cursor.startswith("m:") or not cursor[2:].isdigit():
        raise InvalidParams("unknown cursor", field="cursor")
    return int(cursor[2:])


class FakePlugin(Plugin):
    version = "0.1.0"
    capabilities: dict[str, Any] = {
        "messages.get": True,
        "send.entities": {"types": ENTITY_TYPES},
        "send.silent": True,
        "quote_reply": True,
        "send.file": {"max_bytes": MAX_UPLOAD},
        "send.voice": True,
        "send.video_note": True,
        "send.spoiler": True,
        "send.as_file": True,
        "send.album": {"max": 10},
        "edit": True,
        "edit.caption": True,
        "delete": True,
        "delete.for_me": True,
        "forward": True,
        "forward.drop_author": True,
        "pin": True,
        "unpin": True,
        "pins.list": True,
        "history.jump_to_date": True,
        "history.fill_gap": True,
        "media.download": True,
        "media.search": {"kinds": ["photo", "video", "music", "voice", "document", "link", "gif", "round"]},
        "avatars": True,
        "reactions.set": {"max": 3},
        "reactions.who": True,
        "polls.create": {"quiz": True, "multiple": True, "anonymous": True, "max_options": 10},
        "polls.vote": True,
        "polls.close": True,
        "bots.commands": True,
        "bots.buttons": True,
        "read_receipts": True,
        "typing": {"actions": ["typing", "recording_voice", "uploading", "choosing_sticker"]},
        "presence": True,
        "drafts.sync": True,
        "topics": True,
        "chat.sync": True,
        "chat.get": True,
        "chat.flags": True,
        "chat.pin": True,
        "chat.mute": True,
        "chat.archive": True,
        "chat.mark_unread": True,
        "folders": True,
        "members": True,
        "members.search": True,
        "contacts.list": True,
        "contacts.info": True,
        "lookup.username": True,
        "profile": True,
        "stickers": True,
        "stickers.send": True,
        "gifs": True,
        "gifs.send": True,
        "ghost_mode": True,
        "sign_out": True,
    }

    # ---- lifecycle ---------------------------------------------------------------------

    def config(self, account: Account) -> dict[str, Any]:
        cfg = {}
        for k, d in DEFAULTS.items():
            v = account.setting(k, d)
            try:
                if isinstance(d, bool):
                    v = v if isinstance(v, bool) else str(v).lower() in ("1", "true", "yes")
                elif isinstance(d, int):
                    v = int(v)
                elif isinstance(d, float):
                    v = float(v)
                else:
                    v = str(v)
            except (TypeError, ValueError):
                raise InvalidParams(f"{k} must be a {type(d).__name__}", field=k) from None
            lo_hi = LIMITS.get(k)
            if lo_hi and not lo_hi[0] <= v <= lo_hi[1]:
                raise InvalidParams(f"{k} must be between {lo_hi[0]} and {lo_hi[1]}", field=k)
            cfg[k] = v
        if not 0.0 <= cfg["fail_rate"] <= 1.0:
            raise InvalidParams("fail_rate must be between 0 and 1", field="fail_rate")
        if cfg["start_state"] not in ("auto", "connected", "unavailable", "error"):
            raise InvalidParams("start_state must be auto, connected, unavailable or error", field="start_state")
        if 0 < cfg["live_interval_ms"] < 500:
            cfg["live_interval_ms"] = 500  # spec 16: presence at most every 500 ms per user
        return cfg

    async def on_account_start(self, account: Account) -> None:
        st = State(account, self.config(account))
        account.data["fake"] = st
        await account.status("connecting")
        account.spawn(self._connect(account, st), name="connect")

    async def _connect(self, account: Account, st: State) -> None:
        st.world = await asyncio.to_thread(World, st.seed, st.cfg["chats"], st.cfg["messages_per_chat"])
        if st.cfg["latency_ms"]:
            await asyncio.sleep(st.cfg["latency_ms"] / 1000)
        mode = st.cfg["start_state"]
        if mode == "unavailable":
            await account.status("unavailable", detail="The fake network is switched off (start_state)")
        elif mode == "error":
            await account.status("error", detail="Scripted error (start_state)")
        elif mode == "connected" or account.secrets.get("session"):
            await self._go_online(account, st)
        else:
            await account.status("auth_required")

    async def _go_online(self, account: Account, st: State) -> None:
        st.online = True
        await account.status("connected", me=st.w.me.contact())
        await self._sync(account, st, include_archived=True)
        if st.live and (st.live_task is None or st.live_task.done()):
            st.live_task = account.spawn(self._live(account, st), name="live")

    async def _sync(self, account: Account, st: State, include_archived: bool) -> None:
        w = st.w
        chats = [w.chats[c] for c in w.order]
        convs = [st.conv(c) for c in chats if include_archived or not c.conv.archived]
        for i in range(0, len(convs), 100):
            await account.conversations_upsert(convs[i:i + 100], ack=True)
        await account.conversations_synced([c.id for c in chats], ack=True)
        await account.contacts_upsert([u.contact() for u in w.users.values()], ack=True)
        await account.folders_synced(w.folders, ack=True)
        for c in chats:
            if c.topics:
                await account.topics_upsert(c.id, c.topics, complete=True, ack=True)
        last = [st.out(c, c.messages[-1]) for c in chats if c.messages]
        for i in range(0, len(last), 100):
            await account.messages_upsert(last[i:i + 100], ack=True)

    # ---- auth (spec 7) -------------------------------------------------------------------

    async def on_auth_begin(self, account: Account, method_: str) -> None:
        st: State = account.data["fake"]
        flow = account.auth
        if method_ == "qr":
            token = f"ch-fake://login?seed={st.seed}&n={st.calls['auth.qr'] + 1}"
            st.calls["auth.qr"] += 1
            await flow.qr(token, title="Scan the code", hint="The fake phone 'scans' it after a moment.")
            await asyncio.sleep(st.cfg["qr_scan_ms"] / 1000)
        elif method_ == "phone_code":
            phone = account.data.get("phone")
            err = None
            while phone is None:
                v = (await flow.ask("phone", title="Your phone number", placeholder="+15550100000",
                                    hint="Any number works on the fake platform.", error=err)).strip()
                if re.fullmatch(r"\+?\d{6,15}", v.replace(" ", "")):
                    phone = account.data["phone"] = v.replace(" ", "")
                else:
                    err = "Invalid phone number"
            err = None
            while True:
                code = await flow.ask("code", title="Enter the code", placeholder=TEST_CODE, can_resend=True,
                                      hint=f"Test code: {TEST_CODE} (sent to {phone})",
                                      min_length=len(TEST_CODE), max_length=len(TEST_CODE), error=err)
                if code.strip() == TEST_CODE:
                    break
                err = "Wrong code"
        else:
            raise NotSupported(f"auth method {method_} is not offered", capability="auth.interactive")
        if st.cfg["require_2fa"]:
            err = None
            while True:
                pw = await flow.ask("password", title="Two-step verification", hint=f"Test password: {TEST_PASSWORD}",
                                    error=err)
                if pw == TEST_PASSWORD:
                    break
                err = "Wrong password"
        await account.set_secret("session", f"fake-session-{st.seed}")
        await self._go_online(account, st)

    # ---- vendor --------------------------------------------------------------------------

    @method("x-fake.reconnect")
    async def x_reconnect(self, account: Account, p: dict[str, Any]) -> dict[str, Any]:
        """Test hook: drop and restore the fake connection (status connecting/reconnecting)."""
        st: State = account.data["fake"]
        if not st.online:
            raise Unavailable()

        async def bounce() -> None:
            st.online = False
            await account.status("connecting", reconnecting=True)
            await asyncio.sleep(max(0.05, st.cfg["latency_ms"] / 1000))
            st.online = True
            await account.status("connected", me=st.w.me.contact())

        current_request().after_response(lambda: _spawned(account, bounce()))
        return {}

    # ---- history -------------------------------------------------------------------------

    @staticmethod
    def _scope(chat: Chat, topic_id: str | None) -> list[T.Message]:
        if topic_id is None:
            return chat.messages
        return [m for m in chat.messages if m.topic_id == topic_id]

    @rpc("messages.fetch_recent")
    async def fetch_recent(self, account, st: State, p: T.MessagesFetchRecentParams):
        chat = st.chat(p.chat_id)
        before = id_cursor(p.cursor)
        msgs = [m for m in self._scope(chat, p.topic_id) if before is None or int(m.id) < before]
        pg = msgs[-p.limit:][::-1]
        nxt = f"m:{pg[-1].id}" if pg and len(msgs) > len(pg) else None
        return T.PagedMessagesResult(messages=st.outs(chat, pg), next_cursor=nxt)

    @rpc("messages.get")
    async def get(self, account, st: State, p: T.ChatMessageParams):
        chat = st.chat(p.chat_id)
        m = chat.find(p.message_id)
        return T.OptionalMessageResult(message=st.out(chat, m) if m else None)

    @rpc("history.around")
    async def around(self, account, st: State, p: T.HistoryAroundParams):
        chat = st.chat(p.chat_id)
        msgs = self._scope(chat, p.topic_id)
        if not msgs:
            return T.HistoryAroundResult(messages=[])
        idx = min(range(len(msgs)), key=lambda i: (abs((msgs[i].sent_at - p.at).total_seconds()), i))
        lo = max(0, idx - p.limit // 2)
        pg = msgs[lo:lo + p.limit]
        return T.HistoryAroundResult(
            messages=st.outs(chat, pg[::-1]),
            older_cursor=f"m:{pg[0].id}" if lo > 0 else None,
            newer_cursor=f"m:{pg[-1].id}" if lo + len(pg) < len(msgs) else None,
        )

    @rpc("history.fill_gap")
    async def fill_gap(self, account, st: State, p: T.HistoryFillGapParams):
        chat = st.chat(p.chat_id)
        after = id_cursor(p.cursor) if p.cursor else int(p.after_message_id) if p.after_message_id.isdigit() else 0
        before = int(p.before_message_id) if p.before_message_id and p.before_message_id.isdigit() else None
        msgs = [m for m in self._scope(chat, p.topic_id)
                if int(m.id) > after and (before is None or int(m.id) < before)]
        pg = msgs[:p.limit]
        nxt = f"m:{pg[-1].id}" if pg and len(msgs) > len(pg) else None
        return T.PagedMessagesResult(messages=st.outs(chat, pg), next_cursor=nxt)

    @rpc("pins.list")
    async def pins(self, account, st: State, p: T.ChatTopicPagedParams):
        chat = st.chat(p.chat_id)
        before = id_cursor(p.cursor)
        msgs = [m for m in self._scope(chat, p.topic_id) if m.is_pinned and (before is None or int(m.id) < before)]
        pg = msgs[-p.limit:][::-1]
        nxt = f"m:{pg[-1].id}" if pg and len(msgs) > len(pg) else None
        return T.PagedMessagesResult(messages=st.outs(chat, pg), next_cursor=nxt)

    @rpc("media.search")
    async def media_search(self, account, st: State, p: T.MediaSearchParams):
        chat = st.chat(p.chat_id)
        kind, q = str(p.kind), (p.query or "").casefold()

        def match(m: T.Message) -> bool:
            if kind == "link":
                ok = m.link_preview is not None or any(str(e.type) in ("url", "text_url") for e in m.entities or [])
            else:
                ok = m.media is not None and str(m.media.kind) == SEARCH_KINDS.get(kind)
            hay = f"{m.text or ''} {m.media.filename if m.media else ''}".casefold()
            return ok and (not q or q in hay)

        before = id_cursor(p.cursor)
        msgs = [m for m in self._scope(chat, p.topic_id) if match(m) and (before is None or int(m.id) < before)]
        pg = msgs[-p.limit:][::-1]
        nxt = f"m:{pg[-1].id}" if pg and len(msgs) > len(pg) else None
        return T.PagedMessagesResult(messages=st.outs(chat, pg), next_cursor=nxt)

    # ---- sending -------------------------------------------------------------------------

    def _new_out(self, st: State, chat: Chat, topic_id: str | None, reply_to: T.ReplyTarget | None,
                 silent: bool | None) -> T.Message:
        if chat.conv.can_post is False:
            raise Forbidden("this channel is read-only", reason="read_only")
        if chat.topics:
            topic_id = topic_id or "1"
            if not any(t.id == topic_id for t in chat.topics):
                raise NotFound(f"no topic {topic_id}", kind="topic")
            if any(t.id == topic_id and t.closed for t in chat.topics):
                raise Forbidden("the topic is closed", reason="topic_closed")
        elif topic_id is not None:
            raise InvalidParams("this chat has no topics", field="topic_id")
        m = T.Message(id=chat.new_id(), chat_id=chat.id, topic_id=topic_id, direction="out", sent_at=st.now(),
                      sender_id=ME, sender_name=st.w.me.name, delivery="sent", silent=silent or None)
        if chat.conv.is_self_chat:
            m.delivery = "read"
        if reply_to is not None:
            target = st.msg(chat, reply_to.message_id)
            m.reply_to = T.ReplyRef(message_id=target.id, sender_id=target.sender_id, preview=preview(target),
                                    quote_text=reply_to.quote_text, quote_offset=reply_to.quote_offset)
        return m

    def _commit(self, st: State, chat: Chat, m: T.Message) -> None:
        chat.messages.append(created(m))
        chat.conv.last_message_at = m.sent_at
        chat.conv.draft = T.Draft(text="") if chat.conv.draft and chat.conv.draft.text else chat.conv.draft
        for t in chat.topics:
            if t.id == m.topic_id:
                t.last_message_at, t.last_message_preview = m.sent_at, preview(m)

    @rpc("messages.send")
    async def send(self, account, st: State, p: T.MessagesSendParams):
        chat = st.chat(p.chat_id)
        cmd = p.text.split()
        if cmd and cmd[0] == "/fail":  # test hook: "/fail FLOOD_WAIT 7"
            code = cmd[1] if len(cmd) > 1 and cmd[1] in ERROR_CODES else "NETWORK"
            raise scripted_error(code, cmd[2] if len(cmd) > 2 and cmd[2].isdigit() else None)
        if not p.text.strip():
            raise InvalidParams("empty message", field="text")
        m = self._new_out(st, chat, p.topic_id, p.reply_to, p.silent)
        m.text, m.entities = p.text, p.entities
        if p.client_id:
            m.ext = {PLUGIN: {"client_id": p.client_id}}
        self._commit(st, chat, m)
        if chat.id == BOT_CHAT:
            current_request().after_response(lambda: _spawned(account, self._bot_reply(account, st, chat, m)))
        return T.MessageResult(message=st.out(chat, m))

    async def _upload(self, st: State, chat: Chat, mid: str, src: Path) -> tuple[Path, int]:
        """Copy ``src`` into the cache in chunks, with progress; cancellable (spec 15)."""
        if not src.is_absolute() or not src.is_file():
            raise InvalidParams(f"not a readable file: {src}", field="path")
        size = src.stat().st_size
        if size > MAX_UPLOAD:
            raise InvalidParams("file is larger than send.file.max_bytes", field="path")
        dest = st.cache / chat.id / f"{mid}{_safe_suffix(src.name)}"
        await self._write_chunked(st, dest, src.read_bytes())
        return dest, size

    async def _write_chunked(self, st: State, dest: Path, data: bytes) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")
        try:
            with part.open("wb") as f:
                done = 0
                await st.account.progress(0, len(data), force=True)
                while done < len(data):
                    if st.cfg["transfer_chunk_ms"]:
                        await asyncio.sleep(st.cfg["transfer_chunk_ms"] / 1000)
                    else:
                        await asyncio.sleep(0)
                    f.write(data[done:done + CHUNK])
                    done = min(len(data), done + CHUNK)
                    await st.account.progress(done, len(data), force=True)
            part.replace(dest)
        except BaseException:
            part.unlink(missing_ok=True)
            raise

    def _file_media(self, path: Path, size: int, *, kind: str | None, mime: str | None, filename: str | None,
                    as_file: bool | None, spoiler: bool | None, duration: int | None,
                    waveform: list[int] | None, original: str) -> T.Media:
        mime = mime or mimetypes.guess_type(original)[0] or "application/octet-stream"
        kind = kind or "auto"
        if kind == "auto":
            if as_file:
                kind = "document"
            elif mime == "image/gif":
                kind = "gif"
            elif mime.startswith("image/"):
                kind = "photo"
            elif mime.startswith("video/"):
                kind = "video"
            elif mime.startswith("audio/"):
                kind = "music"
            else:
                kind = "document"
        md = T.Media(kind=kind, mime=mime, bytes=size, filename=filename or _safe_name(original),
                     duration=duration, waveform=waveform, has_spoiler=bool(spoiler))
        if kind == "photo":
            dims = M.png_size(path.read_bytes()[:32])
            if dims:
                md.width, md.height = dims
        return md

    @rpc("messages.send_file")
    async def send_file(self, account, st: State, p: T.MessagesSendFileParams):
        chat = st.chat(p.chat_id)
        m = self._new_out(st, chat, p.topic_id, p.reply_to, p.silent)
        dest, size = await self._upload(st, chat, m.id, Path(p.path))
        m.media = self._file_media(dest, size, kind=None if p.kind in (None, "auto") else str(p.kind), mime=p.mime,
                                   filename=p.filename, as_file=p.as_file, spoiler=p.spoiler, duration=p.duration,
                                   waveform=p.waveform, original=Path(p.path).name)
        m.text, m.entities = p.caption, p.entities if p.caption else None
        chat.media_keys[m.id] = f"file:{dest}"
        self._commit(st, chat, m)
        return T.MessageResult(message=st.out(chat, m))

    @rpc("messages.send_album")
    async def send_album(self, account, st: State, p: T.MessagesSendAlbumParams):
        chat = st.chat(p.chat_id)
        if not 2 <= len(p.items) <= 10:
            raise InvalidParams("an album has 2 to 10 items", field="items")
        out, group = [], None
        for i, item in enumerate(p.items):
            m = self._new_out(st, chat, p.topic_id, p.reply_to if i == 0 else None, p.silent)
            group = group or f"a{chat.id}-{m.id}"
            dest, size = await self._upload(st, chat, m.id, Path(item.path))
            m.media = self._file_media(dest, size, kind=None if item.kind in (None, "auto") else str(item.kind),
                                       mime=None, filename=None, as_file=item.as_file, spoiler=item.spoiler,
                                       duration=None, waveform=None, original=Path(item.path).name)
            m.grouped_id = group
            m.text = item.caption or (p.caption if i == 0 else None)
            m.entities = item.entities if item.caption else (p.entities if i == 0 and p.caption else None)
            chat.media_keys[m.id] = f"file:{dest}"
            self._commit(st, chat, m)
            out.append(st.out(chat, m))
        return T.MessagesResult(messages=out)

    @rpc("messages.edit")
    async def edit(self, account, st: State, p: T.MessagesEditParams):
        chat = st.chat(p.chat_id)
        m = st.msg(chat, p.message_id)
        if m.direction != "out" or m.action is not None or m.poll is not None:
            raise Forbidden("you can only edit your own messages", reason="not_mine")
        if p.text is None:
            raise InvalidParams("nothing to change", field="text")
        if m.media is None and not p.text.strip():
            raise InvalidParams("empty message", field="text")
        m.text, m.entities = (p.text or None), p.entities
        m.edited, m.edited_at = True, st.now()
        return T.MessageResult(message=st.out(chat, m))

    @rpc("messages.delete")
    async def delete(self, account, st: State, p: T.MessagesDeleteParams):
        chat = st.chat(p.chat_id)
        gone = set(p.message_ids)
        chat.messages = [m for m in chat.messages if m.id not in gone]
        if chat.messages:
            chat.conv.last_message_at = chat.messages[-1].sent_at
        return {}

    @rpc("messages.forward")
    async def forward(self, account, st: State, p: T.MessagesForwardParams):
        src, dst = st.chat(p.from_chat_id), st.chat(p.to_chat_id)
        found = sorted((st.msg(src, i) for i in dict.fromkeys(p.message_ids)), key=lambda m: int(m.id))
        out = []
        for orig in found:
            if orig.action is not None:
                continue
            m = self._new_out(st, dst, p.topic_id, None, p.silent)
            for f in ("text", "entities", "media", "poll", "location", "contact_card", "dice", "link_preview",
                      "grouped_id"):
                setattr(m, f, copy.deepcopy(getattr(orig, f)))
            if p.drop_captions and m.media is not None:
                m.text, m.entities = None, None
            if not p.drop_author:
                m.forward_from = orig.forward_from or T.ForwardInfo(
                    name=orig.sender_name, sender_id=orig.sender_id,
                    chat_id=src.id if src.conv.kind == "channel" else None,
                    message_id=orig.id if src.conv.kind == "channel" else None, at=orig.sent_at)
            if orig.media is not None:
                dst.media_keys[m.id] = st.media_key(src, orig)
            self._commit(st, dst, m)
            out.append(st.out(dst, m))
        return T.MessagesResult(messages=out)

    @rpc("messages.pin")
    async def pin(self, account, st: State, p: T.MessagesPinParams):
        st.msg(st.chat(p.chat_id), p.message_id).is_pinned = True
        return {}

    @rpc("messages.unpin")
    async def unpin(self, account, st: State, p: T.MessagesUnpinParams):
        chat = st.chat(p.chat_id)
        targets = [st.msg(chat, p.message_id)] if p.message_id else [m for m in chat.messages if m.is_pinned]
        for m in targets:
            m.is_pinned = False
        return {}

    # ---- media -----------------------------------------------------------------------------

    @rpc("media.download")
    async def download(self, account, st: State, p: T.MediaDownloadParams):
        chat = st.chat(p.chat_id)
        m = st.msg(chat, p.message_id)
        if m.media is None:
            raise NotFound("the message has no media", kind="media")
        if str(p.what) == "thumb":
            thumb = st.thumb_path(chat, m)
            if thumb is None:
                raise NotFound("no thumbnail for this media", kind="thumb")
            return T.MediaDownloadResult(path=str(thumb), mime="image/png", bytes=thumb.stat().st_size)
        path, key = st._full_spec(chat, m)
        if not path.is_file():
            await self._write_chunked(st, path, st.full_bytes(key, m.media))
        return T.MediaDownloadResult(path=str(path), mime=m.media.mime, bytes=path.stat().st_size)

    @rpc("avatars.download")
    async def avatar(self, account, st: State, p: T.AvatarsDownloadParams):
        w = st.w
        if str(p.kind) == "chat":
            chat = st.chat(p.id)
            if chat.conv.is_self_chat:
                return T.PathResult()
            if chat.conv.kind == "dm":
                return self._user_avatar(st, chat.conv.peer_user_id or "", p.size)
            return T.PathResult(path=str(self._avatar_file(st, f"chat_{chat.id}", p.size)))
        if w.user(p.id) is None:
            raise NotFound(f"no user {p.id}", kind="user")
        return self._user_avatar(st, p.id, p.size)

    def _user_avatar(self, st: State, user_id: str, size: Any) -> T.PathResult:
        u = st.w.user(user_id)
        if u is None or not u.has_avatar:
            return T.PathResult()
        return T.PathResult(path=str(self._avatar_file(st, f"user_{u.id}", size)))

    def _avatar_file(self, st: State, name: str, size: Any) -> Path:
        px = 160 if str(size) == "big" else 64
        path = st.cache / "avatars" / f"{name}_{px}.png"
        if not path.is_file():
            write_atomic(path, M.png(px, px, f"avatar/{st.seed}/{name}"))
        return path

    # ---- reactions, polls, bots ------------------------------------------------------------

    @rpc("reactions.available")
    async def reactions_available(self, account, st: State, p: T.ReactionsAvailableParams):
        return T.ReactionsAvailableResult(reactions=[T.ReactionKey(emoji=e) for e in EMOJI], allow_custom=False,
                                          max_per_message=3)

    @rpc("reactions.set")
    async def reactions_set(self, account, st: State, p: T.ReactionsSetParams):
        chat = st.chat(p.chat_id)
        m = st.msg(chat, p.message_id)
        if len(p.reactions) > 3:
            raise InvalidParams("at most 3 reactions", field="reactions")
        want = [k.emoji for k in p.reactions]
        if any(e not in EMOJI for e in want):
            raise InvalidParams("reaction not available here", field="reactions")
        out = []
        for r in m.reactions or []:
            e = r.key.emoji
            count = r.count - (1 if r.mine else 0) + (1 if e in want else 0)
            if count > 0:
                out.append(T.Reaction(key=r.key, count=count, mine=e in want))
        have = {r.key.emoji for r in out}
        out += [T.Reaction(key=T.ReactionKey(emoji=e), count=1, mine=True) for e in want if e not in have]
        m.reactions = out
        return T.OptionalMessageResult(message=st.out(chat, m))

    @rpc("reactions.who")
    async def reactions_who(self, account, st: State, p: T.ReactionsWhoParams):
        chat = st.chat(p.chat_id)
        m = st.msg(chat, p.message_id)
        users = [x.user_id for x in chat.members if x.user_id != ME] or [u.id for u in st.w.people()]
        reactors, k = [], 0
        for r in m.reactions or []:
            if p.key is not None and r.key.emoji != p.key.emoji:
                continue
            for _ in range(r.count - (1 if r.mine else 0)):
                uid = users[k % len(users)]
                k += 1
                reactors.append(T.Reactor(user_id=uid, key=r.key, name=st.w.user(uid).name))
        pg, nxt = page(reactors, p.limit, p.cursor)
        return T.ReactionsWhoResult(reactors=pg, next_cursor=nxt)

    @rpc("polls.create")
    async def polls_create(self, account, st: State, p: T.PollsCreateParams):
        chat = st.chat(p.chat_id)
        if not 2 <= len(p.options) <= 10:
            raise InvalidParams("a poll has 2 to 10 options", field="options")
        if p.quiz and (p.correct_option is None or not 0 <= p.correct_option < len(p.options)):
            raise InvalidParams("a quiz needs correct_option", field="correct_option")
        m = self._new_out(st, chat, p.topic_id, p.reply_to, p.silent)
        m.poll = T.Poll(
            id=f"poll-{chat.id}-{m.id}", question=p.question, total_voters=0, multiple=bool(p.multiple),
            quiz=bool(p.quiz), anonymous=True if p.anonymous is None else p.anonymous, solution=p.solution,
            solution_entities=p.solution_entities,
            options=[T.PollOption(id=str(i), text=t, votes=0, correct=(i == p.correct_option) if p.quiz else None)
                     for i, t in enumerate(p.options)],
            close_at=m.sent_at + timedelta(seconds=p.close_after_seconds) if p.close_after_seconds else None,
        )
        self._commit(st, chat, m)
        return T.MessageResult(message=st.out(chat, m))

    @rpc("polls.vote")
    async def polls_vote(self, account, st: State, p: T.PollsVoteParams):
        chat = st.chat(p.chat_id)
        m = st.msg(chat, p.message_id)
        poll = m.poll
        if poll is None:
            raise NotFound("not a poll", kind="poll")
        if poll.closed:
            raise Forbidden("the poll is closed", reason="closed")
        ids = {o.id for o in poll.options}
        if any(i not in ids for i in p.option_ids) or (len(p.option_ids) > 1 and not poll.multiple):
            raise InvalidParams("bad option_ids", field="option_ids")
        had = any(o.chosen for o in poll.options)
        for o in poll.options:
            was, now = bool(o.chosen), o.id in p.option_ids
            o.votes = (o.votes or 0) - was + now
            o.chosen = now
        poll.total_voters = (poll.total_voters or 0) - had + bool(p.option_ids)
        return T.OptionalMessageResult(message=st.out(chat, m))

    @rpc("polls.close")
    async def polls_close(self, account, st: State, p: T.ChatMessageParams):
        chat = st.chat(p.chat_id)
        m = st.msg(chat, p.message_id)
        if m.poll is None:
            raise NotFound("not a poll", kind="poll")
        if m.direction != "out":
            raise Forbidden("only the author can close a poll", reason="not_mine")
        m.poll.closed = True
        return T.OptionalMessageResult(message=st.out(chat, m))

    @rpc("bots.commands")
    async def bots_commands(self, account, st: State, p: T.BotsCommandsParams):
        if p.bot_id != BOT:
            raise NotFound(f"{p.bot_id} is not a bot", kind="user")
        return {"commands": [{"command": c, "description": d} for c, d in BOT_COMMANDS]}

    @rpc("bots.press_button")
    async def bots_press(self, account, st: State, p: T.BotsPressButtonParams):
        chat = st.chat(p.chat_id)
        m = st.msg(chat, p.message_id)
        if str(p.button.kind) == "url":
            return T.BotsPressButtonResult(url=p.button.url)
        if p.button.data == "ping":
            return T.BotsPressButtonResult(text="Pong")
        if p.button.data == "count":
            st.presses += 1
            m.text, m.edited, m.edited_at = f"Pressed {st.presses} times", True, st.now()
            return T.BotsPressButtonResult(message=st.out(chat, m))
        raise NotFound("unknown button", kind="button")

    async def _bot_reply(self, account: Account, st: State, chat: Chat, sent: T.Message) -> None:
        await account.presence(BOT, action="typing")
        await asyncio.sleep(0.05 + st.cfg["latency_ms"] / 1000)
        bot = st.w.users[BOT]
        cmd = (sent.text or "").split()[0] if sent.text else ""
        r = T.Message(id=chat.new_id(), chat_id=chat.id, direction="in", sent_at=st.now(), sender_id=BOT,
                      sender_name=bot.name, reply_to=T.ReplyRef(message_id=sent.id, preview=preview(sent)))
        if cmd == "/start":
            r.text = "Hi! I am the fake bot. Try /help, /buttons or /photo."
        elif cmd == "/help":
            r.text = "\n".join(f"/{c} - {d}" for c, d in BOT_COMMANDS)
            r.entities, pos = [], 0
            for line in r.text.split("\n"):
                r.entities.append(T.Entity(type="bot_command", offset=u16(r.text[:pos]), length=u16(line.split()[0])))
                pos += len(line) + 1
        elif cmd == "/buttons":
            r.text, r.buttons = "Pick one:", bot_buttons()
        elif cmd == "/photo":
            r.media = T.Media(kind="photo", mime="image/png", width=320, height=240, filename=f"photo_{r.id}.png")
        else:
            r.text = f"You said: {sent.text}"
        self._commit(st, chat, r)
        sent.delivery = "read"
        chat.conv.read_outbox_max_id = sent.id
        await account.messages_upsert([st.out(chat, r)])
        await account.read_outbox(chat.id, sent.id)

    # ---- read state, typing, drafts -----------------------------------------------------

    @rpc("read.send_receipt")
    async def read_receipt(self, account, st: State, p: T.ReadSendReceiptParams):
        chat = st.chat(p.chat_id)
        upto = int(p.max_message_id) if p.max_message_id.isdigit() else 0
        conv = chat.conv
        if p.topic_id is None:
            conv.read_inbox_max_id = str(max(upto, int(conv.read_inbox_max_id or 0)))
        changed: list[T.Topic] = []
        for t in chat.topics:
            if p.topic_id in (None, t.id):
                t.read_inbox_max_id = str(max(upto, int(t.read_inbox_max_id or 0)))
                t.unread_count = sum(1 for m in chat.messages if m.topic_id == t.id and m.direction == "in"
                                     and int(m.id) > int(t.read_inbox_max_id))
                changed.append(T.Topic(id=t.id, chat_id=chat.id, title=t.title, unread_count=t.unread_count,
                                       read_inbox_max_id=t.read_inbox_max_id))
        conv.unread_count = sum(1 for m in chat.messages if m.direction == "in" and m.action is None
                                and int(m.id) > upto)
        conv.unread_mentions = sum(1 for m in chat.messages if m.mentioned and int(m.id) > upto)
        conv.marked_unread = False if conv.marked_unread else conv.marked_unread
        # The platform's update after a read: only the counters that changed (spec 8.4, 10.1).
        await account.conversations_upsert([T.Conversation(
            id=conv.id, kind=conv.kind, unread_count=conv.unread_count, unread_mentions=conv.unread_mentions,
            marked_unread=conv.marked_unread)])
        if changed:
            await account.topics_upsert(chat.id, changed)
        return {}

    @rpc("typing.send")
    async def typing(self, account, st: State, p: T.TypingSendParams):
        st.chat(p.chat_id)
        return {}

    @rpc("drafts.save")
    async def drafts_save(self, account, st: State, p: T.DraftsSaveParams):
        chat = st.chat(p.chat_id)
        if p.topic_id is not None:
            topic = next((t for t in chat.topics if t.id == p.topic_id), None)
            if topic is None:
                raise NotFound(f"no topic {p.topic_id}", kind="topic")
            topic.draft = p.draft
        else:
            chat.conv.draft = p.draft
        return {}

    # ---- chats, topics, folders, members --------------------------------------------------

    @rpc("chats.sync")
    async def chats_sync(self, account, st: State, p: T.ChatsSyncParams):
        await self._sync(account, st, include_archived=p.include_archived is not False)
        return {}

    @rpc("chats.get")
    async def get_chat(self, account, st: State, p: T.ChatsGetParams):
        conv = st.chat(p.chat_id).conv
        return T.ChatsGetResult(conversation=T.Conversation(
            id=conv.id, kind=conv.kind, pinned=bool(conv.pinned), marked_unread=bool(conv.marked_unread)))

    @rpc("chats.set_flags")
    async def set_flags(self, account, st: State, p: T.ChatsSetFlagsParams):
        conv = st.chat(p.chat_id).conv
        if p.pinned is not None:
            if p.pinned and not conv.pinned:
                conv.pin_order = sum(1 for c in st.w.chats.values() if c.conv.pinned)
            conv.pinned, conv.pin_order = p.pinned, conv.pin_order if p.pinned else None
        if p.muted is not None:
            conv.muted, conv.mute_until = p.muted, p.mute_until if p.muted else None
        if p.archived is not None:
            conv.archived = p.archived
        if p.marked_unread is not None:
            conv.marked_unread = p.marked_unread
        return {}

    @rpc("topics.list")
    async def topics_list(self, account, st: State, p: T.TopicsListParams):
        chat = st.chat(p.chat_id)
        if not chat.topics:
            raise NotFound("this chat is not a forum", kind="chat")
        pg, nxt = page(chat.topics, p.limit or 100, p.cursor)
        return T.TopicsListResult(topics=pg, next_cursor=nxt)

    @rpc("folders.list")
    async def folders_list(self, account, st: State, p: T.AccountParams):
        return T.FoldersListResult(folders=st.w.folders)

    @rpc("members.list")
    async def members_list(self, account, st: State, p: T.MembersListParams):
        chat = st.chat(p.chat_id)
        w, q, flt = st.w, (p.query or "").casefold(), str(p.filter or "all")
        out = []
        for x in chat.members:
            u = w.user(x.user_id)
            if u is None:
                continue
            if q and q not in u.name.casefold() and q not in (u.username or "").casefold():
                continue
            if (flt == "admins" and x.role not in ("owner", "admin")) or (flt == "bots" and not u.is_bot) \
                    or flt in ("banned", "restricted") and x.role != flt:
                continue
            out.append(T.Member(user=u.contact(), role=x.role, rank=x.rank, joined_at=x.joined_at))
        pg, nxt = page(out, p.limit, p.cursor)
        return T.MembersListResult(members=pg, next_cursor=nxt, total=len(out))

    # ---- contacts, profile -----------------------------------------------------------------

    @rpc("contacts.list")
    async def contacts_list(self, account, st: State, p: T.AccountParams):
        return T.ContactsResult(contacts=[u.contact() for u in st.w.people() if u.is_contact])

    def _profile(self, st: State, user_id: str) -> T.Profile:
        u = st.w.user(user_id)
        if u is None:
            raise NotFound(f"no user {user_id}", kind="user")
        common = sum(1 for c in st.w.chats.values() if c.conv.kind != "dm"
                     and any(x.user_id == user_id for x in c.members))
        return T.Profile(user_id=u.id, display_name=u.name, first_name=u.first, last_name=u.last,
                         username=u.username, phone=u.phone, bio=u.bio, is_contact=u.is_contact,
                         is_blocked=u.blocked, is_premium=False, common_chats_count=common, status=u.status,
                         birthday=f"--{(int(u.id[1:]) % 12) + 1:02d}-15" if u.id[1:].isdigit() else None)

    @rpc("contacts.info")
    async def contacts_info(self, account, st: State, p: T.UserParams):
        return T.ProfileResult(profile=self._profile(st, p.user_id))

    @rpc("profile.get")
    async def profile_get(self, account, st: State, p: T.AccountParams):
        return T.ProfileResult(profile=self._profile(st, ME))

    @rpc("lookup.username")
    async def lookup_username(self, account, st: State, p: T.LookupUsernameParams):
        name = p.username.lstrip("@").casefold()
        for u in [st.w.me, *st.w.users.values()]:
            if (u.username or "").casefold() == name:
                return T.LookupUsernameResult(contact=u.contact())
        for c in st.w.chats.values():
            if c.conv.kind != "dm" and (c.conv.username or "").casefold() == name:
                return T.LookupUsernameResult(conversation=st.conv(c))
        return T.LookupUsernameResult()

    # ---- stickers, GIFs ----------------------------------------------------------------------

    def _sticker_out(self, st: State, s: T.Sticker) -> T.Sticker:
        o = copy.copy(s)
        thumb = st.cache / "stickers" / f"{s.id}_thumb.png"
        o.thumb_path = str(thumb) if thumb.is_file() else None
        return o

    @rpc("stickers.recent")
    async def stickers_recent(self, account, st: State, p: T.AccountLimitParams):
        ids = st.w.recent_stickers[:p.limit or 20]
        return T.StickersResult(stickers=[self._sticker_out(st, st.w.sticker(i)) for i in ids])

    @rpc("stickers.packs")
    async def stickers_packs(self, account, st: State, p: T.AccountParams):
        return T.PacksResult(packs=st.w.packs)

    @rpc("stickers.pack")
    async def stickers_pack(self, account, st: State, p: T.PackParams):
        pack = next((x for x in st.w.packs if x.id == p.pack_id), None)
        if pack is None:
            raise NotFound(f"no pack {p.pack_id}", kind="pack")
        return T.PackResult(pack=pack, stickers=[self._sticker_out(st, s) for s in st.w.stickers
                                                 if s.pack_id == pack.id])

    @rpc("stickers.thumb")
    async def stickers_thumb(self, account, st: State, p: T.StickerParams):
        if st.w.sticker(p.sticker_id) is None:
            raise NotFound(f"no sticker {p.sticker_id}", kind="sticker")
        path = st.cache / "stickers" / f"{p.sticker_id}_thumb.png"
        if not path.is_file():
            write_atomic(path, M.png(64, 64, f"sticker/{st.seed}/{p.sticker_id}"))
        return T.PathResult(path=str(path))

    @rpc("stickers.send")
    async def stickers_send(self, account, st: State, p: T.StickersSendParams):
        chat = st.chat(p.chat_id)
        s = st.w.sticker(p.sticker_id)
        if s is None:
            raise NotFound(f"no sticker {p.sticker_id}", kind="sticker")
        m = self._new_out(st, chat, p.topic_id, p.reply_to, p.silent)
        animated = str(s.format) == "animated"
        m.media = T.Media(kind="sticker", mime="application/json" if animated else "image/png", width=512,
                          height=512, sticker=T.StickerRef(emoji=s.emoji, pack_id=s.pack_id, format=s.format))
        chat.media_keys[m.id] = f"sticker:{s.id}"
        recent = st.w.recent_stickers
        if s.id in recent:
            recent.remove(s.id)
        recent.insert(0, s.id)
        self._commit(st, chat, m)
        return T.MessageResult(message=st.out(chat, m))

    def _gif(self, st: State, gif_id: str) -> T.Gif:
        g = next((x for x in st.w.gifs if x.id == gif_id), None)
        if g is None:
            raise NotFound(f"no gif {gif_id}", kind="gif")
        return g

    def _gif_file(self, st: State, gif_id: str, thumb: bool) -> Path:
        path = st.cache / "gifs" / (f"{gif_id}_thumb.png" if thumb else f"{gif_id}.gif")
        if not path.is_file():
            key = f"gif/{st.seed}/{gif_id}"
            write_atomic(path, M.png(32, 32, key) if thumb else M.gif(64, 64, key))
        return path

    @rpc("gifs.list")
    async def gifs_list(self, account, st: State, p: T.AccountLimitParams):
        out = []
        for g in st.w.gifs[:p.limit or 50]:
            o = copy.copy(g)
            t = st.cache / "gifs" / f"{g.id}_thumb.png"
            o.thumb_path = str(t) if t.is_file() else None
            out.append(o)
        return T.GifsResult(gifs=out)

    @rpc("gifs.thumb")
    async def gifs_thumb(self, account, st: State, p: T.GifParams):
        self._gif(st, p.gif_id)
        return T.PathResult(path=str(self._gif_file(st, p.gif_id, thumb=True)))

    @rpc("gifs.clip")
    async def gifs_clip(self, account, st: State, p: T.GifParams):
        self._gif(st, p.gif_id)
        return T.PathResult(path=str(self._gif_file(st, p.gif_id, thumb=False)))

    @rpc("gifs.send")
    async def gifs_send(self, account, st: State, p: T.GifsSendParams):
        chat = st.chat(p.chat_id)
        g = self._gif(st, p.gif_id)
        m = self._new_out(st, chat, p.topic_id, p.reply_to, p.silent)
        m.media = T.Media(kind="gif", mime="image/gif", width=g.width, height=g.height, duration=g.duration,
                          filename=f"{g.id}.gif")
        chat.media_keys[m.id] = f"gif:{g.id}"
        self._commit(st, chat, m)
        return T.MessageResult(message=st.out(chat, m))

    # ---- account -----------------------------------------------------------------------------

    @rpc("ghost.apply")
    async def ghost_apply(self, account, st: State, p: T.GhostApplyParams):
        st.ghost = p.enabled
        return {}

    @rpc("account.sign_out", online=False)
    async def sign_out(self, account, st: State, p: T.AccountParams):
        if st.live_task is not None:
            st.live_task.cancel()
            st.live_task = None
        st.online = False
        account.data.pop("phone", None)
        if "session" in account.secrets:
            await account.delete_secret("session")
        current_request().after_response(lambda: _spawned(account, account.status("auth_required")))
        return {}

    # ---- live loop -----------------------------------------------------------------------------

    async def _live(self, account: Account, st: State) -> None:
        rng = random.Random(f"{st.seed}/live")
        interval = st.cfg["live_interval_ms"] / 1000
        w = st.w
        while True:
            await asyncio.sleep(interval)
            if not st.online:
                continue
            action = rng.choices(["message", "typing", "presence", "read", "reaction", "edit"],
                                 weights=[4, 2, 2, 1, 1, 1])[0]
            chats = [c for c in w.chats.values() if not c.conv.is_self_chat and c.id != BOT_CHAT]
            if not chats:
                continue
            chat = rng.choice(chats)
            others = [x.user_id for x in chat.members if x.user_id != ME]
            who = rng.choice(others) if others else None
            group_chat = chat.id if chat.conv.kind != "dm" else None
            if action in ("message", "typing") and who and chat.conv.kind != "channel":
                await account.presence(who, chat_id=group_chat, topic_id="1" if chat.topics else None,
                                       action="typing", expires_at=ms(datetime.now(UTC)) + timedelta(seconds=6))
            if action == "message":
                await asyncio.sleep(min(1.0, interval / 2))
                u = w.user(who) if who else None
                m = T.Message(id=chat.new_id(), chat_id=chat.id, direction="in", sent_at=st.now(),
                              topic_id="1" if chat.topics else None, text=rng.choice(SENTENCES))
                if chat.conv.kind == "channel" or u is None:
                    m.sender_name, m.views = chat.conv.title, rng.randint(1, 100)
                else:
                    m.sender_id, m.sender_name = u.id, u.name
                if chat.topics and who:
                    await account.presence(who, chat_id=chat.id, topic_id="1", action="cancel")
                self._commit(st, chat, m)
                chat.conv.unread_count += 1
                await account.messages_upsert([st.out(chat, m)])
                await account.conversations_upsert([T.Conversation(  # only what changed (spec 8.4)
                    id=chat.id, kind=chat.conv.kind, unread_count=chat.conv.unread_count,
                    last_message_at=chat.conv.last_message_at)])
            elif action == "presence":
                u = rng.choice(w.people())
                now = ms(datetime.now(UTC))
                online = not (u.status and str(u.status.state) == "online")
                u.status = (T.OnlineStatus(state="online", until=now + timedelta(minutes=5)) if online
                            else T.OnlineStatus(state="offline", last_seen_at=now))
                await account.presence(u.id, online=u.status)
            elif action == "read":
                outs = [m for m in chat.messages if m.direction == "out" and m.delivery != "read"]
                if outs:
                    for m in outs:
                        m.delivery = "read"
                    chat.conv.read_outbox_max_id = outs[-1].id
                    await account.read_outbox(chat.id, outs[-1].id)
            elif action == "reaction" and who:
                cand = [m for m in chat.messages[-20:] if m.action is None]
                if cand:
                    m = rng.choice(cand)
                    e = rng.choice(EMOJI)
                    rs = m.reactions or []
                    hit = next((r for r in rs if r.key.emoji == e), None)
                    if hit:
                        hit.count += 1
                    else:
                        rs.append(T.Reaction(key=T.ReactionKey(emoji=e), count=1, mine=False))
                    m.reactions = rs
                    u = w.user(who)
                    await account.reaction_changed(chat.id, m.id, rs, reactors=[T.Reactor(
                        user_id=who, key=T.ReactionKey(emoji=e), name=u.name if u else None,
                        at=ms(datetime.now(UTC)), unread=m.direction == "out")])
            elif action == "edit":
                cand = [m for m in chat.messages[-20:] if m.direction == "in" and m.text and m.action is None]
                if cand:
                    m = rng.choice(cand)
                    if not m.text.endswith(" ✏️"):
                        m.text += " ✏️"
                    m.edited, m.edited_at = True, st.now()
                    await account.message_edited(st.out(chat, m))


async def _spawned(account: Account, coro) -> None:
    account.spawn(coro)


def _safe_name(name: str) -> str:
    clean = re.sub(r"[^\w.\- ]+", "_", name).strip(" .") or "file"
    return clean[:120]


def _safe_suffix(name: str) -> str:
    suf = Path(name).suffix
    return suf if re.fullmatch(r"\.[A-Za-z0-9]{1,8}", suf) else ".bin"
