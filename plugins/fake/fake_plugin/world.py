"""The fake account's data: users, chats, folders, topics, history, stickers and GIFs.

Everything is derived from ``seed`` with seeded RNGs (one per chat, so generation order
never matters) and a fixed base clock. Ids are short ASCII strings; message ids are
increasing integers per chat, rendered as strings (opaque to the core, spec 8.1).
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from contact_hub_sdk import types as T

from .media import CLIP

#: All generated history ends before this instant; new events tick forward from it.
BASE = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
ME = "u0"
SAVED, BOT_CHAT, GROUP, CHANNEL, FORUM = "c0", "c1", "c2", "c3", "c4"
BOT = "b1"
PLUGIN = "fake"
CLIP_BYTES = CLIP.stat().st_size
BLOB_BYTES = 2 * 1024 * 1024  # dataset.bin: big enough to watch progress and cancel

FIRST = ["Alice", "Boris", "Chen", "Dana", "Emil", "Farah", "Gleb", "Hana", "Igor", "Julia", "Kenji",
         "Lena", "Mateo", "Nadia", "Oleg", "Priya", "Quinn", "Rosa", "Sven", "Tanya", "Umar", "Vera",
         "Wei", "Xenia", "Yusuf", "Zoe", "Анна", "Дмитрий", "Мария", "Сергей"]
LAST = ["Andersen", "Baker", "Costa", "Dubois", "Evans", "Fischer", "Garcia", "Huang", "Ivanova",
        "Jensen", "Kowalski", "Lopez", "Müller", "Novak", "Okafor", "Petrov", "Rossi", "Smirnov",
        "Tanaka", "Weber"]
SENTENCES = [
    "Hi! How are you?", "Did you see the news today?", "Let's meet at 6 pm.", "Sounds good 👍",
    "I'll send the files later.", "Can you check the report?", "Haha, that's great 😂",
    "Running a bit late, sorry.", "What do you think about the new design?", "Thanks a lot!",
    "Where are we going for lunch?", "The build is green again 🎉", "Call me when you're free.",
    "Привет! Как дела?", "Это отличная идея.", "See https://example.org/docs for details.",
    "Ping me on #release when it's done.", "Let's discuss this tomorrow 🙂", "Good morning ☀️",
    "Here is the summary of the meeting: we agreed on the plan, the budget and the timeline.",
    "I think we should ship it on Friday.", "Can't wait for the weekend 🏖️", "OK", "Yes", "No 😅",
    "Who's in for a quick sync?", "The deploy finished in 4 minutes.", "Let me know 🙏",
]
EMOJI = ["👍", "❤", "😂", "🔥", "🎉", "😮", "😢", "🙏"]
TOPICS = [("1", "General", None, True), ("2", "Announcements", "📣", False),
          ("3", "Design", "🎨", False), ("4", "Off-topic", "☕", False)]
BOT_COMMANDS = [("start", "Start the bot"), ("help", "What I can do"),
                ("buttons", "Show inline buttons"), ("photo", "Send a photo")]
ENTITY_TYPES = ["bold", "italic", "underline", "strike", "spoiler", "code", "pre", "blockquote", "url",
                "text_url", "mention", "mention_name", "hashtag", "cashtag", "bot_command", "email",
                "phone", "custom_emoji", "bank_card"]


def u16(s: str) -> int:
    """Length in UTF-16 code units (spec 8.3)."""
    return len(s.encode("utf-16-le")) // 2


#: The "on create" values of the merge-updated fields (spec 8.4). The fake writes them
#: explicitly on a chat's, topic's or message's first emission so the recorded wire does
#: not depend on the core's defaults; later updates carry only the fields that changed.
ON_CREATE: dict[type, dict[str, object]] = {
    T.Conversation: {"archived": False, "unread_count": 0, "unread_mentions": 0, "unread_reactions": 0,
                     "is_self_chat": False},
    T.Topic: {"is_general": False, "unread_count": 0, "unread_mentions": 0, "unread_reactions": 0,
              "pinned": False, "closed": False, "hidden": False},
    T.Message: {"edited": False, "is_scheduled": False},
}


def created(obj):
    """Fill the unset merge fields of a new Conversation/Topic/Message with their on-create values."""
    for k, v in ON_CREATE[type(obj)].items():
        if getattr(obj, k) is None:
            setattr(obj, k, v)
    return obj


def ms(dt: datetime) -> datetime:
    return dt.replace(microsecond=dt.microsecond // 1000 * 1000)


@dataclass
class User:
    id: str
    first: str
    last: str | None
    username: str | None
    phone: str | None
    is_contact: bool
    is_bot: bool = False
    has_avatar: bool = True
    bio: str | None = None
    status: T.OnlineStatus | None = None
    blocked: bool = False

    @property
    def name(self) -> str:
        return f"{self.first} {self.last}" if self.last else self.first

    def contact(self) -> T.Contact:
        return T.Contact(id=self.id, display_name=self.name, first_name=self.first, last_name=self.last,
                         username=self.username, phone=self.phone, is_bot=self.is_bot,
                         is_contact=self.is_contact, bio=self.bio, status=self.status)


@dataclass
class MemberInfo:
    user_id: str
    role: str
    rank: str | None
    joined_at: datetime


@dataclass
class Chat:
    conv: T.Conversation
    messages: list[T.Message] = field(default_factory=list)  # ascending by int(id)
    members: list[MemberInfo] = field(default_factory=list)
    topics: list[T.Topic] = field(default_factory=list)
    #: message id -> where its media bytes come from: ``gen:<key>`` (default, generated),
    #: ``sticker:<id>``, ``gif:<id>``, ``file:<path in cache_dir>`` (uploads).
    media_keys: dict[str, str] = field(default_factory=dict)
    next_id: int = 1

    @property
    def id(self) -> str:
        return self.conv.id

    def find(self, message_id: str) -> T.Message | None:
        for m in self.messages:
            if m.id == message_id:
                return m
        return None

    def new_id(self) -> str:
        i = self.next_id
        self.next_id += 1
        return str(i)


class World:
    def __init__(self, seed: int, n_chats: int, n_messages: int):
        self.seed = seed
        self.n_messages = n_messages
        rng = random.Random(f"{seed}/world")
        self.me = User(ME, "Test", "User", "test_user", "+15550100000", False, bio="Fake account for tests")
        self.users: dict[str, User] = {}
        n_dms = max(0, n_chats - 5)
        n_users = max(12, n_dms + 6)
        for i in range(1, n_users + 1):
            first, last = rng.choice(FIRST), rng.choice(LAST + [None])
            self.users[f"u{i}"] = User(
                id=f"u{i}", first=first, last=last,
                username=f"{first.lower()}{i}" if first.isascii() and rng.random() < 0.7 else None,
                phone=f"+1555{i:07d}" if rng.random() < 0.8 else None,
                is_contact=rng.random() < 0.7, has_avatar=rng.random() < 0.75,
                bio=rng.choice([None, "Coffee first ☕", "Engineer", "🌍 Traveller"]),
                status=self._status(rng),
            )
        self.users[BOT] = User(BOT, "Fake Bot", None, "fakehub_bot", None, False, is_bot=True,
                               bio="I echo what you send. Try /buttons.")
        self.users[f"u{n_users}"].blocked = True
        self.chats: dict[str, Chat] = {}
        self.order: list[str] = []
        self._build_chats(n_chats, rng)
        self.folders = self._folders()
        self.packs, self.stickers = self._stickers()
        self.gifs = [T.Gif(id=f"g{i}", width=64, height=64, duration=1, mime="image/gif") for i in range(1, 7)]
        self.recent_stickers = [s.id for s in self.stickers[:6]]

    # ---- users ------------------------------------------------------------------------

    @staticmethod
    def _status(rng: random.Random) -> T.OnlineStatus:
        state = rng.choice(["online", "offline", "offline", "recently", "within_week", "long_ago"])
        if state == "offline":
            return T.OnlineStatus(state=state, last_seen_at=BASE - timedelta(minutes=rng.randint(5, 3000)))
        if state == "online":
            return T.OnlineStatus(state=state, until=BASE + timedelta(minutes=5))
        return T.OnlineStatus(state=state)

    def user(self, user_id: str) -> User | None:
        return self.me if user_id == ME else self.users.get(user_id)

    def people(self) -> list[User]:
        return [u for u in self.users.values() if not u.is_bot]

    # ---- chats ------------------------------------------------------------------------

    def _build_chats(self, n_chats: int, rng: random.Random) -> None:
        people = self.people()
        specs: list[tuple[str, str]] = [(SAVED, "saved"), (BOT_CHAT, "bot"), (GROUP, "group"),
                                        (CHANNEL, "channel"), (FORUM, "forum")][:max(1, n_chats)]
        for i in range(5, n_chats):
            specs.append((f"c{i}", "group" if i % 7 == 6 else "dm"))
        dm_users = iter(people)
        for idx, (cid, kind) in enumerate(specs):
            crng = random.Random(f"{self.seed}/chat/{cid}")
            conv = created(T.Conversation(id=cid, kind="dm"))
            chat = Chat(conv)
            if kind == "saved":
                conv.title, conv.peer_user_id, conv.is_self_chat = "Saved Messages", ME, True
                conv.pinned, conv.pin_order = True, 0
                chat.members = [MemberInfo(ME, "member", None, BASE)]
            elif kind == "bot":
                conv.title, conv.peer_user_id, conv.username = "Fake Bot", BOT, "fakehub_bot"
                chat.members = [MemberInfo(ME, "member", None, BASE), MemberInfo(BOT, "member", None, BASE)]
            elif kind == "dm":
                peer = next(dm_users)
                conv.title, conv.peer_user_id, conv.username = peer.name, peer.id, peer.username
                conv.color_index = crng.randint(0, 6)
                chat.members = [MemberInfo(ME, "member", None, BASE), MemberInfo(peer.id, "member", None, BASE)]
            else:
                conv.kind = "channel" if kind == "channel" else "group"
                names = {"group": f"Team {crng.choice(LAST)}", "channel": "Fake News", "forum": "Fake Forum"}
                conv.title = "Fake Hub Team" if cid == GROUP else names[kind]
                conv.about = {"channel": "Deterministic headlines.", "forum": "A forum with topics."}.get(kind)
                if kind == "channel":
                    conv.username, conv.can_post, conv.muted = "fakenews", False, True
                if kind == "forum":
                    conv.username, conv.is_forum = "fakeforum", True
                if cid == GROUP:
                    conv.pinned, conv.pin_order = True, 1
                    conv.permissions = {"send_messages": True, "send_media": True, "send_polls": True,
                                        "pin_messages": False, "change_info": False}
                k = crng.randint(5, min(12, len(people)))
                picked = crng.sample(people, k)
                my_role = {"group": "admin", "forum": "member", "channel": "member"}[kind]
                chat.members = [MemberInfo(picked[0].id, "owner", "owner", BASE - timedelta(days=400)),
                                MemberInfo(ME, my_role, "admin" if my_role == "admin" else None,
                                           BASE - timedelta(days=300))]
                for j, u in enumerate(picked[1:]):
                    role = "admin" if j == 0 else "member"
                    chat.members.append(MemberInfo(u.id, role, "admin" if role == "admin" else None,
                                                   BASE - timedelta(days=crng.randint(1, 299))))
                conv.member_count = 1234 if kind == "channel" else len(chat.members)
                if kind == "forum":
                    chat.topics = [created(T.Topic(id=t, chat_id=cid, title=title, icon_emoji=emo, is_general=gen,
                                                   icon_color=0x6FB9F0 if not gen else None))
                                   for t, title, emo, gen in TOPICS]
            if kind == "dm" and idx == n_chats - 1 and n_chats > 6:
                conv.archived = True
            end = BASE - timedelta(minutes=7 * idx + crng.randint(0, 6))
            self._history(chat, kind, crng, end)
            self.chats[cid] = chat
            self.order.append(cid)
        dms = [c for c in self.chats.values() if c.conv.kind == "dm" and not c.conv.is_self_chat
               and c.conv.peer_user_id != BOT]
        if dms:
            dms[0].conv.draft = T.Draft(text="Don't forget the slides", updated_at=BASE - timedelta(minutes=3))

    def _history(self, chat: Chat, kind: str, rng: random.Random, end: datetime) -> None:
        n = self.n_messages
        times: list[datetime] = []
        t = end
        for _ in range(n):
            times.append(ms(t))
            t -= timedelta(seconds=rng.choice([20, 45, 90, 300, 900, 3600, 4 * 3600, 86400]))
        times.reverse()
        album: tuple[str, int] | None = None
        for i in range(n):
            mid = chat.new_id()
            msg = self._message(chat, kind, rng, mid, times[i], i, album)
            if msg.grouped_id:
                album = (msg.grouped_id, album[1] - 1 if album else 2)
                if album[1] <= 0:
                    album = None
            chat.messages.append(created(msg))
        conv = chat.conv
        ins = [m for m in chat.messages if m.direction == "in" and m.action is None]
        outs = [m for m in chat.messages if m.direction == "out"]
        unread = min(len(ins), rng.choice([0, 0, 0, 1, 2, 5, 14]))
        if kind == "saved":
            unread = 0
        if ins:
            boundary = ins[-unread - 1] if unread < len(ins) else None
            conv.read_inbox_max_id = boundary.id if boundary else "0"
            conv.unread_count = unread
            conv.unread_mentions = sum(1 for m in ins[len(ins) - unread:] if m.mentioned)
        if outs and kind != "saved":
            read_upto = outs[-1] if rng.random() < 0.6 else (outs[-2] if len(outs) > 1 else None)
            conv.read_outbox_max_id = read_upto.id if read_upto else None
            for m in outs:
                m.delivery = "read" if read_upto and int(m.id) <= int(read_upto.id) else "sent"
        if chat.messages:
            conv.last_message_at = chat.messages[-1].sent_at
            if n >= 10:
                for m in rng.sample(chat.messages[:-1], 2 if n >= 30 else 1):
                    if m.action is None:
                        m.is_pinned = True
        for topic in chat.topics:
            tm = [m for m in chat.messages if m.topic_id == topic.id]
            if tm:
                topic.last_message_at = tm[-1].sent_at
                topic.last_message_preview = preview(tm[-1])
                topic.unread_count = sum(1 for m in tm if m.direction == "in"
                                         and int(m.id) > int(conv.read_inbox_max_id or 0))

    def _message(self, chat: Chat, kind: str, rng: random.Random, mid: str, at: datetime, i: int,
                 album: tuple[str, int] | None) -> T.Message:
        cid = chat.id
        m = T.Message(id=mid, chat_id=cid, direction="in", sent_at=at)
        others = [x.user_id for x in chat.members if x.user_id != ME] or [ME]
        if kind == "saved":
            m.direction = "out"
        elif kind == "channel":
            m.direction = "in"
        else:
            m.direction = "out" if rng.random() < (0.4 if kind in ("dm", "bot") else 0.2) else "in"
        if kind == "bot":
            m.direction = "out" if i % 2 == 0 else "in"
        sender = ME if m.direction == "out" else rng.choice(others)
        if kind == "channel":
            m.sender_name, m.views = chat.conv.title, rng.randint(100, 5000)
        else:
            m.sender_id = sender
            u = self.user(sender)
            m.sender_name = u.name if u else None
            if kind in ("group", "forum"):
                role = next((x.rank for x in chat.members if x.user_id == sender), None)
                m.sender_title = role
        if chat.topics:
            m.topic_id = rng.choice(chat.topics).id
        if i == 0 and kind in ("group", "channel", "forum"):
            m.direction, m.sender_id = "in", chat.members[0].user_id
            m.action = T.ServiceAction(kind="created", actor_id=chat.members[0].user_id, title=chat.conv.title)
            m.topic_id = "1" if chat.topics else None
            return m
        if kind == "bot":
            if m.direction == "out":
                m.text = rng.choice(["/start", "/help", "/buttons", "hello bot"])
                if m.text.startswith("/"):
                    m.entities = [T.Entity(type="bot_command", offset=0, length=u16(m.text))]
            else:
                m.text = "Pick one:"
                m.buttons = bot_buttons()
            return m
        if album is not None:
            m.grouped_id = album[0]
            m.media = self._media(cid, mid, "photo", rng)
            m.topic_id = chat.messages[-1].topic_id if chat.messages else m.topic_id
            m.direction, m.sender_id, m.sender_name = (chat.messages[-1].direction, chat.messages[-1].sender_id,
                                                       chat.messages[-1].sender_name)
            return m
        r = rng.random()
        if r < 0.55:
            m.text = rng.choice(SENTENCES)
            if rng.random() < 0.25:
                m.entities = self._entities(m.text, rng, chat)
        elif r < 0.62:
            m.media = self._media(cid, mid, "photo", rng)
            if rng.random() < 0.5:
                m.text = rng.choice(["Look at this", "📷", "From yesterday"])
            if rng.random() < 0.15 and n_left(i, self.n_messages) > 3:
                m.grouped_id = f"a{cid}-{mid}"
        elif r < 0.65:
            m.media = self._media(cid, mid, "video", rng)
        elif r < 0.69:
            m.media = self._media(cid, mid, "voice", rng)
        elif r < 0.70:
            m.media = self._media(cid, mid, "video_note", rng)
        elif r < 0.73:
            m.media = self._media(cid, mid, "document", rng)
        elif r < 0.75:
            m.media = self._media(cid, mid, "music", rng)
        elif r < 0.79:
            s = rng.choice(self._sticker_seeds())
            m.media = T.Media(kind="sticker", mime=s[2], width=512, height=512,
                              sticker=T.StickerRef(emoji=s[1], pack_id=s[3], format=s[4]))
            chat.media_keys[mid] = f"sticker:{s[0]}"
        elif r < 0.82:
            m.media = self._media(cid, mid, "gif", rng)
        elif r < 0.84 and kind in ("group", "forum", "channel"):
            m.poll = T.Poll(id=f"poll-{cid}-{mid}", question="Where should we meet?",
                            options=[T.PollOption(id=str(k), text=t, votes=rng.randint(0, 9))
                                     for k, t in enumerate(["Office", "Café", "Online"])],
                            anonymous=True)
            m.poll.total_voters = sum(o.votes or 0 for o in m.poll.options)
        elif r < 0.86:
            m.location = T.Location(lat=round(52.5 + rng.random(), 5), lon=round(13.4 + rng.random(), 5),
                                    title="Meeting point")
        elif r < 0.87:
            u = rng.choice(self.people())
            m.contact_card = T.ContactCard(name=u.name, phone=u.phone, user_id=u.id)
        elif r < 0.88:
            m.dice = T.Dice(emoticon="🎲", value=rng.randint(1, 6))
        elif r < 0.90 and kind in ("group", "forum"):
            who = rng.choice(others)
            m.direction, m.sender_id = "in", who
            m.action = T.ServiceAction(kind="joined", actor_id=who, target_ids=[who])
            return m
        else:
            m.text = "Check https://example.org"
            m.entities = [T.Entity(type="url", offset=6, length=19)]
            m.link_preview = T.LinkPreview(url="https://example.org", site="Example", title="Example Domain",
                                           description="This domain is for use in examples.")
        if kind in ("group", "forum") and m.direction == "in" and m.text and rng.random() < 0.08:
            m.text = "@test_user " + m.text
            m.entities = [T.Entity(type="mention", offset=0, length=10)] + [
                T.Entity(type=e.type, offset=e.offset + 11, length=e.length, url=e.url, user_id=e.user_id)
                for e in (m.entities or [])]
            m.mentioned = True
        prev = [x for x in chat.messages if x.action is None]
        if prev and rng.random() < 0.12:
            target = rng.choice(prev[-10:])
            m.reply_to = T.ReplyRef(message_id=target.id, sender_id=target.sender_id, preview=preview(target))
        elif rng.random() < 0.05:
            src = rng.choice(self.people())
            m.forward_from = T.ForwardInfo(name=src.name, sender_id=src.id, at=at - timedelta(days=1))
        if m.direction == "out" and m.text and rng.random() < 0.1:
            m.edited, m.edited_at = True, at + timedelta(minutes=1)
        if rng.random() < 0.15 and kind != "saved":
            keys = rng.sample(EMOJI, rng.randint(1, 3))
            m.reactions = []
            for k in keys:
                count = rng.randint(1, 5)
                mine = rng.random() < 0.25
                m.reactions.append(T.Reaction(key=T.ReactionKey(emoji=k), count=count, mine=mine))
        return m

    def _entities(self, text: str, rng: random.Random, chat: Chat) -> list[T.Entity]:
        words = [(i, w) for i, w in enumerate(text.split(" ")) if w]
        pos, spans = 0, []
        for w in text.split(" "):
            spans.append((pos, w))
            pos += len(w) + 1
        start, word = rng.choice([s for s in spans if s[1]])
        etype = rng.choice(["bold", "italic", "underline", "strike", "spoiler", "code", "text_url", "mention_name"])
        e = T.Entity(type=etype, offset=u16(text[:start]), length=u16(word))
        if etype == "text_url":
            e.url = "https://example.org/" + str(len(words))
        if etype == "mention_name":
            e.user_id = rng.choice([x.user_id for x in chat.members])
        out = [e]
        if "https://" in text:
            k = text.index("https://")
            url = text[k:].split(" ")[0].rstrip(".")
            out.append(T.Entity(type="url", offset=u16(text[:k]), length=u16(url)))
        if "#" in text:
            k = text.index("#")
            tag = text[k:].split(" ")[0]
            out.append(T.Entity(type="hashtag", offset=u16(text[:k]), length=u16(tag)))
        return sorted(out, key=lambda x: (x.offset, x.length))

    def _media(self, cid: str, mid: str, kind: str, rng: random.Random) -> T.Media:
        key = f"{self.seed}/{cid}/{mid}"
        if kind == "photo":
            w, h = rng.choice([(320, 240), (240, 320), (300, 300)])
            return T.Media(kind="photo", mime="image/png", width=w, height=h, filename=f"photo_{mid}.png",
                           has_spoiler=rng.random() < 0.05)
        if kind in ("video", "video_note"):
            return T.Media(kind=kind, mime="video/mp4", width=64, height=64, duration=2, bytes=CLIP_BYTES,
                           filename=f"video_{mid}.mp4")
        if kind == "voice":
            secs = rng.randint(1, 4)
            return T.Media(kind="voice", mime="audio/wav", duration=secs, waveform=_waveform(key), bytes=44 + secs * 8000,
                           filename=f"voice_{mid}.wav")
        if kind == "music":
            return T.Media(kind="music", mime="audio/wav", duration=3, title=f"Track {mid}", bytes=44 + 3 * 8000,
                           performer=rng.choice(["The Fakes", "Seeded Sound"]), filename=f"track_{mid}.wav")
        if kind == "gif":
            return T.Media(kind="gif", mime="image/gif", width=64, height=64, duration=1,
                           filename=f"anim_{mid}.gif")
        doc = rng.choice(["report.pdf", "notes.txt", "dataset.bin"])
        return T.Media(kind="document", mime={"report.pdf": "application/pdf", "notes.txt": "text/plain",
                                              "dataset.bin": "application/octet-stream"}[doc],
                       filename=doc, bytes=BLOB_BYTES if doc == "dataset.bin" else None)

    # ---- folders, stickers --------------------------------------------------------------

    def _folders(self) -> list[T.Folder]:
        work = [c for c in self.order if c not in (SAVED, BOT_CHAT)][:3]
        return [
            T.Folder(id="f1", title="Personal", emoticon="👤", position=0, chat_ids=[],
                     rules=T.FolderRules(include_kinds=["contacts", "non_contacts"], exclude_archived=True)),
            T.Folder(id="f2", title="Groups", emoticon="👥", position=1, chat_ids=[],
                     rules=T.FolderRules(include_kinds=["groups", "channels"])),
            T.Folder(id="f3", title="Work", emoticon="💼", position=2, chat_ids=work),
            T.Folder(id="f4", title="Unread", emoticon="✅", position=3, chat_ids=[],
                     rules=T.FolderRules(include_kinds=["contacts", "non_contacts", "groups", "channels", "bots"],
                                         exclude_read=True)),
        ]

    def folder_ids(self, conv: T.Conversation) -> list[str]:
        out = []
        for f in self.folders:
            if conv.id in (f.chat_ids or []):
                out.append(f.id)
                continue
            r = f.rules
            if r is None or conv.id in (r.exclude_chat_ids or []):
                continue
            if (r.exclude_archived and conv.archived) or (r.exclude_muted and conv.muted):
                continue
            if r.exclude_read and not (conv.unread_count or conv.marked_unread):
                continue
            if self.chat_kind(conv) in (r.include_kinds or []):
                out.append(f.id)
        return out

    def chat_kind(self, conv: T.Conversation) -> str:
        if conv.kind == "group":
            return "groups"
        if conv.kind == "channel":
            return "channels"
        peer = self.user(conv.peer_user_id or "")
        if peer is not None and peer.is_bot:
            return "bots"
        return "contacts" if peer is not None and (peer.is_contact or peer.id == ME) else "non_contacts"

    @staticmethod
    def _sticker_seeds() -> list[tuple[str, str, str, str, str]]:
        out = []
        for i, e in enumerate(["😀", "😎", "🥳", "😴", "🤔", "👋"], 1):
            out.append((f"s{i}", e, "image/png", "p1", "static"))
        for i, e in enumerate(["🐱", "🐶", "🦊", "🐼", "🐸", "🐙"], 7):
            out.append((f"s{i}", e, "application/json", "p2", "animated"))
        return out

    def _stickers(self) -> tuple[list[T.StickerPack], list[T.Sticker]]:
        packs = [T.StickerPack(id="p1", title="Fake Faces", short_name="fake_faces", count=6, kind="stickers",
                               installed=True),
                 T.StickerPack(id="p2", title="Fake Animals", short_name="fake_animals", count=6, kind="stickers",
                               installed=True)]
        stickers = [T.Sticker(id=sid, pack_id=pid, emoji=e, format=fmt, width=512, height=512)
                    for sid, e, _mime, pid, fmt in self._sticker_seeds()]
        return packs, stickers

    def sticker(self, sticker_id: str) -> T.Sticker | None:
        return next((s for s in self.stickers if s.id == sticker_id), None)


def bot_buttons() -> list[list[T.InlineButton]]:
    return [[T.InlineButton(text="Ping", kind="callback", data="ping"),
             T.InlineButton(text="Count", kind="callback", data="count")],
            [T.InlineButton(text="Open example.org", kind="url", url="https://example.org")]]


def preview(m: T.Message) -> str:
    if m.text:
        return m.text[:80]
    if m.media is not None:
        return {"photo": "Photo", "video": "Video", "voice": "Voice message", "video_note": "Video message",
                "music": "Audio", "gif": "GIF", "sticker": f"{(m.media.sticker.emoji if m.media.sticker else '')} Sticker".strip(),
                "document": m.media.filename or "File"}.get(str(m.media.kind), "Media")
    if m.poll is not None:
        return f"📊 {m.poll.question}"
    if m.location is not None:
        return "📍 Location"
    if m.contact_card is not None:
        return f"👤 {m.contact_card.name}"
    if m.dice is not None:
        return m.dice.emoticon
    return "Message"


def n_left(i: int, n: int) -> int:
    return n - i - 1


def _waveform(key: str) -> list[int]:
    from .media import waveform

    return waveform(key)
