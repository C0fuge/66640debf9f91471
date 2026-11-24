"""Only the transport boundary is replaced. Requests, updates and sessions are real TL types."""
import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from telethon import errors, functions, types as tl
from telethon.sessions import SQLiteSession

NOW = datetime(2025, 1, 1, tzinfo=UTC)
PEER = tl.PeerUser(17)
USER = tl.User(17, first_name="Sample", contact=True)


def row(i, peer=PEER, **kwargs):
    return tl.Message(id=i, peer_id=peer, date=NOW + timedelta(seconds=i), message=f"Row {i}", **kwargs)


def document(*attributes, mime="application/octet-stream", size=7):
    return tl.Document(200, 0, b"fixture", NOW, mime, size, 1, list(attributes),
                       thumbs=[tl.PhotoSize("s", 4, 3, 3)])


def photo():
    return tl.Photo(201, 0, b"fixture", NOW, [tl.PhotoSize("s", 4, 3, 3), tl.PhotoSizeProgressive("x", 40, 30, [4, 7])], 1)


class Stub:
    def __init__(self, path, api_id, api_hash, proxy):
        self.session = SQLiteSession(str(path))
        self.authorized = api_id != 2
        self.mode = api_id
        self.open = False
        self.attempt = 0
        self.handlers = []
        self.sent = {}
        self.messages = {i: row(i) for i in range(1, 11)}
        self.messages[100] = row(100, media=tl.MessageMediaDocument(document=document(tl.DocumentAttributeFilename("../../remote.bin"))))
        self.messages[101] = row(101, media=tl.MessageMediaPhoto(photo=photo(), spoiler=True))

    async def connect(self):
        self.attempt += 1
        if self.mode == 3 and self.attempt == 1:
            raise ConnectionError("synthetic-private-error")
        self.open = True
        self._lost = asyncio.get_running_loop().create_future()

    @property
    def disconnected(self):
        return asyncio.shield(self._lost)

    def is_connected(self):
        return self.open

    async def disconnect(self):
        self.open = False
        if hasattr(self, "_lost") and not self._lost.done():
            self._lost.set_result(None)
        self.session.close()

    async def get_me(self):
        return tl.User(100, first_name="Owner", is_self=True) if self.authorized else None

    async def send_code_request(self, phone):
        if phone != "+19995550000":
            raise errors.PhoneNumberInvalidError(None)
        return SimpleNamespace(phone_code_hash="synthetic-hash")

    async def sign_in(self, **kwargs):
        if "code" in kwargs:
            if kwargs["code"] != "55881":
                raise errors.PhoneCodeInvalidError(None)
            raise errors.SessionPasswordNeededError(None)
        if kwargs["password"] != "unique-fixture-password":
            raise errors.PasswordHashInvalidError(None)
        self.authorized = True

    async def qr_login(self):
        async def wait():
            await asyncio.sleep(.015)
            raise errors.SessionPasswordNeededError(None)
        return SimpleNamespace(wait=wait, expires=NOW, url="tg://login?token=synthetic-qr")

    def add_event_handler(self, handler, event):
        self.handlers.append((handler, event))

    async def iter_dialogs(self, folder=None):
        if self.mode == 4:
            raise RuntimeError("synthetic-private-error")
        raw = SimpleNamespace(read_inbox_max_id=2, read_outbox_max_id=4, unread_mark=False, draft=None)
        yield SimpleNamespace(id=17, dialog=raw, entity=USER, title="Sample", message=row(10), date=NOW,
                              is_user=True, is_group=False, archived=False, pinned=False, unread_count=0,
                              unread_mentions_count=0, unread_reactions_count=0)
        if self.mode == 5 and self.attempt == 1:
            asyncio.get_running_loop().call_later(.15, self._lost.set_result, None)

    async def get_input_entity(self, chat):
        if chat in (tl.PeerUser(17), tl.PeerUser(18)):
            return tl.InputPeerUser(chat.user_id, 0)
        if chat == tl.PeerChannel(8):
            return tl.InputPeerChannel(8, 0)
        raise ValueError("synthetic-private-error")

    async def get_messages(self, peer, *, ids):
        # Simulate Telegram's account-wide private-chat lookup.
        if ids == 99:
            return row(99, tl.PeerUser(18))
        return self.messages.get(ids)

    async def download_media(self, message, *, file, thumb, progress_callback):
        content = b"img" if thumb is not None and getattr(thumb, "type", None) == "s" else b"fixture"
        file.write(content)
        await progress_callback(len(content), len(content))
        return file

    async def download_profile_photo(self, peer, *, file, download_big):
        if getattr(peer, "user_id", None) == 18:
            return None
        file.write(b"avatar")
        return file

    async def iter_messages(self, peer, *, limit, reply_to=None, offset_id=0, offset_date=None, reverse=False, min_id=0, max_id=0):
        ids = list(range(1, 11))
        if not reverse:
            ids.reverse()
        if offset_date:
            ids = [i for i in ids if row(i).date < offset_date]
        ids = [i for i in ids if i > min_id and (not max_id or i < max_id) and (not offset_id or i < offset_id)]
        for i in ids[:limit]:
            yield row(i)

    async def __call__(self, req):
        if isinstance(req, functions.contacts.GetContactsRequest):
            return tl.contacts.Contacts([tl.Contact(17, True)], 1, [USER])
        if isinstance(req, functions.messages.EditMessageRequest):
            item = self.messages[req.id]
            if item.message == req.message and item.entities == req.entities:
                raise errors.MessageNotModifiedError(None)
            item.message, item.entities, item.edit_date = req.message, req.entities, NOW
            return tl.Updates([tl.UpdateEditMessage(item, 1, 1)], [], [], NOW, 1)
        if isinstance(req, functions.messages.DeleteMessagesRequest):
            for mid in req.id:
                self.messages.pop(mid, None)
            return tl.messages.AffectedMessages(1, 1)
        if isinstance(req, functions.messages.ForwardMessagesRequest):
            updates = []
            for mid, random in zip(req.id, req.random_id):
                new_id = 20 + len(self.messages)
                item = row(new_id, tl.PeerUser(req.to_peer.user_id), out=True,
                           fwd_from=None if req.drop_author else tl.MessageFwdHeader(NOW, from_id=PEER))
                self.messages[new_id] = item
                updates.extend([tl.UpdateMessageID(new_id, random), tl.UpdateNewMessage(item, 1, 1)])
            return tl.Updates(updates, [], [], NOW, 1)
        if isinstance(req, functions.messages.UpdatePinnedMessageRequest):
            self.messages[req.id].pinned = not req.unpin
            return tl.Updates([], [], [], NOW, 1)
        if isinstance(req, functions.messages.UnpinAllMessagesRequest):
            return tl.messages.AffectedHistory(1, 1, 0)
        if isinstance(req, (functions.messages.ReadHistoryRequest, functions.channels.ReadHistoryRequest,
                            functions.messages.ReadDiscussionRequest, functions.messages.SetTypingRequest,
                            functions.messages.SaveDraftRequest)):
            return True
        assert isinstance(req, functions.messages.SendMessageRequest)
        if req.message == "wait":
            raise errors.FloodWaitError(None, capture=23)
        if req.message == "explode":
            raise OSError("synthetic-private-error")
        msg_id = self.sent.setdefault(req.random_id, 11 + len(self.sent))
        return tl.UpdateShortSentMessage(msg_id, 1, 1, NOW, out=True, entities=req.entities)
