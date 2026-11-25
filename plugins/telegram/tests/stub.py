"""Only the transport boundary is replaced. Requests, updates and sessions are real TL types."""
import asyncio
import copy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from telethon import errors, functions, types as tl, utils
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
        self.uploaded, self.stored_media = {}, {}
        self.poll_solutions = {}
        self.unread_reactions = {("17", None): {2, 5, 9}, ("-1000000000008", None): {11},
                                 ("-1000000000008", "42"): {41, 45}, ("18", "7"): {70, 72}}
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
        raw = self.messages.get(ids)
        if self.mode == 6 and raw is not None and isinstance(raw.media, tl.MessageMediaPoll):
            raw = copy.deepcopy(raw)
            raw.media.results = tl.PollResults(min=True, total_voters=raw.media.results.total_voters)
        if self.mode == 7 and raw is not None and raw.reactions is not None:
            raw = copy.deepcopy(raw)
            raw.reactions.min = True
        return raw

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

    async def upload_file(self, file, *, file_size, file_name, progress_callback):
        payload = file.read()
        assert len(payload) == file_size
        handle = tl.InputFile(400 + len(self.uploaded), 1, file_name, "fixture")
        self.uploaded[handle.id] = payload
        await progress_callback(file_size, file_size)
        return handle

    def media_value(self, media):
        if isinstance(media, tl.InputMediaUploadedPhoto):
            value = tl.MessageMediaPhoto(photo=photo(), spoiler=media.spoiler, ttl_seconds=media.ttl_seconds)
        elif isinstance(media, tl.InputMediaUploadedDocument):
            value = tl.MessageMediaDocument(document=document(*media.attributes, mime=media.mime_type,
                                              size=len(self.uploaded[media.file.id])), spoiler=media.spoiler, ttl_seconds=media.ttl_seconds)
        else:
            value = self.stored_media[media.id.id]
            value.spoiler = media.spoiler
        return value

    async def iter_messages(self, peer, *, limit, reply_to=None, offset_id=0, offset_date=None, reverse=False, min_id=0, max_id=0):
        ids = list(range(1, 11))
        if not reverse:
            ids.reverse()
        if offset_date:
            ids = [i for i in ids if row(i).date < offset_date]
        ids = [i for i in ids if i > min_id and (not max_id or i < max_id) and (not offset_id or i < offset_id)]
        for i in ids[:limit]:
            yield row(i)

    def unread_matches(self, req):
        chat = str(utils.get_peer_id(req.peer))
        return sorted((mid, key) for key, mids in self.unread_reactions.items() if key[0] == chat
                      and (req.top_msg_id is None or str(req.top_msg_id) == (key[1] or "1")) for mid in mids)

    async def __call__(self, req):
        if isinstance(req, functions.contacts.GetContactsRequest):
            return tl.contacts.Contacts([tl.Contact(17, True)], 1, [USER])
        if isinstance(req, functions.channels.GetChannelsRequest):
            return tl.messages.Chats([tl.Channel(id=channel.channel_id, title="Forum fixture", photo=tl.ChatPhotoEmpty(), date=NOW,
                                                 megagroup=True, forum=True) for channel in req.id])
        if isinstance(req, functions.users.GetUsersRequest):
            return [tl.User(user.user_id, first_name="Forum bot" if user.user_id == 18 else "Sample",
                     bot=True if user.user_id == 18 else None, bot_info_version=1 if user.user_id == 18 else None,
                     bot_forum_view=True if user.user_id == 18 else None) for user in req.id]
        if isinstance(req, functions.messages.GetUnreadReactionsRequest):
            assert (req.offset_id, req.add_offset, req.max_id, req.min_id) == (1, -req.limit, 0, 0) and req.saved_peer_id is None
            matches = self.unread_matches(req)
            peer = tl.PeerUser(req.peer.user_id) if isinstance(req.peer, tl.InputPeerUser) else tl.PeerChannel(req.peer.channel_id)
            rows = [row(mid, peer, reply_to=tl.MessageReplyHeader(reply_to_msg_id=int(key[1]), forum_topic=True) if key[1] else None)
                    for mid, key in reversed(matches[:req.limit])]
            return tl.messages.MessagesSlice(len(matches), rows, [], [], [])
        if isinstance(req, functions.messages.ReadReactionsRequest):
            assert req.saved_peer_id is None
            if self.mode == 8:
                raise OSError("synthetic-private-unread-error")
            if self.mode == 9:
                await asyncio.Future()  # A dispatched write with no outcome yet.
            for mid, key in self.unread_matches(req)[:2]:
                self.unread_reactions[key].remove(mid)
            return tl.messages.AffectedHistory(1, 1, len(self.unread_matches(req)))
        if isinstance(req, functions.help.GetAppConfigRequest):
            return tl.help.AppConfig(1, tl.JsonObject([tl.JsonObjectValue("reactions_user_max_default", tl.JsonNumber(1))]))
        if isinstance(req, functions.messages.GetAvailableReactionsRequest):
            def available(emoji, **flags):
                doc = tl.DocumentEmpty(1)
                return tl.AvailableReaction(emoji, "Fixture", doc, doc, doc, doc, doc, **flags)
            return tl.messages.AvailableReactions(1, [available("❤️"), available("👍"),
                                                       available("inactive", inactive=True), available("premium", premium=True)])
        if isinstance(req, functions.messages.SendReactionRequest):
            if any(isinstance(key, tl.ReactionEmoji) and key.emoticon == "explode-reaction" for key in req.reaction):
                raise OSError("synthetic-private-reaction-error")
            raw = self.messages[req.msg_id]
            raw.reactions = tl.MessageReactions([tl.ReactionCount(key, 1, chosen_order=index)
                                                for index, key in enumerate(req.reaction)], recent_reactions=[])
            return tl.Updates([tl.UpdateMessageReactions(raw.peer_id, raw.id, raw.reactions)], [], [], NOW, 1)
        if isinstance(req, functions.messages.GetMessagesReactionsRequest):
            return tl.Updates([tl.UpdateMessageReactions(self.messages[mid].peer_id, mid, self.messages[mid].reactions)
                               for mid in req.id], [], [], NOW, 1)
        if isinstance(req, functions.messages.GetMessageReactionsListRequest):
            if req.offset is None:
                return tl.messages.MessageReactionsList(2, [tl.MessagePeerReaction(tl.PeerUser(17), NOW, tl.ReactionEmoji("❤️"), unread=True)],
                         [], [USER], next_offset="fixture:next")
            assert req.offset == "fixture:next"
            return tl.messages.MessageReactionsList(2, [], [], [])
        if isinstance(req, functions.messages.SendMediaRequest) and isinstance(req.media, tl.InputMediaPoll):
            if req.media.poll.question.text == "explode-poll":
                raise OSError("synthetic-private-poll-error")
            mid = 200 + len(self.messages)
            poll = copy.deepcopy(req.media.poll)
            poll.id, poll.hash = 700 + mid, 900 + mid
            if poll.close_period is not None:
                poll.close_date, poll.close_period = NOW + timedelta(seconds=poll.close_period), None
            result = tl.PollResults(results=[tl.PollAnswerVoters(a.option, voters=0, recent_voters=[]) for a in poll.answers], total_voters=0)
            self.poll_solutions[poll.id] = (req.media.correct_answers, req.media.solution, req.media.solution_entities)
            peer = tl.PeerUser(req.peer.user_id) if isinstance(req.peer, tl.InputPeerUser) else tl.PeerChannel(req.peer.channel_id)
            header = tl.MessageReplyHeader(reply_to_msg_id=req.reply_to.reply_to_msg_id, reply_to_top_id=req.reply_to.top_msg_id,
                                           forum_topic=True) if req.reply_to else None
            item = row(mid, peer, out=True, media=tl.MessageMediaPoll(poll, result), reply_to=header, silent=req.silent)
            item.message = ""
            item.date = NOW
            self.messages[mid] = item
            return tl.Updates([tl.UpdateMessageID(mid, req.random_id), tl.UpdateNewMessage(item, 1, 1)], [], [], NOW, 1)
        if isinstance(req, functions.messages.SendVoteRequest):
            raw = self.messages[req.msg_id]
            poll = raw.media.poll
            correct, solution, entities = self.poll_solutions.get(poll.id, (None, None, None))
            raw.media.results = tl.PollResults(results=[tl.PollAnswerVoters(a.option,
                chosen=True if a.option in req.options else None, voters=int(a.option in req.options), recent_voters=[],
                correct=True if correct is not None and index in correct else None) for index, a in enumerate(poll.answers)],
                total_voters=int(bool(req.options)), solution=solution, solution_entities=entities)
            return tl.Updates([tl.UpdateMessagePoll(poll.id, raw.media.results, poll=poll, peer=raw.peer_id, msg_id=raw.id)], [], [], NOW, 1)
        if isinstance(req, functions.messages.GetPollResultsRequest):
            raw = self.messages[req.msg_id]
            return tl.Updates([tl.UpdateMessagePoll(raw.media.poll.id, raw.media.results, poll=raw.media.poll,
                                                  peer=raw.peer_id, msg_id=raw.id)], [], [], NOW, 1)
        if isinstance(req, functions.messages.EditMessageRequest) and isinstance(req.media, tl.InputMediaPoll):
            raw = self.messages[req.id]
            assert req.media.poll.id == raw.media.poll.id and req.media.poll.closed
            raw.media.poll = copy.deepcopy(req.media.poll)
            return tl.Updates([tl.UpdateMessagePoll(raw.media.poll.id, raw.media.results, poll=raw.media.poll,
                                                  peer=raw.peer_id, msg_id=raw.id)], [], [], NOW, 1)
        if isinstance(req, functions.messages.UploadMediaRequest):
            value = self.media_value(req.media)
            attachment = value.photo if isinstance(value, tl.MessageMediaPhoto) else value.document
            # Give each upload a distinct stable media ID.
            attachment.id = 500 + len(self.stored_media)
            self.stored_media[attachment.id] = value
            return value
        if isinstance(req, (functions.messages.SendMediaRequest, functions.messages.SendMultiMediaRequest)):
            inputs = req.multi_media if isinstance(req, functions.messages.SendMultiMediaRequest) else [req]
            updates = []
            for entry in inputs:
                mid = 200 + len(self.messages)
                value = self.media_value(entry.media)
                header = tl.MessageReplyHeader(reply_to_msg_id=req.reply_to.reply_to_msg_id,
                              reply_to_top_id=req.reply_to.top_msg_id, forum_topic=True) if req.reply_to else None
                peer = tl.PeerUser(req.peer.user_id) if isinstance(req.peer, tl.InputPeerUser) else tl.PeerChannel(req.peer.channel_id)
                item = row(mid, peer, out=True, media=value, entities=entry.entities, reply_to=header, silent=req.silent,
                           grouped_id=123 if len(inputs) > 1 else None)
                item.message = entry.message
                self.messages[mid] = item
                updates.extend([tl.UpdateMessageID(mid, entry.random_id), tl.UpdateNewMessage(item, 1, 1)])
            return tl.Updates(updates, [], [], NOW, 1)
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
