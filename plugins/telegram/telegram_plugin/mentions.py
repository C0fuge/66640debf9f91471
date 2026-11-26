"""Bounded unread-mention observation and explicit same-scope read-all."""
import asyncio
from contextlib import contextmanager

from contact_hub_sdk import errors as rpc, types as dto
from telethon import functions, types as tl, utils

from .mapping import identifier, message, resolve_chat

MAX_READ_ROUNDS = 128
MAX_OPERATIONS = 4


def unknown_read():
    return rpc.NetworkError("Reading mentions may have partially applied; the outcome is unknown.",
                            outcome="unknown", retry_safe=False)


@contextmanager
def admission(account):
    state = account.data.setdefault("mentions_work", {"active": 0, "lock": asyncio.Lock()})
    # Admission is synchronous on the SDK event loop; rejected calls never join
    # the lock's waiter queue. The four admitted operations include queued work.
    if state["active"] >= MAX_OPERATIONS:
        raise rpc.Unavailable("Too many mention operations are active.")
    state["active"] += 1
    try:
        yield state["lock"]
    finally:
        state["active"] -= 1


def live(account):
    if not account.running or account.state != "connected":
        raise rpc.Unavailable("This Telegram account is unavailable.")


def writable(account):
    live(account)
    if account.ghost:
        raise rpc.NotSupported("Reading mentions is disabled in ghost mode.", capability="mentions.unread")


async def scope(client, p):
    topic = identifier(p.topic_id, "topic_id") if p.topic_id is not None else None
    peer = await resolve_chat(client, p.chat_id)
    if isinstance(peer, tl.InputPeerChat):
        result = await client(functions.messages.GetChatsRequest(id=[peer.chat_id]))
        expected, cls = peer.chat_id, tl.Chat
    elif isinstance(peer, tl.InputPeerChannel):
        result = await client(functions.channels.GetChannelsRequest(id=[utils.get_input_channel(peer)]))
        expected, cls = peer.channel_id, tl.Channel
    else:
        raise rpc.NotSupported("Unread mentions are supported for ordinary groups.", capability="mentions.unread")
    if not isinstance(result, (tl.messages.Chats, tl.messages.ChatsSlice)) or not isinstance(result.chats, list):
        raise rpc.Internal("Telegram did not return the mention group.")
    if len(result.chats) != 1 or not isinstance(result.chats[0], cls) or result.chats[0].id != expected:
        raise rpc.Internal("Telegram did not return a matching full mention group.")
    entity = result.chats[0]
    if getattr(entity, "min", False):
        raise rpc.Internal("Telegram returned a partial mention group.")
    if isinstance(entity, tl.Channel) and (entity.monoforum or not entity.megagroup):
        raise rpc.NotSupported("This Telegram channel does not support ordinary mentions.", capability="mentions.unread")
    if isinstance(entity, tl.Chat) and (entity.deactivated or entity.migrated_to is not None):
        raise rpc.NotSupported("This Telegram group is no longer an ordinary mention group.", capability="mentions.unread")
    if topic is not None and (not isinstance(entity, tl.Channel) or not entity.forum):
        raise rpc.InvalidParams("This group does not have forum topics.", field="topic_id")
    return peer, topic


async def list_unread(account, client, p):
    if type(p.limit) is not int or not 1 <= p.limit <= 200:
        raise rpc.InvalidParams(field="limit")
    with admission(account):
        live(account)
        peer, topic = await scope(client, p)
        live(account)
        result = await client(functions.messages.GetUnreadMentionsRequest(peer=peer, top_msg_id=topic,
            offset_id=1, add_offset=-p.limit, limit=p.limit, max_id=0, min_id=0))
        live(account)
        if not isinstance(result, (tl.messages.Messages, tl.messages.MessagesSlice, tl.messages.ChannelMessages)):
            raise rpc.Internal("Telegram did not return an unread mention batch.")
        if not isinstance(result.messages, list) or len(result.messages) > p.limit:
            raise rpc.Internal("Telegram returned an invalid unread mention batch.")
        selected = set()
        for raw in result.messages:
            item = message(raw)
            if item is None or item.chat_id != p.chat_id:
                raise rpc.Internal("Telegram returned mentions for an unavailable or different message scope.")
            if raw.out is not False or raw.mentioned is not True or raw.media_unread is not True:
                raise rpc.Internal("Telegram returned a message without an unread incoming mention.")
            if topic is not None and (item.topic_id or "1") != str(topic):
                raise rpc.Internal("Telegram returned mentions for a different topic.")
            try:
                mid = identifier(item.id, "message_id")
            except rpc.InvalidParams:
                raise rpc.Internal("Telegram returned an invalid unread mention ID.") from None
            if mid in selected:
                raise rpc.Internal("Telegram returned duplicate unread mention messages.")
            selected.add(mid)
        return dto.MessageIdsResult(message_ids=[str(mid) for mid in sorted(selected)])


async def read(account, client, p):
    with admission(account) as lock:
        async with lock:
            writable(account)
            peer, topic = await scope(client, p)
            writable(account)
            request = functions.messages.ReadMentionsRequest(peer=peer, top_msg_id=topic)
            remaining = None
            dispatched = False
            # Everything before this try is read-only preflight. Once a request
            # reaches Telethon, cancellation and any failure are conservative.
            try:
                for _ in range(MAX_READ_ROUNDS):
                    writable(account)
                    dispatched = True
                    result = await client(request)
                    live(account)
                    if not isinstance(result, tl.messages.AffectedHistory) or type(result.offset) is not int or result.offset < 0:
                        raise unknown_read()
                    if result.offset == 0:
                        return
                    if remaining is not None and result.offset >= remaining:
                        raise unknown_read()
                    remaining = result.offset
                    await asyncio.sleep(0)
                raise unknown_read()
            except (Exception, asyncio.CancelledError):
                if not dispatched:
                    raise
                raise unknown_read() from None
