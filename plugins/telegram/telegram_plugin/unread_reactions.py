"""Bounded oldest unread IDs and acknowledged read-all, using locked layer 233."""
import asyncio

from contact_hub_sdk import errors as rpc, types as dto
from telethon import functions, types as tl, utils

from .mapping import identifier, message, peer_id, resolve_chat

MAX_READ_ROUNDS = 128


def unknown_read():
    return rpc.NetworkError("Reading reactions may have partially applied; the outcome is unknown.",
                            outcome="unknown", retry_safe=False)


async def scope(client, p):
    topic = identifier(p.topic_id, "topic_id") if p.topic_id is not None else None
    peer = await resolve_chat(client, p.chat_id)
    if topic is None:
        return peer, None
    if isinstance(peer, tl.InputPeerChannel):
        response = await client(functions.channels.GetChannelsRequest(id=[utils.get_input_channel(peer)]))
        if not isinstance(response, (tl.messages.Chats, tl.messages.ChatsSlice)):
            raise rpc.Internal("Telegram did not return the forum peer.")
        entities = response.chats
        kind, expected = tl.Channel, peer.channel_id
    elif isinstance(peer, tl.InputPeerUser):
        entities = await client(functions.users.GetUsersRequest(id=[utils.get_input_user(peer)]))
        kind, expected = tl.User, peer.user_id
    else:
        raise rpc.InvalidParams("This chat does not have forum topics.", field="topic_id")
    if not isinstance(entities, list) or len(entities) != 1 or not isinstance(entities[0], kind) or entities[0].id != expected or entities[0].min:
        raise rpc.Internal("Telegram did not return a matching full forum peer.")
    entity = entities[0]
    if isinstance(entity, tl.Channel) and entity.monoforum:
        raise rpc.NotSupported("Monoforum peer scopes cannot be used as forum topic IDs.", capability="reactions.unread")
    forum = entity.forum if isinstance(entity, tl.Channel) else entity.bot and entity.bot_forum_view
    if not forum:
        raise rpc.InvalidParams("This chat does not have forum topics.", field="topic_id")
    return peer, topic


async def list_unread(client, p):
    if type(p.limit) is not int or not 1 <= p.limit <= 200:
        raise rpc.InvalidParams(field="limit")
    peer, topic = await scope(client, p)
    # The official Desktop's initial oldest batch uses this exact offset pair.
    # This frozen method has no cursor/exhaustion fields: never invent either.
    response = await client(functions.messages.GetUnreadReactionsRequest(peer=peer, top_msg_id=topic,
                    offset_id=1, add_offset=-p.limit, limit=p.limit, max_id=0, min_id=0))
    if not isinstance(response, (tl.messages.Messages, tl.messages.MessagesSlice, tl.messages.ChannelMessages)):
        raise rpc.Internal("Telegram did not return an unread reaction batch.")
    if not isinstance(response.messages, list) or len(response.messages) > p.limit:
        raise rpc.Internal("Telegram returned an invalid unread reaction batch.")
    selected = []
    for raw in response.messages:
        item = message(raw)
        if item is None or item.chat_id != p.chat_id:
            raise rpc.Internal("Telegram returned unread reactions for an unavailable or different message scope.")
        # General is id=1 and may have no topic header. Only a verified forum
        # peer plus the explicit scoped request makes that fallback meaningful.
        if topic is not None and (item.topic_id or "1") != str(topic):
            raise rpc.Internal("Telegram returned unread reactions for a different topic.")
        try:
            mid = identifier(item.id, "message_id")
        except rpc.InvalidParams:
            raise rpc.Internal("Telegram returned an invalid unread message ID.") from None
        if mid in selected:
            raise rpc.Internal("Telegram returned duplicate unread reaction messages.")
        selected.append(mid)
    return dto.MessageIdsResult(message_ids=[str(mid) for mid in sorted(selected)])


async def read(account, client, p):
    async with account.data.setdefault("reaction_lock", asyncio.Lock()):
        peer, topic = await scope(client, p)
        remaining = None
        request = functions.messages.ReadReactionsRequest(peer=peer, top_msg_id=topic)
        # Scope validation/lookup above is read-only. Once we hand this write to
        # the client, even cancellation or a malformed reply cannot prove that
        # nothing applied. Preserve that fact through the SDK's error boundary.
        try:
            for _ in range(MAX_READ_ROUNDS):
                result = await client(request)
                if not isinstance(result, tl.messages.AffectedHistory) or type(result.offset) is not int or result.offset < 0:
                    raise unknown_read()
                if result.offset == 0:
                    return
                if remaining is not None and result.offset >= remaining:
                    raise unknown_read()
                remaining = result.offset
                # A positive offset explicitly acknowledges continuation, not
                # a retry after an unknown result. Always yield for cancellation.
                await asyncio.sleep(0)
            raise unknown_read()  # Still positive at the finite request budget.
        except (Exception, asyncio.CancelledError):
            raise unknown_read() from None
