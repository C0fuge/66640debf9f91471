"""Chat-scoped message operations using the pinned MTProto layer."""
from contact_hub_sdk import errors as rpc, types as dto
from telethon import errors as tg, functions, types as tl, utils

from .mapping import entities_to_tl, identifier, message, resolve_chat


async def fetch(client, peer, chat_id, message_id):
    raw = await client.get_messages(peer, ids=message_id)
    item = message(raw)
    # Private/basic-group message IDs are account-wide, and Telegram's
    # messages.getMessages ignores the requested peer. Never mutate another chat.
    if item is not None and (item.chat_id != chat_id or item.id != str(message_id)):
        raise rpc.NotFound(kind="message")
    return raw, item


async def get(client, p):
    peer = await resolve_chat(client, p.chat_id)
    _, item = await fetch(client, peer, p.chat_id, identifier(p.message_id, "message_id"))
    return dto.OptionalMessageResult(message=item)


async def edit(client, p):
    peer = await resolve_chat(client, p.chat_id)
    mid = identifier(p.message_id, "message_id")
    raw, current = await fetch(client, peer, p.chat_id, mid)
    if current is None:
        raise rpc.NotFound(kind="message")
    if p.text is None and p.entities is None and p.no_webpage is None:
        raise rpc.InvalidParams("No edit was supplied.")
    text = p.text if p.text is not None else current.text
    if text is None or len(text) > 4096:
        raise rpc.InvalidParams(field="text")
    entities = (entities_to_tl(text, p.entities) if p.entities is not None or p.text is not None
                else getattr(raw, "entities", None))
    try:
        response = await client(functions.messages.EditMessageRequest(peer=peer, id=mid, message=text,
                                entities=entities, no_webpage=p.no_webpage))
    except tg.MessageNotModifiedError:
        # Retried edits are successful when Telegram says the value is unchanged.
        _, item = await fetch(client, peer, p.chat_id, mid)
    else:
        item = next((mapped for update in getattr(response, "updates", [])
                     if isinstance(update, (tl.UpdateEditMessage, tl.UpdateEditChannelMessage))
                     and (mapped := message(update.message)) and mapped.id == p.message_id
                     and mapped.chat_id == p.chat_id), None)
        if item is None:
            _, item = await fetch(client, peer, p.chat_id, mid)
    if item is None:
        raise rpc.Internal("Telegram did not return the edited message.")
    return dto.MessageResult(message=item)


def ids(values):
    if not values or len(values) > 100:
        raise rpc.InvalidParams("Select between 1 and 100 messages.", field="message_ids")
    result = [identifier(value, "message_ids") for value in values]
    if len(set(result)) != len(result):
        raise rpc.InvalidParams("Message IDs must be unique.", field="message_ids")
    return result


async def delete(client, p):
    peer = await resolve_chat(client, p.chat_id)
    selected = ids(p.message_ids)
    if isinstance(peer, tl.InputPeerChannel):
        if not p.revoke:
            raise rpc.NotSupported("Telegram channels do not support deletion only for me.", capability="delete.for_me")
        await client(functions.channels.DeleteMessagesRequest(channel=utils.get_input_channel(peer), id=selected))
    else:
        # Validate the entire batch before its first destructive request.
        present = []
        for mid in selected:
            _, item = await fetch(client, peer, p.chat_id, mid)
            if item is not None:
                present.append(mid)
        if present:
            await client(functions.messages.DeleteMessagesRequest(id=present, revoke=p.revoke))


async def forward(client, p):
    source = await resolve_chat(client, p.from_chat_id, field="from_chat_id")
    target = await resolve_chat(client, p.to_chat_id, field="to_chat_id")
    selected = ids(p.message_ids)
    if p.drop_captions and not p.drop_author:
        raise rpc.InvalidParams("Dropping captions requires dropping the author.", field="drop_captions")
    # getMessages for private chats is account-wide. Validate before forwarding.
    for mid in selected:
        _, item = await fetch(client, source, p.from_chat_id, mid)
        if item is None:
            raise rpc.NotFound(kind="message")
    request = functions.messages.ForwardMessagesRequest(from_peer=source, to_peer=target, id=selected,
                top_msg_id=identifier(p.topic_id, "topic_id") if p.topic_id is not None else None,
                drop_author=p.drop_author, drop_media_captions=p.drop_captions, silent=p.silent)
    response = await client(request)
    updates = getattr(response, "updates", [])
    correlated = {u.random_id: u.id for u in updates if isinstance(u, tl.UpdateMessageID)}
    confirmed = {}
    for update in updates:
        if isinstance(update, (tl.UpdateNewMessage, tl.UpdateNewChannelMessage)):
            item = message(update.message)
            if item is not None and item.chat_id == p.to_chat_id:
                confirmed[int(item.id)] = item
    result = []
    for random_id in request.random_id:
        mid = correlated.get(random_id)
        if mid is None:
            raise rpc.Internal("Telegram returned an incomplete forwarding confirmation.")
        item = confirmed.get(mid)
        if item is None:
            _, item = await fetch(client, target, p.to_chat_id, mid)
        if item is None:
            raise rpc.Internal("Telegram did not return the forwarded message.")
        result.append(item)
    return dto.MessagesResult(messages=result)


async def pin(client, p):
    peer = await resolve_chat(client, p.chat_id)
    mid = identifier(p.message_id, "message_id")
    if p.for_me_only and not isinstance(peer, tl.InputPeerUser):
        raise rpc.InvalidParams("Only private chats support a personal pin.", field="for_me_only")
    _, item = await fetch(client, peer, p.chat_id, mid)
    if item is None:
        raise rpc.NotFound(kind="message")
    try:
        await client(functions.messages.UpdatePinnedMessageRequest(peer=peer, id=mid,
                    silent=p.silent, pm_oneside=p.for_me_only))
    except tg.ChatNotModifiedError:
        pass


async def unpin(client, p):
    peer = await resolve_chat(client, p.chat_id)
    if p.message_id is not None:
        mid = identifier(p.message_id, "message_id")
        _, item = await fetch(client, peer, p.chat_id, mid)
        if item is None:
            return
        try:
            await client(functions.messages.UpdatePinnedMessageRequest(peer=peer, id=mid, unpin=True))
        except tg.ChatNotModifiedError:
            pass
    else:
        # affectedHistory.offset is nonzero while more pins remain to be removed.
        while True:
            try:
                response = await client(functions.messages.UnpinAllMessagesRequest(peer=peer))
            except tg.ChatNotModifiedError:
                break
            if not response.offset:
                break
