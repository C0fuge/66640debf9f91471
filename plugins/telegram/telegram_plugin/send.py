import hashlib
import json

from contact_hub_sdk import errors as rpc
from contact_hub_sdk import types as dto
from telethon import functions, types as tl

from .mapping import entities_from_tl, entities_to_tl, identifier, message, resolve_chat


def random_id(p):
    if p.client_id is None:
        return None  # Telethon generates a random signed long.
    key = json.dumps([p.account_id, p.chat_id, p.client_id], separators=(",", ":")).encode()
    return int.from_bytes(hashlib.blake2b(key, digest_size=8).digest(), "big", signed=True) or 1


async def send(client, p):
    peer = await resolve_chat(client, p.chat_id)
    if not p.text or len(p.text) > 4096:
        raise rpc.InvalidParams("Telegram text must contain 1 to 4096 characters.", field="text")
    topic = identifier(p.topic_id, "topic_id") if p.topic_id is not None else None
    reply = identifier(p.reply_to.message_id, "reply_to.message_id") if p.reply_to else topic
    header = tl.InputReplyToMessage(reply_to_msg_id=reply, top_msg_id=topic) if reply else None
    request = functions.messages.SendMessageRequest(peer=peer, message=p.text, entities=entities_to_tl(p.text, p.entities),
                                                  reply_to=header, random_id=random_id(p), silent=p.silent,
                                                  no_webpage=p.no_webpage)
    response = await client(request)
    if isinstance(response, tl.UpdateShortSentMessage):
        item = dto.Message(id=str(response.id), chat_id=p.chat_id, sent_at=response.date, direction="out",
                           text=p.text, entities=entities_from_tl(response.entities) if response.entities is not None else p.entities,
                           topic_id=p.topic_id, reply_to=dto.ReplyRef(message_id=p.reply_to.message_id) if p.reply_to else None)
    else:
        updates = getattr(response, "updates", [])
        sent_id = next((u.id for u in updates if isinstance(u, tl.UpdateMessageID) and u.random_id == request.random_id), None)
        item = next((m for u in updates if isinstance(u, (tl.UpdateNewMessage, tl.UpdateNewChannelMessage))
                     and u.message.id == sent_id and (m := message(u.message)) and m.chat_id == p.chat_id), None)
        if item is None and sent_id:
            item = message(await client.get_messages(peer, ids=sent_id))
        if item is None or item.chat_id != p.chat_id:
            raise rpc.Internal("Telegram accepted the request without a matching message confirmation.")
    if p.client_id:
        item.ext = {"telegram": {"client_id": p.client_id}}
    return dto.MessageResult(message=item)
