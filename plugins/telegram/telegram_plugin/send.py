import asyncio
import hashlib
import json

from contact_hub_sdk import METHODS
from contact_hub_sdk import errors as rpc
from contact_hub_sdk import types as dto
from telethon import errors as tg, functions, types as tl, utils

from .mapping import entities_from_tl, entities_to_tl, identifier, message, resolve_chat


def random_id(p):
    if p.client_id is None:
        return None  # Telethon generates a random signed long.
    key = json.dumps([p.account_id, p.chat_id, p.client_id], separators=(",", ":")).encode()
    return int.from_bytes(hashlib.blake2b(key, digest_size=8).digest(), "big", signed=True) or 1


def bare_reply(p):
    return p.reply_to is not None and p.reply_to.quote_text is None and p.reply_to.quote_offset is None


def unknown_reply():
    return rpc.NetworkError("Telegram could not confirm the sent reply; the outcome is unknown.",
                            outcome="unknown", retry_safe=False)


def sent_id(value):
    if type(value) is not int or not 0 < value < 2**31:
        raise unknown_reply()
    return value


def observed_reply(raw, p, mid):
    if (not isinstance(raw, tl.Message) or type(raw.id) is not int or raw.id != mid
            or raw.out is not True or utils.get_peer_id(raw.peer_id) != int(p.chat_id)
            or getattr(raw, "saved_peer_id", None) is not None):
        raise unknown_reply()
    header = raw.reply_to
    if (not isinstance(header, tl.MessageReplyHeader)
            or type(header.reply_to_msg_id) is not int or str(header.reply_to_msg_id) != p.reply_to.message_id
            or header.reply_to_scheduled or header.reply_to_ephemeral
            or (header.reply_to_peer_id is not None and utils.get_peer_id(header.reply_to_peer_id) != int(p.chat_id))):
        raise unknown_reply()
    item = message(raw)
    # None is a main/default scope, never a wildcard. Unproven General None/1
    # normalization remains unknown instead of copying the requested topic.
    if item is None or item.topic_id != p.topic_id:
        raise unknown_reply()
    return item


async def send_reply(client, peer, request, p):
    # One send and at most one exact-ID read share the normal method deadline.
    async with asyncio.timeout(METHODS["messages.send"].timeout_seconds):
        try:
            response = await client(request)
        except tg.ServerError:
            raise unknown_reply() from None
        except tg.RPCError:
            raise  # Definite refusal of the send itself retains its typed meaning.
        except (Exception, asyncio.CancelledError):
            raise unknown_reply() from None
        try:
            raw = None
            if isinstance(response, tl.UpdateShortSentMessage):
                if response.out is not True:
                    raise unknown_reply()
                mid = sent_id(response.id)
            elif isinstance(response, (tl.Updates, tl.UpdatesCombined)) and isinstance(response.updates, list):
                ids = [sent_id(update.id) for update in response.updates
                       if isinstance(update, tl.UpdateMessageID) and update.random_id == request.random_id]
                if len(ids) != 1:
                    raise unknown_reply()
                mid = ids[0]
                matches = [update.message for update in response.updates
                           if isinstance(update, (tl.UpdateNewMessage, tl.UpdateNewChannelMessage))
                           and update.message.id == mid and getattr(update.message, "peer_id", None) is not None
                           and utils.get_peer_id(update.message.peer_id) == int(p.chat_id)]
                if len(matches) > 1:
                    raise unknown_reply()
                raw = matches[0] if matches else None
            else:
                raise unknown_reply()
            if raw is None:
                raw = await client.get_messages(peer, ids=mid)
            return observed_reply(raw, p, mid)
        except (Exception, asyncio.CancelledError):
            # Readback FLOOD_WAIT/NotFound/Forbidden/cancel cannot turn an already
            # applied send into a retryable failure or definite refusal.
            raise unknown_reply() from None


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
    if bare_reply(p):
        item = await send_reply(client, peer, request, p)
    elif isinstance(response := await client(request), tl.UpdateShortSentMessage):
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
        if item is None or item.chat_id != p.chat_id or item.id != str(sent_id):
            raise rpc.Internal("Telegram accepted the request without a matching message confirmation.")
    if p.client_id is not None:
        item.ext = {**(item.ext or {}), "telegram": {**(item.ext or {}).get("telegram", {}), "client_id": p.client_id}}
    return dto.MessageResult(message=item)
