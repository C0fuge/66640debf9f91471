import base64
import hashlib
import json
import struct
from datetime import UTC, datetime, timedelta

from contact_hub_sdk import errors as rpc
from contact_hub_sdk import types as dto

from .mapping import identifier, message, resolve_chat


def cursor(params, direction, msg_id):
    scope = json.dumps([params.account_id, params.chat_id, params.topic_id], separators=(",", ":")).encode()
    tag = hashlib.blake2s(scope, digest_size=8).digest()
    blob = struct.pack(">c8sI", direction, tag, int(msg_id))
    return base64.urlsafe_b64encode(blob).decode().rstrip("=")


def boundary(params, direction):
    if params.cursor is None:
        return 0
    try:
        if len(params.cursor) != 18:
            raise ValueError()
        payload = base64.b64decode(params.cursor + "==", altchars=b"-_", validate=True)
        _, _, value = struct.unpack(">c8sI", payload)
        if not 0 < value < 2**31 or params.cursor != cursor(params, direction, value):
            raise ValueError()
        return value
    except (ValueError, struct.error):
        raise rpc.InvalidParams("Invalid history cursor for this thread.", field="cursor") from None


async def read(client, p, count, **offset):
    topic_id = identifier(p.topic_id, "topic_id") if p.topic_id is not None else None
    if not 1 <= p.limit <= 200:
        raise rpc.InvalidParams(field="limit")
    peer = await resolve_chat(client, p.chat_id)
    rows = []
    async for raw in client.iter_messages(peer, limit=count, reply_to=topic_id, **offset):
        if item := message(raw):
            if item.chat_id != p.chat_id:
                raise rpc.Internal("Telegram returned history for a different chat.")
            rows.append(item)
    return rows


async def recent(client, p):
    rows = await read(client, p, p.limit + 1, offset_id=boundary(p, b"B"))
    page = rows[:p.limit]
    return dto.PagedMessagesResult(messages=page, next_cursor=cursor(p, b"B", page[-1].id) if len(rows) > p.limit else None)


async def fill_gap(client, p):
    lower = identifier(p.after_message_id, "after_message_id")
    upper = identifier(p.before_message_id, "before_message_id") if p.before_message_id is not None else 0
    if upper and upper <= lower:
        raise rpc.InvalidParams("Gap endpoints must be ordered.", field="before_message_id")
    start = max(lower, boundary(p, b"F"))
    if upper and start >= upper:
        return dto.PagedMessagesResult(messages=[])
    rows = await read(client, p, p.limit + 1, min_id=start, max_id=upper, reverse=True)
    page = rows[:p.limit]
    return dto.PagedMessagesResult(messages=page, next_cursor=cursor(p, b"F", page[-1].id) if len(rows) > p.limit else None)


async def around(client, p):
    # Telegram dates have second precision and offset_date is exclusive.
    # MTProto serializes dates as signed 32-bit Unix seconds.
    if p.at < datetime(1970, 1, 1, tzinfo=UTC):
        past = []
    elif p.at >= datetime.fromtimestamp(2**31 - 1, UTC):
        past = await read(client, p, p.limit + 1)
    else:
        past = await read(client, p, p.limit + 1, offset_date=p.at.replace(microsecond=0) + timedelta(seconds=1))
    pivot = int(past[0].id) if past else 0
    future = await read(client, p, p.limit + 1, min_id=pivot, reverse=True)
    left = min(len(past), max((p.limit + 1) // 2, p.limit - len(future)))
    right = min(len(future), p.limit - left)
    page = list(reversed(future[:right])) + past[:left]
    return dto.HistoryAroundResult(messages=page,
                                   older_cursor=cursor(p, b"B", page[-1].id) if left < len(past) else None,
                                   newer_cursor=cursor(p, b"F", page[0].id) if right < len(future) else None)
