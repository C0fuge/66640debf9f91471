"""Explicit DTO projection of pinned PyMax models (no shared cross-platform IDs)."""
from datetime import UTC, datetime
from typing import Any

from contact_hub_sdk import PluginError
from contact_hub_sdk import types as T


def timestamp(milliseconds: int) -> datetime:
    return datetime.fromtimestamp(milliseconds / 1000, UTC)


def contact(user: Any) -> T.Contact:
    name = user.names[0] if user.names else None
    return T.Contact(id=str(user.id), display_name=name.name if name else None,
                     first_name=name.first_name if name else None,
                     last_name=name.last_name if name else None,
                     phone=str(user.phone) if user.phone is not None else None,
                     bio=user.description)


def conversation(chat: Any, me: int) -> T.Conversation:
    kind = {"DIALOG": "dm", "CHAT": "group", "CHANNEL": "channel"}.get(chat.type)
    if kind is None:
        raise PluginError("Unsupported MAX conversation type")
    peers = [uid for uid in chat.participants if uid != me]
    return T.Conversation(id=str(chat.id), kind=kind, title=chat.title,
                          peer_user_id=str(peers[0]) if kind == "dm" and len(peers) == 1 else None,
                          unread_count=chat.new_messages,
                          last_message_at=timestamp(chat.last_event_time) if chat.last_event_time else None,
                          member_count=chat.participants_count if kind != "dm" else None,
                          about=chat.description)


def message(raw: Any, me: int, chat_id: int | None = None) -> T.Message:
    if chat_id is not None and raw.chat_id is not None and raw.chat_id != chat_id:
        raise PluginError("MAX message conversation contradicts its parent conversation")
    chat = raw.chat_id if raw.chat_id is not None else chat_id
    if chat is None:
        raise PluginError("MAX message has no conversation identity")
    if raw.sender is None:
        # Missing sender is not evidence of an outgoing message.
        raise PluginError("MAX message has no sender identity")
    link = raw.link
    reply = (T.ReplyRef(message_id=str(link.message.id), chat_id=str(link.chat_id))
             if link is not None and link.type == "REPLY" else None)
    return T.Message(id=str(raw.id), chat_id=str(chat), sender_id=str(raw.sender),
                     direction="out" if raw.sender == me else "in", sent_at=timestamp(raw.time),
                     text=raw.text, edited=raw.status == "EDITED", reply_to=reply,
                     ext={"max": {"has_attachments": bool(raw.attaches), "message_type": raw.type}})
