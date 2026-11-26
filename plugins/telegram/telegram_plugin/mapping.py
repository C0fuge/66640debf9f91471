"""Telegram TL values to and from the public plugin DTOs."""
from contact_hub_sdk import errors as rpc
from contact_hub_sdk import types as dto
from telethon import types as tl, utils

from .media_info import media_info
from .poll_info import poll_info
from .reaction_info import reaction_info, recent_reactors


def identifier(value, field, *, peer=False):
    if not isinstance(value, str) or not 1 <= len(value) <= 20:
        raise rpc.InvalidParams(field=field)
    try:
        number = int(value)
    except ValueError:
        raise rpc.InvalidParams(field=field) from None
    if str(number) != value or number == 0 or not (-(2**63) < number < 2**63 if peer else 0 < number < 2**31):
        raise rpc.InvalidParams(field=field)
    return number


def sid(value):
    return str(value) if value is not None else None


async def resolve_chat(client, chat_id, *, field="chat_id"):
    marked = identifier(chat_id, field, peer=True)
    raw_id, cls = utils.resolve_id(marked)
    peer = cls(raw_id)
    try:
        result = await client.get_input_entity(peer)
        if isinstance(result, tl.InputPeerSelf):
            result = await client.get_me(input_peer=True)
        # The Telethon memory cache is keyed by the unmarked numeric ID. On
        # collision, try the typed lookup in the account's SQLite session.
        if utils.get_peer_id(result) != marked:
            result = client.session.get_input_entity(peer)
        if utils.get_peer_id(result) != marked:
            raise ValueError()
        return result
    except ValueError:
        raise rpc.NotFound(kind="chat") from None


def peer_id(peer):
    return sid(utils.get_peer_id(peer)) if peer is not None else None


_ENTITY_CLASSES = {
    "bold": tl.MessageEntityBold, "italic": tl.MessageEntityItalic,
    "underline": tl.MessageEntityUnderline, "strike": tl.MessageEntityStrike,
    "spoiler": tl.MessageEntitySpoiler, "code": tl.MessageEntityCode,
    "pre": tl.MessageEntityPre, "blockquote": tl.MessageEntityBlockquote,
    "url": tl.MessageEntityUrl, "text_url": tl.MessageEntityTextUrl,
    "mention": tl.MessageEntityMention, "hashtag": tl.MessageEntityHashtag,
    "cashtag": tl.MessageEntityCashtag, "bot_command": tl.MessageEntityBotCommand,
    "email": tl.MessageEntityEmail, "phone": tl.MessageEntityPhone,
    "bank_card": tl.MessageEntityBankCard, "custom_emoji": tl.MessageEntityCustomEmoji,
}
SEND_ENTITY_TYPES = list(_ENTITY_CLASSES)
_ENTITY_NAMES = {cls: key for key, cls in _ENTITY_CLASSES.items()}
_ENTITY_NAMES[tl.MessageEntityMentionName] = "mention_name"


def entities_from_tl(entities):
    result = []
    for entity in entities or ():
        kind = _ENTITY_NAMES.get(type(entity))
        if kind:
            result.append(dto.Entity(type=kind, offset=entity.offset, length=entity.length,
                                     url=getattr(entity, "url", None), language=getattr(entity, "language", None),
                                     collapsed=getattr(entity, "collapsed", None),
                                     user_id=sid(getattr(entity, "user_id", None)),
                                     custom_emoji_id=sid(getattr(entity, "document_id", None))))
    return result


def entities_to_tl(text, entities):
    if not entities:
        return []
    boundary = {0}
    end = 0
    for char in text:
        end += 2 if ord(char) > 0xffff else 1
        boundary.add(end)
    result = []
    for entity in entities:
        if entity.type not in _ENTITY_CLASSES:
            raise rpc.NotSupported("Unsupported Telegram entity type.", capability="send.entities")
        if entity.length <= 0 or entity.offset not in boundary or entity.offset + entity.length not in boundary:
            raise rpc.InvalidParams("Invalid UTF-16 entity range.", field="entities")
        args = {}
        if entity.type == "pre":
            args["language"] = entity.language or ""
        elif entity.type == "text_url":
            if not entity.url:
                raise rpc.InvalidParams(field="entities")
            args["url"] = entity.url
        elif entity.type == "blockquote":
            args["collapsed"] = entity.collapsed
        elif entity.type == "custom_emoji":
            args["document_id"] = identifier(entity.custom_emoji_id, "entities", peer=True)
        result.append(_ENTITY_CLASSES[entity.type](entity.offset, entity.length, **args))
    return result


def contact(user):
    return dto.Contact(id=str(user.id), display_name=utils.get_display_name(user) or None,
                       first_name=user.first_name, last_name=user.last_name, username=user.username,
                       phone=user.phone, is_bot=user.bot, is_contact=user.contact,
                       is_premium=user.premium, is_verified=user.verified, is_deleted=user.deleted)


def draft(raw):
    if isinstance(raw, tl.DraftMessageEmpty):
        return dto.Draft(text="", updated_at=raw.date)
    if isinstance(raw, tl.DraftMessage):
        return dto.Draft(text=raw.message, entities=entities_from_tl(raw.entities),
                         reply_to=sid(getattr(raw.reply_to, "reply_to_msg_id", None)), updated_at=raw.date)
    return None


def archive_state(folder_id):
    # API peer folders are separate from configurable dialog filters. Telegram
    # defines only the default list (absent/0) and archive (1).
    if folder_id is None:
        return False
    if type(folder_id) is int and folder_id in (0, 1):
        return folder_id == 1
    raise rpc.NotSupported("Unsupported Telegram dialog folder.", capability="chat.archive")


def conversation(dialog):
    entity, raw = dialog.entity, dialog.dialog
    return dto.Conversation(id=str(dialog.id), kind="dm" if dialog.is_user else "group" if dialog.is_group else "channel",
                            title=dialog.title, peer_user_id=str(entity.id) if dialog.is_user else None,
                            is_self_chat=getattr(entity, "is_self", None), username=getattr(entity, "username", None),
                            archived=archive_state(getattr(raw, "folder_id", None)), pinned=dialog.pinned, unread_count=dialog.unread_count,
                            unread_mentions=dialog.unread_mentions_count, unread_reactions=dialog.unread_reactions_count,
                            last_message_at=dialog.date, is_forum=getattr(entity, "forum", None),
                            read_inbox_max_id=sid(raw.read_inbox_max_id), read_outbox_max_id=sid(raw.read_outbox_max_id),
                            marked_unread=raw.unread_mark, draft=draft(raw.draft))


_SERVICE_KINDS = {
    tl.MessageActionChatAddUser: "joined", tl.MessageActionChatJoinedByLink: "joined",
    tl.MessageActionChatJoinedByRequest: "joined", tl.MessageActionChatDeleteUser: "left",
    tl.MessageActionPinMessage: "pinned", tl.MessageActionChatEditTitle: "title_changed",
    tl.MessageActionChatEditPhoto: "photo_changed", tl.MessageActionChatDeletePhoto: "photo_removed",
    tl.MessageActionChatCreate: "created", tl.MessageActionChannelCreate: "created",
    tl.MessageActionPhoneCall: "call", tl.MessageActionSetMessagesTTL: "ttl_changed",
    tl.MessageActionTopicCreate: "topic_created", tl.MessageActionTopicEdit: "topic_edited",
    tl.MessageActionHistoryClear: "history_cleared", tl.MessageActionContactSignUp: "contact_joined",
    tl.MessageActionScreenshotTaken: "screenshot",
}


def service_action(raw, sender, reply_id):
    action = getattr(raw, "action", None)
    if action is None or isinstance(action, tl.MessageActionEmpty):
        return None
    kind = _SERVICE_KINDS.get(type(action), "other")
    targets = getattr(action, "users", None)
    if isinstance(action, tl.MessageActionChatDeleteUser):
        targets = [action.user_id]
    return dto.ServiceAction(kind=kind, actor_id=sender, title=getattr(action, "title", None),
                             target_ids=[str(value) for value in targets] if targets is not None else None,
                             message_id=sid(reply_id) if kind == "pinned" else None,
                             seconds=getattr(action, "period", None) if kind == "ttl_changed" else getattr(action, "duration", None),
                             video=getattr(action, "video", None),
                             missed=isinstance(action.reason, tl.PhoneCallDiscardReasonMissed) if kind == "call" else None,
                             text=getattr(action, "message", None) or (type(action).__name__ if kind == "other" else None))


def message(raw):
    if not getattr(raw, "date", None) or not getattr(raw, "peer_id", None) or isinstance(raw, tl.MessageEmpty):
        return None
    header = getattr(raw, "reply_to", None)
    reply_id = getattr(header, "reply_to_msg_id", None)
    topic = None
    if getattr(header, "forum_topic", False):
        topic = getattr(header, "reply_to_top_id", None) or reply_id or 1
    if isinstance(getattr(raw, "action", None), tl.MessageActionTopicCreate):
        topic = raw.id
    reply = dto.ReplyRef(message_id=str(reply_id), chat_id=peer_id(getattr(header, "reply_to_peer_id", None)),
                         quote_text=getattr(header, "quote_text", None),
                         quote_entities=entities_from_tl(header.quote_entities) if getattr(header, "quote_entities", None) is not None else None,
                         quote_offset=getattr(header, "quote_offset", None)) if reply_id else None
    forwarded = getattr(raw, "fwd_from", None)
    origin = getattr(forwarded, "from_id", None)
    forward = dto.ForwardInfo(name=forwarded.from_name, sender_id=peer_id(origin),
                              chat_id=peer_id(origin) if isinstance(origin, (tl.PeerChannel, tl.PeerChat)) else None,
                              message_id=sid(forwarded.channel_post), at=forwarded.date,
                              signature=forwarded.post_author) if forwarded else None
    text = getattr(raw, "message", None)
    sender = sid(getattr(raw, "sender_id", None)) or peer_id(getattr(raw, "from_id", None))
    replies = getattr(raw, "replies", None)
    ttl = getattr(raw, "ttl_period", None)
    if ttl is None:
        ttl = getattr(getattr(raw, "media", None), "ttl_seconds", None)
    return dto.Message(id=str(raw.id), chat_id=peer_id(raw.peer_id), direction="out" if raw.out else "in",
                       sent_at=raw.date, sender_id=sender, text=text, topic_id=sid(topic),
                       entities=entities_from_tl(getattr(raw, "entities", None)) if text is not None else None,
                       media=media_info(getattr(raw, "media", None)),
                       poll=poll_info(getattr(raw, "media", None), entities_from_tl),
                       reactions=reaction_info(getattr(raw, "reactions", None)),
                       reactors=recent_reactors(getattr(raw, "reactions", None)),
                       reply_to=reply, forward_from=forward,
                       grouped_id=sid(getattr(raw, "grouped_id", None)), views=getattr(raw, "views", None),
                       action=service_action(raw, sender, reply_id), mentioned=getattr(raw, "mentioned", None),
                       replies_count=getattr(replies, "replies", None),
                       replies_chat_id=peer_id(tl.PeerChannel(replies.channel_id)) if getattr(replies, "channel_id", None) else None,
                       via_bot_id=sid(getattr(raw, "via_bot_id", None)), effect_id=sid(getattr(raw, "effect", None)),
                       ttl_seconds=ttl,
                       is_pinned=getattr(raw, "pinned", None), silent=getattr(raw, "silent", None),
                       edited=True if getattr(raw, "edit_date", None) else None, edited_at=getattr(raw, "edit_date", None))
