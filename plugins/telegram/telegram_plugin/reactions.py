"""Ordinary reactions using the locked layer 233 requests and scoped full reads."""
import asyncio
import base64
import copy
import hashlib
import json
import logging

from contact_hub_sdk import errors as rpc, types as dto
from telethon import functions, types as tl, utils

from .actions import fetch
from .mapping import identifier, message, peer_id, resolve_chat
from .reaction_info import key_from_tl, reaction_info, reactor_info, recent_reactors
from .session import platform_errors

# An implementation bound, not a claimed Telegram account entitlement. The
# account's reported appConfig limit is returned separately by available().
MAX_SELECTION = 100
log = logging.getLogger(__name__)


def key_to_tl(key, *, field="key"):
    if sum(value is not None for value in (key.emoji, key.custom_emoji_id, key.paid)) != 1:
        raise rpc.InvalidParams("Supply exactly one reaction key.", field=field)
    if key.paid is not None:
        if key.paid is not True:
            raise rpc.InvalidParams(field=field)
        raise rpc.NotSupported("Paid reactions are not supported.", capability="paid.react")
    if key.custom_emoji_id is not None:
        return tl.ReactionCustomEmoji(identifier(key.custom_emoji_id, field, peer=True))
    if not key.emoji or len(key.emoji.encode("utf-8")) > 128:
        raise rpc.InvalidParams(field=field)
    return tl.ReactionEmoji(key.emoji)  # Preserve emoji sequences and variation selectors.


def identity(key):
    return (key.emoji, key.custom_emoji_id, key.paid)


def cursor(p, offset):
    if not isinstance(offset, str) or not offset:
        raise rpc.Internal("Telegram returned an invalid reaction continuation.")
    scope = [p.account_id, p.chat_id, p.message_id, identity(p.key) if p.key is not None else None]
    tag = hashlib.blake2s(json.dumps(scope, ensure_ascii=False).encode(), digest_size=16).hexdigest()
    payload = json.dumps([1, tag, offset], ensure_ascii=False, separators=(",", ":")).encode()
    encoded = base64.urlsafe_b64encode(payload).decode().rstrip("=")
    if len(encoded) > 4096:
        raise rpc.Internal("Telegram's reaction continuation exceeds the wire limit.")
    return encoded


def offset(p):
    if p.cursor is None:
        return None
    try:
        if not 1 <= len(p.cursor) <= 4096:
            raise ValueError()
        payload = base64.b64decode(p.cursor + "=" * (-len(p.cursor) % 4), altchars=b"-_", validate=True)
        version, _, value = json.loads(payload)
        if version != 1 or cursor(p, value) != p.cursor:
            raise ValueError()
        return value
    except (ValueError, TypeError, UnicodeError, rpc.Internal):
        raise rpc.InvalidParams("Invalid reaction cursor for this account, message or filter.", field="cursor") from None


async def available(client, p):
    peer = await resolve_chat(client, p.chat_id) if p.chat_id is not None else None
    me = await client.get_me()
    if not isinstance(me, tl.User):
        raise rpc.AuthRequired()
    premium = bool(me.premium)
    config = await client(functions.help.GetAppConfigRequest(hash=0))
    if not isinstance(config, tl.help.AppConfig) or not isinstance(config.config, tl.JsonObject):
        raise rpc.Internal("Telegram did not report its reaction limits.")
    limit_key = "reactions_user_max_premium" if premium else "reactions_user_max_default"
    reported = next((item.value.value for item in config.config.value
                     if item.key == limit_key and isinstance(item.value, tl.JsonNumber)), None)
    maximum = (min(int(reported), MAX_SELECTION)
               if isinstance(reported, (int, float)) and 1 <= reported < 2**31 and reported == int(reported) else None)
    if isinstance(peer, tl.InputPeerChannel):
        response = await client(functions.channels.GetFullChannelRequest(utils.get_input_channel(peer)))
        expected, kind = peer.channel_id, tl.ChannelFull
    elif isinstance(peer, tl.InputPeerChat):
        response = await client(functions.messages.GetFullChatRequest(peer.chat_id))
        expected, kind = peer.chat_id, tl.ChatFull
    else:
        response = await client(functions.messages.GetAvailableReactionsRequest(hash=0))
        if not isinstance(response, tl.messages.AvailableReactions):
            raise rpc.Internal("Telegram did not report the available reactions.")
        return dto.ReactionsAvailableResult(reactions=[dto.ReactionKey(emoji=value.reaction) for value in response.reactions
                if not value.inactive and (premium or not value.premium)], allow_custom=premium, max_per_message=maximum)
    if not isinstance(response, tl.messages.ChatFull) or not isinstance(response.full_chat, kind) or response.full_chat.id != expected:
        raise rpc.Internal("Telegram returned reaction policy for a different chat.")
    policy = response.full_chat.available_reactions
    if isinstance(policy, tl.ChatReactionsAll):
        return dto.ReactionsAvailableResult(allow_custom=bool(policy.allow_custom) and premium, max_per_message=maximum)
    if isinstance(policy, tl.ChatReactionsNone):
        return dto.ReactionsAvailableResult(reactions=[], allow_custom=False, max_per_message=maximum)
    if isinstance(policy, tl.ChatReactionsSome):
        keys = [key_from_tl(value) for value in policy.reactions]
        if any(key is None or key.paid for key in keys):
            raise rpc.NotSupported("This reaction policy contains unsupported keys.", capability="reactions.set")
        return dto.ReactionsAvailableResult(reactions=keys, allow_custom=False, max_per_message=maximum)
    # Absence is not evidence of either an unrestricted policy or a disabled chat.
    raise rpc.Internal("Telegram did not report this chat's reaction policy.")


def updates(response):
    if isinstance(response, tl.UpdateShort):
        return [response.update]
    if isinstance(response, (tl.Updates, tl.UpdatesCombined)):
        return response.updates
    if isinstance(response, tl.UpdatesTooLong):
        return []
    raise rpc.Internal("Telegram did not return a reaction acknowledgement.")


def matching_update(response, raw):
    for update in updates(response):
        if not isinstance(update, tl.UpdateMessageReactions) or update.msg_id != raw.id or peer_id(update.peer) != peer_id(raw.peer_id):
            continue
        if update.saved_peer_id is not None:
            raise rpc.NotSupported("Saved-peer reaction scopes are not supported.", capability="reactions.set")
        if update.top_msg_id is not None and message(raw).topic_id != str(update.top_msg_id):
            raise rpc.Internal("Telegram returned reactions for a different topic.")
        return update
    return None


def with_update(raw, update):
    result = copy.copy(raw)
    result.reactions = update.reactions
    return result


async def hydrate(client, peer, raw, *, force=False):
    state = getattr(raw, "reactions", None)
    if force or (state is not None and state.min):
        response = await client(functions.messages.GetMessagesReactionsRequest(peer=peer, id=[raw.id]))
        update = matching_update(response, raw)
        if update is not None:
            return with_update(raw, update)
        if force:
            raise rpc.Internal("Telegram did not return a matching full reaction snapshot.")
    return raw


async def set_reactions(account, client, p):
    if len(p.reactions) > MAX_SELECTION:
        raise rpc.InvalidParams(field="reactions")
    keys = [key_to_tl(key, field="reactions") for key in p.reactions]
    requested = {identity(key) for key in p.reactions}
    if len(requested) != len(keys):
        raise rpc.InvalidParams("Reaction keys must be unique.", field="reactions")
    mid = identifier(p.message_id, "message_id")
    async with account.data.setdefault("reaction_lock", asyncio.Lock()):
        peer = await resolve_chat(client, p.chat_id)
        raw, item = await fetch(client, peer, p.chat_id, mid)
        if item is None:
            raise rpc.NotFound(kind="message")
        response = await client(functions.messages.SendReactionRequest(peer=peer, msg_id=mid, reaction=keys, big=p.big))
        update = matching_update(response, raw)
        if update is not None and not update.reactions.min:
            updated = with_update(raw, update)
        else:
            # This is a scoped read after an acknowledgement, never a replay.
            raw, item = await fetch(client, peer, p.chat_id, mid)
            if item is None:
                raise rpc.Internal("Telegram accepted the reaction without an available matching message.")
            updated = await hydrate(client, peer, raw, force=True)
        item = message(updated)
        if item.reactions is None or {identity(value.key) for value in item.reactions if value.mine and not value.key.paid} != requested:
            raise rpc.Internal("Telegram did not confirm the requested ordinary reaction selection.")
        return dto.OptionalMessageResult(message=item)


async def who(client, p):
    if not 1 <= p.limit <= 200:
        raise rpc.InvalidParams(field="limit")
    key = key_to_tl(p.key) if p.key is not None else None
    start = offset(p)
    mid = identifier(p.message_id, "message_id")
    peer = await resolve_chat(client, p.chat_id)
    _, item = await fetch(client, peer, p.chat_id, mid)
    if item is None:
        raise rpc.NotFound(kind="message")
    response = await client(functions.messages.GetMessageReactionsListRequest(peer=peer, id=mid, reaction=key, offset=start, limit=p.limit))
    if not isinstance(response, tl.messages.MessageReactionsList):
        raise rpc.Internal("Telegram did not return a reactor page.")
    names = {str(utils.get_peer_id(entity)): utils.get_display_name(entity) or None for entity in [*response.users, *response.chats]}
    reactors = reactor_info(response.reactions, names=names)
    if reactors is None or len(reactors) > p.limit or (p.key is not None and any(identity(value.key) != identity(p.key) for value in reactors)):
        raise rpc.Internal("Telegram returned an invalid reactor page.")
    if response.next_offset is not None and response.next_offset == start:
        raise rpc.Internal("Telegram returned a reaction continuation that does not advance.")
    return dto.ReactionsWhoResult(reactors=reactors, next_cursor=cursor(p, response.next_offset) if response.next_offset is not None else None)


async def refresh_live(account, client, update):
    if update.saved_peer_id is not None:
        return  # A saved-peer/monoforum identity is not the message-topic identity.
    async with account.data.setdefault("reaction_lock", asyncio.Lock()):
        try:
            with platform_errors():
                chat = peer_id(update.peer)
                peer = await resolve_chat(client, chat)
                raw, item = await fetch(client, peer, chat, update.msg_id)
                if item is None:
                    return
                # Check the outer event as well as any scoped hydration response.
                matching_update(tl.UpdateShort(update, item.sent_at), raw)
                raw = await hydrate(client, peer, raw, force=True) if update.reactions.min else with_update(raw, update)
                values = reaction_info(raw.reactions)
                if values is None:
                    return
        except rpc.PluginError:
            log.debug("Reaction refresh is unavailable; a later scoped read can refresh it.")
            return
        await account.reaction_changed(chat, str(raw.id), values, reactors=recent_reactors(raw.reactions))
