"""Reaction DTOs without turning Telegram's common/min view into personal state."""
from datetime import UTC, datetime

from contact_hub_sdk import types as dto
from telethon import types as tl, utils


def key_from_tl(value):
    if isinstance(value, tl.ReactionEmoji) and value.emoticon:
        return dto.ReactionKey(emoji=value.emoticon)
    if isinstance(value, tl.ReactionCustomEmoji):
        return dto.ReactionKey(custom_emoji_id=str(value.document_id))
    if isinstance(value, tl.ReactionPaid):
        return dto.ReactionKey(paid=True)
    return None


def reaction_info(raw):
    if raw is None or raw.min:
        return None
    result = []
    for value in raw.results:
        key = key_from_tl(value.reaction)
        if key is None:
            return None  # A replacement array must not silently lose a reaction.
        mine, my_count = value.chosen_order is not None, None
        if key.paid:
            # Paid writes are unsupported, but known incoming metadata survives.
            # The leaderboard includes our own row even when it isn't a top row.
            if raw.top_reactors is None:
                return None
            my_count = sum(reactor.count for reactor in raw.top_reactors if reactor.my)
            mine = my_count > 0
        result.append(dto.Reaction(key=key, count=value.count, mine=mine, my_count=my_count))
    return result


def reactor_info(values, *, names=None, exclude_mine=False):
    if values is None:
        return None
    result = []
    for value in values:
        if exclude_mine and value.my:
            continue
        key = key_from_tl(value.reaction)
        if key is None:
            return None
        uid = str(utils.get_peer_id(value.peer_id))
        result.append(dto.Reactor(user_id=uid, key=key, name=(names or {}).get(uid),
                                  at=value.date, unread=bool(value.unread)))
    return sorted(result, key=lambda value: value.at or datetime.min.replace(tzinfo=UTC), reverse=True)


def recent_reactors(raw):
    if raw is None or raw.min:
        return None
    return reactor_info(raw.recent_reactions, exclude_mine=True)
