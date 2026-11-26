"""Bounded plain-text edit policy; no network, account discovery or persistence."""
from dataclasses import dataclass
import math
import time

from contact_hub_sdk import Forbidden, NotSupported, PluginError, Unavailable


@dataclass(frozen=True)
class EditTarget:
    chat_id: int
    message_id: int
    sent_at: int
    policy_generation: int


class EditPolicy:
    def __init__(self, clock=time.time):
        self.clock = clock
        self.generation = 0
        self.timeout = None

    def reset(self):
        self.generation += 1
        self.timeout = None

    def observe(self, config, *, patch=False):
        # The first-party full upsert preserves omitted server maps; patches
        # preserve omitted keys. Invalid supplied policy revokes old permission.
        if not isinstance(config, dict):
            self.reset()
            return
        if "server" not in config:
            if "experiments" in config:
                # This slice uses an explicit server override only. A changed
                # experiments map is not permission to fall back to an old value.
                self.reset()
            return
        server = config["server"]
        if not isinstance(server, dict):
            self.reset()
            return
        if patch and "edit-timeout" not in server:
            return
        self.generation += 1
        value = server.get("edit-timeout")
        self.timeout = value if type(value) is int and 0 <= value <= 2**31 - 1 else None

    def check(self, sent_at=None, generation=None):
        if generation is not None and generation != self.generation:
            raise Unavailable("MAX edit policy changed; refresh before editing")
        if self.timeout is None:
            raise Unavailable("MAX edit policy has not been observed")
        if self.timeout == 0:
            raise Forbidden("MAX does not currently allow text editing")
        if sent_at is not None:
            now = self.clock() * 1000
            if not math.isfinite(now) or type(sent_at) is not int or not 0 <= sent_at <= now:
                raise Forbidden("MAX message time cannot authorize editing")
            if not sent_at > now - self.timeout * 1000:
                raise Forbidden("MAX message edit window has expired")
        return self.generation


def plain_message(row, chat_id, message_id, me, *, edited=False):
    """Validate raw fields before pinned models can coerce them or omit data."""
    from .adapter import response_id

    if not isinstance(row, dict) or response_id(row.get("id")) != message_id:
        raise PluginError("MAX edit response did not identify the requested message")
    # Domain models accept snake_case Python aliases. Raw network objects must
    # not smuggle scope/comment/scheduling fields past the camelCase guards.
    if any(name in row for name in ("chat_id", "post_id", "delayed_attributes")):
        raise PluginError("MAX edit response used ambiguous message metadata")
    if row.get("chatId") is not None and response_id(row["chatId"]) != chat_id:
        raise PluginError("MAX edit response identified a different conversation")
    if row.get("sender") is None or response_id(row["sender"]) != me:
        raise Forbidden("MAX text editing requires the authenticated outgoing sender")
    if row.get("type") != "USER" or row.get("postId") is not None or row.get("delayedAttributes") is not None:
        raise NotSupported("MAX editing is limited to ordinary unscheduled messages")
    if row.get("attaches", []) != [] or row.get("elements", []) != [] or row.get("link") is not None:
        raise NotSupported("MAX editing attachments, links and formatting is not implemented")
    if not isinstance(row.get("text"), str) or not row["text"]:
        raise NotSupported("MAX text editing requires an existing text message")
    if row.get("status") not in ({"EDITED"} if edited else {None, "EDITED"}):
        raise Forbidden("MAX message is no longer editable")
    options = row.get("options")
    if options is not None and (type(options) is not int or not 0 <= options <= 2**32 - 1):
        raise NotSupported("MAX message edit flags could not be verified")
    if (options or 0) & 32:
        raise Forbidden("MAX message is marked as noneditable")
    if type(row.get("time")) is not int or row["time"] < 0:
        raise PluginError("MAX returned an invalid message time")


def writable_chat(row, chat_id, me):
    from .adapter import response_id

    if not isinstance(row, dict) or response_id(row.get("id")) != chat_id:
        raise PluginError("MAX edit preflight returned a different conversation")
    if "video_conversation" in row:
        raise PluginError("MAX edit preflight used ambiguous conversation metadata")
    if row.get("type") == "DIALOG" and chat_id == 0:
        return
    if row.get("type") != "CHAT" or row.get("videoConversation") is not None:
        raise NotSupported("MAX editing is limited to Saved Messages and ordinary groups")
    members, restrictions = row.get("participants"), row.get("restrictions")
    if row.get("status") != "ACTIVE" or not isinstance(members, dict):
        raise Forbidden("MAX group membership could not be verified")
    if type(restrictions) is not int or not 0 <= restrictions <= 2**32 - 1:
        raise Forbidden("MAX group write permissions could not be verified")
    member_ids = {response_id(key) for key in members}
    if any(type(joined) is not int or joined < 0 for joined in members.values()):
        raise Forbidden("MAX group membership could not be verified")
    if me not in member_ids or restrictions & 64:
        raise Forbidden("MAX group does not permit this account to write")
