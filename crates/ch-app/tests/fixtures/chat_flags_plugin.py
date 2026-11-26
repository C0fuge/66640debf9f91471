"""Synthetic Python-SDK dialog fixture; only temporary state, no network or credentials."""
import asyncio
import json
import os
import sys

from contact_hub_sdk import NetworkError, NotSupported, Plugin, method, types as T

MODE = sys.argv[1]


def trace(name, params):
    with open(os.environ["CH_FLAGS_TRACE"], "a", encoding="utf-8") as stream:
        stream.write(json.dumps({"method": name, "params": params}) + "\n")


class ChatFlags(Plugin):
    capabilities = {"chat.get": True, "chat.flags": True, "chat.pin": True, "chat.mark_unread": True}

    def __init__(self):
        super().__init__()
        self.flags = {}
        self.release = asyncio.Event()

    def get_capabilities(self):
        caps = dict(self.capabilities)
        if MODE.startswith("without-"):
            caps[MODE.removeprefix("without-")] = False
        if MODE == "object-caps":
            caps = {key: {} for key in caps}
        return caps

    async def on_account_start(self, account):
        account.spawn(account.status("connected"))

    @method("messages.fetch_recent")
    async def history(self, account, params):
        return {"messages": []}

    @method("messages.send")
    async def send(self, account, params):
        raise NotSupported("This fixture only exercises chat preferences")

    @method("x-flags_fixture.release")
    async def release_work(self, account, params):
        self.release.set()
        return {}

    @method("chats.get")
    async def get(self, account, params: T.ChatsGetParams):
        trace("chats.get", {"account_id": account.id, "chat_id": params.chat_id})
        if MODE == "hold-read":
            await self.release.wait()
        pinned, unread = self.flags.get((account.id, params.chat_id), (False, False))
        conversation = {"id": params.chat_id, "kind": "group", "pinned": pinned, "marked_unread": unread,
                        "title": "must not replace title", "unread_count": 999, "muted": False, "archived": False}
        if MODE in ("sparse", "partial"):
            conversation.pop("marked_unread")
        if MODE == "sparse":
            conversation.pop("pinned")
        if MODE == "wrong-id":
            conversation["id"] = "foreign"
        if MODE == "wrong-kind":
            conversation["kind"] = "private"
        if MODE == "missing":
            return {}
        if MODE == "null":
            return {"conversation": None}
        if MODE == "invalid-flag":
            conversation["pinned"] = "false"
        return {"conversation": conversation}

    @method("chats.set_flags")
    async def set(self, account, params: T.ChatsSetFlagsParams):
        changed = {key: getattr(params, key) for key in ("pinned", "marked_unread") if getattr(params, key) is not None}
        trace("chats.set_flags", {"account_id": account.id, "chat_id": params.chat_id, **changed})
        if MODE == "hold-write":
            await self.release.wait()
        old = self.flags.get((account.id, params.chat_id), (False, False))
        pinned = params.pinned if params.pinned is not None else old[0]
        unread = params.marked_unread if params.marked_unread is not None else old[1]
        self.flags[(account.id, params.chat_id)] = (pinned, unread)
        if MODE == "lost-write":
            await asyncio.Event().wait()
        if MODE == "unknown":
            raise NetworkError("sensitive fixture payload")
        if MODE == "malformed-write":
            return []
        if MODE == "event":
            await account.conversations_upsert([T.Conversation(id=params.chat_id, kind="group", **changed)], ack=True)
        return {}


ChatFlags().run()
