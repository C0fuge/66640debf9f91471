"""A tiny plugin driven by test_e2e.py over the real stdio transport."""

import asyncio
import sys
from datetime import UTC, datetime

from contact_hub_sdk import FloodWait, InvalidParams, Plugin, current_request, method, progress, types as T

AT = datetime(2026, 10, 19, 14, 3, 27, 512000, tzinfo=UTC)


class Echo(Plugin):
    capabilities = {"media.download": True, "send.silent": True, "x-echo.flag": {"a": 1}}

    async def on_initialize(self, params):
        print("a stray print must land on stderr, not stdout")

    async def on_account_start(self, account):
        if account.setting("fail"):
            raise InvalidParams("fail is set", field="fail")
        account.spawn(self.connect(account))

    async def connect(self, account):
        if account.secrets.get("session"):
            await account.status("connected", me=T.Contact(id="me", display_name=account.settings["name"]))
        else:
            await account.status("auth_required")

    async def on_auth_begin(self, account, method):
        phone = await account.auth.ask("phone", title="Phone")
        code = await account.auth.ask("code", hint=f"sent to {phone}", can_resend=True)
        while code != "12345":
            code = await account.auth.ask("code", error="Wrong code")
        await account.set_secret("session", "sess-" + phone)
        await account.status("connected")

    @method("messages.fetch_recent")
    async def fetch(self, account, p: T.MessagesFetchRecentParams):
        msgs = [T.Message(id=str(i), chat_id=p.chat_id, direction=T.Direction.IN, sent_at=AT, text=f"m{i}")
                for i in range(p.limit)]
        return T.PagedMessagesResult(messages=msgs, next_cursor="older")

    @method("messages.send")
    async def send(self, account, p: T.MessagesSendParams):
        if p.text == "flood":
            raise FloodWait(37)
        if p.text == "boom":
            raise RuntimeError("kaboom")
        msg = T.Message(id="m1", chat_id=p.chat_id, direction="out", sent_at=AT, text=p.text,
                        silent=p.silent, delivery=T.Delivery.SENT)
        await account.messages_upsert([msg], ack=True)
        return T.MessageResult(message=msg)

    @method("media.download")
    async def download(self, account, p: T.MediaDownloadParams):
        await progress(10, 100)
        try:
            await asyncio.sleep(3600)
        finally:
            await account.status("connected", detail="download aborted")

    @method("x-echo.whoami")
    def whoami(self, account, params):  # a sync vendor handler, run in a thread
        return {"account": account.id if account else None, "request": current_request().id}

    @method("x-echo.set_secret")
    async def set_secret(self, account, params):  # errors from the core propagate as they are
        await account.request("secrets.set", {"key": "k", "value": "v"}, timeout=params.get("timeout"))
        return {}

    @method("x-echo.slow")
    async def slow(self, account, params):
        await asyncio.sleep(params["seconds"])
        return {"slept": params["seconds"]}


class ChatEcho(Echo):
    capabilities = {**Echo.capabilities, "chat.get": {}}

    @method("chats.get")
    async def chat(self, account, p: T.ChatsGetParams):
        return T.ChatsGetResult(conversation=T.Conversation(id=p.chat_id, kind=T.ConversationKind.DM, pinned=False))


class LegacyEcho(Echo):
    api_version = "1.0.0"


class MinorEcho(ChatEcho):
    api_version = "1.1.0"


class SourceEcho(ChatEcho):
    capabilities = {**ChatEcho.capabilities, "send.file": {"source_release": "on_success"}}

    @method("messages.send_file")
    async def send_file(self, account, p: T.MessagesSendFileParams):
        with open(p.path, "rb") as source:
            payload = source.read()
        return T.MessageResult(message=T.Message(id="file", chat_id=p.chat_id, direction="out", sent_at=AT,
            media=T.Media(kind="document", bytes=len(payload))))


if __name__ == "__main__":
    modes = {"modern": ChatEcho, "legacy": LegacyEcho, "minor": MinorEcho, "source": SourceEcho}
    plugin = (modes[sys.argv[1]] if len(sys.argv) > 1 else Echo)()
    if len(sys.argv) > 2 and isinstance(plugin, SourceEcho):
        flag = {"true": True, "empty": {}, "unknown": {"source_release": "future"}}[sys.argv[2]]
        plugin.capabilities = {**plugin.capabilities, "send.file": flag}
    plugin.run()
