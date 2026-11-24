"""Subprocess injection point used only by synthetic wire tests."""
import asyncio
import os
import sys
from datetime import UTC, datetime
from types import SimpleNamespace

from contact_hub_sdk import types as T
from max_plugin.plugin import MaxPlugin

assert "pymax" not in sys.modules
STAMP = datetime(2026, 1, 1, tzinfo=UTC)


class OfflineClient:
    def __init__(self, account, store, settings, event):
        self.account, self.store, self.event = account, store, event
        self.closed = asyncio.Event()
        self.scenario = os.environ.get("CH_MAX_TEST_SCENARIO", "normal")

    async def connect(self, mode, interactive):
        self.store.identity["attempts"] = self.store.identity.get("attempts", 0) + 1
        self.store.save_identity()
        if self.scenario == "slow":
            await asyncio.Event().wait()
        if self.scenario == "reconnect" and self.store.identity["attempts"] == 1:
            raise ConnectionError("synthetic-private-token-do-not-log")
        if self.scenario == "early-events":
            await self.event("message", self.msg("200", "during login", "in"))
        if interactive:
            if mode == "qr":
                await self.account.auth.qr("max://synthetic-qr")
            else:
                await self.account.auth.ask("phone", title="Synthetic phone")
                await self.account.auth.ask("code", title="Synthetic code")
            await self.account.auth.ask("password", title="Synthetic password")
            self.store.identity["mode"] = mode
            self.store.save_identity()
            session = {"token": "synthetic-token", "phone": "+15550000001",
                       "device_id": self.store.identity["device_id"]}
            await self.store.save_session(SimpleNamespace(model_dump=lambda **_: session))

    async def snapshot(self):
        return (T.Contact(id="1", display_name="Synthetic self"),
                [T.Contact(id="2", display_name="Synthetic peer")],
                [T.Conversation(id="10", kind="dm", peer_user_id="2")],
                [self.msg("20", "incoming", "in")])

    def msg(self, mid, text, direction="out"):
        return T.Message(id=mid, chat_id="10", direction=direction, sent_at=STAMP,
                         sender_id="1" if direction == "out" else "2", text=text)

    async def wait_closed(self):
        await self.closed.wait()

    async def close(self):
        self.closed.set()

    async def recent(self, chat_id, limit):
        if chat_id == 99:
            return []
        return [self.msg("20", "old"), self.msg("22", "new"), self.msg("21", "middle")][:limit]

    async def send(self, chat_id, text, reply_to):
        count = self.store.identity.get("send_count", 0) + 1
        self.store.identity["send_count"] = count
        self.store.save_identity()
        if text == "uncertain":
            raise ConnectionError("synthetic-private-token-do-not-log")
        message = self.msg(str(100 + count), text)
        if text == "live":
            await self.event("message", self.msg("200", "live incoming", "in"))
            await self.event("conversation", T.Conversation(id="10", kind="dm", title="Updated"))
            await self.event("deleted", ("10", ["19"]))
        return message


MaxPlugin(OfflineClient, retry_delay=0.01).run()
