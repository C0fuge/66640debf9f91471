"""Subprocess injection point used only by synthetic wire tests."""
import asyncio
import os
import sys
from datetime import UTC, datetime
from types import SimpleNamespace

from contact_hub_sdk import Forbidden, NotFound, NotSupported, PluginError, types as T
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
        if self.scenario in {"overflow-events", "overflow-pending-discovery"}:
            for index in range(1001):
                # Match PyMax's dispatcher: callback exceptions do not fail login.
                try:
                    await self.event("message", self.msg(str(2000 + index), "during snapshot", "in"))
                except Exception:
                    pass
            if self.scenario == "overflow-pending-discovery":
                try:
                    await asyncio.Event().wait()
                finally:
                    self.store.identity["snapshot_cancelled"] = True
                    self.store.save_identity()
        return (T.Contact(id="1", display_name="Synthetic self"),
                [T.Contact(id="2", display_name="Synthetic peer")],
                [T.Conversation(id="10", kind="dm", peer_user_id="2")],
                [self.msg("20", "incoming", "in")])

    def msg(self, mid, text, direction="out"):
        return T.Message(id=mid, chat_id="10", direction=direction, sent_at=STAMP,
                         sender_id="1" if direction == "out" else "2", text=text,
                         ext={"max": {"message_type": "USER", "has_attachments": False}})

    async def wait_closed(self):
        await self.closed.wait()

    async def close(self):
        self.store.identity["close_count"] = self.store.identity.get("close_count", 0) + 1
        self.store.save_identity()
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

    async def deletion_preflight(self, chat_id, message_ids, revoke):
        self.store.identity["delete_reads"] = self.store.identity.get("delete_reads", 0) + 1
        self.store.save_identity()
        if chat_id == 99 or 404 in message_ids:
            raise NotFound(kind="message")
        if revoke and 20 in message_ids:
            raise Forbidden("Synthetic incoming message")
        if self.scenario == "delete-generation":
            self.account.data["max"].generation += 1
        if self.scenario == "delete-read-error":
            raise ConnectionError("synthetic-private-delete-marker")
        if self.scenario == "delete-read-pending":
            await self.event("conversation", T.Conversation(id="10", kind="dm", title="Preflight pending"))
            await asyncio.Event().wait()

    async def delete(self, chat_id, message_ids, revoke):
        self.store.identity["delete_count"] = self.store.identity.get("delete_count", 0) + 1
        self.store.save_identity()
        if self.scenario == "delete-pending":
            await self.event("conversation", T.Conversation(id="10", kind="dm", title="Deletion dispatched"))
            await asyncio.Event().wait()
        if self.scenario == "delete-uncertain":
            raise ConnectionError("synthetic-private-delete-marker")
        if self.scenario == "delete-partial":
            raise PluginError("Synthetic partial acknowledgment")

    async def edit_preflight(self, chat_id, message_id):
        self.store.identity["edit_reads"] = self.store.identity.get("edit_reads", 0) + 1
        self.store.save_identity()
        if chat_id not in {0, 11}:
            raise NotSupported("Synthetic unsupported dialog")
        if message_id == 20:
            raise Forbidden("Synthetic incoming message")
        if self.scenario == "edit-read-error":
            raise ConnectionError("synthetic-private-edit-marker")
        if self.scenario == "edit-generation":
            self.account.data["max"].generation += 1
        if self.scenario == "edit-read-pending":
            await self.event("conversation", T.Conversation(id=str(chat_id), kind="dm", title="Edit preflight pending"))
            await asyncio.Event().wait()
        return SimpleNamespace(chat_id=chat_id, message_id=message_id)

    def edit(self, target, text):
        if self.scenario == "edit-prepare":
            raise Forbidden("Synthetic expired edit window")
        return self._edit(target, text)

    async def _edit(self, target, text):
        self.store.identity["edit_count"] = self.store.identity.get("edit_count", 0) + 1
        self.store.save_identity()
        if self.scenario == "edit-pending":
            await self.event("conversation", T.Conversation(id=str(target.chat_id), kind="dm", title="Edit dispatched"))
            await asyncio.Event().wait()
        if self.scenario == "edit-unknown":
            raise ConnectionError("synthetic-private-edit-marker")
        if self.scenario == "edit-post-generation":
            self.account.data["max"].generation += 1
        return T.Message(id=str(target.message_id), chat_id=str(target.chat_id), sender_id="1",
                         direction="out", sent_at=STAMP, text="wrong" if self.scenario == "edit-malformed" else text,
                         edited=True)


MaxPlugin(OfflineClient, retry_delay=0.01).run()
