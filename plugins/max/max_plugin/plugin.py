from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable

from contact_hub_sdk import (Account, AuthRequired, InvalidParams, NotSupported, Plugin,
                             PluginError, Unavailable, method)
from contact_hub_sdk import types as T
from contact_hub_sdk.wire import from_wire, to_wire

from .adapter import PyMaxAdapter
from .settings import Settings
from .state import PrivateState


def numeric_id(value: str, field: str) -> int:
    if not re.fullmatch(r"-?(0|[1-9][0-9]{0,19})", value):
        raise InvalidParams("MAX identifiers must be canonical integers", field=field)
    result = int(value)
    if not -(2**63) <= result < 2**63 or str(result) != value:
        raise InvalidParams("MAX identifier is out of range", field=field)
    return result


def failure(exc: Exception) -> PluginError:
    # Library exception messages can contain tokens, phone numbers or raw payloads.
    if isinstance(exc, PluginError):
        return exc
    if isinstance(exc, (ConnectionError, EOFError, TimeoutError, OSError)):
        return PluginError("MAX connection unavailable", code="NETWORK")
    if getattr(exc, "error", None) in {"FAIL_LOGIN_TOKEN", "FAIL_LOGOUT_ALL"}:
        return AuthRequired("MAX session was revoked; sign in again")
    return PluginError("MAX operation failed; restart sign-in or retry the account")


def correlated_confirmation(message: dict, client_id: str) -> dict:
    """Correlate only an authoritative confirmation with its exact request ID."""
    ext = dict(message.get("ext") or {})
    ext["max"] = {**(ext.get("max") or {}), "client_id": client_id}
    return {**message, "ext": ext}


@dataclass
class Runtime:
    store: PrivateState
    settings: Settings
    adapter: Any = None
    task: asyncio.Task | None = None
    ready: bool = False
    revoked: bool = False
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    auth_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    live: bool = False
    events: list = field(default_factory=list)
    generation: int = 0
    fatal: PluginError | None = None
    fatal_event: asyncio.Event = field(default_factory=asyncio.Event)


class MaxPlugin(Plugin):
    version = "0.1.0"
    # Required send/recent/auth have handlers. Full chat/history reconciliation
    # cannot be promised from timestamp-only library pages, so no optional caps.
    capabilities: dict[str, Any] = {"auth.interactive": True}

    def __init__(self, adapter_factory: Callable = PyMaxAdapter, *, retry_delay: float = 1):
        self.adapter_factory = adapter_factory
        self.retry_delay = retry_delay
        super().__init__()

    async def on_account_start(self, account: Account) -> None:
        if account.ghost:
            raise NotSupported("MAX ghost mode is not implemented")
        settings = Settings.parse(account.settings, account.secrets)
        try:
            store = PrivateState(account.state_dir, account.id)
        except BlockingIOError:
            raise InvalidParams("MAX state directory is already in use", field="state_dir") from None
        except PluginError:
            raise
        except Exception:
            raise InvalidParams("Cannot open MAX private state", field="state_dir") from None
        state = Runtime(store, settings)
        account.data["max"] = state
        state.task = account.spawn(self._background(account), name="max:connect")

    async def _close(self, state: Runtime) -> None:
        state.ready = False
        state.live = False
        state.events.clear()
        state.generation += 1
        adapter, state.adapter = state.adapter, None
        if adapter is not None:
            try:
                await asyncio.wait_for(adapter.close(), timeout=1)
            except (Exception, asyncio.CancelledError):
                # Cleanup is bounded. Session writes are already durable/atomic.
                pass

    async def _cancel_background(self, state: Runtime) -> None:
        task, state.task = state.task, None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self._close(state)

    async def on_account_stop(self, account: Account) -> None:
        state = account.data.get("max")
        if state is not None:
            await self._cancel_background(state)
            state.store.release()

    async def _event(self, account: Account, kind: str, data: Any, generation: int) -> None:
        state: Runtime = account.data["max"]
        if generation != state.generation:
            return
        if state.fatal is not None:
            return
        if not state.live:
            if len(state.events) >= 1000:
                # PyMax catches callback exceptions, so the owner must observe a
                # durable failure latch independently of the callback return.
                state.fatal = PluginError("MAX initial event buffer overflow; restart the account")
                state.fatal_event.set()
                return
            state.events.append((kind, data))
            return
        await self._deliver_event(account, kind, data)

    async def _deliver_event(self, account: Account, kind: str, data: Any) -> None:
        if kind == "message":
            await account.messages_upsert([data])
        elif kind == "conversation":
            await account.conversations_upsert([data])
        elif kind == "deleted":
            await account.messages_deleted(data[1], chat_id=data[0])

    async def _connect(self, account: Account, mode: str, interactive: bool) -> None:
        state: Runtime = account.data["max"]
        state.generation += 1
        generation = state.generation
        state.fatal = None
        state.fatal_event.clear()
        state.adapter = self.adapter_factory(account, state.store, state.settings,
                            lambda kind, data: self._event(account, kind, data, generation))
        await state.adapter.connect(mode, interactive)
        self._check_initial_events(state)
        me, contacts, chats, messages = await self._snapshot(state)
        self._check_initial_events(state)
        # Login data is a snapshot, not proof of complete enumeration. Never send
        # conversations.synced, which could delete chats missing from this page.
        for rows, emit in ((contacts, account.contacts_upsert), (chats, account.conversations_upsert),
                           (messages, account.messages_upsert)):
            for offset in range(0, len(rows), 100):
                self._check_initial_events(state)
                await emit(rows[offset:offset + 100], ack=True)
                self._check_initial_events(state)
        while state.events:
            self._check_initial_events(state)
            kind, data = state.events.pop(0)
            await self._deliver_event(account, kind, data)
        self._check_initial_events(state)
        state.live = True
        state.ready = True
        await account.status("connected", me=me)

    async def _snapshot(self, state: Runtime) -> tuple:
        # Discovery now contains paced server reads. Keep them owned by this
        # account task, and cancel immediately on the latched callback overflow.
        snapshot = asyncio.create_task(state.adapter.snapshot())
        overflow = asyncio.create_task(state.fatal_event.wait())
        try:
            await asyncio.wait({snapshot, overflow}, return_when=asyncio.FIRST_COMPLETED)
            self._check_initial_events(state)
            return await snapshot
        finally:
            for task in (snapshot, overflow):
                if not task.done():
                    task.cancel()
            await asyncio.gather(snapshot, overflow, return_exceptions=True)

    @staticmethod
    def _check_initial_events(state: Runtime) -> None:
        if state.fatal is not None:
            raise state.fatal

    async def _background(self, account: Account) -> None:
        state: Runtime = account.data["max"]
        delay, was_live = self.retry_delay, False
        try:
            if not state.store.has_session or state.revoked:
                await account.status("auth_required")
                return
            mode = state.store.identity.get("mode")
            if mode not in {"phone_code", "qr"}:
                await account.status("error", detail="MAX session mode is missing")
                return
            while True:
                await account.status("connecting", reconnecting=was_live)
                try:
                    await self._connect(account, mode, False)
                    was_live = True
                    delay = self.retry_delay
                    await state.adapter.wait_closed()
                    raise ConnectionError()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    error = failure(exc)
                    await self._close(state)
                    if error.code == "AUTH_REQUIRED":
                        state.revoked = True
                        await account.status("auth_required", detail=error.message)
                        return
                    if error.code != "NETWORK":
                        await account.status("error", detail=error.message)
                        return
                    await account.status("unavailable", detail=error.message, reconnecting=was_live)
                    await asyncio.sleep(delay)
                    delay = min(max(delay * 2, self.retry_delay), 60)
        finally:
            await self._close(state)

    @method("auth.begin")
    async def auth_begin(self, account: Account, params: T.AuthBeginParams) -> dict:
        state: Runtime = account.data["max"]
        if params.method not in {"phone_code", "qr"}:
            raise NotSupported("MAX supports phone code and QR sign-in")
        if state.ready:
            raise InvalidParams("MAX account is already connected", field="method")
        if state.store.has_session and not state.revoked:
            raise InvalidParams("A MAX session already exists; restart the account", field="method")
        account.auth.start(self.on_auth_begin(account, params.method))
        return {}

    @method("auth.cancel")
    async def auth_cancel(self, account: Account, params: T.AuthCancelParams) -> dict:
        if params.flow != "sign_in":
            raise NotSupported("MAX supports the sign_in auth flow only")
        if account.auth.running:
            account.auth.cancel()
            await self.on_auth_cancel(account, params.flow)
            await account.status("auth_required")
        return {}

    async def on_auth_begin(self, account: Account, mode: str) -> None:
        state: Runtime = account.data["max"]
        async with state.auth_lock:
            await self._authenticate(account, mode)

    async def _authenticate(self, account: Account, mode: str) -> None:
        state: Runtime = account.data["max"]
        if mode not in {"phone_code", "qr"}:
            raise NotSupported("MAX supports phone code and QR sign-in")
        if state.ready:
            return
        await self._cancel_background(state)
        if state.store.has_session:
            if not state.revoked:
                raise InvalidParams("A MAX session already exists; restart the account", field="method")
            data = state.store.read("session.json")
            await state.store.delete_session(data["token"])
        try:
            await account.status("connecting")
            await self._connect(account, mode, True)
            state.revoked = False
            await account.auth.done("Signed in to MAX")
            state.task = account.spawn(self._watch(account), name="max:watch")
        except asyncio.CancelledError:
            await self._close(state)
            raise
        except Exception as exc:
            await self._close(state)
            error = failure(exc)
            await account.status("auth_required", detail=error.message)

    async def _watch(self, account: Account) -> None:
        state: Runtime = account.data["max"]
        try:
            await state.adapter.wait_closed()
        finally:
            await self._close(state)
        await self._background(account)

    async def on_auth_cancel(self, account: Account, flow: str) -> None:
        if flow == "sign_in":
            state: Runtime = account.data["max"]
            async with state.auth_lock:
                await self._cancel_background(state)

    def _ready(self, account: Account) -> Runtime:
        state: Runtime = account.data["max"]
        if not state.ready:
            if account.state == "auth_required":
                raise AuthRequired()
            raise Unavailable()
        return state

    @method("messages.fetch_recent")
    async def recent(self, account: Account, params: T.MessagesFetchRecentParams) -> dict:
        state = self._ready(account)
        chat = numeric_id(params.chat_id, "chat_id")
        if params.topic_id is not None:
            raise NotSupported("MAX topic history is not implemented")
        if type(params.limit) is not int or not 1 <= params.limit <= 200:
            raise InvalidParams("Limit must be 1–200", field="limit")
        scope = hashlib.sha256((account.id + "\0" + params.chat_id).encode()).hexdigest()[:32]
        cursor = "max-recent-v1:" + scope
        if params.cursor is not None:
            if params.cursor != cursor:
                raise InvalidParams("Cursor does not belong to this MAX account and chat", field="cursor")
            raise NotSupported("MAX history continuation cannot be proven from timestamp-only pages; gap remains open")
        try:
            rows = await state.adapter.recent(chat, params.limit)
        except Exception as exc:
            raise failure(exc) from None
        if any(row.chat_id != params.chat_id for row in rows):
            raise PluginError("MAX history returned a different conversation")
        unique = {row.id: row for row in rows}
        rows = sorted(unique.values(), key=lambda m: (m.sent_at, int(m.id)), reverse=True)[:params.limit]
        # A short/empty server page does not establish exhaustion in this API.
        # This token always fails explicitly, including after a plugin restart.
        return {"messages": rows, "next_cursor": cursor}

    @method("messages.send")
    async def send(self, account: Account, params: T.MessagesSendParams) -> dict:
        state = self._ready(account)
        chat = numeric_id(params.chat_id, "chat_id")
        if params.topic_id is not None:
            raise NotSupported("MAX topic sending is not implemented")
        if not params.text:
            raise InvalidParams("Text must not be empty", field="text")
        reply = numeric_id(params.reply_to.message_id, "reply_to.message_id") if params.reply_to else None
        payload = {"chat": chat, "text": params.text, "reply": reply}
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        key = hashlib.sha256(params.client_id.encode()).hexdigest() if params.client_id is not None else None
        async with state.send_lock:
            if key is not None:
                previous = state.store.ledger.get(key)
                if previous:
                    if previous["digest"] != digest:
                        raise InvalidParams("client_id was already used with another message", field="client_id")
                    if previous["outcome"] == "confirmed":
                        # A durable, payload-matching success is authoritative even
                        # when its original RPC reply was lost. Older journal rows
                        # gain correlation from this exact keyed retry, never from
                        # content/time matching against unrelated platform events.
                        wire = correlated_confirmation(previous["message"], params.client_id)
                        if wire != previous["message"]:
                            previous["message"] = wire
                            state.store.write("sends.json", state.store.ledger)
                        await account.messages_upsert([from_wire(T.Message, wire)])
                        return {"message": wire}
                    raise PluginError("Previous MAX send outcome is unknown; automatic retry is unsafe",
                                      code="NETWORK", outcome="unknown", retry_safe=False)
                if len(state.store.ledger) >= 10000:
                    raise NotSupported("MAX confirmation journal is full; automatic eviction is unsafe")
                state.store.ledger[key] = {"digest": digest, "outcome": "unknown"}
                state.store.write("sends.json", state.store.ledger)
            try:
                result = await state.adapter.send(chat, params.text, reply)
                wire = to_wire(result)
                if wire.get("chat_id") != params.chat_id or wire.get("direction") != "out":
                    raise PluginError("MAX did not confirm the outgoing message")
                if params.client_id is not None:
                    wire = correlated_confirmation(wire, params.client_id)
                    result = from_wire(T.Message, wire)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise PluginError("MAX send outcome is unknown; automatic retry is unsafe",
                                  code="NETWORK", outcome="unknown", retry_safe=False) from None
            if key is not None:
                state.store.ledger[key] = {"digest": digest, "outcome": "confirmed", "message": wire}
                state.store.write("sends.json", state.store.ledger)
            await account.messages_upsert([result])
            return {"message": result}
