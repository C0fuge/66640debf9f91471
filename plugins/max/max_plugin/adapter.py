"""The only module coupled to pinned PyMax internals.

Imports are lazy so offline injected adapters never load PyMax. No console
providers, token/env discovery, built-in reconnect or automatic relogin are used.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from datetime import UTC, datetime
from typing import Any

from contact_hub_sdk import AuthRequired, PluginError

from . import mapping
from .discovery import drain_chat_pages, marker


class InteractiveAuth:
    def __init__(self, account: Any, mode: str):
        self.account = account
        self.mode = mode

    async def authenticate(self, app: Any) -> Any:
        from pymax.auth.models import AuthResult
        from pymax.exceptions import ApiError
        if self.mode == "phone_code":
            start = await app.api.auth.request_code(app.config.phone)
            if not 1 <= start.code_length <= 32:
                raise AuthRequired("MAX returned an invalid verification code length")
            for attempt in range(3):
                code = await self.account.auth.ask("code", title="MAX verification code",
                            min_length=start.code_length, max_length=start.code_length,
                            error="Code not accepted" if attempt else None)
                self.account.plugin._redact.add(code)
                try:
                    result = await app.api.auth.send_code(start.token, code)
                    break
                except ApiError:
                    if attempt == 2:
                        raise AuthRequired("MAX code not accepted; start sign-in again") from None
        else:
            qr = await app.api.auth.request_qr()
            await self.account.auth.show("qr", title="Scan in MAX", data=qr.qr_link,
                                         expires_at=datetime.fromtimestamp(qr.expires_at / 1000, UTC))
            # The pinned source defines polling_interval and expires_at in ms.
            if qr.polling_interval <= 0:
                raise AuthRequired("MAX returned an invalid QR polling interval")
            interval = max(0.5, qr.polling_interval / 1000)
            while time.time() < qr.expires_at / 1000:
                status = await app.api.auth.check_qr(qr.track_id)
                if status.status.login_available:
                    break
                await asyncio.sleep(min(interval, max(0, qr.expires_at / 1000 - time.time())))
            else:
                raise AuthRequired("MAX QR code expired; start sign-in again")
            result = await app.api.auth.confirm_qr(qr.track_id)
        token = result.login_token
        if not token and result.password_challenge:
            challenge = result.password_challenge
            for attempt in range(3):
                password = await self.account.auth.ask("password", title="MAX two-step password",
                                                       hint=challenge.hint,
                                                       error="Password not accepted" if attempt else None)
                self.account.plugin._redact.add(password)
                try:
                    response = await app.api.auth.check_password(challenge.track_id, password)
                except ApiError:
                    continue
                if response.login_token and not response.error:
                    token = response.login_token
                    break
            if not token:
                raise AuthRequired("MAX password not accepted; start sign-in again")
        if not token:
            raise AuthRequired("MAX did not issue a login token; account registration is unsupported")
        self.account.plugin._redact.add(token)
        return AuthResult(token=token)


class PyMaxAdapter:
    def __init__(self, account: Any, store: Any, settings: Any, event: Any, *, sleep=asyncio.sleep):
        self.account, self.store, self.settings, self.event = account, store, settings, event
        self.client: Any = None
        self.me: int | None = None
        self._fatal: PluginError | None = None
        self._sleep = sleep
        self._login_observed = asyncio.Event()
        self._chat_marker: int | None = None

    async def connect(self, mode: str, interactive: bool) -> None:
        from pymax import Client, WebClient
        from pymax.config import ExtraConfig
        from pymax.api.session.payloads import MobileUserAgentPayload
        from pymax.types.domain.sync import SyncOverrides
        from pymax.protocol import Command, Opcode
        session = await self.store.load_session()
        if session:
            self.account.plugin._redact.add(session.token)
            phone = session.phone
        elif interactive and mode == "phone_code":
            phone = ""
            while not re.fullmatch(r"\+[1-9][0-9]{6,14}", phone):
                phone = await self.account.auth.ask("phone", title="MAX phone number",
                         placeholder="+70000000000", error="Use an international phone number" if phone else None)
            self.account.plugin._redact.add(phone)
        elif not interactive:
            raise AuthRequired()
        else:
            phone = ""
        # Raw debug logging in App.invoke contains full tokens/messages. Suppress
        # the entire library logger independently of core's requested log level.
        library_log = logging.getLogger("pymax")
        library_log.handlers = [logging.NullHandler()]
        library_log.propagate = False
        extra = ExtraConfig(store=self.store, persist_session=True, reconnect=False, relogin=False,
                            device_id=self.store.identity["device_id"],
                            mt_instance_id=self.store.identity["mt_instance_id"],
                            proxy=self.settings.proxy, request_timeout=self.settings.request_timeout,
                            telemetry=False, log_level="CRITICAL", password_max_attempts=3,
                            sync=SyncOverrides(chats_sync=-1, contacts_sync=-1))
        stored_agent = self.store.identity.get("user_agent")
        if self.store.identity.get("mode") == mode and stored_agent:
            extra.user_agent = MobileUserAgentPayload.model_validate(stored_agent)
        flow = InteractiveAuth(self.account, mode)
        kwargs = {"work_dir": str(self.store.directory), "extra_config": extra, "auth_flow": flow}
        self.client = WebClient(**kwargs) if mode == "qr" else Client(phone=phone, **kwargs)
        library_log.handlers = [logging.NullHandler()]
        library_log.propagate = False
        library_log.setLevel(logging.CRITICAL + 1)
        # Resolve the bundled profile before network I/O, persist it also for failed
        # auth/reconnect attempts. This private API is fenced by the exact pin.
        await self.client._ensure_runtime()
        self.store.identity.update(mode=mode,
                                   user_agent=self.client._config.device.user_agent.model_dump(mode="json"))
        self.store.save_identity()
        # Register before connect: notifications may arrive while login is in flight.
        pending: list[tuple[str, Any]] = []

        async def deliver(kind: str, raw: Any) -> None:
            if self._fatal is not None:
                return
            if self.me is None:
                if len(pending) >= 1000:
                    # The upstream connection catches/logs handler errors. Latch
                    # overflow for the connect owner instead of trusting a raise.
                    self._fatal = PluginError("MAX login event buffer overflow")
                    return
                pending.append((kind, raw))
                return
            if kind == "message":
                data = mapping.message(raw, self.me)
            elif kind == "conversation":
                data = mapping.conversation(raw, self.me)
            else:
                data = (str(raw.chat_id), [str(mid) for mid in raw.message_ids])
            await self.event(kind, data)

        async def on_message(raw: Any, client: Any) -> None:
            await deliver("message", raw)

        async def on_chat(raw: Any, client: Any) -> None:
            await deliver("conversation", raw)

        async def on_delete(raw: Any, client: Any) -> None:
            await deliver("deleted", raw)

        async def on_raw(frame: Any, client: Any) -> None:
            # Public pinned hook receives matched response frames too. App.start
            # consumes the LOGIN model but otherwise discards this metadata.
            if frame.opcode != Opcode.LOGIN or frame.cmd != Command.RESPONSE:
                return
            if self._login_observed.is_set():
                self._fatal = PluginError("MAX returned duplicate LOGIN discovery metadata")
                return
            if not isinstance(frame.payload, dict):
                self._fatal = PluginError("MAX returned invalid LOGIN discovery metadata")
            elif "chatMarker" in frame.payload:
                try:
                    self._chat_marker = marker(frame.payload["chatMarker"])
                except PluginError as exc:
                    self._fatal = exc
            # Absent is explicitly unknown. It must not be changed into zero.
            self._login_observed.set()

        self.client.on_raw()(on_raw)
        self.client.on_message()(on_message)
        self.client.on_message_edit()(on_message)
        self.client.on_chat_update()(on_chat)
        self.client.on_message_delete()(on_delete)
        await self.client.connect()
        if self._fatal is not None:
            await self.close()
            raise self._fatal
        try:
            await asyncio.wait_for(self._login_observed.wait(), timeout=self.settings.request_timeout)
        except TimeoutError:
            raise PluginError("MAX LOGIN discovery metadata was not observed") from None
        if self._fatal is not None:
            raise self._fatal
        if self.client.me is None:
            raise AuthRequired("MAX login returned no profile")
        self.me = self.client.me.contact.id
        for kind, raw in pending:
            await deliver(kind, raw)

    async def snapshot(self) -> tuple[Any, list, list, list]:
        me = mapping.contact(self.client.me.contact)
        contacts = [mapping.contact(u) for u in self.client.contacts if u is not None]
        raw_chats = await drain_chat_pages(self.client._app.invoke, list(self.client.chats or []),
                                           self._chat_marker, sleep=self._sleep)
        if self._fatal is not None:
            raise self._fatal
        chats = [mapping.conversation(chat, self.me) for chat in raw_chats]
        messages = {(str(cid), str(raw.id)): mapping.message(raw, self.me, cid)
                    for cid, rows in (self.client.messages or {}).items() for raw in rows
                    if raw.sender is not None}
        for chat in raw_chats:
            raw = chat.last_message
            if raw is not None and raw.sender is not None:
                messages[(str(chat.id), str(raw.id))] = mapping.message(raw, self.me, chat.id)
        return me, contacts, chats, list(messages.values())

    async def wait_closed(self) -> None:
        await self.client._connection.wait_closed()

    async def close(self) -> None:
        if self.client is not None:
            await self.client.close()

    async def recent(self, chat_id: int, limit: int) -> list:
        rows = await self.client.fetch_history(chat_id, backward=limit)
        return [mapping.message(row, self.me, chat_id) for row in rows]

    async def send(self, chat_id: int, text: str, reply_to: int | None) -> Any:
        # send_message() interprets Markdown and generates a hidden cid. Use its
        # actual payload models + same invoke/response path for literal plain text.
        # Exactly one request. cid is NOT claimed to provide server deduplication.
        from pymax.api.messages.payloads import ReplyLink, SendMessagePayload, SendMessagePayloadMessage
        from pymax.api.response import require_payload_model
        from pymax.protocol import Opcode
        from pymax.types.domain.message import Message
        cid = max(int(time.time() * 1000), self.store.identity.get("last_cid", 0) + 1)
        self.store.identity["last_cid"] = cid
        self.store.save_identity()
        payload = SendMessagePayload(chat_id=chat_id, notify=True,
                    message=SendMessagePayloadMessage(text=text, cid=cid, elements=[], attaches=[],
                              link=ReplyLink(message_id=reply_to) if reply_to is not None else None))
        response = await self.client._app.invoke(Opcode.MSG_SEND, payload.to_payload())
        raw = require_payload_model(response, Message)
        result = mapping.message(raw, self.me, chat_id)
        if result.chat_id != str(chat_id) or raw.sender != self.me:
            raise PluginError("MAX send response did not confirm this conversation and sender")
        return result
