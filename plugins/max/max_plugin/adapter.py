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

from contact_hub_sdk import AuthRequired, Forbidden, NotFound, NotSupported, PluginError, Unavailable

from . import mapping
from .discovery import drain_chat_pages, marker
from .editing import EditPolicy, EditTarget, plain_message, writable_chat


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
    def __init__(self, account: Any, store: Any, settings: Any, event: Any, *, sleep=asyncio.sleep, clock=time.time):
        self.account, self.store, self.settings, self.event = account, store, settings, event
        self.client: Any = None
        self.me: int | None = None
        self._fatal: PluginError | None = None
        self._sleep = sleep
        self._login_observed = asyncio.Event()
        self._chat_marker: int | None = None
        self.edit_policy = EditPolicy(clock)
        self._editing_active = False

    async def connect(self, mode: str, interactive: bool) -> None:
        from pymax import Client, WebClient
        from pymax.config import ExtraConfig
        from pymax.api.session.payloads import MobileUserAgentPayload
        from pymax.types.domain.sync import SyncOverrides
        from pymax.protocol import Command, Opcode
        self.edit_policy.reset()
        self._editing_active = True
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
            if not self._editing_active:
                return
            if frame.opcode in {Opcode.LOGIN, Opcode.LOGIN2} and frame.cmd == Command.RESPONSE:
                config = frame.payload.get("config") if isinstance(frame.payload, dict) else None
                if config is not None:
                    self.edit_policy.observe(config)
                elif not isinstance(frame.payload, dict) or "config" in frame.payload:
                    self.edit_policy.reset()
            elif frame.opcode == Opcode.NOTIF_CONFIG and frame.cmd in {Command.REQUEST, Command.EVENT}:
                config = frame.payload.get("config") if isinstance(frame.payload, dict) else None
                self.edit_policy.observe(config, patch=True)
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
        self._editing_active = False
        self.edit_policy.reset()
        if self.client is not None:
            await self.client.close()

    async def edit_preflight(self, chat_id: int, message_id: int) -> EditTarget:
        from pymax.api.chats.payloads import GetChatInfoPayload
        from pymax.api.messages.payloads import GetMessagesPayload
        from pymax.api.response import require_payload_dict
        from pymax.protocol import Opcode
        from pymax.types.domain.chat import Chat
        from pymax.types.domain.message import Message

        if type(self.me) is not int:
            raise AuthRequired("MAX account identity is unavailable")
        if not self._editing_active:
            raise Unavailable("MAX edit connection is unavailable")
        generation = self.edit_policy.check()
        response = await self.client._app.invoke(Opcode.MSG_GET,
                        GetMessagesPayload(chat_id=chat_id, message_ids=[message_id]).to_payload())
        payload = require_payload_dict(response)
        if "chatId" in payload and response_id(payload["chatId"]) != chat_id or "postId" in payload:
            raise PluginError("MAX edit preflight contradicted the requested conversation")
        rows = payload.get("messages")
        if rows == []:
            raise NotFound(kind="message")
        if not isinstance(rows, list) or len(rows) != 1:
            raise PluginError("MAX edit preflight did not return one message")
        row = rows[0]
        plain_message(row, chat_id, message_id, self.me)
        Message.model_validate(row)
        response = await self.client._app.invoke(Opcode.CHAT_INFO,
                        GetChatInfoPayload(chat_ids=[chat_id]).to_payload())
        payload = require_payload_dict(response)
        if "chatId" in payload and response_id(payload["chatId"]) != chat_id or "postId" in payload:
            raise PluginError("MAX edit conversation preflight contradicted the requested conversation")
        chats = payload.get("chats")
        if not isinstance(chats, list) or len(chats) != 1:
            raise PluginError("MAX edit preflight did not return one conversation")
        writable_chat(chats[0], chat_id, self.me)
        Chat.model_validate(chats[0])
        self.edit_policy.check(row["time"], generation)
        return EditTarget(chat_id, message_id, row["time"], generation)

    def edit(self, target: EditTarget, text: str) -> Any:
        from pymax.api.messages.payloads import EditMessagePayload

        # These checks run before invoke can write. There is no await between
        # the last policy/age check and entering the single transport request.
        if not self._editing_active:
            raise Unavailable("MAX edit connection is unavailable")
        self.edit_policy.check(target.sent_at, target.policy_generation)
        payload = EditMessagePayload(chat_id=target.chat_id, message_id=target.message_id,
                                     text=text, elements=[]).to_payload()
        payload.pop("attachments", None)
        return self._edit(target, text, payload)

    async def _edit(self, target: EditTarget, text: str, payload: dict) -> Any:
        from pymax.api.response import require_payload_dict
        from pymax.protocol import Opcode
        from pymax.types.domain.message import Message

        response = await self.client._app.invoke(Opcode.MSG_EDIT, payload)
        payload = require_payload_dict(response)
        if "chatId" in payload and response_id(payload["chatId"]) != target.chat_id or "postId" in payload:
            raise PluginError("MAX edit acknowledgment contradicted the requested conversation")
        row = payload.get("message")
        plain_message(row, target.chat_id, target.message_id, self.me, edited=True)
        if row["text"] != text or row["time"] != target.sent_at:
            raise PluginError("MAX edit acknowledgment did not confirm the requested text")
        if not self._editing_active:
            raise Unavailable("MAX edit connection changed")
        self.edit_policy.check(generation=target.policy_generation)
        return mapping.message(Message.model_validate(row), self.me, target.chat_id)

    async def recent(self, chat_id: int, limit: int) -> list:
        rows = await self.client.fetch_history(chat_id, backward=limit)
        return [mapping.message(row, self.me, chat_id) for row in rows]

    async def send(self, chat_id: int, text: str, reply_to: int | None) -> Any:
        # send_message() interprets Markdown and generates a hidden cid. Use its
        # actual payload models + same invoke/response path for literal plain text.
        # Exactly one request. cid is NOT claimed to provide server deduplication.
        from pymax.api.messages.payloads import ReplyLink, SendMessagePayload, SendMessagePayloadMessage
        from pymax.api.response import require_payload_dict, require_payload_model
        from pymax.protocol import Opcode
        from pymax.types.domain.message import Message
        if reply_to is not None and type(self.me) is not int:
            raise AuthRequired("MAX account identity is unavailable")
        cid = max(int(time.time() * 1000), self.store.identity.get("last_cid", 0) + 1)
        self.store.identity["last_cid"] = cid
        self.store.save_identity()
        payload = SendMessagePayload(chat_id=chat_id, notify=True,
                    message=SendMessagePayloadMessage(text=text, cid=cid, elements=[], attaches=[],
                              link=ReplyLink(message_id=reply_to) if reply_to is not None else None))
        response = await self.client._app.invoke(Opcode.MSG_SEND, payload.to_payload())
        if reply_to is not None:
            validate_reply_ack(require_payload_dict(response), chat_id, reply_to, self.me, cid)
        raw = require_payload_model(response, Message)
        result = mapping.message(raw, self.me, chat_id)
        if result.chat_id != str(chat_id) or raw.sender != self.me:
            raise PluginError("MAX send response did not confirm this conversation and sender")
        return result

    async def deletion_preflight(self, chat_id: int, message_ids: list[int], revoke: bool) -> None:
        from pymax.api.messages.payloads import GetMessagesPayload
        from pymax.api.response import require_payload_dict
        from pymax.protocol import Opcode
        from pymax.types.domain.message import Message
        if type(self.me) is not int:
            raise AuthRequired("MAX account identity is unavailable")
        response = await self.client._app.invoke(Opcode.MSG_GET,
                    GetMessagesPayload(chat_id=chat_id, message_ids=message_ids).to_payload())
        payload = require_payload_dict(response)
        if "chatId" in payload and response_id(payload["chatId"]) != chat_id:
            raise PluginError("MAX deletion preflight returned another conversation")
        if "postId" in payload:
            raise NotSupported("MAX comment deletion is not implemented")
        rows = payload.get("messages")
        if not isinstance(rows, list) or len(rows) > len(message_ids):
            raise PluginError("MAX returned invalid deletion preflight data")
        found = set()
        for row in rows:
            if not isinstance(row, dict):
                raise PluginError("MAX returned invalid deletion preflight data")
            response_id(row.get("id"))
            if row.get("chatId") is not None:
                response_id(row["chatId"])
            if row.get("sender") is not None:
                response_id(row["sender"])
            raw = Message.model_validate(row)
            # The actual MSG_GET request supplies parent context when chatId is
            # absent, just as the pinned wrapper does. Contradictions never move
            # an otherwise valid message into the requested conversation.
            if raw.id not in message_ids or raw.id in found or raw.chat_id not in {None, chat_id}:
                raise PluginError("MAX deletion preflight contradicted the requested messages")
            if raw.sender is None or raw.status == "REMOVED":
                raise NotFound(kind="message")
            if raw.type != "USER" or raw.post_id is not None or raw.delayed_attributes is not None:
                raise NotSupported("MAX deletion of comments, scheduled or service messages is not implemented")
            if revoke and raw.sender != self.me:
                raise Forbidden("MAX deletion for everyone requires an outgoing message")
            found.add(raw.id)
        if found != set(message_ids):
            raise NotFound(kind="message")

    async def delete(self, chat_id: int, message_ids: list[int], revoke: bool) -> None:
        from pymax.api.messages.payloads import DeleteMessagePayload
        from pymax.api.response import require_payload_dict
        from pymax.protocol import Opcode
        response = await self.client._app.invoke(Opcode.MSG_DELETE,
                    DeleteMessagePayload(chat_id=chat_id, message_ids=message_ids,
                                         for_me=not revoke).to_payload())
        payload = require_payload_dict(response)
        # The pinned convenience wrapper discards this response and always returns
        # True. The first-party command-66 ack instead identifies chat/message IDs;
        # require the whole selected set before returning protocol Empty success.
        rows = payload.get("messageIds")
        if (response_id(payload.get("chatId")) != chat_id or "postId" in payload
                or not isinstance(rows, list) or not rows or len(rows) > 200
                or {response_id(value) for value in rows} != set(message_ids)):
            raise PluginError("MAX deletion acknowledgment did not confirm the selected messages")


def validate_reply_ack(row: Any, chat_id: int, reply_to: int, me: int, cid: int) -> None:
    """Validate observed wire relation before models can coerce/omit its fields."""
    if not isinstance(row, dict):
        raise PluginError("MAX did not confirm the sent reply")
    response_id(row.get("id"))
    if (any(key in row for key in ("chat_id", "post_id", "delayed_attributes"))
            or row.get("chatId") is not None and response_id(row["chatId"]) != chat_id
            or response_id(row.get("sender")) != me
            or row.get("type") != "USER" or row.get("postId") is not None
            or row.get("delayedAttributes") is not None or row.get("status") == "REMOVED"
            or row.get("cid") is not None and response_id(row["cid"]) != cid):
        raise PluginError("MAX did not confirm the sent reply scope")
    link = row.get("link")
    if (not isinstance(link, dict) or link.get("type") != "REPLY" or "chat_id" in link
            or response_id(link.get("chatId")) != chat_id):
        raise PluginError("MAX did not confirm the reply conversation")
    source = link.get("message")
    if (not isinstance(source, dict) or response_id(source.get("id")) != reply_to
            or any(key in source for key in ("chat_id", "post_id", "delayed_attributes"))
            or source.get("chatId") is not None and response_id(source["chatId"]) != chat_id
            or source.get("postId") is not None or source.get("delayedAttributes") is not None
            or source.get("status") == "REMOVED"):
        raise PluginError("MAX did not confirm the reply message")


def response_id(value: Any) -> int:
    """Validate int64 wire IDs without accepting bool/float coercion."""
    if type(value) is int and -(2**63) <= value < 2**63:
        return value
    if isinstance(value, str) and re.fullmatch(r"-?(0|[1-9][0-9]{0,18})", value):
        number = int(value)
        if -(2**63) <= number < 2**63 and str(number) == value:
            return number
    raise PluginError("MAX returned an invalid message identity")
