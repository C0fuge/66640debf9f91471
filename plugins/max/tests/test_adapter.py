"""Actual pinned model/payload checks, with injected transport and synthetic data."""
import asyncio
from types import SimpleNamespace

import pytest
from contact_hub_sdk import PluginError
from contact_hub_sdk.wire import to_wire
from pymax.protocol import Opcode
from pymax.types.domain.chat import Chat
from pymax.types.domain.message import Message
from pymax.types.domain.user import User

from max_plugin import mapping
from max_plugin.adapter import InteractiveAuth, PyMaxAdapter
from max_plugin.settings import Settings
from max_plugin.state import PrivateState


def raw_message(**fields):
    return Message(id=20, chat_id=10, sender=1, time=1767225600123, type="USER", text="literal", **fields)


def test_actual_models_map_without_guessing_absent_sender():
    user = User(id=1, names=[{"name": "Synthetic", "firstName": "Synthetic"}])
    assert mapping.contact(user).display_name == "Synthetic"
    chat = Chat(id=10, type="DIALOG", status="ACTIVE", owner=1, participants={1: 0, 2: 0})
    assert to_wire(mapping.conversation(chat, 1))["kind"] == "dm"
    assert mapping.conversation(chat, 1).peer_user_id == "2"
    wire = to_wire(mapping.message(raw_message(), 1))
    assert wire["direction"] == "out" and wire["sent_at"] == "2026-01-01T00:00:00.123Z"
    with pytest.raises(PluginError):
        mapping.message(raw_message().model_copy(update={"sender": None}), 1)


def test_known_parent_chat_is_validated_and_missing_identity_uses_parent():
    raw = raw_message()
    with pytest.raises(PluginError, match="contradicts its parent conversation"):
        mapping.message(raw, 1, 99)
    missing = raw.model_copy(update={"chat_id": None})
    assert mapping.message(missing, 1, 41).chat_id == "41"
    # A callback without a parent uses the message's own explicit identity.
    assert mapping.message(raw, 1).chat_id == "10"


def test_literal_text_send_uses_one_pinned_payload_and_no_markdown_parser(tmp_path):
    async def run():
        store = PrivateState(tmp_path / "state", "a")
        calls = []
        text = "**literal** [text](https://example.invalid) `code`"

        async def invoke(opcode, payload):
            calls.append((opcode, payload))
            return SimpleNamespace(payload=raw_message().model_copy(update={"text": text}).model_dump(by_alias=True))

        adapter = PyMaxAdapter(None, store, Settings(30, None), None)
        adapter.me = 1
        adapter.client = SimpleNamespace(_app=SimpleNamespace(invoke=invoke))
        sent = await adapter.send(10, text, 19)
        assert sent.text == text
        assert len(calls) == 1 and calls[0][0] == Opcode.MSG_SEND
        payload = calls[0][1]
        assert payload["message"]["text"] == text
        assert payload["message"]["elements"] == [] and payload["message"]["attaches"] == []
        assert payload["message"]["link"]["messageId"] == 19
        assert payload["notify"] is True
        assert store.identity["last_cid"] == payload["message"]["cid"]
        store.release()
    asyncio.run(run())


class Flow:
    def __init__(self, answers):
        self.answers = iter(answers)
        self.steps = []

    async def ask(self, kind, **fields):
        self.steps.append((kind, fields))
        return next(self.answers)

    async def show(self, kind, **fields):
        self.steps.append((kind, fields))


def account(flow):
    return SimpleNamespace(auth=flow, plugin=SimpleNamespace(_redact=SimpleNamespace(add=lambda _: None)))


def test_actual_phone_code_password_challenge_models():
    from pymax.types.domain.auth import CheckCodeResponse, CheckPasswordResponse, StartAuthResponse
    async def run():
        flow = Flow(["synthetic-code", "bad", "synthetic-password"])
        requests = []

        async def request_code(phone):
            requests.append(phone)
            return StartAuthResponse(token="challenge", codeLength=6, requestMaxDuration=60,
                                     requestCountLeft=2, altActionDuration=30)

        async def send_code(token, code):
            assert (token, code) == ("challenge", "synthetic-code")
            return CheckCodeResponse(passwordChallenge={"trackId": "track", "hint": "Synthetic hint"})

        async def password(track, value):
            assert track == "track"
            return (CheckPasswordResponse(error="rejected") if value == "bad" else
                    CheckPasswordResponse(tokenAttrs={"LOGIN": {"token": "synthetic-login"}}))

        app = SimpleNamespace(config=SimpleNamespace(phone="+15550000001"), api=SimpleNamespace(auth=
                   SimpleNamespace(request_code=request_code, send_code=send_code, check_password=password)))
        result = await InteractiveAuth(account(flow), "phone_code").authenticate(app)
        assert result.token == "synthetic-login" and requests == ["+15550000001"]
        assert [kind for kind, _ in flow.steps] == ["code", "password", "password"]
        assert flow.steps[-1][1]["error"] == "Password not accepted"
    asyncio.run(run())


def test_actual_qr_models_have_millisecond_expiry_and_no_console_output(capsys):
    from pymax.types.domain.auth import CheckCodeResponse, CheckQrResponse, RequestQrResponse
    import time
    async def run():
        flow = Flow([])
        calls = []

        async def request_qr():
            return RequestQrResponse(expiresAt=int(time.time() * 1000) + 5000, pollingInterval=1000,
                                     qrLink="max://synthetic", trackId="track", ttl=5000)

        async def check_qr(track):
            calls.append(("check", track))
            return CheckQrResponse(status={"expiresAt": int(time.time() * 1000) + 5000,
                                           "loginAvailable": True})

        async def confirm_qr(track):
            calls.append(("confirm", track))
            return CheckCodeResponse(tokenAttrs={"LOGIN": {"token": "synthetic-login"}})

        app = SimpleNamespace(api=SimpleNamespace(auth=SimpleNamespace(request_qr=request_qr,
                                  check_qr=check_qr, confirm_qr=confirm_qr)))
        result = await InteractiveAuth(account(flow), "qr").authenticate(app)
        assert result.token == "synthetic-login"
        assert calls == [("check", "track"), ("confirm", "track")]
        assert flow.steps[0][0] == "qr" and flow.steps[0][1]["data"] == "max://synthetic"
        assert flow.steps[0][1]["expires_at"].year > 2020
    asyncio.run(run())
    assert capsys.readouterr().out == ""


def test_login_buffer_overflow_survives_real_upstream_dispatch_exception_catch(tmp_path, monkeypatch):
    import pymax
    from pymax.connection import ConnectionManager
    from pymax.protocol import Command

    created = []

    class CatchingClient:
        def __init__(self, *, extra_config, **kwargs):
            created.append(self)
            self.handlers = {}
            self.closed = False
            self.me = None
            self._config = SimpleNamespace(device=SimpleNamespace(
                user_agent=extra_config.generate_web_user_agent()))

        async def _ensure_runtime(self):
            pass

        def register(self, name):
            def decorator(callback):
                self.handlers[name] = callback
                return callback
            return decorator

        def on_raw(self):
            return self.register("raw")

        def on_message(self):
            return self.register("message")

        def on_message_edit(self):
            return self.register("edit")

        def on_chat_update(self):
            return self.register("chat")

        def on_message_delete(self):
            return self.register("delete")

        async def connect(self):
            connection = ConnectionManager.__new__(ConnectionManager)

            async def on_event(frame):
                await self.handlers["message"](raw_message(), self)

            connection.on_event = on_event
            frame = SimpleNamespace(opcode=Opcode.NOTIF_MESSAGE, cmd=Command.REQUEST, seq=1)
            for _ in range(1001):
                # Actual pinned method catches every callback exception and returns.
                await connection._dispatch_event(frame)
            self.me = SimpleNamespace(contact=User(id=1))

        async def close(self):
            self.closed = True

    monkeypatch.setattr(pymax, "WebClient", CatchingClient)

    async def run():
        store = PrivateState(tmp_path / "state", "a")
        adapter = PyMaxAdapter(account(Flow([])), store, Settings(30, None), None)
        with pytest.raises(PluginError, match="login event buffer overflow"):
            await adapter.connect("qr", True)
        assert created[0].closed
        assert adapter.me is None
        store.release()

    asyncio.run(run())
