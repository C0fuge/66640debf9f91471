"""The plugin base class: lifecycle, dispatch, accounts, events and interactive auth."""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import json
import logging
import os
import re
import sys
import time
from collections.abc import Awaitable, Callable, Coroutine
from datetime import datetime
from pathlib import Path
from typing import Any, TypeVar

from . import types as T
from ._emitters import EventEmitters
from .errors import (
    InvalidParams,
    MethodNotFound,
    NotSupported,
    PluginError,
    UnknownAccount,
)
from .registry import API_VERSION, DEFAULT_TIMEOUT_SECONDS, EVENTS, METHODS
from .transport import Connection, RequestContext, _current, current_request, open_stdio
from .wire import DecodeError, from_wire, to_wire

__all__ = ["Account", "AuthFlow", "Plugin", "method", "progress"]

log = logging.getLogger("contact_hub_sdk")
F = TypeVar("F", bound=Callable[..., Any])

#: Methods the SDK always serves itself (the plugin gets hooks instead).
LIFECYCLE = frozenset({"initialize", "shutdown", "ping", "account.start", "account.stop"})
PROCESS_SCOPED = frozenset({"initialize", "shutdown", "ping"})
MAX_ACK_IN_FLIGHT = 4  # spec 16
PROGRESS_INTERVAL = 0.25  # seconds between transfer.progress frames (spec asks >= every 500 ms)

#: Vendor method names (spec 12.3): ``x-<plugin-id>.<name>``.
VENDOR_NAME = re.compile(r"^x-([a-z][a-z0-9_]{1,31})\.")
EOF_BUDGET = 3.0  # seconds to finish in-flight requests on stdin EOF (spec 2.5)


def method(name: str) -> Callable[[F], F]:
    """Mark a Plugin method as the handler for request ``name``.

    The handler is called as ``handler(self, account, params)`` and may be ``async``
    (preferred) or a plain function (run in a worker thread). ``params`` is the typed
    params dataclass from :mod:`contact_hub_sdk.types` (a ``dict`` for methods outside
    the registry); ``account`` is the started :class:`Account` (``None`` for
    process-scoped vendor methods). Return the result dataclass, a ``dict`` or ``None``
    (= ``{}``). Raise :class:`~contact_hub_sdk.errors.PluginError` subclasses for errors.

    A name outside the registry must be a vendor method, ``x-<plugin-id>.<name>``
    (spec 12.3); anything else raises ``ValueError`` here.
    """
    if name not in METHODS and not VENDOR_NAME.match(name):
        raise ValueError(
            f"@method({name!r}): {name!r} is not a method of plugin API {API_VERSION}; "
            "vendor methods are named 'x-<plugin-id>.<name>' (spec 12.3)"
        )

    def deco(fn: F) -> F:
        fn.__ch_method__ = name  # type: ignore[attr-defined]
        return fn

    return deco


async def progress(done_bytes: int, total_bytes: int | None = None, **kw: Any) -> None:
    """Report ``transfer.progress`` for the request being served (no-op outside one)."""
    ctx = current_request()
    if ctx is not None and ctx.account is not None:
        await ctx.account.progress(done_bytes, total_bytes, **kw)


class AuthFlow:
    """One interactive auth conversation (spec section 7) for an account.

    ``await flow.ask("code", hint=...)`` emits an ``auth.step`` and returns the value the
    user submits. ``show`` emits a step with nothing to submit (``qr``, ``info``) and
    ``done`` closes the flow. A new step replaces the pending one.
    """

    def __init__(self, account: Account, name: str):
        self.account = account
        self.name = name
        self._seq = 0
        self._pending: dict[str, asyncio.Future[str]] = {}
        self._task: asyncio.Task[Any] | None = None

    def _next_id(self) -> str:
        self._seq += 1
        return str(self._seq)

    def _drop_pending(self) -> None:
        for fut in self._pending.values():
            if not fut.done():
                fut.cancel()
        self._pending.clear()

    async def show(self, kind: str, **fields: Any) -> str:
        """Emit a step of ``kind`` with the given AuthStep fields. Returns its ``step_id``."""
        self._drop_pending()
        step_id = self._next_id()
        step = T.AuthStep(flow=self.name, step_id=step_id, kind=kind, **fields)
        await self.account.auth_step(step)
        return step_id

    async def ask(
        self,
        kind: str,
        *,
        title: str | None = None,
        hint: str | None = None,
        placeholder: str | None = None,
        error: str | None = None,
        can_resend: bool = False,
        min_length: int | None = None,
        max_length: int | None = None,
        expires_at: datetime | None = None,
    ) -> str:
        """Emit an input step (``phone``, ``code``, ``password``, ``email``, ``email_code``,
        ``text``) and wait for the matching ``auth.submit``. To reject a value, call
        ``ask`` again with ``error=...`` (spec 7.2)."""
        self._drop_pending()
        step_id = self._next_id()
        fut: asyncio.Future[str] = asyncio.get_running_loop().create_future()
        self._pending[step_id] = fut
        try:
            # The core can submit as soon as the emitted step becomes visible, including
            # before auth_step's awaited write returns. Install its waiter first.
            step = T.AuthStep(
                flow=self.name,
                step_id=step_id,
                kind=kind,
                title=title,
                hint=hint,
                placeholder=placeholder,
                error=error,
                can_resend=can_resend,
                min_length=min_length,
                max_length=max_length,
                expires_at=expires_at,
            )
            await self.account.auth_step(step)
            return await fut
        finally:
            # A replacement step or another flow owns its own waiter. Clean only ours.
            if self._pending.get(step_id) is fut:
                del self._pending[step_id]
            if not fut.done():
                fut.cancel()

    async def qr(self, data: str, *, expires_at: datetime | None = None, **fields: Any) -> str:
        return await self.show("qr", data=data, expires_at=expires_at, **fields)

    async def info(self, title: str, hint: str | None = None) -> str:
        return await self.show("info", title=title, hint=hint)

    async def done(self, title: str | None = None) -> None:
        await self.show("done", title=title)

    def submit(self, step_id: str, value: str) -> bool:
        fut = self._pending.pop(step_id, None)
        if fut is None or fut.done():
            return False
        fut.set_result(value)
        return True

    def start(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Run ``coro`` as this flow (replacing a running one); ``auth.cancel`` cancels it."""
        self.cancel()
        self._task = self.account.spawn(self._guard(coro), name=f"auth:{self.name}")
        return self._task

    def cancel(self) -> None:
        self._drop_pending()
        if self._task is not None and not self._task.done():
            self._task.cancel()
        self._task = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def _guard(self, coro: Coroutine[Any, Any, Any]) -> None:
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # Errors after auth.submit returned cannot fail it; surface them as status.
            log.warning("auth flow %s for %s failed: %s", self.name, self.account.id, e)
            if self.name == "sign_in" and self.account.running:
                msg = e.message if isinstance(e, PluginError) else str(e)
                await self.account.status("auth_required", detail=msg or type(e).__name__)


class Account(EventEmitters):
    """A started account: its ``account.start`` params plus event emitters.

    Every event of spec section 10 has a coroutine here (``status``,
    ``conversations_upsert``, ``messages_upsert``, ``auth_step``, ...), named after the
    event with ``.`` replaced by ``_``; ``account_id`` is filled in. Pass ``ack=True`` to
    send it as a request and wait for the core (bulk sync, spec 16).
    """

    def __init__(self, plugin: Plugin, params: T.AccountStartParams):
        self.plugin = plugin
        self.params = params
        self.id = params.account_id
        # The core fills settings_schema defaults; do it again so tests without one match (spec 6.1).
        self.settings: dict[str, Any] = {**plugin.setting_defaults(), **params.settings}
        self.secrets: dict[str, str] = dict(params.secrets)
        self.state_dir = Path(params.state_dir)
        self.cache_dir = Path(params.cache_dir)
        self.ghost: bool = params.ghost
        self.auto_download: T.AutoDownload = params.auto_download
        self.state: str = "disconnected"  # last state reported through `status`
        self.data: dict[str, Any] = {}  # free for the plugin's own per-account objects
        self.auth = AuthFlow(self, "sign_in")
        self._flows: dict[str, AuthFlow] = {"sign_in": self.auth}
        self._tasks: set[asyncio.Task[Any]] = set()
        self._accepted = False
        self._accepted_ev = asyncio.Event()
        self._queued: list[Any] = []
        self._stopped = False
        self._start_ctx: RequestContext | None = None
        self._last_progress: dict[Any, float] = {}

    @property
    def running(self) -> bool:
        """False once ``account.stop`` began; emits are then dropped."""
        return not self._stopped

    def setting(self, key: str, default: Any = None) -> Any:
        """A settings value, else the ``settings_schema`` default, else ``default``."""
        v = self.settings.get(key)
        if v is not None:
            return v
        prop = (self.plugin.settings_schema.get("properties") or {}).get(key) or {}
        return prop.get("default", default)

    def flow(self, name: str) -> AuthFlow:
        """The auth flow ``name`` (``sign_in`` is :attr:`auth`; e.g. ``two_factor``)."""
        if name not in self._flows:
            self._flows[name] = AuthFlow(self, name)
        return self._flows[name]

    def spawn(self, coro: Coroutine[Any, Any, Any], name: str | None = None) -> asyncio.Task[Any]:
        """Run background work for this account; it is cancelled on ``account.stop``."""
        ctx = contextvars.copy_context()
        ctx.run(_current.set, None)
        task = asyncio.get_running_loop().create_task(self._run_bg(coro), name=name, context=ctx)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _run_bg(self, coro: Coroutine[Any, Any, Any]) -> Any:
        try:
            return await coro
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("background task for %s failed", self.id)
            return None

    # ---- events -------------------------------------------------------------------

    async def emit(self, event: Any, *, ack: bool = False) -> None:
        """Send an event dataclass (``contact_hub_sdk.types.*Event``)."""
        name = _EVENT_NAMES.get(type(event))
        if name is None:
            raise TypeError(f"{type(event).__name__} is not an event")
        if self._stopped:
            log.warning("dropping %s for stopped account %s", name, self.id)
            return
        if name == "status":
            self.state = str(event.state)
        if not self._accepted:
            cur = current_request()
            if not ack or (cur is not None and cur is self._start_ctx):
                if ack:
                    log.debug("ack emit during account.start sent as a notification")
                self._queued.append(event)
                return
            await self._accepted_ev.wait()
            if self._stopped:
                return
        await self.plugin._send_event(name, to_wire(event), ack)

    def emit_nowait(self, event: Any) -> None:
        """Thread-safe fire-and-forget :meth:`emit` (notification)."""
        loop = self.plugin._loop
        if loop is None:
            raise RuntimeError("plugin is not running")
        loop.call_soon_threadsafe(lambda: asyncio.ensure_future(self.emit(event)))

    async def progress(
        self,
        done_bytes: int,
        total_bytes: int | None = None,
        *,
        direction: str | None = None,
        chat_id: str | None = None,
        message_id: str | None = None,
        force: bool = False,
    ) -> None:
        """``transfer.progress`` for the current request, throttled; resets its timeout."""
        ctx = current_request()
        rid = ctx.id if ctx is not None else None
        now = time.monotonic()
        last = self._last_progress.get(rid, 0.0)
        final = total_bytes is not None and done_bytes >= total_bytes
        if not (force or final) and now - last < PROGRESS_INTERVAL:
            return
        self._last_progress[rid] = now
        p = ctx.raw_params if ctx is not None else {}
        if direction is None:
            direction = "download" if (ctx is not None and ctx.method == "media.download") else "upload"
        if ctx is not None:
            ctx.extend_deadline()
        await self.transfer_progress(
            request_id=rid,
            direction=direction,
            chat_id=chat_id or p.get("chat_id"),
            message_id=message_id or p.get("message_id"),
            done_bytes=done_bytes,
            total_bytes=total_bytes,
        )
        if final:
            self._last_progress.pop(rid, None)

    # ---- plugin -> core requests ------------------------------------------------------

    async def request(
        self, method: str, params: dict[str, Any] | None = None, *, timeout: float | None = None
    ) -> dict[str, Any]:
        """Send an account-scoped request to the core (``account_id`` added)."""
        return await self.plugin.request(method, {"account_id": self.id, **(params or {})}, timeout=timeout)

    async def set_secret(self, key: str, value: str) -> None:
        """``secrets.set``: store a runtime credential in the core's keyring (spec 6.2)."""
        await self.request("secrets.set", {"key": key, "value": value})
        self.secrets[key] = value
        self.plugin._redact.add(value)

    async def delete_secret(self, key: str) -> None:
        await self.request("secrets.delete", {"key": key})
        self.secrets.pop(key, None)


_EVENT_NAMES: dict[type, str] = {cls: name for name, cls in EVENTS.items()}


class _Redactor(logging.Filter):
    """Masks secret values in log records (spec 19: never log secrets)."""

    def __init__(self) -> None:
        super().__init__()
        self.values: set[str] = set()

    def add(self, v: Any) -> None:
        if isinstance(v, str) and len(v) >= 4:
            self.values.add(v)

    def filter(self, record: logging.LogRecord) -> bool:
        if self.values:
            msg = record.getMessage()
            for v in self.values:
                msg = msg.replace(v, "***")
            record.msg, record.args = msg, None
        return True


class Plugin:
    """Subclass this, set :attr:`capabilities`, implement handlers with :func:`method`
    and the ``on_*`` hooks you need, then call :meth:`run`.

    The SDK answers ``initialize``, ``ping`` and ``shutdown``, manages ``account.start``
    / ``account.stop``, answers unknown methods with ``METHOD_NOT_FOUND`` and methods
    whose capability is not declared with ``NOT_SUPPORTED``, checks gated parameters,
    and turns exceptions into spec error objects.
    """

    #: Capability key -> ``True`` / parameter dict / parameter dataclass (spec 14).
    capabilities: dict[str, Any] = {}
    #: The plugin's own version, reported in ``initialize`` as ``plugin_version``.
    version: str | None = None
    #: The API version this plugin implements.
    api_version: str = API_VERSION
    #: Reject params whose capability is missing (spec 14.2, registry ``gated_params``).
    enforce_param_capabilities: bool = True
    #: Seconds before a plugin -> core request (``secrets.*``, acked events) times out:
    #: the SDK then sends ``$/cancelRequest`` and raises ``RpcTimeout`` (spec 4.6, 12).
    request_timeout: float = float(DEFAULT_TIMEOUT_SECONDS)

    _handlers: dict[str, str] = {}

    def __init_subclass__(cls, **kw: Any) -> None:
        super().__init_subclass__(**kw)
        handlers = dict(getattr(cls, "_handlers", {}))
        for attr, fn in vars(cls).items():
            name = getattr(fn, "__ch_method__", None)
            if name is not None:
                handlers[name] = attr
        cls._handlers = handlers

    def __init__(self) -> None:
        self.accounts: dict[str, Account] = {}
        self.manifest: T.Manifest | None = None
        self.settings_schema: dict[str, Any] = {}
        self.init_params: T.InitializeParams | None = None
        self.core_api_version: tuple[int, int, int] | None = None
        self._conn: Connection | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._initialized = False
        self._initializing = False
        self._ack_sem: asyncio.Semaphore | None = None
        self._redact = _Redactor()
        self._check_handlers()

    # ---- hooks (override as needed) ----------------------------------------------------

    async def on_initialize(self, params: T.InitializeParams) -> None:
        """Called during ``initialize``, before it is answered (whatever the core's version)."""

    async def on_account_start(self, account: Account) -> None:
        """Accept the account. Must return fast: connect in ``account.spawn(...)`` and
        report through ``account.status(...)``. If nothing reports a status before
        this returns, the SDK emits ``connecting``. Raise ``InvalidParams`` for unusable
        settings (the message is shown to the user)."""

    async def on_account_stop(self, account: Account) -> None:
        """Close sockets and flush the session. Background tasks are already cancelled."""

    async def on_auth_begin(self, account: Account, method: str) -> None:
        """Run the ``sign_in`` flow for ``method`` with ``account.auth.ask(...)`` etc.
        Overriding this makes the SDK serve ``auth.begin/submit/cancel`` and advertise
        ``auth.interactive``. Runs in the background; ``auth.begin`` returns at once."""
        raise NotSupported(capability="auth.interactive")

    async def on_auth_cancel(self, account: Account, flow: str) -> None:
        """Called after the SDK cancelled ``flow`` on ``auth.cancel``."""

    async def on_shutdown(self) -> None:
        """Called after every account was stopped, before ``shutdown`` is answered."""

    def is_busy(self) -> bool:
        """``ping`` reports ``busy: true`` while this returns True (spec 4.3)."""
        return False

    def get_capabilities(self) -> dict[str, Any]:
        """The ``capabilities`` to advertise; defaults to :attr:`capabilities`."""
        caps = {k: v for k, v in self.capabilities.items() if v is not None and v is not False}
        if self._has_auth and "auth.interactive" not in caps:
            caps["auth.interactive"] = True
        return caps

    # ---- running ---------------------------------------------------------------------

    def run(self) -> None:
        """Serve the protocol on stdin/stdout until ``shutdown`` or EOF, then exit 0."""
        level = _LOG_LEVELS.get(os.environ.get("CH_LOG_LEVEL", "info"), logging.INFO)
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        handler.addFilter(self._redact)
        logging.basicConfig(level=level, handlers=[handler], force=True)

        async def main() -> None:
            reader, write = await open_stdio()
            await self.serve(reader, write)

        asyncio.run(main())
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)  # the stdin reader thread may still block in read()

    async def serve(self, reader: asyncio.StreamReader, write: Callable[[bytes], Awaitable[None]]) -> None:
        """Serve one connection (used by :meth:`run`; handy for in-process tests)."""
        self._loop = asyncio.get_running_loop()
        self._ack_sem = asyncio.Semaphore(MAX_ACK_IN_FLIGHT)
        self._conn = conn = Connection(reader, write, self._on_request, timeout_for=_timeout_for)
        conn.accept_notifications = False  # until initialize is answered (spec 4.1)
        try:
            await conn.run()
        finally:
            # EOF (or shutdown): answer what finishes within the budget, drop the rest (spec 2.5).
            await conn.finish_inflight(EOF_BUDGET)
            if self.accounts:  # EOF without shutdown: tear down quickly
                try:
                    await asyncio.wait_for(self._stop_all(), timeout=EOF_BUDGET)
                except TimeoutError:
                    log.warning("accounts did not stop within %s s of EOF", EOF_BUDGET)
            conn.closed = True

    async def request(
        self, method: str, params: dict[str, Any] | None = None, *, timeout: float | None = None
    ) -> dict[str, Any]:
        """Send a request to the core and return its result (raises PluginError).
        ``timeout`` defaults to :attr:`request_timeout`."""
        if self._conn is None:
            raise RuntimeError("plugin is not running")
        return await self._conn.request(method, params or {}, self.request_timeout if timeout is None else timeout)

    async def _send_event(self, name: str, params: dict[str, Any], ack: bool) -> None:
        conn = self._conn
        if conn is None or conn.closed:
            return
        if not ack:
            await conn.notify(name, params)
            return
        assert self._ack_sem is not None
        async with self._ack_sem:
            await conn.request(name, params, self.request_timeout)

    # ---- dispatch --------------------------------------------------------------------

    @property
    def _has_auth(self) -> bool:
        return type(self).on_auth_begin is not Plugin.on_auth_begin

    def _check_handlers(self) -> None:
        missing = []
        caps = self.get_capabilities()
        for info in METHODS.values():
            if info.direction != "core_to_plugin" or info.name in LIFECYCLE:
                continue
            if info.capability != "required" and not caps.get(info.capability):
                continue
            if info.name in self._handlers:
                continue
            if info.name.startswith("auth.") and self._has_auth:
                continue
            missing.append(f"{info.name} ({info.capability})")
        if missing:
            raise TypeError(
                f"{type(self).__name__} must implement (spec 14.2): {', '.join(missing)}"
            )
        for name in self._handlers:
            info = METHODS.get(name)
            if info is not None and info.capability != "required" and not caps.get(info.capability):
                log.warning("handler for %s is unreachable: capability %s is not declared",
                            name, info.capability)

    async def _on_request(self, ctx: RequestContext) -> dict[str, Any]:
        m = ctx.method
        if m == "initialize":
            return await self._initialize(ctx)
        if not self._initialized:
            raise PluginError("initialize must be the first request", code="INVALID_REQUEST")
        if m == "ping":
            return {"busy": True} if self.is_busy() else {}
        if m == "shutdown":
            return await self._shutdown(ctx)
        if m == "account.start":
            return await self._account_start(ctx)

        info = METHODS.get(m)
        handler_attr = self._handlers.get(m)
        builtin_auth = m in ("auth.begin", "auth.submit", "auth.cancel") and self._has_auth
        if info is None or info.direction != "core_to_plugin":
            if handler_attr is None:
                raise MethodNotFound(f"unknown method {m}")
            params: Any = ctx.raw_params
            account = None
            aid = params.get("account_id")
            if aid is not None:
                account = self._account(aid)
        else:
            caps = self.get_capabilities()
            if info.capability != "required" and not caps.get(info.capability):
                raise NotSupported(f"{m} needs capability {info.capability}", capability=info.capability)
            params = self._decode(info.params, ctx.raw_params)
            if m == "account.stop":
                return await self._account_stop(ctx, params.account_id)
            aid = getattr(params, "account_id", None)
            account = self._account(aid) if aid is not None else None
            if self.enforce_param_capabilities:
                self._check_param_caps(m, ctx.raw_params, caps)
        ctx.account = account
        if handler_attr is None and builtin_auth:
            return await self._builtin_auth(m, account, params)
        if handler_attr is None:  # declared capability without handler is caught at init
            raise NotSupported(f"{m} is not implemented", capability=info.capability if info else None)
        return await self._call(getattr(self, handler_attr), account, params)

    async def _call(self, fn: Callable[..., Any], account: Account | None, params: Any) -> dict[str, Any]:
        if inspect.iscoroutinefunction(fn):
            res = await fn(account, params)
        else:
            res = await asyncio.to_thread(fn, account, params)
            if inspect.isawaitable(res):
                res = await res
        out = to_wire(res) if res is not None else {}
        return out

    @staticmethod
    def _decode(cls: type | None, raw: dict[str, Any]) -> Any:
        if cls is None:
            return raw
        try:
            return from_wire(cls, raw)
        except DecodeError as e:
            raise InvalidParams(str(e), field=e.field) from None

    def _account(self, account_id: str) -> Account:
        acc = self.accounts.get(account_id)
        if acc is None or not acc.running:
            raise UnknownAccount(f"account {account_id!r} is not started")
        return acc

    @staticmethod
    def _check_param_caps(m: str, raw: dict[str, Any], caps: dict[str, Any]) -> None:
        info = METHODS.get(m)
        missing = info.missing_capabilities(raw, lambda c: bool(caps.get(c))) if info else []
        if missing:
            g = missing[0]
            raise NotSupported(f"{g.param} needs capability {g.capability}", capability=g.capability)

    # ---- lifecycle ---------------------------------------------------------------------

    async def _initialize(self, ctx: RequestContext) -> dict[str, Any]:
        if self._initialized or self._initializing:
            raise PluginError("initialize was already received", code="INVALID_REQUEST")
        self._initializing = True
        try:
            return await self._do_initialize(ctx)
        finally:
            self._initializing = False

    async def _do_initialize(self, ctx: RequestContext) -> dict[str, Any]:
        p = self._decode(T.InitializeParams, ctx.raw_params)
        core = _semver(p.api_version)
        mine = _semver(self.api_version)
        if core is None or mine is None or core[0] != mine[0]:
            # Answer anyway: the core decides about incompatible majors (spec 4.1).
            log.warning("core speaks API %s, this plugin implements %s", p.api_version, self.api_version)
        self.init_params = p
        self.core_api_version = core
        logging.getLogger().setLevel(_LOG_LEVELS.get(p.log_level, logging.INFO))
        self._load_manifest(Path(p.plugin_dir))
        self._check_vendor_names()
        await self.on_initialize(p)
        self._initialized = True
        assert self._conn is not None
        self._conn.accept_notifications = True
        caps = to_wire(self.get_capabilities())
        return to_wire(
            T.InitializeResult(api_version=self.api_version, capabilities=caps, plugin_version=self.version)
        )

    def _load_manifest(self, plugin_dir: Path) -> None:
        path = plugin_dir / "plugin.json"
        try:
            raw = json.loads(path.read_text())
        except (OSError, ValueError):
            log.debug("no readable manifest at %s", path)
            return
        self.settings_schema = raw.get("settings_schema") or {}
        try:
            self.manifest = from_wire(T.Manifest, raw)
        except DecodeError as e:
            log.warning("plugin.json does not match the manifest schema: %s", e)
        if self.version is None and isinstance(raw.get("version"), str):
            self.version = raw["version"]

    def plugin_id(self) -> str | None:
        """This plugin's id: the manifest's ``id``, else ``CH_PLUGIN_ID``."""
        if self.manifest is not None:
            return self.manifest.id
        return os.environ.get("CH_PLUGIN_ID")

    def _check_vendor_names(self) -> None:
        pid = self.plugin_id()
        for name in self._handlers:
            m = VENDOR_NAME.match(name)
            if m is not None and pid is not None and m.group(1) != pid:
                log.warning("vendor method %s should be named x-%s.<name> (spec 12.3)", name, pid)

    def setting_defaults(self) -> dict[str, Any]:
        """Top-level ``default`` values of the manifest's ``settings_schema`` (spec 6.1)."""
        props = self.settings_schema.get("properties") or {}
        return {k: v["default"] for k, v in props.items() if isinstance(v, dict) and "default" in v}

    def secret_keys(self) -> set[str]:
        """Settings keys flagged ``x-secret: true`` in the manifest's settings_schema."""
        props = self.settings_schema.get("properties") or {}
        return {k for k, v in props.items() if isinstance(v, dict) and v.get("x-secret")}

    async def _account_start(self, ctx: RequestContext) -> dict[str, Any]:
        p = self._decode(T.AccountStartParams, ctx.raw_params)
        if p.account_id in self.accounts:  # the running account is left untouched (spec 4.2)
            raise InvalidParams(f"account {p.account_id!r} is already started", field="account_id")
        acc = Account(self, p)
        acc._start_ctx = ctx
        ctx.account = acc
        for k in self.secret_keys():
            self._redact.add(acc.settings.get(k))
        for v in acc.secrets.values():
            self._redact.add(v)
        self.accounts[p.account_id] = acc
        try:
            await self.on_account_start(acc)
        except BaseException:
            acc._stopped = True
            acc._accepted_ev.set()
            for t in list(acc._tasks):
                t.cancel()
            self.accounts.pop(p.account_id, None)
            raise

        async def accept() -> None:
            # Events emitted while account.start ran go out after its response, in order.
            acc._start_ctx = None
            if not any(type(e) is T.StatusEvent for e in acc._queued):
                acc._queued.insert(0, T.StatusEvent(account_id=acc.id, state=T.AccountState.CONNECTING))
                acc.state = "connecting"
            while acc._queued:
                e = acc._queued.pop(0)
                if acc.running:
                    await self._send_event(_EVENT_NAMES[type(e)], to_wire(e), False)
            acc._accepted = True
            acc._accepted_ev.set()

        ctx.after_response(accept)
        return {}

    async def _account_stop(self, ctx: RequestContext, account_id: str) -> dict[str, Any]:
        acc = self._account(account_id)
        await self._stop_account(acc)
        return {}

    async def _stop_account(self, acc: Account) -> None:
        if not acc._accepted:  # let account.start answer and flush its queued events first
            try:
                await asyncio.wait_for(acc._accepted_ev.wait(), timeout=5)
            except TimeoutError:
                log.warning("account.start for %s did not finish before its stop", acc.id)
        for f in acc._flows.values():
            f.cancel()
        tasks = list(acc._tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=5)
        try:
            await self.on_account_stop(acc)
        except Exception:
            log.exception("on_account_stop for %s failed", acc.id)
        if acc._accepted:
            await acc.status(T.AccountState.DISCONNECTED)
        acc._stopped = True
        acc._accepted_ev.set()
        if self.accounts.get(acc.id) is acc:
            del self.accounts[acc.id]

    async def _stop_all(self) -> None:
        accs = list(self.accounts.values())
        if accs:
            await asyncio.gather(*(self._stop_account(a) for a in accs), return_exceptions=True)

    async def _shutdown(self, ctx: RequestContext) -> dict[str, Any]:
        await self._stop_all()
        try:
            await self.on_shutdown()
        except Exception:
            log.exception("on_shutdown failed")
        assert self._conn is not None
        conn = self._conn

        async def close() -> None:
            conn.stop()

        ctx.after_response(close)
        return {}

    async def _builtin_auth(self, m: str, account: Account, params: Any) -> dict[str, Any]:
        if m == "auth.begin":
            account.auth.start(self.on_auth_begin(account, params.method))
        elif m == "auth.submit":
            flow = account._flows.get(params.flow)
            if flow is None or not flow.submit(params.step_id, params.value):
                log.info("auth.submit for a step that is no longer pending (%s/%s)", params.flow, params.step_id)
        elif m == "auth.cancel":
            flow = account._flows.get(params.flow)
            if flow is not None:
                flow.cancel()
            if params.flow == "sign_in":
                await account.status(T.AccountState.AUTH_REQUIRED)
            await self.on_auth_cancel(account, params.flow)
        return {}


_LOG_LEVELS = {
    "error": logging.ERROR,
    "warn": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
    "trace": logging.DEBUG,
}


def _semver(v: str) -> tuple[int, int, int] | None:
    try:
        core = v.split("+", 1)[0].split("-", 1)[0]
        a, b, c = (int(x) for x in core.split("."))
        return a, b, c
    except (ValueError, AttributeError):
        return None


def _timeout_for(method: str) -> float | None:
    info = METHODS.get(method)
    return float(info.timeout_seconds) if info is not None else None

