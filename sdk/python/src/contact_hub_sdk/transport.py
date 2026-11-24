"""NDJSON JSON-RPC 2.0 transport (spec section 2) over a pair of byte streams.

One JSON value per ``\\n``-terminated line, UTF-8, at most 16 MiB per frame, no batch
arrays. Requests, responses and notifications flow in both directions. Inbound
requests each run in their own task (so ``ping`` is answered while a handler waits on
the network); ``$/cancelRequest`` cancels the task, which then answers ``CANCELLED``.
"""

from __future__ import annotations

import asyncio
import contextvars
import itertools
import json
import logging
import os
import sys
import threading
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .errors import (
    Cancelled,
    Internal,
    PluginError,
    RpcTimeout,
    error_from_exception,
)
from .registry import CANCEL_METHOD, DEFAULT_TIMEOUT_SECONDS

__all__ = ["MAX_FRAME", "Connection", "RequestContext", "current_request", "open_stdio"]

log = logging.getLogger("contact_hub_sdk")

MAX_FRAME = 16 * 1024 * 1024  # bytes, including the newline (spec 2.2)
DEFAULT_OUTBOUND_TIMEOUT = float(DEFAULT_TIMEOUT_SECONDS)  # registry default_timeout_seconds (spec 4.6, 12)

_current: contextvars.ContextVar[RequestContext | None] = contextvars.ContextVar(
    "contact_hub_request", default=None
)


def current_request() -> RequestContext | None:
    """The inbound request the calling code is serving, or ``None``."""
    return _current.get()


def _valid_id(v: Any) -> bool:
    return (isinstance(v, int) and not isinstance(v, bool)) or (isinstance(v, str) and v != "")


def encode_frame(msg: dict[str, Any]) -> bytes:
    data = json.dumps(msg, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
    if len(data) + 1 > MAX_FRAME:
        raise ValueError(f"frame of {len(data) + 1} bytes exceeds the 16 MiB limit")
    return data + b"\n"


class RequestContext:
    """One inbound request being served. Get it with :func:`current_request`."""

    def __init__(self, conn: Connection, id: int | str, method: str, params: dict[str, Any]):
        self.conn = conn
        self.id = id
        self.method = method
        self.raw_params = params
        self.account: Any = None  # set by the plugin layer for account-scoped methods
        self._cancel = threading.Event()
        self._after: list[Callable[[], Awaitable[None]]] = []
        self._timeout: asyncio.Timeout | None = None
        self._timeout_seconds: float | None = None

    @property
    def cancelled(self) -> bool:
        """True once the core sent ``$/cancelRequest`` for this request. Safe from threads."""
        return self._cancel.is_set()

    def after_response(self, fn: Callable[[], Awaitable[None]]) -> None:
        """Run ``fn`` right after the response for this request is written."""
        self._after.append(fn)

    def extend_deadline(self) -> None:
        """Restart this request's timeout (``transfer.progress`` does this, spec 4.6)."""
        if self._timeout is not None and self._timeout_seconds is not None:
            self._timeout.reschedule(asyncio.get_running_loop().time() + self._timeout_seconds)


RequestHandler = Callable[[RequestContext], Awaitable[Any]]
NotificationHandler = Callable[[str, dict[str, Any]], Awaitable[None]]


class Connection:
    """A JSON-RPC peer over ``reader`` (a StreamReader) and ``write`` (bytes sink).

    ``on_request(ctx)`` returns the result (already a JSON object) or raises.
    ``timeout_for(method)`` gives the seconds after which a still-running inbound
    request is abandoned with ``TIMEOUT`` (a backstop: the core cancels first).
    """

    GRACE = 5.0

    def __init__(
        self,
        reader: asyncio.StreamReader,
        write: Callable[[bytes], Awaitable[None]],
        on_request: RequestHandler,
        on_notification: NotificationHandler | None = None,
        timeout_for: Callable[[str], float | None] | None = None,
    ):
        self._reader = reader
        self._write = write
        self._on_request = on_request
        self._on_notification = on_notification
        self._timeout_for = timeout_for or (lambda m: None)
        self._wlock = asyncio.Lock()
        self._ids = itertools.count(1)
        self._pending: dict[int | str, asyncio.Future[Any]] = {}
        self._inflight: dict[int | str, tuple[asyncio.Task[None], RequestContext]] = {}
        #: True once the writer is shut: nothing more is sent.
        self.closed = False
        #: False drops inbound notifications with a log line (before ``initialize``, spec 4.1).
        self.accept_notifications = True
        self._done_reading = False
        self._stop = asyncio.Event()

    # ---- writing ----------------------------------------------------------------

    async def send(self, msg: dict[str, Any]) -> None:
        if self.closed:
            return
        frame = encode_frame(msg)
        async with self._wlock:
            await self._write(frame)

    async def notify(self, method: str, params: dict[str, Any]) -> None:
        await self.send({"jsonrpc": "2.0", "method": method, "params": params})

    async def request(
        self, method: str, params: dict[str, Any], timeout: float | None = None
    ) -> dict[str, Any]:
        """Send a request to the core and wait for its result (raises PluginError). On
        timeout a ``$/cancelRequest`` is sent and ``RpcTimeout`` raised."""
        if self.closed or self._done_reading:
            raise Internal("connection closed")
        rid = f"p-{next(self._ids)}"
        fut: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        try:
            await self.send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
            if timeout is None:
                timeout = DEFAULT_OUTBOUND_TIMEOUT
            try:
                return await asyncio.wait_for(asyncio.shield(fut), timeout)
            except TimeoutError:
                await self.notify(CANCEL_METHOD, {"id": rid})
                raise RpcTimeout(f"{method} timed out after {timeout}s") from None
            except asyncio.CancelledError:
                if not fut.done():
                    await asyncio.shield(self.notify(CANCEL_METHOD, {"id": rid}))
                raise
        finally:
            self._pending.pop(rid, None)

    async def _respond(self, id: Any, result: Any = None, error: PluginError | None = None) -> None:
        if error is not None:
            await self.send({"jsonrpc": "2.0", "id": id, "error": error.to_wire()})
        else:
            await self.send({"jsonrpc": "2.0", "id": id, "result": result})

    # ---- reading ----------------------------------------------------------------

    def stop(self) -> None:
        """Make :meth:`run` return (after an orderly ``shutdown``)."""
        self._stop.set()

    async def run(self) -> None:
        """Read frames until EOF, a fatal framing error or :meth:`stop`."""
        stopper = asyncio.ensure_future(self._stop.wait())
        try:
            while True:
                reader = asyncio.ensure_future(self._reader.readline())
                done, _ = await asyncio.wait({reader, stopper}, return_when=asyncio.FIRST_COMPLETED)
                if stopper in done:
                    reader.cancel()
                    return
                try:
                    line = reader.result()
                except (ValueError, asyncio.LimitOverrunError):
                    log.error("inbound frame exceeds 16 MiB; closing the stream")
                    return
                if not line:
                    return  # EOF: the core is gone (spec 2.5)
                if len(line) > MAX_FRAME:
                    log.error("inbound frame exceeds 16 MiB; closing the stream")
                    return
                await self._dispatch(line)
        finally:
            stopper.cancel()
            self._done_reading = True
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(Internal("connection closed"))

    async def _dispatch(self, line: bytes) -> None:
        line = line.rstrip(b"\n").rstrip(b"\r")
        if not line.strip():
            return
        try:
            msg = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            await self._respond(None, error=PluginError(code="PARSE_ERROR"))
            return
        if not isinstance(msg, dict):
            what = "batch arrays are not supported" if isinstance(msg, list) else "not an object"
            await self._respond(None, error=PluginError(what, code="INVALID_REQUEST"))
            return
        has_id = "id" in msg and msg["id"] is not None
        rid = msg.get("id")
        if msg.get("jsonrpc") != "2.0":
            if "method" in msg or not has_id:
                await self._respond(rid if _valid_id(rid) else None,
                                    error=PluginError("jsonrpc must be \"2.0\"", code="INVALID_REQUEST"))
            return
        if "method" not in msg:
            self._on_response(msg)
            return
        method = msg["method"]
        params = msg.get("params", {})
        if params is None:
            params = {}
        if not isinstance(method, str) or (has_id and not _valid_id(rid)) or ("id" in msg and rid is None):
            await self._respond(rid if _valid_id(rid) else None,
                                error=PluginError("malformed request", code="INVALID_REQUEST"))
            return
        if not isinstance(params, dict):
            if has_id:
                await self._respond(rid, error=PluginError("params must be an object", code="INVALID_REQUEST"))
            return
        if not has_id:
            if not self.accept_notifications:
                log.info("notification %s before initialize ignored", method)
                return
            await self._notification(method, params)
            return
        ctx = RequestContext(self, rid, method, params)
        task = asyncio.ensure_future(self._serve(ctx), )
        self._inflight[rid] = (task, ctx)

    async def _notification(self, method: str, params: dict[str, Any]) -> None:
        if method == CANCEL_METHOD:
            rid = params.get("id")
            entry = self._inflight.get(rid) if _valid_id(rid) else None
            if entry is not None:  # unknown or already answered ids are ignored (spec 11.0)
                task, ctx = entry
                if not ctx.cancelled:
                    ctx._cancel.set()
                    # Only interrupt the handler. A queued task still owes a reply,
                    # and a completed handler must finish writing its response.
                    if ctx._timeout is not None:
                        task.cancel()
            return
        if self._on_notification is not None:
            try:
                await self._on_notification(method, params)
            except Exception:  # errors in notifications cannot be reported (spec 13)
                log.exception("notification %s failed", method)

    def _on_response(self, msg: dict[str, Any]) -> None:
        rid = msg.get("id")
        fut = self._pending.get(rid) if _valid_id(rid) else None
        if fut is None or fut.done():
            log.debug("response for unknown id %r ignored", rid)
            return
        if "error" in msg:
            fut.set_exception(PluginError.from_wire(msg["error"]))
        else:
            res = msg.get("result")
            fut.set_result(res if isinstance(res, dict) else {})

    async def _serve(self, ctx: RequestContext) -> None:
        token = _current.set(ctx)
        result: Any = None
        error: PluginError | None = None
        try:
            if ctx.cancelled:
                raise Cancelled()
            secs = self._timeout_for(ctx.method)
            ctx._timeout_seconds = secs + self.GRACE if secs else None
            async with asyncio.timeout(None) as tm:
                ctx._timeout = tm
                ctx.extend_deadline()
                result = await self._on_request(ctx)
            if not isinstance(result, dict):
                raise Internal(f"{ctx.method} returned {type(result).__name__}, not an object")
        except asyncio.CancelledError:
            if not ctx.cancelled and (self.closed or self._done_reading):
                raise  # dropped at EOF: no response
            error = Cancelled()
        except PluginError as e:
            error = e
        except Exception as e:
            if not isinstance(e, (TimeoutError, NotImplementedError)):
                log.exception("handler for %s failed", ctx.method)
            error = error_from_exception(e)
        finally:
            ctx._timeout = None
            _current.reset(token)
        try:
            try:
                await self._respond(ctx.id, result, error)
            except ValueError as e:  # result too large for one frame
                await self._respond(ctx.id, error=Internal(str(e)))
        finally:
            self._inflight.pop(ctx.id, None)
        for fn in ctx._after:
            try:
                await fn()
            except Exception:
                log.exception("after-response hook for %s failed", ctx.method)

    async def finish_inflight(self, budget: float) -> None:
        """After :meth:`run` returned: let inbound requests still running finish and answer
        for up to ``budget`` seconds, then cancel the rest without answering (spec 2.5)."""
        tasks = [t for t, _ in self._inflight.values()]
        if tasks:
            _, late = await asyncio.wait(tasks, timeout=budget)
            for t in late:
                t.cancel()
            if late:
                log.warning("dropped %d request(s) still running at EOF", len(late))
                await asyncio.wait(late, timeout=1)


async def open_stdio() -> tuple[asyncio.StreamReader, Callable[[bytes], Awaitable[None]]]:
    """Wire the process's stdin/stdout as the protocol channel.

    stdout is moved to a private file descriptor and fd 1 is pointed at stderr, so a
    stray ``print()`` (or a C library writing to stdout) lands in the log instead of
    corrupting the protocol (spec 2.1).
    """
    loop = asyncio.get_running_loop()
    sys.stdout.flush()
    proto_fd = os.dup(1)
    os.dup2(2, 1)
    in_fd = sys.stdin.fileno()

    reader = asyncio.StreamReader(limit=MAX_FRAME + 1, loop=loop)

    def pump() -> None:
        try:
            while True:
                chunk = os.read(in_fd, 1 << 16)
                if not chunk:
                    break
                loop.call_soon_threadsafe(reader.feed_data, chunk)
        except OSError:
            pass
        finally:
            try:
                loop.call_soon_threadsafe(reader.feed_eof)
            except RuntimeError:
                pass

    threading.Thread(target=pump, name="ch-stdin", daemon=True).start()
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ch-stdout")

    def write_all(data: bytes) -> None:
        view = memoryview(data)
        while view:
            n = os.write(proto_fd, view)
            view = view[n:]

    async def write(data: bytes) -> None:
        await loop.run_in_executor(executor, write_all, data)

    return reader, write
