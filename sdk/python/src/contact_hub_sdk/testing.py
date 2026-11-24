"""A stand-in for the Contact Hub core, for plugin tests (no app needed).

:class:`FakeCore` starts the plugin as a child process (its manifest's working
directory, ``CH_DATA_DIR``/``XDG_*`` pointed into a temp dir) and plays the core over
stdin/stdout::

    from contact_hub_sdk.testing import FakeCore

    def test_send(tmp_path):
        with FakeCore(["python", "-m", "my_plugin"], plugin_dir=PLUGIN_DIR, tmp=tmp_path) as core:
            core.start(secrets={"session": "s"})       # initialize + account.start
            core.event("status", state="connected")
            sent = core.call("messages.send", {"account_id": core.account_id,
                                               "chat_id": "c1", "text": "hi"})
            assert sent["message"]["text"] == "hi"
            assert core.shutdown() == 0

Like the real core it fills ``settings_schema`` defaults into ``account.start``
(spec 6.1), acks events sent as requests, keeps ``secrets.set``/``secrets.delete`` in
:attr:`FakeCore.secrets`, and treats every event for an account that is not started as
a test failure (spec 4.2). Each frame the plugin writes must be one JSON-RPC object on
one ``\\n``-terminated line. Pass ``check=schema_check(SCHEMAS_DIR)`` to also validate
every frame against the JSON Schemas (needs ``jsonschema`` and ``referencing``).
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from .errors import PluginError
from .registry import API_VERSION, CANCEL_METHOD, EVENTS, METHODS

__all__ = ["ISOLATED_ENV_VARS", "FakeCore", "schema_check"]

#: Environment variables pointed into the temp dir so a test never sees real data.
ISOLATED_ENV_VARS = ("CH_DATA_DIR", "XDG_DATA_HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME")

Check = Callable[[Any, str], None]


def schema_check(schemas_dir: str | Path) -> Check:
    """A ``check(instance, ref)`` validating against ``schemas_dir`` (the repo's
    ``schemas/``); ``ref`` is relative to it, e.g. ``"events.json#/$defs/status"``.
    Raises ``AssertionError`` listing the first errors."""
    from functools import cache

    from jsonschema import Draft202012Validator
    from referencing import Registry, Resource

    root = Path(schemas_dir)
    docs = [json.loads(p.read_text()) for p in root.rglob("*.json") if "examples" not in p.parts]
    base = "https://contacthub.dev/schemas/v1/"
    registry = Registry().with_resources((d["$id"], Resource.from_contents(d)) for d in docs)

    @cache
    def validator(ref: str) -> Draft202012Validator:
        return Draft202012Validator({"$ref": base + ref}, registry=registry,
                                    format_checker=Draft202012Validator.FORMAT_CHECKER)

    def check(instance: Any, ref: str) -> None:
        errors = sorted(validator(ref).iter_errors(instance), key=lambda e: list(e.path))
        assert not errors, f"{ref}: " + "; ".join(f"{list(e.path)}: {e.message}" for e in errors[:5])

    return check


class FakeCore:
    """Plays the core against a plugin process. See the module docstring.

    ``argv`` is the command that starts the plugin (run in ``plugin_dir``, which must
    hold ``plugin.json``). ``tmp`` holds the isolated data dirs and the accounts'
    ``state_dir``/``cache_dir`` (a fresh temp dir if omitted). ``check`` validates
    frames (see :func:`schema_check`). With ``auto_answer=False`` the plugin's requests
    (``secrets.*``, acked events) wait for :meth:`answer`.

    Calls raise ``AssertionError`` if nothing arrives within ``timeout`` seconds or the
    plugin breaks the protocol, and :class:`~contact_hub_sdk.errors.PluginError` when
    the plugin answers with an error.
    """

    def __init__(
        self,
        argv: Sequence[str],
        *,
        plugin_dir: str | Path,
        tmp: str | Path | None = None,
        env: dict[str, str] | None = None,
        check: Check | None = None,
        auto_answer: bool = True,
        timeout: float = 10.0,
    ):
        self.plugin_dir = Path(plugin_dir).resolve()
        self.manifest: dict[str, Any] = json.loads((self.plugin_dir / "plugin.json").read_text())
        self.plugin_id: str = self.manifest["id"]
        self.tmp = Path(tmp) if tmp is not None else Path(tempfile.mkdtemp(prefix="ch-fakecore-"))
        self.check = check
        self.auto_answer = auto_answer
        self.timeout = timeout
        run_env = {**os.environ, **(env or {}), "CH_PLUGIN_ID": self.plugin_id,
                   "CH_API_VERSION": API_VERSION, "PYTHONDONTWRITEBYTECODE": "1"}
        run_env.setdefault("CH_LOG_LEVEL", "debug")
        for var in ISOLATED_ENV_VARS:  # never the real profile, whatever the caller's env says
            run_env[var] = str(self.tmp / "env" / var.lower())
        self.proc = subprocess.Popen(
            list(argv), cwd=self.plugin_dir, env=run_env,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        #: Every stdout line, in order (for determinism checks).
        self.raw: list[bytes] = []
        #: Events not yet consumed by :meth:`event` (frames: ``{"method", "params", ...}``).
        self.events: list[dict[str, Any]] = []
        #: Every event, consumed or not.
        self.all_events: list[dict[str, Any]] = []
        #: Plugin -> core requests waiting for :meth:`answer` (``auto_answer=False``).
        self.requests: list[dict[str, Any]] = []
        #: account_id -> key -> value, as stored by ``secrets.set``/``secrets.delete``.
        self.secrets: dict[str, dict[str, str]] = {}
        #: Accounts whose ``account.start`` succeeded and that are not stopped since.
        self.started: set[str] = set()
        #: Ids of plugin -> core requests the plugin cancelled (``$/cancelRequest``).
        self.cancelled: list[Any] = []
        #: Every method this core called.
        self.called: set[str] = set()
        self.init_result: dict[str, Any] | None = None
        self.stderr = b""
        self._responses: dict[Any, dict[str, Any]] = {}
        self._pending: dict[Any, tuple[str, dict[str, Any]]] = {}
        self._failure: BaseException | None = None
        self._eof = False
        self._next_id = 0
        self._cond = threading.Condition()
        self._wlock = threading.Lock()
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()
        self._err_reader = threading.Thread(target=self._read_err, daemon=True)
        self._err_reader.start()

    # ---- context manager ------------------------------------------------------------

    def __enter__(self) -> FakeCore:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def close(self) -> None:
        """Kill the plugin if it still runs."""
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()
        self._err_reader.join(timeout=2)

    # ---- the protocol ----------------------------------------------------------------

    @property
    def account_id(self) -> str:
        """The default account id: the plugin id."""
        return self.plugin_id

    @property
    def capabilities(self) -> dict[str, Any]:
        assert self.init_result is not None, "not initialized"
        return self.init_result["capabilities"]

    def initialize(self, **params: Any) -> dict[str, Any]:
        """``initialize`` with sensible params (override any); returns the result."""
        p = {"api_version": API_VERSION, "core_version": "0.0.0-test", "plugin_dir": str(self.plugin_dir),
             "locale": "en-US", "log_level": "debug", **params}
        self.init_result = self.call("initialize", p)
        return self.init_result

    def start_params(self, account_id: str | None = None, settings: dict[str, Any] | None = None,
                     secrets: dict[str, str] | None = None, **params: Any) -> dict[str, Any]:
        """``account.start`` params with the settings defaults filled in (spec 6.1)."""
        account_id = account_id or self.account_id
        base = self.tmp / "accounts" / account_id.replace("#", "_")
        props = (self.manifest.get("settings_schema") or {}).get("properties") or {}
        defaults = {k: v["default"] for k, v in props.items() if isinstance(v, dict) and "default" in v}
        return {"account_id": account_id, "settings": {**defaults, **(settings or {})},
                "secrets": dict(secrets or {}), "state_dir": str(base / "state"),
                "cache_dir": str(base / "cache"), "ghost": False,
                "auto_download": {"photos": False, "videos_max_bytes": 0, "files_max_bytes": 0,
                                  "voice": False}, **params}

    def start(self, account_id: str | None = None, settings: dict[str, Any] | None = None,
              secrets: dict[str, str] | None = None, **params: Any) -> None:
        """``initialize`` (first time only), then ``account.start``."""
        if self.init_result is None:
            self.initialize()
        assert self.call("account.start", self.start_params(account_id, settings, secrets, **params)) == {}

    def stop(self, account_id: str | None = None) -> None:
        assert self.call("account.stop", {"account_id": account_id or self.account_id}) == {}

    def shutdown(self, timeout: float | None = None) -> int:
        """``shutdown``, then wait for the process; returns its exit code."""
        assert self.call("shutdown") == {}
        return self.wait_exit(timeout)

    def call(self, method: str, params: dict[str, Any] | None = None, *, check: bool = True,
             timeout: float | None = None) -> dict[str, Any]:
        """Send a request and return its result; raises PluginError for an error answer."""
        r = self.response(self.send_request(method, params, check=check), timeout)
        if "error" in r:
            raise PluginError.from_wire(r["error"])
        return r["result"]

    def error(self, method: str, params: dict[str, Any] | None = None, *, check: bool = True,
              timeout: float | None = None) -> dict[str, Any]:
        """Send a request that must fail; return the raw JSON-RPC error object."""
        r = self.response(self.send_request(method, params, check=check), timeout)
        assert "error" in r, f"{method} succeeded: {r}"
        return r["error"]

    def send_request(self, method: str, params: dict[str, Any] | None = None, *, check: bool = True) -> int:
        """Send a request without waiting; returns its id (see :meth:`response`)."""
        info = METHODS.get(method)
        if check and self.check is not None and params is not None and info is not None:
            self.check(params, f"methods/{info.schema}#/$defs/{method}.params")
        with self._cond:
            self._next_id += 1
            rid = self._next_id
            self._pending[rid] = (method, params or {})
        self.called.add(method)
        msg: dict[str, Any] = {"jsonrpc": "2.0", "id": rid, "method": method}
        if params is not None:
            msg["params"] = params
        self.send(msg)
        return rid

    def cancel(self, rid: int) -> None:
        self.send({"jsonrpc": "2.0", "method": "$/cancelRequest", "params": {"id": rid}})

    def response(self, rid: Any, timeout: float | None = None) -> dict[str, Any]:
        """Wait for the response frame to request ``rid``."""
        return self._wait(lambda: self._responses.pop(rid, None), f"the response to {rid!r}", timeout)

    def event(self, name: str, where: Callable[[dict[str, Any]], bool] | None = None, *,
              timeout: float | None = None, **match: Any) -> dict[str, Any]:
        """Wait for the next unconsumed event ``name`` whose params contain ``match`` and
        satisfy ``where``; consume it and return its params."""
        def find() -> dict[str, Any] | None:
            for e in self.events:
                p = e["params"]
                if e["method"] == name and all(p.get(k) == v for k, v in match.items()) \
                        and (where is None or where(p)):
                    self.events.remove(e)
                    return p
            return None

        return self._wait(find, f"event {name} {match or ''}", timeout)

    def request(self, method: str | None = None, *, timeout: float | None = None) -> dict[str, Any]:
        """Wait for a plugin -> core request (``auto_answer=False``) and return its frame."""
        def find() -> dict[str, Any] | None:
            for r in self.requests:
                if method is None or r["method"] == method:
                    self.requests.remove(r)
                    return r
            return None

        return self._wait(find, f"request {method or ''}", timeout)

    def answer(self, request: dict[str, Any], result: dict[str, Any] | None = None, *,
               error: dict[str, Any] | None = None) -> None:
        """Answer a plugin -> core request with ``result`` (default ``{}``) or ``error``."""
        if error is not None:
            self.send({"jsonrpc": "2.0", "id": request["id"], "error": error})
        else:
            self.send({"jsonrpc": "2.0", "id": request["id"], "result": result or {}})

    def send(self, msg: dict[str, Any] | bytes) -> None:
        """Write a frame (a dict, or raw bytes for framing tests)."""
        data = msg if isinstance(msg, bytes) else json.dumps(msg, ensure_ascii=False).encode() + b"\n"
        with self._wlock:
            self.proc.stdin.write(data)
            self.proc.stdin.flush()

    def close_stdin(self) -> None:
        """Close the plugin's stdin (EOF: "the core is gone", spec 2.5)."""
        with self._wlock:
            self.proc.stdin.close()

    def drain(self, seconds: float) -> None:
        """Let frames arrive for ``seconds`` (events land in :attr:`events`)."""
        deadline = time.monotonic() + seconds
        with self._cond:
            while not self._eof and (left := deadline - time.monotonic()) > 0:
                self._raise_failure()
                self._cond.wait(left)
            self._raise_failure()

    def wait_exit(self, timeout: float | None = None) -> int:
        """Wait until the plugin exits and closes stdout; returns its exit code."""
        code = self.proc.wait(timeout=timeout or self.timeout)
        self._reader.join(timeout=timeout or self.timeout)
        self._err_reader.join(timeout=2)
        with self._cond:
            self._raise_failure()
        return code

    @property
    def stderr_text(self) -> str:
        return self.stderr.decode(errors="replace")

    # ---- internals -------------------------------------------------------------------

    def _wait(self, find: Callable[[], Any], what: str, timeout: float | None) -> Any:
        deadline = time.monotonic() + (timeout or self.timeout)
        with self._cond:
            while True:
                self._raise_failure()
                got = find()
                if got is not None:
                    return got
                left = deadline - time.monotonic()
                if self._eof or left <= 0:
                    why = "plugin closed stdout" if self._eof else f"timed out after {timeout or self.timeout}s"
                    raise AssertionError(f"waiting for {what}: {why}\n--- plugin stderr ---\n"
                                         + self.stderr_text[-3000:])
                self._cond.wait(left)

    def _raise_failure(self) -> None:
        if self._failure is not None:
            f, self._failure = self._failure, None
            raise f

    def _fail(self, msg: str) -> None:
        if self._failure is None:
            self._failure = AssertionError(msg)

    def _read(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            self.raw.append(line)
            with self._cond:
                try:
                    self._frame(line)
                except AssertionError as e:
                    self._fail(str(e))
                except Exception as e:
                    self._fail(f"{type(e).__name__}: {e}")
                self._cond.notify_all()
        with self._cond:
            self._eof = True
            self._cond.notify_all()

    def _read_err(self) -> None:
        assert self.proc.stderr is not None
        for line in self.proc.stderr:
            self.stderr += line

    def _check(self, instance: Any, ref: str) -> None:
        if self.check is not None:
            self.check(instance, ref)

    def _frame(self, line: bytes) -> None:
        assert line.endswith(b"\n") and not line.endswith(b"\r\n"), f"bad frame ending: {line[-40:]!r}"
        msg = json.loads(line)  # anything on stdout that is not JSON fails here
        assert isinstance(msg, dict) and msg.get("jsonrpc") == "2.0", f"not a JSON-RPC 2.0 object: {msg!r}"
        self._check(msg, "jsonrpc.json#/$defs/Message")
        if "method" not in msg:
            self._response(msg)
            return
        m = msg["method"]
        params = msg.get("params") or {}
        if m in EVENTS:
            self._check(params, f"events.json#/$defs/{m}")
            aid = params.get("account_id")
            assert aid in self.started, f"{m} for account {aid!r}, which is not started (spec 4.2): {msg}"
            self.events.append(msg)
            self.all_events.append(msg)
            if "id" in msg and self.auto_answer:  # an event sent as a request: ack it (spec 12.2)
                self._auto_answer(msg)
            elif "id" in msg:
                self.requests.append(msg)
            return
        if m == CANCEL_METHOD and "id" not in msg:  # the plugin gave up on a request (spec 11.0)
            self.cancelled.append(params.get("id"))
            self.requests[:] = [r for r in self.requests if r["id"] != params.get("id")]
            return
        if m.startswith("x-") and "id" not in msg:  # a vendor event: the core ignores it (spec 12.3)
            self.events.append(msg)
            self.all_events.append(msg)
            return
        assert "id" in msg, f"unexpected notification from the plugin: {msg}"
        info = METHODS.get(m)
        assert info is not None and info.direction == "plugin_to_core", f"unexpected request: {msg}"
        self._check(params, f"methods/{info.schema}#/$defs/{m}.params")
        if not self.auto_answer:
            self.requests.append(msg)
            return
        if m in ("secrets.set", "secrets.delete"):
            store = self.secrets.setdefault(params["account_id"], {})
            if m == "secrets.set":
                store[params["key"]] = params["value"]
            else:
                store.pop(params["key"], None)
        self._auto_answer(msg)

    def _auto_answer(self, msg: dict[str, Any]) -> None:
        try:
            self.answer(msg)
        except (BrokenPipeError, ValueError):  # the plugin is gone or stdin closed
            pass

    def _response(self, msg: dict[str, Any]) -> None:
        rid = msg.get("id")
        method, params = self._pending.pop(rid, (None, {}))
        if "error" in msg:
            self._check(msg, "jsonrpc.json#/$defs/ErrorResponse")
            if method == "account.stop" and msg["error"].get("data", {}).get("code") == "UNKNOWN_ACCOUNT":
                self.started.discard(params.get("account_id"))
        else:
            info = METHODS.get(method) if method else None
            if info is not None:
                self._check(msg["result"], f"methods/{info.schema}#/$defs/{method}.result")
            if method == "account.start":
                self.started.add(params["account_id"])
            elif method == "account.stop":
                self.started.discard(params.get("account_id"))
        self._responses[rid] = msg
