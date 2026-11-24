"""Errors (spec section 13). Raise these from a handler; the SDK turns them into the
JSON-RPC error object. Any other exception becomes ``INTERNAL``."""

from __future__ import annotations

from typing import Any

__all__ = [
    "NUMERIC_CODES",
    "AuthRequired",
    "Cancelled",
    "FloodWait",
    "Forbidden",
    "Internal",
    "InvalidParams",
    "MethodNotFound",
    "NetworkError",
    "NotFound",
    "NotSupported",
    "PluginError",
    "RpcTimeout",
    "Unavailable",
    "UnknownAccount",
]

#: data.code -> numeric JSON-RPC code. The mapping is fixed by the spec.
NUMERIC_CODES: dict[str, int] = {
    "PARSE_ERROR": -32700,
    "INVALID_REQUEST": -32600,
    "METHOD_NOT_FOUND": -32601,
    "INVALID_PARAMS": -32602,
    "INTERNAL": -32603,
    "CANCELLED": -32800,
    "NOT_SUPPORTED": -32001,
    "AUTH_REQUIRED": -32002,
    "FLOOD_WAIT": -32003,
    "NOT_FOUND": -32004,
    "FORBIDDEN": -32005,
    "NETWORK": -32006,
    "TIMEOUT": -32007,
    "UNKNOWN_ACCOUNT": -32008,
    "UNAVAILABLE": -32009,
}
_BY_NUMBER = {v: k for k, v in NUMERIC_CODES.items()}

_DEFAULT_MESSAGES = {
    "PARSE_ERROR": "Parse error",
    "INVALID_REQUEST": "Invalid request",
    "METHOD_NOT_FOUND": "Method not found",
    "INVALID_PARAMS": "Invalid params",
    "INTERNAL": "Internal error",
    "CANCELLED": "Request cancelled",
    "NOT_SUPPORTED": "Not supported",
    "AUTH_REQUIRED": "Sign-in required",
    "FLOOD_WAIT": "Too many requests",
    "NOT_FOUND": "Not found",
    "FORBIDDEN": "Forbidden",
    "NETWORK": "Network error",
    "TIMEOUT": "Timed out",
    "UNKNOWN_ACCOUNT": "Unknown account",
    "UNAVAILABLE": "Account unavailable",
}


class PluginError(Exception):
    """An error with a spec ``data.code``. ``data`` holds the code-specific extras.

    Also raised by :meth:`Account.request` when the core answers with an error; then
    ``code`` may be a value this SDK does not know (treat it as ``INTERNAL``).
    """

    code: str = "INTERNAL"

    def __init__(self, message: str | None = None, *, code: str | None = None, **data: Any):
        if code is not None:
            self.code = code
        self.message = message or _DEFAULT_MESSAGES.get(self.code, self.code)
        self.data = {k: v for k, v in data.items() if v is not None}
        super().__init__(self.message)

    @property
    def numeric(self) -> int:
        return NUMERIC_CODES.get(self.code, NUMERIC_CODES["INTERNAL"])

    def to_wire(self) -> dict[str, Any]:
        return {"code": self.numeric, "message": self.message, "data": {"code": self.code, **self.data}}

    @classmethod
    def from_wire(cls, err: Any) -> PluginError:
        """Build from a JSON-RPC error object (tolerates a missing ``data``)."""
        if not isinstance(err, dict):
            return Internal(f"malformed error object: {err!r}")
        data = dict(err.get("data") or {})
        code = data.pop("code", None) or _BY_NUMBER.get(err.get("code"), "INTERNAL")
        sub = _BY_CODE.get(code)
        e = (sub or PluginError).__new__(sub or PluginError)
        PluginError.__init__(e, str(err.get("message") or ""), code=code, **data)
        return e

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.code}, {self.message!r}, {self.data!r})"


class InvalidParams(PluginError):
    code = "INVALID_PARAMS"

    def __init__(self, message: str | None = None, *, field: str | None = None):
        super().__init__(message, field=field)


class Internal(PluginError):
    code = "INTERNAL"


class MethodNotFound(PluginError):
    code = "METHOD_NOT_FOUND"


class Cancelled(PluginError):
    code = "CANCELLED"


class NotSupported(PluginError):
    """The platform or plugin cannot do this. Name the missing ``capability`` if any."""

    code = "NOT_SUPPORTED"

    def __init__(self, message: str | None = None, *, capability: str | None = None):
        super().__init__(message, capability=capability)


class AuthRequired(PluginError):
    code = "AUTH_REQUIRED"


class FloodWait(PluginError):
    """Rate-limited by the platform. ``seconds`` is required by the spec."""

    code = "FLOOD_WAIT"

    def __init__(self, seconds: int | float, message: str | None = None):
        seconds = max(0, int(-(-seconds // 1)))  # round up, never negative
        super().__init__(message, seconds=seconds)

    @property
    def seconds(self) -> int:
        return int(self.data.get("seconds", 0))


class NotFound(PluginError):
    code = "NOT_FOUND"

    def __init__(self, message: str | None = None, *, kind: str | None = None):
        super().__init__(message, kind=kind)


class Forbidden(PluginError):
    code = "FORBIDDEN"

    def __init__(self, message: str | None = None, *, reason: str | None = None):
        super().__init__(message, reason=reason)


class NetworkError(PluginError):
    code = "NETWORK"


class RpcTimeout(PluginError):
    code = "TIMEOUT"


class UnknownAccount(PluginError):
    code = "UNKNOWN_ACCOUNT"


class Unavailable(PluginError):
    code = "UNAVAILABLE"


_BY_CODE: dict[str, type[PluginError]] = {
    c.code: c
    for c in (
        InvalidParams,
        Internal,
        MethodNotFound,
        Cancelled,
        NotSupported,
        AuthRequired,
        FloodWait,
        NotFound,
        Forbidden,
        NetworkError,
        RpcTimeout,
        UnknownAccount,
        Unavailable,
    )
}


def error_from_exception(exc: BaseException) -> PluginError:
    """Map an arbitrary exception to a spec error."""
    if isinstance(exc, PluginError):
        return exc
    if isinstance(exc, NotImplementedError):
        return NotSupported(str(exc) or None)
    if isinstance(exc, TimeoutError):
        return RpcTimeout(str(exc) or None)
    if isinstance(exc, ConnectionError):
        return NetworkError(f"{type(exc).__name__}: {exc}")
    return Internal(f"{type(exc).__name__}: {exc}")
