"""Wire conventions (spec section 8): DTO <-> JSON value conversion and timestamps.

- ``from_wire(cls, value)`` builds a dataclass from a JSON object: unknown fields are
  ignored, ``null`` is the same as absent, unknown enum strings stay plain ``str``,
  timestamps accept any RFC 3339 date-time.
- ``to_wire(obj)`` turns dataclasses, enums, datetimes, lists and dicts into JSON values:
  ``None`` is omitted (never written as ``null``), absent arrays stay absent (``[]`` is
  kept), non-nullable defaults (``"edited": false``) are written, timestamps are UTC with
  exactly three fractional digits and ``Z``.
"""

from __future__ import annotations

import dataclasses
import re
import types as _pytypes
import typing
from datetime import UTC, datetime, timedelta, timezone
from enum import Enum
from functools import cache
from typing import Any, TypeVar, Union

__all__ = ["DecodeError", "format_timestamp", "from_wire", "parse_timestamp", "to_wire"]

T = TypeVar("T")

_TS = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?([Zz]|[+-]\d{2}:\d{2})$"
)


class DecodeError(ValueError):
    """A JSON value does not fit the expected type. ``field`` is a dotted path."""

    def __init__(self, field: str, message: str):
        super().__init__(f"{field or '<root>'}: {message}")
        self.field = field
        self.reason = message


def parse_timestamp(s: str) -> datetime:
    """Any RFC 3339 date-time -> aware UTC datetime, truncated to milliseconds."""
    m = _TS.match(s)
    if not m:
        raise ValueError(f"not an RFC 3339 date-time: {s!r}")
    y, mo, d, h, mi, sec, frac, off = m.groups()
    ms = int((frac or "0")[:3].ljust(3, "0"))
    second = int(sec)
    if second == 60:  # leap second: clamp to the last representable instant
        second, ms = 59, 999
    if off in ("Z", "z"):
        tz: timezone = UTC
    else:
        sign = 1 if off[0] == "+" else -1
        tz = timezone(sign * timedelta(hours=int(off[1:3]), minutes=int(off[4:6])))
    dt = datetime(int(y), int(mo), int(d), int(h), int(mi), second, ms * 1000, tzinfo=tz)
    return dt.astimezone(UTC)


def format_timestamp(dt: datetime) -> str:
    """``2026-10-19T14:03:27.512Z``. A naive datetime is taken to be UTC."""
    dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{dt.microsecond // 1000:03d}Z"


# --- encoding ---------------------------------------------------------------------


def to_wire(v: Any) -> Any:
    """Convert a value (dataclass, enum, datetime, list, dict, scalar) to a JSON value."""
    if v is None or isinstance(v, (bool, int, float)) and not isinstance(v, Enum):
        return v
    if isinstance(v, Enum):
        return v.value
    if isinstance(v, str):
        return str(v)
    if isinstance(v, datetime):
        return format_timestamp(v)
    if dataclasses.is_dataclass(v) and not isinstance(v, type):
        out = {}
        for f in dataclasses.fields(v):
            x = getattr(v, f.name)
            if x is None:
                continue
            out[f.name] = to_wire(x)
        return out
    if isinstance(v, dict):
        return {str(k): to_wire(x) for k, x in v.items() if x is not None}
    if isinstance(v, (list, tuple)):
        return [to_wire(x) for x in v]
    raise TypeError(f"cannot put {type(v).__name__} on the wire")


# --- decoding ---------------------------------------------------------------------


@cache
def _hints(cls: type) -> dict[str, Any]:
    return typing.get_type_hints(cls)


@cache
def _required(cls: type) -> frozenset[str]:
    return frozenset(
        f.name
        for f in dataclasses.fields(cls)
        if f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING
    )


def from_wire(cls: type[T], value: Any, path: str = "") -> T:
    """Build dataclass ``cls`` from a JSON object. Raises :class:`DecodeError`."""
    if not isinstance(value, dict):
        raise DecodeError(path, "expected an object")
    hints = _hints(cls)
    required = _required(cls)
    kwargs: dict[str, Any] = {}
    for f in dataclasses.fields(cls):  # type: ignore[arg-type]
        raw = value.get(f.name)
        sub = f"{path}.{f.name}" if path else f.name
        if raw is None:  # absent and null are the same (spec 8.4)
            if f.name in required:
                raise DecodeError(sub, "required")
            continue
        kwargs[f.name] = _decode(hints[f.name], raw, sub)
    return cls(**kwargs)


def _decode(tp: Any, raw: Any, path: str) -> Any:
    origin = typing.get_origin(tp)
    if origin is Union or origin is _pytypes.UnionType:
        alts = [a for a in typing.get_args(tp) if a is not type(None)]
        enums = [a for a in alts if isinstance(a, type) and issubclass(a, Enum)]
        if enums:
            if not isinstance(raw, str):
                raise DecodeError(path, "expected a string")
            for e in enums:
                try:
                    return e(raw)
                except ValueError:
                    pass
            return raw  # unknown enum value: keep it verbatim (spec 18)
        if len(alts) == 1:
            return _decode(alts[0], raw, path)
        for a in alts:  # scalar unions such as int | str
            try:
                return _decode(a, raw, path)
            except DecodeError:
                continue
        raise DecodeError(path, f"expected {tp}")
    if tp is Any:
        return raw
    if origin is list:
        if not isinstance(raw, list):
            raise DecodeError(path, "expected an array")
        (it,) = typing.get_args(tp)
        return [_decode(it, x, f"{path}[{i}]") for i, x in enumerate(raw)]
    if origin is dict:
        if not isinstance(raw, dict):
            raise DecodeError(path, "expected an object")
        _, vt = typing.get_args(tp)
        return {k: _decode(vt, x, f"{path}.{k}") for k, x in raw.items() if x is not None}
    if tp is datetime:
        if not isinstance(raw, str):
            raise DecodeError(path, "expected a timestamp string")
        try:
            return parse_timestamp(raw)
        except ValueError as e:
            raise DecodeError(path, str(e)) from None
    if tp is bool:
        if not isinstance(raw, bool):
            raise DecodeError(path, "expected a boolean")
        return raw
    if tp is int:
        if isinstance(raw, bool) or not isinstance(raw, int):
            if isinstance(raw, float) and raw.is_integer():
                return int(raw)
            raise DecodeError(path, "expected an integer")
        return raw
    if tp is float:
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise DecodeError(path, "expected a number")
        return raw
    if tp is str:
        if not isinstance(raw, str):
            raise DecodeError(path, "expected a string")
        return raw
    if isinstance(tp, type) and issubclass(tp, Enum):
        return _decode(tp | str, raw, path)
    if dataclasses.is_dataclass(tp):
        return from_wire(tp, raw, path)  # type: ignore[arg-type]
    raise TypeError(f"unsupported annotation {tp!r} at {path}")
