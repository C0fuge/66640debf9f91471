"""Fail-closed interpretation of optional capability promises, not cleanup authority."""
from collections.abc import Mapping
from typing import Any


def file_source_released_on_success(capabilities: Mapping[str, Any]) -> bool:
    """Recognize only API 1.2's exact send.file source-lifetime promise.

    Upload support remains independent. A successful attempt, source identity,
    reference ownership and live-reader exclusion still have to be established.
    """
    flag = capabilities.get("send.file")
    return isinstance(flag, dict) and type(flag.get("source_release")) is str and flag["source_release"] == "on_success"
