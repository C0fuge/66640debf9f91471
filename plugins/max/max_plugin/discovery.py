"""Bounded, non-authoritative chat discovery from server-issued markers.

Evidence: MAX's public web client D0OzoqHp.js, captured 2026-11-25 Moscow,
https://web.max.ru/_app/immutable/chunks/D0OzoqHp.js
SHA256 3d300974d8508a3674a5f56e4be618249fb4dea8661044398fbc96f91b32a181.
Its actual command-53 ack schema requires chats/marker, and the login-seeded
loop follows returned markers until zero. This is original adapter logic; no
bundled source is copied and no deletion-authoritative snapshot is inferred.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from contact_hub_sdk import PluginError

MAX_CHAT_PAGES = 128
MAX_CHAT_RECORDS = 10000
CHAT_DISCOVERY_SECONDS = 300
CHAT_PAGE_DELAY = 2


def marker(value: Any) -> int:
    # The pinned request model uses int. Do not coerce bool/string/float/null,
    # manufacture a timestamp, or infer a direction from this opaque value.
    if type(value) is not int:
        raise PluginError("MAX returned an invalid chat continuation marker")
    return value


async def drain_chat_pages(
    invoke: Callable[..., Awaitable[Any]],
    initial: list,
    continuation: int | None,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> list:
    """Materialize only validated pages; None means unknown, never exhausted.

    The caller publishes only after this function succeeds. Failure/cancellation
    does not expose a partially drained snapshot as a successful discovery.
    Budgets are adapter safety limits, not claims about MAX server limits.
    """
    from pymax.api.chats.payloads import FetchChatsPayload
    from pymax.protocol import Opcode
    from pymax.types.domain.chat import Chat

    if len(initial) > MAX_CHAT_RECORDS:
        raise PluginError("MAX chat discovery record budget exceeded")
    chats = {chat.id: chat for chat in initial}
    if continuation is None:
        return list(chats.values())
    current = marker(continuation)
    seen: set[int] = set()
    records, pages = len(initial), 0
    async with asyncio.timeout(CHAT_DISCOVERY_SECONDS):
        while current != 0:
            if current in seen:
                raise PluginError("MAX chat continuation repeated; discovery remains partial")
            if pages >= MAX_CHAT_PAGES:
                raise PluginError("MAX chat discovery page budget exceeded")
            seen.add(current)
            await sleep(CHAT_PAGE_DELAY)
            response = await invoke(Opcode.CHATS_LIST, FetchChatsPayload(marker=current).to_payload())
            payload = response.payload
            if not isinstance(payload, dict) or "chats" not in payload or "marker" not in payload:
                raise PluginError("MAX chat page is missing required chats or marker")
            if not isinstance(payload["chats"], list):
                raise PluginError("MAX chat page has an invalid chats field")
            following = marker(payload["marker"])
            if following != 0 and following in seen:
                raise PluginError("MAX chat continuation repeated; discovery remains partial")
            records += len(payload["chats"])
            if records > MAX_CHAT_RECORDS:
                raise PluginError("MAX chat discovery record budget exceeded")
            try:
                page = [Chat.model_validate(row) for row in payload["chats"]]
            except Exception:
                raise PluginError("MAX chat page contains an invalid conversation") from None
            for chat in page:
                chats[chat.id] = chat
            pages += 1
            current = following
    return list(chats.values())
