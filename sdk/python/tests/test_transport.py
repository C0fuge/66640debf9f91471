"""Cancellation can arrive at any point between dispatch and the end of a reply."""

import asyncio
import json

import pytest

from contact_hub_sdk.transport import Connection, encode_frame


@pytest.mark.parametrize("phase", ["queued", "writing"])
def test_immediate_cancel_always_finishes_one_reply(phase):
    async def exercise():
        replies = []
        writing = asyncio.Event()
        release = asyncio.Event()

        async def write(frame):
            writing.set()
            await release.wait()
            replies.append(json.loads(frame))

        async def ping(ctx):
            assert phase != "queued", "a queued cancellation must stop the handler"
            return {}

        conn = Connection(asyncio.StreamReader(), write, ping)
        await conn._dispatch(encode_frame({"jsonrpc": "2.0", "id": 7, "method": "ping"}))
        if phase == "writing":
            await asyncio.wait_for(writing.wait(), 1)
        await conn._dispatch(encode_frame({
            "jsonrpc": "2.0", "method": "$/cancelRequest", "params": {"id": 7},
        }))
        release.set()
        await conn.finish_inflight(1)

        assert not conn._inflight
        reply, = replies
        assert reply["id"] == 7
        if phase == "queued":
            assert reply["error"]["data"]["code"] == "CANCELLED"
        else:
            assert reply["result"] == {}

    asyncio.run(exercise())
