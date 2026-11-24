import asyncio
import os
import stat
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from contact_hub_sdk import errors as rpc, types as dto
from telethon import types as tl, utils

from stub import document, photo, row
from telegram_plugin import downloads
from telegram_plugin.cache import atomic_file
from telegram_plugin.mapping import message
from telegram_plugin.media_info import document_info, photo_info, waveform


def account(tmp_path):
    return SimpleNamespace(data={}, cache_dir=tmp_path / "cache", progress=AsyncMock(), media_ready=AsyncMock(), avatar_ready=AsyncMock())


def client(raw):
    return SimpleNamespace(get_input_entity=AsyncMock(return_value=tl.InputPeerUser(17, 0)),
                           get_messages=AsyncMock(return_value=raw), download_media=AsyncMock())


def test_metadata_preserves_file_kinds_and_packed_waveform():
    samples = bytes(range(32))
    assert waveform(utils.encode_waveform(samples)) == list(samples)
    voice = document(tl.DocumentAttributeAudio(5, voice=True, waveform=utils.encode_waveform(samples)),
                     tl.DocumentAttributeFilename("note.ogg"), mime="audio/ogg")
    mapped = message(row(100, media=tl.MessageMediaDocument(document=voice, spoiler=True))).media
    assert (mapped.kind, mapped.duration, mapped.mime, mapped.bytes, mapped.filename) == ("voice", 5, "audio/ogg", 7, "note.ogg")
    assert mapped.waveform == list(samples) and mapped.has_spoiler
    for attributes, kind in [([tl.DocumentAttributeVideo(3.5, 640, 480)], "video"),
                              ([tl.DocumentAttributeVideo(3, 64, 64, round_message=True)], "video_note"),
                              ([tl.DocumentAttributeAnimated(), tl.DocumentAttributeVideo(2, 16, 16)], "gif"),
                              ([tl.DocumentAttributeAudio(4, title="Track", performer="Fixture")], "music"),
                              ([], "document")]:
        assert document_info(document(*attributes)).kind == kind
    image = photo_info(photo(), True)
    assert (image.width, image.height, image.bytes, image.has_spoiler) == (40, 30, 7, True)
    assert image.path is None and image.thumb_path is None


def test_cache_rejects_symlink_components_and_cleans_failed_writes(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "cache").symlink_to(outside, target_is_directory=True)
    with pytest.raises(OSError):
        with atomic_file(tmp_path / "cache"):
            pass
    assert not list(outside.iterdir())
    (tmp_path / "cache").unlink()
    (tmp_path / "cache").mkdir()
    (tmp_path / "cache" / "telegram-media").symlink_to(outside, target_is_directory=True)
    with pytest.raises(OSError):
        with atomic_file(tmp_path / "cache"):
            pass
    assert not list(outside.iterdir())
    (tmp_path / "cache" / "telegram-media").unlink()
    with pytest.raises(RuntimeError):
        with atomic_file(tmp_path / "cache") as (stream, _):
            stream.write(b"partial")
            raise RuntimeError("fixture failure")
    assert not list((tmp_path / "cache" / "telegram-media").iterdir())


def test_cache_limits_and_atomic_publish(tmp_path):
    with pytest.raises(rpc.NotSupported):
        with atomic_file(tmp_path / "cache", limit=3) as (stream, _):
            stream.write(b"1234")
    with atomic_file(tmp_path / "cache") as (stream, publish):
        stream.write(b"complete")
        path = Path(publish())
        assert path.read_bytes() == b"complete" and path.is_relative_to(tmp_path / "cache")
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert not list(path.parent.glob("*.part"))


def test_download_scopes_and_cancellation_leave_no_partial(tmp_path):
    async def exercise():
        raw = row(100, media=tl.MessageMediaDocument(document=document(tl.DocumentAttributeFilename("../../outside"))))
        value, owner = client(raw), account(tmp_path)
        p = dto.MediaDownloadParams(account_id="a", chat_id="17", message_id="100", what="full", priority=5)
        async def transport(raw, *, file, thumb, progress_callback):
            assert thumb is None
            file.write(b"fixture")
            await progress_callback(7, 7)
            return file
        value.download_media.side_effect = transport
        result = await downloads.download(owner, value, p)
        assert Path(result.path).read_bytes() == b"fixture" and result.bytes == 7
        owner.media_ready.assert_awaited_with("17", "100", path=result.path)
        Path(result.path).unlink()  # Core cache eviction must be recoverable.
        assert Path((await downloads.download(owner, value, p)).path).exists()
        async def cancelled(raw, *, file, **kwargs):
            file.write(b"partial")
            raise asyncio.CancelledError()
        value.download_media.side_effect = cancelled
        with pytest.raises(asyncio.CancelledError):
            await downloads.download(owner, value, p)
        assert not list(owner.cache_dir.rglob("*.part"))
        value.get_messages.return_value = row(100, tl.PeerUser(18), media=raw.media)
        value.download_media.reset_mock()
        with pytest.raises(rpc.NotFound):
            await downloads.download(owner, value, p)
        value.download_media.assert_not_awaited()
    asyncio.run(exercise())


def test_download_rejects_external_media_and_uses_static_thumbnails(tmp_path):
    async def exercise():
        raw = row(101, media=tl.MessageMediaPhoto(photo=photo()))
        value, owner = client(raw), account(tmp_path)
        async def transport(raw, *, file, thumb, **kwargs):
            assert isinstance(thumb, tl.PhotoSize) and thumb.type == "s"
            file.write(b"jpg")
            return file
        value.download_media.side_effect = transport
        p = dto.MediaDownloadParams(account_id="a", chat_id="17", message_id="101", what="thumb")
        result = await downloads.download(owner, value, p)
        assert result.mime == "image/jpeg"
        owner.media_ready.assert_awaited_with("17", "101", thumb_path=result.path)
        value.get_messages.return_value = row(101, media=tl.MessageMediaWebPage(webpage=tl.WebPageEmpty(1)))
        value.download_media.reset_mock()
        with pytest.raises(rpc.NotFound):
            await downloads.download(owner, value, p)
        value.download_media.assert_not_awaited()
    asyncio.run(exercise())


def test_download_queue_priority_and_cancelled_waiter():
    async def exercise():
        queue, order = downloads.DownloadQueue(), []
        async def worker(label, priority):
            async with queue.slot(priority):
                order.append(label)
        async with queue.slot():
            low = asyncio.create_task(worker("low", -1))
            cancelled = asyncio.create_task(worker("cancelled", 99))
            high = asyncio.create_task(worker("high", 4))
            await asyncio.sleep(0)
            cancelled.cancel()
            await asyncio.gather(cancelled, return_exceptions=True)
        await asyncio.gather(low, high)
        assert order == ["high", "low"]
    asyncio.run(exercise())
