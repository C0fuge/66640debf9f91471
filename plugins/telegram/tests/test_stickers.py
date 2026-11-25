import asyncio
import copy
import os
import stat
import struct
import sys
import zlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from contact_hub_sdk import errors as rpc, types as dto
from contact_hub_sdk.testing import FakeCore, schema_check
from telethon import functions, types as tl
from telethon.extensions import BinaryReader

from sticker_stub import JPEG, pack_info, pack_response, sticker
from telegram_plugin import sticker_thumbs as thumbs, stickers as catalog
from telegram_plugin.session import disconnect, make_client, session_path


def owner(tmp_path):
    return SimpleNamespace(data={}, cache_dir=tmp_path / "cache", progress=AsyncMock())


class Client:
    def __init__(self, *responses):
        self.responses, self.calls = list(responses), []
        self.download_file = AsyncMock()

    async def __call__(self, request):
        with BinaryReader(bytes(request)) as reader:
            self.calls.append(reader.tgread_object())
        return self.responses.pop(0)


def test_all_catalog_methods_project_exact_models_and_account_bound_references(tmp_path):
    async def exercise():
        account = owner(tmp_path)
        docs = [sticker(), sticker(81, mime="application/x-tgsticker"), sticker(82, mime="video/webm", dimensions=False)]
        docs[1].video_thumbs = [tl.VideoSize("f", 512, 512, 7)]
        client = Client(tl.messages.RecentStickers(12, [], docs, [1, 2, 3]),
                        tl.messages.AllStickers(13, [pack_info(71), pack_info()]), pack_response())
        recent = await catalog.recent(account, client, dto.AccountLimitParams(account_id="a", limit=3))
        assert [s.format for s in recent.stickers] == ["static", "animated", "video"]
        assert recent.stickers[1].premium is True
        assert recent.stickers[2].width is None and recent.stickers[2].height is None
        assert recent.stickers[0].emoji == "🙂" and recent.stickers[0].width == 512
        assert all(s.path is None and s.thumb_path is None for s in recent.stickers)
        packs = await catalog.packs(account, client)
        assert [p.title for p in packs.packs] == ["Synthetic pack 71", "Synthetic pack 70"]
        assert all(p.installed is True and p.kind == "stickers" for p in packs.packs)
        pack = await catalog.pack(account, client, dto.PackParams(account_id="a", pack_id=packs.packs[1].id))
        assert len(pack.stickers) == pack.pack.count == 2
        assert pack.pack.id == recent.stickers[0].pack_id == packs.packs[1].id
        assert all(s.pack_id == pack.pack.id for s in pack.stickers)
        assert isinstance(client.calls[0], functions.messages.GetRecentStickersRequest)
        assert client.calls[0].hash == 0 and client.calls[0].attached is False
        assert client.calls[2].stickerset.id == 70 and client.calls[2].stickerset.access_hash == 71
        other = owner(tmp_path)
        for token, kind in [(pack.pack.id, "p"), (pack.stickers[0].id, "d")]:
            with pytest.raises(rpc.NotFound):
                catalog.catalog(other).resolve(token, kind)
        # Immutable bytes were retained, not mutable TL objects from the transport.
        docs[0].id = 999
        assert catalog.catalog(account).resolve(recent.stickers[0].id, "d").id == 80
    asyncio.run(exercise())


@pytest.mark.parametrize("method,response", [
    ("recent", tl.messages.RecentStickersNotModified()),
    ("packs", tl.messages.AllStickersNotModified()),
    ("pack", tl.messages.StickerSetNotModified()),
])
def test_not_modified_without_a_matching_baseline_never_becomes_empty(tmp_path, method, response):
    async def exercise():
        account, client = owner(tmp_path), Client(response)
        batch = catalog.Batch(catalog.catalog(account))
        token = batch.reference("p", tl.InputStickerSetID(70, 71))
        batch.commit({})
        with pytest.raises(rpc.Internal):
            if method == "recent":
                await catalog.recent(account, client, dto.AccountLimitParams(account_id="a"))
            elif method == "packs":
                await catalog.packs(account, client)
            else:
                await catalog.pack(account, client, dto.PackParams(account_id="a", pack_id=token))
        assert client.calls[0].hash == 0
    asyncio.run(exercise())


@pytest.mark.parametrize("defect", ["count", "foreign-pack", "foreign-document", "duplicate", "empty-document", "unknown-format", "emoji", "mask"])
def test_sparse_or_contradictory_pack_never_commits_a_partial_catalog(tmp_path, defect):
    async def exercise():
        account, response = owner(tmp_path), pack_response()
        state = catalog.catalog(account)
        batch = catalog.Batch(state)
        token = batch.reference("p", tl.InputStickerSetID(70, 71))
        batch.commit({})
        original = state.entries.copy()
        if defect == "count": response.set.count = 3
        elif defect == "foreign-pack": response.set.id = 71
        elif defect == "foreign-document": response.documents[0].attributes[0].stickerset.id = 71
        elif defect == "duplicate": response.documents[1] = copy.deepcopy(response.documents[0])
        elif defect == "empty-document": response.documents[0] = tl.DocumentEmpty(80)
        elif defect == "unknown-format": response.documents[0].mime_type = "application/unknown"
        elif defect == "emoji": response.set.emojis = True
        elif defect == "mask": response.set.masks = True
        with pytest.raises(rpc.PluginError):
            await catalog.pack(account, Client(response), dto.PackParams(account_id="a", pack_id=token))
        assert state.entries == original
    asyncio.run(exercise())


def test_lru_never_rebinds_evicted_ids_and_whole_batch_has_a_budget(monkeypatch):
    monkeypatch.setattr(catalog, "MAX_REFS", 2)
    state = catalog.Catalog()
    batch = catalog.Batch(state)
    first = batch.reference("p", tl.InputStickerSetID(1, 2))
    evicted = batch.reference("p", tl.InputStickerSetID(2, 3))
    batch.commit({})
    batch = catalog.Batch(state)
    assert batch.reference("p", tl.InputStickerSetID(1, 2)) == first
    third = batch.reference("p", tl.InputStickerSetID(3, 4))
    batch.commit({})
    with pytest.raises(rpc.NotFound): state.resolve(evicted, "p")
    batch = catalog.Batch(state)
    replacement = batch.reference("p", tl.InputStickerSetID(2, 3))
    batch.commit({})
    assert replacement != evicted
    with pytest.raises(rpc.NotFound): state.resolve(evicted, "p")
    original = state.entries.copy()
    batch = catalog.Batch(state)
    batch.reference("p", tl.InputStickerSetID(4, 5))
    batch.reference("p", tl.InputStickerSetID(5, 6))
    with pytest.raises(rpc.NotSupported): batch.reference("p", tl.InputStickerSetID(6, 7))
    assert state.entries == original
    assert third != replacement and len(state.entries) == 2


def test_result_and_byte_budgets_fail_without_eviction(monkeypatch):
    state = catalog.Catalog()
    batch = catalog.Batch(state)
    old = batch.reference("p", tl.InputStickerSetID(1, 2))
    batch.commit({})
    monkeypatch.setattr(catalog, "MAX_REF_BYTES", 20)
    batch = catalog.Batch(state)
    batch.reference("p", tl.InputStickerSetID(2, 3))
    with pytest.raises(rpc.NotSupported): batch.reference("p", tl.InputStickerSetID(3, 4))
    assert state.resolve(old, "p").id == 1
    monkeypatch.setattr(catalog, "MAX_RESULT_BYTES", 20)
    with pytest.raises(rpc.NotSupported): batch.commit({"oversized": "x" * 100})
    assert state.resolve(old, "p").id == 1


def test_static_thumbnail_identity_queue_and_none_returning_real_transport(tmp_path):
    async def exercise():
        account, state = owner(tmp_path), None
        state = catalog.catalog(account)
        batch = catalog.Batch(state)
        document = sticker(cached=False)
        token = catalog.sticker_info(document, batch).id
        batch.commit({})
        client = Client()
        async def download(location, *, file, file_size, dc_id, progress_callback):
            assert isinstance(location, tl.InputDocumentFileLocation)
            assert (location.id, location.access_hash, location.file_reference, location.thumb_size) == (80, 81, document.file_reference, "s")
            assert file_size == len(JPEG) and dc_id == 2
            file.write(JPEG)
            await progress_callback(len(JPEG), len(JPEG))
            # Exactly the pinned public download_file custom-stream result.
            return None
        client.download_file.side_effect = download
        result = await thumbs.thumbnail(account, client, dto.StickerParams(account_id="a", sticker_id=token))
        path = Path(result.path)
        assert path.is_relative_to(account.cache_dir) and path.read_bytes() == JPEG
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert not list(account.cache_dir.rglob("*.part"))
        assert account.data["download_queue"].active is False and state.waiters == 0
        client.download_file.reset_mock()
        with pytest.raises(rpc.NotFound):
            await thumbs.thumbnail(account, client, dto.StickerParams(account_id="a", sticker_id="https://example.invalid/file"))
        client.download_file.assert_not_awaited()
    asyncio.run(exercise())


@pytest.mark.parametrize("defect", ["cancel", "oversize", "incomplete", "html", "symlink"])
def test_failed_thumbnail_never_publishes_a_path_or_partial_file(tmp_path, defect):
    async def exercise():
        account, client = owner(tmp_path), Client()
        batch = catalog.Batch(catalog.catalog(account))
        token = catalog.sticker_info(sticker(cached=False), batch).id
        batch.commit({})
        async def download(location, *, file, **kwargs):
            if defect == "cancel":
                file.write(b"partial")
                raise asyncio.CancelledError()
            if defect == "oversize": file.write(b"x" * (thumbs.THUMB_LIMIT + 1))
            elif defect == "incomplete": file.write(JPEG[:-1])
            elif defect == "html": file.write(b"<html>xx")
            else: file.write(JPEG)
        client.download_file.side_effect = download
        outside = tmp_path / "outside"
        if defect == "symlink":
            outside.mkdir()
            account.cache_dir.symlink_to(outside, target_is_directory=True)
        with pytest.raises((rpc.PluginError, OSError, asyncio.CancelledError)):
            await thumbs.thumbnail(account, client, dto.StickerParams(account_id="a", sticker_id=token))
        if account.cache_dir.exists(): assert not list(account.cache_dir.rglob("*.part"))
        if outside.exists(): assert not list(outside.iterdir())
        assert catalog.catalog(account).waiters == 0 and account.data["download_queue"].active is False
    asyncio.run(exercise())


def test_only_static_thumbnails_are_selected_and_missing_is_honestly_optional(tmp_path):
    async def exercise():
        account, client = owner(tmp_path), Client()
        document = sticker(mime="application/x-tgsticker")
        document.thumbs = [tl.PhotoPathSize("j", b"synthetic outline")]
        document.video_thumbs = [tl.VideoSize("f", 512, 512, 7)]
        batch = catalog.Batch(catalog.catalog(account))
        token = catalog.sticker_info(document, batch).id
        batch.commit({})
        assert (await thumbs.thumbnail(account, client, dto.StickerParams(account_id="a", sticker_id=token))).path is None
        client.download_file.assert_not_awaited()
        assert not account.cache_dir.exists()
        for payload in [b"<svg/>", b"\x1f\x8bgzip TGS", b"\x1aE\xdf\xa3webm"]:
            with pytest.raises(rpc.NotSupported): thumbs.static_format(payload)
    asyncio.run(exercise())


def test_catalog_waiters_are_bounded_and_cancellation_releases_them():
    async def exercise():
        state = catalog.Catalog()
        async with state.slot():
            async def waiting():
                async with state.slot(): pass
            pending = [asyncio.create_task(waiting()) for _ in range(catalog.MAX_WAITERS - 1)]
            await asyncio.sleep(0)
            assert state.waiters == catalog.MAX_WAITERS
            with pytest.raises(rpc.Unavailable):
                async with state.slot(): pass
            for task in pending: task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            assert state.waiters == 1
        assert state.waiters == 0
    asyncio.run(exercise())


def test_real_telethon_custom_stream_download_is_offline_and_scope_exact(tmp_path):
    async def exercise():
        account = owner(tmp_path)
        batch = catalog.Batch(catalog.catalog(account))
        token = catalog.sticker_info(sticker(cached=False), batch).id
        batch.commit({})
        client = make_client(session_path(tmp_path / "state"), 1, "0" * 32, None)
        seen = []
        async def chunks(location, **kwargs):
            seen.append(location)
            yield JPEG[:3]
            yield JPEG[3:]
        client._iter_download = chunks
        try:
            result = await thumbs.thumbnail(account, client, dto.StickerParams(account_id="a", sticker_id=token))
            assert Path(result.path).read_bytes() == JPEG
            assert len(seen) == 1 and seen[0].thumb_size == "s" and seen[0].id == 80
        finally:
            await disconnect(client)
    asyncio.run(exercise())


def test_static_png_webp_and_animation_rejection():
    def chunk(kind, body):
        return struct.pack(">I", len(body)) + kind + body + struct.pack(">I", zlib.crc32(kind + body))
    ihdr = chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 6, 0, 0, 0))
    image = chunk(b"IDAT", zlib.compress(b"\0\0\0\0\0"))
    png = b"\x89PNG\r\n\x1a\n" + ihdr + image + chunk(b"IEND", b"")
    assert thumbs.static_format(png) == ".png"
    animated = b"\x89PNG\r\n\x1a\n" + ihdr + chunk(b"acTL", struct.pack(">II", 2, 0)) + image + chunk(b"IEND", b"")
    with pytest.raises(rpc.NotSupported): thumbs.static_format(animated)
    for bad in [png[:-1], png + b"tail", png.replace(b"IHDR", b"IDHR", 1)]:
        with pytest.raises(rpc.NotSupported): thumbs.static_format(bad)
    payload = b"WEBPVP8L" + (2).to_bytes(4, "little") + b"xx"
    webp = b"RIFF" + len(payload).to_bytes(4, "little") + payload
    assert thumbs.static_format(webp) == ".webp"
    animated_payload = b"WEBPANIM" + (2).to_bytes(4, "little") + b"xx"
    with pytest.raises(rpc.NotSupported):
        thumbs.static_format(b"RIFF" + len(animated_payload).to_bytes(4, "little") + animated_payload)


def test_unrelated_document_attributes_are_not_retained_as_reference_payload(tmp_path):
    raw = sticker()
    raw.attributes.append(tl.DocumentAttributeFilename("x" * 8192))
    raw.attributes[0].alt = ""
    state = catalog.catalog(owner(tmp_path))
    batch = catalog.Batch(state)
    result = catalog.sticker_info(raw, batch)
    batch.commit({})
    assert result.emoji is None
    retained = state.resolve(result.id, "d")
    assert not any(isinstance(a, tl.DocumentAttributeFilename) for a in retained.attributes)
    assert sum(len(blob) for _, blob in state.entries.values()) < 1024


def test_schema_checked_sticker_wire_family_and_reference_lifetime(tmp_path):
    root = Path(__file__).resolve().parents[1]
    bootstrap = """
import os,sys
def forbid(event,args):
    if event=='socket.connect':raise AssertionError('No network in sticker tests')
sys.addaudithook(forbid)
os.umask(0o077)
sys.path.insert(0,'tests')
from sticker_stub import StickerStub
from telegram_plugin.plugin import TelegramPlugin
TelegramPlugin(factory=StickerStub).run()
"""
    with FakeCore([sys.executable, "-c", bootstrap], plugin_dir=root, tmp=tmp_path,
                  env={**os.environ, "HOME": str(tmp_path / "home")},
                  check=schema_check(root.parents[1] / "schemas")) as core:
        assert core.initialize()["capabilities"]["stickers"] is True
        for account in ["a", "b"]:
            core.start(account, {"api_id": 1}, {"api_hash": "0" * 32})
            core.event("status", account_id=account, state="connected")
        recent = core.call("stickers.recent", {"account_id": "a", "limit": 1})["stickers"]
        assert len(recent) == 1 and recent[0]["format"] == "static"
        packs = core.call("stickers.packs", {"account_id": "a"})["packs"]
        result = core.call("stickers.pack", {"account_id": "a", "pack_id": packs[1]["id"]})
        assert result["pack"]["id"] == recent[0]["pack_id"]
        assert len(result["stickers"]) == 2
        image = core.call("stickers.thumb", {"account_id": "a", "sticker_id": recent[0]["id"]})
        assert Path(image["path"]).read_bytes() == JPEG
        assert core.error("stickers.thumb", {"account_id": "a", "sticker_id": packs[0]["id"]})["data"]["code"] == "NOT_FOUND"
        assert core.error("stickers.pack", {"account_id": "a", "pack_id": recent[0]["id"]})["data"]["code"] == "NOT_FOUND"
        assert not any(field in sticker for sticker in result["stickers"] for field in ["file_reference", "access_hash"])
        for method, field, token in [("stickers.pack", "pack_id", packs[0]["id"]),
                                     ("stickers.thumb", "sticker_id", recent[0]["id"])]:
            assert core.error(method, {"account_id": "b", field: token})["data"]["code"] == "NOT_FOUND"
        core.stop("a")
        core.event("status", account_id="a", state="disconnected")
        core.start("a", {"api_id": 1}, {"api_hash": "0" * 32})
        core.event("status", account_id="a", state="connected")
        assert core.error("stickers.thumb", {"account_id": "a", "sticker_id": recent[0]["id"]})["data"]["code"] == "NOT_FOUND"
        assert core.error("stickers.clip", {"account_id": "a", "sticker_id": recent[0]["id"]})["data"]["code"] == "NOT_SUPPORTED"
        assert core.shutdown() == 0
