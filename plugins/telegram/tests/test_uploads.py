import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from contact_hub_sdk import errors as rpc, types as dto
from telethon import functions, types as tl

from stub import NOW, document, row
from telegram_plugin import uploads
from telegram_plugin.media_info import waveform
from telegram_plugin.session import disconnect, make_client, session_path


def client():
    value = AsyncMock()
    value.get_input_entity.return_value = tl.InputPeerUser(17, 0)
    async def upload(stream, *, file_size, file_name, progress_callback):
        assert len(stream.read()) == file_size
        await progress_callback(file_size, file_size)
        return tl.InputFile(1, 1, file_name, "fixture")
    value.upload_file.side_effect = upload
    return value


def test_voice_upload_preserves_flags_caption_and_descriptor(tmp_path):
    selected = tmp_path / "fixture.ogg"
    selected.write_bytes(b"fixture")
    value, owner = client(), SimpleNamespace(progress=AsyncMock())
    held = []
    async def response(request):
        assert isinstance(request, functions.messages.SendMediaRequest)
        assert request.message == "**😀x**" and (request.entities[0].offset, request.entities[0].length) == (2, 3)
        assert request.silent is False and request.reply_to.top_msg_id == 42 and request.reply_to.reply_to_msg_id == 7
        media = request.media
        assert media.mime_type == "audio/ogg" and media.ttl_seconds == 8 and media.spoiler is False
        filename, audio = media.attributes
        assert filename.file_name == "note.ogg" and audio.duration == 7 and audio.voice
        assert waveform(audio.waveform) == [0, 31, 5]
        raw = row(20, out=True, media=tl.MessageMediaDocument(document=document(*media.attributes, mime=media.mime_type), ttl_seconds=8))
        raw.message = request.message
        return tl.Updates([tl.UpdateMessageID(20, request.random_id), tl.UpdateNewMessage(raw, 1, 1)], [], [], NOW, 1)
    value.side_effect = response
    original = value.upload_file.side_effect
    async def capture(stream, **kwargs):
        held.append(stream)
        return await original(stream, **kwargs)
    value.upload_file.side_effect = capture
    p = dto.MessagesSendFileParams(account_id="a", chat_id="17", path=str(selected), kind="voice", duration=7,
           waveform=[0, 31, 5], mime="audio/ogg", filename="note.ogg", caption="**😀x**",
           entities=[dto.Entity(type="bold", offset=2, length=3)], silent=False, spoiler=False, ttl_seconds=8,
           topic_id="42", reply_to=dto.ReplyTarget(message_id="7"))
    result = asyncio.run(uploads.send_file(owner, value, p)).message
    assert result.id == "20" and result.media.kind == "voice" and result.ttl_seconds == 8
    assert result.media.path is None and held[0].closed
    assert owner.progress.await_args.args == (7, 7)


def test_file_and_album_preflight_rejects_unsafe_paths_before_network(tmp_path, monkeypatch):
    selected = tmp_path / "fixture.bin"
    selected.write_bytes(b"fixture")
    alias = tmp_path / "alias"
    alias.symlink_to(selected)
    fifo = tmp_path / "pipe"
    import os
    os.mkfifo(fifo)
    value, owner = client(), SimpleNamespace(progress=AsyncMock())
    for path in ("https://example.invalid/file", "relative", str(alias), str(fifo), str(tmp_path)):
        with pytest.raises(rpc.InvalidParams):
            asyncio.run(uploads.send_file(owner, value, dto.MessagesSendFileParams(account_id="a", chat_id="17", path=path)))
    p = dto.MessagesSendAlbumParams(account_id="a", chat_id="17", items=[dto.AlbumItem(path=str(selected)), dto.AlbumItem(path=str(alias))])
    with pytest.raises(rpc.InvalidParams):
        asyncio.run(uploads.send_album(owner, value, p))
    monkeypatch.setattr(uploads, "MAX_BYTES", 3)
    with pytest.raises(rpc.NotSupported):
        asyncio.run(uploads.send_file(owner, value, dto.MessagesSendFileParams(account_id="a", chat_id="17", path=str(selected))))
    value.get_input_entity.assert_not_awaited()
    value.upload_file.assert_not_awaited()
    value.assert_not_awaited()


def test_file_mutation_or_cancel_does_not_send_media(tmp_path):
    async def exercise():
        selected = tmp_path / "fixture.bin"
        selected.write_bytes(b"fixture")
        value, owner, held = client(), SimpleNamespace(progress=AsyncMock()), []
        async def mutated(stream, **kwargs):
            held.append(stream)
            selected.write_bytes(b"changed-file")
            return tl.InputFile(1, 1, "fixture.bin", "fixture")
        value.upload_file.side_effect = mutated
        p = dto.MessagesSendFileParams(account_id="a", chat_id="17", path=str(selected))
        with pytest.raises(rpc.InvalidParams):
            await uploads.send_file(owner, value, p)
        value.assert_not_awaited()
        assert held[-1].closed
        async def cancelled(stream, **kwargs):
            held.append(stream)
            raise asyncio.CancelledError()
        value.upload_file.side_effect = cancelled
        with pytest.raises(asyncio.CancelledError):
            await uploads.send_file(owner, value, p)
        assert held[-1].closed
        value.assert_not_awaited()
    asyncio.run(exercise())


def test_real_telethon_upload_stream_interface_offline(tmp_path):
    async def exercise():
        value = make_client(session_path(tmp_path / "state"), 1, "0" * 32, None)
        value._call = AsyncMock(return_value=True)
        selected = tmp_path / "fixture.bin"
        selected.write_bytes(b"fixture")
        progress = AsyncMock()
        try:
            with uploads.local_file(str(selected)) as (stream, size, filename):
                result = await value.upload_file(stream, file_size=size, file_name=filename, progress_callback=progress)
            assert result.name == "fixture.bin"
            request = value._call.await_args.args[1]
            assert isinstance(request, functions.upload.SaveFilePartRequest) and request.bytes == b"fixture"
            progress.assert_awaited_with(7, 7)
        finally:
            await disconnect(value)
    asyncio.run(exercise())


def test_extensionless_document_preserves_display_name(tmp_path):
    async def exercise():
        selected = tmp_path / "README"
        selected.write_bytes(b"fixture")
        value, owner = client(), SimpleNamespace(progress=AsyncMock())
        p = dto.MessagesSendFileParams(account_id="a", chat_id="17", path=str(selected), filename="README")
        with uploads.local_file(p.path) as (stream, size, filename):
            spec = uploads.specification(stream, size, filename, p)
            media = await uploads.upload_media(owner, value, spec)
        assert value.upload_file.await_args.kwargs["file_name"] == "README.bin"
        assert media.attributes[0].file_name == "README"
    asyncio.run(exercise())
