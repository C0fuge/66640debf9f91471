"""Explicit bounded static previews; never fetch a sticker's main/animated file."""
import io
import os
import stat
import struct
import zlib

from contact_hub_sdk import errors as rpc, types as dto
from telethon import types as tl, utils

from .cache import atomic_file
from .downloads import DownloadQueue
from .stickers import catalog, integer

THUMB_LIMIT = 2 * 1024**2
STATIC_TYPES = frozenset("smxywabcd")


class Sink:
    def __init__(self):
        self.buffer = io.BytesIO()

    def write(self, data):
        if self.tell() + len(data) > THUMB_LIMIT:
            raise rpc.NotSupported("Sticker thumbnail exceeds the local size limit.", capability="stickers")
        return self.buffer.write(data)

    def tell(self):
        return self.buffer.tell()

    def flush(self):
        pass


def static_format(data):
    """Allow static image containers, without executing or decoding their content."""
    if data.startswith(b"\xff\xd8\xff") and data.endswith(b"\xff\xd9"):
        return ".jpg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        offset, count, image = 8, 0, False
        while offset + 12 <= len(data) and count < 1024:
            size = int.from_bytes(data[offset:offset + 4], "big")
            kind = data[offset + 4:offset + 8]
            end = offset + 12 + size
            if end > len(data) or kind == b"acTL":
                break
            body = data[offset + 4:end - 4]
            if zlib.crc32(body) != int.from_bytes(data[end - 4:end], "big"):
                break
            if count == 0 and (kind != b"IHDR" or size != 13):
                break
            if kind == b"IHDR":
                if count != 0 or size != 13:
                    break
                width, height = struct.unpack(">II", data[offset + 8:offset + 16])
                if not 0 < width <= 4096 or not 0 < height <= 4096:
                    break
            image |= kind == b"IDAT" and size > 0
            if kind == b"IEND":
                if size == 0 and end == len(data) and image:
                    return ".png"
                break
            offset, count = end, count + 1
    if len(data) >= 20 and data[:4] == b"RIFF" and data[8:12] == b"WEBP" and int.from_bytes(data[4:8], "little") == len(data) - 8:
        offset, count, image = 12, 0, False
        while offset + 8 <= len(data) and count < 1024:
            kind, size = data[offset:offset + 4], int.from_bytes(data[offset + 4:offset + 8], "little")
            end = offset + 8 + size
            if end > len(data) or kind in {b"ANIM", b"ANMF"}:
                break
            if kind == b"VP8X" and (size != 10 or data[offset + 8] & 2):
                break
            image |= kind in {b"VP8 ", b"VP8L"} and size > 0
            offset, count = end + size % 2, count + 1
            if offset == len(data) and image:
                return ".webp"
    raise rpc.NotSupported("Telegram did not return a supported static sticker thumbnail.", capability="stickers")


def choose(document):
    choices, stripped = [], []
    for size in document.thumbs or []:
        if isinstance(size, tl.PhotoStrippedSize) and size.type == "i":
            stripped.append(size)
        elif isinstance(size, (tl.PhotoSize, tl.PhotoCachedSize, tl.PhotoSizeProgressive)) and size.type in STATIC_TYPES:
            integer(size.w, minimum=1)
            integer(size.h, minimum=1)
            choices.append(size)
    # Prefer a real static preview to the extremely low-resolution stripped JPEG.
    return min(choices, key=lambda s: (s.w * s.h, s.type), default=None) or next(iter(stripped), None)


async def thumbnail(account, client, params):
    state = catalog(account)
    async with state.slot():
        document = state.resolve(params.sticker_id, "d")
        if not isinstance(document, tl.Document):
            raise rpc.Internal("The sticker reference is invalid.")
        selected = choose(document)
        if selected is None:
            return dto.PathResult()
        queue = account.data.setdefault("download_queue", DownloadQueue())
        async with queue.slot():
            sink = Sink()
            if isinstance(selected, tl.PhotoCachedSize):
                sink.write(selected.bytes)
            elif isinstance(selected, tl.PhotoStrippedSize):
                sink.write(utils.stripped_photo_to_jpg(selected.bytes))
            else:
                expected = integer(max(selected.sizes) if isinstance(selected, tl.PhotoSizeProgressive) and selected.sizes else
                                   selected.size if isinstance(selected, tl.PhotoSize) else 0, minimum=1)
                if expected > THUMB_LIMIT:
                    raise rpc.NotSupported("Sticker thumbnail exceeds the local size limit.", capability="stickers")

                async def progress(done, total):
                    await account.progress(int(done), int(total) if total is not None else None, direction="download")

                location = tl.InputDocumentFileLocation(document.id, document.access_hash,
                                                       document.file_reference, selected.type)
                # Pinned download_file writes to supplied streams and returns None
                # on success. Actual bytes/size, not that return value, confirm it.
                await client.download_file(location, file=sink, file_size=expected,
                                           dc_id=document.dc_id, progress_callback=progress)
                if sink.tell() != expected:
                    raise rpc.Internal("Telegram returned an incomplete sticker thumbnail.")
            data = sink.buffer.getvalue()
            if not data:
                raise rpc.NotFound(kind="thumbnail")
            suffix = static_format(data)
            # The download is bounded before publication; no temporary filesystem
            # artifact exists while the network operation can be cancelled.
            with atomic_file(account.cache_dir, suffix, limit=THUMB_LIMIT) as (stream, publish):
                stream.write(data)
                path = publish()
                before = os.fstat(stream.stream.fileno())
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                try:
                    after = os.fstat(fd)
                    if ((before.st_dev, before.st_ino, before.st_size) != (after.st_dev, after.st_ino, after.st_size)
                            or not stat.S_ISREG(after.st_mode) or after.st_nlink != 1
                            or stat.S_IMODE(after.st_mode) != 0o600 or after.st_uid != os.geteuid()):
                        raise rpc.Internal("Sticker thumbnail cache identity changed.")
                finally:
                    os.close(fd)
            return dto.PathResult(path=path)
