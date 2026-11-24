"""Requested MTProto attachment/avatar downloads into each account's cache."""
import asyncio
import heapq
import itertools
from contextlib import asynccontextmanager

from contact_hub_sdk import errors as rpc, types as dto
from telethon import types as tl

from .actions import fetch
from .cache import MAX_BYTES, atomic_file
from .mapping import identifier, resolve_chat
from .media_info import media_info, photo_size


class DownloadQueue:
    """One active transfer per account; larger priorities lead queued transfers."""
    def __init__(self):
        self.waiting, self.sequence, self.active = [], itertools.count(), False

    def wake(self):
        while not self.active and self.waiting:
            _, _, entry = heapq.heappop(self.waiting)
            if not entry[0].cancelled():
                self.active, entry[1] = True, True
                entry[0].set_result(None)

    @asynccontextmanager
    async def slot(self, priority=0):
        entry = [asyncio.get_running_loop().create_future(), False]
        heapq.heappush(self.waiting, (-priority, next(self.sequence), entry))
        self.wake()
        try:
            await entry[0]
            yield
        finally:
            if entry[1]:
                self.active = False
            else:
                entry[0].cancel()
            self.wake()


def attachment(raw, what):
    media = getattr(raw, "media", None)
    if isinstance(media, tl.MessageMediaPhoto) and isinstance(media.photo, tl.Photo):
        sizes = media.photo.sizes
        selected = photo_size(media.photo) if what == "full" else next((s for s in sizes if isinstance(s, (tl.PhotoSize, tl.PhotoCachedSize, tl.PhotoStrippedSize, tl.PhotoSizeProgressive))), None)
    elif isinstance(media, tl.MessageMediaDocument) and isinstance(media.document, tl.Document):
        selected = None if what == "full" else next((s for s in media.document.thumbs or [] if isinstance(s, (tl.PhotoSize, tl.PhotoCachedSize, tl.PhotoStrippedSize, tl.PhotoSizeProgressive))), None)
    else:
        # In particular, never give WebDocument URLs to Telethon's HTTP helper.
        raise rpc.NotFound(kind="media")
    if what == "thumb" and selected is None:
        raise rpc.NotFound(kind="thumbnail")
    if what == "full" and isinstance(media, tl.MessageMediaPhoto) and selected is None:
        raise rpc.NotFound(kind="media")
    return media, selected


async def download(account, client, p):
    if p.what not in ("full", "thumb"):
        raise rpc.InvalidParams(field="what")
    queue = account.data.setdefault("download_queue", DownloadQueue())
    async with queue.slot(p.priority or 0):
        peer = await resolve_chat(client, p.chat_id)
        raw, item = await fetch(client, peer, p.chat_id, identifier(p.message_id, "message_id"))
        if item is None:
            raise rpc.NotFound(kind="message")
        media, selected = attachment(raw, p.what)
        info = media_info(media)
        expected = info.bytes if p.what == "full" else None
        if isinstance(selected, tl.PhotoSize):
            expected = selected.size
        elif isinstance(selected, tl.PhotoSizeProgressive):
            expected = max(selected.sizes, default=0)
        elif isinstance(selected, tl.PhotoCachedSize):
            expected = len(selected.bytes)
        if expected is not None and expected > MAX_BYTES:
            raise rpc.NotSupported("This attachment exceeds the download size limit.", capability="media.download")
        mime = "image/jpeg" if p.what == "thumb" else info.mime

        async def progress(done, total):
            await account.progress(int(done), int(total) if total is not None else None, direction="download")

        with atomic_file(account.cache_dir, ".jpg" if mime == "image/jpeg" else ".bin") as (stream, publish):
            await account.progress(0, None, direction="download", force=True)
            # An explicit static photo size avoids accidentally downloading a
            # Photo.video_sizes entry as the requested thumbnail/full image.
            result = await client.download_media(raw, file=stream, thumb=selected, progress_callback=progress)
            if result is None or (stream.size == 0 and expected != 0):
                raise rpc.NotFound(kind="media")
            if expected is not None and stream.size != expected:
                raise rpc.Internal("Telegram returned an incomplete attachment.")
            path = publish()
            size = stream.size
        await account.progress(size, size, direction="download", force=True)
        await account.media_ready(p.chat_id, p.message_id, **{"path" if p.what == "full" else "thumb_path": path})
        return dto.MediaDownloadResult(path=path, mime=mime, bytes=size)


async def avatar(account, client, p):
    if p.kind not in ("user", "chat"):
        raise rpc.InvalidParams(field="kind")
    if p.size not in (None, "small", "big"):
        raise rpc.InvalidParams(field="size")
    if p.kind == "user" and identifier(p.id, "id", peer=True) < 0:
        raise rpc.InvalidParams(field="id")
    peer = await resolve_chat(client, p.id, field="id")
    with atomic_file(account.cache_dir, ".jpg", limit=16 * 1024**2) as (stream, publish):
        result = await client.download_profile_photo(peer, file=stream, download_big=p.size != "small")
        if result is None or stream.size == 0:
            return dto.PathResult()
        path = publish()
    await account.avatar_ready(p.kind, p.id, path)
    return dto.PathResult(path=path)
