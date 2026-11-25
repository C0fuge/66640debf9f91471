"""Synthetic real TL sticker responses; only the transport is replaced."""
from telethon import functions, types as tl
from telethon.extensions import BinaryReader

from stub import NOW, Stub

JPEG = b"\xff\xd8\xff\xe0\x00\x02\xff\xd9"


def sticker(mid=80, *, pack=70, mime="image/webp", dimensions=True, cached=True):
    attributes = [tl.DocumentAttributeSticker("🙂", tl.InputStickerSetID(pack, pack + 1))]
    if dimensions:
        attributes.append(tl.DocumentAttributeImageSize(512, 256))
    thumb = tl.PhotoCachedSize("s", 1, 1, JPEG) if cached else tl.PhotoSize("s", 1, 1, len(JPEG))
    return tl.Document(mid, mid + 1, b"synthetic-sticker-reference", NOW, mime, 42, 2, attributes, thumbs=[thumb])


def pack_info(pid=70, count=2, **kwargs):
    return tl.StickerSet(pid, pid + 1, f"Synthetic pack {pid}", f"synthetic_{pid}", count, 123,
                         installed_date=NOW, **kwargs)


def pack_response(pid=70):
    return tl.messages.StickerSet(pack_info(pid), [tl.StickerPack("🙂", [80, 81])], [],
                                  [sticker(80, pack=pid), sticker(81, pack=pid, mime="application/x-tgsticker")])


class StickerStub(Stub):
    async def __call__(self, request):
        # Exercise the exact installed layer's wire constructors in the subprocess.
        if isinstance(request, (functions.messages.GetRecentStickersRequest,
                                functions.messages.GetAllStickersRequest,
                                functions.messages.GetStickerSetRequest)):
            with BinaryReader(bytes(request)) as reader:
                request = reader.tgread_object()
            assert request.hash == 0
        if isinstance(request, functions.messages.GetRecentStickersRequest):
            assert not request.attached
            return tl.messages.RecentStickers(12, [], [sticker(), sticker(81, mime="video/webm")], [1, 2])
        if isinstance(request, functions.messages.GetAllStickersRequest):
            return tl.messages.AllStickers(13, [pack_info(71), pack_info()])
        if isinstance(request, functions.messages.GetStickerSetRequest):
            assert isinstance(request.stickerset, tl.InputStickerSetID)
            return pack_response(request.stickerset.id)
        return await super().__call__(request)
