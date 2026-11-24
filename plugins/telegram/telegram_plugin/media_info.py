"""Attachment metadata from TL objects, without downloading or probing files."""
from contact_hub_sdk import types as dto
from telethon import types as tl


def waveform(data):
    if data is None:
        return None
    # Telegram packs unsigned samples in consecutive little-endian 5-bit fields.
    return [((data[bit // 8] | (data[bit // 8 + 1] << 8 if bit // 8 + 1 < len(data) else 0))
             >> (bit % 8)) & 31 for bit in range(0, len(data) * 8 - 4, 5)]


def photo_size(photo):
    sizes = [s for s in getattr(photo, "sizes", []) if isinstance(s, (tl.PhotoSize, tl.PhotoCachedSize, tl.PhotoSizeProgressive))]
    return max(sizes, key=lambda s: (s.w * s.h, s.w, s.h), default=None)


def photo_info(photo, spoiler=False):
    size = photo_size(photo)
    count = None
    if isinstance(size, tl.PhotoCachedSize):
        count = len(size.bytes)
    elif isinstance(size, tl.PhotoSizeProgressive):
        count = max(size.sizes, default=0)
    elif size is not None:
        count = size.size
    return dto.Media(kind="photo", mime="image/jpeg", bytes=count,
                     width=size.w if size else None, height=size.h if size else None, has_spoiler=bool(spoiler))


def document_info(document, spoiler=False):
    if not isinstance(document, tl.Document):
        return dto.Media(kind="document", has_spoiler=bool(spoiler))
    attributes = {type(attr): attr for attr in document.attributes}
    audio = attributes.get(tl.DocumentAttributeAudio)
    video = attributes.get(tl.DocumentAttributeVideo)
    sticker = attributes.get(tl.DocumentAttributeSticker) or attributes.get(tl.DocumentAttributeCustomEmoji)
    image = attributes.get(tl.DocumentAttributeImageSize)
    filename = attributes.get(tl.DocumentAttributeFilename)
    kind = "document"
    if sticker:
        kind = "sticker"
    elif audio and audio.voice:
        kind = "voice"
    elif video and video.round_message:
        kind = "video_note"
    elif tl.DocumentAttributeAnimated in attributes:
        kind = "gif"
    elif video:
        kind = "video"
    elif audio:
        kind = "music"
    sticker_ref = None
    if sticker:
        fmt = "animated" if document.mime_type == "application/x-tgsticker" else "video" if document.mime_type == "video/webm" else "static"
        pack = getattr(sticker, "stickerset", None)
        sticker_ref = dto.StickerRef(format=fmt, emoji=sticker.alt,
                                    pack_id=str(pack.id) if isinstance(pack, tl.InputStickerSetID) else None)
    dimensions = video or image
    return dto.Media(kind=kind, mime=document.mime_type, bytes=document.size,
                     filename=filename.file_name if filename else None,
                     width=dimensions.w if dimensions else None, height=dimensions.h if dimensions else None,
                     duration=int(video.duration) if video else audio.duration if audio else None,
                     waveform=waveform(audio.waveform) if audio else None,
                     title=audio.title if audio else None, performer=audio.performer if audio else None,
                     has_spoiler=bool(spoiler), sticker=sticker_ref)


def media_info(media):
    if isinstance(media, tl.MessageMediaPhoto):
        return photo_info(media.photo, media.spoiler)
    if isinstance(media, tl.MessageMediaDocument):
        return document_info(media.document, media.spoiler)
    return None
