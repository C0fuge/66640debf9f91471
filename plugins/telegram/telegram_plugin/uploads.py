"""Validated local file handles and explicit MTProto media/caption flags."""
import mimetypes
import os
import re
import stat
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path

from contact_hub_sdk import errors as rpc, types as dto
from telethon import functions, types as tl, utils

from .actions import fetch
from .cache import MAX_BYTES
from .mapping import entities_to_tl, identifier, message, resolve_chat


@dataclass
class Upload:
    stream: object
    size: int
    filename: str
    kind: str
    mime: str
    attributes: list
    spoiler: bool | None
    force_file: bool | None
    ttl: int | None
    fingerprint: tuple


@contextmanager
def local_file(path):
    selected = Path(path)
    if not selected.is_absolute() or ".." in selected.parts:
        raise rpc.InvalidParams("Select an absolute local file path.", field="path")
    try:
        fd = os.open(selected, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        raise rpc.InvalidParams("The selected file cannot be opened.", field="path") from None
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size <= 0:
            raise rpc.InvalidParams("Select a nonempty regular file.", field="path")
        if info.st_size > MAX_BYTES:
            raise rpc.NotSupported("The selected file exceeds the upload size limit.", capability="send.file")
        try:
            stream = os.fdopen(fd, "rb")
        except OSError:
            raise rpc.InvalidParams("The selected file cannot be opened.", field="path") from None
    except BaseException:
        # fdopen can fail before a stream has taken ownership (for example,
        # an allocation or descriptor error). Close our descriptor in every
        # validation/wrapping failure, including cancellation-like exceptions.
        try:
            os.close(fd)
        except OSError:
            pass
        raise
    with stream:
        yield stream, info.st_size, selected.name


def specification(stream, size, basename, p):
    kind = p.kind or "auto"
    if kind not in ("auto", "photo", "document", "voice", "video", "video_note"):
        raise rpc.InvalidParams(field="kind")
    if kind in ("video", "video_note"):
        raise rpc.NotSupported("Uploading video requires metadata support that is not available yet.",
                               capability="send.video_note" if kind == "video_note" else "send.file")
    header = stream.read(12)
    stream.seek(0)
    image = header.startswith(b"\xff\xd8\xff") or header.startswith(b"\x89PNG\r\n\x1a\n")
    if kind == "auto":
        kind = "photo" if image and not p.as_file else "document"
    if p.as_file:
        if kind == "voice":
            raise rpc.InvalidParams("A voice note cannot be forced to a document.", field="as_file")
        kind = "document"
    if kind == "photo" and not image:
        raise rpc.InvalidParams("Select a JPEG or PNG image, or send it as a document.", field="path")
    filename = getattr(p, "filename", None)
    filename = basename if filename is None else filename
    if not filename or filename in (".", "..") or len(filename.encode("utf-8")) > 255 or any(c in filename for c in "/\\") or any(ord(c) < 32 or ord(c) == 127 for c in filename):
        raise rpc.InvalidParams("Enter a plain file name without a directory.", field="filename")
    supplied_mime = getattr(p, "mime", None)
    if supplied_mime is not None and not re.fullmatch(r"[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+", supplied_mime):
        raise rpc.InvalidParams(field="mime")
    duration, samples = getattr(p, "duration", None), getattr(p, "waveform", None)
    if kind != "voice" and (duration is not None or samples is not None):
        raise rpc.InvalidParams("Duration and waveform are currently supported for voice notes.", field="duration" if duration is not None else "waveform")
    if kind == "photo" and supplied_mime is not None:
        raise rpc.InvalidParams("Telegram assigns the MIME type of uploaded photos; use a document to set it.", field="mime")
    mime = supplied_mime or mimetypes.guess_type(filename)[0] or ("audio/ogg" if kind == "voice" else "application/octet-stream")
    attributes = [tl.DocumentAttributeFilename(filename)]
    if kind == "voice":
        if duration is None or type(duration) is not int or not 0 <= duration < 2**31:
            raise rpc.InvalidParams("Supply the voice note duration in seconds.", field="duration")
        if not mime.startswith("audio/"):
            raise rpc.InvalidParams("A voice note requires an audio MIME type.", field="mime")
        if p.spoiler:
            raise rpc.InvalidParams("Voice notes cannot use spoiler covers.", field="spoiler")
        if samples is not None and any(type(sample) is not int or not 0 <= sample <= 31 for sample in samples):
            raise rpc.InvalidParams(field="waveform")
        attributes.append(tl.DocumentAttributeAudio(duration=duration, voice=True,
                          waveform=utils.encode_waveform(bytes(samples)) if samples is not None else None))
    ttl = getattr(p, "ttl_seconds", None)
    if ttl is not None and (type(ttl) is not int or not 0 <= ttl < 2**31):
        raise rpc.InvalidParams(field="ttl_seconds")
    info = os.fstat(stream.fileno())
    if info.st_size != size:
        raise rpc.InvalidParams("The selected file changed before upload.", field="path")
    fingerprint = (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
    return Upload(stream, size, filename, kind, mime, attributes, p.spoiler, p.as_file or kind == "document", ttl, fingerprint)


def caption(text, entities):
    text = text or ""
    if len(text) > 1024:
        raise rpc.InvalidParams("Captions must contain at most 1024 characters.", field="caption")
    # Explicit TL entities bypass all Markdown/HTML parsing (parse_mode=None).
    return text, entities_to_tl(text, entities)


def reply_header(p):
    topic = identifier(p.topic_id, "topic_id") if p.topic_id is not None else None
    reply = identifier(p.reply_to.message_id, "reply_to.message_id") if p.reply_to is not None else topic
    return tl.InputReplyToMessage(reply_to_msg_id=reply, top_msg_id=topic) if reply else None


async def upload_media(account, client, spec, *, offset=0, total=None):
    async def progress(done, _):
        await account.progress(offset + int(done), total or spec.size, direction="upload")
    # Pass a validated, held descriptor; Telethon never reopens a mutable path,
    # treats it as a URL, or infers a different user-visible filename.
    # A held descriptor's .name is an integer. Give Telethon a transport extension
    # explicitly instead of asking its convenience inference to inspect that name.
    # Documents retain the caller's exact display name in their TL attribute.
    transport_name = spec.filename
    if spec.kind == "photo" and Path(transport_name).suffix.lower() not in (".jpg", ".jpeg", ".png"):
        transport_name += ".jpg"
    elif not Path(transport_name).suffix:
        transport_name += ".bin"
    handle = await client.upload_file(spec.stream, file_size=spec.size, file_name=transport_name,
                                      progress_callback=progress)
    info = os.fstat(spec.stream.fileno())
    if (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns) != spec.fingerprint:
        raise rpc.InvalidParams("The selected file changed during upload.", field="path")
    if isinstance(handle, (tl.InputFile, tl.InputFileBig)):
        if spec.kind == "photo":
            return tl.InputMediaUploadedPhoto(handle, spoiler=spec.spoiler, ttl_seconds=spec.ttl)
        return tl.InputMediaUploadedDocument(handle, mime_type=spec.mime, attributes=spec.attributes,
                     force_file=spec.force_file, spoiler=spec.spoiler, ttl_seconds=spec.ttl)
    raise rpc.Internal("Telegram did not return an upload handle.")


async def confirmations(client, peer, chat_id, response, random_ids):
    if isinstance(response, tl.UpdateShortSentMessage) and len(random_ids) == 1:
        ids = {random_ids[0]: response.id}
    else:
        ids = {u.random_id: u.id for u in getattr(response, "updates", []) if isinstance(u, tl.UpdateMessageID)}
    messages = {}
    for update in getattr(response, "updates", []):
        if isinstance(update, (tl.UpdateNewMessage, tl.UpdateNewChannelMessage)):
            item = message(update.message)
            if item is not None and item.chat_id == chat_id:
                messages[int(item.id)] = item
    result = []
    for random_id in random_ids:
        mid = ids.get(random_id)
        if mid is None:
            raise rpc.Internal("Telegram accepted the media without a matching message confirmation.")
        item = messages.get(mid)
        if item is None:
            _, item = await fetch(client, peer, chat_id, mid)
        if item is None:
            raise rpc.Internal("Telegram did not return the sent media message.")
        result.append(item)
    return result


async def send_file(account, client, p):
    text, entities = caption(p.caption, p.entities)
    reply = reply_header(p)
    with local_file(p.path) as (stream, size, filename):
        spec = specification(stream, size, filename, p)
        peer = await resolve_chat(client, p.chat_id)
        await account.progress(0, size, direction="upload", force=True)
        media = await upload_media(account, client, spec)
        request = functions.messages.SendMediaRequest(peer=peer, media=media, message=text, entities=entities,
                                                      reply_to=reply, silent=p.silent)
        response = await client(request)
        result = await confirmations(client, peer, p.chat_id, response, [request.random_id])
        await account.progress(size, size, direction="upload", force=True)
    return dto.MessageResult(message=result[0])


async def send_album(account, client, p):
    if not 2 <= len(p.items) <= 10:
        raise rpc.InvalidParams("Select between two and ten album files.", field="items")
    reply = reply_header(p)
    prepared = []
    with ExitStack() as stack:
        # Validate/open every item before the first upload or send operation.
        for index, item in enumerate(p.items):
            text, entities = item.caption, item.entities
            if index == 0:
                if p.caption is not None:
                    if text is not None:
                        raise rpc.InvalidParams("Use one caption for the first album item.", field="caption")
                    text = p.caption
                if p.entities is not None:
                    if entities is not None:
                        raise rpc.InvalidParams("Use one entity list for the first album item.", field="entities")
                    entities = p.entities
            text, entities = caption(text, entities)
            stream, size, filename = stack.enter_context(local_file(item.path))
            if item.kind == "voice":
                raise rpc.NotSupported("Voice notes cannot be sent in this album.", capability="send.album")
            prepared.append((specification(stream, size, filename, item), text, entities))
        if len({spec.kind for spec, _, _ in prepared}) != 1:
            raise rpc.NotSupported("Photo and document albums must be sent separately.", capability="send.album")
        peer = await resolve_chat(client, p.chat_id)
        total = sum(spec.size for spec, _, _ in prepared)
        await account.progress(0, total, direction="upload", force=True)
        inputs, offset = [], 0
        for spec, text, entities in prepared:
            uploaded = await upload_media(account, client, spec, offset=offset, total=total)
            stored = await client(functions.messages.UploadMediaRequest(peer=peer, media=uploaded))
            if spec.kind == "photo" and isinstance(stored, tl.MessageMediaPhoto) and isinstance(stored.photo, tl.Photo):
                media = tl.InputMediaPhoto(utils.get_input_photo(stored.photo), spoiler=spec.spoiler)
            elif spec.kind != "photo" and isinstance(stored, tl.MessageMediaDocument) and isinstance(stored.document, tl.Document):
                media = tl.InputMediaDocument(utils.get_input_document(stored.document), spoiler=spec.spoiler)
            else:
                raise rpc.Internal("Telegram did not return the uploaded album media.")
            inputs.append(tl.InputSingleMedia(media=media, message=text, entities=entities))
            offset += spec.size
        request = functions.messages.SendMultiMediaRequest(peer=peer, multi_media=inputs, reply_to=reply, silent=p.silent)
        response = await client(request)
        result = await confirmations(client, peer, p.chat_id, response, [item.random_id for item in inputs])
        await account.progress(total, total, direction="upload", force=True)
    return dto.MessagesResult(messages=result)
