"""Read-only normal-sticker catalog, pinned Telethon 1.46/layer 233.

Primary semantics: https://core.telegram.org/api/stickers and the three
messages.getRecentStickers/getAllStickers/getStickerSet method documents.
Public IDs are account-runtime receipts for bounded immutable TL references.
"""
import asyncio
import json
import secrets
from collections import OrderedDict
from contextlib import asynccontextmanager

from contact_hub_sdk import errors as rpc, types as dto
from contact_hub_sdk.wire import to_wire
from telethon import functions, types as tl
from telethon.extensions import BinaryReader

MAX_REFS = 4096
MAX_REF_BYTES = 16 * 1024**2
MAX_ITEM_BYTES = 2 * 1024**2
MAX_RESULT_BYTES = 192 * 1024
MAX_WAITERS = 16
FORMATS = {"image/webp": "static", "application/x-tgsticker": "animated", "video/webm": "video"}


class Catalog:
    def __init__(self):
        self.nonce, self.sequence = secrets.token_hex(16), 0
        self.entries, self.index = OrderedDict(), {}
        self.bytes, self.waiters, self.lock = 0, 0, asyncio.Lock()

    @asynccontextmanager
    async def slot(self):
        if self.waiters >= MAX_WAITERS:
            raise rpc.Unavailable("The sticker catalog is busy.")
        self.waiters += 1
        try:
            async with self.lock:
                yield
        finally:
            self.waiters -= 1

    def resolve(self, token, kind):
        entry = self.entries.get(token)
        if entry is None or entry[0] != kind:
            raise rpc.NotFound("Sticker reference is stale or belongs to another account.", kind="sticker_reference")
        self.entries.move_to_end(token)
        with BinaryReader(entry[1]) as reader:
            return reader.tgread_object()


class Batch:
    """Stage a whole collection; never evict a reference this response returns."""
    def __init__(self, catalog):
        self.catalog, self.new, self.index, self.used = catalog, OrderedDict(), {}, set()
        self.bytes = 0

    def reference(self, kind, value):
        blob = bytes(value)
        if len(blob) > MAX_ITEM_BYTES:
            raise rpc.NotSupported("Sticker reference exceeds the local size limit.")
        key = (kind, blob)
        token = self.index.get(key) or self.catalog.index.get(key)
        if token is None:
            if len(self.new) >= MAX_REFS or self.bytes + len(blob) > MAX_REF_BYTES:
                raise rpc.NotSupported("Sticker collection exceeds the local reference budget.")
            if self.catalog.sequence >= 2**64 - 1:
                raise rpc.NotSupported("Sticker reference issuance limit reached; restart the account.")
            self.catalog.sequence += 1
            token = f"tgs1.{self.catalog.nonce}.{kind}.{self.catalog.sequence:x}"
            self.new[token] = key
            self.index[key] = token
            self.bytes += len(blob)
        self.used.add(token)
        return token

    def commit(self, result):
        if len(json.dumps(to_wire(result), ensure_ascii=True).encode()) > MAX_RESULT_BYTES:
            raise rpc.NotSupported("Sticker collection exceeds the response size limit.")
        kept = sum(len((self.new.get(token) or self.catalog.entries[token])[1]) for token in self.used)
        if len(self.used) > MAX_REFS or kept > MAX_REF_BYTES:
            raise rpc.NotSupported("Sticker collection exceeds the local reference budget.")
        catalog = self.catalog
        while len(catalog.entries) + len(self.new) > MAX_REFS or catalog.bytes + self.bytes > MAX_REF_BYTES:
            victim = next((key for key in catalog.entries if key not in self.used), None)
            if victim is None:
                raise rpc.Internal("Sticker reference budget could not be committed.")
            key = catalog.entries.pop(victim)
            catalog.index.pop(key)
            catalog.bytes -= len(key[1])
        for token in self.used:
            if token in catalog.entries:
                catalog.entries.move_to_end(token)
        catalog.entries.update(self.new)
        catalog.index.update(self.index)
        catalog.bytes += self.bytes
        return result


def catalog(account):
    if "sticker_catalog" not in account.data:
        account.data["sticker_catalog"] = Catalog()
    return account.data["sticker_catalog"]


def integer(value, *, minimum=0):
    if type(value) is not int or not minimum <= value < 2**63:
        raise rpc.Internal("Telegram returned invalid sticker metadata.")
    return value


def text(value, limit):
    if not isinstance(value, str) or len(value) > limit or len(value.encode()) > limit:
        raise rpc.Internal("Telegram returned unavailable or oversized sticker text metadata.")
    return value


def signed(value):
    if type(value) is not int or not -(2**63) <= value < 2**63:
        raise rpc.Internal("Telegram returned an invalid sticker identity.")
    return value


def vector(value):
    if not isinstance(value, list) or len(value) > MAX_REFS:
        raise rpc.Internal("Telegram returned an unavailable or oversized sticker collection.")
    return value


def document_reference(raw, pack):
    """Retain only bounded fields required to address this exact static preview.

    Do not serialize unbounded filenames, unknown attributes, vector outlines or
    animated effects into the reference map. Projection uses the original model.
    """
    signed(raw.access_hash)
    if (not isinstance(raw.file_reference, bytes) or len(raw.file_reference) > 4096
            or type(raw.dc_id) is not int or not 0 < raw.dc_id < 2**31):
        raise rpc.Internal("Telegram returned an invalid sticker document reference.")
    if len(raw.thumbs or []) > 64:
        raise rpc.NotSupported("Sticker thumbnail metadata exceeds the local size limit.")
    thumbs, total = [], 0
    for item in raw.thumbs or []:
        if isinstance(item, (tl.PhotoSize, tl.PhotoCachedSize, tl.PhotoStrippedSize, tl.PhotoSizeProgressive)):
            if not isinstance(item.type, str) or len(item.type) > 4:
                raise rpc.Internal("Telegram returned an invalid thumbnail type.")
            if isinstance(item, (tl.PhotoCachedSize, tl.PhotoStrippedSize)):
                if not isinstance(item.bytes, bytes):
                    raise rpc.Internal("Telegram returned invalid thumbnail bytes.")
                total += len(item.bytes)
            if isinstance(item, tl.PhotoSizeProgressive):
                if len(item.sizes) > 64:
                    raise rpc.NotSupported("Sticker thumbnail metadata exceeds the local size limit.")
                for size in item.sizes:
                    integer(size, minimum=1)
            thumbs.append(item)
    if total > MAX_ITEM_BYTES - 8192:
        raise rpc.NotSupported("Sticker thumbnail metadata exceeds the local size limit.")
    return tl.Document(raw.id, raw.access_hash, raw.file_reference, raw.date,
            raw.mime_type, raw.size, raw.dc_id,
            [tl.DocumentAttributeSticker("", pack)], thumbs=thumbs or None)


def pack_info(raw, batch, *, installed=None):
    if not isinstance(raw, tl.StickerSet) or raw.emojis or raw.masks:
        raise rpc.NotSupported("This catalog supports ordinary sticker packs only.")
    signed(raw.id)
    signed(raw.access_hash)
    text(raw.title, 4096)
    text(raw.short_name, 256)
    token = batch.reference("p", tl.InputStickerSetID(raw.id, raw.access_hash))
    return dto.StickerPack(id=token, title=raw.title, short_name=raw.short_name,
                count=integer(raw.count), kind="stickers",
                installed=raw.installed_date is not None if installed is None else installed)


def sticker_info(raw, batch, parent=None):
    if not isinstance(raw, tl.Document):
        raise rpc.Internal("Telegram returned an unavailable sticker document.")
    signed(raw.id)
    integer(raw.size)
    attributes = vector(raw.attributes)
    if len(attributes) > 64:
        raise rpc.NotSupported("Sticker attribute metadata exceeds the local size limit.")
    sticker = [a for a in attributes if isinstance(a, tl.DocumentAttributeSticker)]
    if len(sticker) != 1 or any(isinstance(a, tl.DocumentAttributeCustomEmoji) for a in attributes):
        raise rpc.Internal("Telegram returned an invalid ordinary sticker document.")
    sticker = sticker[0]
    if sticker.mask:
        raise rpc.NotSupported("Mask sticker catalogs are not implemented.")
    fmt = FORMATS.get(raw.mime_type)
    if fmt is None:
        # The frozen required enum has no unknown format. Do not invent static.
        raise rpc.NotSupported("This sticker format cannot be represented by the protocol.")
    text(sticker.alt, 4096)
    reference, pack_id, bound = sticker.stickerset, None, tl.InputStickerSetEmpty()
    if isinstance(reference, tl.InputStickerSetID):
        signed(reference.id)
        signed(reference.access_hash)
        if parent is not None and (reference.id, reference.access_hash) != (parent.id, parent.access_hash):
            raise rpc.Internal("Sticker document contradicts its pack identity.")
        pack_id = batch.reference("p", reference)
        bound = tl.InputStickerSetID(reference.id, reference.access_hash)
    elif parent is not None:
        if isinstance(reference, tl.InputStickerSetShortName) and reference.short_name != parent.short_name:
            raise rpc.Internal("Sticker document contradicts its pack short name.")
        if not isinstance(reference, (tl.InputStickerSetEmpty, tl.InputStickerSetShortName)):
            raise rpc.Internal("Sticker document has an unsupported pack identity.")
        pack_id = batch.reference("p", tl.InputStickerSetID(parent.id, parent.access_hash))
        bound = tl.InputStickerSetID(parent.id, parent.access_hash)
    # A recent short-name/special reference is not a stable numeric pack identity.
    # Keep pack_id unknown rather than binding an opaque ID to a mutable name.
    dimensions = [a for a in attributes if isinstance(a, (tl.DocumentAttributeImageSize, tl.DocumentAttributeVideo))]
    if len({(integer(a.w), integer(a.h)) for a in dimensions}) > 1:
        raise rpc.Internal("Telegram returned contradictory sticker dimensions.")
    size = dimensions[0] if dimensions else None
    vector(raw.thumbs or [])
    vector(raw.video_thumbs or [])
    premium = any(isinstance(v, tl.VideoSize) and v.type == "f" for v in raw.video_thumbs or [])
    return dto.Sticker(id=batch.reference("d", document_reference(raw, bound)), pack_id=pack_id,
                emoji=sticker.alt or None, format=fmt, width=size.w if size else None,
                height=size.h if size else None, premium=premium)


async def recent(account, client, params):
    limit = params.limit if params.limit is not None else 200
    if type(limit) is not int or not 1 <= limit <= 200:
        raise rpc.InvalidParams(field="limit")
    state = catalog(account)
    async with state.slot():
        response = await client(functions.messages.GetRecentStickersRequest(hash=0, attached=False))
        if not isinstance(response, tl.messages.RecentStickers):
            raise rpc.Internal("Telegram did not return a fresh recent-sticker collection.")
        rows = vector(response.stickers)
        if len({raw.id for raw in rows}) != len(rows):
            raise rpc.Internal("Telegram returned duplicate recent sticker IDs.")
        batch = Batch(state)
        return batch.commit(dto.StickersResult(stickers=[sticker_info(raw, batch) for raw in rows[:limit]]))


async def packs(account, client):
    state = catalog(account)
    async with state.slot():
        response = await client(functions.messages.GetAllStickersRequest(hash=0))
        if not isinstance(response, tl.messages.AllStickers):
            raise rpc.Internal("Telegram did not return a fresh installed sticker catalog.")
        rows = vector(response.sets)
        if len({raw.id for raw in rows}) != len(rows):
            raise rpc.Internal("Telegram returned duplicate sticker pack IDs.")
        batch = Batch(state)
        return batch.commit(dto.PacksResult(packs=[pack_info(raw, batch, installed=True) for raw in rows]))


async def pack(account, client, params):
    state = catalog(account)
    async with state.slot():
        reference = state.resolve(params.pack_id, "p")
        response = await client(functions.messages.GetStickerSetRequest(stickerset=reference, hash=0))
        if not isinstance(response, tl.messages.StickerSet) or not isinstance(response.set, tl.StickerSet):
            raise rpc.Internal("Telegram did not return a fresh complete sticker pack.")
        raw = response.set
        if (raw.id, raw.access_hash) != (reference.id, reference.access_hash):
            raise rpc.Internal("Telegram returned another sticker pack.")
        rows = vector(response.documents)
        if len(rows) != raw.count or len({document.id for document in rows}) != len(rows):
            raise rpc.Internal("Telegram returned an incomplete or contradictory sticker pack.")
        known = {document.id for document in rows}
        if any(not isinstance(group, tl.StickerPack) or any(mid not in known for mid in vector(group.documents))
               for group in vector(response.packs)):
            raise rpc.Internal("Sticker emoji mapping refers to another document.")
        batch = Batch(state)
        result = dto.PackResult(pack=pack_info(raw, batch),
                stickers=[sticker_info(document, batch, raw) for document in rows])
        return batch.commit(result)
