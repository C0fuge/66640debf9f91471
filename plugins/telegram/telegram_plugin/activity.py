"""Explicit read receipts, typing and server drafts."""
from contact_hub_sdk import errors as rpc
from telethon import functions, types as tl, utils

from .mapping import entities_to_tl, identifier, resolve_chat


TYPING = {
    "typing": tl.SendMessageTypingAction,
    "recording_voice": tl.SendMessageRecordAudioAction,
    "recording_video": tl.SendMessageRecordVideoAction,
    "choosing_sticker": tl.SendMessageChooseStickerAction,
    "playing": tl.SendMessageGamePlayAction,
    "cancel": tl.SendMessageCancelAction,
}


async def read(client, p):
    peer = await resolve_chat(client, p.chat_id)
    maximum = identifier(p.max_message_id, "max_message_id")
    if p.topic_id is not None:
        await client(functions.messages.ReadDiscussionRequest(peer=peer,
                     msg_id=identifier(p.topic_id, "topic_id"), read_max_id=maximum))
    elif isinstance(peer, tl.InputPeerChannel):
        await client(functions.channels.ReadHistoryRequest(channel=utils.get_input_channel(peer), max_id=maximum))
    else:
        await client(functions.messages.ReadHistoryRequest(peer=peer, max_id=maximum))


async def typing(client, p):
    if p.action not in TYPING:
        raise rpc.NotSupported("Unsupported Telegram typing action.", capability="typing")
    peer = await resolve_chat(client, p.chat_id)
    await client(functions.messages.SetTypingRequest(peer=peer, action=TYPING[p.action](),
                 top_msg_id=identifier(p.topic_id, "topic_id") if p.topic_id is not None else None))


async def save_draft(client, p):
    if p.topic_id is not None:
        raise rpc.NotSupported("Telegram topic drafts are not supported yet.", capability="drafts.sync")
    peer = await resolve_chat(client, p.chat_id)
    draft = p.draft
    if len(draft.text) > 4096:
        raise rpc.InvalidParams(field="draft.text")
    # Empty text is the contract's explicit clear operation, even when a stale
    # reply target accompanied it. A reply-only Telegram draft is not a clear.
    reply = identifier(draft.reply_to, "draft.reply_to") if draft.text and draft.reply_to is not None else None
    header = tl.InputReplyToMessage(reply_to_msg_id=reply) if reply else None
    await client(functions.messages.SaveDraftRequest(peer=peer, message=draft.text,
                 entities=entities_to_tl(draft.text, draft.entities), reply_to=header))
