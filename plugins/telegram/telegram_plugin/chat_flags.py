"""Account-scoped preferences for a single ordinary Telegram dialog."""
import asyncio

from contact_hub_sdk import errors as rpc, types as dto
from telethon import functions, types as tl

from .mapping import archive_state, identifier, peer_id, resolve_chat
from .session import translated

_UPDATES = (tl.Updates, tl.UpdatesCombined, tl.UpdatesTooLong, tl.UpdateShort,
            tl.UpdateShortMessage, tl.UpdateShortChatMessage, tl.UpdateShortSentMessage)


def uncertain(error=None):
    message = "Dialog changes may have applied. Refresh the chat list to confirm."
    data = {"reason": "outcome_unknown", "outcome": "unknown", "retry_safe": False}
    if isinstance(error, rpc.FloodWait):
        data["seconds"] = error.seconds
        return rpc.PluginError(message, code=error.code, **data)
    if isinstance(error, rpc.AuthRequired):
        message = "Dialog changes may have applied. Sign in and refresh the chat list to confirm."
    if isinstance(error, rpc.RpcTimeout):
        return rpc.RpcTimeout(message, **data)
    return rpc.NetworkError(message, **data)


async def observe(client, target, chat_id, fields, *, capability="chat.flags"):
    result = await client(functions.messages.GetPeerDialogsRequest(peers=[target]))
    if not isinstance(result, tl.messages.PeerDialogs) or not isinstance(result.dialogs, list):
        raise rpc.Internal("Telegram returned an invalid dialog result.")
    if not result.dialogs:
        raise rpc.NotFound(kind="chat")
    if len(result.dialogs) != 1 or not isinstance(result.dialogs[0], tl.Dialog):
        raise rpc.Internal("Telegram did not return one ordinary dialog.")
    dialog = result.dialogs[0]
    if not isinstance(dialog.peer, (tl.PeerUser, tl.PeerChat, tl.PeerChannel)) or peer_id(dialog.peer) != chat_id:
        raise rpc.Internal("Telegram returned a different dialog.")
    table, cls = {
        tl.PeerUser: (result.users, tl.User), tl.PeerChat: (result.chats, tl.Chat),
        tl.PeerChannel: (result.chats, tl.Channel),
    }[type(dialog.peer)]
    entities = [entity for entity in table if isinstance(entity, cls) and peer_id(entity) == chat_id]
    if len(entities) != 1 or getattr(entities[0], "min", False):
        raise rpc.Internal("Telegram did not return a matching full dialog entity.")
    entity = entities[0]
    if getattr(entity, "monoforum", False):
        raise rpc.NotSupported("Telegram monoforums are unsupported.", capability=capability)
    kind = "dm" if cls is tl.User else "group" if cls is tl.Chat or entity.megagroup else "channel"
    flags = {"pinned": dialog.pinned, "marked_unread": dialog.unread_mark}
    if "archived" in fields:
        flags["archived"] = archive_state(dialog.folder_id)
    if any(type(flags[name]) is not bool for name in fields):
        raise rpc.Internal("Telegram returned invalid dialog flags.")
    return dto.Conversation(id=chat_id, kind=kind, **{name: flags[name] for name in fields})


async def get(client, chat_id):
    target = tl.InputDialogPeer(await resolve_chat(client, chat_id))
    conversation = await observe(client, target, chat_id, ("pinned", "marked_unread", "archived"), capability="chat.get")
    return dto.ChatsGetResult(conversation=conversation)


async def set_flags(account, client, p):
    fields = {name: getattr(p, name) for name in ("pinned", "marked_unread", "archived") if getattr(p, name) is not None}
    if not fields:
        identifier(p.chat_id, "chat_id", peer=True)
        return
    target = tl.InputDialogPeer(await resolve_chat(client, p.chat_id))
    # Moving a dialog between lists may affect its pin. Publish that observed
    # side effect even when pinning was not part of the requested change.
    observed_fields = tuple(dict.fromkeys((*fields, "pinned"))) if "archived" in fields else fields
    current = await observe(client, target, p.chat_id, observed_fields)
    acknowledged = False
    failure = None
    for name, desired in fields.items():
        if getattr(current, name) is desired:
            continue
        if name == "archived":
            request = functions.folders.EditPeerFoldersRequest([
                tl.InputFolderPeer(peer=target.peer, folder_id=1 if desired else 0)])
        elif name == "pinned":
            request = functions.messages.ToggleDialogPinRequest(peer=target, pinned=desired)
        else:
            request = functions.messages.MarkDialogUnreadRequest(peer=target, unread=desired)
        try:
            result = await client(request)
            if not (isinstance(result, _UPDATES) if name == "archived" else result is True):
                raise rpc.NetworkError()
            acknowledged = True
        except asyncio.CancelledError:
            raise uncertain() from None
        except Exception as exc:
            error = translated(exc)
            definite = isinstance(error, (rpc.Forbidden, rpc.InvalidParams, rpc.NotFound, rpc.FloodWait, rpc.AuthRequired))
            if definite and not acknowledged:
                raise error from None
            failure = uncertain(error)
            if isinstance(error, (rpc.AuthRequired, rpc.FloodWait)) or account.state != "connected":
                raise failure from None
            break

    try:
        if acknowledged or failure:
            current = await observe(client, target, p.chat_id, observed_fields)
        await account.conversations_upsert([current], ack=True)
    except (Exception, asyncio.CancelledError) as exc:
        if acknowledged or failure:
            raise failure or uncertain(translated(exc)) from None
        raise
    if failure:
        raise failure
    if any(getattr(current, name) is not desired for name, desired in fields.items()):
        raise rpc.Unavailable("Telegram dialog flags changed during the request. Refresh the chat list.",
                              retry_safe=False)
