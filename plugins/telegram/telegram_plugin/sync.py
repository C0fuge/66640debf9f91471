"""Initial snapshots and sparse live events."""
from telethon import events, functions, types as tl

from . import polls, reactions

from .mapping import contact, conversation, draft, message, peer_id, sid


async def contacts(client):
    response = await client(functions.contacts.GetContactsRequest(hash=0))
    ids = {item.user_id for item in response.contacts}
    return [contact(user) for user in response.users if isinstance(user, tl.User) and user.id in ids]


async def dialogs(account, client, include_archived=True):
    ids, batch, previews = [], [], []
    async for dialog in client.iter_dialogs():
        # The closing snapshot is always complete. A caller may skip fetching
        # rich details for archived dialogs without hiding them in the core.
        ids.append(str(dialog.id))
        if dialog.archived and not include_archived:
            continue
        batch.append(conversation(dialog))
        if dialog.message and (preview := message(dialog.message)):
            polls.observe(account, [preview])
            previews.append(preview)
        if len(batch) == 32:
            await account.conversations_upsert(batch, ack=True)
            if previews:
                await account.messages_upsert(previews, ack=True)
            batch, previews = [], []
    if batch:
        await account.conversations_upsert(batch, ack=True)
    await account.conversations_synced(ids, ack=True)
    if previews:
        await account.messages_upsert(previews, ack=True)


async def sync_all(account, client):
    await dialogs(account, client)
    users = await contacts(client)
    for offset in range(0, len(users), 100):
        await account.contacts_upsert(users[offset:offset + 100], ack=True)


async def raw_update(account, update, client=None):
    if isinstance(update, tl.UpdateMessagePoll) and client is not None:
        await polls.refresh_live(account, client, update)
    elif isinstance(update, tl.UpdateMessageReactions) and client is not None:
        await reactions.refresh_live(account, client, update)
    elif isinstance(update, (tl.UpdateDeleteMessages, tl.UpdateDeleteChannelMessages)):
        chat = peer_id(tl.PeerChannel(update.channel_id)) if isinstance(update, tl.UpdateDeleteChannelMessages) else None
        await account.messages_deleted([str(i) for i in update.messages], chat_id=chat)
    elif isinstance(update, tl.UpdateDraftMessage):
        value = draft(update.draft)
        if value is not None and not update.saved_peer_id:
            await account.draft_changed(peer_id(update.peer), value, topic_id=sid(update.top_msg_id))
    elif isinstance(update, (tl.UpdateReadHistoryInbox, tl.UpdateReadHistoryOutbox, tl.UpdateReadChannelInbox,
                             tl.UpdateReadChannelOutbox, tl.UpdateReadChannelDiscussionInbox,
                             tl.UpdateReadChannelDiscussionOutbox)):
        chat = peer_id(update.peer if hasattr(update, "peer") else tl.PeerChannel(update.channel_id))
        topic = sid(getattr(update, "top_msg_id", None))
        maximum = str(update.max_id if hasattr(update, "max_id") else update.read_max_id)
        if isinstance(update, (tl.UpdateReadHistoryInbox, tl.UpdateReadChannelInbox, tl.UpdateReadChannelDiscussionInbox)):
            await account.read_inbox(chat, maximum, topic_id=topic, unread_count=getattr(update, "still_unread_count", None))
        else:
            await account.read_outbox(chat, maximum, topic_id=topic)


def bind_updates(account, client):
    async def new(event):
        if item := message(event.message):
            polls.observe(account, [item])
            await account.messages_upsert([item])

    async def edited(event):
        if item := message(event.message):
            polls.observe(account, [item])
            await account.message_edited(item)

    async def raw(event):
        await raw_update(account, event, client)

    client.add_event_handler(new, events.NewMessage())
    client.add_event_handler(edited, events.MessageEdited())
    client.add_event_handler(raw, events.Raw())
