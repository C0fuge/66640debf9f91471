import asyncio

from contact_hub_sdk import METHODS, Plugin, errors as rpc, method
from contact_hub_sdk import types as dto

from . import actions, activity, chat_flags, downloads, history, polls, reactions, sticker_thumbs, stickers, sync, unread_reactions, uploads
from .auth import sign_in
from .mapping import SEND_ENTITY_TYPES, contact
from .send import send
from .session import disconnect, make_client, options, platform_errors, session_path, translated


class TelegramPlugin(Plugin):
    capabilities = {
        "chat.get": True, "chat.flags": True, "chat.pin": True, "chat.mark_unread": True, "chat.archive": True,
        "chat.sync": True, "contacts.list": True, "history.jump_to_date": True, "history.fill_gap": True,
        "send.entities": {"types": SEND_ENTITY_TYPES}, "send.silent": True, "send.no_webpage": True,
        "messages.get": True, "edit": True, "edit.caption": True, "delete": True, "delete.for_me": True,
        "forward": True, "forward.drop_author": True, "pin": True, "unpin": True,
        "read_receipts": True, "typing": {"actions": list(activity.TYPING)}, "drafts.sync": True,
        "media.download": True, "avatars": True,
        "send.file": {"max_bytes": 2 * 1024**3}, "send.as_file": True, "send.spoiler": True,
        "send.voice": True, "send.ttl": True, "send.album": {"max": 10},
        "polls.create": {"quiz": True, "multiple": True, "anonymous": True, "max_options": polls.MAX_OPTIONS},
        "polls.vote": True, "polls.close": True,
        "reactions.set": {"max": reactions.MAX_SELECTION}, "reactions.custom": True, "reactions.who": True,
        "reactions.unread": True,
        "stickers": True,
    }

    def __init__(self, factory=make_client):
        super().__init__()
        self.factory = factory

    async def on_account_start(self, account):
        config = options(account)
        with platform_errors():
            path = session_path(account.state_dir)
            if any(a is not account and a.data.get("path") == path.resolve() for a in self.accounts.values()):
                raise rpc.InvalidParams("The state directory is already open.", field="state_dir")
            client = self.factory(path, *config)
            account.data.update(path=path.resolve(), client=client, signed_in=asyncio.Event(), sync_lock=asyncio.Lock(),
                                sticker_catalog=stickers.Catalog())
            try:
                sync.bind_updates(account, client)
            except BaseException:
                await disconnect(client)
                raise
        await account.status("connecting")
        account.spawn(self._supervise(account))

    async def _supervise(self, account):
        client = account.data["client"]
        live = False
        backoff = 1
        while True:
            try:
                await client.connect()
                me = await client.get_me()
                if me is None:
                    signed = account.data["signed_in"]
                    signed.clear()
                    await account.status("auth_required")
                    waiter = asyncio.create_task(signed.wait())
                    lost = client.disconnected
                    try:
                        done, _ = await asyncio.wait([waiter, lost], return_when=asyncio.FIRST_COMPLETED)
                        if lost in done:
                            await lost
                            raise rpc.NetworkError()
                    finally:
                        lost.cancel()
                        waiter.cancel()
                        await asyncio.gather(waiter, lost, return_exceptions=True)
                    me = await client.get_me()
                    if me is None:
                        raise rpc.AuthRequired()
                async with account.data["sync_lock"]:
                    await sync.sync_all(account, client)
                await account.status("connected", me=contact(me))
                live, backoff = True, 1
                await client.disconnected
                raise rpc.NetworkError()
            except Exception as exc:
                error = translated(exc)
            account.auth.cancel()
            try:
                await disconnect(client)
            except Exception:
                await account.status("error", detail="Telegram session could not be closed cleanly.")
                return
            if isinstance(error, rpc.FloodWait):
                delay = max(1, error.seconds)
                detail = f"Telegram requested a wait of {error.seconds} seconds."
            elif isinstance(error, (rpc.NetworkError, rpc.RpcTimeout)):
                delay, detail = backoff, error.message
                backoff = min(60, backoff * 2)
            else:
                await account.status("error", detail=error.message)
                return
            await account.status("unavailable", detail=detail, reconnecting=live)
            await asyncio.sleep(delay)
            await account.status("connecting", reconnecting=live)

    async def on_account_stop(self, account):
        account.data.pop("sticker_catalog", None)
        with platform_errors():
            await disconnect(account.data["client"])

    async def on_auth_begin(self, account, method):
        if account.state == "connected":
            await account.auth.done()
            return
        client = account.data["client"]
        if account.state != "auth_required" or not client.is_connected():
            await account.auth.info("Wait for Telegram to request sign-in.")
            return
        with platform_errors():
            await sign_in(client, account.auth, method)
            await account.auth.done()
            account.data["signed_in"].set()

    @staticmethod
    def ready(account):
        if account.state == "auth_required":
            raise rpc.AuthRequired()
        if account.state != "connected":
            raise rpc.Unavailable()
        return account.data["client"]

    @method("stickers.recent")
    async def sticker_recent(self, account, params):
        with platform_errors():
            return await stickers.recent(account, self.ready(account), params)

    @method("stickers.packs")
    async def sticker_packs(self, account, params):
        with platform_errors():
            return await stickers.packs(account, self.ready(account))

    @method("stickers.pack")
    async def sticker_pack(self, account, params):
        with platform_errors():
            return await stickers.pack(account, self.ready(account), params)

    @method("stickers.thumb")
    async def sticker_thumb(self, account, params):
        with platform_errors():
            return await sticker_thumbs.thumbnail(account, self.ready(account), params)

    @method("chats.get")
    async def chats_get(self, account, params):
        with platform_errors():
            async with asyncio.timeout(METHODS["chats.get"].timeout_seconds):
                async with account.data["sync_lock"]:
                    return await chat_flags.get(self.ready(account), params.chat_id)

    @method("chats.set_flags")
    async def chats_set_flags(self, account, params):
        with platform_errors():
            async with asyncio.timeout(METHODS["chats.set_flags"].timeout_seconds):
                async with account.data["sync_lock"]:
                    await chat_flags.set_flags(account, self.ready(account), params)

    @method("chats.sync")
    async def chats_sync(self, account, params):
        with platform_errors():
            async with account.data["sync_lock"]:
                await sync.dialogs(account, self.ready(account), params.include_archived is not False)

    @method("contacts.list")
    async def contacts_list(self, account, params):
        with platform_errors():
            return dto.ContactsResult(contacts=await sync.contacts(self.ready(account)))

    @method("messages.fetch_recent")
    async def fetch_recent(self, account, params):
        with platform_errors():
            result = await history.recent(self.ready(account), params)
            polls.observe(account, result.messages)
            return result

    @method("history.around")
    async def around(self, account, params):
        with platform_errors():
            result = await history.around(self.ready(account), params)
            polls.observe(account, result.messages)
            return result

    @method("history.fill_gap")
    async def gap(self, account, params):
        with platform_errors():
            result = await history.fill_gap(self.ready(account), params)
            polls.observe(account, result.messages)
            return result

    @method("messages.send")
    async def send(self, account, params):
        with platform_errors():
            result = await send(self.ready(account), params)
            # Link the outbox echo even when the stdio response is lost after dispatch.
            if params.client_id is not None:
                await account.messages_upsert([result.message])
            return result

    @method("messages.get")
    async def get(self, account, params):
        async def hydrate(client, peer, raw):
            return await reactions.hydrate(client, peer, await polls.hydrate(client, peer, raw))
        with platform_errors():
            result = await actions.get(self.ready(account), params, hydrate=hydrate)
            polls.observe(account, [result.message])
            return result

    @method("messages.edit")
    async def edit(self, account, params):
        with platform_errors():
            return await actions.edit(self.ready(account), params)

    @method("messages.delete")
    async def delete(self, account, params):
        with platform_errors():
            await actions.delete(self.ready(account), params)

    @method("messages.forward")
    async def forward(self, account, params):
        with platform_errors():
            result = await actions.forward(self.ready(account), params)
            polls.observe(account, result.messages)
            return result

    @method("messages.pin")
    async def pin(self, account, params):
        with platform_errors():
            await actions.pin(self.ready(account), params)

    @method("messages.unpin")
    async def unpin(self, account, params):
        with platform_errors():
            await actions.unpin(self.ready(account), params)

    @method("read.send_receipt")
    async def read_receipt(self, account, params):
        with platform_errors():
            await activity.read(self.ready(account), params)

    @method("typing.send")
    async def typing(self, account, params):
        with platform_errors():
            await activity.typing(self.ready(account), params)

    @method("drafts.save")
    async def draft(self, account, params):
        with platform_errors():
            await activity.save_draft(self.ready(account), params)

    @method("media.download")
    async def download(self, account, params):
        with platform_errors():
            return await downloads.download(account, self.ready(account), params)

    @method("avatars.download")
    async def avatar(self, account, params):
        with platform_errors():
            return await downloads.avatar(account, self.ready(account), params)

    @method("messages.send_file")
    async def send_file(self, account, params):
        with platform_errors():
            return await uploads.send_file(account, self.ready(account), params)

    @method("messages.send_album")
    async def send_album(self, account, params):
        with platform_errors():
            return await uploads.send_album(account, self.ready(account), params)

    @method("polls.create")
    async def create_poll(self, account, params):
        with platform_errors():
            return await polls.create(account, self.ready(account), params)

    @method("polls.vote")
    async def vote_poll(self, account, params):
        with platform_errors():
            return await polls.vote(account, self.ready(account), params)

    @method("polls.close")
    async def close_poll(self, account, params):
        with platform_errors():
            return await polls.close(account, self.ready(account), params)

    @method("reactions.available")
    async def available_reactions(self, account, params):
        with platform_errors():
            return await reactions.available(self.ready(account), params)

    @method("reactions.set")
    async def set_reactions(self, account, params):
        with platform_errors():
            return await reactions.set_reactions(account, self.ready(account), params)

    @method("reactions.who")
    async def reaction_users(self, account, params):
        with platform_errors():
            return await reactions.who(self.ready(account), params)

    @method("reactions.list_unread")
    async def unread_reactions(self, account, params):
        with platform_errors():
            return await unread_reactions.list_unread(self.ready(account), params)

    @method("reactions.read")
    async def read_reactions(self, account, params):
        with platform_errors():
            await unread_reactions.read(account, self.ready(account), params)
