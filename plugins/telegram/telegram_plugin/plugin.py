import asyncio

from contact_hub_sdk import Plugin, errors as rpc, method
from contact_hub_sdk import types as dto

from . import history, sync
from .auth import sign_in
from .mapping import SEND_ENTITY_TYPES, contact
from .send import send
from .session import disconnect, make_client, options, platform_errors, session_path, translated


class TelegramPlugin(Plugin):
    capabilities = {
        "chat.sync": True, "contacts.list": True, "history.jump_to_date": True, "history.fill_gap": True,
        "send.entities": {"types": SEND_ENTITY_TYPES}, "send.silent": True, "send.no_webpage": True,
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
            account.data.update(path=path.resolve(), client=client, signed_in=asyncio.Event(), sync_lock=asyncio.Lock())
            sync.bind_updates(account, client)
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
            return await history.recent(self.ready(account), params)

    @method("history.around")
    async def around(self, account, params):
        with platform_errors():
            return await history.around(self.ready(account), params)

    @method("history.fill_gap")
    async def gap(self, account, params):
        with platform_errors():
            return await history.fill_gap(self.ready(account), params)

    @method("messages.send")
    async def send(self, account, params):
        with platform_errors():
            result = await send(self.ready(account), params)
            # Link the outbox echo even when the stdio response is lost after dispatch.
            if params.client_id:
                await account.messages_upsert([result.message])
            return result
