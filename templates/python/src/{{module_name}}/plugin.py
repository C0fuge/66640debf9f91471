"""{{display_name}} plugin for Contact Hub.

The SDK (``contact_hub_sdk``) does the protocol work: ``initialize``, ``ping``,
``shutdown``, ``account.start``/``account.stop``, capability checks and error mapping.
You write the platform code. Spec: ``docs/plugin-api.md`` in the Contact Hub repo.
"""

from __future__ import annotations

import itertools
import logging
from datetime import UTC, datetime

from contact_hub_sdk import Account, Plugin, method, types as T

log = logging.getLogger(__name__)  # logs go to stderr; stdout belongs to the protocol

_ids = itertools.count(1)


class PlatformPlugin(Plugin):
    # Capabilities (spec section 14). The core only calls optional methods and only sends
    # gated params (e.g. ``silent`` on messages.send) that are declared here.
    # TODO: declare what {{display_name}} supports, e.g. {"send.silent": True, "edit": True}.
    capabilities: dict = {}

    async def on_account_start(self, account: Account) -> None:
        # Must return fast, without waiting for the network (spec 4.2). Connect in a
        # background task; it is cancelled on account.stop.
        server = account.settings["server"]  # settings_schema defaults are filled in (spec 6.1)
        await account.status("connecting")
        account.spawn(self.connect(account, server))

    async def connect(self, account: Account, server: str) -> None:
        # TODO: connect to {{display_name}} at `server` with your client library.
        #   unreachable -> await account.status("unavailable") and keep retrying with backoff
        #   unrecoverable -> await account.status("error", detail="why")
        # BEGIN auth example
        # if not account.secrets.get("token"):
        #     await account.status("auth_required")  # the core offers "Sign in" -> on_auth_begin
        #     return
        # END auth example
        log.info("connected to %s", server)
        await account.status("connected", me=T.Contact(id="me", display_name="Me"))
        # TODO: then push the chat list and keep it fresh:
        #   await account.conversations_upsert([T.Conversation(...)])
        #   await account.conversations_synced([chat_ids])
        #   and on every incoming message: await account.messages_upsert([T.Message(...)])
        # Conversation, Topic and Message updates are merged by the core (spec 8.4): a later
        # update carries only what changed, e.g. T.Conversation(id=..., kind=..., unread_count=3);
        # a field left None means "unchanged".

    async def on_account_stop(self, account: Account) -> None:
        # TODO: close sockets and flush the session. Background tasks are already cancelled.
        pass

    # Interactive sign-in (spec 7). Defining on_auth_begin makes the SDK serve
    # auth.begin/submit/cancel and advertise auth.interactive. To enable it: uncomment this
    # block and the check in connect(), and set "auth_methods": ["token"] in plugin.json.
    # BEGIN auth example
    # async def on_auth_begin(self, account: Account, method: str) -> None:
    #     token = await account.auth.ask("text", title="API token", hint="From your {{display_name}} settings")
    #     while not token.strip():  # TODO: check the token against the platform
    #         token = await account.auth.ask("text", title="API token", error="That token was rejected")
    #     await account.set_secret("token", token)  # comes back in account.secrets on every start
    #     await account.status("connected")
    # END auth example

    @method("messages.fetch_recent")
    async def fetch_recent(self, account: Account, p: T.MessagesFetchRecentParams) -> T.PagedMessagesResult:
        # TODO: return the newest `p.limit` messages of `p.chat_id` (newest first), and a
        # `next_cursor` for the page before them, or None when there is no more history.
        return T.PagedMessagesResult(messages=[])

    @method("messages.send")
    async def send(self, account: Account, p: T.MessagesSendParams) -> T.MessageResult:
        # TODO: send `p.text` to `p.chat_id` on {{display_name}} and return the message as the
        # platform stored it (its real id and timestamp). Raise contact_hub_sdk errors on
        # failure: FloodWait(seconds), NotFound(...), Forbidden(...), NetworkError(...).
        msg = T.Message(
            id=f"local-{next(_ids)}",
            chat_id=p.chat_id,
            direction=T.Direction.OUT,
            sent_at=datetime.now(UTC),
            text=p.text,
            delivery=T.Delivery.SENT,
        )
        return T.MessageResult(message=msg)
