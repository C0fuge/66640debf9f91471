"""Parrot: a toy Contact Hub plugin. One chat with Polly, who repeats what you say."""

import time
from datetime import UTC, datetime, timedelta

from contact_hub_sdk import FloodWait, NotFound, Plugin, method, types as T

CHAT = "polly"
DEMO_CODE = "1234"


class ParrotPlugin(Plugin):
    capabilities = {"messages.get": True, "send.silent": True}

    # ---- lifecycle -------------------------------------------------------------------

    async def on_account_start(self, account):
        # The "platform": 50 old messages, oldest first. Message id = position + 1.
        start = datetime.now(UTC) - timedelta(hours=1)
        account.data["history"] = []
        for i in range(50):
            self.add(account, "in", f"Squawk #{i + 1}!", at=start + timedelta(minutes=i))
        account.data["sends"] = []  # monotonic times of recent sends, for the rate limit

        if "session" in account.secrets:  # signed in before: go straight online
            account.spawn(self.connect(account))
        else:
            await account.status("auth_required")

    async def connect(self, account):
        await account.status("connecting")
        me = T.Contact(id="me", display_name=account.setting("nickname"))
        await account.status("connected", me=me)
        await account.contacts_upsert([T.Contact(id="polly", display_name="Polly", is_bot=True)])
        last = account.data["history"][-1]
        polly = T.Conversation(id=CHAT, kind=T.ConversationKind.DM, title="Polly",
                               peer_user_id="polly", last_message_at=last.sent_at)
        await account.conversations_upsert([polly])
        await account.conversations_synced([CHAT])

    # ---- interactive sign-in -----------------------------------------------------------

    async def on_auth_begin(self, account, method):
        phone = await account.auth.ask("phone", title="Your phone number", placeholder="+15550100")
        error = None
        while True:
            code = await account.auth.ask(
                "code", title="Enter the code", hint=f"Polly's code is always {DEMO_CODE}.",
                min_length=4, max_length=4, error=error,
            )
            if code == DEMO_CODE:
                break
            error = "Wrong code, try again."
        await account.set_secret("session", f"parrot:{phone}")
        await account.auth.done()
        await self.connect(account)

    # ---- history -----------------------------------------------------------------------

    @method("messages.fetch_recent")
    async def fetch_recent(self, account, p: T.MessagesFetchRecentParams):
        history = self.chat(account, p.chat_id)
        end = int(p.cursor) if p.cursor else len(history)  # our cursor: an index into history
        start = max(0, end - p.limit)
        page = history[start:end][::-1]  # newest first
        return T.PagedMessagesResult(messages=page, next_cursor=str(start) if start else None)

    @method("messages.get")
    async def get(self, account, p: T.ChatMessageParams):
        history = self.chat(account, p.chat_id)
        i = int(p.message_id) - 1
        return T.OptionalMessageResult(message=history[i] if 0 <= i < len(history) else None)

    # ---- sending -----------------------------------------------------------------------

    @method("messages.send")
    async def send(self, account, p: T.MessagesSendParams):
        self.chat(account, p.chat_id)
        self.check_rate(account)
        sent = self.add(account, "out", p.text, silent=p.silent)
        account.spawn(self.reply(account, p.text))
        return T.MessageResult(message=sent)

    async def reply(self, account, text):
        msg = self.add(account, "in", f"Squawk! {text}")
        await account.messages_upsert([msg])

    def check_rate(self, account):
        limit = account.setting("max_per_minute")
        now = time.monotonic()
        recent = [t for t in account.data["sends"] if now - t < 60]
        if len(recent) >= limit:
            raise FloodWait(60 - (now - recent[0]))
        account.data["sends"] = [*recent, now]

    # ---- helpers -----------------------------------------------------------------------

    def chat(self, account, chat_id):
        if chat_id != CHAT:
            raise NotFound(f"no chat {chat_id!r}", kind="chat")
        return account.data["history"]

    def add(self, account, direction, text, *, at=None, silent=None):
        history = account.data["history"]
        incoming = direction == "in"
        msg = T.Message(
            id=str(len(history) + 1), chat_id=CHAT, direction=direction,
            sent_at=at or datetime.now(UTC), text=text, silent=silent,
            sender_id="polly" if incoming else "me",
            sender_name="Polly" if incoming else account.setting("nickname"),
        )
        history.append(msg)
        return msg


if __name__ == "__main__":
    ParrotPlugin().run()
