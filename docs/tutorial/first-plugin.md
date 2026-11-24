# Write your first plugin

This tutorial builds a complete Contact Hub plugin from an empty folder. The plugin
talks to a made-up platform called **Parrot**: you have one chat, with Polly, and
Polly repeats whatever you say. Parrot needs no account, no server and no network,
so you can concentrate on the plugin API itself. Every real plugin (Discord, Signal,
Matrix, …) has the same structure; only the calls to the platform differ.

By the end you will have a plugin that:

- reports its connection state and its chats to the app;
- serves chat history page by page;
- sends messages, and receives Polly's replies;
- reports rate limits and unsupported features with the right error codes;
- declares its capabilities honestly;
- signs in with an interactive phone-and-code dialog;
- has a settings form;
- is tested, both by hand and with pytest.

You need Python 3.12 or newer and a checkout of the Contact Hub repository. The
finished code is in [`example/python/`](example/python/). Every snippet below is
copied from those files, and their test passes. A Rust version of the same plugin is
in [first-plugin-rust.md](first-plugin-rust.md).

## 1. What a plugin is

A plugin is a separate program that the Contact Hub core starts as a child process,
one process per platform. The core and the plugin talk over the plugin's standard
streams using JSON-RPC 2.0, one JSON message per line:

```
 ┌──────────── Contact Hub core ────────────┐          ┌────── your plugin ──────┐
 │ UI, database, settings, keyring          │  stdin   │ contact_hub_sdk         │
 │                                          │ ───────▶ │   your handlers         │
 │ requests: initialize, account.start,     │          │     │                   │
 │   messages.fetch_recent, messages.send   │  stdout  │     ▼                   │
 │ ◀─ responses and events: status,         │ ◀─────── │ the platform (Polly)    │
 │    conversations.upsert, messages.upsert │          │                         │
 └──────────────────────────────────────────┘          └── stderr ─▶ log file ───┘
```

- The core sends **requests** ("give me the newest 50 messages of chat X") and the
  plugin answers them.
- The plugin sends **events** whenever it learns something ("this chat exists",
  "a message arrived"). Events are JSON-RPC notifications.
- **stdout carries protocol frames only.** Anything you want to log goes to stderr;
  the core saves it to `logs/plugins/<id>.log`.
- The plugin never touches the database. It pushes what it knows, and the core stores
  and shows it.

The Python SDK, `contact_hub_sdk`, does the protocol work: framing, the handshake,
`ping`, shutdown, accounts, error objects, and capability checks. You write a class
with handlers. The full contract is in [the plugin API spec](../plugin-api.md); this
tutorial links to the relevant section at each step.

## 2. Set up a project

Create a folder for the plugin and a virtual environment with the SDK and pytest in
it. Replace `~/src/CH` with wherever your checkout of the repository is:

```sh
mkdir parrot && cd parrot
python3 -m venv .venv
.venv/bin/pip install -e ~/src/CH/sdk/python pytest
```

The plugin will be two files, `plugin.json` and `parrot.py`, plus a test.

## 3. The manifest

`plugin.json` tells the core what the plugin is and how to start it
([§3.2](../plugin-api.md#32-pluginjson)):

```json
{
  "id": "parrot",
  "name": "Parrot",
  "version": "0.1.0",
  "api_version": "1.0.0",
  "entry": ["./.venv/bin/python", "parrot.py"],
  "brand_color": "#2EAD5B",
  "multi_account": true,
  "auth_methods": ["phone_code"],
  "settings_schema": {
    "type": "object",
    "properties": {
      "nickname": {
        "type": "string", "title": "Your name", "default": "Me", "x-order": 1
      },
      "max_per_minute": {
        "type": "integer", "title": "Messages per minute",
        "description": "Polly gets grumpy if you talk faster than this.",
        "minimum": 1, "maximum": 100, "default": 20, "x-order": 2
      }
    }
  },
  "description": "A toy platform: one chat with Polly, who repeats what you say.",
  "license": "MIT"
}
```

- `id` names the platform everywhere: in account ids, log file names and `ext`
  fields. It cannot change after you publish the plugin.
- `api_version` is the version of the plugin API you implement.
- `entry` is the command line that starts the plugin. The working directory is the
  plugin folder, and an `entry[0]` that starts with `./` is resolved against it.
- `multi_account: true` lets a user add several Parrot accounts; one process serves
  them all.
- `auth_methods` lists the sign-in methods the user can choose from. Section 10
  implements `phone_code`.
- `settings_schema` describes the settings form. Section 11 covers it.

## 4. The Plugin class

A plugin is a subclass of `Plugin` with a `capabilities` dict and some handlers.
`run()` serves the protocol on stdin/stdout until the core says `shutdown`:

```python
import time
from datetime import UTC, datetime, timedelta

from contact_hub_sdk import FloodWait, NotFound, Plugin, method, types as T

CHAT = "polly"
DEMO_CODE = "1234"


class ParrotPlugin(Plugin):
    capabilities = {"messages.get": True, "send.silent": True}
```

```python
if __name__ == "__main__":
    ParrotPlugin().run()
```

The SDK answers `initialize`, `ping`, `shutdown`, `account.start` and
`account.stop` itself, and calls your hooks (`on_account_start`, `on_account_stop`,
`on_auth_begin`, …) at the right moments. Every other request goes to a method
marked with `@method("name")`. Two are required of every plugin:

- `messages.fetch_recent`: a page of a chat's history;
- `messages.send`: send a text message.

Each handler receives the started `Account` and the request's params as a typed
dataclass from `contact_hub_sdk.types` (imported as `T`). It returns the result
dataclass. The SDK checks your class when you create it: if you declare a capability
and forget one of its handlers, `ParrotPlugin()` raises `TypeError` before anything
runs.

## 5. Starting an account

When the user enables an account, the core calls `account.start` with that account's
settings, its secrets and private folders ([§4.2](../plugin-api.md#42-accountstart--accountstop)).
The SDK wraps them in an `Account` and calls `on_account_start`:

```python
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
```

Three rules apply here:

1. **Return fast.** `account.start` must not wait for the network. Start the
   connection with `account.spawn(...)`, which runs it in the background and cancels
   it on `account.stop`.
2. **Report state with `status` events.** The states are `connecting`,
   `auth_required`, `connected`, `unavailable`, `error` and `disconnected`
   ([§4.4](../plugin-api.md#44-account-states)). If the platform cannot be reached,
   report `unavailable` and keep retrying; never fail `account.start` for that.
3. **Keep per-account state on the account.** `account.data` is a dict for your
   own objects. One process serves every account, so module-level globals would be
   shared between them.

Parrot's "platform" is just a list in `account.data["history"]`. A real plugin would
open a socket or a client library session here.

Once connected, the plugin tells the core who the user is and which chats exist:

```python
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
```

Every event in [§10](../plugin-api.md#10-events-plugin--core) has a method on
`Account`, named after the event with `.` replaced by `_`. The SDK fills in
`account_id`.

- `conversations.upsert` adds or updates chats. You can send it any time a chat
  changes.
- `conversations.synced` lists **every** chat the account has. The core hides chats
  that are not in the list, so send it only after a complete sync.
- Ids are opaque strings that you choose. Use the same id for the same object every
  time; that is what makes a re-sync safe ([§8.1](../plugin-api.md#81-ids-are-opaque-strings)).
- Leave out what you don't know. An absent field means "not reported", and the core
  keeps what it already has ([§8.4](../plugin-api.md#84-nullability)). The core merges
  chats, topics and messages field by field, so a later update carries only what
  changed: `T.Conversation(id=CHAT, kind=T.ConversationKind.DM, unread_count=3)` changes
  the unread count and nothing else.

During a first sync with thousands of chats, pass `ack=True` to bulk events. Each one
is then sent as a request, and the SDK waits for the core to store it before sending
more ([§16](../plugin-api.md#16-backpressure-and-batching)).

## 6. History paging

When the user opens a chat, the core asks for its newest messages. As the user
scrolls up, it asks for older pages with the cursor the previous page returned
([§11.3](../plugin-api.md#113-messages)):

```python
    @method("messages.fetch_recent")
    async def fetch_recent(self, account, p: T.MessagesFetchRecentParams):
        history = self.chat(account, p.chat_id)
        end = int(p.cursor) if p.cursor else len(history)  # our cursor: an index into history
        start = max(0, end - p.limit)
        page = history[start:end][::-1]  # newest first
        return T.PagedMessagesResult(messages=page, next_cursor=str(start) if start else None)
```

- No `cursor` means "the newest page".
- `messages` are **newest first**.
- `next_cursor` points at the page *older* than this one. Leave it out on the last
  page.
- The cursor is yours. The core stores it and passes it back without reading it.
  Parrot uses a list index; a real platform might use the oldest message id or a
  timestamp.
- You don't need to emit the returned messages as events as well; the core stores
  the result.

`messages.get` (fetch one message) is optional. Parrot declares the `messages.get`
capability, so it must implement it:

```python
    @method("messages.get")
    async def get(self, account, p: T.ChatMessageParams):
        history = self.chat(account, p.chat_id)
        i = int(p.message_id) - 1
        return T.OptionalMessageResult(message=history[i] if 0 <= i < len(history) else None)
```

## 7. Sending

`messages.send` sends the text and returns the sent message as the platform
recorded it, with its real id and time:

```python
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
```

Polly's reply is a message that arrives from the other side, so it goes to the core
as a `messages.upsert` event, just like a live message from a real server. The helper
that builds both kinds of message:

```python
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
```

`direction` is `out` for messages this account sent and `in` for everything else.
Timestamps are ordinary timezone-aware `datetime`s; the SDK writes them in the
format the spec requires ([§8.2](../plugin-api.md#82-timestamps)).

`silent` ("send without notification") is an optional parameter, and it is only
allowed because Parrot declares the `send.silent` capability. Section 9 explains how
that works.

## 8. Errors

A handler reports failure by raising one of the SDK's exceptions. The SDK turns it
into a JSON-RPC error with the numeric code and the `data.code` string from
[§13](../plugin-api.md#13-errors). The app shows its own message for each code, so
pick the code carefully. Your `message` text only goes to the logs.

Parrot raises `NOT_FOUND` for a chat that does not exist:

```python
    def chat(self, account, chat_id):
        if chat_id != CHAT:
            raise NotFound(f"no chat {chat_id!r}", kind="chat")
        return account.data["history"]
```

**FLOOD_WAIT.** When the platform rate-limits you, raise `FloodWait(seconds)`. The
app shows a countdown and does not retry before it ends. Polly allows
`max_per_minute` messages per minute:

```python
    def check_rate(self, account):
        limit = account.setting("max_per_minute")
        now = time.monotonic()
        recent = [t for t in account.data["sends"] if now - t < 60]
        if len(recent) >= limit:
            raise FloodWait(60 - (now - recent[0]))
        account.data["sends"] = [*recent, now]
```

**NOT_SUPPORTED** means "this plugin or platform cannot do that". You will rarely
raise it yourself, because the SDK does it for you in two cases:

- the core calls a method whose capability you did not declare. Parrot has no `edit`
  capability, so `messages.edit` is answered with `NOT_SUPPORTED`;
- a request uses a parameter whose capability you did not declare.
  `messages.send` with `schedule_at` needs `send.schedule`, which Parrot lacks. The
  list of such parameters is `gated_params` in the method registry,
  `schemas/methods/registry.json` ([§14.2](../plugin-api.md#142-the-rule)).

Raise `NotSupported(capability="...")` yourself when only the platform knows, for
example when one kind of chat on your platform cannot do what the others can. Never
answer a missing feature with an empty success.

The other exceptions are `InvalidParams`, `AuthRequired`, `Forbidden`,
`NetworkError` (temporary, can be retried), `Unavailable` and `Internal`. Any other
exception becomes `INTERNAL` and its traceback goes to the log.

## 9. Capabilities

Capabilities are how the app knows what your plugin can do. The plugin declares them
in `initialize`, and from then on ([§14](../plugin-api.md#14-capabilities)):

- the UI only shows a control (Edit, Pin, React, Schedule, …) if its capability is
  declared;
- the core only calls a method if its capability is declared;
- a declared capability means you implement **every** method it covers. The
  conformance suite checks this.

```python
class ParrotPlugin(Plugin):
    capabilities = {"messages.get": True, "send.silent": True}
```

A value is `True`, or an object of parameters such as `{"edit": {"window_seconds":
172800}}`. The full list is the registry in [§14.3](../plugin-api.md#143-registry).
You don't list `auth.interactive`: the SDK adds it because the class implements
`on_auth_begin` (next section). Start with a few capabilities and add more one at a
time. A user is better served by a missing button than by a button that fails.

## 10. Interactive sign-in

The core has no sign-in screens for specific platforms. The plugin describes each
step, and the core draws it ([§7](../plugin-api.md#7-interactive-auth)):

```
plugin ── status(auth_required) ──▶ core shows "Sign in"
core   ── auth.begin(phone_code) ─▶ plugin
plugin ── auth.step(kind=phone) ──▶ core shows a phone field
core   ── auth.submit(value) ─────▶ plugin
plugin ── auth.step(kind=code) ───▶ core shows a code field   (again, with `error`, if wrong)
plugin ── auth.step(kind=done) ───▶ …
plugin ── status(connected) ──────▶ core closes the dialog
```

With the SDK, the whole flow is one coroutine. `account.auth.ask(kind, ...)` shows
a step and waits for the user's answer:

```python
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
```

- A wrong code is **not** an error response. Ask for the same step again with
  `error=` set, and the dialog shows the message ([§7.2](../plugin-api.md#72-rules)).
- `account.set_secret` stores a credential in the OS keyring through the core
  ([§6.2](../plugin-api.md#62-secrets)). The core passes it back in `account.secrets`
  on every later `account.start`, so the user signs in once (look back at
  `on_account_start`). Never write credentials to disk or to the log yourself.
- If the user closes the dialog, the SDK cancels the coroutine and reports
  `auth_required` again. If the core calls `auth.begin` again, the flow restarts.
- If the plugin signs in from its settings alone (a bot token), declare
  `"auth_methods": ["none"]` and don't implement `on_auth_begin`.

## 11. Settings

`settings_schema` in the manifest is a JSON Schema that the app turns into a form
([§6.1](../plugin-api.md#61-settings_schema)). The core validates what the user
enters and passes the values to `account.start`, with the schema's `default` filled
in for every property the user never set. The SDK applies the same defaults from
`plugin.json`, so `account.settings` is complete even under a test harness.
`account.setting(key)` reads one value. That is why Parrot can call
`account.setting("max_per_minute")` without a fallback of its own.

- Mark tokens and passwords with `"x-secret": true`. The core keeps them in the
  keyring and merges them into the settings at start.
- `x-order`, `x-group` and `x-widget` control the layout of the form.
- When the user changes settings, the core stops the account and starts it again
  with the new values. There is no "settings changed" call to handle.

## 12. Test it standalone

You don't need the app to try the plugin. Any program that writes JSON-RPC lines to
its stdin and reads its stdout can play the core.

### By hand

[`session.ndjson`](example/python/session.ndjson) has six requests: `initialize`,
`account.start` with a saved session, a two-message history page, two sends, and
`shutdown`. Feed it to the plugin:

```sh
.venv/bin/python parrot.py < session.ndjson
```

The output, shortened:

```
{"jsonrpc":"2.0","id":1,"result":{"api_version":"1.0.0","capabilities":{"messages.get":true,"send.silent":true,"auth.interactive":true},"plugin_version":"0.1.0"}}
{"jsonrpc":"2.0","id":2,"result":{}}
{"jsonrpc":"2.0","id":3,"result":{"messages":[{"id":"50",…,"text":"Squawk #50!",…},{"id":"49",…}],"next_cursor":"48"}}
{"jsonrpc":"2.0","id":4,"result":{"message":{"id":"51","chat_id":"polly","direction":"out",…,"text":"hello",…}}}
{"jsonrpc":"2.0","id":5,"error":{"code":-32001,"message":"schedule_at needs capability send.schedule","data":{"code":"NOT_SUPPORTED","capability":"send.schedule"}}}
{"jsonrpc":"2.0","method":"status","params":{"account_id":"parrot","state":"connecting","reconnecting":false}}
{"jsonrpc":"2.0","method":"status","params":{"account_id":"parrot","state":"connected","reconnecting":false,"me":{"id":"me","display_name":"Me"}}}
{"jsonrpc":"2.0","method":"contacts.upsert","params":{"account_id":"parrot","contacts":[…]}}
{"jsonrpc":"2.0","method":"conversations.upsert","params":{"account_id":"parrot","conversations":[…]}}
{"jsonrpc":"2.0","method":"conversations.synced","params":{"account_id":"parrot","chat_ids":["polly"]}}
{"jsonrpc":"2.0","method":"messages.upsert","params":{"account_id":"parrot","messages":[{"id":"52",…,"text":"Squawk! hello",…}]}}
{"jsonrpc":"2.0","method":"status","params":{"account_id":"parrot","state":"disconnected","reconnecting":false}}
{"jsonrpc":"2.0","id":6,"result":{}}
```

The SDK serves requests concurrently, so when they arrive all at once the answers
can overtake the account's first events. Events never come before the
`account.start` answer, though, and `disconnected` comes before the `shutdown`
answer ([§4.2](../plugin-api.md#42-accountstart--accountstop)). The end of stdin
means "the core has gone": the plugin finishes the requests it is still working on
for up to 3 seconds, then exits ([§2.5](../plugin-api.md#25-shutdown-and-eof)).
You can also run `.venv/bin/python parrot.py` and paste the lines one at a time.

### With pytest

[`test_parrot.py`](example/python/test_parrot.py) does the same thing from a test.
The SDK's `contact_hub_sdk.testing.FakeCore` starts `parrot.py` as a child process
and plays the core. `start()` sends `initialize` and `account.start` (with the
settings defaults filled in, as the real core does), `call()` sends a request and
returns its result, `error()` returns the error of a request that must fail, and
`event()` waits for an event. Events sent as requests are acknowledged, and
`secrets.set` lands in `core.secrets`. It also fails the test if the plugin breaks
the protocol, for example by emitting an event for an account that is not started.
The core's data directories point into pytest's temporary folder, so a test can
never touch a real Contact Hub profile:

```python
@pytest.fixture
def core(tmp_path):
    # CH_DATA_DIR and XDG_* point into tmp_path: a test never touches a real profile.
    with FakeCore([sys.executable, "parrot.py"], plugin_dir=HERE, tmp=tmp_path) as c:
        yield c
```

In the tests, `A` is `{"account_id": "parrot"}`: the account `start()` creates is
named after the plugin. The sign-in test walks through the whole dialog, including
the wrong code:

```python
def test_sign_in(core):
    core.start()
    core.event("status", state="auth_required")
    assert core.call("auth.begin", {**A, "method": "phone_code"}) == {}
    step = core.event("auth.step")["step"]
    assert step["kind"] == "phone"
    core.call("auth.submit", {**A, "flow": "sign_in", "step_id": step["step_id"], "value": "+15550100"})
    step = core.event("auth.step")["step"]
    assert step["kind"] == "code"
    core.call("auth.submit", {**A, "flow": "sign_in", "step_id": step["step_id"], "value": "0000"})
    step = core.event("auth.step")["step"]
    assert step["error"] == "Wrong code, try again."
    core.call("auth.submit", {**A, "flow": "sign_in", "step_id": step["step_id"], "value": "1234"})
    core.event("status", state="connected")
    assert core.secrets["parrot"] == {"session": "parrot:+15550100"}
    assert core.event("conversations.synced")["chat_ids"] == ["polly"]
```

The error codes get their own test:

```python
def test_send_and_errors(core):
    core.start(settings={"max_per_minute": 2}, secrets={"session": "s"})
    sent = core.call("messages.send", {**A, "chat_id": "polly", "text": "hi", "silent": True})
    assert sent["message"]["direction"] == "out" and sent["message"]["silent"] is True
    reply = core.event("messages.upsert")["messages"][0]
    assert reply["text"] == "Squawk! hi"

    err = core.error("messages.send", {**A, "chat_id": "nope", "text": "x"})
    assert err["data"]["code"] == "NOT_FOUND"
    err = core.error("messages.send", {**A, "chat_id": "polly", "text": "x",
                                       "schedule_at": "2030-01-01T00:00:00.000Z"})
    assert err["data"] == {"code": "NOT_SUPPORTED", "capability": "send.schedule"}
    err = core.error("messages.edit", {**A, "chat_id": "polly", "message_id": "1", "text": "x"})
    assert err["data"]["code"] == "NOT_SUPPORTED"

    core.call("messages.send", {**A, "chat_id": "polly", "text": "two"})
    err = core.error("messages.send", {**A, "chat_id": "polly", "text": "three"})
    assert err["data"]["code"] == "FLOOD_WAIT" and 0 < err["data"]["seconds"] <= 60
```

Other tests cover paging through all 50 messages with no gaps or repeats, the
declared capabilities, and a clean `shutdown`. Run them:

```sh
.venv/bin/python -m pytest -q
```

```
.....                                                                    [100%]
5 passed in 0.47s
```

Before you share a plugin, also run the conformance suite, `ch plugin test
./parrot`. It checks the handshake, validates every frame against the JSON Schemas,
calls every method of every declared capability, and checks the error codes.

## 13. Install it

The core never installs a plugin's dependencies: it starts `entry` and shows the
plugin as "not installed" while `entry[0]` is missing
([§3.4](../plugin-api.md#34-installation)). So install the dependencies first (the
`.venv` from section 2), then copy the folder (with its `.venv`) into the user
plugin directory,
`$XDG_DATA_HOME/contact-hub-ng/plugins/` (by default
`~/.local/share/contact-hub-ng/plugins/parrot/`), or use "Install plugin…" in
Settings → Accounts ([§3.1](../plugin-api.md#31-discovery)). The app finds
`plugin.json`, shows Parrot next to the built-in platforms, and warns the user that a
third-party plugin can read that account's messages before it enables it.

## 14. Where to go next

- **The spec.** [`docs/plugin-api.md`](../plugin-api.md) is the full contract.
  The [checklist in §20](../plugin-api.md#20-checklist-for-a-new-plugin) is a good
  order for building a real plugin. The data model is in
  [§9](../plugin-api.md#9-data-model-dtos), every request in
  [§11](../plugin-api.md#11-requests-core--plugin), and media and transfers in
  [§15](../plugin-api.md#15-media-files-and-transfers). The machine-readable
  contract is the JSON Schemas in `schemas/`.
- **Templates.** `ch plugin new <name>` creates a new plugin from the templates in
  `templates/`, with the manifest, the SDK skeleton and the conformance tests
  already set up. That is the quickest start for a real plugin.
- **The SDK.** [`sdk/python/README.md`](../../sdk/python/README.md) has the rest of
  the API, such as `progress()` for file transfers, `current_request().cancelled`
  for cancellation, and `ack=True` for bulk sync.
- **Other languages.** The protocol is plain JSON-RPC, so any language works.
  [first-plugin-rust.md](first-plugin-rust.md) builds Parrot again with the Rust SDK,
  `crates/ch-plugin-sdk`.
