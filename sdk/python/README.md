# contact-hub-sdk

Python SDK for Contact Hub plugins (plugin API v1, `docs/plugin-api.md`). It speaks the
NDJSON JSON-RPC transport and answers `initialize`, `ping` and `shutdown`. It also manages
`account.start`/`account.stop`, checks capabilities, and turns exceptions into spec errors.
You write the handlers.

```python
from datetime import UTC, datetime

from contact_hub_sdk import FloodWait, Plugin, method, types as T


class EchoPlugin(Plugin):
    capabilities = {"send.silent": True}            # spec section 14

    async def on_account_start(self, account):       # must return fast
        account.spawn(self.connect(account))         # cancelled on account.stop

    async def connect(self, account):
        await account.status("connected", me=T.Contact(id="me", display_name="Me"))

    async def on_auth_begin(self, account, method):  # optional: interactive sign-in
        code = await account.auth.ask("code", title="Enter the code")
        await account.set_secret("session", code)
        await account.status("connected")

    @method("messages.fetch_recent")
    async def fetch_recent(self, account, p: T.MessagesFetchRecentParams):
        return T.PagedMessagesResult(messages=[])

    @method("messages.send")
    async def send(self, account, p: T.MessagesSendParams):
        if p.text == "spam":
            raise FloodWait(30)
        msg = T.Message(id="1", chat_id=p.chat_id, direction=T.Direction.OUT,
                        sent_at=datetime.now(UTC), text=p.text)
        return T.MessageResult(message=msg)


if __name__ == "__main__":
    EchoPlugin().run()
```

- DTOs, event params and method params/results are dataclasses in `contact_hub_sdk.types`.
  They are generated from `schemas/` by `tools/gen_types.py`.
- `account.<event>(...)` emits an event, for example `account.messages_upsert([...], ack=True)`.
- `contact_hub_sdk.progress(done, total)` reports `transfer.progress` and resets the timeout.
  `current_request().cancelled` tells a handler that the core cancelled its request.
- Capability-gated parameters (a `schedule_at` without `send.schedule`) are answered with
  `NOT_SUPPORTED` before your handler runs. The table is the registry's `gated_params`
  (`schemas/methods/registry.json`, `METHODS[name].gated_params`), not SDK code.
- Updates of `Conversation`, `Topic` and `Message` are merged by the core: set only the
  fields that changed; a field left `None` is not sent and means "unchanged" (spec 8.4).
- `account.settings` already holds the `settings_schema` defaults from `plugin.json`.
- Names outside the registry must be vendor methods, `@method("x-<plugin-id>.<name>")`.
- Plugin -> core requests (`account.set_secret`, `ack=True` events) time out after the
  registry's `default_timeout_seconds` (`Plugin.request_timeout`) with `RpcTimeout`.

## Testing a plugin

`contact_hub_sdk.testing.FakeCore` plays the core against your plugin process: it fills
settings defaults, acks events, stores secrets, and fails the test on protocol errors
or events for accounts that are not started.

```python
from contact_hub_sdk.testing import FakeCore

def test_send(tmp_path):
    with FakeCore(["python", "-m", "my_plugin"], plugin_dir=PLUGIN_DIR, tmp=tmp_path) as core:
        core.start(secrets={"session": "s"})       # initialize + account.start
        core.event("status", state="connected")
        sent = core.call("messages.send", {"account_id": core.account_id, "chat_id": "c1", "text": "hi"})
        assert sent["message"]["text"] == "hi"
        assert core.shutdown() == 0
```

`call()` returns the result or raises `PluginError`; `error()` returns the raw error
object; `event(name, **match)` waits for an event; with `auto_answer=False`,
`request()` and `answer()` let a test answer `secrets.set` itself. Pass
`check=schema_check("path/to/schemas")` to validate every frame (needs `jsonschema`).

SDK tests: `uv run --group dev pytest` (from `sdk/python`).
