# SDK conformance to-do (contract amendments of 2026-11-09)

Source of truth: `docs/plugin-api.md` (sections cited), `schemas/`, `crates/ch-protocol`.
Those three are already updated and green (`cargo test --workspace`, clippy `-D warnings`,
`sdk/python` 27 tests, `plugins/fake` 8 tests). This file lists what the **Python SDK**
(`sdk/python/src/contact_hub_sdk`), the **Rust SDK** (`crates/ch-plugin-sdk`) and the
**fake plugin** (`plugins/fake`) still have to change so both SDKs behave identically.
Each item is self-contained: behaviour, file pointers, a test to add. Work items are
independent unless marked. Run everything isolated (`CH_DATA_DIR`/`XDG_*` to a temp dir),
and `rm -rf target` when done.

Already done in this pass (do not redo): `sdk/python/tools/gen_types.py` was re-run
(`types.py` now has `bool | None`/`int | None` for the 14 merge fields);
`sdk/python/tests/test_wire.py::test_optionals_omitted_merge_fields_not_defaulted` replaces
the old "defaults written" test; `crates/ch-plugin-sdk/tests/e2e.rs` no longer expects
`"edited": false` in the `messages.send` result (no other Rust SDK code needed changes).

## 1. Partial updates: absent = unchanged (spec §8.4, §9.2, §9.3, §9.6, §10)

Behaviour: for `Conversation`, `Topic`, `Message`, a field the plugin did not set is **not
written** on the wire (not `false`, not `0`). Only those three DTOs changed; nested value
objects (`Poll.closed`, `Media.has_spoiler`, …) and `Folder.is_shared`, `StickerPack.count`,
`AuthStep.can_resend`, `status.reconnecting`, `topics.upsert.complete` keep their defaults.

- Python SDK: already correct after regeneration (`to_wire` omits `None`). Check
  `_emitters.py` and `plugin.py` for any place that constructs `T.Conversation`/`T.Topic`/
  `T.Message` with explicit `archived=False`/`unread_count=0`/`edited=False` and drop those
  (they would now be *sent*, meaning "reset"). Test: `tests/test_wire.py` has it.
- Rust SDK: the 14 fields are now `Option<bool>`/`Option<u64>` in `ch_protocol::dto`.
  Nothing in `src/` constructs them, so it compiles. When adding builders (item 12) make the
  builder setters take the plain value and wrap in `Some`. Test (`tests/e2e.rs`): a
  `conversations.upsert` emitted from a handler with only `id`/`kind`/`unread_count` arrives
  with exactly those three keys.
- Fake plugin (`plugins/fake/fake_plugin/world.py`, `plugin.py`): on the *first* emission of
  each chat/topic/message send the full state including `archived`, `unread_count`,
  `unread_mentions`, `unread_reactions`, `is_self_chat` (and `edited`/`is_scheduled` for
  messages, `is_general`/`pinned`/`closed`/`hidden` for topics) **explicitly**, since the
  core would otherwise use the creation defaults (which for the fake are the right values
  anyway, so this is for determinism of the recorded wire). For *later* updates (a read
  that changes `unread_count`, a pin) emit only the changed fields. Test
  (`tests/test_e2e.py`): after `read.send_receipt`, the `conversations.upsert` that follows
  carries `unread_count` and no `archived`/`is_self_chat`.

## 2. Open `ErrorCode` (spec §13)

Behaviour: an unknown `data.code` string and an unknown numeric code validate and are
treated as `INTERNAL` (string kept for logs). Emitters only use known codes.

- Python SDK `errors.py`: `PluginError.numeric` already falls back to `INTERNAL`'s number
  (`NUMERIC_CODES.get(..., -32603)`). Make sure decoding an error *from the core* (used by
  `Plugin.request`, `transport.py`) never raises on an unknown `data.code` or unknown numeric
  code, and that `str(err)`/logging keeps the original string. Test: a `secrets.set` answered
  with `{"code": -32010, "data": {"code": "SOMETHING_NEW"}}` surfaces as a `PluginError`
  whose `.code == "SOMETHING_NEW"` and `.numeric == -32603` on re-emission.
- Rust SDK: `ch_protocol::ErrorCode` already keeps `Unknown(String)`; `error.rs::normalize`
  only rewrites the numeric code for known codes. Nothing to change; add one e2e assertion
  that a core error with an unknown code is handed to the plugin's `request()` caller intact.
- Fake: nothing.

## 3. Capability-gated parameters from the registry (spec §14.2)

Behaviour: `registry.json` rows carry `gated_params: [{param, capability, equals?}]`.
`param` is a dotted path; a `[]` segment means any array element
(`reactions[].custom_emoji_id`, `items[].spoiler`). A gated parameter is *present* when the
value at the path is not absent and not `null` and, if `equals` is set, equals it
(`kind = "voice"` → `send.voice`; `revoke = false` → `delete.for_me`). Present without the
capability ⇒ `NOT_SUPPORTED` with `data.capability = <key>`, before the handler runs.
Rows with gates: `messages.send`, `messages.send_file`, `messages.send_album`,
`messages.edit` (`entities`, `path`), `messages.delete`, `messages.forward`, `polls.create`,
`reactions.set`, `chats.set_flags`, `members.list`.

- Python SDK: `tools/gen_types.py::registry_module` must emit `gated_params` into
  `registry.py` (`MethodInfo` gains `gated_params: tuple[GatedParam, ...]`, a frozen
  dataclass `GatedParam(param, capability, equals=None)`), then delete the hand-written
  `PARAM_CAPABILITIES`, `_SEND_COMMON`, `_QUOTE`, `_QUOTE_METHODS`, `_FILE_KINDS` and the
  ad-hoc checks in `plugin.py` (lines ~43–66 and the `need(...)` block ~600–625) and replace
  them with one generic path walker over `ctx.raw_params`. `messages.edit.entities` is new
  (it was missing from the hand table). Test (`tests/test_e2e.py`): `messages.send` with
  `silent: null` passes; with `reply_to.quote_text` and no `quote_reply` →
  `NOT_SUPPORTED` with `data.capability == "quote_reply"`; `reactions.set` with a
  `custom_emoji_id` element and no `reactions.custom` → `NOT_SUPPORTED`; `messages.edit`
  with `entities` and no `send.entities` → `NOT_SUPPORTED`. `tests/test_generated.py`
  (`--check`) must still pass after regenerating.
- Rust SDK: enforcement is missing entirely. In `src/plugin.rs::dispatch`, after the
  method-level capability check and before calling the handler, run
  `ch_protocol::method(name).missing_capabilities(&params, |c| caps.has(c))` and answer
  `ErrorObject::not_supported(first.capability)` if any. Test (`tests/e2e.rs`): the same four
  cases as Python.
- Fake: declares 56 capabilities; verify its own handlers no longer check gated params
  themselves (the SDK does it), and that `tests/test_e2e.py` still passes.

## 4. Timeouts for plugin → core requests (spec §4.6, §12)

Behaviour: `secrets.set`, `secrets.delete` and events sent as requests time out after
`registry.default_timeout_seconds` (60). On expiry the plugin sends `$/cancelRequest` for
that id and the caller gets `TIMEOUT`.

- Python SDK: `gen_types.py` should export `DEFAULT_TIMEOUT_SECONDS` from the registry JSON
  into `registry.py`; `plugin.py::request`/`_timeout_for` use it (it is 60 today, hardcoded).
  Test: a `secrets.set` the fake core never answers raises `RpcTimeout` after the (patched,
  short) timeout and a `$/cancelRequest` for its id was written.
- Rust SDK: `ch_protocol::DEFAULT_TIMEOUT_SECONDS` exists; make `host.rs` use it for
  `request()`/acked `emit()` instead of a local constant, and send `$/cancelRequest` on
  expiry. Test as above (`tokio::time::pause`).

## 5. `account.start` on an already-started account (spec §4.2)

Behaviour: `INVALID_PARAMS`, `data.field = "account_id"`, message "already started"; the
running account is untouched.

- Python SDK `plugin.py::_account_start` (~line 668): currently stops and restarts. Replace
  with `raise InvalidParams("account_id", ...)`. Test: start twice → second answer is
  `-32602` with `data.field == "account_id"`, and the first account still receives events.
- Rust SDK: already does this (`plugin.rs` ~575). Keep; the e2e test already covers it.
- Fake: nothing.

## 6. Major-version mismatch (spec §4.1)

Behaviour: the plugin answers `initialize` normally with its own `api_version`; it never
refuses because of the version. An SDK may log a warning.

- Python SDK `plugin.py::_initialize` (~line 627): remove the `raise Internal(...)` on major
  mismatch; log a warning instead. Test: `initialize` with `api_version: "2.0.0"` answers
  with `api_version: "1.0.0"` and capabilities.
- Rust SDK: already answers; the `on_initialize` doc comment still suggests returning
  `INTERNAL` to refuse — reword to "return an error only for reasons other than the version".

## 7. Requests before `initialize`; second `initialize` (spec §4.1)

Behaviour: any request before `initialize` → `INVALID_REQUEST`; notifications (including
`$/cancelRequest`) are ignored with a stderr log; a second `initialize` → `INVALID_REQUEST`.

- Python SDK `plugin.py::_on_request` (~line 542): the request case exists; add the
  second-`initialize` case and make sure notifications before init are dropped, not
  processed. Test: `ping` before init → `-32600`; `initialize` twice → second is `-32600`.
- Rust SDK `plugin.rs::dispatch` (~line 482): same; today `initialize` is accepted again
  (`*initialized |= r.is_ok()`). Test as Python.

## 8. `status: disconnected` before the `account.stop` reply (spec §4.2)

Behaviour: SHOULD emit `status(disconnected)` for the account, then answer `{}`, then no
more events for it. `account.stop` for an unknown account → `UNKNOWN_ACCOUNT`.

- Python SDK: `_stop_account` already emits it (only if `_accepted`); fine. Add the test if
  missing: the frame order is `status(disconnected)` then the response.
- Rust SDK: check `plugin.rs::stop_account` order; the e2e test should assert the order.

## 9. No event before the `account.start` reply (spec §4.2)

Behaviour: events produced while `on_account_start` runs are queued and flushed right after
the response, in order; if none of them is a `status`, a `status(connecting)` is inserted
first.

- Python SDK: implemented (`accept()` in `_account_start`). Keep the test.
- Rust SDK: `plugin.rs` replies then calls `inner.ensure_status(shared)`; verify that
  `Account::emit` called *inside* `on_start` is queued, not written immediately (look at
  `AccountInner` in `host.rs`; if there is no queue, add one that is drained after
  `self.reply(...)`). Test (`tests/e2e.rs`): a plugin whose `on_start` emits
  `conversations.upsert` → the core reads the `account.start` response first, then
  `status(connecting)` (inserted), then the upsert.

## 10. EOF with requests in flight (spec §2.5)

Behaviour: on stdin EOF the plugin keeps writing responses of in-flight requests that
finish within its exit budget (3 s in both SDKs), drops the rest, stops accounts, exits 0.
In particular a `shutdown` whose handler is running when EOF arrives is still answered.

- Python SDK `plugin.py::run` (~line 486) calls `self._conn.cancel_all()` first, which
  cancels in-flight tasks without answering. Change to: stop reading, wait up to the budget
  for in-flight tasks to finish and write their responses (stdout stays open), cancel the
  rest, then `_stop_all`. Test (`tests/test_e2e.py`): send `shutdown`, close stdin
  immediately, assert the `shutdown` response still arrives and the process exits 0.
- Rust SDK `plugin.rs` (~line 327–336, `stop_all(EOF_STOP_BUDGET)`): check the same; the
  writer must not be closed before in-flight responses drain. Test as above.

## 11. Settings defaults from `plugin.json` (spec §6.1)

Behaviour: the core fills top-level `settings_schema` `default`s; the SDK does the same from
the manifest it read at `initialize` (`plugin_dir/plugin.json`), so a plugin sees the same
`settings` with or without a core.

- Python SDK: implemented (`_load_manifest` + `Account.settings` fallback). Keep.
- Rust SDK `settings.rs` (comment at line 17 says it does not): read `plugin.json` in
  `initialize` (`InitializeParams.plugin_dir`), keep `settings_schema.properties[*].default`,
  and merge them under the incoming `settings` in `account.start`. Test: a manifest with
  `{"proxy": {"type": "string", "default": ""}}` and `settings: {}` → `settings.get("proxy")
  == Some("")`.

## 12. Rust DTOs: `Default` and builders; box `ErrorObject` in the SDK

- `Conversation`, `Message`, `Contact`, `Topic`, `Folder`, `Draft` need an ergonomic way to
  build partial updates now that almost everything is `Option`. Do **not** add `Default` to
  structs with required fields in `ch-protocol` (a defaulted `id` is a bug waiting to
  happen); instead add constructors in `ch-protocol` taking the required fields
  (`Conversation::new(id, kind)`, `Message::new(id, chat_id, direction, sent_at)`,
  `Topic::new(id, chat_id, title)`) with every optional field `None`, plus chainable
  `with_*`/setter methods for the common optional ones in the Rust SDK (or a small
  `#[derive(bon::Builder)]`-free hand-written builder — no new dependency). Test
  (`ch-protocol/tests/rules.rs`): `Conversation::new("c", Group)` serialises to exactly
  `{"id": "c", "kind": "group"}`.
- `result_large_err`: `ch-protocol` keeps `ErrorObject` unboxed (it is a wire DTO) and keeps
  its crate-level `allow`. The Rust SDK's handler error becomes
  `pub struct PluginError(Box<ErrorObject>)` (`From<ErrorObject>`, `Deref`, the constructors
  in `error.rs` return it, `HandlerResult<T> = Result<T, PluginError>`), so plugin crates
  need no `allow(clippy::result_large_err)`. Update `examples/echo.rs`,
  `templates/rust/src/main.rs` and `templates/TEMPLATE_SPEC.md` accordingly; remove any
  `allow(result_large_err)` from the template.

## 13. Fake-core test helper in both SDKs

Both SDKs' e2e tests hand-roll a core over pipes. Extract it as a public, documented helper
plugin authors can use (`contact_hub_sdk.testing.FakeCore` with `start()`, `call(method,
params) -> result | PluginError`, `event(name)`, `answer(request)`; Rust
`ch_plugin_sdk::testing::FakeCore` with the same surface over in-memory duplex pipes).
It must apply `settings_schema` defaults (item 11) like the real core and reject events for
accounts that are not started (spec §4.2). Rewrite the templates' tests
(`templates/python/tests/test_plugin.py`, `templates/rust/tests/plugin.rs`) and the fake
plugin's `tests/conftest.py` on top of it. Test: each SDK's own e2e suite runs on the helper.

## 14. Vendor methods (spec §12.3)

- Python SDK `@method("x-<id>.<name>")` and Rust `PluginBuilder::method(...)`: validate at
  build time that any method name not in the registry matches `^x-[a-z][a-z0-9_]{1,31}\.`
  and warn (not fail) if it does not use this plugin's `id`. Test: registering `foo.bar`
  (not in the registry) fails fast with a clear message.

## 15. Documentation touch-ups

- `templates/TEMPLATE_SPEC.md` and `docs/tutorial/`: mention that partial updates carry only
  changed fields; `uv sync` is the installer's/developer's job (spec §3.4); install path is
  `~/.local/share/contact-hub-ng/plugins/`.
- `sdk/python/README.md`, `crates/ch-plugin-sdk/src/lib.rs` docs: point to the registry's
  `gated_params` instead of a hand table.

## For the core's plugin-host (not SDK work, recorded here so it is not lost)

- Merge Conversation/Topic/Message field by field; apply "on create" defaults only on insert;
  `message.edited` snapshots only when `text`/`entities`/`media` differ.
- Fill `settings_schema` top-level defaults into `account.start.settings`.
- Enforce `gated_params` on the way out too (never send a gated param to a plugin lacking
  the capability) — one table, `ch_protocol::MethodInfo::missing_capabilities`.
- Ignore events for accounts not started; treat `UNKNOWN_ACCOUNT` from `account.stop` as
  stopped; never `account.start` twice without a stop in between; wait for the stop reply.
- Never close stdin before the `shutdown` reply or the grace expiry.
- Apply successful request effects itself; be idempotent for echoes; ignore `draft.changed`
  older than its own draft.
- Local FTS over ingested messages; no `messages.search`.
- "Not installed" state when `entry[0]` is missing; never run `uv sync` itself.
