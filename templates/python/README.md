# {{display_name}} plugin for Contact Hub

A Contact Hub plugin on the Python SDK (`contact_hub_sdk`, plugin API v1). The protocol
is in `docs/plugin-api.md` of the Contact Hub repo.

## Layout

- `plugin.json`: the manifest (id `{{plugin_id}}`, entry, auth methods, settings form).
- `src/{{module_name}}/plugin.py`: the plugin. Look for `TODO`.
- `tests/test_plugin.py`: runs the plugin over stdio, playing the core.

## Develop

```sh
uv sync                 # creates .venv, which plugin.json's entry points at
uv run pytest           # handshake -> account.start -> messages.send -> shutdown
ch plugin test .        # the conformance suite (once the ch CLI ships)
```

`tests/test_plugin.py` uses the SDK's `contact_hub_sdk.testing.FakeCore`, a stand-in for
the core; extend it as you add features.

To try it in the app, copy or symlink this folder into
`~/.local/share/contact-hub-ng/plugins/{{plugin_id}}/`. Run `uv sync` there first: the
core never installs dependencies, it only starts `entry` and shows the plugin as "not
installed" while `.venv` is missing (spec 3.4).

## What to fill in

1. `connect()`: connect to the platform and report `status` (`connected`,
   `unavailable` while unreachable, `error` when the plugin cannot recover alone). Then
   emit `conversations_upsert` and `messages_upsert` as data arrives. Updates carry only
   the fields that changed (spec 8.4).
2. `messages.fetch_recent` and `messages.send`: the two required methods.
3. `capabilities`: declare each optional feature you implement (spec section 14).
4. Sign-in: if the platform needs one, uncomment the blocks marked `auth example` and set
   `"auth_methods"` in `plugin.json`. Settings flagged `"x-secret": true` in
   `settings_schema` are stored in the OS keyring by the core.

Logs go to stderr through `logging`. Never print to stdout: it carries the protocol.
