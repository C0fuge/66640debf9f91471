# {{display_name}} plugin for Contact Hub

A Contact Hub plugin on the Rust SDK (`ch-plugin-sdk`, plugin API v1). The protocol is
in `docs/plugin-api.md` of the Contact Hub repo.

## Layout

- `plugin.json`: the manifest (id `{{plugin_id}}`, entry, auth methods, settings form).
- `src/main.rs`: the plugin. Look for `TODO`.
- `tests/plugin.rs`: runs the plugin binary over stdio with the SDK's fake core
  (`ch_plugin_sdk::testing::FakeCore`), which fills settings defaults like the real core
  and fails the test on a malformed frame or an event for an account that is not started.

## Develop

```sh
cargo test              # handshake -> account.start -> messages.send -> shutdown
cargo build --release   # plugin.json's entry is ./target/release/{{crate_name}}
ch plugin test .        # the conformance suite (once the ch CLI ships)
```

To try it in the app, copy or symlink this folder (with the release binary built) into
`~/.local/share/contact-hub-ng/plugins/{{plugin_id}}/`. Building is the installer's job
(here: yours); the core only starts `entry` and shows "not installed" while it is missing.
If you ship only the binary, change `entry` in `plugin.json` to where it ends up.

## What to fill in

1. `connect()`: connect to the platform and report `status` (`connected`,
   `Unavailable` while unreachable, `Error` when the plugin cannot recover alone). Then
   emit `conversations_upsert` and `messages_upsert` as data arrives. Chats, topics and
   messages are merge-updated: after the first push, send only the fields that changed
   (`Conversation::new(id, kind).with_unread_count(n)`).
2. `messages.fetch_recent` and `messages.send`: the two required methods.
3. Capabilities: declare each optional feature you implement with
   `.capability("...")` (spec section 14). Parameters that need a capability
   (`silent`, `schedule_at`, ...) are refused by the SDK until you declare it.
4. Sign-in: if the platform needs one, uncomment the blocks marked `auth example` and set
   `"auth_methods"` in `plugin.json`. Settings flagged `"x-secret": true` in
   `settings_schema` are stored in the OS keyring by the core.

Logs go to stderr through `tracing`. Never print to stdout: it carries the protocol.
