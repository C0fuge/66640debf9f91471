# Telegram plugin

The Contact Hub Python SDK runs one process for Telegram. Each started account owns its Telethon client, its event handlers, its login flow and a SQLite session. The plugin uses API 1.0.0 and Telethon 1.46.0. The dependency lock pins the client, proxy support and transitive dependencies. [Telethon release](https://pypi.org/project/Telethon/1.46.0/) · [Upstream MIT license](TELETHON-LICENSE)

## Install and check

Run from `plugins/telegram` in a repository checkout with Python 3.12 or newer and uv.

```sh
uv sync --locked
uv run --locked pytest -q
```

The host discovers `plugin.json` and runs `./.venv/bin/python -m telegram_plugin`. For a runtime-only install use `uv sync --locked --no-dev`. The source checkout supplies the SDK from `../../sdk/python`.

Enter the numeric `api_id` in account settings and the application’s `api_hash` in the secret field. Optional SOCKS5 host/port are settings. The proxy username and password are separate top-level secrets. The host supplies each account's private `state_dir`. The plugin writes `telegram.session` there with mode 0600, inside a mode-0700 directory. Phone numbers, login codes and passwords submitted during sign-in stay in the flow. Telethon also maintains its own entity and update state inside the session database. Contact Hub owns the message cache. Legacy session migration belongs to the later importer. [Telegram application credentials](https://core.telegram.org/api/obtaining_api_id) · [Telethon sessions](https://docs.telethon.dev/en/stable/concepts/sessions.html)

## Implemented wire methods

| Capability | Methods | Behavior |
| --- | --- | --- |
| SDK lifecycle | `initialize`, `ping`, `account.start`, `account.stop`, `shutdown` | Background connection, sign-in state, bounded network reconnect, initial sync, per-account stop and EOF cleanup. A failed initial sync reports a status error or retry. |
| `auth.interactive` | `auth.begin`, `auth.submit`, `auth.cancel` | Phone/code and QR login, including rejected input, expiring QR tokens, and Telegram's optional two-step password challenge. |
| `chat.sync`, `contacts.list` | `chats.sync`, `contacts.list` | Dialog metadata, preview messages, full chat membership snapshot and contacts. Bulk sync waits for host acknowledgements. |
| Required history, `history.jump_to_date`, `history.fill_gap` | `messages.fetch_recent`, `history.around`, `history.fill_gap` | Newest-first recent, older and date-centered pages, oldest-first gap pages with exclusive endpoints. Cursors encode direction and account/chat/topic scope. |
| Required text send, `send.entities`, `send.silent`, `send.no_webpage` | `messages.send` | Plain or formatted text, replies, topic root, silent delivery and preview suppression. A supplied client ID determines the MTProto random ID. A successful acknowledgement echoes the client ID in the returned message and a message upsert. |
| `messages.get`, `edit`, `edit.caption` | `messages.get`, `messages.edit` | Chat-scoped lookup, text/caption and entity edits, preview suppression, unchanged-edit retries. Media replacement is unsupported. |
| `delete`, `delete.for_me`, `forward`, `forward.drop_author` | `messages.delete`, `messages.forward` | Batch deletion and forwarding with complete chat validation before mutation; silent forwarding and author/caption removal. Channel deletion only for me is rejected. Scheduling is unsupported. |
| `pin`, `unpin` | `messages.pin`, `messages.unpin` | Silent and personal private-chat pins, one-message or all-message unpinning, including paged unpin acknowledgements. |
| `read_receipts`, `typing`, `drafts.sync` | `read.send_receipt`, `typing.send`, `drafts.save` | Explicit chat/topic receipts and supported typing actions; formatted chat drafts and explicit clearing. Topic draft writes are rejected until their clearing semantics are verified. Server timestamps are authoritative. |

Entity ranges use UTF-16 units. The handshake lists each supported outbound entity type. Incoming named mentions carry the Telegram user ID. Message and draft events preserve Telegram’s marked peer IDs and forum topic IDs where supplied. Forward attribution, quote entities/offsets, service actions, pin/silent flags, reply counts, TTL and bot/effect IDs retain their known fields. Unmapped service action types stay visible as an `other` action with their type name. The new/edit/delete, inbox/outbox read and draft listeners forward the fields they know. A DM deletion can omit the chat ID. Rich media, reactions and other unimplemented message fields remain absent. The server may report a duplicate random ID without returning the original message after an interrupted send. The plugin requires a matching acknowledgement before reporting success. [Telegram sendMessage](https://core.telegram.org/method/messages.sendMessage) · [Plugin contract](../../docs/plugin-api.md)

## Remaining method groups

The frozen registry contains 121 methods. The table above covers 23 core-to-plugin methods. The 96 core-to-plugin methods below remain unimplemented. The final two, `secrets.set` and `secrets.delete`, are host services for plugin callers. [Method registry](../../schemas/methods/registry.json)

| Follow-up group | Pending methods |
| --- | --- |
| Message actions and attachments | `messages.send_file`, `messages.send_album`, `messages.seen_by`, `messages.translate`, `messages.transcribe` |
| Media and avatars | `media.download`, `media.search`, `avatars.download` |
| Reactions | `reactions.available`, `reactions.set`, `reactions.paid`, `reactions.paid_balance`, `reactions.who`, `reactions.effect`, `reactions.list_unread`, `reactions.read` |
| Polls | `polls.create`, `polls.vote`, `polls.close` |
| Bots | `bots.commands`, `bots.press_button` |
| Mentions and pins | `mentions.list_unread`, `mentions.read`, `pins.list` |
| Scheduling | `scheduled.list`, `scheduled.edit`, `scheduled.delete`, `scheduled.send_now` |
| Replies and forums | `replies.list`, `topics.list`, `topics.create`, `topics.edit`, `topics.pin` |
| Chat settings and folders | `chats.set_flags`, `chats.clear_history`, `chats.leave`, `chats.set_auto_delete`, `chats.reorder_pinned`, `folders.list`, `folders.create`, `folders.rename`, `folders.delete`, `folders.set_chats`, `folders.set_rules` |
| Groups and members | `group.create`, `group.join_by_link`, `group.invite_link`, `group.edit_title`, `group.edit_about`, `group.edit_photo`, `group.set_permissions`, `group.set_slow_mode`, `members.list`, `members.ban`, `members.set_admin`, `members.invite`, `join_requests.list`, `join_requests.answer` |
| Contacts, profile, privacy and sessions | `lookup.username`, `lookup.phone`, `contacts.set`, `contacts.info`, `contacts.common_chats`, `blocked.list`, `blocked.set`, `profile.get`, `profile.update`, `profile.photos`, `stories.list`, `privacy.get`, `privacy.set`, `two_factor.get`, `two_factor.set`, `sessions.list`, `sessions.terminate`, `ghost.apply`, `account.sign_out` |
| Stickers, GIFs and emoji | `stickers.recent`, `stickers.favorites`, `stickers.packs`, `stickers.pack`, `stickers.search`, `stickers.install`, `stickers.set_favorite`, `stickers.thumb`, `stickers.clip`, `stickers.send`, `gifs.list`, `gifs.search`, `gifs.thumb`, `gifs.clip`, `gifs.send`, `emoji.custom_files`, `emoji.packs`, `emoji.pack`, `emoji.animated` |

## Verification boundary

The tests inject a client factory, exercise the SDK through schema-checked stdio, and construct a real Telethon client with a fresh synthetic SQLite session offline. Fixtures isolate HOME, data and XDG paths and prohibit network connections. They cover account isolation, login and cancellation, reconnect, shutdown, sparse updates, peer and topic IDs, UTF-16, history boundaries, confirmation and redacted errors. Action regressions cover cross-chat mutation prevention, edit retries, forward correlation and flags, paged unpinning, activity scopes and draft clearing. The action additions have source/AST checks only until CI runs the dependency-backed suite. Live account sign-in, server-side delivery and proxy connectivity still need a separately authorized smoke test.

Requests and flags were checked against the pinned 1.46.0 package source and Telegram's primary API descriptions: [forwarding](https://core.telegram.org/method/messages.forwardMessages), [edits](https://core.telegram.org/method/messages.editMessage), [pins](https://core.telegram.org/method/messages.updatePinnedMessage), [thread receipts](https://core.telegram.org/method/messages.readDiscussion), [drafts](https://core.telegram.org/method/messages.saveDraft). Forwarding lacks a client id in the frozen contract; an interrupted or incomplete confirmation remains ambiguous and must not be presented as safely retryable.
