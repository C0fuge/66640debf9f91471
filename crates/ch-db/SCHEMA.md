# ch-db schema

SQLite, WAL, `foreign_keys=ON`, busy timeout 5 s, `STRICT` tables. Migrations are numbered SQL
files in `migrations/` (all `IF NOT EXISTS`, so re-running one is harmless), applied on open in
their own transactions and stamped into `PRAGMA user_version` (current: 4). A file newer than the
build is refused (`SchemaTooNew`). `migrations::fixups` runs on every open for idempotent
add-missing-column repairs.

`Db::transaction` uses `BEGIN IMMEDIATE` for service writes. It waits for the WAL writer
before reading, so another service cannot commit between a validation read and its update.
Read-only search snapshots use a deferred transaction on the connection.

Conventions: timestamps are `INTEGER` unix **milliseconds** UTC (`Millis`; `ch_protocol::Timestamp`
round-trips losslessly). Booleans are `0/1`; a nullable boolean column is a tri-state where `NULL`
= unknown/not reported. Nested DTOs are stored as JSON text in the wire format (`*_json`). Platform
ids are opaque `TEXT`, compared byte for byte, never parsed.

## Tables

| table | key | what |
| --- | --- | --- |
| `plugin` | `plugin_id` | Manifest facts, `enabled`, `installed` (entry present). |
| `account` | `account_id` | `plugin_id`, settings object, last `status` (state, detail, reconnecting), `me_user_id`, display order. |
| `person` | `id` | A human across platforms (card + categories). Never merges chats. |
| `category` | `id`, `name` unique | Relationship tag: colour, order. |
| `person_category` | `(person_id, category_id)` | Tags on a person. |
| `platform_handle` | `(account_id, user_id)` | One `Contact` on one account plus identity: `person_id`, `link_source` (`auto_phone`/`auto_username`/`manual`/`created`), latest `OnlineStatus`. |
| `settings` | `key` | Core/UI settings as JSON values. |
| `conversation` | `(account_id, chat_id)` | `Conversation` columns + local state: `is_gone` (left/not in last `conversations.synced`), `notify_override`, `notify_sound`, `wallpaper`, `local_read_at`, `last_message_*` preview. |
| `folder` | `(account_id, folder_id)` | Platform folders; `account_id = ''` is a local hub folder. `rules_json` wholesale. |
| `folder_chat` | `(folder_account_id, folder_id, account_id, chat_id, source)` | Membership by platform ids (survives any event order). `source` = `folder` (`Folder.chat_ids`), `chat` (`Conversation.folder_ids`) or `local` (hand-edited); each event replaces only its own source. `folder_account_id = ""` for local folders. Membership = union (`folder_members`). |
| `topic` | `(conversation_id, topic_id)` | `Topic` columns + `is_gone` (after a complete `topics.upsert`), `read_outbox_max_id`. |
| `message` | `(conversation_id, message_id)` | `Message` scalars as columns, nested objects as JSON; derived `media_kind`, `reply_to_id`, `has_link`; tombstone `deleted_at`/`deleted_by`. |
| `message_version` | `id` → `message` | Content (text, entities, media) before an edit replaced it. |
| `message_reaction` | `(message_id, key)` | Current reactions, replaced wholesale; `key` is `e:<emoji>` / `c:<id>` / `paid`. Reactors stay in `message.reactors_json`. |
| `draft` | `(conversation_id, topic_id)` | `topic_id = ''` for the chat; empty text = row deleted. |
| `member` | `(conversation_id, user_id)` | Group members (from `members.list`). |
| `message_fts` | rowid = `message.id` | FTS5 external-content index over `text` (`unicode61 remove_diacritics 2`, prefix 2/3), maintained by triggers. |
| `outbox` | `id`, `client_id` unique | Core-initiated sends/edits: method + params, `state` (`queued`→`sending`→`sent`/`failed`/`cancelled`), attempts, backoff, echo message, result ids, progress. |
| `history_state` | `(conversation_id, topic_id)` | Oldest fetched page cursor, `reached_start`, oldest/newest known message. |
| `history_gap` | `id` | A hole `(after_message_id, before_message_id?)` to close with `history.fill_gap`. |
| `media_file` | `path` | Every cache file the core knows (kind, bytes, owning message, LRU). |
| `presence_log` | `id` | Local log of a contact's online/offline/typing/read/reacted events (a setting enables it). |

## Invariants

- One row per `(account_id, chat_id)`, `(conversation_id, message_id)`, `(conversation_id, topic_id)`,
  `(account_id, user_id)`: re-sync is idempotent by key.
- `message.topic_id` is a text reference, not a FK: messages may name topics the core has no row
  for yet (legacy data has 996 such rows).
- Deleting an `account` cascades to handles, conversations, topics, messages, versions, reactions,
  drafts, members, history, outbox and presence log; folders and `media_file` rows are removed
  explicitly by `delete_account`. Persons and categories survive account removal.
- Deleting a `person` sets its handles' `person_id` to `NULL` (they become "unlinked").
- `message.deleted_at` set = tombstone (kept-deleted). Tombstones stay in FTS and are filtered out
  by `deleted_at IS NULL` unless the caller asks for them.
- Local outbox echoes are message rows with `message_id = 'local:<client_id>'` and
  `delivery = 'sending'`; a successful send re-keys the row to the platform id.
- On-create defaults (`archived`, `unread_*`, `is_self_chat`, `edited`, `is_scheduled`, topic
  flags) are `NOT NULL DEFAULT` columns; every other DTO field is nullable (absent = unknown).
- `folder.account_id = ''` (not `NULL`) so the unique index covers local folders.

## Legacy import (later)

The old app's `person`, `category`, `platform_handle`, `conversation` (incl. `is_gone`,
`notify_*`, `wallpaper`, per-topic `draft_json`), `message` (incl. `is_deleted` → tombstone),
`message_version`, `presence_log` and `.env`/QSettings settings all have a direct home above.
Media and avatar paths go to `media_file` + the row's path columns after copying.
