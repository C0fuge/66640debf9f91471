# Build "Contact Hub": a unified desktop messenger where each platform is a plugin

You are building a local-first desktop app that shows all of a person's messengers
(Telegram, WhatsApp, MAX, later Signal, Discord, Instagram…) in one window. It also
lets the user tag each contact by relationship (friend, coworker, acquaintance, …)
across platforms.

This is a rebuild from scratch of an existing app. The old app hard-wired Telegram
and MAX into the core. The new app must do the opposite: **the core knows nothing
about any messenger.** Every messenger is a **plugin** that talks to the core
through a documented, versioned **plugin API**. Adding WhatsApp must not require
touching core code.

**Location:** build the new project in `/workspace/project`, a fresh repository. The old app
in `/legacy-reference` is reference material only. Read it, port ideas, themes and
painters from it, but never modify it.

**The plugin API is public.** It is a product in its own right, not an internal
detail. **Anyone**, not just us, must be able to write a plugin for a new messenger
(Discord, Signal, Slack, Matrix, Instagram, …) using only the published Contact Hub
API docs and SDK, without reading or changing core code. Treat third-party plugin
authors as first-class users (see §3.7).

---

## 1. Product goal

> Every function you can use in the original messenger app, you should be able to use
> here, and then more.

- This is **not** a lowest-common-denominator inbox. Each plugin exposes the *maximum*
  feature set its platform allows, measured against that platform's own official client.
- **Ceilings differ, and that's fine.** If Telegram can do something WhatsApp can't,
  Telegram still gets it. A feature is never dropped for one platform just because
  another lacks it.
- **If the API supports it, build it.** Only a real dead end is a reason to leave
  something out. Gaps are declared honestly through capabilities (see §4). The UI
  hides a control, or greys it out with "{platform} doesn't offer this". Never fake a
  feature or leave a stub.
- On top of the platforms sit the hub's own features, which no single messenger has:
  - cross-platform relationship tags;
  - every account in one window;
  - ghost mode;
  - instant local search over everything;
  - kept deleted messages and edit history;
  - a local presence log;
  - an app passcode;
  - backups.

### Fixed design decisions (do not change)
- **Chats stay per platform.** One person on Telegram and on WhatsApp is still two
  chats. Do not build a merged cross-platform thread. The *person* (contact card,
  relationship tag) is shared; the conversations are not.
- **Message bubbles are single-column and left-aligned.** Incoming and outgoing
  messages are distinguished by colour, not by alignment.
- **The accent colour recolours only accent parts** (buttons, highlights, links,
  selection), never backgrounds or bubbles.
- **Parity scope:** match what the official clients do today. Don't chase every
  obscure feature, and don't invent platform features the platforms don't have.

---

## 2. Architecture

```
┌──────────────────────── Desktop app (UI) ────────────────────────┐
│ chat list · thread · composer · media viewer · settings · …      │
└───────────────▲──────────────────────────────────────────────────┘
                │ in-process calls / signals
┌───────────────┴──────────── Core ────────────────────────────────┐
│ Plugin host  │ Ingest & identity │ Local DB │ Search │ Services  │
│ (lifecycle,  │ (raw DTO → rows,  │ (SQLite) │ (FTS)  │ (queries, │
│  routing,    │  person linking)  │          │        │ mutations)│
│  capability  │                   │          │        │           │
│  registry)   │                   │          │        │           │
└───────▲──────┴───────────────────┴──────────┴────────┴───────────┘
        │  Plugin API: JSON-RPC 2.0, versioned
        │  (stdio or a local WebSocket; requests both ways + event stream)
 ┌──────┴─────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐
 │ telegram   │ │ whatsapp   │ │ max        │ │ …future    │
 │ plugin     │ │ plugin     │ │ plugin     │ │ plugins    │
 │ (process)  │ │ (process)  │ │ (process)  │ │            │
 └────────────┘ └────────────┘ └────────────┘ └────────────┘
```

### Key properties
- **Plugins run as separate processes.**
  - A plugin crash, hang or dependency conflict never takes down the app.
  - A plugin may be written in any language. Use the best library for the platform:
    Telethon or TDLib for Telegram, Baileys or whatsmeow for WhatsApp, pymax for
    MAX, and so on.
  - The core supervises plugins: it spawns them, health-checks them, restarts them
    with backoff, and kills them on shutdown.
- **The core is the single source of truth for the UI.**
  - Plugins push raw data into the core.
  - The core normalises it into a local SQLite database.
  - The UI reads only from the database and services, never from plugins directly.
  - The app is fully usable offline and with zero plugins connected.
- **Connectivity is runtime state, never a build assumption.** Each plugin account
  reports one of `connected`, `connecting`, `auth_required`, `unavailable`
  (unreachable, e.g. Telegram blocked without a VPN), `disconnected`, `error`, plus a
  `reconnecting` flag. Any subset of accounts may be live at once.
- **Multi-account:** one plugin can run several accounts (e.g. `telegram`,
  `telegram#2`). Each account has its own session, chats, status badge, and entry in
  the account filter.
- **Ids are opaque strings everywhere:** chat, message, user, folder and topic ids.
  Never assume they are integers.

### Suggested stack (a recommendation; the plugin API matters more)
- Core and UI: Python 3.12+ with PySide6 (Qt Widgets), qasync, SQLAlchemy 2, and
  SQLite with WAL and FTS5. This matches the old app, so its themes and painters can
  be ported. A Tauri or Electron UI over the same core is acceptable if justified.
- Plugin transport: JSON-RPC 2.0 over stdio (simplest), or a localhost WebSocket with
  a per-session token. Large binaries (media) are never sent inline. The plugin
  writes them to a core-provided cache directory and passes back the file path.
- Packaging: AppImage plus an install script. Plugins are discovered from a
  `plugins/` directory, each with its own runtime or virtualenv.

---

## 3. Plugin API (design this carefully; it is the heart of the project)

Write a formal spec (`docs/plugin-api.md`) plus a JSON Schema for every message type.
Also ship a reference plugin SDK (Python first) that hides the transport.

### 3.1 Manifest (`plugin.json`)
```json
{
  "id": "telegram",
  "name": "Telegram",
  "version": "1.0.0",
  "api_version": "1",
  "entry": ["./venv/bin/python", "-m", "tg_plugin"],
  "icon": "icon.svg",
  "brand_color": "#2AABEE",
  "multi_account": true,
  "settings_schema": { "...": "JSON Schema for api_id / api_hash / proxy …" },
  "auth_methods": ["phone_code", "password_2fa", "qr"]
}
```

### 3.2 Lifecycle
1. `initialize` → the plugin returns its API version and **capabilities** (§4).
2. `account.start` / `account.stop`, both per account.
3. `shutdown`.
- Heartbeat or ping. The core restarts a plugin that stops answering.
- Every core→plugin call has a timeout. Errors use typed codes: `NOT_SUPPORTED`,
  `AUTH_REQUIRED`, `FLOOD_WAIT{seconds}`, `NOT_FOUND`, `FORBIDDEN`, `NETWORK`,
  `INTERNAL`.

### 3.3 Interactive auth (driven by the plugin, rendered by the core)
- The plugin sends steps such as `{step: "phone"}`, `{step: "code", hint}`,
  `{step: "password", hint}`, `{step: "qr", data, expires_at}` and
  `{step: "email_code"}`.
- The core renders each step generically and answers with `auth.submit`.
- This way the core needs no per-platform sign-in UI.

### 3.4 Data model (DTOs a plugin emits)
- `Contact`: id, username, display name, phone, avatar path, is_bot, is_contact.
- `Conversation`:
  - id, title, kind (`dm|group|channel`), and peer user id for a DM;
  - archived, muted (with mute-until), pinned, folder ids;
  - unread count, unread mentions, unread reactions, read watermark;
  - is_self_chat (Saved Messages), avatar, member count;
  - can_post, is_forum, draft, colour index.
- `Message`:
  - id, chat id, direction, sender id, sender name and title, sent_at, text;
  - **entities** (bold, italic, underline, strike, code, pre, blockquote, spoiler,
    links, mentions, hashtags, custom emoji) as UTF-16 offsets;
  - media (type, mime, size, filename, thumb path, full path, duration, waveform,
    music title and performer, has_spoiler, ttl);
  - reply_to (+ quote text), topic id, forward_from (+ id), album/grouped id;
  - reactions (emoji, count, mine, paid) and reactors;
  - poll, location, contact card, dice;
  - link preview (site, title, description);
  - inline buttons, views, replies count, effect id;
  - service `action` (call, join, leave, rename, pin, …);
  - edited flag, delivery status, read_by.
- Also: `Folder` (with rule fields: include kinds, exclude muted/read/archived,
  pinned chats, shared), `Topic` (forums), `Profile`, `Session` (device),
  `Sticker`, `StickerPack`.

### 3.5 Events (plugin → core, streamed)
- Data: `status`, `conversations.upsert`, `conversations.synced` (a full id list, so
  chats you left disappear), `messages.upsert`, `messages.deleted`, `message.edited`,
  `reaction.changed`, `contacts.upsert`, `folders.synced`, `topics.upsert`.
- Read state: `read.outbox` (the peer read up to id), `read.inbox` (read on another
  device).
- Other: `presence` (online, last seen, typing, recording…, with an optional chat id
  for groups), `draft.changed`, `avatar.ready`, `media.ready`, `transfer.progress`
  (upload or download, cancellable).

### 3.6 Requests (core → plugin)

Group methods by namespace. Every method is optional except
`messages.fetch_recent`, `messages.send` and the lifecycle methods.

**Messages, history and media**
- History: `history.before`, `history.around(date)`, `history.fill_gap`.
- Sending: `messages.send` (with `reply_to`, `quote`, `silent`, `schedule_at`,
  `no_webpage`, `entities`), `messages.send_file`, `messages.send_album`.
- Changing: `messages.edit`, `messages.delete(revoke)`, `messages.forward(drop_author)`,
  `messages.pin`, `messages.unpin`.
- Media: `media.download` (cancellable), `media.search(kind)`.

**Reactions, polls, bots and read state**
- Reactions: `reactions.available`, `reactions.set`, `reactions.paid`,
  `reactions.who`, `reactions.effect`.
- Polls: `polls.create` (quiz, multiple, anonymous), `polls.vote`.
- Bots: `bots.commands`, `bots.press_button`.
- Read state: `read.send_receipt`, `typing.send`, `mentions.list_unread` / `read`,
  `reactions.list_unread` / `read`.
- Lists: `pins.list`, `seen_by`.
- Extras: `translate`, `transcribe`.

**Drafts, scheduling and threads**
- Drafts: `drafts.save`.
- Scheduled messages: `scheduled.list`, `scheduled.edit`, `scheduled.delete`,
  `scheduled.send_now`.
- Threads: `replies.list` (comments and threads).
- Forum topics: `topics.list`, `topics.create`, `topics.edit`, `topics.pin`.

**Chats, folders and groups**
- Chats: `chats.set_flags` (pin, mute, archive, unread), `chats.clear_history`,
  `chats.leave`, `chats.set_auto_delete`, `chats.reorder_pinned`.
- Folders: `folders.list`, `create`, `rename`, `delete`, `set_chats`, `set_rules`.
- Groups:
  - `group.create`, `group.join_by_link`, `group.invite_link`;
  - `group.edit_title`, `about`, `photo`, `permissions`, `slow_mode`.
- Members: `members.list` (+ search), `ban`, `set_admin`, `invite`;
  `join_requests.list` / `answer`.

**Contacts, profile and account**
- Contacts: `lookup.username`, `lookup.phone`, `contacts.set`, `blocked.list` / `set`,
  `common_chats`.
- Profile: `profile.get`, `profile.update`, `profile.photos`, `contact.info`,
  `stories.list`.
- Privacy and security: `privacy.get` / `set`, `two_factor.get` / `set`.
- Devices: `sessions.list`, `sessions.terminate`.
- `ghost.apply` (stay offline, no receipts, no typing) and `sign_out`.

**Stickers, GIFs and emoji**
- Stickers: `stickers.*` (list, favourites, packs, search, install or remove, send,
  thumb, clip).
- GIFs: `gifs.*`.
- Custom emoji: `emoji.custom_files`, `emoji.packs`, `emoji.animated`.

### 3.7 Third-party plugins: the API is for everyone

Anyone should be able to write a plugin, for example a Discord plugin, against the
Contact Hub API and have it work in the app with no core changes. To make that real:

- **Docs:**
  - A public, self-contained guide (`docs/plugin-api.md` plus the JSON Schemas).
  - A "Write your first plugin" tutorial that builds a minimal working plugin
    end to end.
- **SDKs:**
  - An official Python SDK.
  - The protocol is plain JSON-RPC, so plugins in any language (Node, Go, Rust, …)
    can implement it directly. Ship at least one non-Python example.
- **Templates:** `ch plugin new <name>` (or a cookiecutter) scaffolds a plugin with
  a manifest, an SDK skeleton and the conformance tests already wired in.
- **Example plugins** in the repo serve as references: the fake plugin, plus a
  small skeleton such as `examples/discord-plugin/` showing auth, sync and sending.
- **Conformance suite** published as a tool (`ch plugin test ./my-plugin`): a
  third-party author can check their plugin before they share it.
- **Install:**
  - Drop a folder into `~/.local/share/contact-hub/plugins/`, or use "Install
    plugin…" in Settings → Accounts with a folder or archive.
  - The app discovers it, reads the manifest, and shows it alongside the built-in
    plugins.
  - Built-in plugins use exactly the same mechanism, with no private shortcuts.
- **Stability:**
  - Semantic versioning of `api_version`.
  - Within a major version, changes are additive only: new optional methods,
    events and fields.
  - The core keeps supporting the previous major version for a deprecation
    window.
  - Unknown fields are ignored on both sides.
- **Generic UI:** everything a plugin needs to show (settings form, auth steps,
  brand colour and icon, capability-gated controls) is driven by data from the
  plugin, so a new platform gets a full UI for free.
- **Trust:**
  - Before enabling a third-party plugin, the app warns that the plugin will have
    access to that account's messages.
  - Plugins run as separate processes and never touch the DB or other plugins'
    data directly.

---

## 4. Capabilities

- In `initialize`, each plugin returns a set of capability keys. The UI gates
  **every** platform-specific control on them.
- Example keys:
  - Sending and messages: `send.schedule`, `send.silent`, `send.album`, `edit`,
    `edit.caption`, `delete`, `forward`, `forward.drop_author`, `pin`, `unpin`,
    `quote_reply`.
  - Reactions: `reactions.set`, `reactions.who`, `paid.react`.
  - Polls: `polls.create`, `polls.quiz`, `polls.vote`.
  - Stickers, GIFs and emoji: `stickers.send`, `stickers.packs`, `gifs.send`,
    `custom_emoji`.
  - Chats and folders: `topics`, `topics.manage`, `scheduled`, `folders.rules`,
    `chat.archive`, `chat.mark_unread`, `auto_delete`.
  - Groups: `members.admin`, `join_requests`, `group.permissions`, `group.slow_mode`.
  - Contacts and privacy: `blocked`, `privacy`, `two_factor`, `sessions`, `presence`,
    `typing`, `read_receipts`, `ghost_mode`, `lookup.username`, `lookup.phone`,
    `common_chats`, `stories`.
  - Extras: `translate`, `transcribe`, `bots.buttons`, `bots.commands`,
    `comments.send`, `history.jump_to_date`, `drafts.sync`, `media.search`.
- A capability may carry parameters, e.g.
  `{"polls.create": {"quiz": false}, "send.album": {"max": 10}}`.
- If the core calls a method whose capability is missing, the call fails with
  `NOT_SUPPORTED`. There is no silent no-op.
- A test suite (§9) checks that each plugin implements every capability it declares.

---

## 5. Core responsibilities

- **Ingest and identity.**
  - Turn DTOs into rows.
  - Upsert by `(account, platform id)`, so a re-sync is idempotent.
  - Resolve **Persons**: one Person owns many platform handles. Link handles
    automatically by phone or username where it is safe, and let the user link the
    rest manually ("Unlinked handles").
  - Relationship categories hang off the Person and survive when an account is
    disconnected or reconnected.
- **Local DB** (SQLite, WAL). Tables:
  - `person`, `category`, `person_category`, `platform_handle`;
  - `conversation`, `folder` (platform and local), `topic`;
  - `message`, `message_version` (edit history), `presence_log`;
  - `outbox` (local echo: sending, failed, retry), `settings`.
- Schema evolution must be automatic and idempotent: add missing columns with
  defaults, and run idempotent fix-ups on start.
- **Search:** FTS5 over every message from every account. Telegram-style filters:
  `from:`, `in:`, `media:photo|video|voice|file|link`, `before:`, `after:`. `@name`
  and phone numbers also do a live lookup through plugins that have `lookup.*`.
- **Kept deleted messages and edit history:** when a plugin reports a deletion or an
  edit, keep the old text (each is a setting).
- **Outbox:**
  - A sent message appears instantly as a local echo, then shows sending, then sent,
    then read, or failed with a Retry button.
  - Uploads show progress and can be cancelled.
- **Unread logic:** per chat and per topic. Mention and reaction counters, a forced
  "mark as unread", an unread divider, and reads made on other devices.
- **Notification policy:**
  - Global switches per chat kind, plus overrides per chat and per category.
  - Quiet hours (may wrap past midnight), mute-until, custom sounds.
  - Mentions in muted chats can still notify.
  - Nothing is notified while ghost or streamer mode or the lock screen is hiding it.
- **Ghost mode** (global): never online, no read receipts, no typing. The core
  enforces it, so a plugin bug can't leak a receipt.
- **Presence log:** a local record of each contact's online, offline, typing, read
  and reacted events.
- **Backup and restore** to one file: DB, settings, optionally media, sessions and
  secrets. Restore must be WAL-safe (checkpoint first, then dispose connections,
  then swap files) and must restart the app afterwards.
- **Secrets** (API hashes, session files) live in the OS keyring or an encrypted
  store, never in plain settings.
- **Media cache:** a size limit, auto-download limits per chat kind and media type,
  and a "clear cache" option per category.

---

## 6. UI: feature inventory to rebuild

Rebuild everything in this section, in the style of Telegram Desktop.

### Main window
- Three panes:
  1. sidebar: account filter, relationship-category filter, unlinked handles;
  2. chat list, with a folder chip bar and global search on top;
  3. thread.
- Header: Profile button (avatars of all accounts), Settings, and "+" (New group /
  Join by link, per account).
- Status bar: one connection badge per account. A click signs in or reconnects.
- A forum group swaps the chat list for its **topic list** (icons, unread, pin,
  rename, close).

### Chat list rows
- Avatar, or coloured initials when there is no photo; online dot; **platform badge**
  in the plugin's brand colour.
- Title, pin and mute marks, time.
- Badges: unread (grey when muted), @ mention, reaction, forced-unread dot.
- Second line: preview with a sender prefix, a "Draft:" prefix and a tiny media thumb.
- Live "typing…" with animated dots; sent/read ticks; relationship colour bar.
- Compact mode.
- Context menu:
  - Read/unread, pin, mute (durations), archive.
  - Notifications override, folder add, auto-delete.
  - Export chat (HTML/JSON), clear history, leave or delete.
  - Members, invite link, manage group.

### Folders
- Chip bar: All, platform folders (shown when their account is selected), local
  cross-platform folders, and Archive.
- Folder rules where the plugin supports them. The "All" tab can be hidden.

### Thread
- Header: avatar, title, presence ("last seen", "typing", "recording audio", …),
  ghost toggle, find, and a More menu (pinned messages, jump to date, create poll,
  background).
- Pinned bar ("Pinned message n of N"), find in chat, floating date chip, day
  separators, unread divider. A chat opens at the first unread message.
- Jump-to-latest button with a count; next-mention and next-reaction buttons.
- Read-only channel bar; drag-and-drop overlay; per-chat background.

### Message rendering
- Bubbles grouped into runs. Each shows time, ticks, the "edited" mark and an
  optional message id.
- Full rich text, including spoilers revealed by click.
- 1–3 emoji appear large with no bubble; they animate where supported.
- Media:
  - photo, video, autoplaying GIF (with a cap on how many play at once), album grids;
  - stickers: static, Lottie `.tgs`, and `.webm` with alpha (ffmpeg libvpx);
  - round videos, voice messages with a waveform, music, file cards;
  - location, contact, dice;
  - link previews.
- Download placeholders with a size pill, progress ring and cancel.
- Replies and quote replies (click to jump), "Forwarded from" (click opens the
  source), sender names in groups.
- Polls and quizzes; inline bot buttons; service messages; "deleted" markers.
- Reactions as pills (yours highlighted, paid ones marked) with a fly-in animation.
- Comment counters; a failed message shows Retry.
- Streamer mode masks text until hover.

### Message actions
- Hover reaction bar; double-click to reply or to react (a setting).
- Drag to multi-select, then copy, forward or delete (for everyone or only for me).
- Select text inside a bubble, then copy or quote it.
- Context menu:
  - reply, edit, quote, copy, save as, copy image, show in folder;
  - pin, forward, select;
  - who reacted, seen by, translate;
  - copy link / message id / chat id / sender id;
  - edit history, delete.
- Comments side panel; scheduled messages panel; link-safety confirmation; jump to
  date.

### Composer
- Auto-growing rich text box.
  - Format shortcuts: Ctrl+B / I / U, Ctrl+Shift+X strike, Ctrl+Shift+M mono,
    Ctrl+Shift+P spoiler, Ctrl+K link, Ctrl+Shift+N plain.
  - Inline markdown is converted as you type.
- Suggestions: `@mention` (with server search in large groups), `/command`,
  `:emoji:`.
- Custom emoji inline; a dismissible link-preview chip.
- Drafts saved per chat and per topic, synced where the plugin supports it.
- Reply and edit bars; Up edits your last message.
- Send key: Enter or Ctrl+Enter (a setting). The send button's menu has "Schedule…"
  and "Send without sound".
- Attachments:
  - file picker, drag and drop, paste images or files;
  - a send dialog with caption, "send as file", spoiler, and an album grouping of up
    to 10.
- Hold-to-record voice: slide left to cancel, up to lock, with a level meter. Round
  video messages are also available.

### Emoji, GIF and sticker panel
- A docked, resizable panel: recent and category emoji, custom emoji, GIFs, sticker
  packs (favourites, recent, search, add or remove).

### Media viewer
- Gallery over the chat's media: zoom (keys, Ctrl+wheel, pinch), rotate.
- Actions: save, copy, show in folder, forward, delete, show in chat, full screen.
- Video controls: speed, mute, J/L seek.
- Executables get a warning before they open externally.

### Audio
- One player per thread with an auto-advancing playlist.
- Now-playing bar: previous/next, seek, speed 1×/1.5×/2×, volume.

### Dialogs
- Forward to many chats (option: hide the sender).
- Poll creator.
- My profile (per account): name, bio.
- Contact profile:
  - photos, username, phone, bio, birthday, stories, groups in common;
  - add or remove from contacts, block;
  - mute and notification sound;
  - shared-media tabs (media, files, music, voice, links) with multi-select actions;
  - **Activity log**, from the local presence log.
- Group management: title, about, photo, permissions, slow mode.
- Members: roles, admin, remove, invite, join requests.
- Quick profile of a group sender.

### Notifications and tray
- Native desktop notifications (freedesktop D-Bus on Linux):
  - click to open, inline reply, mark read, avatar;
  - notifications for the same chat are merged.
- Reaction notifications; sound; taskbar flash.
- Tray icon with an unread badge (muted chats optionally counted).
- Close to tray, start minimised.

### App lock
- Passcode screen. Sync keeps running underneath.
- Locks at start, on Ctrl+L, and after idle (1 / 5 / 15 min, 1 / 5 h).

### Themes
- Auto, light or dark mode. Light themes: Day, Day Blue, Sakura, Sunrise, Lavender.
  Dark themes: Night, Night Green, Midnight Violet, Deep Forest, Ocean, Rosé Night,
  AMOLED.
- Theme tint (HSB); 8 accent colours or a custom one.
- Bubble colours for your and their messages, corner radius, colourful replies,
  avatar shape from circle to square.
- Save, rename and delete your own themes; live preview.
- Chat backgrounds: built-in procedural ones, or your own picture with blur and dim.
  One global background plus per-chat overrides.
- Animation length setting (0 turns animations off).

### i18n
- System, English or Russian, with full plural rules. Qt's own dialogs are
  translated too.

### Settings window
- Searchable sidebar. Changes preview live, Cancel undoes all of them, Save applies.
  A dot marks unsaved changes on each section.
- Sections:
  - **Accounts**: the plugins list, enable or disable, add or remove accounts, the
    settings form generated from each plugin's `settings_schema`, sign in and out.
  - **Notifications**.
  - **Privacy**: ghost, streamer, keep deleted, edit history, presence tracking,
    passcode, per-account 2FA, platform privacy rules, blocked users.
  - **Chat**: send key, double-click action, bubble width, text size, 12/24 h clock,
    scroll speed, inertia, large emoji, GIF autoplay, compact list.
  - **Colors**, **Folders**, **Categories**.
  - **Data and storage**: usage, limits, auto-download, backup and restore.
  - **Devices**: sessions per account.
  - **Advanced**: tray, autostart, per-plugin proxy, log level and folder, wipe data.

### Other
- First-run walkthrough. Every step can be skipped. It has a step for picking and
  connecting plugins.
- Generic auth dialog driven by plugin auth steps (phone, code, 2FA, QR, email code).
- Single instance: a second launch raises the running window, including on Wayland.
- Smooth scrolling with touchpad inertia.

### Keyboard shortcuts (same as Telegram Desktop)

| Keys | Action |
| --- | --- |
| Ctrl+Tab / Ctrl+Shift+Tab, Ctrl+PgDn / Ctrl+PgUp | Next / previous chat |
| Alt+↓ / Alt+↑ | Next / previous unread chat |
| Ctrl+1…9, Ctrl+0 | Folder chips (Ctrl+0: All) |
| Ctrl+K | Search |
| Ctrl+F | Find in chat |
| Ctrl+W | Close chat |
| Ctrl+, | Settings |
| Ctrl+L | Lock |
| Ctrl+Shift+G | Ghost mode |
| Ctrl+Shift+S | Streamer mode |
| Ctrl+Q | Quit |
| Esc | Steps back one layer at a time |
| Delete | Delete the selection |

---

## 7. Plugins to ship

| Plugin | Library | Notes |
| --- | --- | --- |
| **telegram** | Telethon (or TDLib) | Personal account over MTProto, not the Bot API. Needs `api_id` / `api_hash`. Highest capability ceiling: forums, scheduled messages, stickers, custom emoji, paid reactions, folder rules, privacy, sessions, translate. Often reachable only through a VPN, so it must report `unavailable` cleanly. |
| **whatsapp** | whatsmeow (Go) or Baileys (Node) | Linked-device multi-device protocol, signed in by QR or pairing code. Covers chats, groups, media, voice, reactions, replies, edits, deletes, read receipts, presence, polls, communities where possible. Unofficial: document the ToS risk. |
| **max** | pymax (`maxapi-python`) | Personal account, signed in by QR or SMS. There is no official API. No stickers, GIFs, topics, blocking or presence; captions can't be edited; polls can't be quizzes. |

Only real API gaps may be left out, each one declared through capabilities. Every
plugin has a README with its capability table and its known gaps.

---

## 8. Non-functional requirements

- **Local-first:** all features that need no network keep working with every plugin
  offline.
- **Performance:**
  - A 100k-message chat scrolls smoothly: virtualised list, painted delegates,
    lazy media, a cap on concurrent GIF and video players.
  - Ingest runs in batches inside one transaction.
  - The UI never blocks on plugin I/O.
- **Robustness:**
  - A plugin crash produces an `error` badge and an auto-restart, never an app crash.
  - Backpressure on event floods.
  - Flood-wait handling.
- **Security:**
  - Plugins get no direct access to the database. Secrets live in the keyring.
  - The WebSocket transport is bound to localhost with a token.
  - Links whose text hides their real address need confirmation.
- **Packaging:** Linux first (AppImage, `.desktop`, autostart). Keep the core
  OS-agnostic.

## 9. Testing

- Core unit tests:
  - ingest idempotency, identity linking, unread math, search filters;
  - notification policy, outbox state machine, backup and restore (including WAL).
- **Plugin conformance suite:** the same test suite runs against every plugin through
  a recorded or fake transport. It checks the handshake, schemas, that declared
  capabilities are implemented, the error codes, and that sync is idempotent.
- A **fake plugin** (`plugins/fake/`) that produces deterministic chats, messages,
  media and presence. UI and e2e tests run against it without real accounts.
- GUI tests run offscreen (`QT_QPA_PLATFORM=offscreen`). Isolate the DB, settings
  and XDG directories per test, and never touch the user's real data.

## 10. Delivery order

1. Scaffold `/workspace/project`, then write the plugin API spec, JSON Schemas, the Python
   plugin SDK, the fake plugin, the plugin template and the "write your first
   plugin" tutorial.
2. Core: DB, ingest, plugin host, services, search.
3. UI skeleton: three panes, chat list, thread, composer (text). Runs fully on the
   fake plugin.
4. Telegram plugin to full parity, plus the matching UI (media, reactions, stickers,
   folders, topics, …).
5. WhatsApp plugin.
6. MAX plugin.
7. Polish: themes, settings, notifications, lock, backup, packaging, i18n.

At every step the app must run, with the conformance suite and the unit tests green.

## 11. Working with subagents

Build this with subagents. The main agent coordinates and does not write feature
code itself.

### Roles
- **Main agent (orchestrator):**
  - plans each delivery step and splits it into tasks;
  - hands each task to a subagent with a self-contained brief (goal, files it
    owns, the contracts it must honour, how to verify it);
  - reviews and merges the results;
  - keeps the build green between steps.
- **Subagents:** one task each. Each works in its own git worktree or branch, runs
  the tests that cover its work, and reports back with a short summary of what
  changed and what it verified. It does not report file dumps.

### Model routing
- **Fable (`claude-fable-5-1`) for critical tasks**, where a mistake is expensive
  or spreads to everything built on top of it:
  - the plugin API spec, the JSON Schemas and the versioning rules (§3);
  - the DB schema, migrations, and ingest idempotency;
  - the plugin host: process lifecycle, transport, handshake, crash and restart,
    backpressure;
  - the outbox state machine, and backup and restore (including WAL);
  - security: keyring secrets, the localhost token, plugin isolation;
  - the plugin conformance suite;
  - reviewing each merge of parallel work, and integration across steps;
  - any bug that one Opus attempt failed to fix.
- **Opus (`claude-opus-5-5`) for everything else:** UI widgets and painters,
  dialogs, settings pages, themes, i18n strings, the fake plugin's data, platform
  features inside a plugin once its contract is fixed, tests for already-specified
  behaviour, docs and the tutorial, packaging.
- If you're unsure whether a task is critical, ask whether a bug in it would force
  changes in other modules or lose user data. If yes, it's critical.

### Parallelism per delivery step (§10)
1. **Spec and SDK:** Fable writes the API spec and schemas first, alone. Once
   they're frozen, Opus subagents build the SDK, the fake plugin, the template and
   the tutorial in parallel.
2. **Core:** Fable writes the DB schema, ingest and plugin host. Opus subagents
   build services and search in parallel against those interfaces.
3. **UI skeleton:** Opus subagents in parallel, one per pane (chat list, thread,
   composer), against the fake plugin. Fable reviews the merge.
4. **Telegram:** one Opus subagent per feature area (media, reactions, stickers,
   folders, topics, …), with each UI change paired with its plugin feature. Fable
   handles auth and the sync and update loop.
5. **WhatsApp and 6. MAX:** these can run in parallel with each other once
   step 4's contract changes are merged. Fable sets up each plugin's transport and
   auth; Opus fills in the features.
7. **Polish:** Opus subagents in parallel, one per area. Fable handles backup and
   restore and the app lock.

### Rules
- Freeze a contract (spec, schema, interface) before you fan out work that
  depends on it. A subagent that needs a contract change stops and reports back.
  It does not change the contract itself.
- Two parallel subagents never own the same file.
- Merge one branch at a time, and run the full unit and conformance suites after
  each merge.
- Write down every architecture decision, and pass the relevant ones into the
  briefs of later subagents.
