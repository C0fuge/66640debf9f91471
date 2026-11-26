# Telegram plugin

The Contact Hub Python SDK runs one process for Telegram. Each started account owns its Telethon client, its event handlers, its login flow and a SQLite session. The plugin uses API 1.1.0 and Telethon 1.46.0. The dependency lock pins the client, proxy support and transitive dependencies. [Telethon release](https://pypi.org/project/Telethon/1.46.0/) · [Upstream MIT license](TELETHON-LICENSE)

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
| `chat.get` | `chats.get` | Reads one ordinary dialog and returns its ID, kind, pin, manual unread mark and archive state. The caller receives the observation in the response. |
| `chat.flags`, `chat.pin`, `chat.mark_unread`, `chat.archive` | `chats.set_flags` | Pins, manual unread marks and archive moves on ordinary dialogs. A single-dialog observation confirms the requested flags. A partial or interrupted write can require a refresh. |
| `pin`, `unpin` | `messages.pin`, `messages.unpin` | Silent and personal private-chat pins, one-message or all-message unpinning, including paged unpin acknowledgements. |
| `read_receipts`, `typing`, `drafts.sync` | `read.send_receipt`, `typing.send`, `drafts.save` | Explicit chat/topic receipts and supported typing actions; formatted chat drafts and explicit clearing. Topic draft writes are rejected until their clearing semantics are verified. Server timestamps are authoritative. |
| `media.download`, `avatars` | `media.download`, `avatars.download` | MTProto photo/document full files and static thumbnails, plus small/big user/chat avatars. Private atomic cache writes, cancellation cleanup, per-account download queue (larger priorities first), 2 GiB attachment and 16 MiB avatar limits. External web-document URL downloads are rejected. |
| `send.file`, `send.as_file`, `send.spoiler`, `send.voice`, `send.ttl` | `messages.send_file` | Validated local regular files up to 2 GiB, JPEG/PNG photos, and voice notes with supplied duration. Literal captions with explicit UTF-16 entities, MIME/name overrides for documents, replies/topics, silent delivery, spoilers and TTL fields. Automatic mode selects JPEG/PNG photos and sends other files as documents. Video/video-note uploads and scheduling are rejected. |
| `send.album` | `messages.send_album` | Two to ten photos or documents, with per-item captions/entities/spoilers/file flags, optional first-item caption, replies/topics and silent delivery. Whole-batch validation precedes upload. Mixed photo/document albums, voice/video items, conflicting first-item captions/entities and scheduling are rejected. |
| `polls.create`, `polls.vote`, `polls.close` | `polls.create`, `polls.vote`, `polls.close` | Two-to-ten-answer plain/multiple-choice polls and quizzes, anonymity, correct-answer index, literal explanations with UTF-16 entities, relative closing period, replies/topics and silent creation. Vote/retract using returned opaque answer IDs; confirm the actual selection and closed state. Scheduling/quote replies remain unsupported. Closing polls with attached poll/solution media is explicitly rejected. |
| `reactions.set`, `reactions.custom`, `reactions.who` | `reactions.available`, `reactions.set`, `reactions.who` | Account limits and explicit chat reaction policy; full ordinary emoji/custom-emoji selection, clear and big reactions; scoped reactor pages with actual server continuation; full personal snapshots and addressed live updates. |
| `reactions.unread` | `reactions.list_unread`, `reactions.read` | One bounded oldest-first unread-message ID batch; acknowledged mark-as-read for a whole chat or a verified group/bot forum topic. Confirmed server continuations finish before success; errors/cancellation are never replayed. |
| `stickers` | `stickers.recent`, `stickers.packs`, `stickers.pack`, `stickers.thumb` | Fresh ordinary-sticker catalog, server-order installed packs, complete verified pack documents and explicit static thumbnail reads. Account/runtime-scoped opaque references; no search, sending, management, emoji/mask catalogs or animated playback. |

Entity ranges use UTF-16 units. The handshake lists each supported outbound entity type. Incoming named mentions carry the Telegram user ID. Message and draft events preserve Telegram’s marked peer IDs and forum topic IDs where supplied. Forward attribution, quote entities/offsets, service actions, pin/silent flags, reply counts, TTL and bot/effect IDs retain their known fields. Incoming photos/documents retain kind, dimensions, size, filename, MIME, duration, packed voice waveform, audio title/performer, spoiler and sticker metadata without probing files. Poll snapshots preserve questions, answers, known counts/selection/correctness, quiz explanations and server-reported close dates. Unmapped service action types stay visible as an `other` action with their type name. The new/edit/delete, inbox/outbox read and draft listeners forward the fields they know. A DM deletion can omit the chat ID. Other unimplemented message fields remain absent. The server may report a duplicate random ID without returning the original message after an interrupted send. The plugin requires a matching acknowledgement before reporting success. [Telegram sendMessage](https://core.telegram.org/method/messages.sendMessage) · [Plugin contract](../../docs/plugin-api.md)

Poll answer IDs are scoped to the poll and preserve opaque Telegram bytes through bounded base64url/digest identifiers; votes resolve them against the current target's actual answers. Minimal/common results never invent a personal choice, missing vote count or unknown correct answer. Addressed live updates and previously observed poll targets are refetched within that account, with a bounded 2,048-target in-memory index. Addressless channel broadcasts for unobserved/evicted targets cannot be assigned a chat and are not fabricated; a later history/get refresh obtains them. The index is not persisted across account restarts. Question/answer rich entities, poll attachments, detailed voter identities and newer eligibility fields have no matching fields in the frozen Poll DTO; its question/answer strings stay literal, and Telegram enforces voting restrictions. These are limits of the current view, not a claim of complete upstream poll parity. Poll creation has no client-id token in this contract: lost acknowledgement remains ambiguous and is never automatically replayed.

Omitted answer rows remain unknown for personal choice and correctness, even in non-minimal results; a reported non-minimal total of zero is evidence that nobody has chosen an answer. This follows the official Desktop client's [poll-result application](https://github.com/telegramdesktop/tdesktop/blob/dev/Telegram/SourceFiles/data/data_poll.cpp#L178-L213) and [listed-answer handling](https://github.com/telegramdesktop/tdesktop/blob/dev/Telegram/SourceFiles/data/data_poll.cpp#L282-L307), rather than treating an absent row as a negative flag. A contradictory explicit topic in a scoped poll-result response is rejected before any snapshot is combined or published.

`messages.get` attempts one scoped personal-results hydration when the fetched poll is minimal or lacks result rows, using its already-fetched message envelope. This gives an application refresh a way to obtain personal choices without another message fetch. Missing rows remain unknown even after hydration; messages without polls add no poll RPC.

Ordinary reactions use the locked [sendReaction](https://core.telegram.org/method/messages.sendReaction) and [full reaction read](https://core.telegram.org/method/messages.getMessagesReactions) requests. `reactions.available` reports active non-premium/premium emoji for the current account, explicit group/channel `All`/`Some`/`None` policies, and the account's reported `reactions_user_max_default` or `reactions_user_max_premium` limit. `Some` may list particular custom keys without allowing arbitrary custom emoji. Chat `reactions_limit` counts unique reactions from everybody and is never substituted for this account's selection limit. The advertised 100-selection ceiling is a plugin resource bound; Telegram enforces the account's actual allowance and chat restrictions. An absent full-chat policy is reported as unknown via an error rather than guessed to mean unrestricted or disabled. [Reaction policy and limits](https://core.telegram.org/api/reactions)

`reactions.set` replaces the ordinary selection in the supplied order, including an explicit empty-vector clear and the `big` flag. Emoji strings retain variation selectors and custom document IDs remain decimal strings. A write requires a matching full personal acknowledgement or one scoped read that confirms the requested set; a timeout or incomplete/mismatched confirmation remains uncertain and is never replayed. Paid keys are rejected before dispatch, and this method never sends Stars. Paid incoming counts and our own known count can be preserved when the full leaderboard is supplied; missing personal paid metadata leaves the replacement reaction array unreported.

Minimal/common [reaction snapshots](https://core.telegram.org/constructor/messageReactions) have no personal choices, while the frozen `Reaction.mine` is required. Those arrays remain absent rather than inventing false or clearing stored reactions; a complete empty array explicitly clears. `messages.get` attempts one scoped hydration for a minimal reaction view, independently of its existing poll hydration. Addressed live reaction updates validate the actual account/chat/message and any explicit forum topic before publishing; minimal events get a scoped full read. Saved-peer/monoforum scopes are not folded into ordinary forum topics. Recent reactor metadata is newest first and excludes all reactions marked as ours, including channel send-as identities.

`reactions.who` preserves marked peer IDs, available names, dates and unread flags from [server reactor pages](https://core.telegram.org/method/messages.getMessageReactionsList). Its opaque cursor binds the server offset to account, chat, message and exact reaction filter. Only absence of Telegram's `next_offset` establishes completion: short or empty pages with a continuation remain pageable. Invalid/nonadvancing/oversized offsets fail explicitly. Paid writes/balances and animation effect downloads have no advertised capability.

Unread listing follows the official Desktop's [initial oldest-batch request](https://github.com/telegramdesktop/tdesktop/blob/dev/Telegram/SourceFiles/api/api_unread_things.cpp#L141-L174): `offset_id=1`, `add_offset=-limit`, with the supplied forum `top_msg_id`. Returned IDs are validated against the actual chat/topic and ordered oldest first. The frozen `reactions.list_unread` method has no cursor or exhaustion field: one bounded response is returned, including a short/empty server slice without inferring global completion or scanning further. Invalid/foreign/duplicate/unavailable rows fail explicitly instead of becoming a partial success. [Unread-reaction request](https://core.telegram.org/method/messages.getUnreadReactions)

Scoped requests verify a full matching peer using the locked `channel.forum` or `user.bot_forum_view` flags before dispatch. Basic groups and ordinary private chats cannot receive topic operations. Verified forums' General topic has id `1` and may lack a message topic header; group and bot topics keep their exact IDs. Monoforum peer identities are not silently treated as ordinary topic IDs. [Telegram forum identity and General topic semantics](https://core.telegram.org/api/forum)

`reactions.read` sends only [readReactions](https://core.telegram.org/method/messages.readReactions), without also marking messages or mentions read. Success requires a real `messages.AffectedHistory` with zero remaining offset. A positive [affected-history offset](https://core.telegram.org/constructor/messages.affectedHistory) triggers another same-scope call only after that confirmed continuation, with a hard budget of **128 total read requests including the initial request**. A still-positive offset at that budget, nondecreasing/invalid acknowledgement, timeout, cancellation or other failure after dispatch becomes `NETWORK` with `outcome: "unknown"` and `retry_safe: false`; no completion is claimed and no write is automatically replayed. Read-only peer lookup and parameter validation before dispatch retain their original error classification. Cancellation releases the account's reaction lock, and no local reaction array, personal choice, or unread counter is reset from a guessed outcome. The same locked 1.46.0/layer-233 artifact cited below supplies both request constructors.

## Dialog archive

Archive moves use one `folders.editPeerFolders` request for the exact marked peer,
with folder `1` to archive and `0` to move to the inbox. These API peer folders
are separate from configurable dialog filters. An ordinary raw dialog's absent
or zero folder means inbox; `1` means archive. Unsupported folder IDs fail
explicitly instead of being guessed. This corrects Telethon 1.46.0's convenience
`archived` property, which treats any non-null folder, including explicit zero,
as archived. The exact locked request and response constructors are retained in
the audited 1.46.0/layer-233 source.
[Peer folders](https://core.telegram.org/api/folders#peer-folders) ·
[Folder mutation](https://core.telegram.org/method/folders.editPeerFolders) ·
[Raw dialog folder](https://core.telegram.org/constructor/dialog)

The setter shares the account sync lock, validates the exact dialog before a
write, and reads it again after every accepted `Updates` variant, including an
empty or `UpdatesTooLong` response. Archive moves may change pin state, so the
sparse acknowledged upsert includes the observed pin alongside archive state.
A mixed pin/unread/archive request is sequential; an earlier effect is never
rolled back or replayed after failure. Lost or malformed mutation replies and
lost core acknowledgements remain uncertain. Only an exact read establishes the
displayed state; the requested values never become local confirmations.

`chats.sync(include_archived=false)` keeps its complete inventory and emits
sparse archive/pin observations for archived rows while skipping their rich
metadata and message previews. This allows previously cached inbox rows to
converge after another client archives them. Bare `updateFolderPeers` updates do
not establish the full peer kind or pin state and do not trigger network read
fanout. Reconcile them through an explicit `chats.get` or `chats.sync`.

## Read-only sticker catalog

The base `stickers` capability implements all four frozen handlers. Each catalog
request sends hash zero and requires a full typed response. An unexpected
`NotModified`, missing/duplicate/foreign document, count mismatch or reference
budget failure is an error, never a fabricated empty/full catalog. Recent results
preserve server order and honor the supplied 1–200 limit (default 200); this method
has no completion field. Installed pack order is returned unchanged. Full packs
must match the requested numeric set/access-hash identity and contain exactly the
declared document count. Explicit document/emoji-group pack contradictions fail.
[Recent stickers](https://core.telegram.org/method/messages.getRecentStickers) ·
[Installed packs](https://core.telegram.org/method/messages.getAllStickers) ·
[Pack contents](https://core.telegram.org/method/messages.getStickerSet)

Public IDs are opaque receipts scoped to this account runtime. A maximum 4096
immutable references totaling 16 MiB are retained with LRU eviction; each reference
is capped at 2 MiB. Whole responses are staged and checked against a 192 KiB wire
budget before references are committed, so a successful response cannot evict its
own entries. At most 16 catalog operations may be active/waiting per account.
Evicted/foreign/stopped-runtime IDs return not-found. An evicted ID is never reused
when the same object is fetched again. Reference issuance is bounded by a 64-bit
counter; overflow requires an explicit account restart. There is no disk reference
map, arbitrary TL deserialization from public IDs or renderer path/URL resolution.

`pack_id` can be followed when an actual stable input-set ID/access hash was
observed. A recent short-name/special reference leaves its numeric pack identity
unknown. These methods accept their issued opaque IDs; the older numeric
`Media.sticker.pack_id` projection is unchanged and is not treated as an access
hash substitute. No message-pack-link UI integration is claimed by this slice.
Normal sticker documents are supported; custom-emoji and mask pack families stay
unadvertised. Known canonical MIME types map to static WebP, animated TGS and video
WebM. Unknown required formats fail explicitly rather than becoming static.
Missing dimensions/emoji stay absent; premium is derived from the documented
`videoSize.type=f` marker. No animation/effect is fetched during projection.
[Canonical formats and premium markers](https://core.telegram.org/api/stickers)

Thumbnail reads resolve only an issued immutable document reference. They select
explicit static PhotoSize/Cached/Stripped/Progressive previews; vector outlines,
video/effect thumbs and the main sticker file are never fallback sources. When no
supported static preview is available, the optional path is absent. Network reads share the
account download queue and use exact InputDocumentFileLocation scope, with a 2 MiB
byte cap and exact known-size check. Cached/stripped previews use the same cap.
Pinned Telethon's public `download_file` returns None on successful custom-stream
writes; actual bytes establish completion, not that return value.
[Preview constructors](https://core.telegram.org/api/files#handling-audio-video-and-vector-previews)

Only JPEG/static PNG/static WebP containers are published. Container signatures,
PNG chunk bounds/CRCs and PNG/WebP animation flags are checked; image decoding is
not performed. The bounded download finishes in memory before the existing held
directory/atomic cache writer publishes a private 0600 file. The returned inode is
checked against its held descriptor. Cancellation/error before publication leaves
no partial cache file. Paths stay inside this account's cache; no external URL
download, SVG/TGS/WebM execution or renderer asset permissions are added.

Constructors and custom-stream behavior were inspected in the locked
Telethon 1.46.0/layer 233 artifact cited below. Synthetic tests exercise real TL
serialization, model projection, stale/foreign/evicted receipts, whole-response
budgets, malformed/NotModified data, bounded waiters, static thumbnails,
cancellation and schema-checked stdio. These new tests await remote CI; no live
Telegram catalog/download or actual sticker rendering has been exercised.

## Remaining method groups

The registry contains 122 methods. The table above covers 41 core-to-plugin methods. The 79 core-to-plugin methods below remain unimplemented. The final two, `secrets.set` and `secrets.delete`, are host services for plugin callers. Archive extends an existing setter rather than adding another method. [Method registry](../../schemas/methods/registry.json)

| Follow-up group | Pending methods |
| --- | --- |
| Message extras | `messages.seen_by`, `messages.translate`, `messages.transcribe` |
| Media search | `media.search` |
| Reactions | `reactions.paid`, `reactions.paid_balance`, `reactions.effect` |
| Bots | `bots.commands`, `bots.press_button` |
| Mentions and pins | `mentions.list_unread`, `mentions.read`, `pins.list` |
| Scheduling | `scheduled.list`, `scheduled.edit`, `scheduled.delete`, `scheduled.send_now` |
| Replies and forums | `replies.list`, `topics.list`, `topics.create`, `topics.edit`, `topics.pin` |
| Chat settings and folders | Mute fields of `chats.set_flags`, `chats.clear_history`, `chats.leave`, `chats.set_auto_delete`, `chats.reorder_pinned`, `folders.list`, `folders.create`, `folders.rename`, `folders.delete`, `folders.set_chats`, `folders.set_rules` |
| Groups and members | `group.create`, `group.join_by_link`, `group.invite_link`, `group.edit_title`, `group.edit_about`, `group.edit_photo`, `group.set_permissions`, `group.set_slow_mode`, `members.list`, `members.ban`, `members.set_admin`, `members.invite`, `join_requests.list`, `join_requests.answer` |
| Contacts, profile, privacy and sessions | `lookup.username`, `lookup.phone`, `contacts.set`, `contacts.info`, `contacts.common_chats`, `blocked.list`, `blocked.set`, `profile.get`, `profile.update`, `profile.photos`, `stories.list`, `privacy.get`, `privacy.set`, `two_factor.get`, `two_factor.set`, `sessions.list`, `sessions.terminate`, `ghost.apply`, `account.sign_out` |
| Stickers, GIFs and emoji | `stickers.favorites`, `stickers.search`, `stickers.install`, `stickers.set_favorite`, `stickers.clip`, `stickers.send`, `gifs.list`, `gifs.search`, `gifs.thumb`, `gifs.clip`, `gifs.send`, `emoji.custom_files`, `emoji.packs`, `emoji.pack`, `emoji.animated` |

## Verification boundary

The tests inject a client factory, exercise the SDK through schema-checked stdio, and construct a real Telethon client with a fresh synthetic SQLite session offline. Fixtures isolate HOME, data and XDG paths and prohibit network connections. They cover account isolation, login and cancellation, reconnect, shutdown, sparse updates, peer and topic IDs, UTF-16, history boundaries, confirmation and redacted errors. Action regressions cover cross-chat mutation prevention, edit retries, forward correlation and flags, paged unpinning, activity scopes and draft clearing. Media regressions cover metadata, packed waveforms, static thumbnails, malicious filenames/peers, symlink rejection, atomic writes, cancelled transfers, cache eviction, byte limits and queued priorities. Real Telethon upload/download helpers are exercised with only their MTProto transport replaced; path validation and mutation/cancellation regressions exercise held local descriptors. The 72-test foundation/media/poll/ordinary-reaction baseline passed offline CI. Poll additions cover locked constructor serialization, sparse results, scoped opaque IDs, quiz/UTF-16 validation, create/vote/retract/close, unknown outcomes and live-account/topic isolation; they passed the baseline CI run. Reaction additions cover full/minimal/unknown snapshot semantics, literal emoji/custom IDs, full-set acknowledgement and explicit clearing, server continuations on short/empty pages, scoped cursor rejection and live topic/account isolation; those regressions passed the baseline CI. Unread additions cover locked offset/request serialization, oldest bounded slices without invented completion, group/bot/General topic identity, cross-account isolation, foreign/duplicate/unavailable rows, explicit completion and continuation, cancellation and redacted unknown failures; their dependency-backed CI is pending. Live account sign-in, server-side delivery and proxy connectivity still need a separately authorized smoke test.

Requests and flags were checked against the pinned 1.46.0 package source and Telegram's primary API descriptions: [forwarding](https://core.telegram.org/method/messages.forwardMessages), [edits](https://core.telegram.org/method/messages.editMessage), [pins](https://core.telegram.org/method/messages.updatePinnedMessage), [thread receipts](https://core.telegram.org/method/messages.readDiscussion), [drafts](https://core.telegram.org/method/messages.saveDraft). Forwarding lacks a client id in the frozen contract; an interrupted or incomplete confirmation remains ambiguous and must not be presented as safely retryable.

Uploads use public `upload_file` and explicit [sendMedia](https://core.telegram.org/method/messages.sendMedia) / [sendMultiMedia](https://core.telegram.org/method/messages.sendMultiMedia) requests. This preserves topic/spoiler fields that the pinned high-level convenience method does not expose. Uploaded bytes have no client id in the frozen `send_file`/`send_album` contract, so a lost send confirmation is ambiguous. Cancelling closes local descriptors; uploaded server-side temporary chunks may expire later. Photos use server-defined MIME; explicit MIME overrides require document mode. Voice notes require an explicit duration; waveform and duration on other outgoing types are rejected. No external encoder, metadata scanner, URL uploader or user-file cache path is used.

Poll constructors were checked byte-for-byte against the locked Telethon 1.46.0 wheel, SHA-256 `c56e55e4a1f95fde556fe13459a542fc758e8ba501c0bf76f2fc5d8409fa81cb`, whose `alltlobjects.py` declares **layer 233**. In this artifact `InputMediaPoll.correct_answers` contains integer indices and `Poll.hash` is required; earlier byte-array examples are incompatible. The primary descriptions of [polls](https://core.telegram.org/api/poll), [minimal results](https://core.telegram.org/constructor/pollResults), and [poll result refresh](https://core.telegram.org/method/messages.getPollResults) supplement that pinned source. No dependency pin or frozen schema was changed.
