-- 0002: conversations, folders, topics, messages, edit history, reactions, drafts, members.

CREATE TABLE IF NOT EXISTS conversation (
    id                  INTEGER PRIMARY KEY,
    account_id          TEXT NOT NULL REFERENCES account(account_id) ON DELETE CASCADE,
    chat_id             TEXT NOT NULL,
    kind                TEXT NOT NULL,            -- dm | group | channel | other wire value
    title               TEXT,
    peer_user_id        TEXT,
    parent_chat_id      TEXT,
    username            TEXT,
    about               TEXT,
    archived            INTEGER NOT NULL DEFAULT 0,   -- on create false
    muted               INTEGER,
    mute_until          INTEGER,
    pinned              INTEGER,
    pin_order           INTEGER,
    unread_count        INTEGER NOT NULL DEFAULT 0,   -- on create 0
    unread_mentions     INTEGER NOT NULL DEFAULT 0,
    unread_reactions    INTEGER NOT NULL DEFAULT 0,
    marked_unread       INTEGER,
    read_inbox_max_id   TEXT,
    read_outbox_max_id  TEXT,
    last_message_at     INTEGER,
    is_self_chat        INTEGER NOT NULL DEFAULT 0,   -- on create false
    avatar_path         TEXT,
    member_count        INTEGER,
    can_post            INTEGER,
    is_forum            INTEGER,
    color_index         INTEGER,
    scheduled_count     INTEGER,
    auto_delete_seconds INTEGER,
    slow_mode_seconds   INTEGER,
    permissions_json    TEXT,
    is_verified         INTEGER,
    is_scam             INTEGER,
    is_restricted       INTEGER,
    ext_json            TEXT,
    -- local state the platform never resends
    is_gone             INTEGER NOT NULL DEFAULT 0,   -- not in the last conversations.synced; history kept
    notify_override     TEXT,                         -- 'on' | 'off' | NULL (= global policy)
    notify_sound        TEXT,
    wallpaper           TEXT,
    local_read_at       INTEGER,                      -- when this app last showed the chat as read
    last_message_preview TEXT,                        -- denormalised for the chat list
    last_message_kind   TEXT,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    UNIQUE (account_id, chat_id)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_conversation_list ON conversation(account_id, is_gone, archived, last_message_at DESC);
CREATE INDEX IF NOT EXISTS ix_conversation_peer ON conversation(account_id, peer_user_id) WHERE peer_user_id IS NOT NULL;

-- Platform folders (account_id = the account) and local cross-platform folders (account_id = '').
CREATE TABLE IF NOT EXISTS folder (
    id         INTEGER PRIMARY KEY,
    account_id TEXT NOT NULL DEFAULT '',
    folder_id  TEXT NOT NULL,
    title      TEXT NOT NULL,
    emoticon   TEXT,
    position   INTEGER,
    rules_json TEXT,                 -- FolderRules, wholesale
    is_shared  INTEGER NOT NULL DEFAULT 0,
    ext_json   TEXT,
    updated_at INTEGER NOT NULL,
    UNIQUE (account_id, folder_id)
) STRICT;

-- Explicit folder membership: Folder.chat_ids and Conversation.folder_ids both land here.
-- Keyed by platform ids (not the folder rowid) so membership reported before the folder itself
-- is kept; folder_account_id is the folder's owner ('' = local folder), account_id the chat's.
CREATE TABLE IF NOT EXISTS folder_chat (
    folder_account_id TEXT NOT NULL,
    folder_id         TEXT NOT NULL,
    account_id        TEXT NOT NULL,
    chat_id           TEXT NOT NULL,
    source            TEXT NOT NULL,               -- 'folder' (Folder.chat_ids) | 'chat' (Conversation.folder_ids) | 'local'
    PRIMARY KEY (folder_account_id, folder_id, account_id, chat_id, source)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_folder_chat_chat ON folder_chat(account_id, chat_id);

CREATE TABLE IF NOT EXISTS topic (
    id                   INTEGER PRIMARY KEY,
    conversation_id      INTEGER NOT NULL REFERENCES conversation(id) ON DELETE CASCADE,
    topic_id             TEXT NOT NULL,
    title                TEXT NOT NULL,
    icon_emoji           TEXT,
    icon_custom_emoji_id TEXT,
    icon_color           INTEGER,
    is_general           INTEGER NOT NULL DEFAULT 0,
    unread_count         INTEGER NOT NULL DEFAULT 0,
    unread_mentions      INTEGER NOT NULL DEFAULT 0,
    unread_reactions     INTEGER NOT NULL DEFAULT 0,
    pinned               INTEGER NOT NULL DEFAULT 0,
    closed               INTEGER NOT NULL DEFAULT 0,
    hidden               INTEGER NOT NULL DEFAULT 0,
    last_message_at      INTEGER,
    last_message_preview TEXT,
    read_inbox_max_id    TEXT,
    read_outbox_max_id   TEXT,
    ext_json             TEXT,
    is_gone              INTEGER NOT NULL DEFAULT 0,   -- not in the last complete topics.upsert
    created_at           INTEGER NOT NULL,
    updated_at           INTEGER NOT NULL,
    UNIQUE (conversation_id, topic_id)
) STRICT;

CREATE TABLE IF NOT EXISTS message (
    id                INTEGER PRIMARY KEY,
    conversation_id   INTEGER NOT NULL REFERENCES conversation(id) ON DELETE CASCADE,
    message_id        TEXT NOT NULL,              -- platform id; 'local:<client_id>' for an outbox echo
    topic_id          TEXT,                       -- platform topic id; no FK (topic rows may lag)
    direction         TEXT NOT NULL,              -- in | out
    sent_at           INTEGER NOT NULL,
    sender_id         TEXT,
    sender_name       TEXT,
    sender_title      TEXT,
    text              TEXT,
    entities_json     TEXT,                       -- [Entity]
    media_kind        TEXT,                       -- Media.kind, for filters
    media_json        TEXT,                       -- Media, wholesale
    reply_to_json     TEXT,                       -- ReplyRef
    reply_to_id       TEXT,                       -- ReplyRef.message_id, for lookups
    forward_from_json TEXT,                       -- ForwardInfo
    grouped_id        TEXT,
    reactors_json     TEXT,                       -- [Reactor], newest first
    poll_json         TEXT,
    location_json     TEXT,
    contact_card_json TEXT,
    dice_json         TEXT,
    link_preview_json TEXT,
    buttons_json      TEXT,                       -- [[InlineButton]]
    views             INTEGER,
    replies_count     INTEGER,
    replies_chat_id   TEXT,
    effect_id         TEXT,
    action_json       TEXT,                       -- ServiceAction; set => service message
    mentioned         INTEGER,
    edited            INTEGER NOT NULL DEFAULT 0, -- on create false
    edited_at         INTEGER,
    delivery          TEXT,                       -- sending | sent | delivered | read | failed
    read_by_json      TEXT,                       -- [user_id]
    is_scheduled      INTEGER NOT NULL DEFAULT 0, -- on create false
    is_pinned         INTEGER,
    silent            INTEGER,
    ttl_seconds       INTEGER,
    via_bot_id        TEXT,
    ext_json          TEXT,
    -- local state
    deleted_at        INTEGER,                    -- tombstone: the platform/user deleted it, content kept
    deleted_by        TEXT,                       -- 'platform' | 'me' | NULL
    has_link          INTEGER NOT NULL DEFAULT 0, -- any url/text_url entity or link preview (media:link)
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL,
    UNIQUE (conversation_id, message_id)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_message_thread ON message(conversation_id, sent_at, id);
CREATE INDEX IF NOT EXISTS ix_message_topic ON message(conversation_id, topic_id, sent_at) WHERE topic_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_message_sender ON message(sender_id, sent_at) WHERE sender_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_message_media ON message(media_kind, sent_at) WHERE media_kind IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_message_grouped ON message(conversation_id, grouped_id) WHERE grouped_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_message_pinned ON message(conversation_id, is_pinned) WHERE is_pinned = 1;
CREATE INDEX IF NOT EXISTS ix_message_scheduled ON message(conversation_id, is_scheduled) WHERE is_scheduled = 1;

-- The content a message had before an edit replaced it (text, entities, media).
CREATE TABLE IF NOT EXISTS message_version (
    id            INTEGER PRIMARY KEY,
    message_id    INTEGER NOT NULL REFERENCES message(id) ON DELETE CASCADE,
    text          TEXT,
    entities_json TEXT,
    media_json    TEXT,
    edited_at     INTEGER,                        -- the edited_at that version carried (NULL = original)
    replaced_at   INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_message_version_message ON message_version(message_id, replaced_at);

-- Current reactions on a message, replaced wholesale from Message.reactions / reaction.changed.
-- key is canonical: 'e:<emoji>' | 'c:<custom_emoji_id>' | 'paid'.
CREATE TABLE IF NOT EXISTS message_reaction (
    message_id INTEGER NOT NULL REFERENCES message(id) ON DELETE CASCADE,
    key        TEXT NOT NULL,
    key_json   TEXT NOT NULL,                     -- ReactionKey as on the wire
    count      INTEGER NOT NULL,
    mine       INTEGER NOT NULL DEFAULT 0,
    my_count   INTEGER,
    position   INTEGER NOT NULL DEFAULT 0,        -- order as reported
    PRIMARY KEY (message_id, key)
) STRICT, WITHOUT ROWID;

-- Per chat and per topic ('' = the chat itself). Empty text never stored: the row is deleted.
CREATE TABLE IF NOT EXISTS draft (
    conversation_id INTEGER NOT NULL REFERENCES conversation(id) ON DELETE CASCADE,
    topic_id        TEXT NOT NULL DEFAULT '',
    text            TEXT NOT NULL,
    entities_json   TEXT,
    reply_to        TEXT,
    updated_at      INTEGER NOT NULL,
    ext_json        TEXT,
    PRIMARY KEY (conversation_id, topic_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS member (
    conversation_id INTEGER NOT NULL REFERENCES conversation(id) ON DELETE CASCADE,
    user_id         TEXT NOT NULL,
    role            TEXT NOT NULL,
    rank            TEXT,
    joined_at       INTEGER,
    invited_by      TEXT,
    updated_at      INTEGER NOT NULL,
    PRIMARY KEY (conversation_id, user_id)
) STRICT, WITHOUT ROWID;
