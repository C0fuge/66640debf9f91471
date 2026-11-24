-- 0004: outbox, history cursors and gaps, media cache index, presence log.

-- Core-initiated sends and edits waiting for / recovering from the plugin. The state machine
-- lives in ch-core (later); this table only records it.
-- states: queued -> sending -> sent | failed (retryable, attempts++) | cancelled
CREATE TABLE IF NOT EXISTS outbox (
    id                INTEGER PRIMARY KEY,
    account_id        TEXT NOT NULL REFERENCES account(account_id) ON DELETE CASCADE,
    chat_id           TEXT NOT NULL,
    topic_id          TEXT,
    client_id         TEXT NOT NULL UNIQUE,       -- also the local echo's message_id suffix ('local:<client_id>')
    kind              TEXT NOT NULL,              -- send_text | send_file | send_album | forward | edit | delete | other
    method            TEXT NOT NULL,              -- the plugin method to call
    params_json       TEXT NOT NULL,              -- its params (paths absolute)
    state             TEXT NOT NULL DEFAULT 'queued',
    attempts          INTEGER NOT NULL DEFAULT 0,
    next_attempt_at   INTEGER,
    last_error_json   TEXT,                       -- ErrorObject of the last failure
    echo_message_id   INTEGER REFERENCES message(id) ON DELETE SET NULL,  -- the local echo row
    result_message_ids TEXT,                      -- JSON [id] once sent
    request_id        TEXT,                       -- in-flight JSON-RPC id (for cancel / progress)
    done_bytes        INTEGER,
    total_bytes       INTEGER,
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_outbox_state ON outbox(account_id, state, next_attempt_at);
CREATE INDEX IF NOT EXISTS ix_outbox_chat ON outbox(account_id, chat_id, created_at);

-- What history the core has for a chat/topic: the oldest page's cursor and whether the start was reached.
CREATE TABLE IF NOT EXISTS history_state (
    conversation_id   INTEGER NOT NULL REFERENCES conversation(id) ON DELETE CASCADE,
    topic_id          TEXT NOT NULL DEFAULT '',
    older_cursor      TEXT,                       -- next_cursor of the oldest fetched page
    reached_start     INTEGER NOT NULL DEFAULT 0, -- a page came back without next_cursor
    oldest_message_id TEXT,
    oldest_sent_at    INTEGER,
    newest_message_id TEXT,
    newest_sent_at    INTEGER,
    last_fetch_at     INTEGER,
    updated_at        INTEGER NOT NULL,
    PRIMARY KEY (conversation_id, topic_id)
) STRICT, WITHOUT ROWID;

-- A hole between two known messages (missed live updates, a reconnect). history.fill_gap closes it.
CREATE TABLE IF NOT EXISTS history_gap (
    id                INTEGER PRIMARY KEY,
    conversation_id   INTEGER NOT NULL REFERENCES conversation(id) ON DELETE CASCADE,
    topic_id          TEXT NOT NULL DEFAULT '',
    after_message_id  TEXT NOT NULL,
    before_message_id TEXT,                       -- NULL = up to now
    cursor            TEXT,                       -- paging inside the gap
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_history_gap_conv ON history_gap(conversation_id, topic_id);

-- Every cache file the core knows about, for size limits and "clear cache".
CREATE TABLE IF NOT EXISTS media_file (
    path            TEXT PRIMARY KEY,
    account_id      TEXT NOT NULL,
    kind            TEXT NOT NULL,                -- full | thumb | avatar | sticker | other
    message_id      INTEGER REFERENCES message(id) ON DELETE SET NULL,
    media_kind      TEXT,
    bytes           INTEGER,
    created_at      INTEGER NOT NULL,
    last_access_at  INTEGER NOT NULL
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_media_file_account ON media_file(account_id, kind, last_access_at);
CREATE INDEX IF NOT EXISTS ix_media_file_message ON media_file(message_id) WHERE message_id IS NOT NULL;

-- Local record of what contacts did (a setting turns it on).
CREATE TABLE IF NOT EXISTS presence_log (
    id          INTEGER PRIMARY KEY,
    account_id  TEXT NOT NULL REFERENCES account(account_id) ON DELETE CASCADE,
    user_id     TEXT NOT NULL,
    chat_id     TEXT,
    at          INTEGER NOT NULL,
    kind        TEXT NOT NULL,                    -- online | offline | typing | recording_voice | ... | read | reacted | message | edit
    detail_json TEXT
) STRICT;
CREATE INDEX IF NOT EXISTS ix_presence_log_user ON presence_log(account_id, user_id, at);
