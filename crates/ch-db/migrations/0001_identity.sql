-- 0001: plugins, accounts, persons/handles/categories, settings.
-- Timestamps are INTEGER unix milliseconds (UTC). Booleans are INTEGER 0/1; NULL = unknown.
-- JSON columns hold ch-protocol DTO fragments serialised as the wire format.

CREATE TABLE IF NOT EXISTS plugin (
    plugin_id     TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    version       TEXT NOT NULL DEFAULT '',
    api_version   TEXT NOT NULL DEFAULT '',
    install_dir   TEXT,
    brand_color   TEXT,
    icon_path     TEXT,
    manifest_json TEXT,
    enabled       INTEGER NOT NULL DEFAULT 1,
    installed     INTEGER NOT NULL DEFAULT 1,
    first_seen_at INTEGER NOT NULL,
    last_seen_at  INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS account (
    account_id    TEXT PRIMARY KEY,
    plugin_id     TEXT NOT NULL REFERENCES plugin(plugin_id) ON DELETE RESTRICT,
    label         TEXT,
    position      INTEGER NOT NULL DEFAULT 0,
    enabled       INTEGER NOT NULL DEFAULT 1,
    settings_json TEXT NOT NULL DEFAULT '{}',
    state         TEXT NOT NULL DEFAULT 'disconnected',
    state_detail  TEXT,
    reconnecting  INTEGER NOT NULL DEFAULT 0,
    state_at      INTEGER,
    me_user_id    TEXT,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_account_plugin ON account(plugin_id);

CREATE TABLE IF NOT EXISTS person (
    id           INTEGER PRIMARY KEY,
    display_name TEXT,
    notes        TEXT,
    created_at   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS category (
    id         INTEGER PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    color      TEXT,
    sort_order INTEGER NOT NULL DEFAULT 0
) STRICT;

CREATE TABLE IF NOT EXISTS person_category (
    person_id   INTEGER NOT NULL REFERENCES person(id) ON DELETE CASCADE,
    category_id INTEGER NOT NULL REFERENCES category(id) ON DELETE CASCADE,
    PRIMARY KEY (person_id, category_id)
) STRICT, WITHOUT ROWID;

-- One identity on one account (the Contact DTO plus local identity state).
CREATE TABLE IF NOT EXISTS platform_handle (
    id            INTEGER PRIMARY KEY,
    account_id    TEXT NOT NULL REFERENCES account(account_id) ON DELETE CASCADE,
    user_id       TEXT NOT NULL,
    person_id     INTEGER REFERENCES person(id) ON DELETE SET NULL,
    link_source   TEXT,                 -- 'auto_phone' | 'auto_username' | 'manual' | 'created' | NULL
    display_name  TEXT,
    first_name    TEXT,
    last_name     TEXT,
    username      TEXT,
    phone         TEXT,
    avatar_path   TEXT,
    is_bot        INTEGER,
    is_contact    INTEGER,
    is_premium    INTEGER,
    is_verified   INTEGER,
    is_deleted    INTEGER,
    bio           TEXT,
    status_json   TEXT,                 -- OnlineStatus, latest known
    status_at     INTEGER,
    ext_json      TEXT,
    first_seen_at INTEGER NOT NULL,
    last_seen_at  INTEGER NOT NULL,
    UNIQUE (account_id, user_id)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_handle_person ON platform_handle(person_id);
CREATE INDEX IF NOT EXISTS ix_handle_phone ON platform_handle(phone) WHERE phone IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_handle_username ON platform_handle(username COLLATE NOCASE) WHERE username IS NOT NULL;

CREATE TABLE IF NOT EXISTS settings (
    key        TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT, WITHOUT ROWID;
