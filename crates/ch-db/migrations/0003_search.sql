-- 0003: FTS5 index over message text (external content, kept in sync by triggers).
-- Deleted (tombstoned) messages stay searchable: kept-deleted is a feature.

CREATE VIRTUAL TABLE IF NOT EXISTS message_fts USING fts5(
    text,
    content='message',
    content_rowid='id',
    tokenize='unicode61 remove_diacritics 2',
    prefix='2 3'
);

CREATE TRIGGER IF NOT EXISTS trg_message_fts_ai AFTER INSERT ON message BEGIN
    INSERT INTO message_fts(rowid, text) VALUES (new.id, new.text);
END;
CREATE TRIGGER IF NOT EXISTS trg_message_fts_ad AFTER DELETE ON message BEGIN
    INSERT INTO message_fts(message_fts, rowid, text) VALUES ('delete', old.id, old.text);
END;
CREATE TRIGGER IF NOT EXISTS trg_message_fts_au AFTER UPDATE OF text ON message BEGIN
    INSERT INTO message_fts(message_fts, rowid, text) VALUES ('delete', old.id, old.text);
    INSERT INTO message_fts(rowid, text) VALUES (new.id, new.text);
END;
