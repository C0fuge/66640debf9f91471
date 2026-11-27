-- Descriptive exclusive-publication facts. No runtime writer, read permission,
-- native dispatch, source closure or unlink authority is enabled by this schema.
CREATE TABLE IF NOT EXISTS managed_stage_publication (
 stage_id TEXT PRIMARY KEY NOT NULL CHECK(length(stage_id)=32 AND stage_id NOT GLOB '*[^0-9a-f]*'),
 nonce TEXT NOT NULL UNIQUE CHECK(length(nonce)=32 AND nonce NOT GLOB '*[^0-9a-f]*'),
 client_id TEXT NOT NULL UNIQUE CHECK(length(client_id)=32 AND client_id NOT GLOB '*[^0-9a-f]*'),
 account_id TEXT NOT NULL REFERENCES account(account_id) ON DELETE CASCADE
   CHECK(length(CAST(account_id AS BLOB)) BETWEEN 1 AND 256),
 plugin_id TEXT NOT NULL CHECK(length(CAST(plugin_id AS BLOB)) BETWEEN 1 AND 256),
 account_created_at INTEGER NOT NULL CHECK(account_created_at>=0),
 path TEXT NOT NULL UNIQUE CHECK(length(CAST(path AS BLOB)) BETWEEN 1 AND 4096),
 version INTEGER NOT NULL DEFAULT 1 CHECK(version=1),
 identity_json TEXT NOT NULL CHECK(length(CAST(identity_json AS BLOB)) BETWEEN 1 AND 65536
   AND json_valid(identity_json) AND json_type(identity_json)='object'),
 source_id INTEGER UNIQUE REFERENCES managed_source(id) ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED
   CHECK(source_id IS NULL OR source_id>0),
 created_at INTEGER NOT NULL CHECK(created_at>=0)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_managed_stage_publication_owner
 ON managed_stage_publication(account_id,account_created_at,stage_id);
CREATE TRIGGER IF NOT EXISTS managed_stage_publication_identity_immutable
 BEFORE UPDATE OF stage_id,nonce,client_id,account_id,plugin_id,account_created_at,path,version,identity_json,created_at
 ON managed_stage_publication
BEGIN SELECT RAISE(ABORT,'managed publication identity is immutable'); END;
CREATE TRIGGER IF NOT EXISTS managed_stage_publication_insert BEFORE INSERT ON managed_stage_publication
BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM account a WHERE a.account_id=NEW.account_id
   AND a.plugin_id=NEW.plugin_id AND a.created_at=NEW.account_created_at)
   THEN RAISE(ABORT,'managed publication owner mismatch') END;
 SELECT CASE WHEN NEW.source_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM managed_source s
   WHERE s.id=NEW.source_id AND s.stage_id=NEW.stage_id AND s.client_id=NEW.client_id
   AND s.account_id=NEW.account_id AND s.plugin_id=NEW.plugin_id
   AND s.account_created_at=NEW.account_created_at AND s.path=NEW.path AND s.state='retained')
   THEN RAISE(ABORT,'managed publication source mismatch') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_stage_publication_source_link BEFORE UPDATE OF source_id ON managed_stage_publication
BEGIN
 SELECT CASE WHEN OLD.source_id IS NOT NULL AND NEW.source_id IS NOT OLD.source_id
   THEN RAISE(ABORT,'managed publication source link is write once') END;
 SELECT CASE WHEN NEW.source_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM managed_source s
   WHERE s.id=NEW.source_id AND s.stage_id=NEW.stage_id AND s.client_id=NEW.client_id
   AND s.account_id=NEW.account_id AND s.plugin_id=NEW.plugin_id
   AND s.account_created_at=NEW.account_created_at AND s.path=NEW.path
   AND (OLD.source_id IS NOT NULL OR s.state='retained'))
   THEN RAISE(ABORT,'managed publication source mismatch') END;
END;
