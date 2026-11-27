-- Metadata foundation only. No runtime journal writer, physical unlink, quota
-- release or reader permission is enabled by these descriptive records.
CREATE TABLE IF NOT EXISTS managed_ready_retirement (
 stage_id TEXT PRIMARY KEY NOT NULL REFERENCES managed_stage_publication(stage_id)
   ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED,
 nonce TEXT NOT NULL UNIQUE CHECK(length(nonce)=32 AND nonce NOT GLOB '*[^0-9a-f]*'),
 birth_nonce TEXT NOT NULL UNIQUE CHECK(length(birth_nonce)=32 AND birth_nonce NOT GLOB '*[^0-9a-f]*'),
 client_id TEXT NOT NULL UNIQUE CHECK(length(client_id)=32 AND client_id NOT GLOB '*[^0-9a-f]*'),
 account_id TEXT NOT NULL REFERENCES account(account_id) ON DELETE CASCADE
   CHECK(length(CAST(account_id AS BLOB)) BETWEEN 1 AND 256),
 plugin_id TEXT NOT NULL CHECK(length(CAST(plugin_id AS BLOB)) BETWEEN 1 AND 256),
 account_created_at INTEGER NOT NULL CHECK(account_created_at>=0),
 path TEXT NOT NULL UNIQUE CHECK(length(CAST(path AS BLOB)) BETWEEN 1 AND 4096),
 version INTEGER NOT NULL DEFAULT 1 CHECK(version=1),
 identity_json TEXT NOT NULL CHECK(length(CAST(identity_json AS BLOB)) BETWEEN 1 AND 65536
   AND json_valid(identity_json) AND json_type(identity_json)='object'),
 phase TEXT NOT NULL DEFAULT 'intent' CHECK(phase IN ('intent','marked','bytes_absent','stage_absent')),
 created_at INTEGER NOT NULL CHECK(created_at>=0),
 updated_at INTEGER NOT NULL CHECK(updated_at>=0)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_managed_ready_retirement_owner
 ON managed_ready_retirement(account_id,account_created_at,stage_id);
CREATE TRIGGER IF NOT EXISTS managed_ready_retirement_insert BEFORE INSERT ON managed_ready_retirement
BEGIN
 SELECT CASE WHEN NEW.phase!='intent'
   THEN RAISE(ABORT,'ready retirement starts at intent') END;
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM managed_stage_publication p
   WHERE p.stage_id=NEW.stage_id AND p.nonce=NEW.birth_nonce AND p.client_id=NEW.client_id
   AND p.account_id=NEW.account_id AND p.plugin_id=NEW.plugin_id
   AND p.account_created_at=NEW.account_created_at AND p.path=NEW.path AND p.version=NEW.version
   AND p.identity_json=NEW.identity_json AND p.source_id IS NULL)
   THEN RAISE(ABORT,'ready retirement birth mismatch') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM managed_source
   WHERE stage_id=NEW.stage_id OR client_id=NEW.client_id OR path=NEW.path)
   OR EXISTS(SELECT 1 FROM outbox WHERE client_id=NEW.client_id)
   THEN RAISE(ABORT,'ready retirement already admitted') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_ready_retirement_identity_immutable
 BEFORE UPDATE OF stage_id,nonce,birth_nonce,client_id,account_id,plugin_id,account_created_at,path,version,identity_json,created_at
 ON managed_ready_retirement
BEGIN SELECT RAISE(ABORT,'ready retirement identity is immutable'); END;
CREATE TRIGGER IF NOT EXISTS managed_ready_retirement_phase BEFORE UPDATE OF phase ON managed_ready_retirement
WHEN NEW.phase!=OLD.phase
BEGIN
 SELECT CASE WHEN NOT ((OLD.phase='intent' AND NEW.phase='marked')
   OR (OLD.phase='marked' AND NEW.phase='bytes_absent')
   OR (OLD.phase='bytes_absent' AND NEW.phase='stage_absent'))
   THEN RAISE(ABORT,'ready retirement phase is not the next step') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_ready_retirement_excludes_source BEFORE INSERT ON managed_source
WHEN EXISTS(SELECT 1 FROM managed_ready_retirement j
 WHERE j.stage_id=NEW.stage_id OR j.client_id=NEW.client_id OR j.path=NEW.path)
 OR EXISTS(SELECT 1 FROM managed_ready_receipt r
 WHERE r.stage_id=NEW.stage_id OR r.client_id=NEW.client_id OR r.path=NEW.path)
BEGIN SELECT RAISE(ABORT,'ready retirement excludes source admission'); END;
CREATE TRIGGER IF NOT EXISTS managed_ready_retirement_excludes_link BEFORE UPDATE OF source_id ON managed_stage_publication
WHEN NEW.source_id IS NOT NULL AND EXISTS(SELECT 1 FROM managed_ready_retirement WHERE stage_id=NEW.stage_id)
BEGIN SELECT RAISE(ABORT,'ready retirement excludes source linkage'); END;

-- Compact history is not an active-source row, replay instruction or IO grant.
-- A future actual owned settlement writer must earn it; this migration has none.
CREATE TABLE IF NOT EXISTS managed_ready_receipt (
 stage_id TEXT PRIMARY KEY NOT NULL CHECK(length(stage_id)=32 AND stage_id NOT GLOB '*[^0-9a-f]*'),
 birth_nonce TEXT NOT NULL UNIQUE CHECK(length(birth_nonce)=32 AND birth_nonce NOT GLOB '*[^0-9a-f]*'),
 retirement_nonce TEXT NOT NULL UNIQUE CHECK(length(retirement_nonce)=32 AND retirement_nonce NOT GLOB '*[^0-9a-f]*'),
 client_id TEXT NOT NULL UNIQUE CHECK(length(client_id)=32 AND client_id NOT GLOB '*[^0-9a-f]*'),
 account_id TEXT NOT NULL REFERENCES account(account_id) ON DELETE CASCADE
   CHECK(length(CAST(account_id AS BLOB)) BETWEEN 1 AND 256),
 plugin_id TEXT NOT NULL CHECK(length(CAST(plugin_id AS BLOB)) BETWEEN 1 AND 256),
 account_created_at INTEGER NOT NULL CHECK(account_created_at>=0),
 path TEXT NOT NULL UNIQUE CHECK(length(CAST(path AS BLOB)) BETWEEN 1 AND 4096),
 version INTEGER NOT NULL DEFAULT 1 CHECK(version=1),
 byte_sha256 TEXT NOT NULL CHECK(length(byte_sha256)=64 AND byte_sha256 NOT GLOB '*[^0-9a-f]*'),
 identity_digest TEXT NOT NULL CHECK(length(identity_digest)=64 AND identity_digest NOT GLOB '*[^0-9a-f]*'),
 created_at INTEGER NOT NULL CHECK(created_at>=0),
 settled_at INTEGER NOT NULL CHECK(settled_at>=0)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_managed_ready_receipt_owner
 ON managed_ready_receipt(account_id,account_created_at,stage_id);
CREATE TRIGGER IF NOT EXISTS managed_ready_receipt_owner BEFORE INSERT ON managed_ready_receipt
BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM account a WHERE a.account_id=NEW.account_id
   AND a.plugin_id=NEW.plugin_id AND a.created_at=NEW.account_created_at)
   THEN RAISE(ABORT,'ready receipt owner mismatch') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_ready_receipt_lineage BEFORE INSERT ON managed_ready_receipt
BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM managed_ready_retirement j
   WHERE j.stage_id=NEW.stage_id AND j.birth_nonce=NEW.birth_nonce AND j.nonce=NEW.retirement_nonce
   AND j.client_id=NEW.client_id AND j.account_id=NEW.account_id AND j.plugin_id=NEW.plugin_id
   AND j.account_created_at=NEW.account_created_at AND j.path=NEW.path AND j.version=NEW.version
   AND j.phase='stage_absent' AND json_extract(j.identity_json,'$.manifest.sha256')=NEW.byte_sha256)
   THEN RAISE(ABORT,'ready receipt journal mismatch') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_ready_receipt_excludes_birth BEFORE INSERT ON managed_stage_publication
WHEN EXISTS(SELECT 1 FROM managed_ready_receipt r
 WHERE r.stage_id=NEW.stage_id OR r.client_id=NEW.client_id OR r.path=NEW.path)
BEGIN SELECT RAISE(ABORT,'ready receipt excludes publication reuse'); END;
CREATE TRIGGER IF NOT EXISTS managed_ready_receipt_immutable BEFORE UPDATE ON managed_ready_receipt
BEGIN SELECT RAISE(ABORT,'ready receipt is immutable'); END;
