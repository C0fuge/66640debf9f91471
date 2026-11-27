-- Managed upload metadata and conservative reverse references. No source admission,
-- settlement or unlink authority is enabled by this migration. Literal refs never
-- prove filesystem identity. Path extraction below is bounded before json_tree.
CREATE TABLE IF NOT EXISTS managed_source (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 stage_id TEXT NOT NULL UNIQUE CHECK(length(stage_id)=32 AND stage_id NOT GLOB '*[^0-9a-f]*'),
 client_id TEXT NOT NULL UNIQUE CHECK(length(client_id)=32 AND client_id NOT GLOB '*[^0-9a-f]*'),
 account_id TEXT NOT NULL REFERENCES account(account_id) ON DELETE CASCADE,
 plugin_id TEXT NOT NULL, account_created_at INTEGER NOT NULL,
 chat_id TEXT NOT NULL, topic_id TEXT,
 path TEXT NOT NULL UNIQUE CHECK(length(CAST(path AS BLOB)) BETWEEN 1 AND 4096),
 fingerprint_json TEXT NOT NULL CHECK(length(CAST(fingerprint_json AS BLOB))<=65536 AND json_valid(fingerprint_json) AND json_type(fingerprint_json)='object'),
 intent_json TEXT NOT NULL CHECK(length(CAST(intent_json AS BLOB))<=65536 AND json_valid(intent_json) AND json_type(intent_json)='object'),
 state TEXT NOT NULL DEFAULT 'retained' CHECK(state IN ('retained','retiring','released')),
 settled_dispatch_id INTEGER,
 created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_managed_source_owner ON managed_source(account_id,account_created_at,state);
CREATE TABLE IF NOT EXISTS managed_producer (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 nonce TEXT NOT NULL UNIQUE CHECK(length(nonce)=32 AND nonce NOT GLOB '*[^0-9a-f]*'),
 core_nonce TEXT NOT NULL CHECK(length(core_nonce)=32 AND core_nonce NOT GLOB '*[^0-9a-f]*'),
 platform TEXT NOT NULL, native_pid INTEGER, process_start TEXT, host_scope TEXT, boot_id TEXT,
 holder_scope_json TEXT CHECK(holder_scope_json IS NULL OR (length(CAST(holder_scope_json AS BLOB))<=65536 AND json_valid(holder_scope_json))),
 proof_version INTEGER NOT NULL DEFAULT 1 CHECK(proof_version=1),
 state TEXT NOT NULL DEFAULT 'unresolved' CHECK(state IN ('unresolved','drained')),
 drain_proof_json TEXT CHECK(drain_proof_json IS NULL OR (length(CAST(drain_proof_json AS BLOB))<=65536 AND json_valid(drain_proof_json))),
 CHECK(state='unresolved' OR drain_proof_json IS NOT NULL)
) STRICT;
CREATE TABLE IF NOT EXISTS managed_dispatch (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 nonce TEXT NOT NULL UNIQUE CHECK(length(nonce)=32 AND nonce NOT GLOB '*[^0-9a-f]*'),
 source_id INTEGER NOT NULL REFERENCES managed_source(id) ON DELETE CASCADE,
 producer_id INTEGER NOT NULL REFERENCES managed_producer(id),
 outbox_id INTEGER NOT NULL, client_id TEXT NOT NULL,
 method TEXT NOT NULL CHECK(method='messages.send_file'),
 params_json TEXT NOT NULL CHECK(length(CAST(params_json AS BLOB))<=65536 AND json_valid(params_json) AND json_type(params_json)='object'),
 source_fingerprint TEXT NOT NULL,
 source_release TEXT CHECK(source_release IS NULL OR source_release='on_success'),
 state TEXT NOT NULL DEFAULT 'prepared' CHECK(state IN ('prepared','exposed','positive','no_wire','unknown','scope_drained_unknown')),
 result_ids_json TEXT CHECK(result_ids_json IS NULL OR (length(CAST(result_ids_json AS BLOB))<=65536 AND json_valid(result_ids_json) AND json_type(result_ids_json)='array')),
 response_digest TEXT,
 created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
 CHECK(state!='positive' OR (source_release IS 'on_success' AND result_ids_json IS NOT NULL AND json_array_length(result_ids_json)>0 AND response_digest IS NOT NULL))
) STRICT;
CREATE INDEX IF NOT EXISTS ix_managed_dispatch_source ON managed_dispatch(source_id,state,id);
CREATE INDEX IF NOT EXISTS ix_managed_dispatch_producer ON managed_dispatch(producer_id,state);
-- Separate binding table preserves idempotent SQL reruns without an ALTER TABLE.
CREATE TABLE IF NOT EXISTS managed_outbox_dispatch (
 outbox_id INTEGER PRIMARY KEY REFERENCES outbox(id) ON DELETE CASCADE,
 dispatch_id INTEGER NOT NULL UNIQUE REFERENCES managed_dispatch(id) ON DELETE CASCADE
) STRICT;
CREATE TABLE IF NOT EXISTS managed_retirement (
 source_id INTEGER PRIMARY KEY REFERENCES managed_source(id) ON DELETE CASCADE,
 nonce TEXT NOT NULL UNIQUE CHECK(length(nonce)=32 AND nonce NOT GLOB '*[^0-9a-f]*'),
 dispatch_id INTEGER NOT NULL,
 evidence_json TEXT NOT NULL CHECK(length(CAST(evidence_json AS BLOB))<=1048576 AND json_valid(evidence_json) AND json_type(evidence_json)='object'),
 phase TEXT NOT NULL CHECK(phase IN ('detached','unlinked','released')),
 error_code TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
) STRICT;
-- A single durable counter prevents row-ID delete/reuse from reusing a proof revision.
CREATE TABLE IF NOT EXISTS managed_ref_clock (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 revision INTEGER NOT NULL CHECK(revision>=0)
) STRICT;
INSERT OR IGNORE INTO managed_ref_clock(singleton,revision) VALUES(1,0);
CREATE TABLE IF NOT EXISTS managed_ref_row (
 consumer_kind TEXT NOT NULL CHECK(consumer_kind IN ('message','message_version','outbox','media_file','conversation','platform_handle','plugin')),
 consumer_key TEXT NOT NULL,
 revision INTEGER NOT NULL CHECK(revision>0),
 state TEXT NOT NULL CHECK(state IN ('dirty','clean','blocked')),
 reason TEXT,
 PRIMARY KEY(consumer_kind,consumer_key)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_managed_ref_row_state ON managed_ref_row(state,consumer_kind,consumer_key);
CREATE TABLE IF NOT EXISTS managed_ref (
 consumer_kind TEXT NOT NULL, consumer_key TEXT NOT NULL CHECK(length(CAST(consumer_key AS BLOB))<=4096),
 column_name TEXT NOT NULL CHECK(length(column_name)<=128), slot TEXT NOT NULL CHECK(length(CAST(slot AS BLOB))<=4096),
 path TEXT NOT NULL CHECK(length(CAST(path AS BLOB))<=4096),
 account_id TEXT CHECK(account_id IS NULL OR length(CAST(account_id AS BLOB))<=4096), message_id INTEGER,
 revision INTEGER NOT NULL,
 source_id INTEGER REFERENCES managed_source(id) ON DELETE SET NULL,
 classification TEXT NOT NULL DEFAULT 'unresolved' CHECK(classification IN ('unresolved','outside','source','blocked')),
 resolved_dev INTEGER, resolved_ino INTEGER, identity_json TEXT
 CHECK(identity_json IS NULL OR (length(CAST(identity_json AS BLOB))<=65536 AND json_valid(identity_json))),
 CHECK(classification NOT IN ('outside','source') OR (resolved_dev IS NOT NULL AND resolved_ino IS NOT NULL AND identity_json IS NOT NULL)),
 PRIMARY KEY(consumer_kind,consumer_key,column_name,slot),
 FOREIGN KEY(consumer_kind,consumer_key) REFERENCES managed_ref_row(consumer_kind,consumer_key) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_managed_ref_source ON managed_ref(source_id,consumer_kind,consumer_key,slot);
CREATE INDEX IF NOT EXISTS ix_managed_ref_path ON managed_ref(path);
CREATE INDEX IF NOT EXISTS ix_managed_ref_identity ON managed_ref(resolved_dev,resolved_ino);
CREATE INDEX IF NOT EXISTS ix_managed_ref_unresolved ON managed_ref(classification,consumer_kind,consumer_key,column_name,slot);
CREATE TABLE IF NOT EXISTS managed_ref_coverage (
 consumer_kind TEXT PRIMARY KEY,
 format_version INTEGER NOT NULL DEFAULT 1 CHECK(format_version=1),
 integer_cursor INTEGER, text_cursor TEXT,
 complete INTEGER NOT NULL DEFAULT 0 CHECK(complete IN (0,1))
) STRICT, WITHOUT ROWID;
CREATE TRIGGER IF NOT EXISTS managed_source_identity_immutable BEFORE UPDATE OF stage_id,client_id,account_id,plugin_id,account_created_at,chat_id,topic_id,path,fingerprint_json,intent_json ON managed_source
BEGIN SELECT RAISE(ABORT,'managed source identity is immutable'); END;
INSERT OR IGNORE INTO managed_ref_coverage(consumer_kind) VALUES ('message');

-- message: bounded extraction and dirty revision on every actual path write.
CREATE TRIGGER IF NOT EXISTS managed_message_insert AFTER INSERT ON message BEGIN
 UPDATE managed_ref_clock SET revision=revision+1 WHERE singleton=1;
 INSERT INTO managed_ref_row(consumer_kind,consumer_key,revision,state,reason)
 VALUES('message',CAST(NEW.id AS TEXT),(SELECT revision FROM managed_ref_clock WHERE singleton=1),'dirty',NULL)
 ON CONFLICT(consumer_kind,consumer_key) DO UPDATE SET revision=excluded.revision,state='dirty',reason=NULL;
 DELETE FROM managed_ref WHERE consumer_kind='message' AND consumer_key=CAST(NEW.id AS TEXT);
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'message',CAST(NEW.id AS TEXT),v.column_name,'$scalar',v.value,(SELECT account_id FROM conversation WHERE id=NEW.conversation_id),NEW.id,r.revision,s.id
 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'link_preview_json' AS column_name,NEW.link_preview_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'reply_to_json' AS column_name,NEW.reply_to_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'forward_from_json' AS column_name,NEW.forward_from_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'poll_json' AS column_name,NEW.poll_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'location_json' AS column_name,NEW.location_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'contact_card_json' AS column_name,NEW.contact_card_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'dice_json' AS column_name,NEW.dice_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'buttons_json' AS column_name,NEW.buttons_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'action_json' AS column_name,NEW.action_json AS value,1 AS is_json,1 AS extension) v JOIN managed_ref_row r ON r.consumer_kind='message' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=v.value
 WHERE v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST((SELECT account_id FROM conversation WHERE id=NEW.conversation_id) AS BLOB)),0)<=4096;
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'message',CAST(NEW.id AS TEXT),v.column_name,j.fullkey,j.value,(SELECT account_id FROM conversation WHERE id=NEW.conversation_id),NEW.id,r.revision,s.id
 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'link_preview_json' AS column_name,NEW.link_preview_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'reply_to_json' AS column_name,NEW.reply_to_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'forward_from_json' AS column_name,NEW.forward_from_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'poll_json' AS column_name,NEW.poll_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'location_json' AS column_name,NEW.location_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'contact_card_json' AS column_name,NEW.contact_card_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'dice_json' AS column_name,NEW.dice_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'buttons_json' AS column_name,NEW.buttons_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'action_json' AS column_name,NEW.action_json AS value,1 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
 JOIN managed_ref_row r ON r.consumer_kind='message' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=j.value
 WHERE v.is_json=1 AND j.type='text' AND (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path')
 AND length(CAST(j.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(j.fullkey AS BLOB))<=4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST((SELECT account_id FROM conversation WHERE id=NEW.conversation_id) AS BLOB)),0)<=4096;
 UPDATE managed_ref_row SET state='blocked',reason='unindexable_path_payload'
 WHERE consumer_kind='message' AND consumer_key=CAST(NEW.id AS TEXT) AND (
 length(CAST(NEW.id AS BLOB))>4096 OR coalesce(length(CAST((SELECT account_id FROM conversation WHERE id=NEW.conversation_id) AS BLOB)),0)>4096
 OR EXISTS(SELECT 1 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'link_preview_json' AS column_name,NEW.link_preview_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'reply_to_json' AS column_name,NEW.reply_to_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'forward_from_json' AS column_name,NEW.forward_from_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'poll_json' AS column_name,NEW.poll_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'location_json' AS column_name,NEW.location_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'contact_card_json' AS column_name,NEW.contact_card_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'dice_json' AS column_name,NEW.dice_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'buttons_json' AS column_name,NEW.buttons_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'action_json' AS column_name,NEW.action_json AS value,1 AS is_json,1 AS extension) v WHERE
   (v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB))>4096) OR
   (v.is_json=1 AND v.value IS NOT NULL AND CASE WHEN length(CAST(v.value AS BLOB))>65536 THEN 1 WHEN json_valid(v.value) THEN 0 ELSE 1 END))
 OR EXISTS(SELECT 1 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'link_preview_json' AS column_name,NEW.link_preview_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'reply_to_json' AS column_name,NEW.reply_to_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'forward_from_json' AS column_name,NEW.forward_from_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'poll_json' AS column_name,NEW.poll_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'location_json' AS column_name,NEW.location_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'contact_card_json' AS column_name,NEW.contact_card_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'dice_json' AS column_name,NEW.dice_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'buttons_json' AS column_name,NEW.buttons_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'action_json' AS column_name,NEW.action_json AS value,1 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path') AND
     (j.type NOT IN ('text','null') OR length(CAST(j.value AS BLOB))>4096 OR length(CAST(j.fullkey AS BLOB))>4096 OR v.extension=1 OR (j.key!='path' AND j.key!='thumb_path') OR (v.column_name='media_json' AND j.path!='$') OR (v.column_name='link_preview_json' AND NOT ((j.path='$' AND j.key='thumb_path') OR (j.path='$.media' AND j.key IN ('path','thumb_path'))))))
 OR EXISTS(SELECT 1 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'link_preview_json' AS column_name,NEW.link_preview_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'reply_to_json' AS column_name,NEW.reply_to_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'forward_from_json' AS column_name,NEW.forward_from_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'poll_json' AS column_name,NEW.poll_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'location_json' AS column_name,NEW.location_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'contact_card_json' AS column_name,NEW.contact_card_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'dice_json' AS column_name,NEW.dice_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'buttons_json' AS column_name,NEW.buttons_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'action_json' AS column_name,NEW.action_json AS value,1 AS is_json,1 AS extension) v WHERE v.extension=1 AND v.is_json=0 AND v.value IS NOT NULL)
 OR EXISTS(SELECT 1 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'link_preview_json' AS column_name,NEW.link_preview_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'reply_to_json' AS column_name,NEW.reply_to_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'forward_from_json' AS column_name,NEW.forward_from_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'poll_json' AS column_name,NEW.poll_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'location_json' AS column_name,NEW.location_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'contact_card_json' AS column_name,NEW.contact_card_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'dice_json' AS column_name,NEW.dice_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'buttons_json' AS column_name,NEW.buttons_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'action_json' AS column_name,NEW.action_json AS value,1 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE j.key IS NOT NULL GROUP BY v.column_name,j.parent,j.key HAVING count(*)>1));
 SELECT CASE WHEN EXISTS(SELECT 1 FROM managed_ref r JOIN managed_source s ON s.path=r.path
 WHERE r.consumer_kind='message' AND r.consumer_key=CAST(NEW.id AS TEXT) AND s.state!='retained')
 THEN RAISE(ABORT,'managed source path is retired') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_message_update AFTER UPDATE OF media_json,link_preview_json,ext_json,reply_to_json,forward_from_json,poll_json,location_json,contact_card_json,dice_json,buttons_json,action_json,conversation_id,id ON message BEGIN
 DELETE FROM managed_ref_row WHERE consumer_kind='message' AND consumer_key=CAST(OLD.id AS TEXT) AND OLD.id IS NOT NEW.id;
 UPDATE managed_ref_clock SET revision=revision+1 WHERE singleton=1;
 INSERT INTO managed_ref_row(consumer_kind,consumer_key,revision,state,reason)
 VALUES('message',CAST(NEW.id AS TEXT),(SELECT revision FROM managed_ref_clock WHERE singleton=1),'dirty',NULL)
 ON CONFLICT(consumer_kind,consumer_key) DO UPDATE SET revision=excluded.revision,state='dirty',reason=NULL;
 DELETE FROM managed_ref WHERE consumer_kind='message' AND consumer_key=CAST(NEW.id AS TEXT);
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'message',CAST(NEW.id AS TEXT),v.column_name,'$scalar',v.value,(SELECT account_id FROM conversation WHERE id=NEW.conversation_id),NEW.id,r.revision,s.id
 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'link_preview_json' AS column_name,NEW.link_preview_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'reply_to_json' AS column_name,NEW.reply_to_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'forward_from_json' AS column_name,NEW.forward_from_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'poll_json' AS column_name,NEW.poll_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'location_json' AS column_name,NEW.location_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'contact_card_json' AS column_name,NEW.contact_card_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'dice_json' AS column_name,NEW.dice_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'buttons_json' AS column_name,NEW.buttons_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'action_json' AS column_name,NEW.action_json AS value,1 AS is_json,1 AS extension) v JOIN managed_ref_row r ON r.consumer_kind='message' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=v.value
 WHERE v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST((SELECT account_id FROM conversation WHERE id=NEW.conversation_id) AS BLOB)),0)<=4096;
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'message',CAST(NEW.id AS TEXT),v.column_name,j.fullkey,j.value,(SELECT account_id FROM conversation WHERE id=NEW.conversation_id),NEW.id,r.revision,s.id
 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'link_preview_json' AS column_name,NEW.link_preview_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'reply_to_json' AS column_name,NEW.reply_to_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'forward_from_json' AS column_name,NEW.forward_from_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'poll_json' AS column_name,NEW.poll_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'location_json' AS column_name,NEW.location_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'contact_card_json' AS column_name,NEW.contact_card_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'dice_json' AS column_name,NEW.dice_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'buttons_json' AS column_name,NEW.buttons_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'action_json' AS column_name,NEW.action_json AS value,1 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
 JOIN managed_ref_row r ON r.consumer_kind='message' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=j.value
 WHERE v.is_json=1 AND j.type='text' AND (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path')
 AND length(CAST(j.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(j.fullkey AS BLOB))<=4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST((SELECT account_id FROM conversation WHERE id=NEW.conversation_id) AS BLOB)),0)<=4096;
 UPDATE managed_ref_row SET state='blocked',reason='unindexable_path_payload'
 WHERE consumer_kind='message' AND consumer_key=CAST(NEW.id AS TEXT) AND (
 length(CAST(NEW.id AS BLOB))>4096 OR coalesce(length(CAST((SELECT account_id FROM conversation WHERE id=NEW.conversation_id) AS BLOB)),0)>4096
 OR EXISTS(SELECT 1 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'link_preview_json' AS column_name,NEW.link_preview_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'reply_to_json' AS column_name,NEW.reply_to_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'forward_from_json' AS column_name,NEW.forward_from_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'poll_json' AS column_name,NEW.poll_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'location_json' AS column_name,NEW.location_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'contact_card_json' AS column_name,NEW.contact_card_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'dice_json' AS column_name,NEW.dice_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'buttons_json' AS column_name,NEW.buttons_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'action_json' AS column_name,NEW.action_json AS value,1 AS is_json,1 AS extension) v WHERE
   (v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB))>4096) OR
   (v.is_json=1 AND v.value IS NOT NULL AND CASE WHEN length(CAST(v.value AS BLOB))>65536 THEN 1 WHEN json_valid(v.value) THEN 0 ELSE 1 END))
 OR EXISTS(SELECT 1 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'link_preview_json' AS column_name,NEW.link_preview_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'reply_to_json' AS column_name,NEW.reply_to_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'forward_from_json' AS column_name,NEW.forward_from_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'poll_json' AS column_name,NEW.poll_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'location_json' AS column_name,NEW.location_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'contact_card_json' AS column_name,NEW.contact_card_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'dice_json' AS column_name,NEW.dice_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'buttons_json' AS column_name,NEW.buttons_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'action_json' AS column_name,NEW.action_json AS value,1 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path') AND
     (j.type NOT IN ('text','null') OR length(CAST(j.value AS BLOB))>4096 OR length(CAST(j.fullkey AS BLOB))>4096 OR v.extension=1 OR (j.key!='path' AND j.key!='thumb_path') OR (v.column_name='media_json' AND j.path!='$') OR (v.column_name='link_preview_json' AND NOT ((j.path='$' AND j.key='thumb_path') OR (j.path='$.media' AND j.key IN ('path','thumb_path'))))))
 OR EXISTS(SELECT 1 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'link_preview_json' AS column_name,NEW.link_preview_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'reply_to_json' AS column_name,NEW.reply_to_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'forward_from_json' AS column_name,NEW.forward_from_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'poll_json' AS column_name,NEW.poll_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'location_json' AS column_name,NEW.location_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'contact_card_json' AS column_name,NEW.contact_card_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'dice_json' AS column_name,NEW.dice_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'buttons_json' AS column_name,NEW.buttons_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'action_json' AS column_name,NEW.action_json AS value,1 AS is_json,1 AS extension) v WHERE v.extension=1 AND v.is_json=0 AND v.value IS NOT NULL)
 OR EXISTS(SELECT 1 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'link_preview_json' AS column_name,NEW.link_preview_json AS value,1 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'reply_to_json' AS column_name,NEW.reply_to_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'forward_from_json' AS column_name,NEW.forward_from_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'poll_json' AS column_name,NEW.poll_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'location_json' AS column_name,NEW.location_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'contact_card_json' AS column_name,NEW.contact_card_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'dice_json' AS column_name,NEW.dice_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'buttons_json' AS column_name,NEW.buttons_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'action_json' AS column_name,NEW.action_json AS value,1 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE j.key IS NOT NULL GROUP BY v.column_name,j.parent,j.key HAVING count(*)>1));
 SELECT CASE WHEN EXISTS(SELECT 1 FROM managed_ref r JOIN managed_source s ON s.path=r.path
 WHERE r.consumer_kind='message' AND r.consumer_key=CAST(NEW.id AS TEXT) AND s.state!='retained')
 AND (OLD.media_json IS NOT NEW.media_json OR OLD.link_preview_json IS NOT NEW.link_preview_json OR OLD.ext_json IS NOT NEW.ext_json OR OLD.reply_to_json IS NOT NEW.reply_to_json OR OLD.forward_from_json IS NOT NEW.forward_from_json OR OLD.poll_json IS NOT NEW.poll_json OR OLD.location_json IS NOT NEW.location_json OR OLD.contact_card_json IS NOT NEW.contact_card_json OR OLD.dice_json IS NOT NEW.dice_json OR OLD.buttons_json IS NOT NEW.buttons_json OR OLD.action_json IS NOT NEW.action_json OR OLD.conversation_id IS NOT NEW.conversation_id OR OLD.id IS NOT NEW.id) THEN RAISE(ABORT,'managed source path is retired') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_message_delete AFTER DELETE ON message
BEGIN DELETE FROM managed_ref_row WHERE consumer_kind='message' AND consumer_key=CAST(OLD.id AS TEXT); END;
INSERT OR IGNORE INTO managed_ref_coverage(consumer_kind) VALUES ('message_version');

-- message_version: bounded extraction and dirty revision on every actual path write.
CREATE TRIGGER IF NOT EXISTS managed_message_version_insert AFTER INSERT ON message_version BEGIN
 UPDATE managed_ref_clock SET revision=revision+1 WHERE singleton=1;
 INSERT INTO managed_ref_row(consumer_kind,consumer_key,revision,state,reason)
 VALUES('message_version',CAST(NEW.id AS TEXT),(SELECT revision FROM managed_ref_clock WHERE singleton=1),'dirty',NULL)
 ON CONFLICT(consumer_kind,consumer_key) DO UPDATE SET revision=excluded.revision,state='dirty',reason=NULL;
 DELETE FROM managed_ref WHERE consumer_kind='message_version' AND consumer_key=CAST(NEW.id AS TEXT);
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'message_version',CAST(NEW.id AS TEXT),v.column_name,'$scalar',v.value,(SELECT c.account_id FROM message m JOIN conversation c ON c.id=m.conversation_id WHERE m.id=NEW.message_id),NEW.message_id,r.revision,s.id
 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension) v JOIN managed_ref_row r ON r.consumer_kind='message_version' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=v.value
 WHERE v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST((SELECT c.account_id FROM message m JOIN conversation c ON c.id=m.conversation_id WHERE m.id=NEW.message_id) AS BLOB)),0)<=4096;
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'message_version',CAST(NEW.id AS TEXT),v.column_name,j.fullkey,j.value,(SELECT c.account_id FROM message m JOIN conversation c ON c.id=m.conversation_id WHERE m.id=NEW.message_id),NEW.message_id,r.revision,s.id
 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
 JOIN managed_ref_row r ON r.consumer_kind='message_version' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=j.value
 WHERE v.is_json=1 AND j.type='text' AND (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path')
 AND length(CAST(j.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(j.fullkey AS BLOB))<=4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST((SELECT c.account_id FROM message m JOIN conversation c ON c.id=m.conversation_id WHERE m.id=NEW.message_id) AS BLOB)),0)<=4096;
 UPDATE managed_ref_row SET state='blocked',reason='unindexable_path_payload'
 WHERE consumer_kind='message_version' AND consumer_key=CAST(NEW.id AS TEXT) AND (
 length(CAST(NEW.id AS BLOB))>4096 OR coalesce(length(CAST((SELECT c.account_id FROM message m JOIN conversation c ON c.id=m.conversation_id WHERE m.id=NEW.message_id) AS BLOB)),0)>4096
 OR EXISTS(SELECT 1 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension) v WHERE
   (v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB))>4096) OR
   (v.is_json=1 AND v.value IS NOT NULL AND CASE WHEN length(CAST(v.value AS BLOB))>65536 THEN 1 WHEN json_valid(v.value) THEN 0 ELSE 1 END))
 OR EXISTS(SELECT 1 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path') AND
     (j.type NOT IN ('text','null') OR length(CAST(j.value AS BLOB))>4096 OR length(CAST(j.fullkey AS BLOB))>4096 OR v.extension=1 OR (j.key!='path' AND j.key!='thumb_path') OR (v.column_name='media_json' AND j.path!='$') OR (v.column_name='link_preview_json' AND NOT ((j.path='$' AND j.key='thumb_path') OR (j.path='$.media' AND j.key IN ('path','thumb_path'))))))
 OR EXISTS(SELECT 1 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension) v WHERE v.extension=1 AND v.is_json=0 AND v.value IS NOT NULL)
 OR EXISTS(SELECT 1 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE j.key IS NOT NULL GROUP BY v.column_name,j.parent,j.key HAVING count(*)>1));
 SELECT CASE WHEN EXISTS(SELECT 1 FROM managed_ref r JOIN managed_source s ON s.path=r.path
 WHERE r.consumer_kind='message_version' AND r.consumer_key=CAST(NEW.id AS TEXT) AND s.state!='retained')
 THEN RAISE(ABORT,'managed source path is retired') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_message_version_update AFTER UPDATE OF media_json,message_id,id ON message_version BEGIN
 DELETE FROM managed_ref_row WHERE consumer_kind='message_version' AND consumer_key=CAST(OLD.id AS TEXT) AND OLD.id IS NOT NEW.id;
 UPDATE managed_ref_clock SET revision=revision+1 WHERE singleton=1;
 INSERT INTO managed_ref_row(consumer_kind,consumer_key,revision,state,reason)
 VALUES('message_version',CAST(NEW.id AS TEXT),(SELECT revision FROM managed_ref_clock WHERE singleton=1),'dirty',NULL)
 ON CONFLICT(consumer_kind,consumer_key) DO UPDATE SET revision=excluded.revision,state='dirty',reason=NULL;
 DELETE FROM managed_ref WHERE consumer_kind='message_version' AND consumer_key=CAST(NEW.id AS TEXT);
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'message_version',CAST(NEW.id AS TEXT),v.column_name,'$scalar',v.value,(SELECT c.account_id FROM message m JOIN conversation c ON c.id=m.conversation_id WHERE m.id=NEW.message_id),NEW.message_id,r.revision,s.id
 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension) v JOIN managed_ref_row r ON r.consumer_kind='message_version' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=v.value
 WHERE v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST((SELECT c.account_id FROM message m JOIN conversation c ON c.id=m.conversation_id WHERE m.id=NEW.message_id) AS BLOB)),0)<=4096;
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'message_version',CAST(NEW.id AS TEXT),v.column_name,j.fullkey,j.value,(SELECT c.account_id FROM message m JOIN conversation c ON c.id=m.conversation_id WHERE m.id=NEW.message_id),NEW.message_id,r.revision,s.id
 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
 JOIN managed_ref_row r ON r.consumer_kind='message_version' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=j.value
 WHERE v.is_json=1 AND j.type='text' AND (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path')
 AND length(CAST(j.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(j.fullkey AS BLOB))<=4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST((SELECT c.account_id FROM message m JOIN conversation c ON c.id=m.conversation_id WHERE m.id=NEW.message_id) AS BLOB)),0)<=4096;
 UPDATE managed_ref_row SET state='blocked',reason='unindexable_path_payload'
 WHERE consumer_kind='message_version' AND consumer_key=CAST(NEW.id AS TEXT) AND (
 length(CAST(NEW.id AS BLOB))>4096 OR coalesce(length(CAST((SELECT c.account_id FROM message m JOIN conversation c ON c.id=m.conversation_id WHERE m.id=NEW.message_id) AS BLOB)),0)>4096
 OR EXISTS(SELECT 1 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension) v WHERE
   (v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB))>4096) OR
   (v.is_json=1 AND v.value IS NOT NULL AND CASE WHEN length(CAST(v.value AS BLOB))>65536 THEN 1 WHEN json_valid(v.value) THEN 0 ELSE 1 END))
 OR EXISTS(SELECT 1 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path') AND
     (j.type NOT IN ('text','null') OR length(CAST(j.value AS BLOB))>4096 OR length(CAST(j.fullkey AS BLOB))>4096 OR v.extension=1 OR (j.key!='path' AND j.key!='thumb_path') OR (v.column_name='media_json' AND j.path!='$') OR (v.column_name='link_preview_json' AND NOT ((j.path='$' AND j.key='thumb_path') OR (j.path='$.media' AND j.key IN ('path','thumb_path'))))))
 OR EXISTS(SELECT 1 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension) v WHERE v.extension=1 AND v.is_json=0 AND v.value IS NOT NULL)
 OR EXISTS(SELECT 1 FROM (SELECT 'media_json' AS column_name,NEW.media_json AS value,1 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE j.key IS NOT NULL GROUP BY v.column_name,j.parent,j.key HAVING count(*)>1));
 SELECT CASE WHEN EXISTS(SELECT 1 FROM managed_ref r JOIN managed_source s ON s.path=r.path
 WHERE r.consumer_kind='message_version' AND r.consumer_key=CAST(NEW.id AS TEXT) AND s.state!='retained')
 AND (OLD.media_json IS NOT NEW.media_json OR OLD.message_id IS NOT NEW.message_id OR OLD.id IS NOT NEW.id) THEN RAISE(ABORT,'managed source path is retired') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_message_version_delete AFTER DELETE ON message_version
BEGIN DELETE FROM managed_ref_row WHERE consumer_kind='message_version' AND consumer_key=CAST(OLD.id AS TEXT); END;
INSERT OR IGNORE INTO managed_ref_coverage(consumer_kind) VALUES ('outbox');

-- outbox: bounded extraction and dirty revision on every actual path write.
CREATE TRIGGER IF NOT EXISTS managed_outbox_insert AFTER INSERT ON outbox BEGIN
 UPDATE managed_ref_clock SET revision=revision+1 WHERE singleton=1;
 INSERT INTO managed_ref_row(consumer_kind,consumer_key,revision,state,reason)
 VALUES('outbox',CAST(NEW.id AS TEXT),(SELECT revision FROM managed_ref_clock WHERE singleton=1),'dirty',NULL)
 ON CONFLICT(consumer_kind,consumer_key) DO UPDATE SET revision=excluded.revision,state='dirty',reason=NULL;
 DELETE FROM managed_ref WHERE consumer_kind='outbox' AND consumer_key=CAST(NEW.id AS TEXT);
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'outbox',CAST(NEW.id AS TEXT),v.column_name,'$scalar',v.value,NEW.account_id,NEW.echo_message_id,r.revision,s.id
 FROM (SELECT 'params_json' AS column_name,NEW.params_json AS value,1 AS is_json,0 AS extension) v JOIN managed_ref_row r ON r.consumer_kind='outbox' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=v.value
 WHERE v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST(NEW.account_id AS BLOB)),0)<=4096;
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'outbox',CAST(NEW.id AS TEXT),v.column_name,j.fullkey,j.value,NEW.account_id,NEW.echo_message_id,r.revision,s.id
 FROM (SELECT 'params_json' AS column_name,NEW.params_json AS value,1 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
 JOIN managed_ref_row r ON r.consumer_kind='outbox' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=j.value
 WHERE v.is_json=1 AND j.type='text' AND (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path')
 AND length(CAST(j.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(j.fullkey AS BLOB))<=4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST(NEW.account_id AS BLOB)),0)<=4096;
 UPDATE managed_ref_row SET state='blocked',reason='unindexable_path_payload'
 WHERE consumer_kind='outbox' AND consumer_key=CAST(NEW.id AS TEXT) AND (
 length(CAST(NEW.id AS BLOB))>4096 OR coalesce(length(CAST(NEW.account_id AS BLOB)),0)>4096
 OR EXISTS(SELECT 1 FROM (SELECT 'params_json' AS column_name,NEW.params_json AS value,1 AS is_json,0 AS extension) v WHERE
   (v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB))>4096) OR
   (v.is_json=1 AND v.value IS NOT NULL AND CASE WHEN length(CAST(v.value AS BLOB))>65536 THEN 1 WHEN json_valid(v.value) THEN 0 ELSE 1 END))
 OR EXISTS(SELECT 1 FROM (SELECT 'params_json' AS column_name,NEW.params_json AS value,1 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path') AND
     (j.type NOT IN ('text','null') OR length(CAST(j.value AS BLOB))>4096 OR length(CAST(j.fullkey AS BLOB))>4096 OR v.extension=1 OR (j.key!='path' AND j.key!='thumb_path') OR (v.column_name='media_json' AND j.path!='$') OR (v.column_name='link_preview_json' AND NOT ((j.path='$' AND j.key='thumb_path') OR (j.path='$.media' AND j.key IN ('path','thumb_path'))))))
 OR EXISTS(SELECT 1 FROM (SELECT 'params_json' AS column_name,NEW.params_json AS value,1 AS is_json,0 AS extension) v WHERE v.extension=1 AND v.is_json=0 AND v.value IS NOT NULL)
 OR EXISTS(SELECT 1 FROM (SELECT 'params_json' AS column_name,NEW.params_json AS value,1 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE j.key IS NOT NULL GROUP BY v.column_name,j.parent,j.key HAVING count(*)>1));
 SELECT CASE WHEN EXISTS(SELECT 1 FROM managed_ref r JOIN managed_source s ON s.path=r.path
 WHERE r.consumer_kind='outbox' AND r.consumer_key=CAST(NEW.id AS TEXT) AND s.state!='retained')
 THEN RAISE(ABORT,'managed source path is retired') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_outbox_update AFTER UPDATE OF params_json,account_id,echo_message_id,method,id ON outbox BEGIN
 DELETE FROM managed_ref_row WHERE consumer_kind='outbox' AND consumer_key=CAST(OLD.id AS TEXT) AND OLD.id IS NOT NEW.id;
 UPDATE managed_ref_clock SET revision=revision+1 WHERE singleton=1;
 INSERT INTO managed_ref_row(consumer_kind,consumer_key,revision,state,reason)
 VALUES('outbox',CAST(NEW.id AS TEXT),(SELECT revision FROM managed_ref_clock WHERE singleton=1),'dirty',NULL)
 ON CONFLICT(consumer_kind,consumer_key) DO UPDATE SET revision=excluded.revision,state='dirty',reason=NULL;
 DELETE FROM managed_ref WHERE consumer_kind='outbox' AND consumer_key=CAST(NEW.id AS TEXT);
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'outbox',CAST(NEW.id AS TEXT),v.column_name,'$scalar',v.value,NEW.account_id,NEW.echo_message_id,r.revision,s.id
 FROM (SELECT 'params_json' AS column_name,NEW.params_json AS value,1 AS is_json,0 AS extension) v JOIN managed_ref_row r ON r.consumer_kind='outbox' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=v.value
 WHERE v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST(NEW.account_id AS BLOB)),0)<=4096;
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'outbox',CAST(NEW.id AS TEXT),v.column_name,j.fullkey,j.value,NEW.account_id,NEW.echo_message_id,r.revision,s.id
 FROM (SELECT 'params_json' AS column_name,NEW.params_json AS value,1 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
 JOIN managed_ref_row r ON r.consumer_kind='outbox' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=j.value
 WHERE v.is_json=1 AND j.type='text' AND (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path')
 AND length(CAST(j.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(j.fullkey AS BLOB))<=4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST(NEW.account_id AS BLOB)),0)<=4096;
 UPDATE managed_ref_row SET state='blocked',reason='unindexable_path_payload'
 WHERE consumer_kind='outbox' AND consumer_key=CAST(NEW.id AS TEXT) AND (
 length(CAST(NEW.id AS BLOB))>4096 OR coalesce(length(CAST(NEW.account_id AS BLOB)),0)>4096
 OR EXISTS(SELECT 1 FROM (SELECT 'params_json' AS column_name,NEW.params_json AS value,1 AS is_json,0 AS extension) v WHERE
   (v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB))>4096) OR
   (v.is_json=1 AND v.value IS NOT NULL AND CASE WHEN length(CAST(v.value AS BLOB))>65536 THEN 1 WHEN json_valid(v.value) THEN 0 ELSE 1 END))
 OR EXISTS(SELECT 1 FROM (SELECT 'params_json' AS column_name,NEW.params_json AS value,1 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path') AND
     (j.type NOT IN ('text','null') OR length(CAST(j.value AS BLOB))>4096 OR length(CAST(j.fullkey AS BLOB))>4096 OR v.extension=1 OR (j.key!='path' AND j.key!='thumb_path') OR (v.column_name='media_json' AND j.path!='$') OR (v.column_name='link_preview_json' AND NOT ((j.path='$' AND j.key='thumb_path') OR (j.path='$.media' AND j.key IN ('path','thumb_path'))))))
 OR EXISTS(SELECT 1 FROM (SELECT 'params_json' AS column_name,NEW.params_json AS value,1 AS is_json,0 AS extension) v WHERE v.extension=1 AND v.is_json=0 AND v.value IS NOT NULL)
 OR EXISTS(SELECT 1 FROM (SELECT 'params_json' AS column_name,NEW.params_json AS value,1 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE j.key IS NOT NULL GROUP BY v.column_name,j.parent,j.key HAVING count(*)>1));
 SELECT CASE WHEN EXISTS(SELECT 1 FROM managed_ref r JOIN managed_source s ON s.path=r.path
 WHERE r.consumer_kind='outbox' AND r.consumer_key=CAST(NEW.id AS TEXT) AND s.state!='retained')
 AND (OLD.params_json IS NOT NEW.params_json OR OLD.account_id IS NOT NEW.account_id OR OLD.method IS NOT NEW.method OR OLD.id IS NOT NEW.id) THEN RAISE(ABORT,'managed source path is retired') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_outbox_delete AFTER DELETE ON outbox
BEGIN DELETE FROM managed_ref_row WHERE consumer_kind='outbox' AND consumer_key=CAST(OLD.id AS TEXT); END;
INSERT OR IGNORE INTO managed_ref_coverage(consumer_kind) VALUES ('media_file');

-- media_file: bounded extraction and dirty revision on every actual path write.
CREATE TRIGGER IF NOT EXISTS managed_media_file_insert AFTER INSERT ON media_file BEGIN
 UPDATE managed_ref_clock SET revision=revision+1 WHERE singleton=1;
 INSERT INTO managed_ref_row(consumer_kind,consumer_key,revision,state,reason)
 VALUES('media_file',CAST(NEW.path AS TEXT),(SELECT revision FROM managed_ref_clock WHERE singleton=1),'dirty',NULL)
 ON CONFLICT(consumer_kind,consumer_key) DO UPDATE SET revision=excluded.revision,state='dirty',reason=NULL;
 DELETE FROM managed_ref WHERE consumer_kind='media_file' AND consumer_key=CAST(NEW.path AS TEXT);
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'media_file',CAST(NEW.path AS TEXT),v.column_name,'$scalar',v.value,NEW.account_id,NEW.message_id,r.revision,s.id
 FROM (SELECT 'path' AS column_name,NEW.path AS value,0 AS is_json,0 AS extension) v JOIN managed_ref_row r ON r.consumer_kind='media_file' AND r.consumer_key=CAST(NEW.path AS TEXT)
 LEFT JOIN managed_source s ON s.path=v.value
 WHERE v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(NEW.path AS BLOB))<=4096 AND coalesce(length(CAST(NEW.account_id AS BLOB)),0)<=4096;
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'media_file',CAST(NEW.path AS TEXT),v.column_name,j.fullkey,j.value,NEW.account_id,NEW.message_id,r.revision,s.id
 FROM (SELECT 'path' AS column_name,NEW.path AS value,0 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
 JOIN managed_ref_row r ON r.consumer_kind='media_file' AND r.consumer_key=CAST(NEW.path AS TEXT)
 LEFT JOIN managed_source s ON s.path=j.value
 WHERE v.is_json=1 AND j.type='text' AND (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path')
 AND length(CAST(j.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(j.fullkey AS BLOB))<=4096 AND length(CAST(NEW.path AS BLOB))<=4096 AND coalesce(length(CAST(NEW.account_id AS BLOB)),0)<=4096;
 UPDATE managed_ref_row SET state='blocked',reason='unindexable_path_payload'
 WHERE consumer_kind='media_file' AND consumer_key=CAST(NEW.path AS TEXT) AND (
 length(CAST(NEW.path AS BLOB))>4096 OR coalesce(length(CAST(NEW.account_id AS BLOB)),0)>4096
 OR EXISTS(SELECT 1 FROM (SELECT 'path' AS column_name,NEW.path AS value,0 AS is_json,0 AS extension) v WHERE
   (v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB))>4096) OR
   (v.is_json=1 AND v.value IS NOT NULL AND CASE WHEN length(CAST(v.value AS BLOB))>65536 THEN 1 WHEN json_valid(v.value) THEN 0 ELSE 1 END))
 OR EXISTS(SELECT 1 FROM (SELECT 'path' AS column_name,NEW.path AS value,0 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path') AND
     (j.type NOT IN ('text','null') OR length(CAST(j.value AS BLOB))>4096 OR length(CAST(j.fullkey AS BLOB))>4096 OR v.extension=1 OR (j.key!='path' AND j.key!='thumb_path') OR (v.column_name='media_json' AND j.path!='$') OR (v.column_name='link_preview_json' AND NOT ((j.path='$' AND j.key='thumb_path') OR (j.path='$.media' AND j.key IN ('path','thumb_path'))))))
 OR EXISTS(SELECT 1 FROM (SELECT 'path' AS column_name,NEW.path AS value,0 AS is_json,0 AS extension) v WHERE v.extension=1 AND v.is_json=0 AND v.value IS NOT NULL)
 OR EXISTS(SELECT 1 FROM (SELECT 'path' AS column_name,NEW.path AS value,0 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE j.key IS NOT NULL GROUP BY v.column_name,j.parent,j.key HAVING count(*)>1));
 SELECT CASE WHEN EXISTS(SELECT 1 FROM managed_ref r JOIN managed_source s ON s.path=r.path
 WHERE r.consumer_kind='media_file' AND r.consumer_key=CAST(NEW.path AS TEXT) AND s.state!='retained')
 THEN RAISE(ABORT,'managed source path is retired') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_media_file_update AFTER UPDATE OF path,account_id,message_id ON media_file BEGIN
 DELETE FROM managed_ref_row WHERE consumer_kind='media_file' AND consumer_key=CAST(OLD.path AS TEXT) AND OLD.path IS NOT NEW.path;
 UPDATE managed_ref_clock SET revision=revision+1 WHERE singleton=1;
 INSERT INTO managed_ref_row(consumer_kind,consumer_key,revision,state,reason)
 VALUES('media_file',CAST(NEW.path AS TEXT),(SELECT revision FROM managed_ref_clock WHERE singleton=1),'dirty',NULL)
 ON CONFLICT(consumer_kind,consumer_key) DO UPDATE SET revision=excluded.revision,state='dirty',reason=NULL;
 DELETE FROM managed_ref WHERE consumer_kind='media_file' AND consumer_key=CAST(NEW.path AS TEXT);
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'media_file',CAST(NEW.path AS TEXT),v.column_name,'$scalar',v.value,NEW.account_id,NEW.message_id,r.revision,s.id
 FROM (SELECT 'path' AS column_name,NEW.path AS value,0 AS is_json,0 AS extension) v JOIN managed_ref_row r ON r.consumer_kind='media_file' AND r.consumer_key=CAST(NEW.path AS TEXT)
 LEFT JOIN managed_source s ON s.path=v.value
 WHERE v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(NEW.path AS BLOB))<=4096 AND coalesce(length(CAST(NEW.account_id AS BLOB)),0)<=4096;
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'media_file',CAST(NEW.path AS TEXT),v.column_name,j.fullkey,j.value,NEW.account_id,NEW.message_id,r.revision,s.id
 FROM (SELECT 'path' AS column_name,NEW.path AS value,0 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
 JOIN managed_ref_row r ON r.consumer_kind='media_file' AND r.consumer_key=CAST(NEW.path AS TEXT)
 LEFT JOIN managed_source s ON s.path=j.value
 WHERE v.is_json=1 AND j.type='text' AND (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path')
 AND length(CAST(j.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(j.fullkey AS BLOB))<=4096 AND length(CAST(NEW.path AS BLOB))<=4096 AND coalesce(length(CAST(NEW.account_id AS BLOB)),0)<=4096;
 UPDATE managed_ref_row SET state='blocked',reason='unindexable_path_payload'
 WHERE consumer_kind='media_file' AND consumer_key=CAST(NEW.path AS TEXT) AND (
 length(CAST(NEW.path AS BLOB))>4096 OR coalesce(length(CAST(NEW.account_id AS BLOB)),0)>4096
 OR EXISTS(SELECT 1 FROM (SELECT 'path' AS column_name,NEW.path AS value,0 AS is_json,0 AS extension) v WHERE
   (v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB))>4096) OR
   (v.is_json=1 AND v.value IS NOT NULL AND CASE WHEN length(CAST(v.value AS BLOB))>65536 THEN 1 WHEN json_valid(v.value) THEN 0 ELSE 1 END))
 OR EXISTS(SELECT 1 FROM (SELECT 'path' AS column_name,NEW.path AS value,0 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path') AND
     (j.type NOT IN ('text','null') OR length(CAST(j.value AS BLOB))>4096 OR length(CAST(j.fullkey AS BLOB))>4096 OR v.extension=1 OR (j.key!='path' AND j.key!='thumb_path') OR (v.column_name='media_json' AND j.path!='$') OR (v.column_name='link_preview_json' AND NOT ((j.path='$' AND j.key='thumb_path') OR (j.path='$.media' AND j.key IN ('path','thumb_path'))))))
 OR EXISTS(SELECT 1 FROM (SELECT 'path' AS column_name,NEW.path AS value,0 AS is_json,0 AS extension) v WHERE v.extension=1 AND v.is_json=0 AND v.value IS NOT NULL)
 OR EXISTS(SELECT 1 FROM (SELECT 'path' AS column_name,NEW.path AS value,0 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE j.key IS NOT NULL GROUP BY v.column_name,j.parent,j.key HAVING count(*)>1));
 SELECT CASE WHEN EXISTS(SELECT 1 FROM managed_ref r JOIN managed_source s ON s.path=r.path
 WHERE r.consumer_kind='media_file' AND r.consumer_key=CAST(NEW.path AS TEXT) AND s.state!='retained')
 AND (OLD.path IS NOT NEW.path OR OLD.account_id IS NOT NEW.account_id OR OLD.message_id IS NOT NEW.message_id) THEN RAISE(ABORT,'managed source path is retired') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_media_file_delete AFTER DELETE ON media_file
BEGIN DELETE FROM managed_ref_row WHERE consumer_kind='media_file' AND consumer_key=CAST(OLD.path AS TEXT); END;
INSERT OR IGNORE INTO managed_ref_coverage(consumer_kind) VALUES ('conversation');

-- conversation: bounded extraction and dirty revision on every actual path write.
CREATE TRIGGER IF NOT EXISTS managed_conversation_insert AFTER INSERT ON conversation BEGIN
 UPDATE managed_ref_clock SET revision=revision+1 WHERE singleton=1;
 INSERT INTO managed_ref_row(consumer_kind,consumer_key,revision,state,reason)
 VALUES('conversation',CAST(NEW.id AS TEXT),(SELECT revision FROM managed_ref_clock WHERE singleton=1),'dirty',NULL)
 ON CONFLICT(consumer_kind,consumer_key) DO UPDATE SET revision=excluded.revision,state='dirty',reason=NULL;
 DELETE FROM managed_ref WHERE consumer_kind='conversation' AND consumer_key=CAST(NEW.id AS TEXT);
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'conversation',CAST(NEW.id AS TEXT),v.column_name,'$scalar',v.value,NEW.account_id,NULL,r.revision,s.id
 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'wallpaper' AS column_name,NEW.wallpaper AS value,0 AS is_json,1 AS extension) v JOIN managed_ref_row r ON r.consumer_kind='conversation' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=v.value
 WHERE v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST(NEW.account_id AS BLOB)),0)<=4096;
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'conversation',CAST(NEW.id AS TEXT),v.column_name,j.fullkey,j.value,NEW.account_id,NULL,r.revision,s.id
 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'wallpaper' AS column_name,NEW.wallpaper AS value,0 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
 JOIN managed_ref_row r ON r.consumer_kind='conversation' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=j.value
 WHERE v.is_json=1 AND j.type='text' AND (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path')
 AND length(CAST(j.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(j.fullkey AS BLOB))<=4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST(NEW.account_id AS BLOB)),0)<=4096;
 UPDATE managed_ref_row SET state='blocked',reason='unindexable_path_payload'
 WHERE consumer_kind='conversation' AND consumer_key=CAST(NEW.id AS TEXT) AND (
 length(CAST(NEW.id AS BLOB))>4096 OR coalesce(length(CAST(NEW.account_id AS BLOB)),0)>4096
 OR EXISTS(SELECT 1 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'wallpaper' AS column_name,NEW.wallpaper AS value,0 AS is_json,1 AS extension) v WHERE
   (v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB))>4096) OR
   (v.is_json=1 AND v.value IS NOT NULL AND CASE WHEN length(CAST(v.value AS BLOB))>65536 THEN 1 WHEN json_valid(v.value) THEN 0 ELSE 1 END))
 OR EXISTS(SELECT 1 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'wallpaper' AS column_name,NEW.wallpaper AS value,0 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path') AND
     (j.type NOT IN ('text','null') OR length(CAST(j.value AS BLOB))>4096 OR length(CAST(j.fullkey AS BLOB))>4096 OR v.extension=1 OR (j.key!='path' AND j.key!='thumb_path') OR (v.column_name='media_json' AND j.path!='$') OR (v.column_name='link_preview_json' AND NOT ((j.path='$' AND j.key='thumb_path') OR (j.path='$.media' AND j.key IN ('path','thumb_path'))))))
 OR EXISTS(SELECT 1 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'wallpaper' AS column_name,NEW.wallpaper AS value,0 AS is_json,1 AS extension) v WHERE v.extension=1 AND v.is_json=0 AND v.value IS NOT NULL)
 OR EXISTS(SELECT 1 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'wallpaper' AS column_name,NEW.wallpaper AS value,0 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE j.key IS NOT NULL GROUP BY v.column_name,j.parent,j.key HAVING count(*)>1));
 SELECT CASE WHEN EXISTS(SELECT 1 FROM managed_ref r JOIN managed_source s ON s.path=r.path
 WHERE r.consumer_kind='conversation' AND r.consumer_key=CAST(NEW.id AS TEXT) AND s.state!='retained')
 THEN RAISE(ABORT,'managed source path is retired') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_conversation_update AFTER UPDATE OF avatar_path,ext_json,wallpaper,account_id,id ON conversation BEGIN
 DELETE FROM managed_ref_row WHERE consumer_kind='conversation' AND consumer_key=CAST(OLD.id AS TEXT) AND OLD.id IS NOT NEW.id;
 UPDATE managed_ref_clock SET revision=revision+1 WHERE singleton=1;
 INSERT INTO managed_ref_row(consumer_kind,consumer_key,revision,state,reason)
 VALUES('conversation',CAST(NEW.id AS TEXT),(SELECT revision FROM managed_ref_clock WHERE singleton=1),'dirty',NULL)
 ON CONFLICT(consumer_kind,consumer_key) DO UPDATE SET revision=excluded.revision,state='dirty',reason=NULL;
 DELETE FROM managed_ref WHERE consumer_kind='conversation' AND consumer_key=CAST(NEW.id AS TEXT);
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'conversation',CAST(NEW.id AS TEXT),v.column_name,'$scalar',v.value,NEW.account_id,NULL,r.revision,s.id
 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'wallpaper' AS column_name,NEW.wallpaper AS value,0 AS is_json,1 AS extension) v JOIN managed_ref_row r ON r.consumer_kind='conversation' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=v.value
 WHERE v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST(NEW.account_id AS BLOB)),0)<=4096;
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'conversation',CAST(NEW.id AS TEXT),v.column_name,j.fullkey,j.value,NEW.account_id,NULL,r.revision,s.id
 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'wallpaper' AS column_name,NEW.wallpaper AS value,0 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
 JOIN managed_ref_row r ON r.consumer_kind='conversation' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=j.value
 WHERE v.is_json=1 AND j.type='text' AND (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path')
 AND length(CAST(j.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(j.fullkey AS BLOB))<=4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST(NEW.account_id AS BLOB)),0)<=4096;
 UPDATE managed_ref_row SET state='blocked',reason='unindexable_path_payload'
 WHERE consumer_kind='conversation' AND consumer_key=CAST(NEW.id AS TEXT) AND (
 length(CAST(NEW.id AS BLOB))>4096 OR coalesce(length(CAST(NEW.account_id AS BLOB)),0)>4096
 OR EXISTS(SELECT 1 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'wallpaper' AS column_name,NEW.wallpaper AS value,0 AS is_json,1 AS extension) v WHERE
   (v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB))>4096) OR
   (v.is_json=1 AND v.value IS NOT NULL AND CASE WHEN length(CAST(v.value AS BLOB))>65536 THEN 1 WHEN json_valid(v.value) THEN 0 ELSE 1 END))
 OR EXISTS(SELECT 1 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'wallpaper' AS column_name,NEW.wallpaper AS value,0 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path') AND
     (j.type NOT IN ('text','null') OR length(CAST(j.value AS BLOB))>4096 OR length(CAST(j.fullkey AS BLOB))>4096 OR v.extension=1 OR (j.key!='path' AND j.key!='thumb_path') OR (v.column_name='media_json' AND j.path!='$') OR (v.column_name='link_preview_json' AND NOT ((j.path='$' AND j.key='thumb_path') OR (j.path='$.media' AND j.key IN ('path','thumb_path'))))))
 OR EXISTS(SELECT 1 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'wallpaper' AS column_name,NEW.wallpaper AS value,0 AS is_json,1 AS extension) v WHERE v.extension=1 AND v.is_json=0 AND v.value IS NOT NULL)
 OR EXISTS(SELECT 1 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'wallpaper' AS column_name,NEW.wallpaper AS value,0 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE j.key IS NOT NULL GROUP BY v.column_name,j.parent,j.key HAVING count(*)>1));
 SELECT CASE WHEN EXISTS(SELECT 1 FROM managed_ref r JOIN managed_source s ON s.path=r.path
 WHERE r.consumer_kind='conversation' AND r.consumer_key=CAST(NEW.id AS TEXT) AND s.state!='retained')
 AND (OLD.avatar_path IS NOT NEW.avatar_path OR OLD.ext_json IS NOT NEW.ext_json OR OLD.wallpaper IS NOT NEW.wallpaper OR OLD.account_id IS NOT NEW.account_id OR OLD.id IS NOT NEW.id) THEN RAISE(ABORT,'managed source path is retired') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_conversation_delete AFTER DELETE ON conversation
BEGIN DELETE FROM managed_ref_row WHERE consumer_kind='conversation' AND consumer_key=CAST(OLD.id AS TEXT); END;
INSERT OR IGNORE INTO managed_ref_coverage(consumer_kind) VALUES ('platform_handle');

-- platform_handle: bounded extraction and dirty revision on every actual path write.
CREATE TRIGGER IF NOT EXISTS managed_platform_handle_insert AFTER INSERT ON platform_handle BEGIN
 UPDATE managed_ref_clock SET revision=revision+1 WHERE singleton=1;
 INSERT INTO managed_ref_row(consumer_kind,consumer_key,revision,state,reason)
 VALUES('platform_handle',CAST(NEW.id AS TEXT),(SELECT revision FROM managed_ref_clock WHERE singleton=1),'dirty',NULL)
 ON CONFLICT(consumer_kind,consumer_key) DO UPDATE SET revision=excluded.revision,state='dirty',reason=NULL;
 DELETE FROM managed_ref WHERE consumer_kind='platform_handle' AND consumer_key=CAST(NEW.id AS TEXT);
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'platform_handle',CAST(NEW.id AS TEXT),v.column_name,'$scalar',v.value,NEW.account_id,NULL,r.revision,s.id
 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'status_json' AS column_name,NEW.status_json AS value,1 AS is_json,1 AS extension) v JOIN managed_ref_row r ON r.consumer_kind='platform_handle' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=v.value
 WHERE v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST(NEW.account_id AS BLOB)),0)<=4096;
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'platform_handle',CAST(NEW.id AS TEXT),v.column_name,j.fullkey,j.value,NEW.account_id,NULL,r.revision,s.id
 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'status_json' AS column_name,NEW.status_json AS value,1 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
 JOIN managed_ref_row r ON r.consumer_kind='platform_handle' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=j.value
 WHERE v.is_json=1 AND j.type='text' AND (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path')
 AND length(CAST(j.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(j.fullkey AS BLOB))<=4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST(NEW.account_id AS BLOB)),0)<=4096;
 UPDATE managed_ref_row SET state='blocked',reason='unindexable_path_payload'
 WHERE consumer_kind='platform_handle' AND consumer_key=CAST(NEW.id AS TEXT) AND (
 length(CAST(NEW.id AS BLOB))>4096 OR coalesce(length(CAST(NEW.account_id AS BLOB)),0)>4096
 OR EXISTS(SELECT 1 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'status_json' AS column_name,NEW.status_json AS value,1 AS is_json,1 AS extension) v WHERE
   (v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB))>4096) OR
   (v.is_json=1 AND v.value IS NOT NULL AND CASE WHEN length(CAST(v.value AS BLOB))>65536 THEN 1 WHEN json_valid(v.value) THEN 0 ELSE 1 END))
 OR EXISTS(SELECT 1 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'status_json' AS column_name,NEW.status_json AS value,1 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path') AND
     (j.type NOT IN ('text','null') OR length(CAST(j.value AS BLOB))>4096 OR length(CAST(j.fullkey AS BLOB))>4096 OR v.extension=1 OR (j.key!='path' AND j.key!='thumb_path') OR (v.column_name='media_json' AND j.path!='$') OR (v.column_name='link_preview_json' AND NOT ((j.path='$' AND j.key='thumb_path') OR (j.path='$.media' AND j.key IN ('path','thumb_path'))))))
 OR EXISTS(SELECT 1 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'status_json' AS column_name,NEW.status_json AS value,1 AS is_json,1 AS extension) v WHERE v.extension=1 AND v.is_json=0 AND v.value IS NOT NULL)
 OR EXISTS(SELECT 1 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'status_json' AS column_name,NEW.status_json AS value,1 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE j.key IS NOT NULL GROUP BY v.column_name,j.parent,j.key HAVING count(*)>1));
 SELECT CASE WHEN EXISTS(SELECT 1 FROM managed_ref r JOIN managed_source s ON s.path=r.path
 WHERE r.consumer_kind='platform_handle' AND r.consumer_key=CAST(NEW.id AS TEXT) AND s.state!='retained')
 THEN RAISE(ABORT,'managed source path is retired') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_platform_handle_update AFTER UPDATE OF avatar_path,ext_json,status_json,account_id,id ON platform_handle BEGIN
 DELETE FROM managed_ref_row WHERE consumer_kind='platform_handle' AND consumer_key=CAST(OLD.id AS TEXT) AND OLD.id IS NOT NEW.id;
 UPDATE managed_ref_clock SET revision=revision+1 WHERE singleton=1;
 INSERT INTO managed_ref_row(consumer_kind,consumer_key,revision,state,reason)
 VALUES('platform_handle',CAST(NEW.id AS TEXT),(SELECT revision FROM managed_ref_clock WHERE singleton=1),'dirty',NULL)
 ON CONFLICT(consumer_kind,consumer_key) DO UPDATE SET revision=excluded.revision,state='dirty',reason=NULL;
 DELETE FROM managed_ref WHERE consumer_kind='platform_handle' AND consumer_key=CAST(NEW.id AS TEXT);
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'platform_handle',CAST(NEW.id AS TEXT),v.column_name,'$scalar',v.value,NEW.account_id,NULL,r.revision,s.id
 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'status_json' AS column_name,NEW.status_json AS value,1 AS is_json,1 AS extension) v JOIN managed_ref_row r ON r.consumer_kind='platform_handle' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=v.value
 WHERE v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST(NEW.account_id AS BLOB)),0)<=4096;
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'platform_handle',CAST(NEW.id AS TEXT),v.column_name,j.fullkey,j.value,NEW.account_id,NULL,r.revision,s.id
 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'status_json' AS column_name,NEW.status_json AS value,1 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
 JOIN managed_ref_row r ON r.consumer_kind='platform_handle' AND r.consumer_key=CAST(NEW.id AS TEXT)
 LEFT JOIN managed_source s ON s.path=j.value
 WHERE v.is_json=1 AND j.type='text' AND (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path')
 AND length(CAST(j.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(j.fullkey AS BLOB))<=4096 AND length(CAST(NEW.id AS BLOB))<=4096 AND coalesce(length(CAST(NEW.account_id AS BLOB)),0)<=4096;
 UPDATE managed_ref_row SET state='blocked',reason='unindexable_path_payload'
 WHERE consumer_kind='platform_handle' AND consumer_key=CAST(NEW.id AS TEXT) AND (
 length(CAST(NEW.id AS BLOB))>4096 OR coalesce(length(CAST(NEW.account_id AS BLOB)),0)>4096
 OR EXISTS(SELECT 1 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'status_json' AS column_name,NEW.status_json AS value,1 AS is_json,1 AS extension) v WHERE
   (v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB))>4096) OR
   (v.is_json=1 AND v.value IS NOT NULL AND CASE WHEN length(CAST(v.value AS BLOB))>65536 THEN 1 WHEN json_valid(v.value) THEN 0 ELSE 1 END))
 OR EXISTS(SELECT 1 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'status_json' AS column_name,NEW.status_json AS value,1 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path') AND
     (j.type NOT IN ('text','null') OR length(CAST(j.value AS BLOB))>4096 OR length(CAST(j.fullkey AS BLOB))>4096 OR v.extension=1 OR (j.key!='path' AND j.key!='thumb_path') OR (v.column_name='media_json' AND j.path!='$') OR (v.column_name='link_preview_json' AND NOT ((j.path='$' AND j.key='thumb_path') OR (j.path='$.media' AND j.key IN ('path','thumb_path'))))))
 OR EXISTS(SELECT 1 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'status_json' AS column_name,NEW.status_json AS value,1 AS is_json,1 AS extension) v WHERE v.extension=1 AND v.is_json=0 AND v.value IS NOT NULL)
 OR EXISTS(SELECT 1 FROM (SELECT 'avatar_path' AS column_name,NEW.avatar_path AS value,0 AS is_json,0 AS extension UNION ALL SELECT 'ext_json' AS column_name,NEW.ext_json AS value,1 AS is_json,1 AS extension UNION ALL SELECT 'status_json' AS column_name,NEW.status_json AS value,1 AS is_json,1 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE j.key IS NOT NULL GROUP BY v.column_name,j.parent,j.key HAVING count(*)>1));
 SELECT CASE WHEN EXISTS(SELECT 1 FROM managed_ref r JOIN managed_source s ON s.path=r.path
 WHERE r.consumer_kind='platform_handle' AND r.consumer_key=CAST(NEW.id AS TEXT) AND s.state!='retained')
 AND (OLD.avatar_path IS NOT NEW.avatar_path OR OLD.ext_json IS NOT NEW.ext_json OR OLD.status_json IS NOT NEW.status_json OR OLD.account_id IS NOT NEW.account_id OR OLD.id IS NOT NEW.id) THEN RAISE(ABORT,'managed source path is retired') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_platform_handle_delete AFTER DELETE ON platform_handle
BEGIN DELETE FROM managed_ref_row WHERE consumer_kind='platform_handle' AND consumer_key=CAST(OLD.id AS TEXT); END;
INSERT OR IGNORE INTO managed_ref_coverage(consumer_kind) VALUES ('plugin');

-- plugin: bounded extraction and dirty revision on every actual path write.
CREATE TRIGGER IF NOT EXISTS managed_plugin_insert AFTER INSERT ON plugin BEGIN
 UPDATE managed_ref_clock SET revision=revision+1 WHERE singleton=1;
 INSERT INTO managed_ref_row(consumer_kind,consumer_key,revision,state,reason)
 VALUES('plugin',CAST(NEW.plugin_id AS TEXT),(SELECT revision FROM managed_ref_clock WHERE singleton=1),'dirty',NULL)
 ON CONFLICT(consumer_kind,consumer_key) DO UPDATE SET revision=excluded.revision,state='dirty',reason=NULL;
 DELETE FROM managed_ref WHERE consumer_kind='plugin' AND consumer_key=CAST(NEW.plugin_id AS TEXT);
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'plugin',CAST(NEW.plugin_id AS TEXT),v.column_name,'$scalar',v.value,NULL,NULL,r.revision,s.id
 FROM (SELECT 'icon_path' AS column_name,NEW.icon_path AS value,0 AS is_json,0 AS extension) v JOIN managed_ref_row r ON r.consumer_kind='plugin' AND r.consumer_key=CAST(NEW.plugin_id AS TEXT)
 LEFT JOIN managed_source s ON s.path=v.value
 WHERE v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(NEW.plugin_id AS BLOB))<=4096 AND coalesce(length(CAST(NULL AS BLOB)),0)<=4096;
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'plugin',CAST(NEW.plugin_id AS TEXT),v.column_name,j.fullkey,j.value,NULL,NULL,r.revision,s.id
 FROM (SELECT 'icon_path' AS column_name,NEW.icon_path AS value,0 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
 JOIN managed_ref_row r ON r.consumer_kind='plugin' AND r.consumer_key=CAST(NEW.plugin_id AS TEXT)
 LEFT JOIN managed_source s ON s.path=j.value
 WHERE v.is_json=1 AND j.type='text' AND (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path')
 AND length(CAST(j.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(j.fullkey AS BLOB))<=4096 AND length(CAST(NEW.plugin_id AS BLOB))<=4096 AND coalesce(length(CAST(NULL AS BLOB)),0)<=4096;
 UPDATE managed_ref_row SET state='blocked',reason='unindexable_path_payload'
 WHERE consumer_kind='plugin' AND consumer_key=CAST(NEW.plugin_id AS TEXT) AND (
 length(CAST(NEW.plugin_id AS BLOB))>4096 OR coalesce(length(CAST(NULL AS BLOB)),0)>4096
 OR EXISTS(SELECT 1 FROM (SELECT 'icon_path' AS column_name,NEW.icon_path AS value,0 AS is_json,0 AS extension) v WHERE
   (v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB))>4096) OR
   (v.is_json=1 AND v.value IS NOT NULL AND CASE WHEN length(CAST(v.value AS BLOB))>65536 THEN 1 WHEN json_valid(v.value) THEN 0 ELSE 1 END))
 OR EXISTS(SELECT 1 FROM (SELECT 'icon_path' AS column_name,NEW.icon_path AS value,0 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path') AND
     (j.type NOT IN ('text','null') OR length(CAST(j.value AS BLOB))>4096 OR length(CAST(j.fullkey AS BLOB))>4096 OR v.extension=1 OR (j.key!='path' AND j.key!='thumb_path') OR (v.column_name='media_json' AND j.path!='$') OR (v.column_name='link_preview_json' AND NOT ((j.path='$' AND j.key='thumb_path') OR (j.path='$.media' AND j.key IN ('path','thumb_path'))))))
 OR EXISTS(SELECT 1 FROM (SELECT 'icon_path' AS column_name,NEW.icon_path AS value,0 AS is_json,0 AS extension) v WHERE v.extension=1 AND v.is_json=0 AND v.value IS NOT NULL)
 OR EXISTS(SELECT 1 FROM (SELECT 'icon_path' AS column_name,NEW.icon_path AS value,0 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE j.key IS NOT NULL GROUP BY v.column_name,j.parent,j.key HAVING count(*)>1));
 SELECT CASE WHEN EXISTS(SELECT 1 FROM managed_ref r JOIN managed_source s ON s.path=r.path
 WHERE r.consumer_kind='plugin' AND r.consumer_key=CAST(NEW.plugin_id AS TEXT) AND s.state!='retained')
 THEN RAISE(ABORT,'managed source path is retired') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_plugin_update AFTER UPDATE OF icon_path,plugin_id ON plugin BEGIN
 DELETE FROM managed_ref_row WHERE consumer_kind='plugin' AND consumer_key=CAST(OLD.plugin_id AS TEXT) AND OLD.plugin_id IS NOT NEW.plugin_id;
 UPDATE managed_ref_clock SET revision=revision+1 WHERE singleton=1;
 INSERT INTO managed_ref_row(consumer_kind,consumer_key,revision,state,reason)
 VALUES('plugin',CAST(NEW.plugin_id AS TEXT),(SELECT revision FROM managed_ref_clock WHERE singleton=1),'dirty',NULL)
 ON CONFLICT(consumer_kind,consumer_key) DO UPDATE SET revision=excluded.revision,state='dirty',reason=NULL;
 DELETE FROM managed_ref WHERE consumer_kind='plugin' AND consumer_key=CAST(NEW.plugin_id AS TEXT);
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'plugin',CAST(NEW.plugin_id AS TEXT),v.column_name,'$scalar',v.value,NULL,NULL,r.revision,s.id
 FROM (SELECT 'icon_path' AS column_name,NEW.icon_path AS value,0 AS is_json,0 AS extension) v JOIN managed_ref_row r ON r.consumer_kind='plugin' AND r.consumer_key=CAST(NEW.plugin_id AS TEXT)
 LEFT JOIN managed_source s ON s.path=v.value
 WHERE v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(NEW.plugin_id AS BLOB))<=4096 AND coalesce(length(CAST(NULL AS BLOB)),0)<=4096;
 INSERT OR IGNORE INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision,source_id)
 SELECT 'plugin',CAST(NEW.plugin_id AS TEXT),v.column_name,j.fullkey,j.value,NULL,NULL,r.revision,s.id
 FROM (SELECT 'icon_path' AS column_name,NEW.icon_path AS value,0 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
 JOIN managed_ref_row r ON r.consumer_kind='plugin' AND r.consumer_key=CAST(NEW.plugin_id AS TEXT)
 LEFT JOIN managed_source s ON s.path=j.value
 WHERE v.is_json=1 AND j.type='text' AND (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path')
 AND length(CAST(j.value AS BLOB)) BETWEEN 1 AND 4096 AND length(CAST(j.fullkey AS BLOB))<=4096 AND length(CAST(NEW.plugin_id AS BLOB))<=4096 AND coalesce(length(CAST(NULL AS BLOB)),0)<=4096;
 UPDATE managed_ref_row SET state='blocked',reason='unindexable_path_payload'
 WHERE consumer_kind='plugin' AND consumer_key=CAST(NEW.plugin_id AS TEXT) AND (
 length(CAST(NEW.plugin_id AS BLOB))>4096 OR coalesce(length(CAST(NULL AS BLOB)),0)>4096
 OR EXISTS(SELECT 1 FROM (SELECT 'icon_path' AS column_name,NEW.icon_path AS value,0 AS is_json,0 AS extension) v WHERE
   (v.is_json=0 AND v.value IS NOT NULL AND length(CAST(v.value AS BLOB))>4096) OR
   (v.is_json=1 AND v.value IS NOT NULL AND CASE WHEN length(CAST(v.value AS BLOB))>65536 THEN 1 WHEN json_valid(v.value) THEN 0 ELSE 1 END))
 OR EXISTS(SELECT 1 FROM (SELECT 'icon_path' AS column_name,NEW.icon_path AS value,0 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE (j.key='path' OR CAST(j.key AS TEXT) GLOB '*_path') AND
     (j.type NOT IN ('text','null') OR length(CAST(j.value AS BLOB))>4096 OR length(CAST(j.fullkey AS BLOB))>4096 OR v.extension=1 OR (j.key!='path' AND j.key!='thumb_path') OR (v.column_name='media_json' AND j.path!='$') OR (v.column_name='link_preview_json' AND NOT ((j.path='$' AND j.key='thumb_path') OR (j.path='$.media' AND j.key IN ('path','thumb_path'))))))
 OR EXISTS(SELECT 1 FROM (SELECT 'icon_path' AS column_name,NEW.icon_path AS value,0 AS is_json,0 AS extension) v WHERE v.extension=1 AND v.is_json=0 AND v.value IS NOT NULL)
 OR EXISTS(SELECT 1 FROM (SELECT 'icon_path' AS column_name,NEW.icon_path AS value,0 AS is_json,0 AS extension) v,json_tree(CASE WHEN v.is_json=0 THEN 'null' WHEN length(CAST(v.value AS BLOB))>65536 THEN 'null' WHEN json_valid(v.value) THEN v.value ELSE 'null' END) j
   WHERE j.key IS NOT NULL GROUP BY v.column_name,j.parent,j.key HAVING count(*)>1));
 SELECT CASE WHEN EXISTS(SELECT 1 FROM managed_ref r JOIN managed_source s ON s.path=r.path
 WHERE r.consumer_kind='plugin' AND r.consumer_key=CAST(NEW.plugin_id AS TEXT) AND s.state!='retained')
 AND (OLD.icon_path IS NOT NEW.icon_path OR OLD.plugin_id IS NOT NEW.plugin_id) THEN RAISE(ABORT,'managed source path is retired') END;
END;
CREATE TRIGGER IF NOT EXISTS managed_plugin_delete AFTER DELETE ON plugin
BEGIN DELETE FROM managed_ref_row WHERE consumer_kind='plugin' AND consumer_key=CAST(OLD.plugin_id AS TEXT); END;

-- Attempt and producer identities cannot be repurposed by retry/reset updates.
CREATE TRIGGER IF NOT EXISTS managed_dispatch_identity_immutable BEFORE UPDATE OF nonce,source_id,producer_id,outbox_id,client_id,method,params_json,source_fingerprint,source_release ON managed_dispatch
BEGIN SELECT RAISE(ABORT,'managed dispatch identity is immutable'); END;
CREATE TRIGGER IF NOT EXISTS managed_producer_identity_immutable BEFORE UPDATE OF nonce,core_nonce,platform,native_pid,process_start,host_scope,boot_id,holder_scope_json,proof_version ON managed_producer
BEGIN SELECT RAISE(ABORT,'managed producer identity is immutable'); END;
