//! Metadata-only source journal and bounded reverse-index invariants. Synthetic DBs/files only.

use ch_db::*;
use ch_protocol::dto::{LinkPreview, Media, MediaKind};
use rusqlite::params;
use serde_json::json;

const SOURCE: &str = "/synthetic/private-upload/bytes";
const STAGE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn seed(conn: &rusqlite::Connection) {
    conn.execute_batch("INSERT INTO plugin(plugin_id,name,first_seen_at,last_seen_at)
        VALUES('fixture','Fixture',0,0);
        INSERT INTO account(account_id,plugin_id,created_at,updated_at)
        VALUES('owner','fixture',0,0);
        INSERT INTO conversation(account_id,chat_id,kind,created_at,updated_at)
        VALUES('owner','chat','dm',0,0);").unwrap();
}

fn database() -> Db {
    let db = Db::open_in_memory().unwrap();
    seed(db.conn());
    db
}

fn source(conn: &rusqlite::Connection, path: &str) -> ManagedSourceId {
    // SQL fixtures are metadata only, never fake Core source/FD/retirement proofs.
    conn.execute("INSERT INTO managed_source(stage_id,client_id,account_id,plugin_id,
        account_created_at,chat_id,path,fingerprint_json,intent_json,created_at,updated_at)
        VALUES(?1,'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','owner','fixture',0,'chat',?2,'{}','{}',0,0)",
        params![STAGE,path]).unwrap();
    managed_source_id(conn, STAGE).unwrap().unwrap()
}

fn drain(db: &Db) -> ManagedCoverage {
    for _ in 0..4000 {
        let step = backfill_managed_references(db.conn()).unwrap();
        assert!(step.rows <= 128 && step.bytes <= 1_048_576);
        if step.coverage.complete && !step.coverage.dirty { return step.coverage; }
    }
    panic!("bounded backfill did not converge");
}

fn state(conn: &rusqlite::Connection, kind: &str, key: impl ToString) -> String {
    conn.query_row("SELECT state FROM managed_ref_row WHERE consumer_kind=?1 AND consumer_key=?2",
        params![kind,key.to_string()],|r|r.get(0)).unwrap()
}

fn message(path: &str) -> MessageRow {
    MessageRow {
        conversation_id: 1, message_id: "local:fixture".into(), direction: "out".into(),
        text: Some("caption".into()),
        media: Some(Media { kind: MediaKind::Document, path: Some(path.into()),
            filename: Some("original.bin".into()), bytes: Some(7), mime: None,
            width: None, height: None, duration: None, thumb_path: None,
            waveform: None, title: None, performer: None, has_spoiler: false,
            sticker: None }),
        ..Default::default()
    }
}

#[test]
fn adopted_message_versions_and_media_index_are_all_reverse_references() {
    let db = database();
    let source = source(db.conn(), SOURCE);
    let mut row = message(SOURCE);
    row.link_preview = Some(LinkPreview { thumb_path: Some("/ordinary/cache/thumb".into()), ..Default::default() });
    row.id = insert_message(db.conn(), &row).unwrap();
    let version = insert_message_version(db.conn(), &row, 1).unwrap();
    row.message_id = "remote-42".into();
    row.delivery = Some("read".into());
    update_message(db.conn(), &row).unwrap();
    upsert_media_file(db.conn(), &MediaFileRow { path: SOURCE.into(), account_id: "owner".into(),
        kind: "full".into(), message_id: Some(row.id), media_kind: Some("document".into()),
        bytes: Some(7), created_at: 0, last_access_at: 0 }).unwrap();
    let params = json!({"account_id":"owner","chat_id":"chat","path":SOURCE,"filename":"original.bin"});
    let outbox = insert_outbox(db.conn(), &OutboxRow { account_id:"owner".into(),chat_id:"chat".into(),
        client_id:"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),kind:"send_file".into(),
        method:"messages.send_file".into(),params:params.clone(),echo_message_id:Some(row.id),..Default::default() }).unwrap();
    set_outbox_sent(db.conn(), outbox, &["remote-42".into()]).unwrap();
    let refs = managed_references(db.conn(), source).unwrap();
    assert!(!refs.truncated);
    assert_eq!(refs.entries.len(), 4);
    assert!(refs.entries.iter().any(|r| r.consumer == RefConsumer::MessageVersion && r.key == version.to_string()));
    assert!(refs.entries.iter().any(|r| r.consumer == RefConsumer::MediaFile && r.message_id == Some(row.id)));
    assert!(refs.entries.iter().all(|r| r.account_id.as_deref() == Some("owner")));
    for (kind,key) in [("message",row.id),("message_version",version),("outbox",outbox)] {
        assert_eq!(state(db.conn(), kind, key), "clean");
    }
    let stored = get_message_by_id(db.conn(), row.id).unwrap().unwrap();
    assert_eq!(stored.message_id, "remote-42");
    assert_eq!(stored.delivery.as_deref(), Some("read"));
    assert_eq!(stored.media.unwrap().filename.as_deref(), Some("original.bin"));
    assert_eq!(get_outbox(db.conn(), outbox).unwrap().unwrap().params, params);
    assert!(managed_current_dispatch(db.conn(), outbox).unwrap().is_none());
}

#[test]
fn legacy_unknown_json_survives_backfill_and_direct_sql_stays_dirty() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("isolated.sqlite");
    let db = Db::open_unmigrated(&path).unwrap();
    db.migrate_to(4).unwrap();
    seed(db.conn());
    let raw = r#"{ "kind":"document", "path":"/synthetic/private-upload/bytes", "future":{"opaque":[1,2]} }"#;
    db.conn().execute("INSERT INTO message(conversation_id,message_id,direction,sent_at,created_at,updated_at,media_json)
        VALUES(1,'legacy','out',0,0,0,?1)",[raw]).unwrap();
    db.migrate_to(migrations::LATEST).unwrap();
    assert!(!managed_reference_coverage(db.conn()).unwrap().complete);
    let source = source(db.conn(), SOURCE);
    assert!(drain(&db).complete);
    let after: String = db.conn().query_row("SELECT media_json FROM message WHERE message_id='legacy'",[],|r|r.get(0)).unwrap();
    assert_eq!(after,raw);
    assert_eq!(managed_references(db.conn(),source).unwrap().entries.len(),1);
    // Another connection bypasses typed helpers, but not atomic dirty/index triggers.
    let second = Db::open(&path).unwrap();
    second.conn().execute("UPDATE message SET link_preview_json=?1 WHERE message_id='legacy'",
        [json!({"thumb_path":SOURCE}).to_string()]).unwrap();
    assert!(managed_reference_coverage(db.conn()).unwrap().dirty);
    assert_eq!(managed_references(db.conn(),source).unwrap().entries.len(),2);
    assert!(!drain(&db).dirty);
}

#[test]
fn invalid_duplicate_oversize_and_extension_paths_are_blocked_without_data_loss() {
    let db = database();
    let values = ["{broken".to_owned(),
        format!(r#"{{"path":"{SOURCE}","path":"/foreign"}}"#),
        json!({"path":SOURCE,"opaque":"x".repeat(65536)}).to_string(),
        json!({"kind":"document","future_path":SOURCE}).to_string(),
        json!({"future":{"path":SOURCE}}).to_string()];
    for (index,raw) in values.iter().enumerate() {
        db.conn().execute("INSERT INTO message(conversation_id,message_id,direction,sent_at,created_at,updated_at,media_json)
            VALUES(1,?1,'out',0,0,0,?2)",params![format!("invalid-{index}"),raw]).unwrap();
        let id = db.conn().last_insert_rowid();
        assert_eq!(state(db.conn(),"message",id),"blocked");
        let stored:String=db.conn().query_row("SELECT media_json FROM message WHERE id=?1",[id],|r|r.get(0)).unwrap();
        assert_eq!(&stored,raw);
    }
    // A plain text body containing a source spelling is not a path consumer.
    db.conn().execute("INSERT INTO message(conversation_id,message_id,direction,sent_at,created_at,updated_at,text)
        VALUES(1,'quoted','out',0,0,0,?1)",[SOURCE]).unwrap();
    let id = db.conn().last_insert_rowid();
    let count:i64=db.conn().query_row("SELECT count(*) FROM managed_ref WHERE consumer_kind='message' AND consumer_key=?1",[id.to_string()],|r|r.get(0)).unwrap();
    assert_eq!(count,0);
    assert!(drain(&db).blocked);
}

#[test]
fn thumbnail_slot_policy_uses_json_parent_and_key_not_fullkey_quoting() {
    let db = database();
    db.conn().execute(
        "INSERT INTO message(conversation_id,message_id,direction,sent_at,created_at,updated_at,media_json,link_preview_json)
         VALUES(1,'thumbnails','in',0,0,0,?1,?2)",
        params![json!({"kind":"photo","path":"/ordinary/full","thumb_path":"/ordinary/thumb"}).to_string(),
            json!({"thumb_path":"/ordinary/preview","media":{"path":"/ordinary/nested-full","thumb_path":"/ordinary/nested-thumb"}}).to_string()],
    ).unwrap();
    let id = db.conn().last_insert_rowid();
    let coverage = drain(&db);
    assert!(!coverage.blocked);
    assert_eq!(state(db.conn(), "message", id), "clean");
    let refs: i64 = db.conn().query_row(
        "SELECT count(*) FROM managed_ref WHERE consumer_kind='message' AND consumer_key=?1",
        [id.to_string()], |r| r.get(0),
    ).unwrap();
    assert_eq!(refs, 5);
    // An identically named field in an unknown parent remains unsupported.
    db.conn().execute("UPDATE message SET media_json=?1 WHERE id=?2",
        params![json!({"future":{"thumb_path":"/ordinary/unknown"}}).to_string(), id]).unwrap();
    assert_eq!(state(db.conn(), "message", id), "blocked");
}

#[test]
fn retired_literal_paths_reject_new_message_preview_album_and_scalar_refs() {
    let db = database();
    source(db.conn(),SOURCE);
    db.conn().execute("UPDATE managed_source SET state='retiring'",[]).unwrap();
    assert!(insert_message(db.conn(),&message(SOURCE)).is_err());
    for column in ["media_json","link_preview_json"] {
        assert!(db.conn().execute(&format!("INSERT INTO message(conversation_id,message_id,direction,sent_at,created_at,updated_at,{column})
            VALUES(1,'rejected','out',0,0,0,?1)"),[json!({"thumb_path":SOURCE}).to_string()]).is_err());
    }
    assert!(insert_outbox(db.conn(),&OutboxRow {account_id:"owner".into(),chat_id:"chat".into(),
        client_id:"album".into(),kind:"send_album".into(),method:"messages.send_album".into(),
        params:json!({"items":[{"path":SOURCE}]}),..Default::default()}).is_err());
    assert!(!set_handle_avatar(db.conn(),1,Some(SOURCE)).unwrap()); // missing row remains harmless
    assert!(db.conn().execute("INSERT INTO media_file(path,account_id,kind,created_at,last_access_at)
        VALUES(?1,'owner','full',0,0)",[SOURCE]).is_err());
    let count:i64=db.conn().query_row("SELECT count(*) FROM message",[],|r|r.get(0)).unwrap();
    assert_eq!(count,0); // savepoints did not leave echoes or partial index rows
}

#[test]
fn all_typed_path_writers_acknowledge_only_their_own_revision() {
    let db = database();
    let mut plugin = get_plugin(db.conn(),"fixture").unwrap().unwrap();
    plugin.icon_path=Some("/ordinary/icon.svg".into());
    upsert_plugin(db.conn(),&plugin).unwrap();
    let mut conversation=get_conversation_by_id(db.conn(),1).unwrap().unwrap();
    conversation.avatar_path=Some("/ordinary/chat-avatar".into());
    update_conversation(db.conn(),&conversation).unwrap();
    let id=insert_handle(db.conn(),&HandleRow {account_id:"owner".into(),user_id:"user".into(),
        avatar_path:Some("/ordinary/avatar".into()),..Default::default()}).unwrap();
    let mut handle=get_handle_by_id(db.conn(),id).unwrap().unwrap();
    handle.avatar_path=Some("/ordinary/avatar2".into());
    update_handle(db.conn(),&handle).unwrap();
    set_handle_avatar(db.conn(),id,Some("/ordinary/avatar3")).unwrap();
    for (kind,key) in [("plugin","fixture".to_owned()),("conversation","1".into()),("platform_handle",id.to_string())] {
        assert_eq!(state(db.conn(),kind,key),"clean");
    }
    db.conn().execute("UPDATE plugin SET icon_path='/ordinary/direct' WHERE plugin_id='fixture'",[]).unwrap();
    set_handle_avatar(db.conn(),id,None).unwrap();
    assert_eq!(state(db.conn(),"plugin","fixture"),"dirty");
    assert!(!drain(&db).dirty);
}

#[test]
fn rollback_and_cascades_keep_reference_records_consistent() {
    let db=database();
    let source=source(db.conn(),SOURCE);
    let id;
    { let tx=db.transaction().unwrap(); id=insert_message(&tx,&message(SOURCE)).unwrap(); }
    assert!(get_message_by_id(db.conn(),id).unwrap().is_none());
    assert!(managed_references(db.conn(),source).unwrap().entries.is_empty());
    let mut row=message(SOURCE);row.id=insert_message(db.conn(),&row).unwrap();
    insert_message_version(db.conn(),&row,1).unwrap();
    delete_conversation(db.conn(),1).unwrap();
    assert!(managed_references(db.conn(),source).unwrap().entries.is_empty());
}

#[test]
fn old_cursor_row_reuse_and_changed_behind_cursor_are_not_missed() {
    let db=database();
    let source=source(db.conn(),SOURCE);
    let id=insert_message(db.conn(),&message("/ordinary/cache")).unwrap();
    assert!(drain(&db).complete);
    let old_revision:i64=db.conn().query_row("SELECT revision FROM managed_ref_row WHERE consumer_kind='message' AND consumer_key=?1",[id.to_string()],|r|r.get(0)).unwrap();
    db.conn().execute("DELETE FROM message WHERE id=?1",[id]).unwrap();
    db.conn().execute("INSERT INTO message(id,conversation_id,message_id,direction,sent_at,created_at,updated_at,media_json)
        VALUES(?1,1,'replacement','out',0,0,0,?2)",params![id,json!({"path":SOURCE}).to_string()]).unwrap();
    assert!(managed_reference_coverage(db.conn()).unwrap().dirty);
    let new_revision:i64=db.conn().query_row("SELECT revision FROM managed_ref_row WHERE consumer_kind='message' AND consumer_key=?1",[id.to_string()],|r|r.get(0)).unwrap();
    assert!(new_revision>old_revision);
    assert_eq!(managed_references(db.conn(),source).unwrap().entries.len(),1);
    assert!(!drain(&db).dirty);
    db.conn().execute("UPDATE message SET media_json=NULL WHERE id=?1",[id]).unwrap();
    assert!(managed_references(db.conn(),source).unwrap().entries.is_empty());
    assert!(!drain(&db).dirty);
}

#[test]
fn hundred_thousand_old_messages_backfill_in_pages_and_source_lookup_uses_indices() {
    let tmp=tempfile::tempdir().unwrap();
    let db=Db::open_unmigrated(tmp.path().join("large.sqlite")).unwrap();
    db.migrate_to(4).unwrap();seed(db.conn());
    db.conn().execute_batch("WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000)
        INSERT INTO message(conversation_id,message_id,direction,sent_at,created_at,updated_at)
        SELECT 1,CAST(x AS TEXT),'in',x,0,0 FROM n;").unwrap();
    db.migrate_to(migrations::LATEST).unwrap();
    let source=source(db.conn(),SOURCE);
    let mut row=message(SOURCE);row.id=insert_message(db.conn(),&row).unwrap();
    insert_message_version(db.conn(),&row,1).unwrap();
    let mut pages=0;
    loop {
        let page=backfill_managed_references(db.conn()).unwrap();
        assert!(page.rows<=128 && page.bytes<=1_048_576);
        pages+=1;
        if pages==2 {
            // A write behind a persisted cursor is indexed even while backfill runs.
            db.conn().execute("UPDATE message SET media_json=?1 WHERE id=1",
                [json!({"path":"/ordinary/concurrent"}).to_string()]).unwrap();
        }
        if page.coverage.complete && !page.coverage.dirty {break;}
        assert!(pages<1000);
    }
    assert!(pages>700);
    assert_eq!(managed_references(db.conn(),source).unwrap().entries.len(),2);
    for (sql,index) in [("EXPLAIN QUERY PLAN SELECT * FROM managed_ref WHERE source_id=1","ix_managed_ref_source"),
        ("EXPLAIN QUERY PLAN SELECT * FROM managed_ref WHERE path='/synthetic'","ix_managed_ref_path")] {
        let plan=db.conn().prepare(sql).unwrap().query_map([],|r|r.get::<_,String>(3)).unwrap()
            .collect::<rusqlite::Result<Vec<_>>>().unwrap().join(" ");
        assert!(plan.contains(index),"{plan}");
    }
}

#[test]
fn actual_source_fanout_has_an_explicit_incomplete_result() {
    let db=database();let source=source(db.conn(),SOURCE);
    for index in 0..257 {
        let mut row=message(SOURCE);row.message_id=format!("shared-{index}");
        insert_message(db.conn(),&row).unwrap();
    }
    let result=managed_references(db.conn(),source).unwrap();
    assert_eq!(result.entries.len(),256);assert!(result.truncated);
}

#[cfg(unix)]
#[test]
fn literal_index_does_not_claim_hardlink_or_symlink_independence() {
    let tmp=tempfile::tempdir().unwrap();
    let original=tmp.path().join("bytes");let hard=tmp.path().join("hard");let link=tmp.path().join("link");
    std::fs::write(&original,b"synthetic").unwrap();std::fs::hard_link(&original,&hard).unwrap();
    std::os::unix::fs::symlink(&original,&link).unwrap();
    let db=database();let source=source(db.conn(),original.to_str().unwrap());
    for (index,path) in [&original,&hard,&link].into_iter().enumerate() {
        let mut row=message(path.to_str().unwrap());row.message_id=index.to_string();insert_message(db.conn(),&row).unwrap();
    }
    drain(&db);
    let unresolved:i64=db.conn().query_row("SELECT count(*) FROM managed_ref WHERE classification='unresolved'",[],|r|r.get(0)).unwrap();
    assert_eq!(unresolved,3);
    // This is a literal candidate set, not a no-alias certificate or cleanup API.
    assert_eq!(managed_references(db.conn(),source).unwrap().entries.len(),1);
    assert_eq!(std::fs::read(&hard).unwrap(),b"synthetic");
}

#[test]
fn migration_reapply_preserves_revision_and_coverage_and_failure_rolls_back() {
    let db=database();
    insert_message(db.conn(),&message("/ordinary")).unwrap();
    drain(&db);
    let before:i64=db.conn().query_row("SELECT revision FROM managed_ref_row WHERE consumer_kind='message'",[],|r|r.get(0)).unwrap();
    db.conn().execute_batch(migrations::MIGRATIONS.last().unwrap().sql).unwrap();
    assert!(managed_reference_coverage(db.conn()).unwrap().complete);
    let after:i64=db.conn().query_row("SELECT revision FROM managed_ref_row WHERE consumer_kind='message'",[],|r|r.get(0)).unwrap();
    assert_eq!(before,after);

    let tmp=tempfile::tempdir().unwrap();
    let broken=Db::open_unmigrated(tmp.path().join("broken.sqlite")).unwrap();
    broken.migrate_to(4).unwrap();
    broken.conn().execute("CREATE TABLE managed_dispatch(deliberate_collision TEXT)",[]).unwrap();
    assert!(broken.migrate_to(migrations::LATEST).is_err());
    assert_eq!(broken.schema_version().unwrap(),4);
    let created:i64=broken.conn().query_row("SELECT count(*) FROM sqlite_master WHERE name='managed_source'",[],|r|r.get(0)).unwrap();
    assert_eq!(created,0); // no partial source/journal foundation after failed migration
}

#[test]
fn dispatch_nonce_is_unique_immutable_and_survives_current_row_deletion() {
    let db=database();let source=source(db.conn(),SOURCE);
    let outbox=insert_outbox(db.conn(),&OutboxRow {account_id:"owner".into(),chat_id:"chat".into(),
        client_id:"request".into(),kind:"send_file".into(),method:"messages.send_file".into(),
        params:json!({"path":SOURCE}),..Default::default()}).unwrap();
    db.conn().execute("INSERT INTO managed_producer(nonce,core_nonce,platform)
        VALUES(?1,?2,'unprovable')",params!["c".repeat(32),"d".repeat(32)]).unwrap();
    let producer=db.conn().last_insert_rowid();
    let insert="INSERT INTO managed_dispatch(nonce,source_id,producer_id,outbox_id,client_id,
        method,params_json,source_fingerprint,created_at,updated_at)
        VALUES(?1,?2,?3,?4,'request','messages.send_file',?5,'unverified',0,0)";
    db.conn().execute(insert,params!["e".repeat(32),source.as_i64(),producer,outbox,json!({"path":SOURCE}).to_string()]).unwrap();
    let dispatch=db.conn().last_insert_rowid();
    assert!(db.conn().execute(insert,params!["e".repeat(32),source.as_i64(),producer,outbox,"{}"]).is_err());
    db.conn().execute("INSERT INTO managed_outbox_dispatch(outbox_id,dispatch_id) VALUES(?1,?2)",params![outbox,dispatch]).unwrap();
    assert_eq!(managed_current_dispatch(db.conn(),outbox).unwrap().unwrap().as_i64(),dispatch);
    db.conn().execute("UPDATE outbox SET attempts=0,state='queued' WHERE id=?1",[outbox]).unwrap();
    assert!(db.conn().execute("UPDATE managed_dispatch SET nonce=?1 WHERE id=?2",params!["f".repeat(32),dispatch]).is_err());
    // A nullable/missing promise cannot even populate a metadata-positive row.
    assert!(db.conn().execute("UPDATE managed_dispatch SET state='positive',result_ids_json='[\"42\"]',response_digest='not-proof' WHERE id=?1",[dispatch]).is_err());
    delete_outbox(db.conn(),outbox).unwrap();
    assert!(managed_current_dispatch(db.conn(),outbox).unwrap().is_none());
    let retained:i64=db.conn().query_row("SELECT count(*) FROM managed_dispatch WHERE id=?1 AND state='prepared'",[dispatch],|r|r.get(0)).unwrap();
    assert_eq!(retained,1); // missing current mapping never erases the old exposure record
}

#[test]
fn oversized_text_keys_advance_inside_sql_and_owner_hints_never_escape_read_bounds() {
    let tmp=tempfile::tempdir().unwrap();
    let db=Db::open_unmigrated(tmp.path().join("keys.sqlite")).unwrap();
    db.migrate_to(4).unwrap();seed(db.conn());
    let huge="x".repeat(8192);
    db.conn().execute("INSERT INTO plugin(plugin_id,name,first_seen_at,last_seen_at,icon_path)
        VALUES(?1,'Malformed',0,0,'/ordinary/icon')",[&huge]).unwrap();
    db.conn().execute("INSERT INTO media_file(path,account_id,kind,created_at,last_access_at)
        VALUES(?1,'owner','full',0,0)",[&huge]).unwrap();
    db.conn().execute("INSERT INTO plugin(plugin_id,name,first_seen_at,last_seen_at,icon_path)
        VALUES('z-last','Later',0,0,'/ordinary/later')",[]).unwrap();
    db.migrate_to(migrations::LATEST).unwrap();
    let source=source(db.conn(),SOURCE);
    assert!(drain(&db).blocked);
    assert_eq!(state(db.conn(),"plugin","z-last"),"clean");
    let bad_keys:i64=db.conn().query_row("SELECT count(*) FROM managed_ref_row WHERE state='blocked' AND length(CAST(consumer_key AS BLOB))>4096",[],|r|r.get(0)).unwrap();
    assert_eq!(bad_keys,2);
    db.conn().execute("INSERT INTO account(account_id,plugin_id,created_at,updated_at)
        VALUES(?1,'fixture',0,0)",[&huge]).unwrap();
    db.conn().execute("INSERT INTO conversation(account_id,chat_id,kind,created_at,updated_at)
        VALUES(?1,'large-owner','dm',0,0)",[&huge]).unwrap();
    let conversation=db.conn().last_insert_rowid();
    db.conn().execute("INSERT INTO message(conversation_id,message_id,direction,sent_at,created_at,updated_at,media_json)
        VALUES(?1,'large-hint','out',0,0,0,?2)",params![conversation,json!({"path":SOURCE}).to_string()]).unwrap();
    assert_eq!(state(db.conn(),"message",db.conn().last_insert_rowid()),"blocked");
    assert!(managed_references(db.conn(),source).unwrap().entries.is_empty());
}
