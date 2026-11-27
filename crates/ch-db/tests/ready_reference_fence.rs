//! Real SQLite metadata guards only. Seeded journals/receipts never certify
//! actual byte absence, reader closure, cleanup or quota settlement.
use ch_db::{Db,migrations};
use rusqlite::{Connection,params};

const STAGE:&str="11111111111111111111111111111111";
const CLIENT:&str="22222222222222222222222222222222";
const BIRTH:&str="33333333333333333333333333333333";
const JOURNAL:&str="44444444444444444444444444444444";
const PATH:&str="/synthetic/accounts/owner/uploads/stage/bytes";

fn seed(conn:&Connection) {
    conn.execute_batch("INSERT INTO plugin(plugin_id,name,first_seen_at,last_seen_at) VALUES('fixture','Fixture',0,0);
        INSERT INTO account(account_id,plugin_id,created_at,updated_at) VALUES('owner','fixture',0,0);
        INSERT INTO conversation(id,account_id,chat_id,kind,created_at,updated_at) VALUES(1,'owner','chat','group',0,0);
        INSERT INTO platform_handle(id,account_id,user_id,first_seen_at,last_seen_at) VALUES(1,'owner','contact',0,0);
        INSERT INTO message(id,conversation_id,message_id,direction,sent_at,media_json,created_at,updated_at)
        VALUES(1,1,'original','in',0,'{\"kind\":\"document\",\"path\":\"/outside/original\"}',0,0);").unwrap();
    conn.execute("INSERT INTO managed_stage_publication(stage_id,nonce,client_id,account_id,plugin_id,
        account_created_at,path,identity_json,created_at) VALUES(?1,?2,?3,'owner','fixture',0,?4,?5,0)",
        params![STAGE,BIRTH,CLIENT,PATH,serde_json::json!({"manifest":{"sha256":"a".repeat(64)}}).to_string()]).unwrap();
}
fn journal(conn:&Connection) {
    conn.execute("INSERT INTO managed_ready_retirement(stage_id,nonce,birth_nonce,client_id,account_id,
        plugin_id,account_created_at,path,identity_json,created_at,updated_at)
        SELECT stage_id,?1,nonce,client_id,account_id,plugin_id,account_created_at,path,identity_json,0,0
        FROM managed_stage_publication WHERE stage_id=?2",params![JOURNAL,STAGE]).unwrap();
}
fn snapshot(conn:&Connection)->(String,i64,String,i64) {
    conn.query_row("SELECT (SELECT media_json FROM message WHERE id=1),
        (SELECT revision FROM managed_ref_clock),
        (SELECT group_concat(path,'|') FROM (SELECT path FROM managed_ref ORDER BY path)),
        (SELECT count(*) FROM message)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap()
}
fn refused(conn:&Connection,sql:&str) {
    let before=snapshot(conn);
    let error=conn.execute(sql,[]).unwrap_err().to_string();
    assert!(error.contains("pending ready retirement fences"),"{sql}: {error}");
    assert_eq!(snapshot(conn),before,"consumer/ref deletion/revision must roll back: {sql}");
}

#[test]
fn every_pending_phase_fences_all_consumers_and_ambiguous_payloads_without_lifting_on_receipts() {
    let db=Db::open_in_memory().unwrap();let conn=db.conn();seed(conn);journal(conn);
    for phase in ["intent","marked","bytes_absent","stage_absent"] {
        conn.execute("UPDATE managed_ready_retirement SET phase=?1",[phase]).unwrap();
        for sql in [
            "UPDATE message SET media_json='{\"kind\":\"document\",\"path\":\"/synthetic/accounts/owner/uploads/stage/bytes\"}' WHERE id=1",
            "UPDATE message SET ext_json='{\"nested\":{\"alias_path\":\"/outside/unrelated\"}}' WHERE id=1",
            "UPDATE message SET media_json='{' WHERE id=1",
            "UPDATE message SET ext_json=printf('%.*c',65537,'x') WHERE id=1",
            "UPDATE conversation SET avatar_path='/outside/avatar' WHERE id=1",
            "UPDATE conversation SET wallpaper='/outside/background' WHERE id=1",
            "UPDATE platform_handle SET avatar_path='/outside/contact' WHERE id=1",
            "UPDATE platform_handle SET status_json='{\"path\":\"/outside/status\"}' WHERE id=1",
            "UPDATE plugin SET icon_path='/outside/icon' WHERE plugin_id='fixture'",
            "INSERT INTO message_version(message_id,media_json,replaced_at) VALUES(1,'{\"path\":\"/outside/history\"}',0)",
            "INSERT INTO media_file(path,account_id,kind,created_at,last_access_at) VALUES('/outside/index','owner','full',0,0)",
            "INSERT INTO outbox(account_id,chat_id,client_id,kind,method,params_json,state,created_at,updated_at)
             VALUES('owner','chat','55555555555555555555555555555555','send_file','messages.send_file','{\"path\":\"/outside/upload\"}','queued',0,0)",
            "UPDATE managed_ref SET classification='blocked' WHERE consumer_kind='message'",
            "INSERT INTO managed_ref(consumer_kind,consumer_key,column_name,slot,path,revision)
             SELECT 'message','1','ext_json','$.extra_path','/outside/direct',revision FROM managed_ref_row WHERE consumer_kind='message' AND consumer_key='1'",
            "UPDATE managed_ref_row SET state='blocked' WHERE consumer_kind='message' AND consumer_key='1'",
            "INSERT INTO managed_ref_row(consumer_kind,consumer_key,revision,state) VALUES('plugin','ambiguous',999,'blocked')",
        ] {refused(conn,sql);}
    }
    conn.execute("INSERT INTO managed_ready_receipt(stage_id,birth_nonce,retirement_nonce,client_id,account_id,
        plugin_id,account_created_at,path,byte_sha256,identity_digest,created_at,settled_at)
        VALUES(?1,?2,?3,?4,'owner','fixture',0,?5,?6,?7,0,0)",
        params![STAGE,BIRTH,JOURNAL,CLIENT,PATH,"a".repeat(64),"b".repeat(64)]).unwrap();
    refused(conn,"UPDATE plugin SET icon_path='/outside/receipt-is-not-release' WHERE plugin_id='fixture'");
    conn.execute("INSERT INTO message(id,conversation_id,message_id,direction,sent_at,text,created_at,updated_at)
        VALUES(2,1,'ordinary','in',0,'plain text',0,0)",[]).unwrap();
    conn.execute("UPDATE message SET text='still ordinary' WHERE id=2",[]).unwrap();
    // Removing a literal slot remains only negative evidence, not a release grant.
    conn.execute("UPDATE message SET media_json=NULL WHERE id=1",[]).unwrap();
    assert_eq!(conn.query_row("SELECT count(*) FROM managed_ref",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    conn.execute("DELETE FROM managed_ready_retirement",[]).unwrap();
    conn.execute("UPDATE plugin SET icon_path='/outside/after-metadata-fence' WHERE plugin_id='fixture'",[]).unwrap();
    assert!(db.integrity_ok().unwrap());
}

#[test]
fn a_real_other_sqlite_writer_cannot_add_a_reference_after_the_intent_commit() {
    use std::{sync::mpsc,time::Duration};
    let temp=tempfile::tempdir().unwrap();let path=temp.path().join("hub.db");
    let db=Db::open(&path).unwrap();seed(db.conn());let other=Db::open(&path).unwrap();
    let before=snapshot(db.conn());
    let tx=db.transaction().unwrap();journal(&tx);
    let (ready,started)=mpsc::channel();let (done,finished)=mpsc::channel();
    let writer=std::thread::spawn(move|| {
        ready.send(()).unwrap();
        let result=other.conn().execute("UPDATE message SET ext_json='{\"path\":\"/outside/racing-writer\"}' WHERE id=1",[]);
        done.send(result.map_err(|e|e.to_string())).unwrap();
    });
    started.recv_timeout(Duration::from_secs(5)).unwrap();tx.commit().unwrap();
    let error=finished.recv_timeout(Duration::from_secs(10)).unwrap().unwrap_err();writer.join().unwrap();
    assert!(error.contains("pending ready retirement fences"),"{error}");
    assert_eq!(snapshot(db.conn()),before);assert!(db.integrity_ok().unwrap());
}

#[test]
fn fence_upgrade_reapply_reopen_and_account_metadata_cascade_preserve_old_rows() {
    let temp=tempfile::tempdir().unwrap();let path=temp.path().join("hub.db");
    let db=Db::open_unmigrated(&path).unwrap();db.migrate_to(7).unwrap();seed(db.conn());journal(db.conn());
    let before=snapshot(db.conn());db.migrate_to(8).unwrap();
    let sql=migrations::MIGRATIONS.iter().find(|m|m.version==8).unwrap().sql;
    db.conn().execute_batch(sql).unwrap();db.conn().execute_batch(sql).unwrap();drop(db);
    let db=Db::open(&path).unwrap();assert_eq!(db.schema_version().unwrap(),8);
    assert_eq!(snapshot(db.conn()),before);
    refused(db.conn(),"UPDATE plugin SET icon_path='/outside/reopened' WHERE plugin_id='fixture'");
    db.conn().execute("DELETE FROM account WHERE account_id='owner'",[]).unwrap();
    for table in ["managed_stage_publication","managed_ready_retirement","managed_ready_receipt","managed_ref"] {
        assert_eq!(db.conn().query_row(&format!("SELECT count(*) FROM {table}"),[],|r|r.get::<_,i64>(0)).unwrap(),0);
    }
    db.conn().execute("UPDATE plugin SET icon_path='/outside/after-cascade' WHERE plugin_id='fixture'",[]).unwrap();
    assert!(db.integrity_ok().unwrap());
}

#[test]
fn failed_fence_migration_rolls_back_its_earlier_triggers_and_keeps_version_seven() {
    let temp=tempfile::tempdir().unwrap();
    let db=Db::open_unmigrated(temp.path().join("broken.db")).unwrap();db.migrate_to(7).unwrap();seed(db.conn());
    // Deliberately malformed synthetic old schema: first two trigger statements
    // can run, then creating the blocked-row trigger must fail atomically.
    db.conn().execute("ALTER TABLE managed_ref_row RENAME TO previous_ref_row",[]).unwrap();
    assert!(db.migrate_to(8).is_err());assert_eq!(db.schema_version().unwrap(),7);
    assert_eq!(db.conn().query_row("SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name GLOB 'managed_ready_ref_*'",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    assert_eq!(db.conn().query_row("SELECT count(*) FROM managed_stage_publication",[],|r|r.get::<_,i64>(0)).unwrap(),1);
    assert_eq!(db.conn().query_row("SELECT count(*) FROM managed_ref",[],|r|r.get::<_,i64>(0)).unwrap(),1);
}
