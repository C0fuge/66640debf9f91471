//! Publication SQL invariants only. These metadata fixtures are never byte,
//! protected-birth, native-reader or cleanup certificates.
use ch_db::{Db,migrations};
use rusqlite::{Connection,params};

const STAGE:&str="11111111111111111111111111111111";
const CLIENT:&str="22222222222222222222222222222222";
const NONCE:&str="33333333333333333333333333333333";
const PATH:&str="/synthetic/accounts/owner/uploads/stage/bytes";

fn seed(conn:&Connection) {
    conn.execute_batch("INSERT INTO plugin(plugin_id,name,first_seen_at,last_seen_at)
        VALUES('fixture','Fixture',0,0);
        INSERT INTO account(account_id,plugin_id,created_at,updated_at) VALUES('owner','fixture',0,0);").unwrap();
}
fn source(conn:&Connection)->i64 {
    conn.execute("INSERT INTO managed_source(stage_id,client_id,account_id,plugin_id,account_created_at,
        chat_id,path,fingerprint_json,intent_json,created_at,updated_at)
        VALUES(?1,?2,'owner','fixture',0,'chat',?3,'{}','{}',0,0)",params![STAGE,CLIENT,PATH]).unwrap();
    conn.last_insert_rowid()
}
fn publication(conn:&Connection,source:Option<i64>)->rusqlite::Result<usize> {
    conn.execute("INSERT INTO managed_stage_publication(stage_id,nonce,client_id,account_id,plugin_id,
        account_created_at,path,identity_json,source_id,created_at)
        VALUES(?1,?2,?3,'owner','fixture',0,?4,'{}',?5,0)",params![STAGE,NONCE,CLIENT,PATH,source])
}

#[test]
fn publication_identity_and_established_source_link_cannot_be_rewritten_or_forgotten() {
    let db=Db::open_in_memory().unwrap();seed(db.conn());let source=source(db.conn());
    publication(db.conn(),None).unwrap();
    for sql in ["UPDATE managed_stage_publication SET nonce=nonce",
        "UPDATE managed_stage_publication SET identity_json='{}'",
        "UPDATE managed_stage_publication SET account_created_at=1"] {
        assert!(db.conn().execute(sql,[]).is_err());
    }
    db.conn().execute("UPDATE managed_stage_publication SET source_id=?1",[source]).unwrap();
    db.conn().execute("UPDATE managed_stage_publication SET source_id=?1",[source]).unwrap();
    assert!(db.conn().execute("UPDATE managed_stage_publication SET source_id=NULL",[]).is_err());
    assert!(db.conn().execute("UPDATE managed_stage_publication SET source_id=999",[]).is_err());
    let tx=db.transaction().unwrap();
    tx.execute("DELETE FROM managed_source WHERE id=?1",[source]).unwrap();
    assert!(tx.commit().is_err(),"source deletion cannot silently erase its established birth link");
    let tx=db.transaction().unwrap();
    tx.execute("DELETE FROM managed_stage_publication",[]).unwrap();
    tx.execute("DELETE FROM managed_source WHERE id=?1",[source]).unwrap();
    tx.commit().unwrap(); // Pure metadata transaction; no physical removal is asserted.
}

#[test]
fn insert_and_first_link_reject_foreign_or_retired_source_identity() {
    let db=Db::open_in_memory().unwrap();seed(db.conn());let id=source(db.conn());
    db.conn().execute("UPDATE managed_source SET state='released' WHERE id=?1",[id]).unwrap();
    assert!(publication(db.conn(),Some(id)).is_err());
    publication(db.conn(),None).unwrap();
    assert!(db.conn().execute("UPDATE managed_stage_publication SET source_id=?1",[id]).is_err());
    db.conn().execute("DELETE FROM managed_stage_publication",[]).unwrap();
    db.conn().execute("UPDATE managed_source SET state='retained' WHERE id=?1",[id]).unwrap();
    db.conn().execute("INSERT INTO managed_source(stage_id,client_id,account_id,plugin_id,account_created_at,
        chat_id,path,fingerprint_json,intent_json,created_at,updated_at)
        VALUES(?1,?2,'owner','fixture',0,'other',?3,'{}','{}',0,0)",
        params!["4".repeat(32),"5".repeat(32),"/synthetic/foreign/bytes"]).unwrap();
    let foreign=db.conn().last_insert_rowid();
    assert!(publication(db.conn(),Some(foreign)).is_err());
    publication(db.conn(),None).unwrap();
    assert!(db.conn().execute("UPDATE managed_stage_publication SET source_id=?1",[foreign]).is_err());
    db.conn().execute("UPDATE managed_stage_publication SET source_id=?1",[id]).unwrap();
}

#[test]
fn publication_owner_and_bounded_identity_are_checked_before_metadata_insertion() {
    let db=Db::open_in_memory().unwrap();seed(db.conn());
    let insert="INSERT INTO managed_stage_publication(stage_id,nonce,client_id,account_id,plugin_id,
        account_created_at,path,identity_json,created_at) VALUES(?1,?2,?3,'owner','fixture',?4,?5,?6,0)";
    let invalid:[(String,String,i64,String,String);6]=[
        ("bad".into(),NONCE.into(),0,PATH.into(),"{}".into()),
        (STAGE.into(),"A".repeat(32),0,PATH.into(),"{}".into()),
        (STAGE.into(),NONCE.into(),1,PATH.into(),"{}".into()),
        (STAGE.into(),NONCE.into(),0,"x".repeat(4097),"{}".into()),
        (STAGE.into(),NONCE.into(),0,PATH.into(),"[]".into()),
        (STAGE.into(),NONCE.into(),0,PATH.into(),serde_json::json!({"oversized":"x".repeat(65536)}).to_string()),
    ];
    for (stage,nonce,owner_birth,path,identity) in invalid {
        assert!(db.conn().execute(insert,params![stage,nonce,CLIENT,owner_birth,path,identity]).is_err());
    }
    assert_eq!(db.conn().query_row("SELECT count(*) FROM managed_stage_publication",[],|r|r.get::<_,i64>(0)).unwrap(),0);
}

#[test]
fn publication_upgrade_reapply_reopen_and_account_cascade_preserve_consistency() {
    let temp=tempfile::tempdir().unwrap();let path=temp.path().join("hub.db");
    let db=Db::open_unmigrated(&path).unwrap();db.migrate_to(5).unwrap();seed(db.conn());
    let source=source(db.conn());db.migrate_to(migrations::LATEST).unwrap();
    publication(db.conn(),Some(source)).unwrap();
    let sql=migrations::MIGRATIONS.iter().find(|m|m.version==6).unwrap().sql;
    db.conn().execute_batch(sql).unwrap();db.conn().execute_batch(sql).unwrap();drop(db);
    let db=Db::open(&path).unwrap();
    assert_eq!(db.conn().query_row("SELECT source_id FROM managed_stage_publication",[],|r|r.get::<_,i64>(0)).unwrap(),source);
    db.conn().execute("DELETE FROM account WHERE account_id='owner'",[]).unwrap();
    for table in ["managed_source","managed_stage_publication"] {
        assert_eq!(db.conn().query_row(&format!("SELECT count(*) FROM {table}"),[],|r|r.get::<_,i64>(0)).unwrap(),0);
    }
    assert!(db.integrity_ok().unwrap());
}

#[test]
fn failed_publication_migration_leaves_version_five_and_source_rows_intact() {
    let temp=tempfile::tempdir().unwrap();let db=Db::open_unmigrated(temp.path().join("broken.db")).unwrap();
    db.migrate_to(5).unwrap();seed(db.conn());source(db.conn());
    db.conn().execute("CREATE TABLE managed_stage_publication(deliberate_collision TEXT)",[]).unwrap();
    assert!(db.migrate_to(migrations::LATEST).is_err());assert_eq!(db.schema_version().unwrap(),5);
    assert_eq!(db.conn().query_row("SELECT count(*) FROM managed_source",[],|r|r.get::<_,i64>(0)).unwrap(),1);
    assert_eq!(db.conn().query_row("SELECT count(*) FROM sqlite_master WHERE name='ix_managed_stage_publication_owner'",[],|r|r.get::<_,i64>(0)).unwrap(),0);
}
