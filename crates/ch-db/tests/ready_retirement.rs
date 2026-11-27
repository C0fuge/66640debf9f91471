//! SQL metadata invariants only; none of these fixtures certifies byte absence,
//! physical reader closure, quota settlement or permission to delete real data.
use ch_db::{Db,migrations};
use rusqlite::{Connection,params};

const STAGE:&str="11111111111111111111111111111111";
const CLIENT:&str="22222222222222222222222222222222";
const BIRTH:&str="33333333333333333333333333333333";
const JOURNAL:&str="44444444444444444444444444444444";
const PATH:&str="/synthetic/accounts/owner/uploads/stage/bytes";

fn seed(conn:&Connection) {
    conn.execute_batch("INSERT INTO plugin(plugin_id,name,first_seen_at,last_seen_at) VALUES('fixture','Fixture',0,0);
        INSERT INTO account(account_id,plugin_id,created_at,updated_at) VALUES('owner','fixture',0,0);").unwrap();
    let identity=serde_json::json!({"manifest":{"sha256":"a".repeat(64)}}).to_string();
    conn.execute("INSERT INTO managed_stage_publication(stage_id,nonce,client_id,account_id,plugin_id,
        account_created_at,path,identity_json,created_at) VALUES(?1,?2,?3,'owner','fixture',0,?4,?5,0)",
        params![STAGE,BIRTH,CLIENT,PATH,identity]).unwrap();
}
fn journal(conn:&Connection)->rusqlite::Result<usize> {
    conn.execute("INSERT INTO managed_ready_retirement(stage_id,nonce,birth_nonce,client_id,account_id,
        plugin_id,account_created_at,path,identity_json,created_at,updated_at)
        SELECT stage_id,?1,nonce,client_id,account_id,plugin_id,account_created_at,path,identity_json,0,0
        FROM managed_stage_publication WHERE stage_id=?2",params![JOURNAL,STAGE])
}
fn source(conn:&Connection,stage:&str,client:&str,path:&str)->rusqlite::Result<usize> {
    conn.execute("INSERT INTO managed_source(stage_id,client_id,account_id,plugin_id,account_created_at,
        chat_id,path,fingerprint_json,intent_json,created_at,updated_at)
        VALUES(?1,?2,'owner','fixture',0,'chat',?3,'{}','{}',0,0)",params![stage,client,path])
}
fn receipt(conn:&Connection)->rusqlite::Result<usize> {
    conn.execute("INSERT INTO managed_ready_receipt(stage_id,birth_nonce,retirement_nonce,client_id,
        account_id,plugin_id,account_created_at,path,byte_sha256,identity_digest,created_at,settled_at)
        VALUES(?1,?2,?3,?4,'owner','fixture',0,?5,?6,?7,0,0)",
        params![STAGE,BIRTH,JOURNAL,CLIENT,PATH,"a".repeat(64),"b".repeat(64)])
}
fn advance(conn:&Connection) {
    for phase in ["marked","bytes_absent","stage_absent"] {
        conn.execute("UPDATE managed_ready_retirement SET phase=?1",[phase]).unwrap();
    }
}

#[test]
fn journal_binds_exact_unadmitted_birth_and_excludes_every_source_identity_collision() {
    let db=Db::open_in_memory().unwrap();let conn=db.conn();seed(conn);
    for (column,value) in [("birth_nonce","'ffffffffffffffffffffffffffffffff'"),
        ("plugin_id","'foreign'"),("account_created_at","1"),("identity_json","'{}'"),("phase","'marked'")] {
        let sql=format!("INSERT INTO managed_ready_retirement(stage_id,nonce,birth_nonce,client_id,account_id,
            plugin_id,account_created_at,path,identity_json,phase,created_at,updated_at)
            SELECT stage_id,'{JOURNAL}',{},client_id,account_id,{}, {},path,{}, {},0,0 FROM managed_stage_publication",
            if column=="birth_nonce" {value} else {"nonce"},
            if column=="plugin_id" {value} else {"plugin_id"},
            if column=="account_created_at" {value} else {"account_created_at"},
            if column=="identity_json" {value} else {"identity_json"},
            if column=="phase" {value} else {"'intent'"});
        assert!(conn.execute(&sql,[]).is_err(),"{column}");
    }
    journal(conn).unwrap();
    for (stage,client,path) in [(STAGE,"55555555555555555555555555555555","/synthetic/other/bytes"),
        ("55555555555555555555555555555555",CLIENT,"/synthetic/other/bytes"),
        ("55555555555555555555555555555555","66666666666666666666666666666666",PATH)] {
        assert!(source(conn,stage,client,path).is_err());
    }
    assert_eq!(conn.query_row("SELECT count(*) FROM managed_source",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    conn.execute("DELETE FROM managed_ready_retirement",[]).unwrap();
    source(conn,STAGE,CLIENT,PATH).unwrap();
    assert!(journal(conn).is_err(),"existing source prevents a new Ready journal");
    conn.execute("DELETE FROM managed_source",[]).unwrap();
    conn.execute("INSERT INTO outbox(account_id,chat_id,client_id,kind,method,params_json,state,created_at,updated_at)
        VALUES('owner','chat',?1,'send_file','messages.send_file','{}','failed',0,0)",[CLIENT]).unwrap();
    assert!(journal(conn).is_err(),"a canonical legacy receipt also prevents retirement");
}

#[test]
fn journal_identity_and_progress_are_immutable_or_monotonic_and_receipt_needs_matching_lineage() {
    let db=Db::open_in_memory().unwrap();let conn=db.conn();seed(conn);
    assert!(receipt(conn).is_err());journal(conn).unwrap();
    for sql in ["UPDATE managed_ready_retirement SET nonce=nonce",
        "UPDATE managed_ready_retirement SET identity_json='{}'",
        "UPDATE managed_ready_retirement SET phase='stage_absent'"] {
        assert!(conn.execute(sql,[]).is_err());
    }
    assert!(receipt(conn).is_err());
    for (old,next) in [("intent","marked"),("marked","bytes_absent"),("bytes_absent","stage_absent")] {
        conn.execute("UPDATE managed_ready_retirement SET phase=?1",[next]).unwrap();
        conn.execute("UPDATE managed_ready_retirement SET phase=?1",[next]).unwrap();
        assert!(conn.execute("UPDATE managed_ready_retirement SET phase=?1",[old]).is_err());
    }
    receipt(conn).unwrap();
    assert!(conn.execute("UPDATE managed_ready_receipt SET settled_at=1",[]).is_err());
    // The schema assertion is metadata only, never a physical cleanup result.
    let tx=db.transaction().unwrap();
    tx.execute("DELETE FROM managed_stage_publication",[]).unwrap();
    assert!(tx.commit().is_err(),"birth cannot silently vanish beneath its journal");
    let tx=db.transaction().unwrap();
    tx.execute("DELETE FROM managed_ready_retirement",[]).unwrap();
    tx.execute("DELETE FROM managed_stage_publication",[]).unwrap();tx.commit().unwrap();
    assert!(source(conn,STAGE,CLIENT,PATH).is_err(),"compact history vetoes old source reuse");
    assert!(conn.execute("INSERT INTO managed_stage_publication(stage_id,nonce,client_id,account_id,plugin_id,
        account_created_at,path,identity_json,created_at) VALUES(?1,?2,?3,'owner','fixture',0,?4,'{}',0)",
        params![STAGE,BIRTH,CLIENT,PATH]).is_err());
}

#[test]
fn ready_journal_upgrade_reapply_reopen_and_account_metadata_cascade_are_coherent() {
    let temp=tempfile::tempdir().unwrap();let path=temp.path().join("hub.db");
    let db=Db::open_unmigrated(&path).unwrap();db.migrate_to(6).unwrap();seed(db.conn());
    db.migrate_to(migrations::LATEST).unwrap();journal(db.conn()).unwrap();advance(db.conn());receipt(db.conn()).unwrap();
    let sql=migrations::MIGRATIONS.iter().find(|m|m.version==7).unwrap().sql;
    db.conn().execute_batch(sql).unwrap();db.conn().execute_batch(sql).unwrap();drop(db);
    let db=Db::open(&path).unwrap();
    assert_eq!(db.conn().query_row("SELECT phase FROM managed_ready_retirement",[],|r|r.get::<_,String>(0)).unwrap(),"stage_absent");
    assert!(db.integrity_ok().unwrap());
    db.conn().execute("DELETE FROM account WHERE account_id='owner'",[]).unwrap();
    for table in ["managed_stage_publication","managed_ready_retirement","managed_ready_receipt"] {
        assert_eq!(db.conn().query_row(&format!("SELECT count(*) FROM {table}"),[],|r|r.get::<_,i64>(0)).unwrap(),0);
    }
    assert!(db.integrity_ok().unwrap());
}

#[test]
fn failed_ready_journal_migration_rolls_back_all_new_objects_and_preserves_version_six() {
    let temp=tempfile::tempdir().unwrap();let db=Db::open_unmigrated(temp.path().join("broken.db")).unwrap();
    db.migrate_to(6).unwrap();seed(db.conn());
    db.conn().execute("CREATE TABLE managed_ready_receipt(deliberate_collision TEXT)",[]).unwrap();
    assert!(db.migrate_to(migrations::LATEST).is_err());assert_eq!(db.schema_version().unwrap(),6);
    assert_eq!(db.conn().query_row("SELECT count(*) FROM managed_stage_publication",[],|r|r.get::<_,i64>(0)).unwrap(),1);
    assert_eq!(db.conn().query_row("SELECT count(*) FROM sqlite_master WHERE name='managed_ready_retirement'",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    assert_eq!(db.conn().query_row("SELECT count(*) FROM sqlite_master WHERE name='managed_ready_receipt_excludes_birth'",[],|r|r.get::<_,i64>(0)).unwrap(),0);
}
