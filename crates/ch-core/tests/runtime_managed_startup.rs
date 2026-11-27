//! Until coherent managed recovery/Host guards exist, durable lifetime records
//! must stop startup before plugin discovery or autostart. Everything is synthetic.

mod host_support;

use std::sync::Arc;
use std::time::Duration;

use ch_core::host::MemorySecretStore;
use ch_core::runtime::{Core, CoreConfig, RuntimeError};
use ch_db::Db;
use host_support::Env;
use serde_json::json;

const NONCE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn config(env: &Env) -> CoreConfig {
    let mut config = CoreConfig::new(&env.data_dir, Arc::new(MemorySecretStore::new()));
    config.host = env.config();
    config
}

fn seed(env: &Env) -> Db {
    let db = Db::open(env.data_dir.join("hub.db")).unwrap();
    db.conn().execute_batch("INSERT INTO plugin(plugin_id,name,installed,first_seen_at,last_seen_at)
        VALUES('fixture','Fixture',0,0,0);
        INSERT INTO account(account_id,plugin_id,enabled,created_at,updated_at)
        VALUES('owner','fixture',1,0,0);").unwrap();
    db
}

async fn refused(env: &Env) {
    let error = Core::open(config(env)).await.unwrap_err();
    assert!(matches!(&error, RuntimeError::ManagedLifetimeRecoveryUnavailable));
    assert_eq!(error.to_string(), "managed lifetime recovery is not available in this build");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_source_state_stops_autostart_and_empty_store_still_runs() {
    let env = Env::new();
    let dir = env.plugin_dir("fixture", &["python3", "-I", "./startup.py", "--mode", "good"], json!({}));
    let fixture = serde_json::to_string(&host_support::misbehaving_script()).unwrap();
    std::fs::write(dir.join("startup.py"), format!(
        "from pathlib import Path\nimport runpy\nPath(__file__).with_name('spawned').write_text('spawned')\nrunpy.run_path({fixture}, run_name='__main__')\n"
    )).unwrap();
    let marker = dir.join("spawned");
    let db = seed(&env);
    for state in ["retained", "retiring", "released"] {
        // Released rows are tombstones too; none of these states permits legacy routing.
        db.conn().execute("INSERT INTO managed_source(stage_id,client_id,account_id,plugin_id,
            account_created_at,chat_id,path,fingerprint_json,intent_json,state,created_at,updated_at)
            VALUES(?1,?1,'owner','fixture',0,'chat','/synthetic/stage/bytes','{}','{}',?2,0,0)",
            [NONCE, state]).unwrap();
        refused(&env).await;
        assert!(!marker.exists());
        // Discovery itself must not run before the boundary, independent of task scheduling.
        let installed: bool = db.conn().query_row(
            "SELECT installed FROM plugin WHERE plugin_id='fixture'", [], |r| r.get(0)).unwrap();
        assert!(!installed);
        db.conn().execute("DELETE FROM managed_source", []).unwrap();
    }
    // Positive control: the same wanted account/entry really autostarts with no records.
    let core = Core::open(config(&env)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !marker.exists() { tokio::time::sleep(Duration::from_millis(10)).await; }
    }).await.unwrap();
    core.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphan_historical_attempts_and_retirement_journal_also_refuse() {
    let env = Env::new();
    let db = seed(&env);
    // Simulate a foreign-key-disabled/raw writer: no source, producer, outbox,
    // or current mapping survives. History is never inferred absent from pointers.
    db.conn().pragma_update(None, "foreign_keys", "OFF").unwrap();
    for state in ["unknown", "positive"] {
        db.conn().execute("INSERT INTO managed_dispatch(nonce,source_id,producer_id,outbox_id,
            client_id,method,params_json,source_fingerprint,source_release,state,
            result_ids_json,response_digest,created_at,updated_at)
            VALUES(?1,91,92,93,?1,'messages.send_file','{}','synthetic','on_success',?2,
                '[\"remote\"]','synthetic',0,0)", [NONCE, state]).unwrap();
        assert_eq!(db.conn().query_row("SELECT count(*) FROM managed_outbox_dispatch", [],
            |r| r.get::<_, i64>(0)).unwrap(), 0);
        refused(&env).await;
        db.conn().execute("DELETE FROM managed_dispatch", []).unwrap();
    }
    db.conn().execute("INSERT INTO managed_retirement(source_id,nonce,dispatch_id,
        evidence_json,phase,created_at,updated_at) VALUES(91,?1,92,'{}','detached',0,0)",
        [NONCE]).unwrap();
    refused(&env).await;
    db.conn().execute("DELETE FROM managed_retirement", []).unwrap();
    db.conn().pragma_update(None, "foreign_keys", "ON").unwrap();
    let core = Core::open(config(&env)).await.unwrap();
    core.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[cfg(target_os="linux")]
async fn unknown_initial_catalog_is_nonfatal_and_normal_autostart_still_runs() {
    let env=Env::new();
    let dir=env.plugin_dir("fixture",&["python3","-I","./startup.py","--mode","good"],json!({}));
    let peer=serde_json::to_string(&host_support::misbehaving_script()).unwrap();
    std::fs::write(dir.join("startup.py"),format!(
        "from pathlib import Path\nimport runpy\nPath(__file__).with_name('spawned').write_text('spawned')\nrunpy.run_path({peer},run_name='__main__')\n"
    )).unwrap();
    let uploads=env.data_dir.join("accounts/owner/uploads");std::fs::create_dir_all(&uploads).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&uploads,std::fs::Permissions::from_mode(0o700)).unwrap();
    let orphan=uploads.join("orphan");std::fs::write(&orphan,b"preserve unknown").unwrap();
    let _db=seed(&env);
    let core=Core::open(config(&env)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5),async {
        while !dir.join("spawned").exists() {tokio::time::sleep(Duration::from_millis(10)).await;}
    }).await.unwrap();
    assert!(matches!(tokio::time::timeout(Duration::from_secs(5),core.accounts().remove("owner")).await.unwrap(),
        Err(RuntimeError::SourceInUse)));
    assert!(core.accounts().get("owner").await.unwrap().is_some());
    assert_eq!(std::fs::read(orphan).unwrap(),b"preserve unknown");
    core.shutdown().await;
}
