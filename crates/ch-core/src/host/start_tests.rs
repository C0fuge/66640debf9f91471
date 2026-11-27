//! Real issuer observations with the actual Pending/resolve/failure machinery.
//! The bounded writer channel records frames; it never gives file or OS authority.
use super::*;
use crate::{host::{MemorySecretStore, NullSink}, runtime::{Core, CoreConfig, NewAccount}};

async fn fixture() -> (tempfile::TempDir, Core) {
    let temp = tempfile::tempdir().unwrap(); let plugin = temp.path().join("plugins/fixture");
    std::fs::create_dir_all(&plugin).unwrap();
    std::fs::write(plugin.join("plugin.json"), serde_json::json!({"id":"fixture","name":"Fixture",
        "version":"0.0.1","api_version":"1.2.0","entry":["python3","-I","-c","pass"],
        "brand_color":"#123456","multi_account":true,"auth_methods":["none"]}).to_string()).unwrap();
    let mut config = CoreConfig::new(temp.path(), Arc::new(MemorySecretStore::new())); config.autostart = false;
    let core = Core::open(config).await.unwrap();
    core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
    (temp, core)
}

fn connection() -> (Arc<Conn>, mpsc::Receiver<Out>) {
    let (writer, frames) = mpsc::channel(16);
    (Arc::new(Conn { plugin_id: "fixture".into(), pending: Mutex::default(), writer,
        ping_misses: AtomicU32::new(0), stalled: AtomicBool::new(false), initialized: AtomicBool::new(true) }), frames)
}

fn supervisor(core: &Core, registry: Arc<Registry>) -> Supervisor {
    let (_send, rx) = mpsc::channel(1);
    Supervisor { plugin_id: "fixture".into(), info: core.host().plugin("fixture").unwrap().info,
        config: Arc::new(core.host().config().clone()), registry, sink: Arc::new(NullSink),
        secrets: Arc::new(MemorySecretStore::new()), directories: None, rx, failures: 0, last_failure: String::new() }
}

fn finish(kind: &str, core: &Core, registry: &Arc<Registry>, conn: &Arc<Conn>, id: i64) {
    match kind {
        "success" => resolve(conn, registry, RequestId::Number(id), Ok(serde_json::json!({}))),
        "error" => resolve(conn, registry, RequestId::Number(id), Err(ErrorObject::new(ErrorCode::Internal, "synthetic refusal"))),
        "fail_all" => conn.fail_all(registry, "synthetic old process end"),
        "sweep" => {
            conn.pending().get_mut(&id).unwrap().deadline = Instant::now() - Duration::from_secs(1);
            let _ = supervisor(core, registry.clone()).sweep(conn);
        },
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn old_start_success_error_process_end_and_timeout_cannot_adopt_newer_record_or_create_missing_one() {
    for kind in ["success", "error", "fail_all", "sweep"] {
        let (_temp, core) = fixture().await;
        let registry = core.host().test_registry();
        let old = core.host().capture_start("fixture", "owner").await.unwrap();
        assert!(registry.set_start("fixture", "owner", AccountSpec::default(), &old));
        registry.update_start("fixture", "owner", &old, true, |record| { record.info.phase = AccountPhase::Starting; });
        let (conn, mut frames) = connection();
        let (id, result) = conn.request(PendingKind::AccountStart("owner".into(), old.clone()), "account.start",
            serde_json::json!({}), Duration::from_secs(1), None, &conn).unwrap();
        assert!(matches!(frames.try_recv(), Ok(Out::Frame(_))));
        let fresh = core.host().capture_start("fixture", "owner").await.unwrap();
        assert!(!old.same(&fresh)); // Even the same live owner needs exact queued request identity.
        assert!(registry.set_start("fixture", "owner", AccountSpec::default(), &fresh));
        registry.update_start("fixture", "owner", &fresh, true, |record| {
            record.info.phase = AccountPhase::Stopped; record.info.wanted = true; record.info.detail = Some("replacement".into());
        });
        let before = registry.lock().get("fixture").unwrap().accounts["owner"].info.clone();
        let mut changes = registry.changes.subscribe();
        finish(kind, &core, &registry, &conn, id);
        let _ = tokio::time::timeout(Duration::from_secs(5), result).await.unwrap().unwrap();
        assert_eq!(registry.lock().get("fixture").unwrap().accounts["owner"].info, before);
        assert!(matches!(changes.try_recv(), Err(broadcast::error::TryRecvError::Empty)));

        // A genuine remove revokes this exact observation. A stale completion
        // must not recreate even a missing local record from just the ID.
        let owner = old.owner.as_ref().unwrap().observed.clone();
        core.accounts().remove("owner").await.unwrap();
        core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
        core.open_db().unwrap().conn().execute("UPDATE account SET created_at=?1 WHERE account_id='owner'", [owner.owner().created_at]).unwrap();
        registry.lock().get_mut("fixture").unwrap().accounts.remove("owner");
        assert!(!old.live());
        let (id, result) = conn.request(PendingKind::AccountStart("owner".into(), old.clone()), "account.start",
            serde_json::json!({}), Duration::from_secs(1), None, &conn).unwrap();
        let mut changes = registry.changes.subscribe();
        finish(kind, &core, &registry, &conn, id);
        let _ = tokio::time::timeout(Duration::from_secs(5), result).await.unwrap().unwrap();
        assert!(!registry.lock().get("fixture").unwrap().accounts.contains_key("owner"));
        assert!(matches!(changes.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
        core.shutdown().await;
    }
}
