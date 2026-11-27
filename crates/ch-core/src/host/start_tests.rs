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
    config.host.timeout_scale = 0.02;
    let core = Core::open(config).await.unwrap();
    core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
    (temp, core)
}

fn connection(registry: &Arc<Registry>) -> (Arc<Conn>, mpsc::Receiver<Out>) {
    let (writer, frames) = mpsc::channel(16);
    (Arc::new(Conn { plugin_id: "fixture".into(), identity: Arc::new(()), registry: Arc::downgrade(registry), pending: Mutex::default(), writer,
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
        let (conn, mut frames) = connection(&registry);
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

        // This synthetic lifecycle exposure now correctly refuses deletion.
        // Its genuine controlled refusal/rearm still permanently revokes the
        // exact old observation without changing the persisted owner metadata.
        let owner = old.owner.as_ref().unwrap().observed.clone();
        assert!(matches!(core.accounts().remove("owner").await, Err(crate::runtime::RuntimeError::SourceInUse)));
        assert_eq!(core.accounts().get("owner").await.unwrap().unwrap().created_at, owner.owner().created_at);
        assert!(!old.live());
        let (id, result) = conn.request(PendingKind::AccountStart("owner".into(), old.clone()), "account.start",
            serde_json::json!({}), Duration::from_secs(1), None, &conn).unwrap();
        // Fault-inject missing local metadata AFTER the real synthetic frame
        // was queued. The stale callback must not manufacture a replacement.
        registry.lock().get_mut("fixture").unwrap().accounts.remove("owner");
        let mut changes = registry.changes.subscribe();
        finish(kind, &core, &registry, &conn, id);
        let _ = tokio::time::timeout(Duration::from_secs(5), result).await.unwrap().unwrap();
        assert!(!registry.lock().get("fixture").unwrap().accounts.contains_key("owner"));
        assert!(matches!(changes.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
        core.shutdown().await;
    }
}

async fn known_active() -> (tempfile::TempDir, Core, Arc<Registry>, Arc<Conn>, mpsc::Receiver<Out>) {
    let (temp, core) = fixture().await;
    let registry = core.host().test_registry();
    let scope = core.host().capture_start("fixture", "owner").await.unwrap();
    assert!(registry.set_start("fixture", "owner", AccountSpec::default(), &scope));
    let (conn, mut frames) = connection(&registry);
    let (id, reply) = conn.request(PendingKind::AccountStart("owner".into(), scope), "account.start",
        serde_json::json!({}), Duration::from_secs(1), None, &conn).unwrap();
    assert!(matches!(frames.try_recv(), Ok(Out::Frame(_))));
    resolve(&conn, &registry, RequestId::Number(id), Ok(serde_json::json!({})));
    reply.await.unwrap().unwrap();
    assert!(matches!(registry.removal_decision("fixture", "owner", Some(&conn.identity)), Decision::Dispatch));
    (temp, core, registry, conn, frames)
}

#[tokio::test]
async fn native_stop_queue_and_response_channel_loss_never_become_confirmation() {
    for lost_reply in [false, true] {
        let (_temp, core, registry, conn, frames) = known_active().await;
        let wait = super::super::removal::Wait::new();
        let (send, receive) = oneshot::channel();
        let mut frames = if lost_reply { Some(frames) } else { drop(frames); None };
        supervisor(&core, registry.clone()).handle_removal_stop(&conn, "owner".into(), wait.command(), send);
        if let Some(frames) = &mut frames {
            assert!(matches!(frames.try_recv(), Ok(Out::Frame(_))));
            // Lose the ACTUAL request sender, not just its native caller.
            let id = *conn.pending().keys().next().unwrap();
            drop(conn.pending().remove(&id));
        }
        assert_eq!(tokio::time::timeout(Duration::from_secs(5), receive).await.unwrap().unwrap(), RemovalStop::Refused);
        registry.update_account("fixture", "owner", |record| record.info.phase = AccountPhase::Stopped);
        assert!(matches!(registry.removal_decision("fixture", "owner", Some(&conn.identity)), Decision::Refuse));
        // A later positive attempt and healthy shutdown cannot erase this older unknown.
        let newer = core.host().capture_start("fixture", "owner").await.unwrap();
        assert!(registry.set_start("fixture", "owner", AccountSpec::default(), &newer));
        let (next, _frames) = connection(&registry);
        let (id, reply) = next.request(PendingKind::AccountStart("owner".into(), newer), "account.start",
            serde_json::json!({}), Duration::from_secs(1), None, &next).unwrap();
        resolve(&next, &registry, RequestId::Number(id), Ok(serde_json::json!({})));
        reply.await.unwrap().unwrap();
        registry.shutdown_confirmed("fixture", &next.identity);
        assert!(matches!(registry.removal_decision("fixture", "owner", Some(&next.identity)), Decision::Refuse));
        core.shutdown().await;
    }
}

#[tokio::test]
async fn dropped_or_timed_out_stop_waiter_preserves_queue_cancellation_and_actual_reply_observer() {
    for (dispatch, timed_out) in [(false, false), (false, true), (true, false), (true, true)] {
        let (_temp, core, registry, conn, mut frames) = known_active().await;
        let (commands, mut commands_rx) = mpsc::channel(1);
        registry.lock().get_mut("fixture").unwrap().cmd = Some(commands);
        let host = core.host().clone();
        let caller = tokio::spawn(async move { host.stop_for_removal("fixture", "owner").await });
        let command = tokio::time::timeout(Duration::from_secs(5), commands_rx.recv()).await.unwrap().unwrap();
        let Command::StopAccountForRemoval { account_id, queued, reply } = command else { panic!("expected native stop"); };
        let held_command = if dispatch {
            supervisor(&core, registry.clone()).handle_removal_stop(&conn, account_id, queued, reply);
            assert!(matches!(frames.try_recv(), Ok(Out::Frame(_))));
            assert!(matches!(registry.removal_decision("fixture", "owner", Some(&conn.identity)), Decision::Refuse));
            None
        } else { Some((account_id, queued, reply)) };
        if timed_out {
            assert_eq!(tokio::time::timeout(Duration::from_secs(5), caller).await.unwrap().unwrap(), RemovalStop::Refused);
        } else {
            caller.abort();
            assert!(tokio::time::timeout(Duration::from_secs(5), caller).await.unwrap().unwrap_err().is_cancelled());
        }
        if let Some((account, queued, reply)) = held_command {
            supervisor(&core, registry.clone()).handle_removal_stop(&conn, account, queued, reply);
            assert!(matches!(frames.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
            assert!(matches!(registry.removal_decision("fixture", "owner", Some(&conn.identity)), Decision::Dispatch));
        } else {
            // The actual pending request survives both kinds of caller loss.
            // An exact late positive ACK settles only its own logical attempt.
            let id = *conn.pending().keys().next().unwrap();
            resolve(&conn, &registry, RequestId::Number(id), Ok(serde_json::json!({})));
            tokio::time::timeout(Duration::from_secs(5), async {
                while !matches!(registry.removal_decision("fixture", "owner", None), Decision::Local) {
                    tokio::task::yield_now().await;
                }
            }).await.unwrap();
            assert!(conn.pending().is_empty());
        }
        assert!(core.accounts().get("owner").await.unwrap().is_some()); // Stop alone never deletes.
        registry.lock().get_mut("fixture").unwrap().cmd = None;
        core.shutdown().await;
    }
}
