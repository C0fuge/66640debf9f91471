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
    (Arc::new(Conn { plugin_id: "fixture".into(), identity: Arc::new(()), registry: Arc::downgrade(registry), origins: None, pending: Mutex::default(), writer,
        ping_misses: AtomicU32::new(0), stalled: AtomicBool::new(false), initialized: AtomicBool::new(true) }), frames)
}

fn owned_connection(registry: &Arc<Registry>) -> (Arc<Conn>, mpsc::Receiver<Out>) {
    let (mut conn, frames) = connection(registry);
    Arc::get_mut(&mut conn).unwrap().origins = Some(super::super::origins::Origins::new());
    (conn, frames)
}

fn supervisor(core: &Core, registry: Arc<Registry>) -> Supervisor {
    let (_send, rx) = mpsc::channel(1);
    Supervisor { plugin_id: "fixture".into(), info: core.host().plugin("fixture").unwrap().info,
        config: Arc::new(core.host().config().clone()), registry, sink: Arc::new(NullSink),
        secrets: Arc::new(MemorySecretStore::new()), source_inputs: None, directories: None, owned_sink: None, rx, failures: 0, last_failure: String::new() }
}

#[tokio::test]
async fn actual_writer_boundary_rechecks_pending_quarantined_and_closed_source_admission() {
    let (_temp,core)=fixture().await;
    let registry=core.host().test_registry();
    let (conn,mut frames)=connection(&registry);
    let mut sup=supervisor(&core,registry);
    let sources=core.managed_sources();
    sup.source_inputs=Some(sources.owner_barriers());
    // Construct the frame before reserving, as with a previously queued Call.
    let params=serde_json::json!({"account_id":"owner","path":"/synthetic/input"});
    let frame=encode(&Request::new(17,"messages.send_file",&params)).unwrap();
    let observed=core.accounts().storage_binding_for("owner").await.unwrap().observation();
    let mut reservation=sources.reserve(observed.clone(),"pending-stage".into()).unwrap();
    assert!(sup.queue_call(&conn,"messages.send_file",&params,frame.clone(),None).is_err());
    assert!(matches!(frames.try_recv(),Err(mpsc::error::TryRecvError::Empty)));
    // Only a definite no-new-source result removes this pending barrier.
    reservation.no_new_source();drop(reservation);
    assert!(sup.queue_call(&conn,"messages.send_file",&params,frame.clone(),None).unwrap());
    assert!(matches!(frames.try_recv(),Ok(Out::Frame(_))));
    let mut reservation=sources.reserve(observed,"unknown-stage".into()).unwrap();
    reservation.quarantine(None);drop(reservation);
    assert!(sup.queue_call(&conn,"messages.send_file",&params,frame,None).is_err());
    assert!(matches!(frames.try_recv(),Err(mpsc::error::TryRecvError::Empty)));
    sources.close();
    let params=serde_json::json!({"photo_path":"/synthetic/other"});
    assert!(sup.queue_call(&conn,"profile.update",&params,vec![b'\n'],None).is_err());
    assert!(matches!(frames.try_recv(),Err(mpsc::error::TryRecvError::Empty)));
    core.shutdown().await;
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
async fn actual_external_frame_rejects_stale_catalog_and_original_native_run() {
    let (temp,core,registry,conn,mut frames)=known_active().await;
    let sources=core.managed_sources();
    let barriers=sources.owner_barriers();
    let mut sup=supervisor(&core,registry);
    sup.source_inputs=Some(barriers.clone());
    let observed=core.accounts().storage_binding_for("owner").await.unwrap().observation();
    let mut reservation=sources.reserve(observed,"pending-stage".into()).unwrap();
    let outside=temp.path().join("independent.bin");std::fs::write(&outside,b"outside").unwrap();
    let params=serde_json::json!({"account_id":"owner","path":outside});
    let run=core.host().running_account_token("fixture","owner").unwrap();
    let proof=barriers.prepare_generic(&sources.lookups(),"messages.send_file",&params).await.unwrap().unwrap();
    let input=super::super::inputs::InputScope::new(proof,run.clone());
    let frame=encode(&Request::new(23,"messages.send_file",&params)).unwrap();
    assert!(sup.queue_call(&conn,"messages.send_file",&params,frame.clone(),Some(&input)).unwrap());
    assert!(matches!(frames.try_recv(),Ok(Out::Frame(_))));
    let writer=core.stage_mutations().begin().unwrap();
    assert!(sup.queue_call(&conn,"messages.send_file",&params,frame.clone(),Some(&input)).is_err());
    assert!(matches!(frames.try_recv(),Err(mpsc::error::TryRecvError::Empty)));
    drop(writer);
    let proof=barriers.prepare_generic(&sources.lookups(),"messages.send_file",&params).await.unwrap().unwrap();
    let input=super::super::inputs::InputScope::new(proof,run);
    core.host().revoke_account_run("fixture","owner");
    assert!(sup.queue_call(&conn,"messages.send_file",&params,frame,Some(&input)).is_err());
    assert!(matches!(frames.try_recv(),Err(mpsc::error::TryRecvError::Empty)));
    reservation.no_new_source();drop(reservation);
    core.shutdown().await;
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

#[tokio::test]
async fn original_status_and_queued_delivery_survive_stop_only_as_the_old_logical_origin() {
    use crate::host::{OwnedEventSink, OwnedStateChange};
    let (_temp, core) = fixture().await;
    let registry = core.host().test_registry();
    let scope = core.host().capture_start("fixture", "owner").await.unwrap();
    let original = scope.origin().unwrap().clone();
    assert!(registry.set_start("fixture", "owner", AccountSpec::default(), &scope));
    let (conn, mut frames) = owned_connection(&registry);
    let (id, response) = conn.request(PendingKind::AccountStart("owner".into(), scope), "account.start",
        serde_json::json!({}), Duration::from_secs(1), None, &conn).unwrap();
    assert!(matches!(frames.try_recv(), Ok(Out::Frame(_))));
    resolve(&conn, &registry, RequestId::Number(id), Ok(serde_json::json!({})));
    tokio::time::timeout(Duration::from_secs(5), response).await.unwrap().unwrap().unwrap();
    let mut changes = core.host().subscribe_owned();
    let (send, mut events) = mpsc::channel(1);
    let secrets: Arc<dyn SecretStore> = Arc::new(MemorySecretStore::new());
    handle_incoming(&conn, &registry, &secrets, &send, None, "status", Some(serde_json::json!({
        "account_id":"owner","state":"connected","detail":"old-detail","reconnecting":false,
        "me":{"id":"old-me","display_name":"Synthetic"}
    }))).await;
    let delivery = tokio::time::timeout(Duration::from_secs(5), events.recv()).await.unwrap().unwrap();
    assert!(delivery.origin.as_ref().unwrap().same(&original));
    let change = tokio::time::timeout(Duration::from_secs(5), changes.recv()).await.unwrap().unwrap();
    let OwnedStateChange::Account(snapshot) = change else { panic!("expected original account snapshot"); };
    assert!(snapshot.origin().same(&original));
    assert_eq!(snapshot.info().me.as_ref().unwrap().id, "old-me");
    assert_eq!(core.host().account_for_origin(&original).unwrap().info().detail.as_deref(), Some("old-detail"));
    // Frozen Empty permits extension object fields. The exact real Pending ID
    // still has to be resolved before the producer interval can be reused.
    let (stop, response) = conn.request(PendingKind::AccountStop("owner".into()), "account.stop",
        serde_json::json!({"account_id":"owner"}), Duration::from_secs(1), None, &conn).unwrap();
    assert!(conn.origins.as_ref().unwrap().freeze("owner").is_some());
    resolve(&conn, &registry, RequestId::Number(stop), Ok(serde_json::json!({"future":true})));
    tokio::time::timeout(Duration::from_secs(5), response).await.unwrap().unwrap().unwrap();
    assert!(conn.origins.as_ref().unwrap().freeze("owner").is_none());
    let created = core.accounts().get("owner").await.unwrap().unwrap().created_at;
    core.accounts().remove("owner").await.unwrap();
    core.open_db().unwrap().conn().execute_batch(&format!("CREATE TRIGGER equal_birth_owner AFTER INSERT ON account
        WHEN NEW.account_id='owner' BEGIN UPDATE account SET created_at={created} WHERE account_id=NEW.account_id; END;")).unwrap();
    core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
    core.open_db().unwrap().conn().execute_batch("DROP TRIGGER equal_birth_owner").unwrap();
    assert!(original.is_revoked());
    assert!(core.host().accounts_owned().is_empty());
    assert!(core.host().accounts().is_empty());
    assert!(core.host().plugin("fixture").unwrap().accounts.is_empty());
    assert!(core.host().plugins().iter().all(|plugin| plugin.accounts.is_empty()));
    assert!(core.host().account("fixture", "owner").is_none());
    assert!(core.host().account_for_origin(&original).is_none());
    let new_scope = core.host().capture_start("fixture", "owner").await.unwrap();
    let new_origin = new_scope.origin().unwrap().clone();
    assert!(!new_origin.same(&original));
    assert!(registry.set_start("fixture", "owner", AccountSpec::default(), &new_scope));
    let (start, response) = conn.request(PendingKind::AccountStart("owner".into(), new_scope), "account.start",
        serde_json::json!({}), Duration::from_secs(1), None, &conn).unwrap();
    resolve(&conn, &registry, RequestId::Number(start), Ok(serde_json::json!({})));
    tokio::time::timeout(Duration::from_secs(5), response).await.unwrap().unwrap().unwrap();
    assert!(conn.origins.as_ref().unwrap().freeze("owner").unwrap().same(&new_origin));
    assert!(delivery.origin.as_ref().unwrap().same(&original)); // Never relabel at dequeue.
    core.ingest().deliver_owned("fixture", delivery.origin.unwrap(), delivery.event).await.unwrap();
    let current = core.accounts().get("owner").await.unwrap().unwrap();
    assert!(current.me_user_id.is_none()); assert!(current.state_detail.is_none());
    let handles: i64 = core.open_db().unwrap().conn().query_row("SELECT COUNT(*) FROM platform_handle WHERE account_id='owner'", [], |row| row.get(0)).unwrap();
    assert_eq!(handles, 0);
    assert!(core.host().account_for_origin(&new_origin).unwrap().info().me.is_none());
    assert!(snapshot.origin().is_revoked());
    core.shutdown().await;
}

#[tokio::test]
async fn unknown_account_native_stop_ends_the_same_connection_origin_interval() {
    let (_temp, core) = fixture().await;
    let registry = core.host().test_registry();
    let scope = core.host().capture_start("fixture", "owner").await.unwrap();
    assert!(registry.set_start("fixture", "owner", AccountSpec::default(), &scope));
    let (conn, _frames) = owned_connection(&registry);
    let (start, response) = conn.request(PendingKind::AccountStart("owner".into(), scope.clone()), "account.start",
        serde_json::json!({}), Duration::from_secs(1), None, &conn).unwrap();
    resolve(&conn, &registry, RequestId::Number(start), Ok(serde_json::json!({})));
    tokio::time::timeout(Duration::from_secs(5), response).await.unwrap().unwrap().unwrap();
    let (stop, response) = conn.request(PendingKind::AccountStop("owner".into()), "account.stop",
        serde_json::json!({"account_id":"owner"}), Duration::from_secs(1), None, &conn).unwrap();
    resolve(&conn, &registry, RequestId::Number(stop), Err(ErrorObject::new(ErrorCode::UnknownAccount, "already stopped")));
    assert!(tokio::time::timeout(Duration::from_secs(5), response).await.unwrap().unwrap().is_err());
    assert!(conn.origins.as_ref().unwrap().freeze("owner").is_none());
    let next = scope.replay();
    assert!(registry.set_start("fixture", "owner", AccountSpec::default(), &next));
    let (start, response) = conn.request(PendingKind::AccountStart("owner".into(), next), "account.start",
        serde_json::json!({}), Duration::from_secs(1), None, &conn).unwrap();
    resolve(&conn, &registry, RequestId::Number(start), Ok(serde_json::json!({})));
    tokio::time::timeout(Duration::from_secs(5), response).await.unwrap().unwrap().unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn native_origin_interval_is_not_retagged_by_unknown_or_wrong_stop_completion() {
    let (_temp, core) = fixture().await;
    let (_other_temp, other) = fixture().await;
    let scope = core.host().capture_start("fixture", "owner").await.unwrap();
    let old = scope.origin().unwrap().clone();
    let foreign_scope = other.host().capture_start("fixture", "owner").await.unwrap();
    let foreign = foreign_scope.origin().unwrap().clone();
    let origins = super::super::origins::Origins::new();
    origins.start("owner", &old).unwrap().commit();
    assert!(origins.start("owner", &foreign).is_none());
    let identity = Arc::new(()); let stopping = LifecycleRequest::new(&identity, 1);
    origins.stop("owner", stopping).unwrap().commit();
    origins.stop_confirmed("owner", &LifecycleRequest::new(&Arc::new(()), 1));
    assert!(origins.freeze("owner").unwrap().same(&old));
    assert!(origins.start("owner", &foreign).is_none());
    origins.stop_confirmed("owner", &LifecycleRequest::new(&identity, 2));
    assert!(origins.freeze("owner").unwrap().same(&old));
    // No failed/unknown/native-local phase path ends this interval. Only resolve
    // of the exact stop above can do that, exercised by the actual Pending test.
    core.shutdown().await; other.shutdown().await;
}

#[tokio::test]
async fn bound_status_without_this_connections_start_origin_cannot_adopt_another_peers_record() {
    let (_temp, core) = fixture().await;
    let registry = core.host().test_registry();
    let scope = core.host().capture_start("fixture", "owner").await.unwrap();
    assert!(registry.set_start("fixture", "owner", AccountSpec::default(), &scope));
    let (started, _frames) = owned_connection(&registry);
    let (id, response) = started.request(PendingKind::AccountStart("owner".into(), scope), "account.start",
        serde_json::json!({}), Duration::from_secs(1), None, &started).unwrap();
    resolve(&started, &registry, RequestId::Number(id), Ok(serde_json::json!({})));
    tokio::time::timeout(Duration::from_secs(5), response).await.unwrap().unwrap().unwrap();
    let before = core.host().account("fixture", "owner").unwrap();
    let (foreign, _foreign_frames) = owned_connection(&registry);
    let mut changes = core.host().subscribe_owned();
    let (send, mut deliveries) = mpsc::channel(1);
    let secrets: Arc<dyn SecretStore> = Arc::new(MemorySecretStore::new());
    handle_incoming(&foreign, &registry, &secrets, &send, None, "status", Some(serde_json::json!({
        "account_id":"owner","state":"connected","detail":"foreign-context","reconnecting":false,
        "me":{"id":"foreign-me","display_name":"Synthetic"}
    }))).await;
    assert_eq!(core.host().account("fixture", "owner").unwrap(), before);
    assert!(matches!(changes.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
    assert!(matches!(deliveries.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    core.shutdown().await;
}
