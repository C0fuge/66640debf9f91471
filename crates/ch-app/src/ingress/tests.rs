use super::*;
use crate::{App, SaveDialogCallback};
use ch_core::{
    host::MemorySecretStore,
    runtime::{CoreConfig, NewAccount},
};
use ch_db::{ConversationRow, TopicRow};
use std::{path::Path, time::Duration};
use tempfile::TempDir;

struct Fixture {
    root: TempDir,
    source: TempDir,
    thread: Thread,
}
impl Fixture {
    async fn new(caps: Value) -> (Self, App) {
        let root = tempfile::tempdir().unwrap();
        let source = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join("report.txt"), b"reviewed bytes").unwrap();
        let dir = root.path().join("plugins/ingress_fixture");
        std::fs::create_dir_all(&dir).unwrap();
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/ingress_plugin.py");
        std::fs::write(dir.join("plugin.json"),json!({"id":"ingress_fixture","name":"Ingress fixture","version":"0.0.1","api_version":"1.0.0","entry":["python3","-B","-s",script],"brand_color":"#123456","multi_account":true,"auth_methods":["none"]}).to_string()).unwrap();
        std::fs::write(root.path().join("caps"), caps.to_string()).unwrap();
        let mut f = Self {
            root,
            source,
            thread: Thread {
                account_id: "fixture".into(),
                chat_id: "chat".into(),
                topic_id: Some("topic".into()),
            },
        };
        let app = f.reopen().await;
        let account = app
            .core
            .accounts()
            .add(NewAccount::new("ingress_fixture").id("fixture"))
            .await
            .unwrap();
        f.thread.account_id = account.account_id.clone();
        app.read(move |db| {
            let conv = ch_db::insert_conversation(
                db.conn(),
                &ConversationRow {
                    account_id: account.account_id,
                    chat_id: "chat".into(),
                    kind: "group".into(),
                    can_post: Some(true),
                    ..Default::default()
                },
            )?;
            for id in ["topic", "other-topic"] {
                ch_db::insert_topic(
                    db.conn(),
                    &TopicRow {
                        conversation_id: conv,
                        topic_id: id.into(),
                        title: "Topic".into(),
                        ..Default::default()
                    },
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
        (f, app)
    }
    async fn reopen(&self) -> App {
        let mut c = CoreConfig::new(self.root.path(), Arc::new(MemorySecretStore::new()));
        c.host.env.insert(
            "CH_FIXTURE_CAPS".into(),
            std::fs::read_to_string(self.root.path().join("caps")).unwrap(),
        );
        c.host.env.insert(
            "CH_FIXTURE_TRACE".into(),
            self.root.path().join("trace").to_str().unwrap().to_owned(),
        );
        let mut app = App::open(c, None).await.unwrap();
        app.set_open_file_dialog(Arc::new(Fixed(self.source.path().join("report.txt"))));
        app
    }
    fn calls(&self) -> Vec<Value> {
        std::fs::read_to_string(self.root.path().join("trace"))
            .unwrap_or_default()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }
    fn scope(&self, app: &App) -> Value {
        json!({"thread":self.thread,"sessionId":app.ingress.session})
    }
    fn enqueue(&self, app: &App, id: &str) -> Value {
        let mut p = self.scope(app);
        p["stageId"] = json!(id);
        p["caption"] = json!(" literal  ");
        p["asFile"] = json!(true);
        p["spoiler"] = json!(false);
        p
    }
    async fn start(&self, app: &App, key: &str) -> Value {
        let mut p = self.scope(app);
        p["requestKey"] = json!(key);
        call(app, "startFileSelection", p).await
    }
    async fn page(&self, app: &App, key: Option<&str>) -> Value {
        call(
            app,
            "listFileStages",
            json!({"thread":self.thread,"requestKey":key,"cursor":null}),
        )
        .await["value"]
            .clone()
    }
    async fn state(&self, app: &App, key: &str, want: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let s = self.page(app, Some(key)).await["items"][0].clone();
                if s["state"] == want {
                    return s;
                }
                assert_ne!(s["state"], "failed", "stage failed before {want}: {s}");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap()
    }
}
struct Fixed(PathBuf);
impl OpenFileDialog for Fixed {
    fn choose_open(&self, cb: SaveDialogCallback) {
        cb(Ok(Some(self.0.clone())));
    }
}
#[derive(Default)]
struct Held(Mutex<Option<SaveDialogCallback>>);
impl OpenFileDialog for Held {
    fn choose_open(&self, cb: SaveDialogCallback) {
        *lock(&self.0) = Some(cb);
    }
}
async fn call(app: &App, method: &str, params: Value) -> Value {
    app.request(json!({"method":method,"params":params})).await
}
fn caps() -> Value {
    json!({"send.file":{},"send.as_file":true,"send.spoiler":true})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_claim_sends_only_reviewed_bytes_and_recovery_survives_row_deletion() {
    let (f, app) = Fixture::new(caps()).await;
    assert_eq!(f.start(&app, "one").await["ok"], true);
    let ready = f.state(&app, "one", "ready").await;
    assert_eq!(ready["name"], "report.txt");
    assert_eq!(ready["bytes"], "14");
    assert_eq!(ready["format"], "other");
    assert!(f.calls().is_empty());
    assert!(
        !ready
            .to_string()
            .contains(f.source.path().to_str().unwrap())
    );
    std::fs::write(f.source.path().join("report.txt"), b"changed original").unwrap();
    assert_eq!(
        f.start(&app, "one").await["value"]["stageId"],
        ready["stageId"]
    );
    let id = ready["stageId"].as_str().unwrap();
    let p = f.enqueue(&app, id);
    let (x, y) = tokio::join!(
        call(&app, "enqueueFile", p.clone()),
        call(&app, "enqueueFile", p.clone())
    );
    assert_eq!(x["ok"], true, "{x}");
    assert_eq!(y["ok"], true, "{y}");
    let accepted = f.state(&app, "one", "enqueued").await;
    let cid = accepted["outboxId"].as_str().unwrap();
    let sent = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows = call(&app, "listOutbox", json!({"thread":f.thread})).await;
            let row = &rows["value"][0];
            if row["state"] == "sent" {
                break row.clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(sent["kind"], "sendFile");
    assert_eq!(sent["thread"]["topicId"], "topic");
    assert_eq!(sent["id"], cid);
    assert_eq!(f.calls().len(), 1);
    let sent_params = &f.calls()[0];
    assert_eq!(sent_params["body"], "reviewed bytes");
    assert_eq!(sent_params["caption"], " literal  ");
    assert!(sent_params.get("spoiler").is_none() && sent_params.get("client_id").is_none());
    let mut changed = p.clone();
    changed["asFile"] = json!(false);
    assert_eq!(
        call(&app, "enqueueFile", changed).await["error"]["code"],
        "conflict"
    );
    assert_eq!(
        call(&app, "retryOutbox", json!({"thread":f.thread,"id":cid})).await["error"]["code"],
        "unsupported"
    );
    // Consume and its directory sync may still be settling after the first positive receipt.
    app.shutdown().await;
    drop(app);
    let app = f.reopen().await;
    assert_eq!(
        call(&app, "enqueueFile", p).await["error"]["code"],
        "conflict"
    );
    assert_eq!(f.page(&app, None).await["items"][0]["outboxRow"], "present");
    let row = app.outbox.get_by_client_id(cid).unwrap().unwrap();
    app.read(move |db| {
        ch_db::delete_outbox(db.conn(), row.id)?;
        Ok(())
    })
    .await
    .unwrap();
    let recovered = f.page(&app, None).await;
    assert_eq!(recovered["items"][0]["state"], "enqueued");
    assert_eq!(recovered["items"][0]["outboxRow"], "missing");
    assert!(recovered["items"][0]["requestKey"].is_null());
    assert_eq!(f.calls().len(), 1);
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claimed_before_core_and_damaged_bytes_recover_without_inserting() {
    let (f, app) = Fixture::new(caps()).await;
    f.start(&app, "claim").await;
    let ready = f.state(&app, "claim", "ready").await;
    let id = ready["stageId"].as_str().unwrap().to_owned();
    let a = app.core.accounts().get("fixture").await.unwrap().unwrap();
    let store = app.ingress.store().unwrap();
    let stage = store.recover(&app.ingress.fs_owner(&a), &id).unwrap();
    // Fixture crashes immediately after the durable claim, before its only initial core call.
    let i = fs::Intent {
        caption: " literal  ".into(),
        as_file: true,
        spoiler: false,
    };
    let claim = stage
        .verify(fs::MAX_BYTES, |_| Ok(()))
        .unwrap()
        .claim(i)
        .unwrap();
    let cid = claim.manifest().client_id.clone();
    drop(claim);
    std::fs::remove_file(stage.managed_path()).unwrap();
    app.shutdown().await;
    drop(app);
    let app = f.reopen().await;
    let p = f.page(&app, None).await;
    assert_eq!(p["items"][0]["state"], "admissionUnknown");
    assert_eq!(p["items"][0]["outboxRow"], "missing");
    let repeat = call(&app, "enqueueFile", f.enqueue(&app, &id)).await;
    assert_eq!(repeat["value"]["state"], "admissionUnknown");
    assert!(app.outbox.get_by_client_id(&cid).unwrap().is_none());
    assert!(f.calls().is_empty());
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_chooser_does_not_block_removal_or_shutdown_and_late_callback_has_no_authority() {
    for remove in [true, false] {
        let (f, mut app) = Fixture::new(caps()).await;
        let held = Arc::new(Held::default());
        app.set_open_file_dialog(held.clone());
        assert_eq!(f.start(&app, "held").await["ok"], true);
        let cb = tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                if let Some(cb) = lock(&held.0).take() {
                    return cb;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let upload = app.core.host().config().upload_dir("fixture");
        if remove {
            tokio::time::timeout(
                Duration::from_secs(5),
                app.core.accounts().remove("fixture"),
            )
            .await
            .unwrap()
            .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(5), app.shutdown())
            .await
            .unwrap();
        cb(Ok(Some(f.source.path().join("report.txt"))));
        assert!(!upload.exists());
        assert!(f.calls().is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopped_run_revokes_pending_chooser_even_without_app_notifications() {
    let (f, mut app) = Fixture::new(caps()).await;
    f.start(&app, "ready").await;
    let ready = f.state(&app, "ready", "ready").await;
    let held = Arc::new(Held::default());
    app.set_open_file_dialog(held.clone());
    // Leave the handle for shutdown; no delayed stop notification can cancel this task.
    app.tasks.lock().await.pump.as_ref().unwrap().abort();
    assert_eq!(f.start(&app, "old-chooser").await["ok"], true);
    let cb = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(cb) = lock(&held.0).take() {
                return cb;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let old = app
        .core
        .host()
        .running_account_token("ingress_fixture", "fixture")
        .unwrap();
    app.core
        .accounts()
        .set_enabled("fixture", false)
        .await
        .unwrap();
    assert!(old.is_cancelled());
    assert_eq!(
        f.page(&app, Some("ready")).await["items"][0]["state"],
        "ready"
    );
    app.core
        .accounts()
        .set_enabled("fixture", true)
        .await
        .unwrap();
    cb(Ok(Some(f.source.path().join("report.txt"))));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = f.page(&app, Some("old-chooser")).await["items"][0]["state"].clone();
            if state == "failed" || state == "cancelled" {
                break;
            }
            assert_ne!(state, "ready");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(app.ingress.store().unwrap().inventory().total.slots, 1);
    let id = ready["stageId"].as_str().unwrap();
    assert_eq!(
        call(&app, "enqueueFile", f.enqueue(&app, id)).await["ok"],
        true
    );
    f.state(&app, "ready", "enqueued").await;
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn strict_scope_limits_and_explicit_discard_retain_a_referenced_ready_stage() {
    let (f, app) = Fixture::new(caps()).await;
    let mut p = f.scope(&app);
    p["requestKey"] = json!("one");
    p["path"] = json!("/denied");
    assert_eq!(
        call(&app, "startFileSelection", p).await["error"]["code"],
        "invalid_request"
    );
    assert_eq!(
        call(
            &app,
            "listFileStages",
            json!({"thread":f.thread,"cursor":null})
        )
        .await["error"]["code"],
        "invalid_request"
    );
    f.start(&app, "one").await;
    let ready = f.state(&app, "one", "ready").await;
    let id = ready["stageId"].as_str().unwrap();
    let mut bad = f.enqueue(&app, id);
    bad["thread"]["topicId"] = json!("other-topic");
    assert_eq!(
        call(&app, "enqueueFile", bad).await["error"]["code"],
        "not_found"
    );
    let mut bad = f.enqueue(&app, id);
    bad["caption"] = json!("a".repeat(1025));
    assert_eq!(
        call(&app, "enqueueFile", bad).await["error"]["code"],
        "invalid_request"
    );
    let a = app.core.accounts().get("fixture").await.unwrap().unwrap();
    let stage = app
        .ingress
        .store()
        .unwrap()
        .recover(&app.ingress.fs_owner(&a), id)
        .unwrap();
    let path = stage.managed_path();
    // A generic pre-existing core reference may use a different absolute spelling.
    let alias = f.source.path().join("stage-alias");
    std::os::unix::fs::symlink(path.parent().unwrap(), &alias).unwrap();
    let saved = alias.join("bytes").to_str().unwrap().to_owned();
    app.read(move|db|{
        db.conn().execute("INSERT INTO media_file(path,account_id,kind,last_access_at,created_at) VALUES (?1,'fixture','full',1,1)",[saved]).map_err(ch_db::Error::from)?;
        Ok(())
    }).await.unwrap();
    let mut cancel = f.scope(&app);
    cancel["stageId"] = json!(id);
    assert_eq!(
        call(&app, "cancelFileSelection", cancel.clone()).await["ok"],
        true
    );
    let retained = f.state(&app, "one", "ready").await;
    assert!(retained["error"].is_object());
    assert!(path.is_file());
    app.read(|db| {
        db.conn()
            .execute("DELETE FROM media_file", [])
            .map_err(ch_db::Error::from)?;
        Ok(())
    })
    .await
    .unwrap();
    let alias = format!("{}/./bytes", path.parent().unwrap().display());
    app.read(move |db| {
        let params = json!({"account_id":"fixture","chat_id":"chat","path":alias});
        db.conn().execute("INSERT INTO outbox(account_id,chat_id,client_id,kind,method,params_json,state,created_at,updated_at) VALUES ('fixture','chat','old-file','send_file','messages.send_file',?1,'failed',0,0)",[params.to_string()]).map_err(ch_db::Error::from)?;
        Ok(())
    }).await.unwrap();
    assert_eq!(
        call(&app, "cancelFileSelection", cancel.clone()).await["ok"],
        true
    );
    f.state(&app, "one", "ready").await;
    assert!(path.is_file());
    // A legacy row with a missing required path cannot prove that no reference exists.
    app.read(|db| {
        db.conn()
            .execute(
                "UPDATE outbox SET params_json='{}' WHERE client_id='old-file'",
                [],
            )
            .map_err(ch_db::Error::from)?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        call(&app, "cancelFileSelection", cancel.clone()).await["ok"],
        true
    );
    f.state(&app, "one", "ready").await;
    assert!(path.is_file());
    app.read(|db| {
        db.conn()
            .execute("DELETE FROM outbox WHERE client_id='old-file'", [])
            .map_err(ch_db::Error::from)?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(call(&app, "cancelFileSelection", cancel).await["ok"], true);
    f.state(&app, "one", "cancelled").await;
    assert!(!path.exists());
    assert!(f.calls().is_empty());
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cap_shapes_and_optional_presence_are_checked_before_launch() {
    for cap in [
        json!(false),
        json!({"max_bytes":null}),
        json!({"max_bytes":0}),
        json!({"max_bytes":"100"}),
    ] {
        let (f, app) = Fixture::new(json!({"send.file":cap})).await;
        assert_eq!(f.start(&app, "bad").await["error"]["code"], "unsupported");
        assert!(
            f.page(&app, None).await["items"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        app.shutdown().await;
    }
    let (f, app) = Fixture::new(json!({"send.file":true})).await;
    f.start(&app, "no-as-file").await;
    let ready = f.state(&app, "no-as-file", "ready").await;
    let mut p = f.enqueue(&app, ready["stageId"].as_str().unwrap());
    assert_eq!(
        call(&app, "enqueueFile", p.clone()).await["error"]["code"],
        "unsupported"
    );
    p["asFile"] = json!(false);
    p["spoiler"] = json!(true);
    assert_eq!(
        call(&app, "enqueueFile", p).await["error"]["code"],
        "unsupported"
    );
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncertain_unlink_keeps_charge_and_full_store_refuses_new_keys() {
    let (f, app) = Fixture::new(caps()).await;
    f.start(&app, "discard").await;
    let ready = f.state(&app, "discard", "ready").await;
    let initial = f.page(&app, None).await["remainingRequestKeys"].clone();
    *lock(&app.ingress.checkpoint) = Some(Arc::new(|point| {
        if point == "discard-settlement" {
            Err(fs::Error::Unknown)
        } else {
            Ok(())
        }
    }));
    let mut p = f.scope(&app);
    p["stageId"] = ready["stageId"].clone();
    call(&app, "cancelFileSelection", p).await;
    f.state(&app, "discard", "failed").await;
    assert_eq!(app.ingress.store().unwrap().inventory().total.slots, 0);
    assert_eq!(f.start(&app, "new").await["error"]["code"], "conflict");
    assert_eq!(f.start(&app, "discard").await["value"]["state"], "failed");
    assert_eq!(f.page(&app, None).await["remainingRequestKeys"], initial);
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hidden_startup_manifests_are_imported_after_owner_lifecycle_refresh() {
    let (f, app) = Fixture::new(caps()).await;
    f.start(&app, "before").await;
    let ready = f.state(&app, "before", "ready").await;
    let root = app.core.host().config().upload_dir("fixture");
    app.shutdown().await;
    drop(app);
    let hidden = f.root.path().join("fixture-hidden-uploads");
    std::fs::rename(&root, &hidden).unwrap();
    let app = f.reopen().await;
    assert_eq!(
        f.page(&app, None).await["items"].as_array().unwrap().len(),
        0
    );
    app.core
        .accounts()
        .set_enabled("fixture", false)
        .await
        .unwrap();
    std::fs::rename(hidden, root).unwrap();
    app.core
        .accounts()
        .set_enabled("fixture", true)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let page = f.page(&app, None).await;
            if page["items"][0]["stageId"] == ready["stageId"] {
                assert!(page["items"][0]["requestKey"].is_null());
                assert_eq!(page["items"][0]["state"], "ready");
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(f.calls().is_empty());
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn equal_birth_replacement_cannot_observe_old_terminal_metadata_or_reuse_key() {
    let (f, app) = Fixture::new(caps()).await;
    f.start(&app, "old").await;
    let ready = f.state(&app, "old", "ready").await;
    let owner = app.core.accounts().get("fixture").await.unwrap().unwrap();
    app.core.accounts().remove("fixture").await.unwrap();
    app.read(move|db|{
        // The same synthetic wall-clock millisecond is returned on both creations.
        let sql=format!("CREATE TRIGGER same_birth AFTER INSERT ON account BEGIN UPDATE account SET created_at={} WHERE account_id=NEW.account_id; END;",owner.created_at);
        db.conn().execute_batch(&sql).map_err(ch_db::Error::from)?;
        Ok(())
    }).await.unwrap();
    let replacement = app
        .core
        .accounts()
        .add(NewAccount::new("ingress_fixture").id("fixture"))
        .await
        .unwrap();
    assert_eq!(replacement.created_at, owner.created_at);
    let page = f.page(&app, None).await;
    assert_eq!(page["items"].as_array().unwrap().len(), 0);
    assert!(
        f.page(&app, Some("old")).await["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(f.start(&app, "old").await["error"]["code"], "not_found");
    let mut cancel = f.scope(&app);
    cancel["stageId"] = ready["stageId"].clone();
    assert_eq!(
        call(&app, "cancelFileSelection", cancel).await["error"]["code"],
        "not_found"
    );
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn copy_publication_guard_drains_before_removing_account_directory() {
    let (f, app) = Fixture::new(caps()).await;
    let app = Arc::new(app);
    let (reached, rx) = tokio::sync::oneshot::channel();
    let reached = Mutex::new(Some(reached));
    let (release, end) = std::sync::mpsc::channel();
    let end = Mutex::new(end);
    *lock(&app.ingress.checkpoint) = Some(Arc::new(move |point| {
        if point == "copy-publication"
            && let Some(signal) = lock(&reached).take()
        {
            let _ = signal.send(());
            lock(&end).recv_timeout(Duration::from_secs(8)).unwrap();
        }
        Ok(())
    }));
    let owner = app.core.accounts().get("fixture").await.unwrap().unwrap();
    let (revoked, lease) = app.core.accounts().storage_lease(&owner).await.unwrap();
    drop(lease);
    f.start(&app, "copy").await;
    tokio::time::timeout(Duration::from_secs(8), rx)
        .await
        .unwrap()
        .unwrap();
    let removing = app.clone();
    let task = tokio::spawn(async move {
        removing.core.accounts().remove("fixture").await.unwrap();
    });
    tokio::time::timeout(Duration::from_secs(5), revoked.cancelled())
        .await
        .unwrap();
    assert!(!task.is_finished());
    assert!(app.core.host().config().state_dir("fixture").exists());
    release.send(()).unwrap();
    task.await.unwrap();
    app.shutdown().await;
    assert!(!app.core.host().config().upload_dir("fixture").exists());
    assert!(f.calls().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_consume_after_commit_recovers_exact_row_and_never_enqueues_again() {
    let (f, app) = Fixture::new(caps()).await;
    *lock(&app.ingress.checkpoint) = Some(Arc::new(|pt| {
        if pt == "committed" {
            Err(fs::Error::Unknown)
        } else {
            Ok(())
        }
    }));
    f.start(&app, "lost").await;
    let ready = f.state(&app, "lost", "ready").await;
    let id = ready["stageId"].as_str().unwrap();
    call(&app, "enqueueFile", f.enqueue(&app, id)).await;
    f.state(&app, "lost", "enqueued").await;
    app.shutdown().await;
    drop(app);
    let app = f.reopen().await;
    let row = f.page(&app, None).await["items"][0].clone();
    assert_eq!(row["state"], "enqueued");
    assert_eq!(row["outboxRow"], "present");
    assert_eq!(
        call(&app, "enqueueFile", f.enqueue(&app, id)).await["value"]["outboxId"],
        row["outboxId"]
    );
    assert_eq!(app.outbox.list("fixture").unwrap().len(), 1);
    assert!(f.calls().len() <= 1);
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quota_and_receipt_bounds_preserve_exact_scope_and_continuations() {
    let (f, app) = Fixture::new(caps()).await;
    f.start(&app, "first").await;
    f.state(&app, "first", "ready").await;
    let owner = app.core.accounts().get("fixture").await.unwrap().unwrap();
    let fsowner = app.ingress.fs_owner(&owner);
    let mut inv = app.ingress.store().unwrap().inventory();
    assert!(inv.complete);
    inv.total.slots = 16;
    assert!(storage::budget(&inv, &fsowner, 1).is_err());
    inv.total.slots = 0;
    inv.total.bytes = (8u64 << 30) - 1;
    assert!(storage::budget(&inv, &fsowner, 1).is_ok());
    assert!(storage::budget(&inv, &fsowner, 2).is_err());
    inv.total.bytes = 0;
    inv.per_directory.values_mut().next().unwrap().bytes = (4u64 << 30) - 1;
    assert!(storage::budget(&inv, &fsowner, 1).is_ok());
    assert!(storage::budget(&inv, &fsowner, 2).is_err());
    // Populate synthetic process receipts without opening thousands of native dialogs.
    {
        let mut l = lock(&app.ingress.ledger);
        let base = l.entries.values().next().unwrap().clone();
        for k in 1..KEYS {
            let mut e = base.clone();
            e.stage = format!("{k:032x}");
            e.key = Some(format!("key{k}"));
            e.state = State::Cancelled;
            e.bound = None;
            e.active = None;
            l.insert(e);
        }
    }
    let page = f.page(&app, None).await;
    assert_eq!(page["items"].as_array().unwrap().len(), PAGE);
    assert!(serde_json::to_vec(&page).unwrap().len() <= MAX_REPLY);
    assert_eq!(page["remainingRequestKeys"], 0);
    let cursor = page["nextCursor"].clone();
    let next = call(
        &app,
        "listFileStages",
        json!({"thread":f.thread,"requestKey":null,"cursor":cursor}),
    )
    .await;
    assert_eq!(next["value"]["items"].as_array().unwrap().len(), PAGE);
    assert_ne!(
        next["value"]["items"][0]["stageId"],
        page["items"][0]["stageId"]
    );
    let mut other = f.thread.clone();
    other.topic_id = None;
    assert_eq!(
        call(
            &app,
            "listFileStages",
            json!({"thread":other,"requestKey":null,"cursor":cursor})
        )
        .await["error"]["code"],
        "invalid_request"
    );
    assert_eq!(f.start(&app, "fresh").await["error"]["code"], "conflict");
    assert_eq!(f.start(&app, "first").await["value"]["state"], "ready");
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn four_owned_jobs_refuse_a_fifth_before_binding_the_key() {
    let (f, mut app) = Fixture::new(caps()).await;
    let held = Arc::new(Held::default());
    app.set_open_file_dialog(held.clone());
    let mut callbacks = Vec::new();
    for n in 0..JOBS {
        assert_eq!(f.start(&app, &format!("hold-{n}")).await["ok"], true);
        let cb = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(cb) = lock(&held.0).take() {
                    return cb;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        callbacks.push(cb);
    }
    {
        let _preflight = app.ingress.starts.lock().await;
        let refusal = tokio::time::timeout(Duration::from_secs(1), f.start(&app, "preflight-busy"))
            .await
            .unwrap();
        assert_eq!(refusal["error"]["code"], "conflict");
        assert_eq!(f.start(&app, "hold-0").await["value"]["state"], "choosing");
    }
    assert_eq!(f.start(&app, "fifth").await["error"]["code"], "conflict");
    assert_eq!(f.start(&app, "hold-0").await["value"]["state"], "choosing");
    assert_eq!(
        f.page(&app, None).await["remainingRequestKeys"],
        KEYS - JOBS
    );
    for cb in callbacks {
        cb(Ok(None));
    }
    f.state(&app, "hold-0", "cancelled").await;
    assert!(f.calls().is_empty());
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_bytes_and_orphan_entries_refuse_new_keys_but_allow_ready_send() {
    let (f, app) = Fixture::new(caps()).await;
    let mut ready = Value::Null;
    for n in 0..16 {
        let key = format!("keep-{n}");
        assert_eq!(f.start(&app, &key).await["ok"], true);
        ready = f.state(&app, &key, "ready").await;
        // Wait for the previous owned blocking task to release the single scan slot.
        let permit = app.ingress.scan.acquire().await.unwrap();
        drop(permit);
    }
    assert_eq!(app.ingress.store().unwrap().inventory().total.slots, 16);
    assert_eq!(f.start(&app, "excess").await["error"]["code"], "conflict");
    assert_eq!(
        f.start(&app, "keep-15").await["value"]["stageId"],
        ready["stageId"]
    );
    assert_eq!(f.page(&app, None).await["remainingRequestKeys"], KEYS - 16);
    let id = ready["stageId"].as_str().unwrap();
    assert_eq!(
        call(&app, "enqueueFile", f.enqueue(&app, id)).await["ok"],
        true
    );
    f.state(&app, "keep-15", "enqueued").await;
    let mut cancel = f.scope(&app);
    cancel["stageId"] = f.page(&app, Some("keep-0")).await["items"][0]["stageId"].clone();
    assert_eq!(call(&app, "cancelFileSelection", cancel).await["ok"], true);
    f.state(&app, "keep-0", "cancelled").await;
    let foreign = app
        .core
        .host()
        .config()
        .upload_dir("fixture")
        .join("unknown-entry");
    std::fs::create_dir(&foreign).unwrap();
    assert!(!app.ingress.store().unwrap().inventory().complete);
    assert_eq!(
        f.start(&app, "unaccounted").await["error"]["code"],
        "conflict"
    );
    assert_eq!(f.page(&app, None).await["remainingRequestKeys"], KEYS - 16);
    assert!(foreign.exists());
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_plugin_reply_leaves_one_ambiguous_delivery_and_no_retry_route() {
    let (f, app) = Fixture::new(caps()).await;
    std::fs::write(f.root.path().join("lose-reply"), b"yes").unwrap();
    f.start(&app, "ambiguous").await;
    let ready = f.state(&app, "ambiguous", "ready").await;
    let id = ready["stageId"].as_str().unwrap();
    let params = f.enqueue(&app, id);
    call(&app, "enqueueFile", params.clone()).await;
    let accepted = f.state(&app, "ambiguous", "enqueued").await;
    let cid = accepted["outboxId"].as_str().unwrap();
    let row = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let rows = call(&app, "listOutbox", json!({"thread":f.thread})).await;
            let row = &rows["value"][0];
            if row["state"] == "failed" {
                break row.clone();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(row["outcomeUnknown"], true);
    assert_eq!(
        call(&app, "retryOutbox", json!({"thread":f.thread,"id":cid})).await["error"]["code"],
        "unsupported"
    );
    assert_eq!(
        call(&app, "enqueueFile", params).await["value"]["outboxId"],
        cid
    );
    assert_eq!(f.calls().len(), 1);
    assert_eq!(app.outbox.list("fixture").unwrap().len(), 1);
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rolled_back_removal_reimports_verified_manifest_with_retired_key_isolation() {
    let (f, app) = Fixture::new(caps()).await;
    assert_eq!(f.start(&app, "old-generation").await["ok"], true);
    let ready = f.state(&app, "old-generation", "ready").await;
    let id = ready["stageId"].as_str().unwrap();
    let ordinal = lock(&app.ingress.ledger).ids[id];
    // Leave a current, successful inventory. Rollback need not publish a new event.
    f.page(&app, None).await;
    app.tasks.lock().await.pump.as_ref().unwrap().abort();
    app.read(|db|{
        db.conn().execute_batch("CREATE TRIGGER refuse_remove BEFORE DELETE ON account BEGIN SELECT RAISE(ABORT,'synthetic failure'); END;").map_err(ch_db::Error::from)?;
        Ok(())
    }).await.unwrap();
    assert!(app.core.accounts().remove("fixture").await.is_err());
    let restored = f.page(&app, None).await;
    assert_eq!(restored["items"][0]["stageId"], ready["stageId"]);
    assert_eq!(lock(&app.ingress.ledger).ids[id], ordinal);
    assert_eq!(restored["items"][0]["state"], "ready");
    assert!(restored["items"][0]["requestKey"].is_null());
    assert_eq!(restored["remainingRequestKeys"], KEYS - 1);
    assert!(
        f.page(&app, Some("old-generation")).await["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        f.start(&app, "old-generation").await["error"]["code"],
        "not_found"
    );
    let mut cancel = f.scope(&app);
    cancel["stageId"] = ready["stageId"].clone();
    assert_eq!(call(&app, "cancelFileSelection", cancel).await["ok"], true);
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn cancelled_shutdown_waiters_keep_writer_lock_and_temp_directory_until_io_settles() {
    let (f, mut app) = Fixture::new(caps()).await;
    let disposable = tempfile::tempdir().unwrap();
    let disposable_path = disposable.path().to_owned();
    app._demo = Some(Arc::new(disposable));
    let app = Arc::new(app);
    let (reached, rx) = tokio::sync::oneshot::channel();
    let reached = Mutex::new(Some(reached));
    let (release, end) = std::sync::mpsc::channel();
    let end = Mutex::new(end);
    *lock(&app.ingress.checkpoint) = Some(Arc::new(move |pt| {
        if pt == "copy-publication"
            && let Some(signal) = lock(&reached).take()
        {
            let _ = signal.send(());
            lock(&end).recv_timeout(Duration::from_secs(10)).unwrap();
        }
        Ok(())
    }));
    assert_eq!(f.start(&app, "shutdown").await["ok"], true);
    tokio::time::timeout(Duration::from_secs(8), rx)
        .await
        .unwrap()
        .unwrap();
    let waiter_app = app.clone();
    let first = tokio::spawn(async move {
        waiter_app.shutdown().await;
    });
    tokio::time::timeout(Duration::from_secs(5), app.ingress.stop.cancelled())
        .await
        .unwrap();
    first.abort();
    let _ = first.await;
    assert!(
        tokio::time::timeout(Duration::from_millis(40), app.shutdown())
            .await
            .is_err()
    );
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(f.root.path().join("app.lock"))
        .unwrap();
    drop(app);
    assert!(file.try_lock().is_err());
    assert!(disposable_path.exists());
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(8), async {
        while disposable_path.exists() || file.try_lock().is_err() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(f.calls().is_empty());
}
