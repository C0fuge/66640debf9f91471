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
    let saved = path.to_str().unwrap().to_owned();
    app.read(move|db|{db.conn().execute("INSERT INTO media_file(path,account_id,kind,last_access_at,created_at) VALUES (?1,'fixture','full',1,1)",[saved]).map_err(ch_db::Error::from)?;Ok(())}).await.unwrap();
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
