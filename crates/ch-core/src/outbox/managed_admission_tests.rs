use super::*;
use crate::host::{HostConfig, HostError, MemorySecretStore, SecretStore};
use crate::managed_stage::{VerifiedStageBinding, manifest::{Fingerprint, Format, Intent, Manifest, Phase, Scope}};
use crate::runtime::{CoreConfig, NewAccount, RuntimeError};
use sha2::{Digest, Sha256};
use std::os::unix::fs::PermissionsExt;
use std::sync::Condvar;
use std::time::Duration;
use tokio::sync::oneshot;
use rusqlite::OptionalExtension;

const STAGE: &str = "11111111111111111111111111111111";
const CLIENT: &str = "22222222222222222222222222222222";

struct Fixture { temp: tempfile::TempDir, core: Arc<Core>, outbox: Outbox, secrets:Arc<MemorySecretStore> }

impl Fixture {
    async fn new(start: bool) -> Self {
        Self::with_mode(start,None).await
    }
    async fn with_mode(start:bool,mode:Option<&str>) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("plugins/fixture");
        std::fs::create_dir_all(&plugin).unwrap();
        std::fs::write(plugin.join("peer.py"), include_bytes!("../../tests/fixtures/managed_admission.py")).unwrap();
        std::fs::write(plugin.join("plugin.json"), serde_json::json!({
            "id":"fixture","name":"Fixture","version":"0.0.1","api_version":"1.2.0",
            "entry":["python3","-I","./peer.py"],"brand_color":"#123456",
            "multi_account":true,"auth_methods":["none"]
        }).to_string()).unwrap();
        let secrets=Arc::new(MemorySecretStore::new());
        let mut config = CoreConfig::new(temp.path(), secrets.clone());
        config.host = HostConfig::fast(temp.path(), 50);
        for key in ["CH_DATA_DIR", "HOME", "XDG_DATA_HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME"] {
            config.host.env.insert(key.into(), temp.path().join(key).to_string_lossy().into_owned());
        }
        config.host.env.insert("CH_TEST_MANAGED_WIRE".into(), temp.path().join("wire").to_string_lossy().into_owned());
        if let Some(mode)=mode {config.host.env.insert("CH_TEST_SOURCE_MODE".into(),mode.into());}
        config.autostart = false;
        let core = Arc::new(Core::open(config).await.unwrap());
        let owner = core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
        let db = core.open_db().unwrap();
        let conversation = ch_db::insert_conversation(db.conn(), &ch_db::ConversationRow {
            account_id:"owner".into(), chat_id:"chat".into(),kind:"group".into(),can_post:Some(true),
            ..Default::default()
        }).unwrap();
        ch_db::insert_topic(db.conn(), &ch_db::TopicRow {
            conversation_id:conversation,topic_id:"topic".into(),title:"Topic".into(),..Default::default()
        }).unwrap();
        let uploads = core.host().config().upload_dir("owner");
        let stage = uploads.join(STAGE);
        std::fs::create_dir_all(&stage).unwrap();
        for dir in [&uploads, &stage] {
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let bytes = stage.join("bytes");
        let content = b"synthetic bytes";
        std::fs::write(&bytes, content).unwrap();
        std::fs::set_permissions(&bytes, std::fs::Permissions::from_mode(0o400)).unwrap();
        let manifest = Manifest {
            version:1,stage_id:STAGE.into(),client_id:CLIENT.into(),
            scope:Scope {account:"owner".into(),plugin:"fixture".into(),account_created:owner.created_at,
                chat:"chat".into(),topic:Some("topic".into())},
            created:1,name:"original.bin".into(),size:content.len() as u64,format:Format::Other,
            fingerprint:Fingerprint::from(std::fs::metadata(&bytes).unwrap()),
            sha256:format!("{:x}",Sha256::digest(content)),phase:Phase::Ready,
        };
        std::fs::write(stage.join("manifest.json"), serde_json::to_vec(&manifest).unwrap()).unwrap();
        std::fs::set_permissions(stage.join("manifest.json"), std::fs::Permissions::from_mode(0o600)).unwrap();
        let outbox = Outbox::for_core(&core, OutboxPolicy::default());
        let fixture = Self { temp, core, outbox, secrets };
        if start {
            fixture.core.accounts().start("owner").await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while !crate::outbox::is_online(fixture.core.host().account("fixture","owner").as_ref()) {
                    tokio::task::yield_now().await;
                }
            }).await.unwrap();
        }
        fixture
    }

    fn stage(&self) -> std::path::PathBuf { self.core.host().config().upload_dir("owner").join(STAGE) }
    fn root(&self) -> std::path::PathBuf { self.core.host().config().state_dir("owner").parent().unwrap().to_owned() }
    async fn claim(&self) -> LiveStageClaim {
        let binding = self.core.accounts().storage_binding_for("owner").await.unwrap();
        VerifiedStageBinding::verify_ready(&binding, STAGE, crate::managed_stage::MAX_BYTES, |_| Ok(())).unwrap()
            .claim(Intent { caption:" exact caption ".into(),as_file:true,spoiler:false }).unwrap()
    }
    fn count(&self, table: &str) -> i64 {
        self.core.open_db().unwrap().conn().query_row(&format!("SELECT count(*) FROM {table}"), [], |r|r.get(0)).unwrap()
    }
    fn assert_no_admission(&self) {
        for table in ["managed_source", "outbox", "message", "managed_ref"] { assert_eq!(self.count(table),0,"{table}"); }
    }
    fn assert_no_wire(&self) { assert!(!self.temp.path().join("wire").exists()); }
    fn legacy_receipt(&self, claim: &LiveStageClaim) -> OutboxItem {
        let owner=claim.owner_binding().owner();
        let request=claim.canonical_request().unwrap();
        let g=self.outbox.inner.ingest();
        let tx=g.db().transaction().unwrap();
        let (_,item)=self.outbox.insert_in(&g,&tx,&owner.account_id,CLIENT,&request,None).unwrap();
        tx.commit().unwrap();item
    }
    async fn disconnect(&self) {
        let run=self.core.host().running_account_token("fixture","owner").unwrap();
        self.core.host().call_raw("fixture","messages.fetch_recent",
            serde_json::json!({"account_id":"owner","chat_id":"disconnect"}),None).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5),async {
            while !self.core.host().account("fixture","owner").is_some_and(|info|
                info.state==ch_protocol::events::AccountState::Disconnected)
            { tokio::task::yield_now().await; }
        }).await.unwrap();
        assert!(!run.is_cancelled(),"status disconnect alone keeps the start interval alive");
    }
    async fn close(self) { self.outbox.shutdown().await; self.core.shutdown().await; }
}

fn defer_commit_failure(f:&Fixture) {
    f.core.open_db().unwrap().conn().execute_batch("CREATE TABLE deferred_fault(
        owner TEXT REFERENCES account(account_id) DEFERRABLE INITIALLY DEFERRED);
        CREATE TRIGGER deferred_admission_fault AFTER INSERT ON managed_source BEGIN
            INSERT INTO deferred_fault(owner) VALUES('absent-owner'); END;").unwrap();
}

struct Release(Arc<(Mutex<bool>, Condvar)>);
impl Release {
    fn release(&self) { *self.0.0.lock().unwrap()=true;self.0.1.notify_all(); }
}
impl Drop for Release { fn drop(&mut self) { self.release(); } }

fn pause(phase: &'static str) -> (Release, Arc<TextCheckpoint>, oneshot::Receiver<()>) {
    let gate=Arc::new((Mutex::new(false),Condvar::new()));
    let release=Release(Arc::clone(&gate));
    let (send,recv)=oneshot::channel();
    let send=Mutex::new(Some(send));
    let hook: Arc<TextCheckpoint>=Arc::new(move |at| {
        if at != phase { return; }
        if let Some(send)=send.lock().unwrap().take() { let _=send.send(()); }
        let mut released=gate.0.lock().unwrap();
        while !*released { released=gate.1.wait(released).unwrap(); }
    });
    (release,hook,recv)
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn actual_claim_commits_source_echo_outbox_refs_and_parked_worker() {
    let f=Fixture::new(true).await;
    let item=f.outbox.enqueue_managed_file_checked(f.claim().await).await.unwrap();
    assert_eq!(item.state,OutboxState::Queued);
    assert_eq!(item.attempts,0);
    assert_eq!(f.count("managed_source"),1);
    assert_eq!(f.count("outbox"),1);
    assert_eq!(f.count("message"),1);
    assert_eq!(f.count("managed_ref"),2);
    let db=f.core.open_db().unwrap();
    let refs=ch_db::managed_references(db.conn(),ch_db::managed_source_id(db.conn(),STAGE).unwrap().unwrap()).unwrap();
    assert_eq!(refs.entries.len(),2);
    assert!(refs.entries.iter().all(|r|r.account_id.as_deref()==Some("owner")));
    let row=ch_db::get_outbox(db.conn(),item.id).unwrap().unwrap();
    assert_eq!(row.params["caption"]," exact caption ");
    assert_eq!(row.params["filename"],"original.bin");
    let manifest:Manifest=serde_json::from_slice(&std::fs::read(f.stage().join("manifest.json")).unwrap()).unwrap();
    assert!(matches!(manifest.phase,Phase::Consumed(_)));
    assert_eq!(f.core.managed_sources().census(),(0,1,0));
    f.outbox.start().unwrap(); // Does not bypass the shared managed guard.
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(f.outbox.get(item.id).unwrap().state,OutboxState::Queued);
    f.assert_no_wire();
    f.close().await;
}

fn assert_input_barrier(error: HostError) {
    let HostError::Rpc(error) = error else { panic!("expected local source-input refusal"); };
    assert_eq!(error.code(), ch_protocol::ErrorCode::NotSupported);
    assert!(error.message.starts_with("generic file inputs are unavailable"), "{error:?}");
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn admitted_source_blocks_all_generic_file_routes_and_aliases_without_harming_text_calls() {
    let f=Fixture::new(true).await;
    let item=f.outbox.enqueue_managed_file_checked(f.claim().await).await.unwrap();
    let outside=f.temp.path().join("outside.bin");
    let hardlink=f.temp.path().join("alias.bin");
    let symlink=f.temp.path().join("link.bin");
    std::fs::write(&outside,b"independent synthetic bytes").unwrap();
    std::fs::hard_link(f.stage().join("bytes"),&hardlink).unwrap();
    std::os::unix::fs::symlink(f.stage().join("bytes"),&symlink).unwrap();
    // This deny-only first boundary deliberately refuses even independent input.
    // No lexical, alias, existence, relative-path or account hint can grant access.
    for path in [f.stage().join("bytes"),hardlink,symlink,outside,f.temp.path().join("missing"),"relative.bin".into()] {
        for (method,params) in [
            ("messages.send_file",serde_json::json!({"account_id":"owner","chat_id":"chat","path":path})),
            ("messages.send_album",serde_json::json!({"account_id":"other","items":[{"path":path}]})),
            ("messages.edit",serde_json::json!({"account_id":"owner","path":path})),
            ("group.edit_photo",serde_json::json!({"account_id":"owner","path":path})),
            ("profile.update",serde_json::json!({"account_id":"owner","photo_path":path})),
            ("x-fixture.upload",serde_json::json!({"nested":[{"arbitrary":path}]})),
        ] {
            assert_input_barrier(f.core.host().call_raw("fixture",method,params,None).await.unwrap_err());
            f.assert_no_wire();
        }
    }
    for (method,params) in [
        ("messages.send_file",serde_json::json!({})),
        ("messages.send_file",serde_json::json!({"path":null})),
        ("messages.send_album",serde_json::json!({"items":"malformed"})),
        ("messages.edit",serde_json::json!({"path":{}})),
        ("profile.update",serde_json::json!({"photo_path":false})),
    ] {
        assert_input_barrier(f.core.host().call_raw("fixture",method,params,None).await.unwrap_err());
    }
    let params=serde_json::json!({"account_id":"owner","path":f.stage().join("bytes")});
    let error=f.core.host().call::<_,serde_json::Value>("fixture","messages.send_file",&params).await.unwrap_err();
    assert_input_barrier(error);
    let id=Arc::new(std::sync::OnceLock::new());
    assert_input_barrier(f.core.host().call_raw_tracked("fixture","messages.send_file",params,None,Some(id)).await.unwrap_err());
    f.assert_no_wire();
    // Frozen protocol text/null-photo operations still reach the same healthy
    // peer. Its METHOD_NOT_FOUND marker is a positive actual-wire control.
    for (method,params) in [
        ("messages.edit",serde_json::json!({"account_id":"owner","text":"/a/path/in/text"})),
        ("group.edit_photo",serde_json::json!({"account_id":"owner","path":null})),
        ("profile.update",serde_json::json!({"account_id":"owner","about":"/path","photo_path":null})),
    ] {
        let error=f.core.host().call_raw("fixture",method,params,None).await.unwrap_err();
        assert_eq!(error.error_code(),Some(ch_protocol::ErrorCode::MethodNotFound));
        assert_eq!(std::fs::read_to_string(f.temp.path().join("wire")).unwrap(),method);
        std::fs::remove_file(f.temp.path().join("wire")).unwrap();
    }
    assert_eq!(f.outbox.get(item.id).unwrap().state,OutboxState::Queued);
    assert_eq!(f.count("managed_dispatch"),0);
    f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn unregistered_legacy_file_flow_stays_explicitly_unchanged() {
    let f=Fixture::new(true).await;
    for method in ["messages.send_file","x-fixture.upload"] {
        let error=f.core.host().call_raw("fixture",method,
            serde_json::json!({"account_id":"owner","path":f.stage().join("bytes")}),None).await.unwrap_err();
        assert_eq!(error.error_code(),Some(ch_protocol::ErrorCode::MethodNotFound));
        assert_eq!(std::fs::read_to_string(f.temp.path().join("wire")).unwrap(),method);
    }
    assert_eq!(f.count("managed_source"),0);
    f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn complete_catalog_allows_independent_external_files_and_aliases_with_a_retained_source() {
    let f=Fixture::new(true).await;
    f.outbox.enqueue_managed_file_checked(f.claim().await).await.unwrap();
    let outside=f.temp.path().join("independent.bin");
    let hardlink=f.temp.path().join("independent-hardlink.bin");
    let symlink=f.temp.path().join("independent-link.bin");
    std::fs::write(&outside,b"independent synthetic bytes").unwrap();
    std::fs::hard_link(&outside,&hardlink).unwrap();
    std::os::unix::fs::symlink(&outside,&symlink).unwrap();
    for path in [&outside,&hardlink,&symlink] {
        for (method,params) in [
            ("messages.send_file",serde_json::json!({"account_id":"owner","path":path})),
            ("messages.send_album",serde_json::json!({"account_id":"owner","items":[{"path":path},{"path":path}]})),
            ("messages.edit",serde_json::json!({"account_id":"owner","path":path})),
            ("group.edit_photo",serde_json::json!({"account_id":"owner","path":path})),
            ("profile.update",serde_json::json!({"account_id":"owner","photo_path":path})),
        ] {
            let error=f.core.host().call_raw("fixture",method,params,None).await.unwrap_err();
            assert_eq!(error.error_code(),Some(ch_protocol::ErrorCode::MethodNotFound),"{error:?}");
            assert_eq!(std::fs::read_to_string(f.temp.path().join("wire")).unwrap(),method);
            std::fs::remove_file(f.temp.path().join("wire")).unwrap();
        }
    }
    assert_eq!(f.count("managed_dispatch"),0,"generic outside calls never mint managed attempts");
    assert_eq!(f.count("managed_source"),1);
    f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn actual_managed_admission_reopens_without_replay_or_false_closure_from_sql_labels() {
    let f=Fixture::new(true).await;
    let item=f.outbox.enqueue_managed_file_checked(f.claim().await).await.unwrap();
    f.outbox.shutdown().await;f.core.shutdown().await;
    let outside=f.temp.path().join("independent.bin");std::fs::write(&outside,b"outside").unwrap();
    for (source_state,outbox_state) in [("retained","queued"),("retiring","sending"),("released","sent")] {
        let db=f.core.open_db().unwrap();
        db.conn().execute("UPDATE managed_source SET state=?1",[source_state]).unwrap();
        db.conn().execute("UPDATE outbox SET state=?1",[outbox_state]).unwrap();
        let mut config=CoreConfig::new(f.temp.path(),f.secrets.clone());
        config.host=f.core.host().config().clone();config.autostart=false;
        let core=Core::open(config).await.unwrap();
        assert_eq!(core.managed_sources().census(),(0,0,1));
        let outbox=Outbox::for_core(&core,OutboxPolicy::default());outbox.start().unwrap();
        core.accounts().start("owner").await.unwrap();
        for path in [f.stage().join("bytes"),outside.clone()] {
            assert_input_barrier(core.host().call_raw("fixture","messages.send_file",
                serde_json::json!({"account_id":"owner","path":path}),None).await.unwrap_err());
            f.assert_no_wire();
        }
        // Positive native text control also gives the real worker a chance to
        // observe online status. It must never reset/replay the managed row.
        let error=core.host().call_raw("fixture","messages.edit",
            serde_json::json!({"account_id":"owner","text":"text still works"}),None).await.unwrap_err();
        assert_eq!(error.error_code(),Some(ch_protocol::ErrorCode::MethodNotFound));
        assert_eq!(std::fs::read_to_string(f.temp.path().join("wire")).unwrap(),"messages.edit");
        std::fs::remove_file(f.temp.path().join("wire")).unwrap();
        assert_eq!(outbox.get(item.id).unwrap().state.as_str(),outbox_state);
        assert_eq!(outbox.get(item.id).unwrap().attempts,0);
        assert!(matches!(core.accounts().remove("owner").await,Err(RuntimeError::SourceInUse)));
        assert!(f.stage().join("bytes").exists());
        assert!(core.accounts().get("owner").await.unwrap().is_some());
        assert!(matches!(f.core.accounts().remove("owner").await,Err(RuntimeError::Host(HostError::ShuttingDown))));
        let unrelated=format!("unrelated-{source_state}");
        core.accounts().add(NewAccount::new("fixture").id(&unrelated).no_start()).await.unwrap();
        core.accounts().remove(&unrelated).await.unwrap();
        assert!(core.accounts().get(&unrelated).await.unwrap().is_none());
        assert_eq!(f.count("managed_dispatch"),0);
        outbox.shutdown().await;core.shutdown().await;
    }
}

async fn wait_native_state(f:&Fixture,id:i64,state:OutboxState) {
    tokio::time::timeout(Duration::from_secs(20),async {
        while f.outbox.get(id).unwrap().state!=state {tokio::time::sleep(Duration::from_millis(10)).await;}
    }).await.unwrap();
}
async fn wait_attempt_state(f:&Fixture,state:&str) {
    tokio::time::timeout(Duration::from_secs(5),async {
        loop {
            let db=f.core.open_db().unwrap();
            let actual:Option<String>=db.conn().query_row("SELECT state FROM managed_dispatch ORDER BY id DESC LIMIT 1",[],|r|r.get(0))
                .optional().unwrap();
            if actual.as_deref()==Some(state) {break;}
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn actual_native_managed_file_has_durable_exposure_before_read_and_atomic_success() {
    let f=Fixture::with_mode(true,Some("success")).await;
    let item=f.outbox.enqueue_managed_file_checked(f.claim().await).await.unwrap();
    f.outbox.start().unwrap();wait_native_state(&f,item.id,OutboxState::Sent).await;
    wait_attempt_state(&f,"positive").await;
    let wire:serde_json::Value=serde_json::from_slice(&std::fs::read(f.temp.path().join("wire")).unwrap()).unwrap();
    assert_eq!(wire["request"]["method"],"messages.send_file");
    assert_eq!(wire["request"]["params"]["path"],f.stage().join("bytes").to_str().unwrap());
    assert!(wire["request"]["params"].get("source_release").is_none());
    assert_eq!(wire["exposure"][0],"exposed");
    assert_eq!(wire["exposure"][1],wire["pid"]);
    assert_eq!(wire["exposure"][2],"on_success");
    let db=f.core.open_db().unwrap();
    let source_sha:String=serde_json::from_slice::<Manifest>(&std::fs::read(f.stage().join("manifest.json")).unwrap()).unwrap().sha256;
    assert_eq!(wire["sha256"],source_sha);
    let (attempts,positive,settled):(i64,i64,i64)=db.conn().query_row(
        "SELECT (SELECT count(*) FROM managed_dispatch),(SELECT count(*) FROM managed_dispatch WHERE state='positive'),
            (SELECT count(*) FROM managed_source WHERE settled_dispatch_id IS NOT NULL)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    assert_eq!((attempts,positive,settled),(1,1,1));
    assert_eq!(f.outbox.get(item.id).unwrap().result_message_ids,Some(vec!["remote-file".into()]));
    assert_eq!(f.count("message"),1,"native settlement adopts the original echo without duplication");
    assert_eq!(std::fs::read_to_string(f.temp.path().join("wire.closed")).unwrap(),"True");
    let producer:String=db.conn().query_row("SELECT state FROM managed_producer",[],|r|r.get(0)).unwrap();
    assert_eq!(producer,"unresolved","success closes its own readers, not the whole helper scope");
    f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn actual_native_wrong_destination_and_malformed_success_never_certify_source_closure() {
    for mode in ["wrong-topic","malformed"] {
        let f=Fixture::with_mode(true,Some(mode)).await;
        let item=f.outbox.enqueue_managed_file_checked(f.claim().await).await.unwrap();
        f.outbox.start().unwrap();wait_native_state(&f,item.id,OutboxState::Failed).await;
        wait_attempt_state(&f,"unknown").await;
        assert_eq!(f.count("message"),1);
        let db=f.core.open_db().unwrap();
        assert_eq!(db.conn().query_row("SELECT count(*) FROM managed_dispatch WHERE state='positive'",[],|r|r.get::<_,i64>(0)).unwrap(),0);
        assert!(f.stage().join("bytes").exists());
        assert!(matches!(f.core.accounts().remove("owner").await,Err(RuntimeError::SourceInUse)));
        assert!(matches!(f.outbox.retry(item.id),Err(OutboxError::ManagedUnavailable)));
        f.close().await;
    }
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn actual_native_late_success_after_timeout_and_storage_rearm_settles_without_new_read_or_send() {
    let f=Fixture::with_mode(true,Some("late")).await;
    let item=f.outbox.enqueue_managed_file_checked(f.claim().await).await.unwrap();
    f.outbox.start().unwrap();wait_native_state(&f,item.id,OutboxState::Failed).await;
    let observed=f.outbox.get(item.id).unwrap();
    assert_eq!(observed.last_error.as_ref().unwrap().data.as_ref().unwrap().reason.as_deref(),Some("outcome_unknown"));
    assert_eq!(f.count("managed_dispatch"),1);
    let before=std::fs::read(f.temp.path().join("wire")).unwrap();
    assert!(!f.temp.path().join("wire.closed").exists(),"the native peer still holds its actual source reader");
    assert!(matches!(tokio::time::timeout(Duration::from_secs(3),f.core.accounts().remove("owner")).await.unwrap(),
        Err(RuntimeError::SourceInUse)),"network lifetime cannot retain a Rust storage read lease");
    std::fs::write(f.temp.path().join("wire.release"),b"release actual reader").unwrap();
    wait_native_state(&f,item.id,OutboxState::Sent).await;wait_attempt_state(&f,"positive").await;
    assert_eq!(std::fs::read(f.temp.path().join("wire")).unwrap(),before,"late observation never sends or reads again");
    assert_eq!(f.count("managed_dispatch"),1);assert_eq!(f.count("message"),1);
    assert_eq!(std::fs::read_to_string(f.temp.path().join("wire.closed")).unwrap(),"True");
    f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn exact_legacy_receipt_precedes_spent_changed_bytes_and_missing_run() {
    let f=Fixture::new(false).await;
    let mut claim=f.claim().await;
    claim.checked_path().unwrap(); // Already spent, but retained lookup must still work.
    let item=f.legacy_receipt(&claim);
    std::fs::set_permissions(f.stage().join("bytes"),std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::write(f.stage().join("bytes"),b"changed").unwrap();
    let retained=f.outbox.enqueue_managed_file_checked(claim).await.unwrap();
    assert_eq!(retained,item);
    assert_eq!(f.count("managed_source"),0); // Legacy receipts are never retro-registered.
    assert_eq!(f.core.managed_sources().census(),(0,0,0));
    f.assert_no_wire();
    f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn caller_loss_keeps_counted_actual_job_until_publication_and_consumed() {
    let f=Fixture::new(true).await;
    let claim=f.claim().await;
    let (release,hook,entered)=pause("before-writer");
    *f.outbox.inner.managed_checkpoint.lock().unwrap()=Some(hook);
    let outbox=f.outbox.clone();
    let caller=tokio::spawn(async move { outbox.enqueue_managed_file_checked(claim).await });
    entered.await.unwrap();
    assert_eq!(f.core.managed_sources().census(),(1,0,0));
    caller.abort();assert!(caller.await.unwrap_err().is_cancelled());
    release.release();
    tokio::time::timeout(Duration::from_secs(5),async {
        while f.core.managed_sources().census() != (0,1,0) { tokio::task::yield_now().await; }
    }).await.unwrap();
    // Publication may precede Consumed; the actual job barrier settles both.
    f.core.managed_sources().drain().await.unwrap();
    let manifest:Manifest=serde_json::from_slice(&std::fs::read(f.stage().join("manifest.json")).unwrap()).unwrap();
    assert!(matches!(manifest.phase,Phase::Consumed(_)));
    assert_eq!(f.count("outbox"),1);
    f.assert_no_wire();
    f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn cancelled_metadata_waiter_drops_claim_while_owned_lookup_remains_paused() {
    let f=Fixture::new(true).await;
    let claim=f.claim().await;
    let (release,hook,entered)=pause("lookup");
    *f.outbox.inner.managed_checkpoint.lock().unwrap()=Some(hook);
    let outbox=f.outbox.clone();
    let caller=tokio::spawn(async move { outbox.enqueue_managed_file_checked(claim).await });
    entered.await.unwrap();
    caller.abort();assert!(caller.await.unwrap_err().is_cancelled());
    // The metadata worker holds neither claim FD/flock nor account read lease.
    // Prompt refusal BEFORE lookup release proves storage drain completed: the
    // detached metadata worker owns no lease. Durable Claimed bytes stay unproved.
    assert!(matches!(tokio::time::timeout(Duration::from_secs(3),f.core.accounts().remove("owner")).await.unwrap(),
        Err(RuntimeError::SourceInUse)));
    assert!(f.stage().join("bytes").exists());
    assert_eq!(f.core.managed_sources().census(),(0,0,0));
    release.release();
    f.core.managed_sources().drain().await.unwrap();
    f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn fallback_lookup_cannot_follow_a_rearmed_generation_with_identical_owner_metadata() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let f=Fixture::new(true).await;
    let claim=f.claim().await;
    let old=claim.owner_binding().observation();
    let request=claim.canonical_request().unwrap();
    // Fresh work is unavailable, but the separate bounded receipt lane stays open.
    f.core.managed_sources().stop_fresh_admission();
    let (release,paused,entered)=pause("fallback");
    let lookups=AtomicUsize::new(0);
    *f.outbox.inner.managed_checkpoint.lock().unwrap()=Some(Arc::new(move |phase| {
        if phase=="lookup" && lookups.fetch_add(1,Ordering::SeqCst)==1 { paused("fallback"); }
    }));
    let outbox=f.outbox.clone();
    let caller=tokio::spawn(async move { outbox.enqueue_managed_file_checked(claim).await });
    entered.await.unwrap(); // Busy/Closed dropped the actual claim before fallback.
    // The unproved Claimed root now refuses deletion and rotates the real Gate.
    // The row itself stays byte-for-byte the same owner comparator, exposing the
    // same ABA risk without erasing/resetting legacy bytes to force row recreation.
    assert!(matches!(tokio::time::timeout(Duration::from_secs(3),f.core.accounts().remove("owner")).await.unwrap(),
        Err(RuntimeError::SourceInUse)));
    let db=f.core.open_db().unwrap();
    let new=ch_db::get_account(db.conn(),"owner").unwrap().unwrap();
    assert_eq!(new.created_at,old.owner().created_at);
    let replacement=f.core.accounts().storage_observation_for("owner").await.unwrap();
    assert!(old != replacement && old.is_revoked());
    let receipt={
        let g=f.outbox.inner.ingest();
        let tx=g.db().transaction().unwrap();
        let (_,item)=f.outbox.insert_in(&g,&tx,"owner",CLIENT,&request,None).unwrap();
        tx.commit().unwrap();item
    };
    // All persisted comparator fields match; only the original Gate distinguishes owners.
    assert_eq!(f.outbox.get_matching_admission(&new,CLIENT,&request).unwrap(),Some(receipt.clone()));
    release.release();
    assert!(matches!(caller.await.unwrap(),Err(ManagedAdmissionError::Rejected(OutboxError::AdmissionConflict))));
    assert_eq!(f.outbox.get(receipt.id).unwrap(),receipt);
    assert_eq!(f.count("managed_source"),0);
    f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn foreign_core_root_with_equal_owner_metadata_cannot_admit_a_real_claim() {
    let target=Fixture::new(true).await;
    let other=Fixture::new(false).await;
    let expected=target.core.accounts().get("owner").await.unwrap().unwrap();
    other.core.open_db().unwrap().conn().execute(
        "UPDATE account SET created_at=?1 WHERE account_id='owner'",[expected.created_at]).unwrap();
    let path=other.stage().join("manifest.json");
    let mut manifest:Manifest=serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    manifest.scope.account_created=expected.created_at;
    std::fs::write(&path,serde_json::to_vec(&manifest).unwrap()).unwrap();
    let claim=other.claim().await; // Genuine proof/claim from the other configured Core.
    assert_eq!(claim.owner_binding().owner().created_at,expected.created_at);
    assert_eq!(claim.owner_binding().owner().plugin_id,expected.plugin_id);
    let result=target.outbox.enqueue_managed_file_checked(claim).await;
    assert!(matches!(result,Err(ManagedAdmissionError::Rejected(OutboxError::AdmissionConflict))));
    target.assert_no_admission();other.assert_no_admission();
    target.assert_no_wire();other.assert_no_wire();
    other.close().await;target.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn same_root_foreign_accounts_issuer_and_unbound_outbox_are_not_managed_contexts() {
    let f=Fixture::new(true).await;
    let actual=f.core.accounts().storage_observation_for("owner").await.unwrap();
    let other=Accounts::new(f.core.open_db().unwrap(),f.core.host().clone(),
        Arc::new(MemorySecretStore::new()),crate::runtime::ChangeBus::new(16));
    let binding=other.storage_binding_for("owner").await.unwrap();
    assert!(binding.observation()!=actual);
    assert_eq!(binding.owner().created_at,actual.owner().created_at);
    let before=std::fs::read(f.stage().join("manifest.json")).unwrap();
    let claim=VerifiedStageBinding::verify_ready(&binding,STAGE,crate::managed_stage::MAX_BYTES,|_|Ok(())).unwrap()
        .claim(Intent {caption:" exact caption ".into(),as_file:true,spoiler:false});
    // Standalone Accounts has no Core-owned writer hook: refuse before any
    // Ready -> Claimed publication, rather than manufacturing one for admission.
    assert!(matches!(claim,Err(crate::managed_stage::Error::TargetChanged)));
    drop(binding);
    assert_eq!(std::fs::read(f.stage().join("manifest.json")).unwrap(),before);
    let manifest:Manifest=serde_json::from_slice(&before).unwrap();
    assert!(manifest.phase==Phase::Ready);
    f.assert_no_admission();f.assert_no_wire();f.close().await;

    let f=Fixture::new(true).await;
    let unbound=Outbox::new(f.core.ingest().shared_ingest(),Arc::new(f.core.host().clone()),OutboxPolicy::default());
    // This separate refusal receives a genuine Core-issued claim.
    assert!(matches!(unbound.enqueue_managed_file_checked(f.claim().await).await,
        Err(ManagedAdmissionError::Unavailable)));
    f.assert_no_admission();f.assert_no_wire();
    unbound.shutdown().await;f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn changed_destination_rolls_back_without_creating_a_stub() {
    let f=Fixture::new(true).await;
    let claim=f.claim().await;
    let (release,hook,entered)=pause("before-writer");
    *f.outbox.inner.managed_checkpoint.lock().unwrap()=Some(hook);
    let outbox=f.outbox.clone();
    let caller=tokio::spawn(async move { outbox.enqueue_managed_file_checked(claim).await });
    entered.await.unwrap();
    f.core.open_db().unwrap().conn().execute("UPDATE conversation SET can_post=0 WHERE chat_id='chat'",[]).unwrap();
    release.release();
    assert!(matches!(caller.await.unwrap(),Err(ManagedAdmissionError::Rejected(_))));
    f.assert_no_admission();
    assert_eq!(f.count("conversation"),1);
    assert_eq!(f.core.managed_sources().census(),(0,0,0));
    f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn running_disconnected_accounts_preserve_local_file_admission_policy() {
    let f=Fixture::new(true).await;
    let claim=f.claim().await;
    let (release,hook,entered)=pause("before-writer");
    *f.outbox.inner.managed_checkpoint.lock().unwrap()=Some(hook);
    let outbox=f.outbox.clone();
    let caller=tokio::spawn(async move { outbox.enqueue_managed_file_checked(claim).await });
    entered.await.unwrap(); // The actual run/scope has already been captured.
    f.disconnect().await;
    release.release();
    assert_eq!(caller.await.unwrap().unwrap().state,OutboxState::Queued);
    assert_eq!(f.count("managed_source"),1);
    assert_eq!(f.core.managed_sources().census(),(0,1,0));
    f.assert_no_wire();f.close().await;

    let f=Fixture::new(true).await;
    let claim=f.claim().await;
    f.disconnect().await;
    // A fresh capture while already disconnected is allowed too. Only the
    // original running interval/wanted owner, not connectivity, gates admission.
    assert_eq!(f.outbox.enqueue_managed_file_checked(claim).await.unwrap().state,OutboxState::Queued);
    assert_eq!(f.count("managed_source"),1);
    f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn final_revocation_after_all_inserts_rolls_back_source_echo_outbox_and_refs() {
    let f=Fixture::new(true).await;
    let stop=f.core.managed_sources().stopped();
    *f.outbox.inner.managed_checkpoint.lock().unwrap()=Some(Arc::new(move |phase| {
        if phase=="in-writer" { stop.cancel(); }
    }));
    let result=f.outbox.enqueue_managed_file_checked(f.claim().await).await;
    assert!(matches!(result,Err(ManagedAdmissionError::Rejected(OutboxError::Stopped))));
    f.assert_no_admission();
    assert_eq!(f.core.managed_sources().census(),(0,0,0));
    f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn failed_real_commit_quarantines_without_replay_or_a_durability_receipt() {
    let f=Fixture::new(true).await;
    // A real deferred-FK COMMIT failure, not a forged callback result/boolean.
    defer_commit_failure(&f);
    let result=f.outbox.enqueue_managed_file_checked(f.claim().await).await;
    assert!(matches!(result,Err(ManagedAdmissionError::Unknown)));
    f.assert_no_admission();
    assert_eq!(f.core.managed_sources().census(),(0,0,1));
    let manifest:Manifest=serde_json::from_slice(&std::fs::read(f.stage().join("manifest.json")).unwrap()).unwrap();
    assert!(matches!(manifest.phase,Phase::Claimed(_)));
    f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn committed_receipt_survives_local_manifest_bookkeeping_failure() {
    let f=Fixture::new(true).await;
    let manifest=f.stage().join("manifest.json");
    *f.outbox.inner.managed_checkpoint.lock().unwrap()=Some(Arc::new(move |phase| {
        if phase=="committed" { std::fs::write(&manifest,b"{}").unwrap(); }
    }));
    let item=f.outbox.enqueue_managed_file_checked(f.claim().await).await.unwrap();
    assert_eq!(f.outbox.get(item.id).unwrap(),item);
    assert_eq!(f.count("managed_source"),1);
    assert_eq!(f.core.managed_sources().census(),(0,1,0));
    f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn source_refusal_preserves_data_and_rearms_only_fresh_generations_without_growth() {
    let f=Fixture::new(true).await;
    f.secrets.set("fixture","owner","session","synthetic").await.unwrap();
    let item=f.outbox.enqueue_managed_file_checked(f.claim().await).await.unwrap();
    let bytes=std::fs::read(f.stage().join("bytes")).unwrap();
    let mut old=f.core.accounts().storage_observation_for("owner").await.unwrap();
    for _ in 0..3 {
        assert!(matches!(f.core.accounts().remove("owner").await,Err(RuntimeError::SourceInUse)));
        assert!(old.is_revoked());
        assert!(f.core.accounts().storage_binding_for_observation(&old).await.is_err());
        let fresh=f.core.accounts().storage_observation_for("owner").await.unwrap();
        assert!(fresh!=old && !fresh.is_revoked());old=fresh;
        assert_eq!(f.core.managed_sources().owner_barriers().closing_count(),0);
        assert_eq!(f.outbox.get(item.id).unwrap(),item);
        assert_eq!(std::fs::read(f.stage().join("bytes")).unwrap(),bytes);
        assert_eq!(f.secrets.snapshot("fixture","owner")["session"],"synthetic");
        assert!(f.core.host().account("fixture","owner").unwrap().wanted);
        assert_eq!(f.count("managed_source"),1);
    }
    f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn pending_pre_id_work_is_drained_and_committed_winner_is_retained() {
    for phase in ["before-writer","committed"] {
        let f=Fixture::new(true).await;
        let claim=f.claim().await;
        let (release,hook,entered)=pause(phase);
        *f.outbox.inner.managed_checkpoint.lock().unwrap()=Some(hook);
        let (owner_release,owner_hook,revoked)=pause("revoked");
        f.core.accounts().owner_close_checkpoint(owner_hook);
        let outbox=f.outbox.clone();
        let sending=tokio::spawn(async move{outbox.enqueue_managed_file_checked(claim).await});
        entered.await.unwrap();
        assert_eq!(f.core.managed_sources().census().0,1);
        assert_eq!(f.count("managed_source"),i64::from(phase=="committed"));
        let accounts=f.core.accounts().clone();
        let removal=tokio::spawn(async move{accounts.remove("owner").await});
        revoked.await.unwrap();owner_release.release();
        assert!(!removal.is_finished()); // The actual claim still owns its old storage lease.
        release.release();
        let sent=sending.await.unwrap();let removed=removal.await.unwrap();
        if phase=="before-writer" {
            assert!(matches!(sent,Err(ManagedAdmissionError::Rejected(_))));
            assert!(matches!(removed,Err(RuntimeError::SourceInUse)));
            assert!(f.core.accounts().get("owner").await.unwrap().is_some());
            assert!(f.stage().join("bytes").exists()); // Claimed is not proof of no previous exposure.
        } else {
            assert_eq!(sent.unwrap().state,OutboxState::Queued);
            assert!(matches!(removed,Err(RuntimeError::SourceInUse)));
            assert!(f.core.accounts().get("owner").await.unwrap().is_some());
            assert!(f.stage().join("bytes").exists());
        }
        assert_eq!(f.core.managed_sources().owner_barriers().closing_count(),0);
        f.assert_no_wire();f.close().await;
    }
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn root_swap_never_rearms_or_heals_by_recapturing_a_replacement() {
    use std::sync::atomic::{AtomicBool,Ordering};
    let f=Fixture::new(true).await;
    f.outbox.enqueue_managed_file_checked(f.claim().await).await.unwrap();
    let old=f.core.accounts().storage_observation_for("owner").await.unwrap();
    let root=f.root();let previous=root.with_file_name("previous-owner");
    let keep=previous.clone();let current=root.clone();let once=AtomicBool::new(false);
    f.core.accounts().owner_close_checkpoint(Arc::new(move|phase| {
        if phase=="before-rearm" && !once.swap(true,Ordering::SeqCst) {
            std::fs::rename(&current,&keep).unwrap();std::fs::create_dir(&current).unwrap();
            std::fs::write(current.join("replacement"),b"preserve").unwrap();
        }
    }));
    for _ in 0..2 {
        assert!(matches!(f.core.accounts().remove("owner").await,Err(RuntimeError::SourceInUse)));
        assert!(old.is_revoked());
        assert!(f.core.accounts().storage_binding_for("owner").await.is_err());
        assert_eq!(f.core.managed_sources().owner_barriers().closing_count(),1);
        assert!(f.core.accounts().get("owner").await.unwrap().is_some());
        assert!(previous.join("uploads").join(STAGE).join("bytes").exists());
        assert_eq!(std::fs::read(root.join("replacement")).unwrap(),b"preserve");
    }
    f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn failed_root_capture_cannot_fall_through_to_unanchored_removal() {
    let f=Fixture::new(false).await;
    f.secrets.set("fixture","owner","session","synthetic").await.unwrap();
    let root=f.root();let previous=root.with_file_name("previous-owner");
    std::fs::rename(&root,&previous).unwrap();
    std::os::unix::fs::symlink(&previous,&root).unwrap();
    assert!(matches!(f.core.accounts().remove("owner").await,Err(RuntimeError::SourceInUse)));
    assert!(f.core.accounts().get("owner").await.unwrap().is_some());
    assert!(std::fs::symlink_metadata(&root).unwrap().file_type().is_symlink());
    assert!(previous.join("uploads").join(STAGE).join("bytes").exists());
    assert_eq!(f.secrets.snapshot("fixture","owner")["session"],"synthetic");
    f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn deleting_transaction_refuses_a_source_inserted_after_preflight() {
    let f=Fixture::new(false).await;
    f.secrets.set("fixture","owner","session","synthetic").await.unwrap();
    let old=f.core.accounts().storage_observation_for("owner").await.unwrap();
    let (release,hook,entered)=pause("before-delete-tx");
    f.core.accounts().owner_close_checkpoint(hook);
    let accounts=f.core.accounts().clone();
    let caller=tokio::spawn(async move{accounts.remove("owner").await});
    entered.await.unwrap();
    // Unproved negative SQL evidence from a concurrent writer, not a forged grant.
    f.core.open_db().unwrap().conn().execute("INSERT INTO managed_source(stage_id,client_id,
        account_id,plugin_id,account_created_at,chat_id,path,fingerprint_json,intent_json,created_at,updated_at)
        VALUES(?1,?2,'owner','fixture',?3,'chat',?4,'{}','{}',0,0)",
        rusqlite::params![STAGE,CLIENT,old.owner().created_at,f.stage().join("bytes").to_str()]).unwrap();
    release.release();
    assert!(matches!(caller.await.unwrap(),Err(RuntimeError::SourceInUse)));
    assert!(f.core.accounts().get("owner").await.unwrap().is_some());
    assert_eq!(f.count("managed_source"),1);
    assert!(f.stage().join("bytes").exists());
    assert_eq!(f.secrets.snapshot("fixture","owner")["session"],"synthetic");
    assert!(old.is_revoked());
    assert!(f.core.accounts().storage_observation_for("owner").await.is_ok());
    f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn unknown_registry_survives_hookless_factory_and_closed_core_accounts_clone() {
    let f=Fixture::new(true).await;defer_commit_failure(&f);
    f.secrets.set("fixture","owner","session","synthetic").await.unwrap();
    assert!(matches!(f.outbox.enqueue_managed_file_checked(f.claim().await).await,Err(ManagedAdmissionError::Unknown)));
    assert_eq!(f.count("managed_source"),0);
    assert_eq!(f.core.managed_sources().census(),(0,0,1));
    let standalone=Accounts::new(f.core.open_db().unwrap(),f.core.host().clone(),f.secrets.clone(),
        crate::runtime::ChangeBus::new(16));
    assert!(matches!(standalone.remove("owner").await,Err(RuntimeError::Host(HostError::ShuttingDown))));
    assert!(f.core.accounts().get("owner").await.unwrap().is_some());drop(standalone);
    // The actual issuer also refuses from in-memory quarantine despite zero DB rows.
    assert!(matches!(f.core.accounts().remove("owner").await,Err(RuntimeError::SourceInUse)));
    assert!(f.stage().join("bytes").exists());
    let accounts=f.core.accounts().clone();let root=f.root();let path=f.core.db_path().to_owned();
    let Fixture {temp,core,outbox,secrets}=f;
    outbox.shutdown().await;core.shutdown().await;drop(outbox);drop(core);
    assert!(matches!(accounts.remove("owner").await,Err(RuntimeError::Host(HostError::ShuttingDown))));
    assert!(ch_db::get_account(ch_db::Db::open(path).unwrap().conn(),"owner").unwrap().is_some());
    assert!(root.join("uploads").join(STAGE).join("bytes").exists());
    assert_eq!(secrets.snapshot("fixture","owner")["session"],"synthetic");
    drop(accounts);drop(temp);
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn cancelled_remove_and_shutdown_wait_for_actual_destructive_worker() {
    let f=Fixture::new(false).await;
    let root=f.root();let (release,hook,entered)=pause("before-root-delete");
    f.core.accounts().owner_close_checkpoint(hook);
    let accounts=f.core.accounts().clone();
    let caller=tokio::spawn(async move{accounts.remove("owner").await});
    entered.await.unwrap(); // Inside counted blocking worker, just before std::fs deletion.
    assert!(f.core.accounts().get("owner").await.unwrap().is_none());
    assert!(root.exists());caller.abort();assert!(caller.await.unwrap_err().is_cancelled());
    let core=f.core.clone();let mut shutdown=tokio::spawn(async move{core.shutdown().await});
    assert!(tokio::time::timeout(Duration::from_millis(50),&mut shutdown).await.is_err());
    assert!(matches!(f.core.accounts().remove("owner").await,Err(RuntimeError::Host(HostError::ShuttingDown))));
    shutdown.abort();assert!(shutdown.await.unwrap_err().is_cancelled());
    let sources=f.core.managed_sources();let mut drain=tokio::spawn(async move{sources.drain().await});
    assert!(tokio::time::timeout(Duration::from_millis(25),&mut drain).await.is_err());
    release.release();
    tokio::time::timeout(Duration::from_secs(5),drain).await.unwrap().unwrap().unwrap();
    assert!(!root.exists());
    assert_eq!(f.core.managed_sources().owner_barriers().closing_count(),0);
    f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn root_replacement_after_committed_account_delete_preserves_both_directories() {
    let f=Fixture::new(false).await;
    let old=f.core.accounts().storage_observation_for("owner").await.unwrap();
    let root=f.root();let previous=root.with_file_name("removed-owner");
    let current=root.clone();let keep=previous.clone();
    f.core.accounts().owner_close_checkpoint(Arc::new(move|phase| {
        if phase=="before-root-delete" {
            std::fs::rename(&current,&keep).unwrap();
            std::fs::create_dir(&current).unwrap();
            std::fs::write(current.join("replacement"),b"preserve replacement").unwrap();
        }
    }));
    f.core.accounts().remove("owner").await.unwrap();
    assert!(f.core.accounts().get("owner").await.unwrap().is_none());
    assert!(old.is_revoked());
    assert_eq!(std::fs::read(previous.join("uploads").join(STAGE).join("bytes")).unwrap(),b"synthetic bytes");
    assert_eq!(std::fs::read(root.join("replacement")).unwrap(),b"preserve replacement");
    // A committed row deletion is not proof of root cleanup. Existing bytes
    // continue to veto ID reuse, and the original observation stays revoked.
    assert!(f.core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.is_err());
    assert!(f.core.accounts().get("owner").await.unwrap().is_none());
    assert!(old.is_revoked());
    f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn ordinary_no_source_removal_with_parent_anchored_absence_clears_close_state() {
    let f=Fixture::new(false).await;
    std::fs::remove_dir_all(f.root().parent().unwrap()).unwrap();
    f.core.accounts().remove("owner").await.unwrap();
    assert!(f.core.accounts().get("owner").await.unwrap().is_none());
    assert_eq!(f.core.managed_sources().owner_barriers().closing_count(),0);
    assert_eq!(f.core.managed_sources().census(),(0,0,0));
    f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn legacy_claimed_and_consumed_deny_by_actual_folder_not_forged_owner_hints() {
    for consumed in [false,true] {
        let f=Fixture::new(false).await;
        f.secrets.set("fixture","owner","session","synthetic").await.unwrap();
        let mut claim=f.claim().await;
        if consumed {
            claim.checked_path().unwrap();f.legacy_receipt(&claim);
            claim.consume(&f.outbox).unwrap();
        } else {drop(claim);}
        let path=f.stage().join("manifest.json");
        let mut manifest:Manifest=serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        manifest.scope.account="forged-account".into();manifest.scope.plugin="forged-plugin".into();
        manifest.scope.account_created=42;manifest.scope.chat="forged-chat".into();
        manifest.client_id="ffffffffffffffffffffffffffffffff".into();
        let mutation=f.core.stage_mutations().begin().unwrap();
        std::fs::write(&path,serde_json::to_vec(&manifest).unwrap()).unwrap();drop(mutation);
        assert!(f.core.managed_sources().catalog().refresh().unwrap().complete());
        assert!(matches!(tokio::time::timeout(Duration::from_secs(5),f.core.accounts().remove("owner")).await.unwrap(),
            Err(RuntimeError::SourceInUse)));
        assert!(f.core.accounts().get("owner").await.unwrap().is_some());
        assert!(f.stage().join("bytes").exists());
        assert_eq!(f.secrets.snapshot("fixture","owner")["session"],"synthetic");
        assert_eq!(f.count("managed_source"),0);
        f.assert_no_wire();f.close().await;
    }
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn failed_fresh_scan_retains_data_and_later_ready_hint_does_not_clear_unknown() {
    let f=Fixture::new(false).await;
    f.secrets.set("fixture","owner","session","synthetic").await.unwrap();
    let path=f.stage().join("manifest.json");let original=std::fs::read(&path).unwrap();
    let mutation=f.core.stage_mutations().begin().unwrap();
    std::fs::write(&path,b"{").unwrap();drop(mutation);
    for restore in [false,true] {
        if restore {
            let mutation=f.core.stage_mutations().begin().unwrap();
            std::fs::write(&path,&original).unwrap();drop(mutation);
        }
        assert!(matches!(tokio::time::timeout(Duration::from_secs(5),f.core.accounts().remove("owner")).await.unwrap(),
            Err(RuntimeError::SourceInUse)));
        assert!(f.core.accounts().get("owner").await.unwrap().is_some());
        assert!(f.stage().join("bytes").exists());
        assert_eq!(f.secrets.snapshot("fixture","owner")["session"],"synthetic");
    }
    f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn actual_other_owner_claim_invalidates_final_delete_epoch_but_not_folder_identity() {
    let f=Fixture::new(false).await;
    f.secrets.set("fixture","owner","session","synthetic").await.unwrap();
    let other=f.core.accounts().add(NewAccount::new("fixture").id("other").no_start()).await.unwrap();
    let uploads=f.core.host().config().upload_dir("other");
    let id="33333333333333333333333333333333";let stage=uploads.join(id);
    let mutation=f.core.stage_mutations().begin().unwrap();
    std::fs::create_dir_all(&stage).unwrap();
    for dir in [&uploads,&stage] {std::fs::set_permissions(dir,std::fs::Permissions::from_mode(0o700)).unwrap();}
    let bytes=stage.join("bytes");std::fs::write(&bytes,b"synthetic bytes").unwrap();
    std::fs::set_permissions(&bytes,std::fs::Permissions::from_mode(0o400)).unwrap();
    let mut manifest:Manifest=serde_json::from_slice(&std::fs::read(f.stage().join("manifest.json")).unwrap()).unwrap();
    manifest.stage_id=id.into();manifest.client_id="44444444444444444444444444444444".into();
    manifest.scope.account="other".into();manifest.scope.account_created=other.created_at;
    manifest.fingerprint=Fingerprint::from(std::fs::metadata(&bytes).unwrap());
    let metadata=stage.join("manifest.json");std::fs::write(&metadata,serde_json::to_vec(&manifest).unwrap()).unwrap();
    std::fs::set_permissions(&metadata,std::fs::Permissions::from_mode(0o600)).unwrap();drop(mutation);
    let (release,hook,entered)=pause("before-delete-tx");f.core.accounts().owner_close_checkpoint(hook);
    let accounts=f.core.accounts().clone();let deleting=tokio::spawn(async move{accounts.remove("owner").await});
    tokio::time::timeout(Duration::from_secs(5),entered).await.unwrap().unwrap();
    // Genuine other-issuer binding/claim publisher starts AFTER the target scan.
    // No raw epoch, capability, caller proof or fake mutation-completion boolean.
    let binding=f.core.accounts().storage_binding_for("other").await.unwrap();
    let claim=VerifiedStageBinding::verify_ready(&binding,id,crate::managed_stage::MAX_BYTES,|_|Ok(())).unwrap()
        .claim(Intent {caption:String::new(),as_file:true,spoiler:false}).unwrap();
    drop(binding);release.release();
    assert!(matches!(tokio::time::timeout(Duration::from_secs(5),deleting).await.unwrap().unwrap(),Err(RuntimeError::SourceInUse)));
    assert!(f.core.accounts().get("owner").await.unwrap().is_some());
    assert!(f.stage().join("bytes").exists());
    assert_eq!(f.secrets.snapshot("fixture","owner")["session"],"synthetic");
    drop(claim);
    f.core.accounts().owner_close_checkpoint(Arc::new(|_|{}));
    // A fresh stable Ready-only target is removable. Other's Claimed bytes stay
    // retained in their actual physical folder rather than globally misattributed.
    tokio::time::timeout(Duration::from_secs(5),f.core.accounts().remove("owner")).await.unwrap().unwrap();
    assert!(f.core.accounts().get("owner").await.unwrap().is_none());
    assert!(bytes.exists());assert!(f.core.accounts().get("other").await.unwrap().is_some());
    f.assert_no_wire();f.close().await;
}
