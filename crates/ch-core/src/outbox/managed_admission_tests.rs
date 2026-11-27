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

struct Fixture { temp: tempfile::TempDir, core: Arc<Core>, outbox: Outbox, secrets:Arc<MemorySecretStore>, client:String }

impl Fixture {
    async fn new(start: bool) -> Self {
        Self::with_mode(start,None).await
    }
    async fn with_mode(start:bool,mode:Option<&str>) -> Self {
        Self::setup(start,mode,true).await
    }
    async fn setup(start:bool,mode:Option<&str>,publish:bool)->Self {
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
        let client=if publish {
            // Create the account namespace through the real Host directory
            // service. The exclusive publisher must not invent a missing owner root.
            core.accounts().start("owner").await.unwrap();
            core.host().stop_account("fixture","owner").await.unwrap();
            let binding=core.accounts().storage_binding_for("owner").await.unwrap();
            core.stage_publisher().copy(binding,crate::managed_stage::CopyDescription {
                stage_id:STAGE.into(),scope:Scope {account:"owner".into(),plugin:"fixture".into(),
                    account_created:owner.created_at,chat:"chat".into(),topic:Some("topic".into())},
                created:1,name:"original.bin".into(),size:15,limit:1024,
            },memory_copy(b"synthetic bytes"),|_|Ok(())).await.unwrap().client_id
        } else {String::new()};
        let outbox = Outbox::for_core(&core, OutboxPolicy::default());
        let fixture = Self { temp, core, outbox, secrets, client };
        if start {
            fixture.core.accounts().start("owner").await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while !crate::outbox::is_online(fixture.core.host().account("fixture","owner").as_ref()) {
                    tokio::task::yield_now().await;
                }
            }).await.unwrap();
            // Ordinary admission/guard fixtures deliberately stay offline, so
            // their rows remain parked while running-account admission is valid.
            // A missing release promise is no longer a reason to disable upload.
            if mode.is_none() {fixture.disconnect().await;}
        }
        fixture
    }

    async fn empty()->Self {
        let f=Self::setup(false,None,false).await;
        std::fs::create_dir_all(f.root()).unwrap();
        std::fs::write(f.root().join("marker"),b"synthetic bytes").unwrap();
        f
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
        assert_eq!(self.core.open_db().unwrap().conn().query_row(
            "SELECT count(*) FROM managed_stage_publication WHERE source_id IS NOT NULL",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    }
    fn assert_no_wire(&self) { assert!(!self.temp.path().join("wire").exists()); }
    fn legacy_receipt(&self, claim: &LiveStageClaim) -> OutboxItem {
        let owner=claim.owner_binding().owner();
        let request=claim.canonical_request().unwrap();
        let g=self.outbox.inner.ingest();
        let tx=g.db().transaction().unwrap();
        let (_,item)=self.outbox.insert_in(&g,&tx,&owner.account_id,&self.client,&request,None).unwrap();
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

struct MemoryCopy(std::io::Cursor<Vec<u8>>);
impl std::io::Read for MemoryCopy {
    fn read(&mut self,buffer:&mut [u8])->std::io::Result<usize> {std::io::Read::read(&mut self.0,buffer)}
}
impl crate::managed_stage::CopyInput for MemoryCopy {
    fn check(&self)->crate::managed_stage::Result<()> {Ok(())}
    fn publication_check(&self)->crate::managed_stage::Result<()> {Ok(())}
}
fn memory_copy(bytes:&[u8])->MemoryCopy {MemoryCopy(std::io::Cursor::new(bytes.to_vec()))}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn actual_core_copy_exclusively_publishes_ready_without_source_registration_or_escaped_flock() {
    let f=Fixture::new(false).await;
    let binding=f.core.accounts().storage_binding_for("owner").await.unwrap();
    let original:Manifest=serde_json::from_slice(&std::fs::read(f.stage().join("manifest.json")).unwrap()).unwrap();
    let id="33333333333333333333333333333333";
    let content=b"actual Core-created synthetic bytes";
    let description=||crate::managed_stage::CopyDescription {stage_id:id.into(),scope:original.scope.clone(),
        created:1,name:"new.bin".into(),size:content.len() as u64,limit:1024};
    let manifest=f.core.stage_publisher().copy(binding.retain_binding(),description(),memory_copy(content),|_|Ok(())).await.unwrap();
    let path=f.core.host().config().upload_dir("owner").join(id);
    assert_eq!(std::fs::read(path.join("bytes")).unwrap(),content);
    assert_eq!(manifest.sha256,format!("{:x}",Sha256::digest(content)));
    assert!(manifest.fingerprint.staged());assert!(manifest.phase==Phase::Ready);
    assert_ne!(manifest.fingerprint.ino,original.fingerprint.ino);
    assert_eq!(f.count("managed_source"),0);assert_eq!(f.count("outbox"),0);
    // The actual creation writer has dropped its byte descriptor/flock before
    // returning metadata; a real independent verifier can immediately claim.
    let proof=VerifiedStageBinding::verify_ready(&binding,id,1024,|_|Ok(())).unwrap();
    drop(proof);
    assert!(f.core.stage_publisher().copy(binding.retain_binding(),description(),memory_copy(b"foreign replacement"),|_|Ok(())).await.is_err());
    assert_eq!(std::fs::read(path.join("bytes")).unwrap(),content);
    let other=Fixture::new(false).await;
    let foreign=other.core.accounts().storage_binding_for("owner").await.unwrap();
    assert!(f.core.stage_mutations().check_binding(&foreign).is_err(),"same owner labels cannot bridge Core issuers");
    assert!(matches!(f.core.stage_publisher().copy(foreign,description(),memory_copy(content),|_|Ok(())).await,
        Err(crate::managed_stage::Error::TargetChanged)));
    other.close().await;drop(binding);f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn publication_metadata_cannot_make_ordinary_owner_removal_forget_unproved_history() {
    // Empty actual namespace: the metadata-only guard must independently refuse.
    let f=Fixture::empty().await;
    f.core.open_db().unwrap().conn().execute("INSERT INTO managed_stage_publication(stage_id,nonce,client_id,
        account_id,plugin_id,account_created_at,path,identity_json,created_at)
        SELECT ?1,?1,?1,account_id,plugin_id,created_at,'/synthetic/unproved/bytes','{}',0
        FROM account WHERE account_id='owner'",[STAGE]).unwrap();
    assert!(matches!(f.core.accounts().remove("owner").await,Err(RuntimeError::SourceInUse)));
    assert!(f.core.accounts().get("owner").await.unwrap().is_some());
    assert_eq!(f.count("managed_stage_publication"),1);assert!(f.root().exists());
    f.core.accounts().add(NewAccount::new("fixture").id("unrelated").no_start()).await.unwrap();
    f.core.accounts().remove("unrelated").await.unwrap();
    assert!(f.core.accounts().get("unrelated").await.unwrap().is_none());
    f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn actual_core_copy_checks_bounded_eof_and_cancel_before_ready_publication() {
    let f=Fixture::new(false).await;
    let binding=f.core.accounts().storage_binding_for("owner").await.unwrap();
    let original:Manifest=serde_json::from_slice(&std::fs::read(f.stage().join("manifest.json")).unwrap()).unwrap();
    let id="44444444444444444444444444444444";
    for (content,size,cancel) in [(&b"short"[..],6,false),(&b"long"[..],3,false),(&b"exact"[..],5,true)] {
        let description=crate::managed_stage::CopyDescription {stage_id:id.into(),scope:original.scope.clone(),
            created:1,name:"new.bin".into(),size,limit:1024};
        let result=f.core.stage_publisher().copy(binding.retain_binding(),description,memory_copy(content),move|count| {
            if cancel && count==size {Err(crate::managed_stage::Error::Cancelled)} else {Ok(())}
        }).await;
        assert!(result.is_err());
        assert!(!f.core.host().config().upload_dir("owner").join(id).exists(),"only the actual fresh incomplete allocation is rolled back");
        assert_eq!(f.count("managed_source"),0);assert_eq!(f.count("outbox"),0);
    }
    drop(binding);f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn actual_core_copy_caller_loss_and_cancelled_shutdown_still_drain_its_real_writer() {
    let f=Fixture::new(false).await;
    let binding=f.core.accounts().storage_binding_for("owner").await.unwrap();
    let original:Manifest=serde_json::from_slice(&std::fs::read(f.stage().join("manifest.json")).unwrap()).unwrap();
    let scope=original.scope.clone();
    let description=crate::managed_stage::CopyDescription {stage_id:"5".repeat(32),scope:original.scope,
        created:1,name:"new.bin".into(),size:5,limit:1024};
    let (release,hook,entered)=pause("copied");
    let publisher=f.core.stage_publisher().clone();
    let caller=tokio::spawn(async move {
        publisher.copy(binding,description,memory_copy(b"exact"),move|count| {
            if count==5 {hook("copied");}Ok(())
        }).await
    });
    entered.await.unwrap();caller.abort();assert!(matches!(caller.await,Err(error) if error.is_cancelled()));
    let closing=f.core.clone();let mut shutdown=tokio::spawn(async move{closing.shutdown().await});
    assert!(tokio::time::timeout(Duration::from_millis(40),&mut shutdown).await.is_err());
    shutdown.abort();assert!(shutdown.await.unwrap_err().is_cancelled());
    let closing=f.core.clone();let mut resumed=tokio::spawn(async move{closing.shutdown().await});
    assert!(tokio::time::timeout(Duration::from_millis(40),&mut resumed).await.is_err());
    release.release();resumed.await.unwrap();
    assert!(!f.core.host().config().upload_dir("owner").join("5".repeat(32)).exists());
    if let Ok(binding)=f.core.accounts().storage_binding_for("owner").await {
        let description=crate::managed_stage::CopyDescription {stage_id:"6".repeat(32),scope,
            created:1,name:"new.bin".into(),size:5,limit:1024};
        assert!(f.core.stage_publisher().copy(binding,description,memory_copy(b"exact"),|_|Ok(())).await.is_err(),
            "an authentic storage binding cannot reopen the closed Core copying service");
        assert!(!f.core.host().config().upload_dir("owner").join("6".repeat(32)).exists());
    }
    f.outbox.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn actual_core_copy_reservations_bound_real_workers_before_allocating_more_stages() {
    let f=Fixture::new(false).await;
    let binding=f.core.accounts().storage_binding_for("owner").await.unwrap();
    let original:Manifest=serde_json::from_slice(&std::fs::read(f.stage().join("manifest.json")).unwrap()).unwrap();
    let mut callers=Vec::new();let mut releases=Vec::new();
    for index in 10..14 {
        let description=crate::managed_stage::CopyDescription {stage_id:format!("{index:032x}"),scope:original.scope.clone(),
            created:1,name:"new.bin".into(),size:5,limit:1024};
        let (release,hook,entered)=pause("copied");releases.push(release);
        let publisher=f.core.stage_publisher().clone();let owner=binding.retain_binding();
        callers.push(tokio::spawn(async move {
            publisher.copy(owner,description,memory_copy(b"exact"),move|count| {
                if count==5 {hook("copied");}Ok(())
            }).await
        }));
        entered.await.unwrap();
    }
    let description=crate::managed_stage::CopyDescription {stage_id:"f".repeat(32),scope:original.scope,
        created:1,name:"overflow.bin".into(),size:5,limit:1024};
    assert!(matches!(f.core.stage_publisher().copy(binding.retain_binding(),description,memory_copy(b"exact"),|_|Ok(())).await,
        Err(crate::managed_stage::Error::Busy)));
    assert!(!f.core.host().config().upload_dir("owner").join("f".repeat(32)).exists());
    for release in releases {release.release();}
    for caller in callers {caller.await.unwrap().unwrap();}
    drop(binding);f.close().await;
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
    let linked:i64=db.conn().query_row("SELECT source_id FROM managed_stage_publication WHERE stage_id=?1",
        [STAGE],|r|r.get(0)).unwrap();
    assert_eq!(Some(linked),ch_db::managed_source_id(db.conn(),STAGE).unwrap().map(|id|id.as_i64()));
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
async fn private_inputs_are_denied_before_initial_admission_and_empty_registry_is_not_a_bypass() {
    let f=Fixture::new(true).await;
    for method in ["messages.send_file","x-fixture.upload"] {
        let error=f.core.host().call_raw("fixture",method,
            serde_json::json!({"account_id":"owner","path":f.stage().join("bytes")}),None).await.unwrap_err();
        assert_ne!(error.error_code(),Some(ch_protocol::ErrorCode::MethodNotFound));
        f.assert_no_wire();
    }
    assert_eq!(f.count("managed_source"),0);
    let outside=f.temp.path().join("independent.bin");std::fs::write(&outside,b"outside").unwrap();
    let error=f.core.host().call_raw("fixture","messages.send_file",
        serde_json::json!({"account_id":"owner","path":outside}),None).await.unwrap_err();
    assert_eq!(error.error_code(),Some(ch_protocol::ErrorCode::MethodNotFound));
    assert_eq!(std::fs::read_to_string(f.temp.path().join("wire")).unwrap(),"messages.send_file");
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
    }).await.unwrap_or_else(|_| {
        let db=f.core.open_db().unwrap();
        let dispatches:Vec<(String,Option<String>)>=db.conn().prepare("SELECT state,result_ids_json FROM managed_dispatch ORDER BY id")
            .unwrap().query_map([],|r|Ok((r.get(0)?,r.get(1)?))).unwrap().collect::<std::result::Result<_,_>>().unwrap();
        panic!("native state did not reach {state:?}: row={:?}, dispatches={dispatches:?}, wire={}, closed={}",
            f.outbox.get(id).unwrap(),f.temp.path().join("wire").exists(),f.temp.path().join("wire.closed").exists());
    });
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
async fn actual_managed_admission_wakes_the_already_idle_account_worker() {
    let f=Fixture::with_mode(true,Some("success")).await;
    let (send,entered)=oneshot::channel();let send=Mutex::new(Some(send));
    *f.outbox.inner.managed_checkpoint.lock().unwrap()=Some(Arc::new(move|phase| {
        if phase=="worker-idle" && let Some(send)=send.lock().unwrap().take() {let _=send.send(());}
    }));
    f.outbox.start().unwrap();entered.await.unwrap();
    // The worker completed its initial empty pass. No restart, status event,
    // second enqueue or manual start may be required to notice this new source.
    let item=f.outbox.enqueue_managed_file_checked(f.claim().await).await.unwrap();
    wait_native_state(&f,item.id,OutboxState::Sent).await;wait_attempt_state(&f,"positive").await;
    assert_eq!(f.count("managed_dispatch"),1);assert_eq!(f.count("message"),1);
    f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn actual_older_and_unpromised_native_peers_deliver_without_a_source_release_certificate() {
    for mode in ["old-peer","old-bool-peer","unknown-release","malformed-release"] {
    let f=Fixture::with_mode(true,Some(mode)).await;
    let item=f.outbox.enqueue_managed_file_checked(f.claim().await).await.unwrap();
    wait_native_state(&f,item.id,OutboxState::Sent).await;wait_attempt_state(&f,"unknown").await;
    let db=f.core.open_db().unwrap();
    let (promise,settled):(Option<String>,Option<i64>)=db.conn().query_row(
        "SELECT d.source_release,s.settled_dispatch_id FROM managed_dispatch d JOIN managed_source s ON s.id=d.source_id",
        [],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
    assert!(promise.is_none());assert!(settled.is_none());
    assert_eq!(db.conn().query_row("SELECT count(*) FROM managed_dispatch WHERE state='positive'",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    assert_eq!(f.outbox.get(item.id).unwrap().result_message_ids,Some(vec!["remote-file".into()]));
    assert_eq!(f.count("message"),1);assert_eq!(f.count("managed_dispatch"),1);
    assert!(f.stage().join("bytes").exists());
    assert!(matches!(f.core.accounts().remove("owner").await,Err(RuntimeError::SourceInUse)));
    assert!(matches!(f.outbox.retry(item.id),Err(OutboxError::ManagedUnavailable)));
    f.close().await;
    }
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
async fn actual_native_reply_certifies_already_sent_exact_ids_without_changing_read_delivery() {
    let f=Fixture::with_mode(true,Some("early")).await;
    let item=f.outbox.enqueue_managed_file_checked(f.claim().await).await.unwrap();
    f.outbox.start().unwrap();
    tokio::time::timeout(Duration::from_secs(5),async {
        while !f.temp.path().join("wire").exists() {tokio::time::sleep(Duration::from_millis(10)).await;}
    }).await.unwrap();
    // Model an independent event/refetch adopting the same remote identity before
    // the actual native RPC reply. This is deliberately not a source certificate.
    let db=f.core.open_db().unwrap();
    let tx=db.transaction().unwrap();
    let row=ch_db::get_outbox(&tx,item.id).unwrap().unwrap();
    let mut echo=ch_db::get_message_by_id(&tx,row.echo_message_id.unwrap()).unwrap().unwrap();
    echo.message_id="remote-file".into();echo.delivery=Some("read".into());
    ch_db::update_message(&tx,&echo).unwrap();
    ch_db::set_outbox_sent(&tx,item.id,&["remote-file".into()]).unwrap();
    tx.commit().unwrap();
    assert_eq!(db.conn().query_row("SELECT count(*) FROM managed_dispatch WHERE state='positive'",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    let before=ch_db::get_message_by_id(db.conn(),echo.id).unwrap().unwrap();
    std::fs::write(f.temp.path().join("wire.release"),b"release actual reader").unwrap();
    wait_attempt_state(&f,"positive").await;
    assert_eq!(ch_db::get_message_by_id(db.conn(),echo.id).unwrap().unwrap(),before,
        "the late certificate is proof-only and cannot rewrite content, timestamps or read delivery");
    assert_eq!(f.count("managed_dispatch"),1);assert_eq!(f.count("message"),1);
    assert_eq!(f.outbox.get(item.id).unwrap().state,OutboxState::Sent);
    assert_eq!(std::fs::read_to_string(f.temp.path().join("wire.closed")).unwrap(),"True");
    f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn actual_native_replacement_between_exposure_and_queue_cannot_use_the_old_permit() {
    let f=Fixture::with_mode(true,Some("success")).await;
    let (release,hook,entered)=pause("native-before-frame");
    *f.outbox.inner.managed_checkpoint.lock().unwrap()=Some(hook);
    let item=f.outbox.enqueue_managed_file_checked(f.claim().await).await.unwrap();
    entered.await.unwrap();
    let db=f.core.open_db().unwrap();
    let old_nonce:String=db.conn().query_row("SELECT nonce FROM managed_producer",[],|r|r.get(0)).unwrap();
    f.assert_no_wire();
    f.core.host().restart_plugin("fixture").await.unwrap();
    tokio::time::timeout(Duration::from_secs(5),async {
        loop {
            if let Ok(producer)=f.core.host().source_producer("fixture","owner").await
                && producer.nonce()!=old_nonce {break;}
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    release.release();
    wait_attempt_state(&f,"no_wire").await;wait_native_state(&f,item.id,OutboxState::Failed).await;
    f.assert_no_wire();
    assert_eq!(f.count("managed_dispatch"),1);
    assert_eq!(db.conn().query_row("SELECT count(*) FROM managed_dispatch WHERE state='positive'",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    assert!(f.stage().join("bytes").exists());
    assert!(matches!(f.outbox.retry(item.id),Err(OutboxError::ManagedUnavailable)));
    f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn actual_native_observer_settles_after_outbox_shutdown_aborts_its_rpc_waiter() {
    let f=Fixture::with_mode(true,Some("early")).await;
    let item=f.outbox.enqueue_managed_file_checked(f.claim().await).await.unwrap();
    f.outbox.start().unwrap();
    tokio::time::timeout(Duration::from_secs(5),async {
        while !f.temp.path().join("wire").exists() {tokio::time::sleep(Duration::from_millis(10)).await;}
    }).await.unwrap();
    let before=std::fs::read(f.temp.path().join("wire")).unwrap();
    tokio::time::timeout(Duration::from_secs(3),f.outbox.shutdown()).await.unwrap();
    assert!(f.outbox.inner.inflight.lock().unwrap().is_empty(),"the scheduler's actual task was aborted");
    assert!(!f.temp.path().join("wire.closed").exists(),"the real native source reader remains open");
    std::fs::write(f.temp.path().join("wire.release"),b"release actual reader").unwrap();
    wait_native_state(&f,item.id,OutboxState::Sent).await;wait_attempt_state(&f,"positive").await;
    assert_eq!(std::fs::read(f.temp.path().join("wire")).unwrap(),before);
    assert_eq!(f.count("managed_dispatch"),1);assert_eq!(f.count("message"),1);
    f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn actual_native_process_exit_never_certifies_helper_scope_or_replays_a_source() {
    let f=Fixture::with_mode(true,Some("exit-after-read")).await;
    let item=f.outbox.enqueue_managed_file_checked(f.claim().await).await.unwrap();
    f.outbox.start().unwrap();wait_native_state(&f,item.id,OutboxState::Failed).await;
    wait_attempt_state(&f,"unknown").await;
    let db=f.core.open_db().unwrap();
    assert_eq!(db.conn().query_row("SELECT count(*) FROM managed_producer WHERE state='drained'",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    assert_eq!(db.conn().query_row("SELECT count(*) FROM managed_dispatch WHERE state='positive'",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    assert_eq!(f.count("managed_dispatch"),1);
    assert!(f.stage().join("bytes").exists());
    assert!(matches!(f.core.accounts().remove("owner").await,Err(RuntimeError::SourceInUse)));
    assert!(matches!(f.outbox.retry(item.id),Err(OutboxError::ManagedUnavailable)));
    f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn exact_legacy_receipt_precedes_spent_changed_bytes_and_missing_run() {
    let f=Fixture::new(false).await;
    f.core.open_db().unwrap().conn().execute("DELETE FROM managed_stage_publication",[]).unwrap();
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
        let (_,item)=f.outbox.insert_in(&g,&tx,"owner",&f.client,&request,None).unwrap();
        tx.commit().unwrap();item
    };
    // All persisted comparator fields match; only the original Gate distinguishes owners.
    assert_eq!(f.outbox.get_matching_admission(&new,&f.client,&request).unwrap(),Some(receipt.clone()));
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
    let f=Fixture::empty().await;
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
        rusqlite::params![STAGE,"22222222222222222222222222222222",old.owner().created_at,f.stage().join("bytes").to_str()]).unwrap();
    release.release();
    assert!(matches!(caller.await.unwrap(),Err(RuntimeError::SourceInUse)));
    assert!(f.core.accounts().get("owner").await.unwrap().is_some());
    assert_eq!(f.count("managed_source"),1);
    assert!(f.root().join("marker").exists());
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
    let Fixture {temp,core,outbox,secrets,..}=f;
    outbox.shutdown().await;core.shutdown().await;drop(outbox);drop(core);
    assert!(matches!(accounts.remove("owner").await,Err(RuntimeError::Host(HostError::ShuttingDown))));
    assert!(ch_db::get_account(ch_db::Db::open(path).unwrap().conn(),"owner").unwrap().is_some());
    assert!(root.join("uploads").join(STAGE).join("bytes").exists());
    assert_eq!(secrets.snapshot("fixture","owner")["session"],"synthetic");
    drop(accounts);drop(temp);
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn cancelled_remove_and_shutdown_wait_for_actual_destructive_worker() {
    let f=Fixture::empty().await;
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
    let f=Fixture::empty().await;
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
    assert_eq!(std::fs::read(previous.join("marker")).unwrap(),b"synthetic bytes");
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
    let f=Fixture::empty().await;
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
    let f=Fixture::empty().await;
    f.secrets.set("fixture","owner","session","synthetic").await.unwrap();
    let other=f.core.accounts().add(NewAccount::new("fixture").id("other").no_start()).await.unwrap();
    let uploads=f.core.host().config().upload_dir("other");
    let id="33333333333333333333333333333333";let stage=uploads.join(id);
    let mutation=f.core.stage_mutations().begin().unwrap();
    std::fs::create_dir_all(&stage).unwrap();
    for dir in [&uploads,&stage] {std::fs::set_permissions(dir,std::fs::Permissions::from_mode(0o700)).unwrap();}
    let bytes=stage.join("bytes");std::fs::write(&bytes,b"synthetic bytes").unwrap();
    std::fs::set_permissions(&bytes,std::fs::Permissions::from_mode(0o400)).unwrap();
    let manifest=Manifest {version:1,stage_id:id.into(),client_id:"44444444444444444444444444444444".into(),
        scope:Scope {account:"other".into(),plugin:"fixture".into(),account_created:other.created_at,
            chat:"chat".into(),topic:None},created:1,name:"other.bin".into(),size:15,format:Format::Other,
        fingerprint:Fingerprint::from(std::fs::metadata(&bytes).unwrap()),
        sha256:format!("{:x}",Sha256::digest(b"synthetic bytes")),phase:Phase::Ready};
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
    assert!(f.root().join("marker").exists());
    assert_eq!(f.secrets.snapshot("fixture","owner")["session"],"synthetic");
    drop(claim);
    f.core.accounts().owner_close_checkpoint(Arc::new(|_|{}));
    // A fresh stable empty target is removable. Other's Claimed bytes stay
    // retained in their actual physical folder rather than globally misattributed.
    tokio::time::timeout(Duration::from_secs(5),f.core.accounts().remove("owner")).await.unwrap().unwrap();
    assert!(f.core.accounts().get("owner").await.unwrap().is_none());
    assert!(bytes.exists());assert!(f.core.accounts().get("other").await.unwrap().is_some());
    f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn fresh_admission_never_retrofits_legacy_ready_without_actual_protected_birth() {
    let f=Fixture::new(true).await;
    f.core.open_db().unwrap().conn().execute("DELETE FROM managed_stage_publication",[]).unwrap();
    let binding=f.core.accounts().storage_binding_for("owner").await.unwrap();
    let proof=VerifiedStageBinding::verify_ready(&binding,STAGE,1024,|_|Ok(())).unwrap();
    assert!(f.core.stage_publisher().check_unadmitted_ready(&proof).await.is_err());drop(proof);
    assert!(f.core.stage_publisher().recover_ready(binding,STAGE.into()).await.is_err());
    assert!(matches!(f.outbox.enqueue_managed_file_checked(f.claim().await).await,
        Err(ManagedAdmissionError::Rejected(OutboxError::AdmissionConflict))));
    f.assert_no_admission();f.assert_no_wire();assert!(f.stage().join("bytes").exists());
    f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn actual_unadmitted_publication_reopens_without_reading_or_automatically_sending() {
    let f=Fixture::new(false).await;
    f.outbox.shutdown().await;f.core.shutdown().await;
    let mut config=CoreConfig::new(f.temp.path(),f.secrets.clone());
    config.host=f.core.host().config().clone();config.autostart=false;
    let core=Core::open(config).await.unwrap();
    assert_eq!(core.managed_sources().census(),(0,0,0));
    f.assert_no_admission();f.assert_no_wire();
    let binding=core.accounts().storage_binding_for("owner").await.unwrap();
    let proof=VerifiedStageBinding::verify_ready(&binding,STAGE,1024,|_|Ok(())).unwrap();
    core.stage_publisher().check_unadmitted_ready(&proof).await.unwrap();drop(proof);drop(binding);
    assert!(matches!(core.accounts().remove("owner").await,Err(RuntimeError::SourceInUse)));
    assert!(f.stage().join("bytes").exists());
    core.shutdown().await;f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn cancellation_after_actual_birth_retains_bytes_and_reports_unknown_instead_of_free_cancel() {
    let f=Fixture::empty().await;
    let binding=f.core.accounts().storage_binding_for("owner").await.unwrap();
    let scope=Scope {account:"owner".into(),plugin:"fixture".into(),account_created:binding.owner().created_at,
        chat:"chat".into(),topic:None};
    let path=f.core.db_path().to_owned();
    let result=f.core.stage_publisher().copy(binding,crate::managed_stage::CopyDescription {
        stage_id:STAGE.into(),scope,created:1,name:"cancelled.bin".into(),size:15,limit:1024,
    },memory_copy(b"synthetic bytes"),move|_| {
        let db=ch_db::Db::open(&path).map_err(|_|crate::managed_stage::Error::Storage)?;
        let count:i64=db.conn().query_row("SELECT count(*) FROM managed_stage_publication",[],|r|r.get(0))
            .map_err(|_|crate::managed_stage::Error::Storage)?;
        if count>0 {Err(crate::managed_stage::Error::Cancelled)} else {Ok(())}
    }).await;
    assert!(matches!(result,Err(crate::managed_stage::Error::Unknown)));
    assert_eq!(f.count("managed_stage_publication"),1);
    assert!(f.stage().join("bytes").exists());assert!(!f.stage().join("manifest.json").exists());
    f.assert_no_admission();f.assert_no_wire();
    // Cold restore itself never reads/sends a source. Explicit Core-owned local
    // recovery can finish this actual birth only after full independent checking.
    f.outbox.shutdown().await;f.core.shutdown().await;
    let mut config=CoreConfig::new(f.temp.path(),f.secrets.clone());
    config.host=f.core.host().config().clone();config.autostart=false;
    let core=Core::open(config).await.unwrap();
    assert!(!f.stage().join("manifest.json").exists());
    let binding=core.accounts().storage_binding_for("owner").await.unwrap();
    let recovered=core.stage_publisher().recover_ready(binding.retain_binding(),STAGE.into()).await.unwrap();
    assert_eq!(recovered.stage_id,STAGE);assert!(recovered.phase==Phase::Ready);
    let again=core.stage_publisher().recover_ready(binding.retain_binding(),STAGE.into()).await.unwrap();
    assert!(again==recovered);f.assert_no_admission();f.assert_no_wire();
    let proof=VerifiedStageBinding::verify_ready(&binding,STAGE,1024,|_|Ok(())).unwrap();
    let claimed=proof.claim(Intent {caption:String::new(),as_file:true,spoiler:false}).unwrap();drop(claimed);
    let prior=std::fs::read(f.stage().join("manifest.json")).unwrap();
    assert!(core.stage_publisher().recover_ready(binding.retain_binding(),STAGE.into()).await.is_err());
    assert_eq!(std::fs::read(f.stage().join("manifest.json")).unwrap(),prior);
    f.assert_no_admission();f.assert_no_wire();drop(binding);core.shutdown().await;f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn full_publication_budget_refuses_before_reading_input_or_allocating_a_stage() {
    let f=Fixture::empty().await;
    let db=f.core.open_db().unwrap();
    // Synthetic unresolved negative history consumes the safety budget. These
    // rows grant no copy/read authority and would independently refuse restore.
    for id in 1..=256 {
        let id=format!("{id:032x}");
        db.conn().execute("INSERT INTO managed_stage_publication(stage_id,nonce,client_id,
            account_id,plugin_id,account_created_at,path,identity_json,created_at)
            SELECT ?1,?1,?1,account_id,plugin_id,created_at,?2,'{}',0 FROM account WHERE account_id='owner'",
            rusqlite::params![id,format!("/synthetic/unresolved/{id}/bytes")]).unwrap();
    }
    let binding=f.core.accounts().storage_binding_for("owner").await.unwrap();
    let scope=Scope {account:"owner".into(),plugin:"fixture".into(),account_created:binding.owner().created_at,
        chat:"chat".into(),topic:None};
    let result=f.core.stage_publisher().copy(binding,crate::managed_stage::CopyDescription {
        stage_id:STAGE.into(),scope,created:1,name:"capacity.bin".into(),size:15,limit:1024,
    },memory_copy(b"synthetic bytes"),|_|panic!("full budget must refuse before copy checkpoint")).await;
    assert!(matches!(result,Err(crate::managed_stage::Error::Busy)));
    assert!(!f.stage().exists());assert_eq!(f.count("managed_stage_publication"),256);
    f.assert_no_admission();f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn local_recovery_never_replaces_changed_bytes_or_bridges_another_core_issuer() {
    let f=Fixture::new(false).await;
    let foreign=Fixture::new(false).await;
    let binding=foreign.core.accounts().storage_binding_for("owner").await.unwrap();
    assert!(matches!(f.core.stage_publisher().recover_ready(binding,STAGE.into()).await,
        Err(crate::managed_stage::Error::TargetChanged)));
    foreign.close().await;
    // Synthetic interrupted local publication followed by byte corruption.
    // The historical fact cannot manufacture a fresh matching inode/hash.
    std::fs::remove_file(f.stage().join("manifest.json")).unwrap();
    let path=f.stage().join("bytes");
    std::fs::set_permissions(&path,std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::write(&path,b"changed payload").unwrap();
    std::fs::set_permissions(&path,std::fs::Permissions::from_mode(0o400)).unwrap();
    let binding=f.core.accounts().storage_binding_for("owner").await.unwrap();
    assert!(f.core.stage_publisher().recover_ready(binding,STAGE.into()).await.is_err());
    assert_eq!(std::fs::read(&path).unwrap(),b"changed payload");
    assert!(!f.stage().join("manifest.json").exists());assert_eq!(f.count("managed_stage_publication"),1);
    f.assert_no_admission();f.assert_no_wire();f.close().await;
}

fn negative_ready_journal(f:&Fixture) {
    // Deliberate unproved SQL intent is only a negative barrier, never a physical
    // result, source permission or completed-retirement provider.
    f.core.open_db().unwrap().conn().execute("INSERT INTO managed_ready_retirement(stage_id,nonce,
        birth_nonce,client_id,account_id,plugin_id,account_created_at,path,identity_json,created_at,updated_at)
        SELECT stage_id,?1,nonce,client_id,account_id,plugin_id,account_created_at,path,identity_json,0,0
        FROM managed_stage_publication WHERE stage_id=?2",rusqlite::params!["f".repeat(32),STAGE]).unwrap();
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn pending_ready_journal_never_grants_recovery_discard_or_fresh_source_admission() {
    let f=Fixture::new(true).await;negative_ready_journal(&f);
    let binding=f.core.accounts().storage_binding_for("owner").await.unwrap();
    let proof=VerifiedStageBinding::verify_ready(&binding,STAGE,1024,|_|Ok(())).unwrap();
    assert!(f.core.stage_publisher().check_unadmitted_ready(&proof).await.is_err());drop(proof);
    assert!(f.core.stage_publisher().recover_ready(binding,STAGE.into()).await.is_err());
    assert!(matches!(f.outbox.enqueue_managed_file_checked(f.claim().await).await,
        Err(ManagedAdmissionError::Rejected(OutboxError::AdmissionConflict))));
    f.assert_no_admission();f.assert_no_wire();assert!(f.stage().join("bytes").exists());
    f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn actual_pending_ready_journal_stops_before_autostart_and_removing_only_intent_reopens() {
    let f=Fixture::new(false).await;negative_ready_journal(&f);
    f.outbox.shutdown().await;f.core.shutdown().await;
    let marker=f.temp.path().join("unexpected-start");
    let config=|| {
        let mut config=CoreConfig::new(f.temp.path(),f.secrets.clone());
        config.host=f.core.host().config().clone();config.autostart=true;
        config.host.env.insert("CH_TEST_LIFETIME_START_MARKER".into(),marker.to_string_lossy().into_owned());
        config
    };
    assert!(matches!(Core::open(config()).await,Err(RuntimeError::ManagedLifetimeRecoveryUnavailable)));
    assert!(!marker.exists());assert!(f.stage().join("bytes").exists());f.assert_no_wire();
    f.core.open_db().unwrap().conn().execute("DELETE FROM managed_ready_retirement",[]).unwrap();
    let core=Core::open(config()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5),async {
        while !marker.exists() {tokio::time::sleep(Duration::from_millis(10)).await;}
    }).await.unwrap();
    f.assert_no_admission();f.assert_no_wire();core.shutdown().await;f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn orphaned_pending_ready_identity_refuses_before_any_new_copy_allocation() {
    let f=Fixture::new(false).await;negative_ready_journal(&f);
    let manifest:Manifest=serde_json::from_slice(&std::fs::read(f.stage().join("manifest.json")).unwrap()).unwrap();
    // Synthetic loss isolates the old journal from normal birth/source guards.
    // It never certifies that old readers closed or earns physical cleanup.
    let db=f.core.open_db().unwrap();
    db.conn().pragma_update(None,"foreign_keys","OFF").unwrap();
    db.conn().execute("DELETE FROM managed_stage_publication",[]).unwrap();
    db.conn().pragma_update(None,"foreign_keys","ON").unwrap();
    let mutation=f.core.stage_mutations().begin().unwrap();
    std::fs::remove_dir_all(f.stage()).unwrap();drop(mutation);
    let binding=f.core.accounts().storage_binding_for("owner").await.unwrap();
    let result=f.core.stage_publisher().copy(binding,crate::managed_stage::CopyDescription {
        stage_id:STAGE.into(),scope:manifest.scope,created:1,name:"replacement.bin".into(),size:15,limit:1024,
    },memory_copy(b"synthetic bytes"),|_|panic!("orphan journal must refuse before file allocation")).await;
    assert!(matches!(result,Err(crate::managed_stage::Error::Integrity)));
    assert!(!f.stage().exists());assert_eq!(f.count("managed_stage_publication"),0);
    assert_eq!(f.count("managed_ready_retirement"),1);f.assert_no_admission();f.assert_no_wire();
    f.close().await;
}
