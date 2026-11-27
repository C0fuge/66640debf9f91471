use super::*;
use crate::host::{HostConfig, HostError, MemorySecretStore, SecretStore};
use crate::managed_stage::{VerifiedStageBinding, manifest::{Fingerprint, Format, Intent, Manifest, Phase, Scope}};
use crate::runtime::{CoreConfig, NewAccount, RuntimeError};
use sha2::{Digest, Sha256};
use std::os::unix::fs::PermissionsExt;
use std::sync::Condvar;
use std::time::Duration;
use tokio::sync::oneshot;

const STAGE: &str = "11111111111111111111111111111111";
const CLIENT: &str = "22222222222222222222222222222222";

struct Fixture { temp: tempfile::TempDir, core: Arc<Core>, outbox: Outbox, secrets:Arc<MemorySecretStore> }

impl Fixture {
    async fn new(start: bool) -> Self {
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
    // The real owned removal can finish while that worker is still paused.
    tokio::time::timeout(Duration::from_secs(3),f.core.accounts().remove("owner")).await.unwrap().unwrap();
    assert!(!f.stage().exists());
    assert_eq!(f.core.managed_sources().census(),(0,0,0));
    release.release();
    f.core.managed_sources().drain().await.unwrap();
    f.assert_no_wire();f.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn fallback_lookup_cannot_return_a_recreated_owners_exact_looking_receipt() {
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
    tokio::time::timeout(Duration::from_secs(3),f.core.accounts().remove("owner")).await.unwrap().unwrap();
    f.core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
    let db=f.core.open_db().unwrap();
    db.conn().execute("UPDATE account SET created_at=?1 WHERE account_id='owner'",
        [old.owner().created_at]).unwrap();
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
            removed.unwrap();
            assert!(f.core.accounts().get("owner").await.unwrap().is_none());
            assert!(!f.root().exists());
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
async fn ordinary_no_source_removal_with_parent_anchored_absence_clears_close_state() {
    let f=Fixture::new(false).await;
    std::fs::remove_dir_all(f.root().parent().unwrap()).unwrap();
    f.core.accounts().remove("owner").await.unwrap();
    assert!(f.core.accounts().get("owner").await.unwrap().is_none());
    assert_eq!(f.core.managed_sources().owner_barriers().closing_count(),0);
    assert_eq!(f.core.managed_sources().census(),(0,0,0));
    f.assert_no_wire();f.close().await;
}
