//! Native producer provenance, current DB writer and real Core lifetime races.
use super::*;
use crate::{host::{EventSink, HostConfig, MemorySecretStore, OwnedEventSink}, runtime::{Core, CoreConfig}};
use ch_protocol::events::Event;
use std::{fs, sync::Condvar, time::Duration};
use tokio::sync::oneshot;

fn config(temp: &tempfile::TempDir, secrets: Arc<MemorySecretStore>) -> CoreConfig {
    let data = temp.path().join("data");
    let mut config = CoreConfig::new(&data, secrets);
    config.autostart = false; config.host = HostConfig::fast(&data, 50);
    for key in ["HOME", "CH_DATA_DIR", "XDG_DATA_HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME"] {
        config.host.env.insert(key.into(), temp.path().join(key).to_string_lossy().into_owned());
    }
    config.host.env.insert("CH_TEST_METADATA_ROOT".into(), temp.path().to_string_lossy().into_owned());
    config
}

struct Fixture { temp: tempfile::TempDir, core: Arc<Core>, secrets: Arc<MemorySecretStore> }
impl Fixture {
    async fn new() -> Self {
        Self::with_birth(None).await
    }
    async fn with_birth(birth: Option<i64>) -> Self {
        let temp = tempfile::tempdir().unwrap(); let plugin = temp.path().join("data/plugins/fixture");
        fs::create_dir_all(&plugin).unwrap();
        fs::write(plugin.join("peer.py"), include_bytes!("../../../tests/fixtures/metadata_origin.py")).unwrap();
        fs::write(plugin.join("plugin.json"), serde_json::json!({"id":"fixture","name":"Fixture",
            "version":"0.0.1","api_version":"1.2.0","entry":["python3","-I","./peer.py"],
            "brand_color":"#123456","multi_account":true,"auth_methods":["none"]}).to_string()).unwrap();
        let secrets = Arc::new(MemorySecretStore::new());
        let core = Arc::new(Core::open(config(&temp, secrets.clone())).await.unwrap());
        if let Some(birth) = birth { fixed_birth(&core, birth); }
        core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
        if birth.is_some() { core.open_db().unwrap().conn().execute_batch("DROP TRIGGER metadata_fixed_birth").unwrap(); }
        core.accounts().start("owner").await.unwrap();
        let f = Self { temp, core, secrets };
        f.pulse("old", "old-state"); f.wait_me("old").await;
        f
    }
    fn pulse(&self, me: &str, detail: &str) {
        fs::write(self.temp.path().join("pulse.json"), serde_json::json!({"state":"connected",
            "detail":detail,"reconnecting":false,"me":{"id":me,"display_name":"Synthetic"}}).to_string()).unwrap();
    }
    async fn wait_me(&self, id: &str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let row = self.core.accounts().get("owner").await.unwrap().unwrap();
                if row.me_user_id.as_deref() == Some(id)
                    && self.core.host().account("fixture", "owner").is_some_and(|info| info.me.as_ref().is_some_and(|me| me.id == id)) { break; }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await.unwrap();
        self.wait_ack(id).await;
    }
    async fn wait_ack(&self, id: &str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !self.temp.path().join(format!("ack-pulse-{id}")).exists() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await.unwrap();
    }
    fn snapshot(&self) -> crate::host::OwnedAccountSnapshot {
        self.core.host().accounts_owned().into_iter().find(|snapshot| snapshot.info().account_id == "owner").unwrap()
    }
    async fn remove_recreate(&self, birth: i64) {
        tokio::time::timeout(Duration::from_secs(5), self.core.accounts().remove("owner")).await.unwrap().unwrap();
        let db = self.core.open_db().unwrap();
        fixed_birth(&self.core, birth);
        let row = self.core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
        db.conn().execute_batch("DROP TRIGGER metadata_fixed_birth").unwrap();
        assert_eq!(row.created_at, birth);
    }
    async fn assert_fresh(&self) {
        let row = self.core.accounts().get("owner").await.unwrap().unwrap();
        assert_eq!(row.state, "disconnected"); assert!(row.state_detail.is_none()); assert!(row.me_user_id.is_none());
        assert!(self.core.host().account("fixture", "owner").is_none());
        let inventory = self.core.accounts().inventory().await.unwrap();
        assert_eq!(inventory.len(), 1); assert!(inventory[0].1.is_none());
    }
}

fn fixed_birth(core: &Core, birth: i64) {
    core.open_db().unwrap().conn().execute_batch(&format!("CREATE TRIGGER metadata_fixed_birth AFTER INSERT ON account
        WHEN NEW.account_id='owner' BEGIN UPDATE account SET created_at={birth} WHERE account_id='owner'; END;")).unwrap();
}

struct Release(Arc<(Mutex<bool>, Condvar)>);
impl Release { fn release(&self) { *self.0.0.lock().unwrap() = true; self.0.1.notify_all(); } }
impl Drop for Release { fn drop(&mut self) { self.release(); } }
fn pause(domain: &LogicalOwners, phase: &'static str) -> (Release, oneshot::Receiver<()>, oneshot::Receiver<()>) {
    let gate = Arc::new((Mutex::new(false), Condvar::new())); let held = gate.clone();
    let (send, receive) = oneshot::channel(); let send = Mutex::new(Some(send));
    let (done, completed) = oneshot::channel(); let done = Mutex::new(Some(done));
    domain.checkpoint(phase, Arc::new(move || {
        let _ = send.lock().unwrap().take().unwrap().send(());
        let mut ready = held.0.lock().unwrap(); while !*ready { ready = held.1.wait(ready).unwrap(); }
    }), Arc::new(move || { let _ = done.lock().unwrap().take().unwrap().send(()); }));
    (Release(gate), receive, completed)
}
fn status(id: &str) -> Event {
    Event::from_params("status", serde_json::json!({"account_id":"owner","state":"connected",
        "detail":"late-native","reconnecting":false,"me":{"id":id,"display_name":"Synthetic"}})).unwrap().unwrap()
}

#[tokio::test(flavor="multi_thread", worker_threads=3)]
async fn valid_queued_state_snapshot_and_lagged_resync_cannot_write_recreated_equal_birth_owner() {
    for resync in [false, true] {
        let f = Fixture::new().await;
        if resync { f.core.stop_host_forwarding_for_test().await; }
        let old = f.snapshot(); let birth = f.core.accounts().get("owner").await.unwrap().unwrap().created_at;
        let (release, entered, completed) = pause(&f.core.accounts().0.logical_owners, "snapshot");
        let task = if resync {
            let accounts = f.core.accounts().clone();
            Some(tokio::spawn(async move { accounts.resync_from_host().await }))
        } else { f.pulse("queued-snapshot", "queued-state"); None };
        tokio::time::timeout(Duration::from_secs(5), entered).await.unwrap().unwrap();
        assert!(!old.origin().is_revoked()); // Previously valid, actual callback already owns its slot.
        f.remove_recreate(birth).await;
        assert!(old.origin().is_revoked()); f.assert_fresh().await;
        release.release();
        // This is the EXACT paused snapshot callback, after its DB guard/tx
        // settled. Ingest's independent ACK does not prove snapshot completion.
        tokio::time::timeout(Duration::from_secs(5), completed).await.unwrap().unwrap();
        if let Some(task) = task { tokio::time::timeout(Duration::from_secs(5), task).await.unwrap().unwrap().unwrap(); }
        else { f.wait_ack("queued-snapshot").await; }
        // A captured late snapshot must also fail through the same direct native validator.
        assert!(!f.core.accounts().persist_owned(old).await.unwrap());
        f.core.accounts().resync_from_host().await.unwrap(); f.assert_fresh().await;
        f.core.accounts().start("owner").await.unwrap(); f.pulse("new", "new-state"); f.wait_me("new").await;
        assert!(f.core.accounts().inventory().await.unwrap()[0].1.is_some());
        f.core.shutdown().await;
    }
}

#[tokio::test(flavor="multi_thread", worker_threads=3)]
async fn actual_queued_ingest_status_cannot_write_state_me_or_contact_after_equal_birth_recreation() {
    let f = Fixture::new().await;
    let old = f.snapshot(); let birth = f.core.accounts().get("owner").await.unwrap().unwrap().created_at;
    let (release, entered, _completed) = pause(&f.core.accounts().0.logical_owners, "event");
    f.pulse("late-ingest", "late-ingest-state");
    tokio::time::timeout(Duration::from_secs(5), entered).await.unwrap().unwrap();
    f.remove_recreate(birth).await; release.release();
    // The actual peer ACK proves the queued native delivery completed, without
    // closing the new owner's domain or treating waiter loss as settlement.
    f.wait_ack("late-ingest").await;
    f.core.accounts().resync_from_host().await.unwrap();
    f.assert_fresh().await;
    let db = f.core.open_db().unwrap();
    let contacts: i64 = db.conn().query_row("SELECT COUNT(*) FROM platform_handle WHERE account_id='owner'", [], |row| row.get(0)).unwrap();
    assert_eq!(contacts, 0);
    assert!(old.origin().is_revoked());
    f.core.shutdown().await;
}

#[tokio::test(flavor="multi_thread", worker_threads=3)]
async fn storage_refusal_rearm_preserves_logical_metadata_and_foreign_or_raw_inputs_refuse() {
    let f = Fixture::new().await;
    let foreign = Fixture::with_birth(Some(f.core.accounts().get("owner").await.unwrap().unwrap().created_at)).await;
    let old = f.snapshot(); let storage = f.core.accounts().storage_observation_for("owner").await.unwrap();
    let uploads = f.core.host().config().upload_dir("owner"); fs::create_dir_all(&uploads).unwrap();
    fs::write(uploads.join("unproved"), b"preserve").unwrap();
    assert!(matches!(tokio::time::timeout(Duration::from_secs(5), f.core.accounts().remove("owner")).await.unwrap(), Err(RuntimeError::SourceInUse)));
    assert!(storage.is_revoked()); assert!(!old.origin().is_revoked());
    f.pulse("same-owner-after-rearm", "valid"); f.wait_me("same-owner-after-rearm").await;
    assert!(old.origin().same(f.snapshot().origin()));
    assert!(f.core.accounts().inventory().await.unwrap()[0].1.is_some());
    assert!(!f.core.accounts().persist_owned(foreign.snapshot()).await.unwrap());
    f.core.ingest().deliver_owned("fixture", foreign.snapshot().origin().clone(), status("foreign")).await.unwrap();
    assert!(f.core.ingest().deliver("fixture", status("raw")).await.is_err());
    assert!(f.core.accounts().on_host_change(&StateChange::Account(old.info().clone())).await.is_err());
    assert_eq!(f.core.accounts().get("owner").await.unwrap().unwrap().me_user_id.as_deref(), Some("same-owner-after-rearm"));
    assert_eq!(fs::read(uploads.join("unproved")).unwrap(), b"preserve");
    foreign.core.shutdown().await; f.core.shutdown().await;
}

#[tokio::test(flavor="multi_thread", worker_threads=3)]
async fn unknown_actual_delete_commit_never_heals_from_recreated_identical_row() {
    let f = Fixture::new().await; let old = f.snapshot();
    let birth = f.core.accounts().get("owner").await.unwrap().unwrap().created_at;
    *f.core.accounts().0.delete_commit_checkpoint.lock().unwrap() = Some(Arc::new(move |conn| {
        // A real premature SQL COMMIT makes Transaction::commit report failure.
        // Recreate equal-looking metadata before the deleting worker regains
        // control; this is fault injection, never a published owner proof.
        conn.execute_batch("COMMIT").unwrap();
        let row = ch_db::create_account(conn, "owner", "fixture", None, &Default::default()).unwrap();
        assert_eq!(row.account_id, "owner");
        conn.execute("UPDATE account SET created_at=?1 WHERE account_id='owner'", [birth]).unwrap();
    }));
    assert!(tokio::time::timeout(Duration::from_secs(5), f.core.accounts().remove("owner")).await.unwrap().is_err());
    assert!(old.origin().is_revoked());
    assert!(!f.core.accounts().persist_owned(old.clone()).await.unwrap());
    f.core.ingest().deliver_owned("fixture", old.origin().clone(), status("late")).await.unwrap();
    let row = f.core.accounts().get("owner").await.unwrap().unwrap(); assert_eq!(row.created_at, birth);
    assert!(f.core.accounts().0.logical_owners.capture(&row).is_none());
    assert!(row.me_user_id.is_none()); assert!(f.core.accounts().inventory().await.unwrap()[0].1.is_none());
    f.core.shutdown().await;
}

#[tokio::test(flavor="multi_thread", worker_threads=3)]
async fn positively_rolled_back_delete_retains_logical_epoch_while_old_storage_gate_stays_revoked() {
    let f = Fixture::new().await; let old = f.snapshot();
    let storage = f.core.accounts().storage_observation_for("owner").await.unwrap();
    let db = f.core.open_db().unwrap();
    db.conn().execute_batch("CREATE TRIGGER metadata_refuse_delete BEFORE DELETE ON account
        BEGIN SELECT RAISE(ABORT, 'synthetic refusal'); END;").unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(5), f.core.accounts().remove("owner")).await.unwrap().is_err());
    assert!(storage.is_revoked()); assert!(!old.origin().is_revoked());
    db.conn().execute_batch("DROP TRIGGER metadata_refuse_delete").unwrap();
    // Strict stop was positively confirmed before the SQL refusal. Fresh
    // explicit start can use the new storage Gate with the SAME logical epoch.
    f.core.accounts().start("owner").await.unwrap();
    f.pulse("after-known-rollback", "valid"); f.wait_me("after-known-rollback").await;
    assert!(old.origin().same(f.snapshot().origin()));
    f.core.shutdown().await;
}

#[tokio::test(flavor="multi_thread", worker_threads=3)]
async fn dropping_core_revokes_only_metadata_and_retained_handles_cannot_mint_or_write() {
    let f = Fixture::new().await; let old = f.snapshot();
    let Fixture { temp, core, secrets } = f;
    let accounts = core.accounts().clone(); let sink = core.ingest().clone(); let host = core.host().clone();
    drop(core);
    assert!(old.origin().is_revoked());
    assert!(!accounts.persist_owned(old.clone()).await.unwrap());
    sink.deliver_owned("fixture", old.origin().clone(), status("after-drop")).await.unwrap();
    let row = accounts.get("owner").await.unwrap().unwrap();
    assert_ne!(row.me_user_id.as_deref(), Some("after-drop"));
    assert!(accounts.0.logical_owners.capture(&row).is_none());
    // Explicit test cleanup: synchronous Core Drop never claimed process or
    // physical-reader closure, nor a synchronous worker drain.
    tokio::time::timeout(Duration::from_secs(5), host.shutdown()).await.unwrap();
    accounts.0.logical_owners.drain().await.unwrap();
    let reopened = Core::open(config(&temp, secrets)).await.unwrap();
    reopened.shutdown().await;
}

#[tokio::test(flavor="multi_thread", worker_threads=3)]
async fn cancelled_shutdown_and_drainer_keep_actual_metadata_work_owned_and_old_handles_closed() {
    let f = Fixture::new().await; let old = f.snapshot(); let domain = f.core.accounts().0.logical_owners.clone();
    let (release, entered, completed) = pause(&domain, "event"); f.pulse("paused-shutdown", "paused");
    tokio::time::timeout(Duration::from_secs(5), entered).await.unwrap().unwrap();
    let core = f.core.clone(); let shutdown = tokio::spawn(async move { core.shutdown().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !matches!(f.core.host().plugin_state("fixture"), Some(crate::host::PluginState::Stopped)) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }).await.unwrap();
    shutdown.abort(); assert!(shutdown.await.unwrap_err().is_cancelled());
    // Retry can finish Host teardown/resync, but the actual callback still owns
    // its permit and cannot be discharged by cancelling this second drainer.
    let core = f.core.clone(); let mut shutdown = tokio::spawn(async move { core.shutdown().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !old.origin().is_revoked() { tokio::task::yield_now().await; }
    }).await.unwrap();
    // This same valid status can already have reached the row through its
    // independent native snapshot or the legitimate final shutdown resync.
    // The boundary under test is NO CHANGE after issuer close, not absence of
    // metadata that was positively admitted for this owner before close.
    let baseline = f.core.accounts().get("owner").await.unwrap().unwrap();
    let handles = || -> i64 {
        f.core.open_db().unwrap().conn().query_row("SELECT COUNT(*) FROM platform_handle
            WHERE account_id='owner' AND user_id IN ('paused-shutdown', 'after-close')", [], |row| row.get(0)).unwrap()
    };
    // Only the paused Ingest event would create this Contact identity; the
    // snapshot/resync path records me_user_id without inserting a handle.
    assert_eq!(handles(), 0);
    assert!(tokio::time::timeout(Duration::from_millis(25), &mut shutdown).await.is_err());
    shutdown.abort(); assert!(shutdown.await.unwrap_err().is_cancelled());
    let retry = domain.clone(); let mut drain = tokio::spawn(async move { retry.drain().await });
    assert!(tokio::time::timeout(Duration::from_millis(25), &mut drain).await.is_err());
    release.release();
    tokio::time::timeout(Duration::from_secs(5), completed).await.unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(5), drain).await.unwrap().unwrap().unwrap();
    assert_eq!(f.core.accounts().get("owner").await.unwrap().unwrap(), baseline);
    assert_eq!(handles(), 0);
    f.core.shutdown().await;
    let reopened = Core::open(config(&f.temp, f.secrets.clone())).await.unwrap();
    assert!(!f.core.accounts().persist_owned(old.clone()).await.unwrap());
    f.core.ingest().deliver_owned("fixture", old.origin().clone(), status("after-close")).await.unwrap();
    let row = reopened.accounts().get("owner").await.unwrap().unwrap();
    assert_eq!(row, baseline);
    assert_eq!(handles(), 0);
    assert!(f.core.accounts().0.logical_owners.capture(&row).is_none());
    reopened.shutdown().await;
}
