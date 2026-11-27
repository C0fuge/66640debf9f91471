use super::*;
use crate::{host::{AccountPhase, AccountSpec, MemorySecretStore, NullSink}, runtime::{Core, CoreConfig, NewAccount}};
use std::{fs, sync::{Condvar, Mutex, atomic::{AtomicUsize, Ordering}}, time::Duration};

struct Fixture { _temp: tempfile::TempDir, core: Arc<Core>, secrets: Arc<MemorySecretStore> }
impl Fixture {
    async fn new() -> Self { Self::configured(None).await }
    async fn configured(mode: Option<&str>) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("data");
        let plugin = data.join("plugins/fixture");
        fs::create_dir_all(&plugin).unwrap();
        let peer: &[u8] = if mode.is_some() { include_bytes!("../../tests/fixtures/removal_stop.py") }
            else { include_bytes!("../../tests/fixtures/account_directories.py") };
        fs::write(plugin.join("peer.py"), peer).unwrap();
        fs::write(plugin.join("plugin.json"), serde_json::json!({
            "id":"fixture","name":"Fixture","version":"0.0.1","api_version":"1.2.0",
            "entry":["python3","-I","./peer.py"],"brand_color":"#123456",
            "multi_account":true,"auth_methods":["none"]
        }).to_string()).unwrap();
        let secrets = Arc::new(MemorySecretStore::new());
        let mut config = CoreConfig::new(&data, secrets.clone());
        config.host = HostConfig::fast(&data, 50);
        if matches!(mode, Some("delayed-start" | "delayed-stop")) { config.host.timeout_scale = 0.1; }
        for key in ["CH_DATA_DIR", "HOME", "XDG_DATA_HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME"] {
            config.host.env.insert(key.into(), temp.path().join(key).to_string_lossy().into_owned());
        }
        config.host.env.insert("CH_TEST_ACCOUNT_STARTS".into(), temp.path().join("starts.log").to_string_lossy().into_owned());
        if let Some(mode) = mode { config.host.env.insert("CH_TEST_REMOVAL_MODE".into(), mode.into()); }
        config.autostart = false;
        let core = Arc::new(Core::open(config).await.unwrap());
        core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
        Self { _temp: temp, core, secrets }
    }
    fn directories(&self) -> Arc<crate::host::directories::AccountDirectories> {
        self.core.host().0.directories.as_ref().unwrap().clone()
    }
    fn starts(&self) -> Vec<String> {
        match fs::read_to_string(self._temp.path().join("starts.log")) {
            Ok(value) => value.lines().map(str::to_owned).collect(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => panic!("isolated recorder: {error}"),
        }
    }
    async fn scope(&self, account: &str) -> crate::host::start_scope::StartScope {
        if self.core.accounts().get(account).await.unwrap().is_none() {
            self.core.accounts().add(NewAccount::new("fixture").id(account).no_start()).await.unwrap();
        }
        self.core.host().capture_start("fixture", account).await.unwrap()
    }
    async fn close(self) { self.directories().checkpoint(None); self.core.shutdown().await; }
}

async fn until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !predicate() { tokio::time::sleep(Duration::from_millis(1)).await; }
    }).await.unwrap();
}

struct Release(Arc<(Mutex<bool>, Condvar)>);
impl Release { fn release(&self) { *self.0.0.lock().unwrap() = true; self.0.1.notify_all(); } }
impl Drop for Release { fn drop(&mut self) { self.release(); } }

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_explicit_start_and_handshake_replay_share_local_directory_tracking() {
    let f = Fixture::new().await;
    let catalog = f.core.managed_sources().catalog();
    catalog.refresh().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone(); let observed = catalog.clone();
    f.directories().checkpoint(Some(Arc::new(move |phase| {
        if phase != "writing" { return; }
        assert_eq!(observed.view().writers(), 1);
        assert!(!observed.view().complete());
        counter.fetch_add(1, Ordering::SeqCst);
    })));
    f.core.accounts().start("owner").await.unwrap();
    until(|| f.core.host().account("fixture", "owner").is_some_and(|info| info.phase == AccountPhase::Running)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(catalog.view().writers(), 0);
    assert!(!catalog.view().complete());
    let config = f.core.host().config();
    assert!(config.state_dir("owner").is_dir() && config.cache_dir("owner").is_dir());
    // Synthetic disappearance; subsequent replay must actually recreate both.
    let mutation = f.core.stage_mutations().begin().unwrap();
    fs::remove_dir_all(config.state_dir("owner")).unwrap();
    fs::remove_dir_all(config.cache_dir("owner")).unwrap();
    drop(mutation);
    catalog.refresh().unwrap();
    f.core.host().restart_plugin("fixture").await.unwrap();
    until(|| calls.load(Ordering::SeqCst) == 2 && f.core.host().account("fixture", "owner")
        .is_some_and(|info| info.phase == AccountPhase::Running)).await;
    assert!(config.state_dir("owner").is_dir() && config.cache_dir("owner").is_dir());
    assert_eq!(catalog.view().writers(), 0); // No token on the account.start network lifetime.
    assert_eq!(f.starts(), vec!["owner", "owner"]);
    f.core.managed_sources().close();
    assert!(matches!(f.core.host().start_account("fixture", "fresh", AccountSpec::default()).await,
        Err(HostError::ShuttingDown)));
    assert!(!config.state_dir("fresh").parent().unwrap().exists());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(f.core.host().account("fixture", "fresh").is_none()); // Closed before queue capture.
    assert!(f.core.host().running_account_token("fixture", "fresh").is_none());
    f.core.host().restart_plugin("fixture").await.unwrap();
    until(|| f.core.host().account("fixture", "owner").is_some_and(|info|
        info.phase == AccountPhase::Stopped && info.detail.as_deref() == Some("Account start was cancelled; start the account again"))).await;
    let replay = f.core.host().account("fixture", "owner").unwrap();
    assert!(replay.wanted);
    assert!(f.core.host().running_account_token("fixture", "owner").is_none());
    assert_eq!(f.starts(), vec!["owner", "owner"]); // Neither refused path sent a frame.
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_directory_waiter_and_cancelled_drain_retain_actual_blocking_work() {
    let f = Fixture::new().await;
    let catalog = f.core.managed_sources().catalog();
    catalog.refresh().unwrap();
    let pending = f.scope("pending").await;
    let fresh = f.scope("fresh").await;
    let gate = Arc::new((Mutex::new(false), Condvar::new())); let release = Release(gate.clone());
    let (entered_tx, entered_rx) = oneshot::channel(); let entered = Mutex::new(Some(entered_tx));
    f.directories().checkpoint(Some(Arc::new(move |phase| {
        if phase != "writing" { return; }
        let _ = entered.lock().unwrap().take().unwrap().send(());
        let mut ready = gate.0.lock().unwrap();
        while !*ready { ready = gate.1.wait(ready).unwrap(); }
    })));
    let directories = f.directories(); let config = f.core.host().config().clone();
    let caller = tokio::spawn(async move { directories.prepare(&config, "pending", &pending).await });
    tokio::time::timeout(Duration::from_secs(5), entered_rx).await.unwrap().unwrap();
    caller.abort();
    assert!(tokio::time::timeout(Duration::from_secs(5), caller).await.unwrap().unwrap_err().is_cancelled());
    assert_eq!(catalog.view().writers(), 1);
    assert!(!f.core.host().config().state_dir("pending").parent().unwrap().exists());
    f.core.managed_sources().close();
    assert!(matches!(f.directories().prepare(f.core.host().config(), "fresh", &fresh).await, Err(HostError::ShuttingDown)));
    let sources = f.core.managed_sources(); let mut drain = tokio::spawn(async move { sources.drain().await });
    assert!(tokio::time::timeout(Duration::from_millis(25), &mut drain).await.is_err());
    drain.abort();
    assert!(tokio::time::timeout(Duration::from_secs(5), drain).await.unwrap().unwrap_err().is_cancelled());
    let sources = f.core.managed_sources(); let mut retry = tokio::spawn(async move { sources.drain().await });
    assert!(tokio::time::timeout(Duration::from_millis(25), &mut retry).await.is_err());
    release.release();
    tokio::time::timeout(Duration::from_secs(5), retry).await.unwrap().unwrap().unwrap();
    // Source close cancelled the original scope while its real worker was
    // paused before first mkdir; settlement releases handles without creating.
    assert!(!f.core.host().config().state_dir("pending").parent().unwrap().exists());
    assert!(!f.core.host().config().state_dir("fresh").parent().unwrap().exists());
    assert_eq!(catalog.view().writers(), 0); assert!(!catalog.view().complete());
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturated_directory_lane_waits_for_capacity_for_start_and_handshake_replay() {
    let f = Fixture::new().await;
    f.core.host().enable_plugin("fixture").await.unwrap();
    for replay in [false, true] {
        let gate = Arc::new((Mutex::new(false), Condvar::new())); let release = Release(gate.clone());
        let entered = Arc::new(AtomicUsize::new(0)); let count = entered.clone();
        f.directories().checkpoint(Some(Arc::new(move |phase| {
            if phase != "writing" { return; }
            count.fetch_add(1, Ordering::SeqCst);
            let mut ready = gate.0.lock().unwrap();
            while !*ready { ready = gate.1.wait(ready).unwrap(); }
        })));
        let mut occupants = Vec::new();
        for number in 0..4 {
            let directories = f.directories(); let config = f.core.host().config().clone();
            let account = format!("occupant-{number}"); let scope = f.scope(&account).await;
            occupants.push(tokio::spawn(async move { directories.prepare(&config, &account, &scope).await }));
        }
        until(|| entered.load(Ordering::SeqCst) == 4).await;
        let (waiting_tx, waiting_rx) = oneshot::channel(); let waiting = Mutex::new(Some(waiting_tx));
        f.directories().checkpoint(Some(Arc::new(move |phase| {
            if phase == "waiting" && let Some(send) = waiting.lock().unwrap().take() { let _ = send.send(()); }
        })));
        let mut start = if replay {
            f.core.host().restart_plugin("fixture").await.unwrap();
            None
        } else {
            let accounts = f.core.accounts().clone();
            Some(tokio::spawn(async move { accounts.start("owner").await }))
        };
        // The actual shared prepare is waiting before any spawn. Timeout drops
        // Release too, so a missing hook cannot strand the blocked workers.
        tokio::time::timeout(Duration::from_secs(5), waiting_rx).await.unwrap().unwrap();
        if replay {
            let info = f.core.host().account("fixture", "owner").unwrap();
            assert_eq!(info.phase, AccountPhase::Stopped); assert!(info.wanted);
        } else if let Some(start) = &mut start {
            assert!(tokio::time::timeout(Duration::from_millis(25), start).await.is_err());
        }
        assert_eq!(f.core.managed_sources().catalog().view().writers(), 4);
        release.release();
        for occupant in occupants {
            tokio::time::timeout(Duration::from_secs(5), occupant).await.unwrap().unwrap().unwrap();
        }
        f.directories().checkpoint(None);
        if let Some(start) = start {
            tokio::time::timeout(Duration::from_secs(5), start).await.unwrap().unwrap().unwrap();
        }
        until(|| f.core.host().account("fixture", "owner").is_some_and(|info| info.phase == AccountPhase::Running)).await;
    }
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_cancels_slot_waiter_promptly_and_cancelled_waiter_never_creates_dirs() {
    let f = Fixture::new().await;
    let scope = f.scope("never-created").await;
    let gate = Arc::new((Mutex::new(false), Condvar::new())); let release = Release(gate.clone());
    let entered = Arc::new(AtomicUsize::new(0)); let count = entered.clone();
    f.directories().checkpoint(Some(Arc::new(move |phase| {
        if phase != "writing" { return; }
        count.fetch_add(1, Ordering::SeqCst);
        let mut ready = gate.0.lock().unwrap();
        while !*ready { ready = gate.1.wait(ready).unwrap(); }
    })));
    let mut occupants = Vec::new();
    for number in 0..4 {
        let directories = f.directories(); let config = f.core.host().config().clone();
        let account = format!("occupant-{number}"); let scope = f.scope(&account).await;
        occupants.push(tokio::spawn(async move { directories.prepare(&config, &account, &scope).await }));
    }
    until(|| entered.load(Ordering::SeqCst) == 4).await;
    for cancel_caller in [true, false] {
        let (waiting_tx, waiting_rx) = oneshot::channel(); let waiting = Mutex::new(Some(waiting_tx));
        f.directories().checkpoint(Some(Arc::new(move |phase| {
            if phase == "waiting" { let _ = waiting.lock().unwrap().take().unwrap().send(()); }
        })));
        let directories = f.directories(); let config = f.core.host().config().clone();
        let scope = scope.clone();
        let mut waiter = tokio::spawn(async move { directories.prepare(&config, "never-created", &scope).await });
        tokio::time::timeout(Duration::from_secs(5), waiting_rx).await.unwrap().unwrap();
        if cancel_caller {
            waiter.abort();
            assert!(tokio::time::timeout(Duration::from_secs(5), waiter).await.unwrap().unwrap_err().is_cancelled());
        } else {
            f.core.managed_sources().close();
            assert!(matches!(tokio::time::timeout(Duration::from_millis(100), &mut waiter).await.unwrap().unwrap(),
                Err(HostError::ShuttingDown)));
        }
        assert_eq!(f.core.managed_sources().catalog().view().writers(), 4);
        assert!(!f.core.host().config().state_dir("never-created").parent().unwrap().exists());
    }
    release.release();
    for occupant in occupants {
        assert!(matches!(tokio::time::timeout(Duration::from_secs(5), occupant).await.unwrap().unwrap(),
            Err(HostError::ShuttingDown)));
    }
    for number in 0..4 {
        assert!(!f.core.host().config().state_dir(&format!("occupant-{number}")).parent().unwrap().exists());
    }
    assert!(!f.core.host().config().state_dir("never-created").parent().unwrap().exists());
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_standalone_host_keeps_unbound_directory_behavior() {
    let f = Fixture::new().await;
    let standalone = HostHandle::new(f.core.host().config().clone(), Arc::new(NullSink), Arc::new(MemorySecretStore::new()));
    assert!(standalone.0.directories.is_none());
    f.core.managed_sources().close();
    standalone.start_account("fixture", "standalone", AccountSpec::default()).await.unwrap();
    assert!(standalone.config().state_dir("standalone").is_dir());
    assert!(standalone.config().cache_dir("standalone").is_dir());
    standalone.shutdown().await; f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_directory_io_failure_keeps_existing_host_start_behavior() {
    let f = Fixture::new().await;
    let config = f.core.host().config();
    let mutation = f.core.stage_mutations().begin().unwrap();
    fs::create_dir_all(config.cache_dir("owner").parent().unwrap()).unwrap();
    fs::write(config.cache_dir("owner"), b"synthetic collision").unwrap();
    drop(mutation);
    // Existing Host behavior warns on mkdir IO errors and still sends start.
    f.core.accounts().start("owner").await.unwrap();
    until(|| f.core.host().account("fixture", "owner").is_some_and(|info| info.phase == AccountPhase::Running)).await;
    assert!(config.state_dir("owner").is_dir());
    assert_eq!(fs::read(config.cache_dir("owner")).unwrap(), b"synthetic collision");
    assert_eq!(f.core.managed_sources().catalog().view().writers(), 0);
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_capture_caller_loss_keeps_actual_accounts_db_worker_counted() {
    let f = Fixture::new().await;
    let gate = Arc::new((Mutex::new(false), Condvar::new())); let release = Release(gate.clone());
    let (entered_tx, entered_rx) = oneshot::channel(); let entered = Mutex::new(Some(entered_tx));
    f.core.accounts().host_capture_checkpoint(Arc::new(move || {
        let _ = entered.lock().unwrap().take().unwrap().send(());
        let mut ready = gate.0.lock().unwrap();
        while !*ready { ready = gate.1.wait(ready).unwrap(); }
    }));
    let host = f.core.host().clone();
    let caller = tokio::spawn(async move { host.start_account("fixture", "owner", AccountSpec::default()).await });
    tokio::time::timeout(Duration::from_secs(5), entered_rx).await.unwrap().unwrap();
    caller.abort();
    assert!(tokio::time::timeout(Duration::from_secs(5), caller).await.unwrap().unwrap_err().is_cancelled());
    f.core.managed_sources().close();
    let sources = f.core.managed_sources(); let mut drain = tokio::spawn(async move { sources.drain().await });
    assert!(tokio::time::timeout(Duration::from_millis(25), &mut drain).await.is_err());
    release.release();
    tokio::time::timeout(Duration::from_secs(5), drain).await.unwrap().unwrap().unwrap();
    assert!(!f.core.host().config().state_dir("owner").parent().unwrap().exists());
    assert!(f.starts().is_empty());
    assert!(f.core.host().account("fixture", "owner").is_none());
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn equal_birth_recreated_owner_rejects_original_queued_scope_and_actual_replay() {
    for replay in [false, true] {
        let f = Fixture::new().await;
        let old = f.scope("owner").await;
        let saved = if replay {
            f.core.accounts().start("owner").await.unwrap();
            until(|| f.core.host().account("fixture", "owner").is_some_and(|info| info.phase == AccountPhase::Running)).await;
            Some(f.core.host().0.registry.lock().get("fixture").unwrap().accounts["owner"].clone())
        } else { None };
        let birth = old.owner.as_ref().unwrap().observed.owner().created_at;
        f.core.accounts().remove("owner").await.unwrap();
        f.core.open_db().unwrap().conn().execute_batch(&format!("CREATE TRIGGER equal_birth_owner AFTER INSERT ON account
            WHEN NEW.account_id='owner' BEGIN UPDATE account SET created_at={birth} WHERE account_id=NEW.account_id; END;")).unwrap();
        f.core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
        f.core.open_db().unwrap().conn().execute_batch("DROP TRIGGER equal_birth_owner").unwrap();
        assert!(!old.live());
        assert!(!f.core.host().config().state_dir("owner").parent().unwrap().exists());
        if let Some(saved) = saved {
            // Restore only a real frozen in-memory wish, never a forged owner
            // or a fresh by-ID observation. Restart must reject that old Gate.
            f.core.host().0.registry.lock().get_mut("fixture").unwrap().accounts.insert("owner".into(), saved);
            f.core.host().restart_plugin("fixture").await.unwrap();
            until(|| f.core.host().0.registry.lock().get("fixture").unwrap().accounts.get("owner").is_some_and(|record|
                record.info.phase == AccountPhase::Stopped && record.info.detail.as_deref() == Some("Account start was cancelled; start the account again"))).await;
            assert!(f.core.host().account("fixture", "owner").is_none());
            assert_eq!(f.starts(), vec!["owner"]);
        } else {
            // Ordinary stop's legitimate placeholder is independent of strict
            // destructive removal, which does not manufacture a local record.
            f.core.host().stop_account("fixture", "owner").await.unwrap();
            let command = f.core.host().ensure_supervisor("fixture").unwrap();
            // The stale start must preserve every field of this placeholder,
            // not mistake its pre-existing presence for stale resurrection.
            for missing in [false, true] {
                if missing { f.core.host().0.registry.lock().get_mut("fixture").unwrap().accounts.remove("owner"); }
                let before = f.core.host().0.registry.lock().get("fixture").unwrap().accounts.get("owner").cloned();
                assert_eq!(before.is_none(), missing);
                let mut changes = f.core.host().0.registry.changes.subscribe();
                let (send, recv) = oneshot::channel();
                command.send(Command::StartAccount { account_id: "owner".into(), spec: AccountSpec::default(), scope: old.clone(), reply: send }).await.unwrap();
                assert!(matches!(tokio::time::timeout(Duration::from_secs(5), recv).await.unwrap().unwrap(), Err(HostError::ShuttingDown)));
                let after = f.core.host().0.registry.lock().get("fixture").unwrap().accounts.get("owner").cloned();
                match (before, after) {
                    (Some(before), Some(after)) => assert!(before.same_stopped_record(&after)),
                    (None, None) => {},
                    _ => panic!("stale start changed registry membership"),
                }
                loop {
                    match changes.try_recv() {
                        Ok(StateChange::Account(_)) => panic!("stale start broadcast account metadata"),
                        Ok(_) => {}, // Initial handshake/plugin-state changes are independent.
                        Err(broadcast::error::TryRecvError::Empty) => break,
                        Err(error) => panic!("registry observer: {error}"),
                    }
                }
                assert!(f.starts().is_empty());
                assert!(!f.core.host().config().state_dir("owner").parent().unwrap().exists());
            }
        }
        assert!(!f.core.host().config().state_dir("owner").parent().unwrap().exists());
        f.core.accounts().start("owner").await.unwrap();
        until(|| f.core.host().account("fixture", "owner").is_some_and(|info| info.phase == AccountPhase::Running)).await;
        assert_eq!(f.starts().len(), if replay { 2 } else { 1 });
        f.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_mkdir_read_lease_blocks_removal_until_its_revoked_worker_settles() {
    let f = Fixture::new().await;
    let scope = f.scope("owner").await;
    let original = scope.owner.as_ref().unwrap().observed.clone();
    let gate = Arc::new((Mutex::new(false), Condvar::new())); let release = Release(gate.clone());
    let (entered_tx, entered_rx) = oneshot::channel(); let entered = Mutex::new(Some(entered_tx));
    f.directories().checkpoint(Some(Arc::new(move |phase| {
        if phase != "writing" { return; }
        let _ = entered.lock().unwrap().take().unwrap().send(());
        let mut ready = gate.0.lock().unwrap();
        while !*ready { ready = gate.1.wait(ready).unwrap(); }
    })));
    let directories = f.directories(); let config = f.core.host().config().clone();
    let mut writing = tokio::spawn(async move { directories.prepare(&config, "owner", &scope).await });
    tokio::time::timeout(Duration::from_secs(5), entered_rx).await.unwrap().unwrap();
    let accounts = f.core.accounts().clone();
    let mut removal = tokio::spawn(async move { accounts.remove("owner").await });
    tokio::time::timeout(Duration::from_secs(5), original.revocation().cancelled()).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(25), &mut removal).await.is_err());
    assert!(tokio::time::timeout(Duration::from_millis(25), &mut writing).await.is_err());
    release.release();
    assert!(matches!(tokio::time::timeout(Duration::from_secs(5), writing).await.unwrap().unwrap(), Err(HostError::ShuttingDown)));
    tokio::time::timeout(Duration::from_secs(5), removal).await.unwrap().unwrap().unwrap();
    assert!(f.core.accounts().get("owner").await.unwrap().is_none());
    assert!(!f.core.host().config().state_dir("owner").parent().unwrap().exists());
    assert!(f.starts().is_empty());
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revocation_cancels_directory_slot_wait_so_supervisor_can_stop_without_waiting_other_writers() {
    let f = Fixture::new().await;
    f.core.host().enable_plugin("fixture").await.unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new())); let release = Release(gate.clone());
    let entered = Arc::new(AtomicUsize::new(0)); let count = entered.clone();
    f.directories().checkpoint(Some(Arc::new(move |phase| {
        if phase != "writing" { return; }
        count.fetch_add(1, Ordering::SeqCst);
        let mut ready = gate.0.lock().unwrap(); while !*ready { ready = gate.1.wait(ready).unwrap(); }
    })));
    let mut occupants = Vec::new();
    for number in 0..4 {
        let name = format!("held-{number}"); let scope = f.scope(&name).await;
        let directories = f.directories(); let config = f.core.host().config().clone();
        occupants.push(tokio::spawn(async move { directories.prepare(&config, &name, &scope).await }));
    }
    until(|| entered.load(Ordering::SeqCst) == 4).await;
    let (waiting_tx, waiting_rx) = oneshot::channel(); let waiting = Mutex::new(Some(waiting_tx));
    f.directories().checkpoint(Some(Arc::new(move |phase| {
        if phase == "waiting" { let _ = waiting.lock().unwrap().take().unwrap().send(()); }
    })));
    let host = f.core.host().clone();
    let start = tokio::spawn(async move { host.start_account("fixture", "owner", AccountSpec::default()).await });
    tokio::time::timeout(Duration::from_secs(5), waiting_rx).await.unwrap().unwrap();
    let accounts = f.core.accounts().clone();
    let mut removal = tokio::spawn(async move { accounts.remove("owner").await });
    assert!(matches!(tokio::time::timeout(Duration::from_secs(5), start).await.unwrap().unwrap(), Err(HostError::ShuttingDown)));
    // Other owners' live directory writers may conservatively prevent catalog
    // completion, but the revoked start must no longer strand stop_on_host.
    assert!(tokio::time::timeout(Duration::from_secs(5), &mut removal).await.unwrap().unwrap().is_err());
    tokio::time::timeout(Duration::from_secs(5), f.core.host().stop_account("fixture", "owner")).await.unwrap().unwrap();
    assert_eq!(f.core.managed_sources().catalog().view().writers(), 4);
    assert!(f.starts().is_empty());
    release.release();
    for occupant in occupants { tokio::time::timeout(Duration::from_secs(5), occupant).await.unwrap().unwrap().unwrap(); }
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revoke_after_starting_before_frame_returns_stopped_without_exposing_a_start() {
    let f = Fixture::new().await;
    let original = f.scope("owner").await.owner.as_ref().unwrap().observed.clone();
    f.directories().checkpoint(Some(Arc::new(move |phase| {
        if phase == "before_frame" { original.revocation().cancel(); }
    })));
    assert!(matches!(f.core.host().start_account("fixture", "owner", AccountSpec::default()).await,
        Err(HostError::ShuttingDown)));
    let record = f.core.host().account("fixture", "owner").unwrap();
    assert_eq!(record.phase, AccountPhase::Stopped);
    assert_eq!(record.detail.as_deref(), Some("Account start was cancelled; start the account again"));
    assert!(record.wanted);
    assert!(f.core.host().running_account_token("fixture", "owner").is_none());
    assert!(f.starts().is_empty());
    f.core.host().stop_account("fixture", "owner").await.unwrap(); // No stranded Starting/Busy.
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_removal_rearm_never_heals_future_replay_without_a_fresh_explicit_start() {
    let f = Fixture::new().await;
    f.core.accounts().start("owner").await.unwrap();
    until(|| f.core.host().account("fixture", "owner").is_some_and(|info| info.phase == AccountPhase::Running)).await;
    let old = f.scope("owner").await.owner.as_ref().unwrap().observed.clone();
    // This malformed/unproved DB metadata is negative removal evidence only.
    // It cannot manufacture a stage claim, payload reader or release permission.
    f.core.open_db().unwrap().conn().execute("INSERT INTO managed_source(stage_id,client_id,account_id,
        plugin_id,account_created_at,chat_id,path,fingerprint_json,intent_json,created_at,updated_at)
        VALUES('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','owner','fixture',
        ?1,'chat',?2,'{}','{}',0,0)", rusqlite::params![old.owner().created_at,
        f.core.host().config().state_dir("owner").join("unproved").to_str()]).unwrap();
    assert!(matches!(f.core.accounts().remove("owner").await, Err(crate::runtime::RuntimeError::SourceInUse)));
    assert!(old.is_revoked());
    f.core.host().restart_plugin("fixture").await.unwrap();
    until(|| f.core.host().account("fixture", "owner").is_some_and(|info| info.phase == AccountPhase::Stopped
        && info.detail.as_deref() == Some("Account start was cancelled; start the account again"))).await;
    assert_eq!(f.starts(), vec!["owner"]);
    f.core.accounts().start("owner").await.unwrap();
    until(|| f.core.host().account("fixture", "owner").is_some_and(|info| info.phase == AccountPhase::Running)).await;
    assert_eq!(f.starts(), vec!["owner", "owner"]);
    assert!(old.is_revoked());
    f.close().await;
}

#[path = "removal_tests.rs"]
mod removal_tests;
