use super::*;
use crate::{host::{AccountPhase, AccountSpec, MemorySecretStore, NullSink}, runtime::{Core, CoreConfig, NewAccount}};
use std::{fs, sync::{Condvar, Mutex, atomic::{AtomicUsize, Ordering}}, time::Duration};

struct Fixture { _temp: tempfile::TempDir, core: Arc<Core> }
impl Fixture {
    async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("data");
        let plugin = data.join("plugins/fixture");
        fs::create_dir_all(&plugin).unwrap();
        fs::write(plugin.join("peer.py"), include_bytes!("../../tests/fixtures/account_directories.py")).unwrap();
        fs::write(plugin.join("plugin.json"), serde_json::json!({
            "id":"fixture","name":"Fixture","version":"0.0.1","api_version":"1.2.0",
            "entry":["python3","-I","./peer.py"],"brand_color":"#123456",
            "multi_account":true,"auth_methods":["none"]
        }).to_string()).unwrap();
        let mut config = CoreConfig::new(&data, Arc::new(MemorySecretStore::new()));
        config.host = HostConfig::fast(&data, 50);
        for key in ["CH_DATA_DIR", "HOME", "XDG_DATA_HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME"] {
            config.host.env.insert(key.into(), temp.path().join(key).to_string_lossy().into_owned());
        }
        config.host.env.insert("CH_TEST_ACCOUNT_STARTS".into(), temp.path().join("starts.log").to_string_lossy().into_owned());
        config.autostart = false;
        let core = Arc::new(Core::open(config).await.unwrap());
        core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
        Self { _temp: temp, core }
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
    let refused = f.core.host().account("fixture", "fresh").unwrap();
    assert_eq!(refused.phase, AccountPhase::Stopped); assert!(refused.wanted);
    assert_eq!(refused.detail.as_deref(), Some("account directory preparation unavailable"));
    assert!(f.core.host().running_account_token("fixture", "fresh").is_none());
    f.core.host().restart_plugin("fixture").await.unwrap();
    until(|| f.core.host().account("fixture", "owner").is_some_and(|info|
        info.phase == AccountPhase::Stopped && info.detail.as_deref() == Some("account directory preparation unavailable"))).await;
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
    let gate = Arc::new((Mutex::new(false), Condvar::new())); let release = Release(gate.clone());
    let (entered_tx, entered_rx) = oneshot::channel(); let entered = Mutex::new(Some(entered_tx));
    f.directories().checkpoint(Some(Arc::new(move |phase| {
        if phase != "writing" { return; }
        let _ = entered.lock().unwrap().take().unwrap().send(());
        let mut ready = gate.0.lock().unwrap();
        while !*ready { ready = gate.1.wait(ready).unwrap(); }
    })));
    let directories = f.directories(); let config = f.core.host().config().clone();
    let caller = tokio::spawn(async move { directories.prepare(&config, "pending").await });
    tokio::time::timeout(Duration::from_secs(5), entered_rx).await.unwrap().unwrap();
    caller.abort();
    assert!(tokio::time::timeout(Duration::from_secs(5), caller).await.unwrap().unwrap_err().is_cancelled());
    assert_eq!(catalog.view().writers(), 1);
    assert!(!f.core.host().config().state_dir("pending").parent().unwrap().exists());
    f.core.managed_sources().close();
    assert!(matches!(f.directories().prepare(f.core.host().config(), "fresh").await, Err(HostError::ShuttingDown)));
    let sources = f.core.managed_sources(); let mut drain = tokio::spawn(async move { sources.drain().await });
    assert!(tokio::time::timeout(Duration::from_millis(25), &mut drain).await.is_err());
    drain.abort();
    assert!(tokio::time::timeout(Duration::from_secs(5), drain).await.unwrap().unwrap_err().is_cancelled());
    let sources = f.core.managed_sources(); let mut retry = tokio::spawn(async move { sources.drain().await });
    assert!(tokio::time::timeout(Duration::from_millis(25), &mut retry).await.is_err());
    release.release();
    tokio::time::timeout(Duration::from_secs(5), retry).await.unwrap().unwrap().unwrap();
    assert!(f.core.host().config().state_dir("pending").is_dir());
    assert!(f.core.host().config().cache_dir("pending").is_dir());
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
            occupants.push(tokio::spawn(async move { directories.prepare(&config, &format!("occupant-{number}")).await }));
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
        occupants.push(tokio::spawn(async move { directories.prepare(&config, &format!("occupant-{number}")).await }));
    }
    until(|| entered.load(Ordering::SeqCst) == 4).await;
    for cancel_caller in [true, false] {
        let (waiting_tx, waiting_rx) = oneshot::channel(); let waiting = Mutex::new(Some(waiting_tx));
        f.directories().checkpoint(Some(Arc::new(move |phase| {
            if phase == "waiting" { let _ = waiting.lock().unwrap().take().unwrap().send(()); }
        })));
        let directories = f.directories(); let config = f.core.host().config().clone();
        let mut waiter = tokio::spawn(async move { directories.prepare(&config, "never-created").await });
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
        tokio::time::timeout(Duration::from_secs(5), occupant).await.unwrap().unwrap().unwrap();
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
