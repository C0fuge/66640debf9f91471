//! Actual isolated peer + Core removal boundaries; no source-closure claims.
use super::*;
use crate::{host::SecretStore, runtime::RuntimeError};

fn requests(f: &Fixture, method: &str) -> usize {
    fs::read_to_string(f._temp.path().join("requests.log")).unwrap_or_default()
        .lines().filter(|line| *line == method).count()
}

async fn preserved_refusal(f: &Fixture, original: &crate::runtime::AccountStorageObservation) {
    assert!(matches!(tokio::time::timeout(Duration::from_secs(5), f.core.accounts().remove("owner"))
        .await.unwrap(), Err(RuntimeError::SourceInUse)));
    assert!(original.is_revoked());
    assert!(f.core.accounts().get("owner").await.unwrap()
        .is_some_and(|row| row.created_at == original.owner().created_at));
    let root = f.core.host().config().state_dir("owner");
    assert_eq!(fs::read(root.parent().unwrap().join("preserve")).unwrap(), b"unchanged");
    assert_eq!(f.secrets.snapshot("fixture", "owner").get("session").map(String::as_str), Some("synthetic"));
    let fresh = f.core.accounts().storage_observation_for("owner").await.unwrap();
    assert_ne!(fresh, *original); // Exact anchored refusal rearmed, never the old Gate.
}

async fn preserve_inputs(f: &Fixture) -> crate::runtime::AccountStorageObservation {
    let observed = f.core.accounts().storage_observation_for("owner").await.unwrap();
    let state = f.core.host().config().state_dir("owner");
    fs::create_dir_all(state.parent().unwrap()).unwrap();
    fs::write(state.parent().unwrap().join("preserve"), b"unchanged").unwrap();
    f.secrets.set("fixture", "owner", "session", "synthetic").await.unwrap();
    observed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removal_rpc_error_and_timeout_remain_unknown_after_local_stopped_and_process_end() {
    for mode in ["stop-error", "stop-timeout", "stop-malformed"] {
        let f = Fixture::configured(Some(mode)).await;
        f.core.accounts().start("owner").await.unwrap();
        let old = preserve_inputs(&f).await;
        preserved_refusal(&f, &old).await;
        until(|| f.core.host().account("fixture", "owner").is_some_and(|info| info.phase == AccountPhase::Stopped)).await;
        // The ordinary best-effort API still has its existing local fast path.
        f.core.host().stop_account("fixture", "owner").await.unwrap();
        let next = f.core.accounts().storage_observation_for("owner").await.unwrap();
        preserved_refusal(&f, &next).await;
        assert_eq!(requests(&f, "account.stop"), 1); // No automatic RPC retry.
        tokio::time::timeout(Duration::from_secs(5), f.core.host().disable_plugin("fixture")).await.unwrap().unwrap();
        let next = f.core.accounts().storage_observation_for("owner").await.unwrap();
        preserved_refusal(&f, &next).await; // Shutdown does not erase older unknown.
        assert!(old.is_revoked());
        f.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn positive_stop_unknown_account_and_never_started_preserve_normal_removal() {
    for mode in ["success", "stop-unknown", "never-started"] {
        let f = Fixture::configured(Some(mode)).await;
        if mode != "never-started" { f.core.accounts().start("owner").await.unwrap(); }
        let old = preserve_inputs(&f).await;
        tokio::time::timeout(Duration::from_secs(5), f.core.accounts().remove("owner")).await.unwrap().unwrap();
        assert!(old.is_revoked());
        assert!(f.core.accounts().get("owner").await.unwrap().is_none());
        assert!(!f.core.host().config().state_dir("owner").parent().unwrap().exists());
        assert!(f.secrets.snapshot("fixture", "owner").is_empty());
        assert_eq!(requests(&f, "account.stop"), usize::from(mode != "never-started"));
        f.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_shutdown_ack_settles_known_active_but_process_exit_without_it_does_not() {
    for mode in ["success", "shutdown-error", "shutdown-malformed"] {
        let f = Fixture::configured(Some(mode)).await;
        f.core.accounts().start("owner").await.unwrap();
        let old = preserve_inputs(&f).await;
        tokio::time::timeout(Duration::from_secs(5), f.core.host().disable_plugin("fixture")).await.unwrap().unwrap();
        if mode == "success" {
            tokio::time::timeout(Duration::from_secs(5), f.core.accounts().remove("owner")).await.unwrap().unwrap();
            assert!(f.core.accounts().get("owner").await.unwrap().is_none());
            assert!(!f.core.host().config().state_dir("owner").parent().unwrap().exists());
        } else { preserved_refusal(&f, &old).await; }
        assert!(old.is_revoked());
        f.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revoked_pending_start_refuses_then_exact_positive_ack_can_be_strictly_stopped() {
    let f = Fixture::configured(Some("delayed-start")).await;
    let old = preserve_inputs(&f).await;
    // Direct Host call intentionally avoids Accounts' separate lifecycle lock.
    let host = f.core.host().clone();
    let caller = tokio::spawn(async move { host.start_account("fixture", "owner", AccountSpec::default()).await });
    until(|| f._temp.path().join("start-waiting").exists()).await;
    preserved_refusal(&f, &old).await;
    assert_eq!(requests(&f, "account.stop"), 0);
    assert!(f.core.host().account("fixture", "owner").is_some_and(|info| info.phase == AccountPhase::Starting));
    fs::write(f._temp.path().join("release-start"), b"").unwrap();
    tokio::time::timeout(Duration::from_secs(5), caller).await.unwrap().unwrap().unwrap();
    // The old scope still cannot adopt a Running event, but its actual positive
    // lifecycle response is known, so one strict stop may settle that same peer.
    assert!(old.is_revoked());
    assert!(f.core.host().account("fixture", "owner").is_some_and(|info| info.phase == AccountPhase::Starting));
    tokio::time::timeout(Duration::from_secs(5), f.core.accounts().remove("owner")).await.unwrap().unwrap();
    assert_eq!(requests(&f, "account.stop"), 1);
    assert!(f.core.accounts().get("owner").await.unwrap().is_none());
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_flight_ordinary_stop_refuses_removal_until_its_exact_reply() {
    let f = Fixture::configured(Some("delayed-stop")).await;
    f.core.accounts().start("owner").await.unwrap();
    let old = preserve_inputs(&f).await;
    let host = f.core.host().clone();
    let caller = tokio::spawn(async move { host.stop_account("fixture", "owner").await });
    until(|| f._temp.path().join("stop-waiting").exists()).await;
    preserved_refusal(&f, &old).await;
    assert_eq!(requests(&f, "account.stop"), 1);
    assert!(f.core.host().account("fixture", "owner").is_some_and(|info| info.phase == AccountPhase::Stopping));
    fs::write(f._temp.path().join("release-stop"), b"").unwrap();
    tokio::time::timeout(Duration::from_secs(5), caller).await.unwrap().unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(5), f.core.accounts().remove("owner")).await.unwrap().unwrap();
    assert_eq!(requests(&f, "account.stop"), 1); // Exact existing confirmation, no resend.
    assert!(f.core.accounts().get("owner").await.unwrap().is_none());
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_core_stop_recovers_accepted_revoked_start_without_discarding_unproved_copy() {
    use std::os::unix::fs::PermissionsExt;
    let f = Fixture::configured(Some("delayed-start")).await;
    let old = preserve_inputs(&f).await;
    let host = f.core.host().clone();
    let starting = tokio::spawn(async move { host.start_account("fixture", "owner", AccountSpec::default()).await });
    until(|| f._temp.path().join("start-waiting").exists()).await;
    // Pending start has no positive native ACK and remains busy, with no stop frame.
    assert!(matches!(f.core.host().stop_account("fixture", "owner").await,
        Err(HostError::AccountBusy(_, "starting"))));
    assert_eq!(requests(&f, "account.stop"), 0);
    // Real unregistered private bytes: catalog denial, never fabricated source
    // registration or a positive physical-reader/closure claim.
    let uploads = f.core.host().config().upload_dir("owner");
    let unproved = uploads.join("unproved-copy");
    let mutation = f.core.stage_mutations().begin().unwrap();
    fs::create_dir_all(&uploads).unwrap();
    fs::set_permissions(&uploads, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(&unproved, b"unproved private bytes").unwrap();
    fs::set_permissions(&unproved, fs::Permissions::from_mode(0o400)).unwrap();
    drop(mutation);
    preserved_refusal(&f, &old).await; // Refused before strict stop; old Gate is revoked.
    assert_eq!(requests(&f, "account.stop"), 0);
    fs::write(f._temp.path().join("release-start"), b"").unwrap();
    tokio::time::timeout(Duration::from_secs(5), starting).await.unwrap().unwrap().unwrap();
    assert!(f.core.host().account("fixture", "owner").is_some_and(|info| info.phase == AccountPhase::Starting));
    // The exact peer's ACK is native Active evidence despite stale UI adoption.
    // Explicit Core disable follows ordinary stop policy and sends one real RPC.
    tokio::time::timeout(Duration::from_secs(5), f.core.accounts().set_enabled("owner", false)).await.unwrap().unwrap();
    assert!(f.core.host().account("fixture", "owner").is_some_and(|info| info.phase == AccountPhase::Stopped && !info.wanted));
    assert_eq!(requests(&f, "account.stop"), 1);
    assert_eq!(fs::read(&unproved).unwrap(), b"unproved private bytes");
    let fresh = f.core.accounts().storage_observation_for("owner").await.unwrap();
    preserved_refusal(&f, &fresh).await; // Stop did not turn the copy into safe deletion.
    assert_eq!(requests(&f, "account.stop"), 1);
    assert_eq!(fs::read(&unproved).unwrap(), b"unproved private bytes");
    let sources: i64 = f.core.open_db().unwrap().conn().query_row("SELECT count(*) FROM managed_source", [], |row| row.get(0)).unwrap();
    assert_eq!(sources, 0);
    assert!(f.core.accounts().get("owner").await.unwrap().is_some_and(|row| !row.enabled));
    f.close().await;
}
