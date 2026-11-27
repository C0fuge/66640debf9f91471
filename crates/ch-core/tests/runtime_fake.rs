//! `ch_core::runtime` over the real Python fake plugin: add an account, sign in through the
//! fake's QR flow, data lands in the DB and on the bus, restart resumes the account from the
//! stored session without signing in again. Ignored by default (needs `uv`; `uv sync` into a
//! temp copy is slow): `cargo test -p ch-core --test runtime_fake -- --ignored`.

mod host_support;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ch_core::host::{AccountPhase, MemorySecretStore, StateChange};
use ch_core::ingest::Change;
use ch_core::runtime::{Core, CoreChange, CoreConfig, NewAccount};
use ch_protocol::events::AccountState;
use host_support::*;
use serde_json::json;
use tokio::sync::broadcast;

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap().filter_map(Result::ok) {
        let name = e.file_name();
        if name == ".venv" || name == "__pycache__" || name == ".pytest_cache" {
            continue;
        }
        let dst = to.join(&name);
        if e.file_type().unwrap().is_dir() {
            copy_tree(&e.path(), &dst);
        } else {
            std::fs::copy(e.path(), dst).unwrap();
        }
    }
}

fn install_fake(env: &Env) {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let dst = env.plugins_dir.join("fake");
    copy_tree(&repo.join("plugins").join("fake"), &dst);
    // `../../sdk/python` from `<data>/plugins/fake` is `<data>/sdk/python`.
    copy_tree(
        &repo.join("sdk").join("python"),
        &env.data_dir.join("sdk").join("python"),
    );
    let st = std::process::Command::new("uv")
        .args(["sync", "--quiet"])
        .current_dir(&dst)
        .env("UV_CACHE_DIR", env.tmp.path().join("uv-cache"))
        .env("HOME", env.tmp.path().join("HOME"))
        .env("XDG_CACHE_HOME", env.tmp.path().join("XDG_CACHE_HOME"))
        .status()
        .expect("uv is installed");
    assert!(st.success(), "uv sync failed");
}

fn config(env: &Env, secrets: &Arc<MemorySecretStore>) -> CoreConfig {
    let mut c = CoreConfig::new(&env.data_dir, secrets.clone());
    c.host = env.config();
    // Python start-up is slower than the test divisor allows for.
    c.host.timeout_scale = 0.2;
    c
}

async fn wait_change(
    rx: &mut broadcast::Receiver<CoreChange>,
    timeout: Duration,
    mut pred: impl FnMut(&CoreChange) -> bool,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(left, rx.recv()).await {
            Ok(Ok(c)) if pred(&c) => return,
            Ok(Ok(_) | Err(broadcast::error::RecvError::Lagged(_))) => {}
            Ok(Err(e)) => panic!("bus closed: {e}"),
            Err(_) => panic!("change not seen in {timeout:?}"),
        }
    }
}

fn count(core: &Core, sql: &str, account: &str) -> i64 {
    let db = core.open_db().unwrap();
    db.conn().query_row(sql, [account], |r| r.get(0)).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs uv and a network-less uv sync; slow"]
async fn fake_sign_in_ingest_restart() {
    let env = Env::new();
    install_fake(&env);
    let secrets = Arc::new(MemorySecretStore::new());

    let core = Core::open(config(&env, &secrets)).await.unwrap();
    let mut rx = core.subscribe();
    let row = core
        .accounts()
        .add(NewAccount::new("fake").settings(json!({
            "seed": 3, "chats": 4, "messages_per_chat": 5, "require_2fa": false, "qr_scan_ms": 100
        })))
        .await
        .unwrap();
    let id = row.account_id.clone();

    // No session yet: the fake asks to sign in.
    wait_change(&mut rx, secs(20.0), |c| {
        matches!(c, CoreChange::Host(StateChange::Account(a))
            if a.account_id == id && a.state == AccountState::AuthRequired)
    })
    .await;
    core.host()
        .call_raw(
            "fake",
            "auth.begin",
            json!({"account_id": id, "method": "qr"}),
            None,
        )
        .await
        .unwrap();
    // The QR step passes through the bus; then the account connects and syncs.
    wait_change(&mut rx, secs(10.0), |c| {
        matches!(c, CoreChange::Ingested { applied, .. }
            if applied.changes.iter().any(|ch| matches!(ch, Change::AuthStep(_))))
    })
    .await;
    wait_change(&mut rx, secs(20.0), |c| {
        matches!(c, CoreChange::Ingested { applied, .. }
            if applied.event == "messages.upsert"
                && applied.changes.iter().any(|ch| matches!(ch, Change::MessagesUpserted { .. })))
    })
    .await;
    let q_conv = "SELECT count(*) FROM conversation WHERE account_id = ?1";
    let q_handles = "SELECT count(*) FROM platform_handle WHERE account_id = ?1";
    assert_eq!(count(&core, q_conv, &id), 4);
    assert!(count(&core, q_handles, &id) > 0);
    let r = core.accounts().get(&id).await.unwrap().unwrap();
    assert_eq!(r.state, "connected");
    assert!(r.me_user_id.is_some(), "me persisted");
    assert!(
        secrets.snapshot("fake", &id).contains_key("session"),
        "runtime secret stored"
    );

    core.shutdown().await;
    drop(core);

    // Restart: the stored session skips sign-in; the data is still there.
    let core = Core::open(config(&env, &secrets)).await.unwrap();
    let mut seen_auth = false;
    let deadline = Instant::now() + secs(20.0);
    loop {
        if let Some(a) = core.host().account("fake", &id) {
            seen_auth |= a.state == AccountState::AuthRequired;
            if a.phase == AccountPhase::Running && a.state == AccountState::Connected {
                break;
            }
        }
        assert!(Instant::now() < deadline, "account did not resume");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!seen_auth, "no second sign-in");
    assert_eq!(count(&core, q_conv, &id), 4);
    core.shutdown().await;
    #[cfg(target_os="linux")]
    assert!(core.accounts().get(&id).await.is_err(),"closed account service refuses new work");
    assert_eq!(
        ch_db::get_account(core.open_db().unwrap().conn(),&id).unwrap().unwrap().state,
        "disconnected"
    );
}
