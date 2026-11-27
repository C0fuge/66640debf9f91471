//! `ch_core::runtime` end to end over fast plugins: the Rust SDK `echo` example (data flow,
//! restart, remove) and the raw-NDJSON `misbehaving.py --mode good` (settings, `x-secret`
//! routing, enable/disable). Everything lives in a temp dir.

mod host_support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use ch_core::host::{AccountPhase, EventSink, MemorySecretStore, StateChange};
use ch_core::ingest::{Change, Ingest, IngestOptions};
use ch_core::runtime::{
    AccountChange, ChangeBus, Core, CoreChange, CoreConfig, IngestSink, NewAccount, RuntimeError,
    settings_secret_namespace,
};
use ch_protocol::events::{AccountState, Event};
use host_support::*;
use serde_json::{Value, json};
use tokio::sync::broadcast;

fn core_config(env: &Env, secrets: &Arc<MemorySecretStore>) -> CoreConfig {
    let mut c = CoreConfig::new(&env.data_dir, secrets.clone());
    c.host = env.config();
    c
}

/// Receive until `pred` matches a change; panics after `timeout`.
async fn wait_change<T>(
    rx: &mut broadcast::Receiver<CoreChange>,
    timeout: Duration,
    mut pred: impl FnMut(&CoreChange) -> Option<T>,
) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(left, rx.recv()).await {
            Ok(Ok(c)) => {
                if let Some(t) = pred(&c) {
                    return t;
                }
            }
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => {}
            Ok(Err(e)) => panic!("bus closed: {e}"),
            Err(_) => panic!("change not seen in {timeout:?}"),
        }
    }
}

fn count(core: &Core, sql: &str, account: &str) -> i64 {
    let db = core.open_db().unwrap();
    db.conn().query_row(sql, [account], |r| r.get(0)).unwrap()
}

async fn wait_connected(core: &Core, plugin: &str, account: &str) {
    wait_account(core.host(), plugin, account, secs(10.0), |a| {
        a.phase == AccountPhase::Running && a.state == AccountState::Connected
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn echo_add_ingest_restart_remove() {
    let env = Env::new();
    env.echo("echo");
    let secrets = Arc::new(MemorySecretStore::new());

    let core = Core::open(core_config(&env, &secrets)).await.unwrap();
    assert_eq!(core.db_path(), env.data_dir.join("hub.db"));
    assert!(core.db_path().exists());
    let mut rx = core.subscribe();

    let row = core
        .accounts()
        .add(NewAccount::new("echo").label("Echo"))
        .await
        .unwrap();
    let id = row.account_id.clone();
    assert_eq!(id, "echo-1");
    assert!(row.enabled);

    // The bus carries the account change, the ingested conversation and the host status.
    let mut added = false;
    let mut conv = false;
    let mut connected = false;
    wait_change(&mut rx, secs(10.0), |c| {
        match c {
            CoreChange::Account(AccountChange::Added { account_id, .. }) if *account_id == id => {
                added = true
            }
            CoreChange::Ingested { plugin_id, applied } if plugin_id == "echo" => {
                if applied.changes.iter().any(|ch| {
                    matches!(ch, Change::ConversationUpserted { chat_id, created: true } if chat_id == "echo")
                }) {
                    conv = true;
                }
            }
            CoreChange::Host(StateChange::Account(a))
                if a.account_id == id && a.state == AccountState::Connected =>
            {
                connected = true
            }
            _ => {}
        }
        (added && conv && connected).then_some(())
    })
    .await;
    wait_connected(&core, "echo", &id).await;

    // A message the plugin emits lands in the DB and on the bus.
    let _: ch_protocol::methods::MessageResult = core
        .host()
        .call(
            "echo",
            "messages.send",
            &json!({"account_id": id, "chat_id": "echo", "text": "hello"}),
        )
        .await
        .unwrap();
    wait_change(&mut rx, secs(10.0), |c| match c {
        CoreChange::Ingested { applied, .. } => applied
            .changes
            .iter()
            .any(|ch| matches!(ch, Change::MessagesUpserted { created, .. } if !created.is_empty()))
            .then_some(()),
        _ => None,
    })
    .await;
    let q_conv = "SELECT count(*) FROM conversation WHERE account_id = ?1";
    let q_msg = "SELECT count(*) FROM message m JOIN conversation c ON c.id = m.conversation_id \
                 WHERE c.account_id = ?1 AND m.text = 'hello'";
    assert_eq!(count(&core, q_conv, &id), 1);
    assert_eq!(count(&core, q_msg, &id), 1);
    let r = core.accounts().get(&id).await.unwrap().unwrap();
    assert_eq!(r.state, "connected");
    assert_eq!(r.label.as_deref(), Some("Echo"));
    // The plugin row mirrors the manifest.
    let p = ch_db::get_plugin(core.open_db().unwrap().conn(), "echo")
        .unwrap()
        .unwrap();
    assert!(p.installed);

    core.shutdown().await;
    let r = core.accounts().get(&id).await.unwrap().unwrap();
    assert_eq!(r.state, "disconnected", "final host state persisted");
    assert!(r.enabled, "still wanted");
    drop(core);

    // Restart on the same data dir: the account resumes by itself, the data is still there.
    let core = Core::open(core_config(&env, &secrets)).await.unwrap();
    wait_connected(&core, "echo", &id).await;
    assert_eq!(count(&core, q_conv, &id), 1);
    assert_eq!(count(&core, q_msg, &id), 1);
    assert_eq!(core.accounts().list().await.unwrap().len(), 1);

    // A second account gets a fresh id; removing the first never frees "echo-1" for reuse.
    let mut rx = core.subscribe();
    core.accounts().remove(&id).await.unwrap();
    wait_change(&mut rx, secs(5.0), |c| {
        matches!(c, CoreChange::Account(AccountChange::Removed { account_id, .. }) if *account_id == id)
            .then_some(())
    })
    .await;
    assert!(core.accounts().get(&id).await.unwrap().is_none());
    assert_eq!(count(&core, q_conv, &id), 0);
    assert!(!env.data_dir.join("accounts").join(&id).exists());
    // The stopped internal record may remain, but the deleted logical owner
    // must no longer be exposed through public Host metadata snapshots.
    assert!(core.host().account("echo", &id).is_none());
    let row2 = core
        .accounts()
        .add(NewAccount::new("echo").no_start())
        .await
        .unwrap();
    assert_eq!(row2.account_id, "echo-2");
    assert!(matches!(
        core.accounts()
            .add(NewAccount::new("echo").id("echo-2"))
            .await,
        Err(RuntimeError::AccountExists(_))
    ));
    assert!(matches!(
        core.accounts().add(NewAccount::new("nope")).await,
        Err(RuntimeError::UnknownPlugin(_))
    ));
    core.shutdown().await;
}

fn mis_schema() -> Value {
    json!({"settings_schema": {"type": "object", "properties": {
        "seed": {"type": "integer", "minimum": 0, "default": 1},
        "mode": {"type": "string", "enum": ["a", "b"], "default": "a"},
        "token": {"type": "string", "x-secret": true, "minLength": 4}
    }, "required": ["token"]}})
}

/// The `settings` the misbehaving plugin saw at its latest `account.start` (from its status
/// detail, as persisted in the DB).
async fn seen(core: &Core, id: &str, start: u64) -> serde_json::Map<String, Value> {
    let deadline = Instant::now() + secs(10.0);
    loop {
        let r = core.accounts().get(id).await.unwrap().unwrap();
        if let Some(d) = r.state_detail.as_deref() {
            let v: Value = serde_json::from_str(d).unwrap();
            if v["start"] == json!(start) {
                return v["settings"].as_object().unwrap().clone();
            }
        }
        assert!(
            Instant::now() < deadline,
            "start {start} not seen: {:?}",
            r.state_detail
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settings_validation_and_secret_routing() {
    let env = Env::new();
    env.misbehaving("mis", "good", mis_schema());
    let secrets = Arc::new(MemorySecretStore::new());
    let core = Core::open(core_config(&env, &secrets)).await.unwrap();
    let acc = core.accounts();
    let ns = settings_secret_namespace("mis");

    // Invalid: wrong type, out of range, missing required secret, bad enum, non-string secret.
    for bad in [
        json!({"seed": "x", "token": "abcd"}),
        json!({"seed": -1, "token": "abcd"}),
        json!({"seed": 2}),
        json!({"mode": "c", "token": "abcd"}),
        json!({"token": 1234}),
    ] {
        let e = acc
            .add(NewAccount::new("mis").settings(bad.clone()))
            .await
            .unwrap_err();
        assert!(matches!(e, RuntimeError::InvalidSettings(_)), "{bad}: {e}");
    }
    assert!(acc.list().await.unwrap().is_empty(), "nothing persisted");

    let row = acc
        .add(NewAccount::new("mis").settings(json!({"seed": 2, "token": "s3cret"})))
        .await
        .unwrap();
    let id = row.account_id.clone();
    // The secret is in the store, not in the DB.
    assert_eq!(Value::Object(row.settings.clone()), json!({"seed": 2}));
    assert_eq!(
        secrets.snapshot(&ns, &id).get("token").map(String::as_str),
        Some("s3cret")
    );
    let raw: String = core
        .open_db()
        .unwrap()
        .conn()
        .query_row(
            "SELECT settings_json FROM account WHERE account_id = ?1",
            [&id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(!raw.contains("s3cret"), "{raw}");
    // The plugin got one flat object: stored values, the secret merged in, defaults filled.
    assert_eq!(
        Value::Object(seen(&core, &id, 1).await),
        json!({"seed": 2, "mode": "a", "token": "s3cret"})
    );
    let s = acc.settings(&id).await.unwrap();
    assert_eq!(Value::Object(s.values), json!({"seed": 2}));
    assert_eq!(s.secrets_set.into_iter().collect::<Vec<_>>(), ["token"]);
    // The plugin's runtime secret goes to its own namespace, apart from the setting secrets.
    let deadline = Instant::now() + secs(5.0);
    while !secrets.snapshot("mis", &id).contains_key("session") {
        assert!(Instant::now() < deadline, "runtime secret not stored");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!secrets.snapshot("mis", &id).contains_key("token"));

    // A bad value is refused without echoing it, and nothing changes.
    let e = acc
        .set_settings(&id, json!({"token": "abc"}).as_object().unwrap().clone())
        .await
        .unwrap_err();
    assert!(e.to_string().contains("/token"), "{e}");
    assert!(
        !e.to_string().contains("\"abc\""),
        "secret value leaked: {e}"
    );
    assert_eq!(
        secrets.snapshot(&ns, &id).get("token").map(String::as_str),
        Some("s3cret")
    );

    // Saving without the secret keeps it; the running account restarts with the new values.
    acc.set_settings(
        &id,
        json!({"seed": 5, "mode": "b"}).as_object().unwrap().clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        Value::Object(seen(&core, &id, 2).await),
        json!({"seed": 5, "mode": "b", "token": "s3cret"})
    );
    // Replacing the secret.
    acc.set_settings(
        &id,
        json!({"seed": 5, "token": "n3w-secret"})
            .as_object()
            .unwrap()
            .clone(),
    )
    .await
    .unwrap();
    assert_eq!(seen(&core, &id, 3).await["token"], json!("n3w-secret"));
    // Deleting a required secret is invalid.
    assert!(
        acc.set_settings(&id, json!({"token": null}).as_object().unwrap().clone())
            .await
            .is_err()
    );

    // Disable: stopped now and not started on the next open.
    acc.set_enabled(&id, false).await.unwrap();
    assert_eq!(
        core.host().account("mis", &id).unwrap().phase,
        AccountPhase::Stopped
    );
    assert!(!acc.get(&id).await.unwrap().unwrap().enabled);
    core.shutdown().await;
    drop(core);
    let core = Core::open(core_config(&env, &secrets)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        core.host().account("mis", &id).is_none(),
        "disabled account not started"
    );
    // Enable again: starts with the stored secret.
    core.accounts().set_enabled(&id, true).await.unwrap();
    wait_connected(&core, "mis", &id).await;
    assert_eq!(
        core.accounts().spec(&id).await.unwrap().settings["token"],
        json!("n3w-secret")
    );

    // Remove: every secret of the account is gone.
    core.accounts().remove(&id).await.unwrap();
    assert!(secrets.snapshot(&ns, &id).is_empty());
    assert!(secrets.snapshot("mis", &id).is_empty());
    core.shutdown().await;
}

#[tokio::test]
async fn ingest_sink_drops_unknown_accounts_and_rejects_mismatches() {
    let db = ch_db::Db::open_in_memory().unwrap();
    let now = ch_db::now_millis();
    for p in ["a", "b"] {
        ch_db::upsert_plugin(
            db.conn(),
            &ch_db::types::PluginRow {
                plugin_id: p.into(),
                name: p.into(),
                version: "1".into(),
                api_version: "1.0.0".into(),
                install_dir: None,
                brand_color: None,
                icon_path: None,
                manifest_json: None,
                enabled: true,
                installed: true,
                first_seen_at: now,
                last_seen_at: now,
            },
        )
        .unwrap();
    }
    ch_db::create_account(db.conn(), "a-1", "a", None, &Default::default()).unwrap();
    let bus = ChangeBus::new(64);
    let mut rx = bus.subscribe();
    let sink = IngestSink::new(Ingest::new(db, IngestOptions::default()), bus);
    let status = |acc: &str| {
        Event::from_params(
            "status",
            json!({"account_id": acc, "state": "connected", "reconnecting": false}),
        )
        .unwrap()
        .unwrap()
    };
    // Unknown account: logged and dropped, nothing published.
    sink.deliver("a", status("ghost")).await.unwrap();
    // Wrong plugin: rejected.
    assert!(sink.deliver("b", status("a-1")).await.is_err());
    assert!(rx.try_recv().is_err());
    // The real thing.
    sink.deliver("a", status("a-1")).await.unwrap();
    match rx.try_recv().unwrap() {
        CoreChange::Ingested { plugin_id, applied } => {
            assert_eq!(plugin_id, "a");
            assert!(matches!(applied.changes[0], Change::AccountStatus { .. }));
        }
        other => panic!("{other:?}"),
    }
    // Order is kept: many events in a row arrive in sequence.
    for i in 0..20 {
        let e = Event::from_params(
            "conversations.upsert",
            json!({"account_id": "a-1", "conversations": [{"id": format!("c{i}"), "kind": "dm"}]}),
        )
        .unwrap()
        .unwrap();
        sink.deliver("a", e).await.unwrap();
    }
    for i in 0..20 {
        match rx.try_recv().unwrap() {
            CoreChange::Ingested { applied, .. } => assert!(matches!(
                &applied.changes[0],
                Change::ConversationUpserted { chat_id, .. } if *chat_id == format!("c{i}")
            )),
            other => panic!("{other:?}"),
        }
    }
}

#[test]
fn default_data_dir_follows_adr_0001() {
    // Only reads the environment of this process; the order of precedence is what matters.
    let d = ch_core::runtime::default_data_dir();
    if let Some(ch) = std::env::var_os("CH_DATA_DIR").filter(|v| !v.is_empty()) {
        assert_eq!(d.unwrap(), std::path::PathBuf::from(ch));
    } else if let Some(x) = std::env::var_os("XDG_DATA_HOME").filter(|v| !v.is_empty()) {
        assert_eq!(
            d.unwrap(),
            std::path::PathBuf::from(x).join("contact-hub-ng")
        );
    } else if let Some(d) = d {
        assert!(d.ends_with(".local/share/contact-hub-ng"));
    }
}

async fn storage_fixture() -> (Env, Core) {
    let env = Env::new();
    env.misbehaving("storage_fixture", "good", json!({}));
    let mut config = core_config(&env, &Arc::new(MemorySecretStore::new()));
    config.autostart = false;
    let core = Core::open(config).await.unwrap();
    (env, core)
}

fn storage_account(id: &str) -> NewAccount {
    NewAccount::new("storage_fixture").id(id).no_start()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_lease_revocation_drains_actual_io_and_serializes_abandoned_remove_and_add() {
    let (env, core) = storage_fixture().await;
    let accounts = core.accounts();
    let id = "same-id";
    let row = accounts.add(storage_account(id)).await.unwrap();
    let root = env.data_dir.join("accounts").join(id);
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("marker"), b"old").unwrap();
    let (choosing, chooser_guard) = accounts.storage_lease(&row).await.unwrap();
    drop(chooser_guard);
    let (writer, held) = accounts.storage_lease(&row).await.unwrap();
    let mut events = core.subscribe();
    {
        let mut removal = Box::pin(accounts.remove(id));
        // Poll the real API until its owned task reaches the reader drain.
        tokio::select! {
            biased;
            result=&mut removal => panic!("remove returned with active reader: {result:?}"),
            _=tokio::time::timeout(secs(5.0),writer.cancelled()) => assert!(writer.is_cancelled()),
        }
        assert!(choosing.is_cancelled());
        assert!(root.join("marker").is_file());
        assert!(accounts.get(id).await.unwrap().is_some());
        assert!(accounts.storage_lease(&row).await.is_err());
        let mut addition = Box::pin(accounts.add(storage_account(id)));
        tokio::select! {
            biased;
            result=&mut addition => panic!("replacement overtook pending teardown: {result:?}"),
            _=std::future::ready(())=>{}
        }
        // Both API futures disappear; their tasks must still own the gates.
    }
    drop(held);
    wait_change(&mut events, secs(5.0), |event| {
        matches!(event,CoreChange::Account(AccountChange::Added{account_id,..}) if account_id==id)
            .then_some(())
    })
    .await;
    let replacement = accounts.get(id).await.unwrap().unwrap();
    let (fresh, guard) = accounts.storage_lease(&replacement).await.unwrap();
    assert!(!fresh.is_cancelled());
    assert!(choosing.is_cancelled());
    assert!(
        !root.exists(),
        "replacement inherited the removed directory"
    );
    drop(guard);
    let mut foreign = replacement.clone();
    foreign.account_id = "absent".into();
    assert!(accounts.storage_lease(&foreign).await.is_err());
    foreign = replacement.clone();
    foreign.plugin_id = "foreign".into();
    assert!(accounts.storage_lease(&foreign).await.is_err());
    foreign = replacement;
    foreign.created_at -= 1;
    assert!(accounts.storage_lease(&foreign).await.is_err());
    core.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn colliding_storage_names_share_the_drain_and_preserve_the_other_owner() {
    let (env, core) = storage_fixture().await;
    let accounts = core.accounts();
    let a = accounts.add(storage_account("one/two")).await.unwrap();
    let (old, held) = accounts.storage_lease(&a).await.unwrap();
    // Both persisted IDs exist before either has created a file. They use the
    // same HostConfig component, including after a restart with historical rows.
    let b = accounts.add(storage_account("one_two")).await.unwrap();
    assert!(old.is_cancelled());
    assert!(accounts.storage_lease(&a).await.is_err());
    assert!(accounts.storage_lease(&b).await.is_err());
    let root = env.data_dir.join("accounts/one_two");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("marker"), b"retained").unwrap();
    let mut remove = Box::pin(accounts.remove(&b.account_id));
    tokio::select! {
        result=&mut remove=>panic!("directory alias bypassed the drain: {result:?}"),
        _=tokio::time::sleep(Duration::from_millis(30))=>{}
    }
    assert!(accounts.get(&b.account_id).await.unwrap().is_some());
    drop(held);
    tokio::time::timeout(secs(5.0), remove)
        .await
        .unwrap()
        .unwrap();
    assert!(root.join("marker").is_file());
    let (fresh, guard) = accounts.storage_lease(&a).await.unwrap();
    assert!(!fresh.is_cancelled());
    drop(guard);
    accounts.remove(&a.account_id).await.unwrap();
    assert!(!root.exists());
    core.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_account_delete_rolls_back_related_rows_and_rotates_revocation() {
    let (env, core) = storage_fixture().await;
    let accounts = core.accounts();
    let row = accounts.add(storage_account("rollback")).await.unwrap();
    let root = env.data_dir.join("accounts/rollback");
    std::fs::create_dir_all(&root).unwrap();
    let (old, guard) = accounts.storage_lease(&row).await.unwrap();
    drop(guard);
    let db = core.open_db().unwrap();
    db.conn().execute_batch("INSERT INTO media_file(path,account_id,kind,created_at,last_access_at)
            VALUES ('synthetic','rollback','other',1,1);
        CREATE TRIGGER refuse_remove BEFORE DELETE ON account BEGIN SELECT RAISE(ABORT,'synthetic failure'); END;").unwrap();
    assert!(accounts.remove(&row.account_id).await.is_err());
    assert!(old.is_cancelled());
    assert!(root.exists());
    assert_eq!(
        count(
            &core,
            "SELECT COUNT(*) FROM media_file WHERE account_id=?1",
            &row.account_id
        ),
        1
    );
    let (fresh, guard) = accounts.storage_lease(&row).await.unwrap();
    assert!(!fresh.is_cancelled());
    drop(guard);
    db.conn()
        .execute_batch("DROP TRIGGER refuse_remove")
        .unwrap();
    accounts.remove(&row.account_id).await.unwrap();
    assert!(fresh.is_cancelled());
    assert!(!root.exists());
    core.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn account_creation_never_adopts_or_erases_an_unowned_namespace() {
    let (env, core) = storage_fixture().await;
    let accounts = core.accounts();
    let roots = env.data_dir.join("accounts");
    std::fs::create_dir_all(roots.join("orphan/uploads")).unwrap();
    std::fs::write(roots.join("orphan/uploads/marker"), b"orphan").unwrap();
    std::fs::write(roots.join("ordinary-file"), b"file").unwrap();
    std::os::unix::fs::symlink("missing", roots.join("dangling")).unwrap();
    for id in ["orphan", "ordinary-file", "dangling", ""] {
        assert!(matches!(
            accounts.add(storage_account(id)).await,
            Err(RuntimeError::AccountExists(_))
        ));
        assert!(accounts.get(id).await.unwrap().is_none());
    }
    let automatic = roots.join("storage_fixture-1");
    std::fs::create_dir(&automatic).unwrap();
    assert!(matches!(
        accounts
            .add(NewAccount::new("storage_fixture").no_start())
            .await,
        Err(RuntimeError::AccountExists(_))
    ));
    assert!(automatic.is_dir());
    assert!(roots.join("orphan/uploads/marker").is_file());
    core.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_authority_follows_the_account_start_not_status_or_shared_capabilities() {
    let (_env, core) = storage_fixture().await;
    let plugin = "storage_fixture";
    let id = "run-test";
    let h = core.host();
    assert!(h.running_account_token(plugin, id).is_none());
    let row = core.accounts().add(storage_account(id)).await.unwrap();
    core.accounts().start(id).await.unwrap();
    wait_connected(&core, plugin, id).await;
    let run = h.running_account_token(plugin, id).unwrap();
    let sibling = h.running_account_token(plugin, id).unwrap();
    sibling.cancel();
    assert!(!run.is_cancelled());
    assert!(!h.running_account_token(plugin, id).unwrap().is_cancelled());
    let handshake = h
        .plugin_state(plugin)
        .unwrap()
        .capabilities()
        .unwrap()
        .clone();
    let _: Value = h
        .call(
            plugin,
            "messages.fetch_recent",
            &json!({"account_id":id,"chat_id":"run_status","limit":1}),
        )
        .await
        .unwrap();
    wait_account(h, plugin, id, secs(5.), |a| {
        a.detail.as_deref() == Some("updated")
    })
    .await;
    assert!(!run.is_cancelled());

    h.stop_account(plugin, id).await.unwrap();
    assert!(run.is_cancelled());
    assert!(h.running_account_token(plugin, id).is_none());
    core.accounts().start(&row.account_id).await.unwrap();
    let second = h.running_account_token(plugin, id).unwrap();
    assert!(Arc::ptr_eq(
        &handshake,
        h.plugin_state(plugin).unwrap().capabilities().unwrap()
    ));
    assert!(!second.is_cancelled());

    let mut states = h.subscribe();
    h.restart_plugin(plugin).await.unwrap();
    assert!(second.is_cancelled());
    let third = tokio::time::timeout(secs(10.), async {
        loop {
            if let Some(token) = h.running_account_token(plugin, id) {
                break token;
            }
            states.recv().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(!Arc::ptr_eq(
        &handshake,
        h.plugin_state(plugin).unwrap().capabilities().unwrap()
    ));
    assert!(!third.is_cancelled());
    core.shutdown().await;
    assert!(third.is_cancelled());
    assert!(h.running_account_token(plugin, id).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_disable_and_reenable_complete_in_order_and_revoke_old_run() {
    let env = Env::new();
    let plugin = "hold_stop";
    let id = "toggle-test";
    env.misbehaving(plugin, "hold_stop", json!({}));
    let mut config = core_config(&env, &Arc::new(MemorySecretStore::new()));
    config.autostart = false;
    config.host.timeout_scale = 1.0;
    let core = Core::open(config).await.unwrap();
    let accounts = core.accounts();
    accounts.add(NewAccount::new(plugin).id(id)).await.unwrap();
    wait_connected(&core, plugin, id).await;
    let run = core.host().running_account_token(plugin, id).unwrap();
    let mut events = core.subscribe();
    {
        let mut disable = Box::pin(accounts.set_enabled(id, false));
        tokio::select! {
            result=&mut disable=>panic!("disable returned before the plugin settled: {result:?}"),
            _=wait_change(&mut events,secs(5.),|c|matches!(c,CoreChange::Account(AccountChange::Enabled {account_id,enabled:false}) if account_id==id).then_some(()))=>{}
        }
    }
    assert!(run.is_cancelled());
    assert!(core.host().running_account_token(plugin, id).is_none());
    {
        let mut enable = Box::pin(accounts.set_enabled(id, true));
        tokio::select! {
            biased;
            result=&mut enable=>panic!("enable overtook the outstanding account.stop: {result:?}"),
            _=std::future::ready(())=>{}
        }
    }
    assert!(!accounts.get(id).await.unwrap().unwrap().enabled);
    let marker = core.host().config().state_dir(id).join("release-stop");
    std::fs::write(marker, b"release").unwrap();
    wait_change(&mut events,secs(5.),|c|matches!(c,CoreChange::Account(AccountChange::Enabled {account_id,enabled:true}) if account_id==id).then_some(())).await;
    wait_connected(&core, plugin, id).await;
    assert!(
        !core
            .host()
            .running_account_token(plugin, id)
            .unwrap()
            .is_cancelled()
    );
    assert!(run.is_cancelled());
    core.shutdown().await;
}
