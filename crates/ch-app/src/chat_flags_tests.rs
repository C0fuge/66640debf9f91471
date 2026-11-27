//! Actual App requests against the Python SDK in isolated temporary storage.
use super::*;
use ch_core::{host::MemorySecretStore, runtime::{CoreConfig, NewAccount}};
use std::path::Path;

async fn fixture(mode: &str) -> (App, Thread) {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("plugins/flags_fixture");
    std::fs::create_dir_all(&dir).unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let script = root.join("crates/ch-app/tests/fixtures/chat_flags_plugin.py");
    std::fs::write(dir.join("plugin.json"), json!({
        "id":"flags_fixture", "name":"Chat flag fixture", "version":"0.0.1", "api_version":"1.1.0",
        "entry":["python3","-B","-s",script,mode], "brand_color":"#123456", "multi_account":true, "auth_methods":["none"]
    }).to_string()).unwrap();
    let mut config = CoreConfig::new(temp.path(), Arc::new(MemorySecretStore::new()));
    if mode == "lost-write" { config.host.timeout_scale = 0.1; }
    config.host.env.insert("PYTHONPATH".into(), root.join("sdk/python/src").to_string_lossy().into_owned());
    config.host.env.insert("CH_FLAGS_TRACE".into(), temp.path().join("trace.jsonl").to_string_lossy().into_owned());
    config.host.env.insert("CH_DATA_DIR".into(), temp.path().to_string_lossy().into_owned());
    for key in ["XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "XDG_CACHE_HOME", "XDG_RUNTIME_DIR"] {
        let path = temp.path().join(key);
        std::fs::create_dir(&path).unwrap();
        config.host.env.insert(key.into(), path.to_string_lossy().into_owned());
    }
    let app = App::open(config, Some(temp)).await.unwrap();
    let account = app.core.accounts().add(NewAccount::new("flags_fixture")).await.unwrap();
    let thread = Thread { account_id: account.account_id, chat_id: "chat".into(), topic_id: None };
    seed(&app, &thread).await;
    (app, thread)
}

async fn seed(app: &App, thread: &Thread) {
    let thread = thread.clone();
    app.read(move |db| {
        ch_db::insert_conversation(db.conn(), &ConversationRow {
            account_id: thread.account_id, chat_id: thread.chat_id, kind: "group".into(),
            title: Some("Original title".into()), pinned: Some(true), marked_unread: Some(true),
            archived: true, muted: Some(true), unread_count: 7, ..Default::default()
        })?;
        Ok(())
    }).await.unwrap();
}
async fn request(app: &App, method: &str, params: Value) -> Value {
    app.request(json!({"method":method,"params":params})).await
}
async fn refresh(app: &App, thread: &Thread) -> Value {
    request(app, "refreshChatFlags", json!({"thread":thread})).await
}
async fn set(app: &App, thread: &Thread, context: &Value, flag: &str, value: bool) -> Value {
    request(app, "setChatFlag", json!({"thread":thread,"sessionId":context["sessionId"],
        "contextToken":context["contextToken"],"flag":flag,"value":value})).await
}
fn trace(app: &App) -> Vec<Value> {
    std::fs::read_to_string(app.core.data_dir().join("trace.jsonl")).unwrap_or_default().lines()
        .map(|line| serde_json::from_str(line).unwrap()).collect()
}
fn calls(app: &App, method: &str) -> usize { trace(app).iter().filter(|v| v["method"] == method).count() }
async fn stored(app: &App, thread: &Thread) -> ConversationRow {
    let thread = thread.clone();
    app.read(move |db| Ok(ch_db::get_conversation(db.conn(), &thread.account_id, &thread.chat_id)?.unwrap())).await.unwrap()
}
async fn wait_calls(app: &App, method: &str, count: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while calls(app, method) < count { tokio::time::sleep(Duration::from_millis(5)).await; }
    }).await.unwrap();
}
async fn release(app: &App, thread:&Thread) {
    let _: Value = app.core.host().call("flags_fixture", "messages.fetch_recent",
        &json!({"account_id":thread.account_id,"chat_id":"fixture-release","limit":1})).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_observations_commit_only_exact_flags_and_each_boolean_reaches_one_rpc() {
    let (app, thread) = fixture("event").await;
    for flag in ["pinned", "markedUnread"] {
        for value in [true, false] {
            let c = refresh(&app, &thread).await;
            assert_eq!(c["ok"], true, "{c}");
            assert_eq!(c["value"]["thread"], json!(thread));
            assert_eq!(set(&app, &thread, &c["value"], flag, value).await["ok"], true);
            assert_eq!(set(&app, &thread, &c["value"], flag, value).await["ok"], false);
            let row = stored(&app, &thread).await;
            assert_eq!(if flag == "pinned" { row.pinned } else { row.marked_unread }, Some(value));
            assert_eq!(row.title.as_deref(), Some("Original title"));
            assert_eq!((row.archived, row.muted, row.unread_count), (true, Some(true), 7));
        }
    }
    let writes: Vec<_> = trace(&app).into_iter().filter(|v| v["method"] == "chats.set_flags").collect();
    assert_eq!(writes.len(), 4);
    for (index, field) in ["pinned", "pinned", "marked_unread", "marked_unread"].iter().enumerate() {
        assert_eq!(writes[index]["params"][field], index % 2 == 0);
        assert_eq!(writes[index]["params"].as_object().unwrap().len(), 3);
    }
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_setter_result_never_fabricates_requested_flags() {
    let (app, thread) = fixture("good").await;
    let c = refresh(&app, &thread).await;
    assert_eq!(set(&app, &thread, &c["value"], "pinned", true).await["ok"], true);
    assert_eq!(stored(&app, &thread).await.pinned, Some(false));
    let c = refresh(&app, &thread).await;
    assert_eq!(c["value"]["pinned"], true, "only a new observation confirms the remote state");
    assert_eq!(stored(&app, &thread).await.pinned, Some(true));
    assert_eq!(calls(&app, "chats.set_flags"), 1);
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sparse_observations_keep_cache_but_never_authorize_unknown_fields() {
    for mode in ["partial", "sparse"] {
        let (app, thread) = fixture(mode).await;
        let c = refresh(&app, &thread).await;
        assert_eq!(c["ok"], true, "{c}");
        assert!(c["value"]["markedUnread"].is_null());
        assert_eq!(stored(&app, &thread).await.marked_unread, Some(true));
        assert_eq!(set(&app, &thread, &c["value"], "markedUnread", false).await["ok"], false);
        if mode == "sparse" {
            assert!(c["value"]["contextToken"].is_null());
            assert_eq!(stored(&app, &thread).await.pinned, Some(true));
        } else {
            assert_eq!(c["value"]["pinned"], false);
            let c = refresh(&app, &thread).await;
            assert_eq!(set(&app, &thread, &c["value"], "pinned", true).await["ok"], true);
        }
        assert_eq!(calls(&app, "chats.set_flags"), usize::from(mode == "partial"));
        app.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreign_malformed_and_missing_reads_never_mutate_or_issue_proofs() {
    for mode in ["wrong-id", "wrong-kind", "missing", "null", "invalid-flag"] {
        let (app, thread) = fixture(mode).await;
        let before = stored(&app, &thread).await;
        let c = refresh(&app, &thread).await;
        assert_eq!(c["ok"], false, "{mode}: {c}");
        assert!(app.chat_flags.state().proofs.is_empty());
        assert_eq!(stored(&app, &thread).await, before);
        assert_eq!(calls(&app, "chats.get"), 1);
        app.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn method_and_each_parameter_capability_apply_to_false_as_well_as_true() {
    for cap in ["chat.get", "chat.flags", "chat.pin", "chat.mark_unread"] {
        let (app, thread) = fixture(&format!("without-{cap}")).await;
        for flag in ["pinned", "markedUnread"] {
            for value in [true, false] {
                let c = refresh(&app, &thread).await;
                if cap == "chat.get" { assert_eq!(c["error"]["code"], "unsupported"); continue; }
                assert_eq!(c["ok"], true, "{c}");
                let result = set(&app, &thread, &c["value"], flag, value).await;
                let disabled = cap == "chat.flags" || cap == "chat.pin" && flag == "pinned"
                    || cap == "chat.mark_unread" && flag == "markedUnread";
                assert_eq!(result["ok"], !disabled, "{cap}/{flag}/{value}: {result}");
            }
        }
        let expected = if matches!(cap, "chat.pin" | "chat.mark_unread") { 2 } else { 0 };
        assert_eq!(calls(&app, "chats.set_flags"), expected);
        app.shutdown().await;
    }
    let (app, thread) = fixture("object-caps").await;
    let c = refresh(&app, &thread).await;
    assert_eq!(set(&app, &thread, &c["value"], "pinned", false).await["ok"], true);
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_topics_provisionals_null_values_and_foreign_proofs_never_dispatch() {
    let (app, thread) = fixture("good").await;
    let c = refresh(&app, &thread).await["value"].clone();
    let other = app.core.accounts().add(NewAccount::new("flags_fixture")).await.unwrap();
    let foreign = Thread { account_id: other.account_id, ..thread.clone() };
    seed(&app, &foreign).await;
    for target in [foreign, Thread { topic_id: Some("topic".into()), ..thread.clone() },
        Thread { chat_id: "local:provisional".into(), ..thread.clone() }] {
        assert_eq!(set(&app, &target, &c, "pinned", true).await["ok"], false);
    }
    for value in [Value::Null, json!("false"), json!(0)] {
        assert_eq!(request(&app, "setChatFlag", json!({"thread":thread,"sessionId":c["sessionId"],
            "contextToken":c["contextToken"],"flag":"pinned","value":value})).await["error"]["code"], "invalid_request");
    }
    assert_eq!(calls(&app, "chats.set_flags"), 0);
    assert_eq!(set(&app, &thread, &c, "pinned", true).await["ok"], true, "foreign attempts do not consume a matching proof");
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn superseded_expired_and_previous_run_proofs_cannot_authorize_writes() {
    let (app, thread) = fixture("good").await;
    let old = refresh(&app, &thread).await["value"].clone();
    let fresh = refresh(&app, &thread).await["value"].clone();
    assert_eq!(set(&app, &thread, &old, "pinned", true).await["ok"], false);
    app.chat_flags.state().proofs.get_mut(fresh["contextToken"].as_str().unwrap()).unwrap().expires = Instant::now() - Duration::from_secs(1);
    assert_eq!(set(&app, &thread, &fresh, "pinned", true).await["ok"], false);
    let c = refresh(&app, &thread).await["value"].clone();
    app.core.host().stop_account("flags_fixture", &thread.account_id).await.unwrap();
    app.core.accounts().start(&thread.account_id).await.unwrap();
    assert_eq!(set(&app, &thread, &c, "pinned", true).await["ok"], false);
    let c = refresh(&app, &thread).await["value"].clone();
    app.core.accounts().remove(&thread.account_id).await.unwrap();
    app.core.accounts().add(NewAccount::new("flags_fixture").id(&thread.account_id)).await.unwrap();
    seed(&app, &thread).await;
    assert_eq!(set(&app, &thread, &c, "pinned", true).await["ok"], false);
    assert_eq!(calls(&app, "chats.set_flags"), 0);
    let c = refresh(&app, &thread).await;
    assert_eq!(set(&app, &thread, &c["value"], "pinned", true).await["ok"], true);
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatched_failures_are_unknown_redacted_and_never_replayed() {
    for mode in ["unknown", "malformed-write", "lost-write"] {
        let (app, thread) = fixture(mode).await;
        let c = refresh(&app, &thread).await["value"].clone();
        let reply = set(&app, &thread, &c, "pinned", true).await;
        assert_eq!(reply["error"]["outcomeUnknown"], true, "{mode}: {reply}");
        assert!(!reply.to_string().contains("sensitive"));
        assert_eq!(stored(&app, &thread).await.pinned, Some(false));
        assert_eq!(set(&app, &thread, &c, "pinned", true).await["ok"], false);
        assert_eq!(calls(&app, "chats.set_flags"), 1);
        let fresh = refresh(&app, &thread).await;
        assert_eq!(fresh["value"]["pinned"], true);
        assert_eq!(calls(&app, "chats.set_flags"), 1);
        app.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_read_cannot_restore_removed_rows_or_a_replaced_account_run() {
    for replace in [false, true] {
        let (app, thread) = fixture("hold-read").await;
        let app = Arc::new(app);
        let pending = { let app = app.clone(); let thread = thread.clone(); tokio::spawn(async move { refresh(&app, &thread).await }) };
        wait_calls(&app, "chats.get", 1).await;
        if replace {
            app.core.host().stop_account("flags_fixture", &thread.account_id).await.unwrap();
            app.core.accounts().start(&thread.account_id).await.unwrap();
        } else {
            let target = thread.clone();
            app.read(move |db| {
                db.conn().execute("DELETE FROM conversation WHERE account_id=?1 AND chat_id=?2", [&target.account_id, &target.chat_id]).map_err(ch_db::Error::from)?;
                Ok(())
            }).await.unwrap();
        }
        release(&app,&thread).await;
        assert_eq!(pending.await.unwrap()["ok"], false);
        assert!(app.chat_flags.state().proofs.is_empty());
        if replace { assert_eq!(stored(&app, &thread).await.pinned, Some(true)); }
        else {
            let target = thread.clone();
            assert!(app.read(move |db| Ok(ch_db::get_conversation(db.conn(), &target.account_id, &target.chat_id)?.is_none())).await.unwrap());
        }
        app.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_callers_keep_four_task_slots_and_thread_locks_until_settlement() {
    let (app, base) = fixture("hold-write").await;
    let app = Arc::new(app);
    let mut tasks = Vec::new();
    for n in 0..4 {
        let thread = Thread { chat_id: format!("c{n}"), ..base.clone() };
        seed(&app, &thread).await;
        let c = refresh(&app, &thread).await["value"].clone();
        let owner = app.clone();
        tasks.push(tokio::spawn(async move { set(&owner, &thread, &c, "pinned", true).await }));
    }
    wait_calls(&app, "chats.set_flags", 4).await;
    for task in tasks { task.abort(); let _ = task.await; }
    assert_eq!(app.chat_flags.slots.available_permits(), 0);
    assert_eq!(refresh(&app, &base).await["error"]["code"], "conflict");
    release(&app,&base).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while app.chat_flags.slots.available_permits() != 4 { tokio::time::sleep(Duration::from_millis(5)).await; }
    }).await.unwrap();
    assert_eq!(calls(&app, "chats.set_flags"), 4);
    assert_eq!(refresh(&app, &base).await["ok"], true);
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn context_limit_and_disabled_owner_block_before_any_mutation() {
    let (app, thread) = fixture("good").await;
    let c = refresh(&app, &thread).await["value"].clone();
    {
        let mut state = app.chat_flags.state();
        let seed = state.proofs.remove(c["contextToken"].as_str().unwrap()).unwrap();
        for n in 0..LIMIT {
            let target = Arc::new(Target {
                thread: Thread { chat_id: format!("proof-{n}"), ..thread.clone() },
                owner: seed.target.owner.clone(), row: seed.target.row.clone(),
                caps: seed.target.caps.clone(), run: seed.target.run.clone(),
            });
            state.proofs.insert(format!("context-{n}"), Proof {
                target, expires: Instant::now() + TTL, pinned: Some(false), marked_unread: None, archived: None,
            });
        }
    }
    assert_eq!(refresh(&app, &thread).await["error"]["code"], "conflict");
    assert_eq!(app.chat_flags.state().proofs.len(), LIMIT);
    for proof in app.chat_flags.state().proofs.values_mut() { proof.expires = Instant::now() - Duration::from_secs(1); }
    let c = refresh(&app, &thread).await["value"].clone();
    assert_eq!(app.chat_flags.state().proofs.len(), 1);
    app.core.accounts().set_enabled(&thread.account_id, false).await.unwrap();
    assert_eq!(set(&app, &thread, &c, "pinned", true).await["ok"], false);
    assert_eq!(refresh(&app, &thread).await["ok"], false);
    assert_eq!(calls(&app, "chats.set_flags"), 0);
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_requires_an_observed_field_and_each_live_capability() {
    for mode in ["missing-archive", "without-chat.archive", "without-chat.flags", "without-chat.get"] {
        let (app, thread) = fixture(mode).await;
        for value in [true, false] {
            let c = refresh(&app, &thread).await;
            if mode == "without-chat.get" {
                assert_eq!(c["error"]["code"], "unsupported");
                continue;
            }
            assert_eq!(set(&app, &thread, &c["value"], "archived", value).await["ok"], false);
        }
        assert_eq!(calls(&app, "chats.set_flags"), 0);
        assert!(stored(&app, &thread).await.archived);
        app.shutdown().await;
    }
    let (app, thread) = fixture("archive-only").await;
    let c = refresh(&app, &thread).await["value"].clone();
    assert!(c["pinned"].is_null() && c["markedUnread"].is_null());
    assert_eq!(c["archived"], true);
    assert!(c["contextToken"].is_string());
    assert_eq!(set(&app, &thread, &c, "archived", false).await["ok"], true);
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_moves_only_on_observed_state_and_sends_exact_boolean_once() {
    let (app, thread) = fixture("good").await;
    for value in [false, true] {
        let c = refresh(&app, &thread).await["value"].clone();
        assert_eq!(c["archived"], !value);
        assert_eq!(set(&app, &thread, &c, "archived", value).await["ok"], true);
        assert_eq!(stored(&app, &thread).await.archived, !value, "Empty is not an observation");
        assert_eq!(set(&app, &thread, &c, "archived", value).await["ok"], false);
        let fresh = refresh(&app, &thread).await;
        assert_eq!(fresh["value"]["archived"], value);
        let row = stored(&app, &thread).await;
        assert_eq!(row.archived, value);
        assert_eq!((row.title.as_deref(), row.muted, row.unread_count), (Some("Original title"), Some(true), 7));
        for archived in [false, true] {
            let chats = request(&app, "listChats", json!({"accountId":thread.account_id,"archived":archived})).await;
            assert_eq!(chats["value"].as_array().unwrap().len(), usize::from(archived == value));
        }
    }
    let mutations: Vec<_> = trace(&app).into_iter().filter(|v| v["method"] == "chats.set_flags").collect();
    assert_eq!(mutations.len(), 2);
    for (request, value) in mutations.iter().zip([false, true]) {
        assert_eq!(request["params"], json!({"account_id":thread.account_id,"chat_id":thread.chat_id,"archived":value}));
    }
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_lost_reply_requires_a_new_observation_without_replaying() {
    let (app, thread) = fixture("unknown").await;
    let observed = refresh(&app, &thread).await;
    assert_eq!(observed["ok"], true, "{observed}");
    let c = observed["value"].clone();
    let result = set(&app, &thread, &c, "archived", false).await;
    assert_eq!(result["error"]["outcomeUnknown"], true, "{result}");
    assert!(stored(&app, &thread).await.archived);
    assert_eq!(set(&app, &thread, &c, "archived", false).await["ok"], false);
    assert_eq!(refresh(&app, &thread).await["value"]["archived"], false);
    assert_eq!(calls(&app, "chats.set_flags"), 1);
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_unknown_write_releases_thread_and_slot_before_reply_tail_finishes() {
    let (app, thread) = fixture("unknown").await;
    let observed = refresh(&app, &thread).await;
    assert_eq!(observed["ok"], true, "{observed}");
    let reached = Arc::new(tokio::sync::Notify::new());
    let signal = reached.clone();
    let (release, wait) = std::sync::mpsc::channel();
    *app.chat_flags.after_reply.lock().unwrap() = Some(Box::new(move || {
        signal.notify_one();
        // Dropping the sender on assertion failure also releases this worker.
        let _ = wait.recv_timeout(Duration::from_secs(5));
    }));
    let result = set(&app, &thread, &observed["value"], "archived", false).await;
    assert_eq!(result["error"]["outcomeUnknown"], true, "{result}");
    tokio::time::timeout(Duration::from_secs(5), reached.notified()).await.unwrap();
    assert_eq!(app.chat_flags.slots.available_permits(), 4);
    let fresh = refresh(&app, &thread).await;
    assert_eq!(fresh["ok"], true, "{fresh}");
    assert_eq!(fresh["value"]["archived"], false);
    assert_eq!(calls(&app, "chats.set_flags"), 1);
    release.send(()).unwrap();
    app.shutdown().await;
}
