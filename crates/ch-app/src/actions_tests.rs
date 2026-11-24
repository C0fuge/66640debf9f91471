//! App-level round trips over an isolated, deliberately controllable plugin process.
use super::*;
use ch_core::{host::MemorySecretStore, runtime::{CoreConfig, NewAccount}};
use ch_db::types::{ConversationRow, MessageRow, TopicRow};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};

async fn fixture(mode: &str) -> (App, Thread) {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("plugins/action_fixture");
    std::fs::create_dir_all(&dir).unwrap();
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/action_plugin.py");
    let manifest = json!({"id":"action_fixture","name":"Actions fixture","version":"0.0.1","api_version":"1.0.0",
        "entry":["python3","-B","-s",script,mode],"brand_color":"#123456","multi_account":true,"auth_methods":["none"]});
    std::fs::write(dir.join("plugin.json"), manifest.to_string()).unwrap();
    let mut config = CoreConfig::new(temp.path(), Arc::new(MemorySecretStore::new()));
    config.host.env.insert("CH_ACTION_TRACE".into(), temp.path().join("trace.jsonl").to_string_lossy().into_owned());
    config.host.env.insert("CH_DATA_DIR".into(), temp.path().to_string_lossy().into_owned());
    for key in ["XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "XDG_CACHE_HOME", "XDG_RUNTIME_DIR"] {
        let path = temp.path().join(key);
        std::fs::create_dir(&path).unwrap();
        config.host.env.insert(key.into(), path.to_string_lossy().into_owned());
    }
    let app = App::open(config, Some(temp)).await.unwrap();
    let account = app.core.accounts().add(NewAccount::new("action_fixture")).await.unwrap().account_id;
    let key = Thread { account_id: account.clone(), chat_id: "chat".into(), topic_id: None };
    app.read(move |db| {
        let chat = ch_db::insert_conversation(db.conn(), &ConversationRow { account_id: account.clone(), chat_id: "chat".into(), kind: "group".into(), ..Default::default() })?;
        ch_db::insert_conversation(db.conn(), &ConversationRow { account_id: account, chat_id: "other".into(), kind: "group".into(), ..Default::default() })?;
        ch_db::insert_topic(db.conn(), &TopicRow { conversation_id: chat, topic_id: "closed".into(), title: "Closed topic".into(), closed: true, ..Default::default() })?;
        for (id, topic, direction) in [("message", None, "out"), ("topic-message", Some("closed"), "out"), ("incoming", None, "in"), ("local:provisional", None, "out")] {
            ch_db::insert_message(db.conn(), &MessageRow { conversation_id: chat, message_id: id.into(), topic_id: topic.map(str::to_owned), direction: direction.into(), text: Some("original".into()), sent_at: 1793491200000, ..Default::default() })?;
        }
        Ok(())
    }).await.unwrap();
    (app, key)
}

fn trace(app: &App) -> Vec<Value> {
    std::fs::read_to_string(app.core.data_dir().join("trace.jsonl")).unwrap_or_default().lines().map(|l| serde_json::from_str(l).unwrap()).collect()
}
fn calls(app: &App) -> Vec<Value> { trace(app).into_iter().filter(|r| r["method"].is_string()).collect() }
async fn stored(app: &App, thread: &Thread, id: &str) -> Option<MessageRow> {
    let thread = thread.clone(); let id = id.to_owned();
    app.read(move |db| {
        let chat = ch_db::conversation_rowid(db.conn(), &thread.account_id, &thread.chat_id)?.unwrap();
        Ok(ch_db::get_message(db.conn(), chat, &id)?)
    }).await.unwrap()
}
async fn request(app: &App, method: &str, params: Value) -> Value { app.request(json!({"method":method,"params":params})).await }
async fn edit(app: &App, thread: &Thread, id: &str) -> Value {
    request(app, "editText", json!({"thread":thread,"messageId":id,"text":"edited 😀","entities":[{"type":"bold","offset":7,"length":2}]})).await
}
async fn echoes(app: &App, count: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while trace(app).iter().filter(|r| r["echo"].is_number()).count() < count { tokio::time::sleep(Duration::from_millis(5)).await; }
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn four_actions_commit_and_echo_orders_converge() {
    for mode in ["good", "before", "after"] {
        let (app, thread) = fixture(mode).await;
        assert_eq!(edit(&app, &thread, "message").await["ok"], true);
        if mode != "good" { echoes(&app, 1).await; }
        let row = stored(&app, &thread, "message").await.unwrap();
        assert_eq!(row.text.as_deref(), Some("edited 😀"));
        assert_eq!(app.read(move |db| Ok(ch_db::list_message_versions(db.conn(), row.id)?.len())).await.unwrap(), 1);
        let page = request(&app,"history",json!({"thread":thread,"window":{"kind":"latest"},"limit":100})).await;
        assert_eq!(page["value"]["items"].as_array().unwrap().iter().filter(|i| i["message"]["messageId"] == "message").count(), 1);
        assert_eq!(request(&app, "pinMessage", json!({"thread":thread,"messageId":"message","silent":false})).await["ok"], true);
        if mode != "good" { echoes(&app, 2).await; }
        assert_eq!(stored(&app, &thread, "message").await.unwrap().is_pinned, Some(true));
        assert_eq!(request(&app, "unpinMessage", json!({"thread":thread,"messageId":"message"})).await["ok"], true);
        if mode != "good" { echoes(&app, 3).await; }
        assert_eq!(stored(&app, &thread, "message").await.unwrap().is_pinned, Some(false));
        assert_eq!(request(&app, "deleteMessages", json!({"thread":thread,"messageIds":["message"],"revoke":false})).await["ok"], true);
        if mode != "good" { echoes(&app, 4).await; }
        assert!(stored(&app, &thread, "message").await.is_none(), "local deletion is hard, even after a remote tombstone");
        assert_eq!(calls(&app).len(), 4);
        assert_eq!(calls(&app)[0]["params"]["entities"][0]["length"], 2);
        assert_eq!(calls(&app)[1]["params"]["silent"], false);
        assert!(!calls(&app)[1]["params"].as_object().unwrap().contains_key("for_me_only"));
        app.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_foreign_and_provisional_targets_never_reach_rpc() {
    let (app, thread) = fixture("good").await;
    let second = app.core.accounts().add(NewAccount::new("action_fixture").no_start()).await.unwrap();
    for target in [Thread { chat_id: "other".into(), ..thread.clone() }, Thread { topic_id: Some("closed".into()), ..thread.clone() }, Thread { account_id: second.account_id, ..thread.clone() }] {
        for (method, args) in [("editText",json!({"thread":target,"messageId":"message","text":"no","entities":[]})),
            ("deleteMessages",json!({"thread":target,"messageIds":["message"],"revoke":true})),
            ("pinMessage",json!({"thread":target,"messageId":"message","silent":false})),
            ("unpinMessage",json!({"thread":target,"messageId":"message"}))] {
            assert_eq!(request(&app,method,args).await["ok"],false);
        }
    }
    for id in ["local:provisional", "incoming", "missing"] { assert_eq!(edit(&app,&thread,id).await["ok"], false); }
    for ids in [json!([]),json!(["local:provisional"]),json!(["message","message"]),json!(vec!["message";101])] {
        assert_eq!(request(&app,"deleteMessages",json!({"thread":thread,"messageIds":ids,"revoke":true})).await["ok"],false);
    }
    assert_eq!(request(&app,"editText",json!({"thread":thread,"messageId":"message","text":"A😀B","entities":[{"type":"bold","offset":1,"length":1}]})).await["error"]["code"],"invalid_request");
    assert!(calls(&app).is_empty());
    assert_eq!(stored(&app,&thread,"message").await.unwrap().text.as_deref(),Some("original"));
    let closed = Thread { topic_id: Some("closed".into()), ..thread.clone() };
    assert_eq!(edit(&app,&closed,"topic-message").await["ok"],true,"closed topic does not prohibit an edit");
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn current_method_and_parameter_gates_block_rpc() {
    for (cap, method, extra) in [
        ("edit","editText",json!({"messageId":"message","text":"x","entities":[]})),
        ("send.entities","editText",json!({"messageId":"message","text":"x","entities":[]})),
        ("delete","deleteMessages",json!({"messageIds":["message"],"revoke":true})),
        ("delete.for_me","deleteMessages",json!({"messageIds":["message"],"revoke":false})),
        ("pin","pinMessage",json!({"messageId":"message","silent":false})),
        ("unpin","unpinMessage",json!({"messageId":"message"})),
    ] {
        let (app, thread) = fixture(&format!("without-{cap}")).await;
        let mut params = extra; params["thread"] = json!(thread);
        assert_eq!(request(&app,method,params).await["error"]["code"],"unsupported");
        assert!(calls(&app).is_empty());
        assert_eq!(stored(&app,&thread,"message").await.unwrap().text.as_deref(),Some("original"));
        if cap == "delete.for_me" {
            assert_eq!(request(&app,"deleteMessages",json!({"thread":thread,"messageIds":["message"],"revoke":true})).await["ok"],true);
        }
        app.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn denied_actions_preserve_database_and_redact_plugin_data() {
    let (app, thread) = fixture("denied").await;
    let original = stored(&app,&thread,"message").await.unwrap();
    for (method, params) in [("editText",json!({"thread":thread,"messageId":"message","text":"no","entities":[]})),
        ("deleteMessages",json!({"thread":thread,"messageIds":["message"],"revoke":true})),
        ("pinMessage",json!({"thread":thread,"messageId":"message","silent":false})),
        ("unpinMessage",json!({"thread":thread,"messageId":"message"}))] {
        let reply = request(&app,method,params).await;
        assert_eq!(reply["error"]["code"],"conflict");
        assert_ne!(reply["error"]["outcomeUnknown"],true);
        assert!(!reply.to_string().contains("sensitive"));
        assert_eq!(stored(&app,&thread,"message").await.unwrap(),original);
    }
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn edit_requires_exact_confirmation_and_unknown_never_replays() {
    for mode in ["missing","wrong-id","wrong-chat","wrong-topic","unknown"] {
        let (app, thread) = fixture(mode).await;
        let mut invalidations = app.subscribe();
        let reply = edit(&app,&thread,"message").await;
        assert_eq!(reply["error"]["outcomeUnknown"],true,"{mode}: {reply}");
        assert!(!reply.to_string().contains("sensitive"));
        assert_eq!(stored(&app,&thread,"message").await.unwrap().text.as_deref(),Some("original"));
        assert_eq!(calls(&app).len(),1);
        assert!(invalidations.try_recv().is_ok());
        let _ = request(&app,"history",json!({"thread":thread,"window":{"kind":"latest"},"limit":100})).await;
        assert_eq!(calls(&app).len(),1,"cached reload never replays mutation");
        app.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_local_apply_after_platform_success_is_unknown() {
    let (app, thread) = fixture("good").await;
    app.read(|db| {
        db.conn().execute_batch("CREATE TRIGGER reject_edit BEFORE UPDATE ON message BEGIN SELECT RAISE(ABORT, 'synthetic persistence failure'); END;").map_err(ch_db::Error::from)?;
        Ok(())
    }).await.unwrap();
    let reply = edit(&app,&thread,"message").await;
    assert_eq!(reply["error"]["outcomeUnknown"],true);
    assert_eq!(calls(&app).len(),1);
    assert_eq!(stored(&app,&thread,"message").await.unwrap().text.as_deref(),Some("original"));
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutation_locks_reject_overlap_and_prune_invalid_thread_keys() {
    let (app, thread) = fixture("good").await;
    let guard = app.action_lock(&thread).await.unwrap();
    assert!(matches!(app.action_lock(&thread).await, Err(Failure::Conflict)));
    drop(guard);
    for index in 0..64 {
        let invalid = Thread { chat_id: format!("missing-{index}"), ..thread.clone() };
        assert_eq!(edit(&app,&invalid,"message").await["ok"],false);
        assert!(app.action_locks.lock().unwrap().len() <= 1);
    }
    assert!(calls(&app).is_empty());
    app.shutdown().await;
}

async fn seed_poll(app: &App, thread: &Thread, quiz: bool, chosen: Option<bool>) {
    let thread = thread.clone();
    app.read(move |db| {
        let chat = ch_db::conversation_rowid(db.conn(), &thread.account_id, &thread.chat_id)?.unwrap();
        let mut row = ch_db::get_message(db.conn(), chat, "message")?.unwrap();
        row.poll = Some(ch_protocol::dto::Poll {
            id: Some("poll".into()), question: "Literal <question>".into(), quiz,
            options: vec![ch_protocol::dto::PollOption { id: "a".into(), text: "First".into(), chosen, correct: Some(true), ..Default::default() },
                ch_protocol::dto::PollOption { id: "b".into(), text: "Second".into(), chosen, ..Default::default() }],
            solution: Some("hidden answer".into()), ..Default::default()
        });
        ch_db::update_message(db.conn(), &row)?;
        Ok(())
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poll_actions_ingest_only_reported_state_and_refresh_exact_older_target() {
    let (app, thread) = fixture("good").await;
    seed_poll(&app, &thread, false, Some(false)).await;
    let vote = request(&app,"votePoll",json!({"thread":thread,"messageId":"message","optionIds":["a"]})).await;
    assert_eq!(vote["value"]["poll"]["options"][0]["chosen"],true);
    assert!(!stored(&app,&thread,"message").await.unwrap().edited);
    assert_eq!(request(&app,"closePoll",json!({"thread":thread,"messageId":"message"})).await["value"]["poll"]["closed"],true);
    let scope = thread.clone();
    app.read(move |db| {
        let chat = ch_db::conversation_rowid(db.conn(), &scope.account_id, &scope.chat_id)?.unwrap();
        ch_db::insert_message(db.conn(), &MessageRow { conversation_id: chat, message_id: "newer".into(), direction: "in".into(), sent_at: 1793491201000, ..Default::default() })?;
        db.conn().execute("INSERT INTO history_state (conversation_id,older_cursor,oldest_message_id,newest_message_id,last_fetch_at,updated_at) VALUES (?1,'opaque-cursor','old','newer',7,8)", [chat]).map_err(ch_db::Error::from)?;
        db.conn().execute("INSERT INTO history_gap (conversation_id,after_message_id,before_message_id,cursor,created_at,updated_at) VALUES (?1,'old','newer','opaque-gap',3,4)", [chat]).map_err(ch_db::Error::from)?;
        Ok(())
    }).await.unwrap();
    let latest = request(&app,"history",json!({"thread":thread,"window":{"kind":"latest"},"limit":1})).await;
    assert!(latest["value"]["items"].as_array().unwrap().iter().all(|item| item["message"]["messageId"] != "message"));
    let before = poll_history_metadata(&app).await;
    assert_eq!(request(&app,"refreshPoll",json!({"thread":thread,"messageId":"message"})).await["ok"],true);
    let sent = calls(&app);
    assert_eq!(sent.iter().map(|v|v["method"].as_str().unwrap()).collect::<Vec<_>>(),vec!["polls.vote","polls.close","messages.get"]);
    assert_eq!(sent[2]["params"]["message_id"],"message");
    assert!(!sent[2]["params"].as_object().unwrap().contains_key("limit"));
    assert_eq!(poll_history_metadata(&app).await,before);
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn polls_optional_confirmation_is_legal_but_foreign_writes_are_unknown() {
    for mode in ["missing","null-message","no-poll","wrong-id","wrong-chat","wrong-topic","wrong-poll","unknown"] {
        let (app, thread) = fixture(mode).await;
        seed_poll(&app,&thread,false,Some(false)).await;
        let result = request(&app,"votePoll",json!({"thread":thread,"messageId":"message","optionIds":["a"]})).await;
        if matches!(mode,"missing"|"null-message"|"no-poll") {
            assert_eq!(result["ok"],true,"{mode}: {result}");
            assert!(result["value"]["poll"].is_null());
        } else { assert_eq!(result["error"]["outcomeUnknown"],true,"{mode}: {result}"); }
        assert_eq!(stored(&app,&thread,"message").await.unwrap().poll.unwrap().options[0].chosen,Some(false));
        assert_eq!(calls(&app).len(),1);
        app.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn polls_quiz_sparse_and_ownership_guards_precede_dispatch() {
    let (app, thread) = fixture("good").await;
    seed_poll(&app,&thread,true,None).await;
    let row = stored(&app,&thread,"message").await.unwrap();
    let projected = crate::projection::poll(&row).unwrap();
    assert!(projected["solution"].is_null());
    assert!(projected["options"][0]["correct"].is_null());
    assert!(projected["options"][0]["chosen"].is_null());
    for ids in [json!(["a"]),json!([]),json!(["a","a"]),json!(["foreign"])] {
        assert_eq!(request(&app,"votePoll",json!({"thread":thread,"messageId":"message","optionIds":ids})).await["ok"],false);
    }
    let foreign = Thread { topic_id: Some("closed".into()), ..thread.clone() };
    assert_eq!(request(&app,"refreshPoll",json!({"thread":foreign,"messageId":"message"})).await["ok"],false);
    assert!(calls(&app).is_empty());
    seed_poll(&app,&thread,true,Some(true)).await;
    let projected = crate::projection::poll(&stored(&app,&thread,"message").await.unwrap()).unwrap();
    assert_eq!(projected["solution"],"hidden answer");
    assert_eq!(projected["options"][0]["correct"],true);
    assert_eq!(request(&app,"votePoll",json!({"thread":thread,"messageId":"message","optionIds":[]})).await["ok"],false);
    app.shutdown().await;
}

async fn poll_history_metadata(app: &App) -> (String, String) {
    app.read(|db| {
        let state = db.conn().query_row("SELECT older_cursor || ':' || oldest_message_id || ':' || newest_message_id || ':' || last_fetch_at || ':' || updated_at FROM history_state", [], |r|r.get::<_,String>(0)).map_err(ch_db::Error::from)?;
        let gap = db.conn().query_row("SELECT after_message_id || ':' || before_message_id || ':' || cursor || ':' || created_at || ':' || updated_at FROM history_gap", [], |r|r.get::<_,String>(0)).map_err(ch_db::Error::from)?;
        Ok((state,gap))
    }).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn normal_poll_validation_and_live_capabilities_precede_rpc() {
    let (app, thread) = fixture("good").await;
    seed_poll(&app,&thread,false,Some(false)).await;
    for ids in [json!([]),json!(["a","a"]),json!(["a","b"]),json!(["foreign"])] {
        assert_eq!(request(&app,"votePoll",json!({"thread":thread,"messageId":"message","optionIds":ids})).await["ok"],false);
    }
    assert!(calls(&app).is_empty());
    seed_poll(&app,&thread,false,Some(true)).await;
    assert_eq!(request(&app,"votePoll",json!({"thread":thread,"messageId":"message","optionIds":[]})).await["ok"],true);
    assert_eq!(calls(&app)[0]["params"]["option_ids"],json!([]));
    app.shutdown().await;
    for (cap,method,extra) in [("messages.get","votePoll",json!({"optionIds":["a"]})),("polls.vote","votePoll",json!({"optionIds":["a"]})),("polls.close","closePoll",json!({}))] {
        let (app,thread) = fixture(&format!("without-{cap}")).await;
        seed_poll(&app,&thread,false,Some(false)).await;
        let mut params = extra; params["thread"] = json!(thread); params["messageId"] = json!("message");
        assert_eq!(request(&app,method,params).await["error"]["code"],"unsupported");
        assert!(calls(&app).is_empty());
        app.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poll_event_reply_orders_converge_without_edit_labels_or_duplicate_rows() {
    for mode in ["before","after"] {
        let (app,thread) = fixture(mode).await;
        seed_poll(&app,&thread,false,Some(false)).await;
        assert_eq!(request(&app,"votePoll",json!({"thread":thread,"messageId":"message","optionIds":["b"]})).await["ok"],true);
        echoes(&app,1).await;
        let row = stored(&app,&thread,"message").await.unwrap();
        assert_eq!(row.poll.unwrap().options[1].chosen,Some(true));
        assert!(!row.edited);
        let history = request(&app,"history",json!({"thread":thread,"window":{"kind":"latest"},"limit":100})).await;
        assert_eq!(history["value"]["items"].as_array().unwrap().iter().filter(|item| item["message"]["messageId"] == "message").count(),1);
        assert_eq!(calls(&app).len(),1);
        app.shutdown().await;
    }
}

#[test]
fn poll_projection_preserves_precision_false_and_zero_without_revealing_hidden_answers() {
    let mut row = MessageRow { message_id: "confirmed".into(), poll: Some(ch_protocol::dto::Poll {
        id: None, question: "quiz".into(), quiz: true, total_voters: Some(u64::MAX),
        options: vec![ch_protocol::dto::PollOption { id: "opaque".into(), text: "literal".into(), votes: Some(0), chosen: Some(false), correct: Some(false) }],
        solution: Some("hidden".into()), ..Default::default()
    }), ..Default::default() };
    let projected = crate::projection::poll(&row).unwrap();
    assert_eq!(projected["totalVoters"],"18446744073709551615");
    assert_eq!(projected["options"][0]["votes"],"0");
    assert_eq!(projected["options"][0]["chosen"],false);
    assert!(projected["solution"].is_null());
    assert!(projected["options"][0]["correct"].is_null());
    row.poll.as_mut().unwrap().closed = true;
    assert_eq!(crate::projection::poll(&row).unwrap()["options"][0]["correct"],false);
    row.message_id = "local:unconfirmed".into();
    assert!(crate::projection::poll(&row).is_none());
    row.message_id = "confirmed".into(); row.deleted_at = Some(1);
    assert!(crate::projection::poll(&row).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_poll_reads_preserve_stored_state_and_foreign_reads_are_ordinary_failures() {
    for mode in ["missing","null-message","no-poll","wrong-topic","wrong-poll"] {
        let (app,thread) = fixture(mode).await;
        seed_poll(&app,&thread,false,None).await;
        let before = stored(&app,&thread,"message").await.unwrap().poll;
        let result = request(&app,"refreshPoll",json!({"thread":thread,"messageId":"message"})).await;
        if mode.starts_with("wrong-") {
            assert_eq!(result["ok"],false);
            assert_ne!(result["error"]["outcomeUnknown"],true);
        } else {
            assert_eq!(result["ok"],true);
            assert!(result["value"]["poll"].is_null());
        }
        assert_eq!(stored(&app,&thread,"message").await.unwrap().poll,before);
        assert_eq!(calls(&app).len(),1);
        app.shutdown().await;
    }
}
