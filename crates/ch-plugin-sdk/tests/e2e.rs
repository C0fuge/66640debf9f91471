//! End-to-end: `ch_plugin_sdk::testing::FakeCore` drives plugins over in-memory pipes and a
//! spawned `examples/echo` process.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ch_plugin_sdk::ch_protocol::events::{ContactsUpsertEvent, PresenceEvent};
use ch_plugin_sdk::prelude::*;
use ch_plugin_sdk::testing::FakeCore;
use serde_json::{Value, json};

#[derive(Default)]
struct Flags {
    download_cleaned_up: AtomicBool,
    spammer_stopped: AtomicBool,
}

fn ok_value(c: Call<Value>) -> impl Future<Output = HandlerResult<Empty>> {
    drop(c);
    async { Ok(Empty {}) }
}

fn test_plugin(flags: Arc<Flags>) -> PluginBuilder {
    let f1 = flags.clone();
    PluginBuilder::new()
        .version("9.9.9")
        .capability("media.download")
        .capability_with("chat.get", &json!({}))
        .method("chats.get", |c: Call<ChatsGetParams>| async move {
            Ok(ChatsGetResult {
                conversation: Conversation::new(c.params.chat_id, ConversationKind::Dm).with_pinned(false),
            })
        })
        .capability_with("send.album", &json!({"max": 10}))
        .capability("reactions.set")
        .capability("edit")
        .on_account_start(move |acct: Account| {
            let flags = f1.clone();
            async move {
                // Settings and secrets as the spec hands them over.
                assert_eq!(
                    acct.settings().get::<String>("greeting")?.as_deref(),
                    Some("hello")
                );
                assert_eq!(acct.settings().get_or::<u32>("missing", 7)?, 7);
                assert!(acct.settings().get::<bool>("retries").is_err());
                assert_eq!(acct.secret("session"), Some("s1"));
                acct.set_data(String::from("client"));
                let bg = acct.clone();
                tokio::spawn(async move {
                    bg.emit_acked(Event::ContactsUpsert(ContactsUpsertEvent {
                        account_id: bg.id().clone(),
                        contacts: vec![],
                    }))
                    .await
                    .unwrap();
                    bg.connected(None).unwrap();
                    // A background loop that ends when the account stops.
                    loop {
                        let ev = PresenceEvent {
                            account_id: String::new(),
                            user_id: "u1".into(),
                            chat_id: None,
                            topic_id: None,
                            online: None,
                            action: None,
                            expires_at: None,
                        };
                        if bg.presence(ev).is_err() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    flags.spammer_stopped.store(true, Ordering::SeqCst);
                });
                Ok(())
            }
        })
        .method(
            "messages.fetch_recent",
            |c: Call<MessagesFetchRecentParams>| async move {
                if c.params.chat_id == "flood" {
                    return Err(error::flood_wait(37));
                }
                Ok(PagedMessagesResult::default())
            },
        )
        .method("messages.send", |c: Call<MessagesSendParams>| async move {
            let acct = c.account()?;
            assert_eq!(
                acct.data::<String>().as_deref().map(String::as_str),
                Some("client")
            );
            if c.params.chat_id == "c1" {
                acct.set_secret("token", "t0k").await?;
            }
            let m = Message::new("m1", &c.params.chat_id, Direction::Out, Timestamp::now())
                .with_text(&c.params.text);
            acct.messages_upsert(vec![m.clone()])?;
            Ok(MessageResult { message: m })
        })
        .method("messages.send_album", ok_value)
        .method("messages.edit", ok_value)
        .method("reactions.set", ok_value)
        .method("reactions.available", ok_value)
        .method("media.download", move |c: Call<MediaDownloadParams>| {
            let flags = flags.clone();
            async move {
                if c.params.chat_id == "secret" {
                    // A plugin -> core request that the core never answers.
                    let e = c.account()?.set_secret("k", "v").await.unwrap_err();
                    return Err(e.into());
                }
                c.progress(TransferDirection::Download, 10, Some(100))?;
                c.cancelled().await;
                flags.download_cleaned_up.store(true, Ordering::SeqCst);
                Err::<MediaDownloadResult, _>(error::cancelled())
            }
        })
        .method("x-test.secret", |c: Call<Value>| async move {
            // The core's error reaches the caller as sent, unknown code included (§13).
            let e = match c.account()?.set_secret("k", "v").await {
                Err(EmitError::Rejected(e)) => e,
                other => panic!("expected a rejection, got {other:?}"),
            };
            if c.params["reemit"] == true {
                return Err(e);
            }
            let code = e.data.as_ref().map(|d| d.code.to_string());
            Ok(json!({"numeric": e.code, "code": code, "message": e.message}))
        })
        .method("x-test.hang", |_c: Call<Value>| {
            std::future::pending::<HandlerResult<Empty>>()
        })
}

fn start_params(account: &str) -> Value {
    json!({"account_id": account, "settings": {"greeting": "hello", "retries": 3},
           "secrets": {"session": "s1"}})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handshake_start_call_errors_cancel_stop_shutdown() {
    let flags = Arc::new(Flags::default());
    let mut core = FakeCore::new(test_plugin(flags.clone()).build().unwrap());

    // Before initialize: requests are INVALID_REQUEST, notifications are ignored (§4.1).
    let e = core.call("ping", json!({})).await.unwrap_err();
    assert_eq!(e.code, -32600);
    core.cancel(1).await;
    core.notify("x-test.note", json!({})).await;
    // Malformed frames.
    core.send_raw(b"\r\n\nnot json\n").await;
    let e = core.next_frame(Duration::from_secs(5)).await.unwrap();
    assert_eq!(e["id"], Value::Null);
    assert_eq!(
        (e["error"]["code"].as_i64(), &e["error"]["data"]["code"]),
        (Some(-32700), &json!("PARSE_ERROR"))
    );
    core.send_raw(b"[{\"jsonrpc\":\"2.0\",\"id\":\"b\",\"method\":\"ping\"}]\n")
        .await;
    let e = core.next_frame(Duration::from_secs(5)).await.unwrap();
    assert_eq!(e["error"]["code"], -32600);

    // Handshake; a second initialize is INVALID_REQUEST (§4.1).
    let init = core.initialize().await.unwrap();
    assert_eq!(init["api_version"], "1.1.0");
    assert_eq!(init["plugin_version"], "9.9.9");
    assert_eq!(
        init["capabilities"],
        json!({"media.download": true, "chat.get": {}, "send.album": {"max": 10}, "reactions.set": true, "edit": true})
    );
    assert_eq!(core.initialize().await.unwrap_err().code, -32600);
    assert_eq!(core.call("ping", json!({})).await.unwrap(), json!({}));
    let id = core.request("ping", Value::Null).await;
    assert_eq!(core.response(id).await.unwrap(), json!({}));

    // account.start: `{}` first, then status connecting (SDK), the acked bulk event, connected.
    core.start_with(start_params("acc")).await.unwrap();
    let first = core.next_frame(Duration::from_secs(5)).await.unwrap();
    assert_eq!(
        (first["method"].as_str(), first["params"]["state"].as_str()),
        (Some("status"), Some("connecting"))
    );
    let bulk = core.event_frame("contacts.upsert").await;
    assert!(bulk.get("id").is_some(), "sent as a request: {bulk}");
    assert_eq!(core.event("status").await["state"], "connected");
    let observed = core.call("chats.get", json!({"account_id": "acc", "chat_id": "c", "future": true}))
        .await.unwrap();
    assert_eq!(observed, json!({"conversation": {"id": "c", "kind": "dm", "pinned": false}}));
    // A second start: INVALID_PARAMS naming account_id; the account keeps running (§4.2).
    let e = core.start_with(start_params("acc")).await.unwrap_err();
    assert_eq!(
        (e.code, &e.data.as_ref().unwrap().field),
        (-32602, &Some("account_id".to_owned()))
    );

    // A method call that makes a plugin -> core request on the way.
    let id = core
        .request(
            "messages.send",
            json!({"account_id": "acc", "chat_id": "c1", "text": "hi", "x-future-field": 1}),
        )
        .await;
    let sreq = core.plugin_request("secrets.set").await;
    assert_eq!(
        sreq["params"],
        json!({"account_id": "acc", "key": "token", "value": "t0k"})
    );
    core.answer(&sreq, Ok(json!({}))).await;
    let r = core.response(id).await.unwrap();
    assert_eq!(r["message"]["text"], "hi");
    assert!(
        r["message"].get("edited").is_none(),
        "merge fields are not defaulted on the wire (spec §8.4)"
    );
    assert_eq!(
        core.event("messages.upsert").await["messages"][0]["chat_id"],
        "c1"
    );

    // An error with an unknown code from the core reaches the caller intact, and is
    // re-emitted with INTERNAL's number (§13).
    for reemit in [false, true] {
        let id = core
            .request(
                "x-test.secret",
                json!({"account_id": "acc", "reemit": reemit}),
            )
            .await;
        let sreq = core.plugin_request("secrets.set").await;
        let unknown: ErrorObject = serde_json::from_value(
            json!({"code": -32010, "message": "new", "data": {"code": "SOMETHING_NEW"}}),
        )
        .unwrap();
        core.answer(&sreq, Err(unknown)).await;
        match core.response(id).await {
            Ok(r) => {
                assert!(!reemit);
                assert_eq!(
                    r,
                    json!({"numeric": -32010, "code": "SOMETHING_NEW", "message": "new"})
                );
            }
            Err(e) => {
                assert!(reemit);
                assert_eq!(
                    (e.code, e.data.as_ref().unwrap().code.to_string()),
                    (-32603, "SOMETHING_NEW".to_owned())
                );
                assert_eq!(e.code(), ErrorCode::Internal);
            }
        }
    }

    // Capability-gated parameters from the registry (§14.2).
    let send = |extra: Value| {
        let mut p = json!({"account_id": "acc", "chat_id": "c2", "text": "x"});
        p.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        p
    };
    let gated = [
        ("messages.send", send(json!({"silent": null})), None),
        (
            "messages.send",
            send(json!({"silent": true})),
            Some("send.silent"),
        ),
        (
            "messages.send",
            send(json!({"reply_to": {"message_id": "m1", "quote_text": "q"}})),
            Some("quote_reply"),
        ),
        (
            "reactions.set",
            json!({"account_id": "acc", "chat_id": "c", "message_id": "m",
                   "reactions": [{"emoji": "👍"}, {"custom_emoji_id": "e1"}]}),
            Some("reactions.custom"),
        ),
        (
            "reactions.set",
            json!({"account_id": "acc", "chat_id": "c", "message_id": "m",
                   "reactions": [{"emoji": "👍"}]}),
            None,
        ),
        (
            "messages.edit",
            json!({"account_id": "acc", "chat_id": "c", "message_id": "m", "text": "x",
                   "entities": [{"type": "bold", "offset": 0, "length": 1}]}),
            Some("send.entities"),
        ),
    ];
    for (method, params, missing) in gated {
        let r = core.call(method, params.clone()).await;
        match missing {
            None => assert!(r.is_ok(), "{method} {params}: {r:?}"),
            Some(cap) => {
                let e = r.unwrap_err();
                assert_eq!(
                    (e.code, e.data.as_ref().unwrap().capability.as_deref()),
                    (-32001, Some(cap)),
                    "{method} {params}"
                );
            }
        }
    }
    let _ = core.event("messages.upsert").await;

    // Error paths.
    let e = core
        .call(
            "messages.fetch_recent",
            json!({"account_id": "acc", "chat_id": "flood", "limit": 5}),
        )
        .await
        .unwrap_err();
    assert_eq!(e.code, -32003);
    assert_eq!(
        serde_json::to_value(&e.data).unwrap(),
        json!({"code": "FLOOD_WAIT", "seconds": 37})
    );
    let e = core
        .call("history.fill_gap", json!({"account_id": "acc"}))
        .await
        .unwrap_err();
    assert_eq!(e.code, -32001);
    let e = core.call("no.such_method", json!({})).await.unwrap_err();
    assert_eq!(e.code(), ErrorCode::MethodNotFound);
    let e = core
        .call(
            "messages.send",
            json!({"account_id": "other", "chat_id": "c", "text": "x"}),
        )
        .await
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::UnknownAccount);
    let e = core
        .call(
            "messages.send",
            json!({"account_id": "acc", "chat_id": "c"}),
        )
        .await
        .unwrap_err();
    assert_eq!(e.code, -32602);
    let e = core
        .call("messages.send", json!({"chat_id": "c", "text": "x"}))
        .await
        .unwrap_err();
    assert_eq!(e.code, -32602);
    let e = core
        .call("secrets.set", json!({"account_id": "acc"}))
        .await
        .unwrap_err();
    assert_eq!(e.code, -32601);

    // Cancellation of a transfer: progress carries the request id, then CANCELLED.
    let id = core
        .request(
            "media.download",
            json!({"account_id": "acc", "chat_id": "c1", "message_id": "m1", "what": "full"}),
        )
        .await;
    let p = core.event("transfer.progress").await;
    assert_eq!(
        (p["request_id"].as_i64(), p["done_bytes"].as_u64()),
        (Some(id), Some(10))
    );
    core.cancel(id).await;
    core.cancel(999).await; // ignored
    let e = core.response(id).await.unwrap_err();
    assert_eq!(e.code(), ErrorCode::Cancelled);
    assert!(flags.download_cleaned_up.load(Ordering::SeqCst));

    // account.stop: status(disconnected), then `{}`, then silence for the account (§4.2).
    let id = core
        .request("account.stop", json!({"account_id": "acc"}))
        .await;
    let mut disconnected = false;
    loop {
        let v = core
            .next_frame(Duration::from_secs(5))
            .await
            .expect("stop response");
        if v["id"] == id && v.get("method").is_none() {
            assert_eq!(v["result"], json!({}));
            break;
        }
        assert!(!disconnected, "frame after status(disconnected): {v}");
        disconnected = v["method"] == "status" && v["params"]["state"] == "disconnected";
    }
    assert!(disconnected, "status(disconnected) right before the reply");
    if let Some(v) = core.next_frame(Duration::from_millis(150)).await {
        panic!("frame after account.stop: {v}");
    }
    assert!(flags.spammer_stopped.load(Ordering::SeqCst));
    let e = core
        .call(
            "messages.fetch_recent",
            json!({"account_id": "acc", "chat_id": "c", "limit": 1}),
        )
        .await
        .unwrap_err();
    assert_eq!(e.code, -32008);
    assert_eq!(core.stop("acc").await.unwrap_err().code, -32008);

    // Restart the account, then shutdown stops it and exits cleanly.
    core.start_with(start_params("acc")).await.unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn events_from_on_start_follow_the_reply_and_updates_carry_only_set_fields() {
    let plugin = PluginBuilder::new()
        .on_account_start(|acct: Account| async move {
            let chat = Conversation::new("c1", ConversationKind::Group).with_unread_count(2);
            acct.conversations_upsert(vec![chat])?;
            Ok(())
        })
        .method("messages.fetch_recent", ok_value)
        .method("messages.send", ok_value)
        .build()
        .unwrap();
    let mut core = FakeCore::new(plugin);
    core.initialize().await.unwrap();
    let id = core
        .request(
            "account.start",
            json!({"account_id": "a", "settings": {}, "secrets": {}, "state_dir": "/s",
                   "cache_dir": "/c", "ghost": false, "auto_download":
                   {"photos": false, "videos_max_bytes": 0, "files_max_bytes": 0, "voice": false}}),
        )
        .await;
    let mut order = vec![];
    for _ in 0..3 {
        let v = core.next_frame(Duration::from_secs(5)).await.unwrap();
        order.push(v);
    }
    assert_eq!(order[0]["id"], id, "the reply comes first: {order:?}");
    assert_eq!(
        (
            order[1]["method"].as_str(),
            order[1]["params"]["state"].as_str()
        ),
        (Some("status"), Some("connecting"))
    );
    assert_eq!(order[2]["method"], "conversations.upsert");
    assert_eq!(
        order[2]["params"]["conversations"][0],
        json!({"id": "c1", "kind": "group", "unread_count": 2})
    );
    core.shutdown().await;
}

#[tokio::test]
async fn settings_defaults_come_from_plugin_json_and_any_major_is_answered() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = json!({"settings_schema": {"type": "object", "properties": {
        "proxy": {"type": "string", "default": ""}, "port": {"type": "integer", "default": 1}}}});
    std::fs::write(dir.path().join("plugin.json"), manifest.to_string()).unwrap();
    let plugin = PluginBuilder::new()
        .on_account_start(|acct: Account| async move {
            assert_eq!(acct.settings().get::<String>("proxy")?, Some(String::new()));
            assert_eq!(acct.settings().get::<u16>("port")?, Some(2));
            acct.connected(None)?;
            Ok(())
        })
        .method("messages.fetch_recent", ok_value)
        .method("messages.send", ok_value)
        .build()
        .unwrap();
    // No `.plugin_dir()`: the fake core fills nothing, so the defaults are the SDK's own.
    let mut core = FakeCore::new(plugin);
    let init = core
        .call(
            "initialize",
            json!({"api_version": "2.0.0", "core_version": "t", "plugin_dir": dir.path(),
                   "locale": "en-US", "log_level": "debug"}),
        )
        .await
        .unwrap();
    assert_eq!(
        (init["api_version"].as_str(), &init["capabilities"]),
        (Some("1.1.0"), &json!({}))
    );
    core.start("a", json!({"port": 2})).await.unwrap();
    assert_eq!(core.event("status").await["state"], "connected");
    core.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn hung_handler_is_answered_at_the_registry_timeout() {
    let mut core = FakeCore::new(test_plugin(Arc::default()).build().unwrap())
        .timeout(Duration::from_secs(3600)); // virtual time
    core.initialize().await.unwrap();
    let started = tokio::time::Instant::now();
    let e = core.call("x-test.hang", json!({})).await.unwrap_err();
    assert_eq!(e.code(), ErrorCode::Timeout);
    let waited = started.elapsed().as_secs();
    assert!((60..=63).contains(&waited), "answered after {waited} s");
    core.wait_exit().await.unwrap(); // EOF: run() returns
}

#[tokio::test(start_paused = true)]
async fn plugin_to_core_request_times_out_and_is_cancelled() {
    let mut core = FakeCore::new(test_plugin(Arc::default()).build().unwrap())
        .timeout(Duration::from_secs(3600));
    core.start_with(start_params("acc")).await.unwrap();
    let started = tokio::time::Instant::now();
    let id = core
        .request(
            "media.download",
            json!({"account_id": "acc", "chat_id": "secret", "message_id": "m", "what": "full"}),
        )
        .await;
    let sreq = core.plugin_request("secrets.set").await;
    let e = core.response(id).await.unwrap_err();
    assert_eq!(e.code(), ErrorCode::Timeout);
    let waited = started.elapsed().as_secs();
    assert_eq!(
        u64::from(ch_plugin_sdk::ch_protocol::DEFAULT_TIMEOUT_SECONDS),
        waited
    );
    let cancel = core.event_frame("$/cancelRequest").await;
    assert_eq!(cancel["params"]["id"], sreq["id"]);
    core.wait_exit().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn eof_answers_requests_that_finish_within_the_budget() {
    let stopped = Arc::new(AtomicBool::new(false));
    let s = stopped.clone();
    let plugin = PluginBuilder::new()
        .on_account_stop(move |_acct: Account| {
            let s = s.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                s.store(true, Ordering::SeqCst);
            }
        })
        .method("messages.fetch_recent", ok_value)
        .method("messages.send", ok_value)
        .method("x-test.slow", |_c: Call<Value>| async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok(json!({"slow": true}))
        })
        .method("x-test.hang", |_c: Call<Value>| {
            std::future::pending::<HandlerResult<Empty>>()
        })
        .build()
        .unwrap();

    // shutdown in flight when stdin closes: still answered (§2.5).
    let mut core = FakeCore::new(plugin).timeout(Duration::from_secs(3600));
    core.start("a", json!({})).await.unwrap();
    let id = core.request("shutdown", json!({})).await;
    core.close_stdin().await;
    assert_eq!(core.response(id).await.unwrap(), json!({}));
    assert!(stopped.load(Ordering::SeqCst));
    core.wait_exit().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn eof_drops_requests_that_outlast_the_budget() {
    let plugin = PluginBuilder::new()
        .method("messages.fetch_recent", ok_value)
        .method("messages.send", ok_value)
        .method("x-test.slow", |_c: Call<Value>| async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok(json!({"slow": true}))
        })
        .method("x-test.hang", |_c: Call<Value>| {
            std::future::pending::<HandlerResult<Empty>>()
        })
        .build()
        .unwrap();
    let mut core = FakeCore::new(plugin).timeout(Duration::from_secs(3600));
    core.start("a", json!({})).await.unwrap();
    let hang = core.request("x-test.hang", json!({})).await;
    let slow = core.request("x-test.slow", json!({})).await;
    core.close_stdin().await;
    let started = tokio::time::Instant::now();
    assert_eq!(core.response(slow).await.unwrap(), json!({"slow": true}));
    let mut rest = vec![];
    while let Some(v) = core.next_frame(Duration::from_secs(10)).await {
        rest.push(v);
    }
    assert!(
        rest.iter().all(|v| v["id"] != hang),
        "the hung request is dropped: {rest:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(5));
    core.wait_exit().await.unwrap();
}

#[test]
fn build_enforces_required_methods_capabilities_and_vendor_names() {
    let e = PluginBuilder::new()
        .method("messages.send", ok_value)
        .build()
        .err()
        .unwrap();
    assert!(e.to_string().contains("messages.fetch_recent"), "{e}");
    let e = PluginBuilder::new()
        .method("messages.send", ok_value)
        .method("messages.fetch_recent", ok_value)
        .capability("reactions.set")
        .method("reactions.set", ok_value)
        .build()
        .err()
        .unwrap();
    assert!(e.to_string().contains("reactions.available"), "{e}");
    let e = PluginBuilder::new()
        .method("messages.send", ok_value)
        .method("messages.fetch_recent", ok_value)
        .method("ping", ok_value)
        .build()
        .err()
        .unwrap();
    assert!(e.to_string().contains("ping"), "{e}");
    for bad in ["foo.bar", "x-Foo.bar", "x-a.bar", "x-foo.", "x-foo"] {
        let e = PluginBuilder::new()
            .method("messages.send", ok_value)
            .method("messages.fetch_recent", ok_value)
            .method(bad, ok_value)
            .build()
            .err()
            .unwrap_or_else(|| panic!("{bad} accepted"));
        assert!(e.to_string().contains("x-<plugin id>.<name>"), "{e}");
    }
    assert!(
        PluginBuilder::new()
            .method("messages.send", ok_value)
            .method("messages.fetch_recent", ok_value)
            .method("x-my_plugin2.do_thing", ok_value)
            .build()
            .is_ok()
    );
}

/// Path of the built `echo` example (cargo builds examples for `cargo test`).
fn echo_binary() -> PathBuf {
    let deps = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let bin = deps
        .parent()
        .unwrap()
        .join("examples")
        .join(format!("echo{}", std::env::consts::EXE_SUFFIX));
    if !bin.exists() {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
        let st = std::process::Command::new(cargo)
            .args(["build", "-p", "ch-plugin-sdk", "--example", "echo"])
            .status()
            .unwrap();
        assert!(st.success());
    }
    bin
}

fn spawn_echo(tmp: &Path) -> FakeCore {
    let mut cmd = std::process::Command::new(echo_binary());
    for var in [
        "CH_DATA_DIR",
        "XDG_DATA_HOME",
        "XDG_CONFIG_HOME",
        "XDG_CACHE_HOME",
        "XDG_STATE_HOME",
    ] {
        cmd.env(var, tmp.join(var));
    }
    cmd.env("CH_PLUGIN_ID", "echo")
        .env("CH_LOG_LEVEL", "debug")
        .current_dir(tmp)
        .stderr(std::process::Stdio::null());
    FakeCore::spawn(cmd).expect("spawn echo example")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn echo_example_process() {
    let tmp = tempfile::tempdir().unwrap();
    let mut core = spawn_echo(tmp.path());

    assert_eq!(core.initialize().await.unwrap()["capabilities"], json!({}));
    core.start("echo#1", json!({})).await.unwrap();
    assert_eq!(core.event("status").await["state"], "connected");
    assert_eq!(
        core.event("conversations.upsert").await["conversations"][0],
        json!({"id": "echo", "kind": "dm", "title": "Echo"})
    );
    assert_eq!(
        core.event("conversations.synced").await["chat_ids"],
        json!(["echo"])
    );

    let r = core
        .call(
            "messages.send",
            json!({"account_id": "echo#1", "chat_id": "echo", "text": "ping 🏓"}),
        )
        .await
        .unwrap();
    assert_eq!(
        (
            r["message"]["direction"].as_str(),
            r["message"]["text"].as_str()
        ),
        (Some("out"), Some("ping 🏓"))
    );
    let up = core.event("messages.upsert").await;
    assert_eq!(
        (
            up["messages"][0]["direction"].as_str(),
            up["messages"][0]["text"].as_str()
        ),
        (Some("in"), Some("ping 🏓"))
    );
    let e = core
        .call(
            "messages.send",
            json!({"account_id": "echo#1", "chat_id": "nope", "text": "x"}),
        )
        .await
        .unwrap_err();
    assert_eq!(
        (e.code, e.data.as_ref().unwrap().kind.as_deref()),
        (-32004, Some("chat"))
    );
    core.shutdown().await;

    // EOF on stdin also ends the process promptly (spec §2.5).
    let mut core = spawn_echo(tmp.path());
    core.initialize().await.unwrap();
    core.wait_exit().await.expect("exits on EOF");
}
