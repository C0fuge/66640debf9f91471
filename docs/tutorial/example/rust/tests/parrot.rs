//! Drive the parrot binary as a real child process, with the SDK's fake core playing the
//! core over stdin/stdout.
use ch_plugin_sdk::prelude::*;
use ch_plugin_sdk::testing::FakeCore;
use serde_json::{Value, json};

/// The fake core and the temp dir holding CH_DATA_DIR / XDG_* (never the real app data).
async fn start(settings: Value, secrets: Value) -> (FakeCore, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_parrot"));
    cmd.current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("CH_PLUGIN_ID", "parrot")
        .env("CH_DATA_DIR", tmp.path().join("data"))
        .env("XDG_DATA_HOME", tmp.path().join("xdg"));
    let mut core = FakeCore::spawn(cmd)
        .unwrap()
        .plugin_dir(env!("CARGO_MANIFEST_DIR")) // settings defaults come from plugin.json
        .answer_plugin_requests(true); // secrets.set -> {}
    let start = json!({"account_id": "a1", "settings": settings, "secrets": secrets});
    core.start_with(start).await.unwrap();
    (core, tmp)
}

#[tokio::test]
async fn sign_in() {
    let (mut core, _tmp) = start(json!({}), json!({})).await;
    assert_eq!(core.event("status").await["state"], "auth_required");
    let begin = json!({"account_id": "a1", "method": "phone_code"});
    core.call("auth.begin", begin).await.unwrap();
    let step = core.event("auth.step").await["step"].clone();
    assert_eq!(step["kind"], "phone");
    let submit = |step: &Value, value: &str| {
        json!({"account_id": "a1", "flow": "sign_in",
        "step_id": step["step_id"], "value": value})
    };
    core.call("auth.submit", submit(&step, "+15550100"))
        .await
        .unwrap();
    let step = core.event("auth.step").await["step"].clone();
    assert_eq!(step["kind"], "code");
    core.call("auth.submit", submit(&step, "0000"))
        .await
        .unwrap();
    let step = core.event("auth.step").await["step"].clone();
    assert_eq!(step["error"], "Wrong code, try again.");
    core.call("auth.submit", submit(&step, "1234"))
        .await
        .unwrap();
    let secret = core.plugin_request("secrets.set").await;
    assert_eq!(secret["params"]["key"], "session");
    assert_eq!(
        core.event("conversations.synced").await["chat_ids"],
        json!(["polly"])
    );
}

#[tokio::test]
async fn history_paging() {
    let (mut core, _tmp) = start(json!({}), json!({"session": "s"})).await;
    let (mut ids, mut cursor) = (vec![], Value::Null);
    loop {
        let mut p = json!({"account_id": "a1", "chat_id": "polly", "limit": 20});
        if !cursor.is_null() {
            p["cursor"] = cursor;
        }
        let page = core.call("messages.fetch_recent", p).await.unwrap();
        ids.extend(
            page["messages"]
                .as_array()
                .unwrap()
                .iter()
                .map(|m| m["id"].as_str().unwrap().to_owned()),
        );
        cursor = page.get("next_cursor").cloned().unwrap_or(Value::Null);
        if cursor.is_null() {
            break;
        }
    }
    assert_eq!(
        ids,
        (1..=50).rev().map(|i| i.to_string()).collect::<Vec<_>>()
    );
    let get = json!({"account_id": "a1", "chat_id": "polly", "message_id": "7"});
    let got = core.call("messages.get", get).await.unwrap();
    assert_eq!(got["message"]["text"], "Squawk #7!");
}

#[tokio::test]
async fn send_and_errors() {
    let (mut core, _tmp) = start(json!({"max_per_minute": 2}), json!({"session": "s"})).await;
    let send = |chat: &str, text: &str| json!({"account_id": "a1", "chat_id": chat, "text": text});
    let mut hi = send("polly", "hi");
    hi["silent"] = json!(true);
    let sent = core.call("messages.send", hi).await.unwrap();
    assert_eq!(sent["message"]["silent"], true);
    assert_eq!(
        core.event("messages.upsert").await["messages"][0]["text"],
        "Squawk! hi"
    );

    let code = |r: Result<Value, PluginError>| r.unwrap_err().code();
    assert_eq!(
        code(core.call("messages.send", send("nope", "x")).await),
        ErrorCode::NotFound
    );
    let mut later = send("polly", "x");
    later["schedule_at"] = json!("2030-01-01T00:00:00.000Z");
    assert_eq!(
        code(core.call("messages.send", later).await),
        ErrorCode::NotSupported
    );
    let edit = json!({"account_id": "a1", "chat_id": "polly", "message_id": "1", "text": "x"});
    assert_eq!(
        code(core.call("messages.edit", edit).await),
        ErrorCode::NotSupported
    );

    core.call("messages.send", send("polly", "two"))
        .await
        .unwrap();
    let flood = core
        .call("messages.send", send("polly", "three"))
        .await
        .unwrap_err();
    assert_eq!(flood.code(), ErrorCode::FloodWait);
    let seconds = flood.data.as_ref().unwrap().seconds.unwrap();
    assert!((1..=60).contains(&seconds));
}

#[tokio::test]
async fn shutdown() {
    let (core, _tmp) = start(json!({}), json!({"session": "s"})).await;
    core.shutdown().await; // answers {} and exits 0
}
