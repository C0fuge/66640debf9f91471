//! Run the plugin binary as a child process over stdio, with the SDK's fake core playing
//! the core: initialize -> account.start -> messages.send -> shutdown.

use ch_plugin_sdk::testing::FakeCore;
use serde_json::json;

const PLUGIN_DIR: &str = env!("CARGO_MANIFEST_DIR");

#[tokio::test]
async fn session() {
    let tmp = std::env::temp_dir().join(format!("{{crate_name}}-test-{}", std::process::id()));
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_{{crate_name}}"));
    cmd.current_dir(PLUGIN_DIR)
        .env("CH_PLUGIN_ID", "{{plugin_id}}")
        .env("CH_LOG_LEVEL", "debug");
    for var in ["CH_DATA_DIR", "XDG_DATA_HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME"] {
        cmd.env(var, tmp.join(var.to_lowercase()));
    }
    // plugin_dir: the core sends it in `initialize`; settings defaults come from its plugin.json.
    let mut core = FakeCore::spawn(cmd).expect("spawn plugin").plugin_dir(PLUGIN_DIR);

    let res = core.initialize().await.unwrap();
    let major = |v: &str| v.split('.').next().unwrap().to_owned();
    assert_eq!(major(res["api_version"].as_str().unwrap()), major(ch_plugin_sdk::ch_protocol::API_VERSION));
    assert_eq!(res["plugin_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(core.call("ping", json!({})).await.unwrap(), json!({}));

    let acc = "{{plugin_id}}#1";
    core.start(acc, json!({})).await.unwrap(); // "server" gets its default
    let status = loop {
        let st = core.event("status").await;
        if st["state"] == "connected" {
            break st;
        }
    };
    assert_eq!(status["account_id"], acc);

    let res = core
        .call("messages.fetch_recent", json!({"account_id": acc, "chat_id": "c1", "limit": 20}))
        .await
        .unwrap();
    assert!(res["messages"].is_array());
    let res = core
        .call("messages.send", json!({"account_id": acc, "chat_id": "c1", "text": "hello"}))
        .await
        .unwrap();
    assert_eq!(res["message"]["chat_id"], "c1");
    assert_eq!(res["message"]["text"], "hello");
    assert_eq!(res["message"]["direction"], "out");

    core.shutdown().await; // answers {} and exits 0
    let _ = std::fs::remove_dir_all(&tmp);
}
