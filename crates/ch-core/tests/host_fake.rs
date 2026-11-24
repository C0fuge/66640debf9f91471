//! The host driving the real Python fake plugin (`plugins/fake`) through the Python SDK.
//! Ignored by default: it needs `uv` and runs `uv sync` into a temp copy (slow the first time).
//! Run with `cargo test -p ch-core --test host_fake -- --ignored`.

mod host_support;

use std::path::Path;

use ch_core::host::{AccountPhase, PluginState};
use ch_protocol::events::{AccountState, Event};
use ch_protocol::methods::PagedMessagesResult;
use host_support::*;
use serde_json::json;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs uv and a network-less uv sync; slow"]
async fn fake_plugin_under_the_host() {
    let env = Env::new();
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let src = repo.join("plugins").join("fake");
    let dst = env.plugins_dir.join("fake");
    copy_tree(&src, &dst);
    // The fake imports the SDK from a relative path in its pyproject; keep the layout.
    let sdk_src = repo.join("sdk").join("python");
    if std::fs::read_to_string(dst.join("pyproject.toml"))
        .unwrap()
        .contains("../../sdk/python")
    {
        // `../../sdk/python` from `<data>/plugins/fake` is `<data>/sdk/python`.
        copy_tree(
            &sdk_src,
            &env.plugins_dir.parent().unwrap().join("sdk").join("python"),
        );
    }
    let st = std::process::Command::new("uv")
        .args(["sync", "--quiet"])
        .current_dir(&dst)
        .env("UV_CACHE_DIR", env.tmp.path().join("uv-cache"))
        .status()
        .expect("uv is installed");
    assert!(st.success(), "uv sync failed");

    let mut config = env.config();
    // Python start-up is slower than the test divisor allows for; give the handshake room.
    config.timeout_scale = 0.2;
    let sink = RecSink::new();
    let h = ch_core::host::HostHandle::new(
        config,
        sink.clone(),
        std::sync::Arc::new(ch_core::host::MemorySecretStore::new()),
    );
    assert_eq!(
        h.plugin_state("fake").unwrap(),
        PluginState::Stopped,
        "entry resolved after uv sync"
    );

    h.start_account(
        "fake",
        "fake#1",
        spec(json!({"seed": 3, "chats": 4, "messages_per_chat": 5, "start_state": "connected"})),
    )
    .await
    .unwrap();
    let st = h.plugin_state("fake").unwrap();
    let PluginState::Running { capabilities, .. } = &st else {
        panic!("{st}")
    };
    assert!(capabilities.has("reactions.set"));

    wait_account(&h, "fake", "fake#1", secs(15.0), |a| {
        a.state == AccountState::Connected
    })
    .await;
    sink.wait_for(secs(15.0), |evs| {
        evs.iter()
            .any(|e| matches!(e, Event::ConversationsSynced(s) if s.chat_ids.len() == 4))
    })
    .await;
    let chat_id = sink
        .events()
        .into_iter()
        .find_map(|e| match e {
            Event::ConversationsSynced(s) => Some(s.chat_ids[0].clone()),
            _ => None,
        })
        .unwrap();
    let r: PagedMessagesResult = h
        .call(
            "fake",
            "messages.fetch_recent",
            &json!({"account_id": "fake#1", "chat_id": chat_id, "limit": 3}),
        )
        .await
        .unwrap();
    assert_eq!(r.messages.len(), 3);

    let upload = env.tmp.path().join("transfer.bin");
    std::fs::write(&upload, vec![1u8; 8192]).unwrap();
    let id = std::sync::Arc::new(std::sync::OnceLock::new());
    h.call_raw_tracked(
        "fake",
        "messages.send_file",
        json!({"account_id":"fake#1","chat_id":chat_id,"path":upload}),
        None,
        Some(id.clone()),
    )
    .await
    .unwrap();
    let wire_id = ch_protocol::RequestId::Number(*id.get().unwrap());
    sink.wait_for(secs(5.0), |events| {
        events.iter().any(|e| {
            matches!(e,
        Event::TransferProgress(p) if p.done_bytes == 0 && p.request_id.as_ref() == Some(&wire_id))
        })
    })
    .await;

    h.stop_account("fake", "fake#1").await.unwrap();
    assert_eq!(
        h.account("fake", "fake#1").unwrap().phase,
        AccountPhase::Stopped
    );
    h.disable_plugin("fake").await.unwrap();
    h.start_account(
        "fake",
        "fake#1",
        spec(json!({"seed":3,"chats":4,"messages_per_chat":5,"start_state":"connected"})),
    )
    .await
    .unwrap();
    wait_account(&h, "fake", "fake#1", secs(15.0), |a| {
        a.state == AccountState::Connected
    })
    .await;
    let next_id = std::sync::Arc::new(std::sync::OnceLock::new());
    h.call_raw_tracked(
        "fake",
        "messages.fetch_recent",
        json!({"account_id":"fake#1","chat_id":chat_id,"limit":1}),
        None,
        Some(next_id.clone()),
    )
    .await
    .unwrap();
    assert!(next_id.get().unwrap() > id.get().unwrap());
    h.shutdown().await;
    assert_eq!(h.plugin_state("fake").unwrap(), PluginState::Stopped);
}
