//! The host driving the real Rust SDK `echo` example, plus discovery.

mod host_support;

use std::time::Duration;

use ch_core::host::{AccountPhase, DiscoveryProblem, HostError, PluginState};
use ch_protocol::ErrorCode;
use ch_protocol::events::{AccountState, Event};
use host_support::*;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discovery_states_and_shadowing() {
    let env = Env::new();
    env.echo("echo");
    env.plugin_dir("missing", &["./nope"], json!({}));
    env.plugin_dir("onpath", &["python3", "-c", "pass"], json!({}));
    std::fs::create_dir_all(env.plugins_dir.join("broken")).unwrap();
    std::fs::write(
        env.plugins_dir.join("broken").join("plugin.json"),
        br#"{"id": "Broken!", "name": "x"}"#,
    )
    .unwrap();
    // A built-in copy of "echo" with a different name; the user one must shadow it.
    let builtin = env.tmp.path().join("install-plugins");
    std::fs::create_dir_all(builtin.join("echo")).unwrap();
    std::fs::write(
        builtin.join("echo").join("plugin.json"),
        json!({"id": "echo", "name": "Builtin Echo", "version": "0.0.1", "api_version": "1.0.0",
               "entry": ["./nope"], "brand_color": "#000000", "multi_account": false, "auth_methods": ["none"]})
        .to_string(),
    )
    .unwrap();
    std::fs::create_dir_all(builtin.join("only_builtin")).unwrap();
    std::fs::write(
        builtin.join("only_builtin").join("plugin.json"),
        json!({"id": "only_builtin", "name": "B", "version": "0.0.1", "api_version": "1.0.0",
               "entry": ["./nope"], "brand_color": "#000000", "multi_account": false, "auth_methods": ["none"]})
        .to_string(),
    )
    .unwrap();

    let mut config = env.config();
    config.builtin_plugins_dir = Some(builtin);
    let h = ch_core::host::HostHandle::new(
        config,
        std::sync::Arc::new(ch_core::host::NullSink),
        std::sync::Arc::new(ch_core::host::MemorySecretStore::new()),
    );
    let plugins = h.plugins();
    let ids: Vec<_> = plugins.iter().map(|p| p.info.id.as_str()).collect();
    assert_eq!(ids, ["broken", "echo", "missing", "only_builtin", "onpath"]);

    let echo = h.plugin("echo").unwrap();
    assert_eq!(
        echo.info.manifest.as_ref().unwrap().name,
        "echo",
        "user plugin shadows built-in"
    );
    assert!(!echo.info.builtin);
    assert_eq!(echo.state, PluginState::Stopped);

    let only_builtin = h.plugin("only_builtin").unwrap();
    assert!(only_builtin.info.builtin);

    match h.plugin_state("missing").unwrap() {
        PluginState::Unavailable(DiscoveryProblem::NotInstalled { path }) => {
            assert!(path.ends_with("missing/nope"), "{path:?}");
        }
        other => panic!("{other}"),
    }
    assert!(matches!(
        h.plugin_state("broken").unwrap(),
        PluginState::Unavailable(DiscoveryProblem::InvalidManifest(_))
    ));
    assert_eq!(h.plugin_state("onpath").unwrap(), PluginState::Stopped);
    assert!(
        h.plugin("onpath")
            .unwrap()
            .info
            .executable
            .unwrap()
            .ends_with("python3")
    );

    // Not installed: the host refuses to start it and never runs anything.
    let e = h
        .start_account("missing", "m#1", spec(json!({})))
        .await
        .unwrap_err();
    assert!(matches!(e, HostError::PluginNotRunning { .. }), "{e}");
    let e = h
        .start_account("nope", "x", spec(json!({})))
        .await
        .unwrap_err();
    assert!(matches!(e, HostError::UnknownPlugin(_)), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn echo_end_to_end() {
    let env = Env::new();
    env.echo("echo");
    let (h, sink, _secrets) = host(&env);
    let mut changes = h.subscribe();

    h.start_account("echo", "echo#1", spec(json!({})))
        .await
        .unwrap();
    let st = h.plugin_state("echo").unwrap();
    let PluginState::Running {
        api_version,
        capabilities,
        ..
    } = &st
    else {
        panic!("{st}")
    };
    assert_eq!(api_version, "1.0.0");
    assert!(capabilities.0.is_empty());

    let acc = h.account("echo", "echo#1").unwrap();
    assert_eq!(acc.phase, AccountPhase::Running);
    assert!(acc.wanted);
    assert!(env.data_dir.join("accounts/echo#1/state").is_dir());
    assert!(env.data_dir.join("accounts/echo#1/cache").is_dir());

    sink.wait_for(secs(5.0), |evs| evs.len() >= 3).await;
    assert_eq!(
        sink.names(),
        ["status", "conversations.upsert", "conversations.synced"]
    );
    let acc = wait_account(&h, "echo", "echo#1", secs(2.0), |a| {
        a.state == AccountState::Connected
    })
    .await;
    assert_eq!(acc.state, AccountState::Connected);

    // Gated parameter without the capability: refused locally, never sent.
    let e = h
        .call_raw(
            "echo",
            "messages.send",
            json!({"account_id": "echo#1", "chat_id": "echo", "text": "x", "silent": true}),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(e.error_code(), Some(ErrorCode::NotSupported));
    if let HostError::Rpc(err) = &e {
        assert_eq!(
            err.data.as_ref().unwrap().capability.as_deref(),
            Some("send.silent")
        );
    }
    // A gated method the plugin lacks: same, locally.
    let e = h
        .call_raw(
            "echo",
            "reactions.set",
            json!({"account_id": "echo#1", "chat_id": "echo", "message_id": "1", "reactions": []}),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(e.error_code(), Some(ErrorCode::NotSupported));
    // Lifecycle and plugin→core methods are not callable.
    assert!(matches!(
        h.call_raw("echo", "shutdown", json!({}), None)
            .await
            .unwrap_err(),
        HostError::InvalidMethod(..)
    ));
    assert!(matches!(
        h.call_raw("echo", "secrets.set", json!({}), None)
            .await
            .unwrap_err(),
        HostError::InvalidMethod(..)
    ));
    assert!(matches!(
        h.call_raw("echo", "messages.frobnicate", json!({}), None)
            .await
            .unwrap_err(),
        HostError::InvalidMethod(..)
    ));

    // A real call.
    let r: ch_protocol::methods::MessageResult = h
        .call(
            "echo",
            "messages.send",
            &json!({"account_id": "echo#1", "chat_id": "echo", "text": "ping 🏓"}),
        )
        .await
        .unwrap();
    assert_eq!(r.message.text.as_deref(), Some("ping 🏓"));
    sink.wait_for(secs(5.0), |evs| {
        evs.iter().any(|e| matches!(e, Event::MessagesUpsert(u) if u.messages[0].text.as_deref() == Some("ping 🏓")))
    })
    .await;
    // Exactly one upsert: the refused call never reached the plugin.
    assert_eq!(
        sink.names()
            .iter()
            .filter(|n| *n == "messages.upsert")
            .count(),
        1
    );

    // An error from the plugin is passed through.
    let e = h
        .call_raw(
            "echo",
            "messages.send",
            json!({"account_id": "echo#1", "chat_id": "nope", "text": "x"}),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(e.error_code(), Some(ErrorCode::NotFound));

    // Second start without a stop: refused by the host.
    let e = h
        .start_account("echo", "echo#1", spec(json!({})))
        .await
        .unwrap_err();
    assert!(matches!(e, HostError::AccountAlreadyStarted(_)), "{e}");

    // Stop: marked disconnected, no longer wanted.
    h.stop_account("echo", "echo#1").await.unwrap();
    let acc = h.account("echo", "echo#1").unwrap();
    assert_eq!(
        (acc.phase, acc.state, acc.wanted),
        (AccountPhase::Stopped, AccountState::Disconnected, false)
    );
    // Stopping again is fine (already stopped).
    h.stop_account("echo", "echo#1").await.unwrap();
    assert!(matches!(
        h.stop_account("echo", "ghost").await.unwrap_err(),
        HostError::UnknownAccount(_)
    ));

    // Start again on the same process works.
    h.start_account("echo", "echo#1", spec(json!({})))
        .await
        .unwrap();
    wait_account(&h, "echo", "echo#1", secs(2.0), |a| {
        a.state == AccountState::Connected
    })
    .await;

    // Graceful shutdown.
    let t = std::time::Instant::now();
    h.disable_plugin("echo").await.unwrap();
    assert!(
        t.elapsed() < Duration::from_secs(3),
        "shutdown took {:?}",
        t.elapsed()
    );
    assert_eq!(h.plugin_state("echo").unwrap(), PluginState::Stopped);
    let acc = h.account("echo", "echo#1").unwrap();
    assert_eq!((acc.phase, acc.wanted), (AccountPhase::Stopped, false));

    // State changes were broadcast, including the Running transition.
    let mut saw_running = false;
    while let Ok(c) = changes.try_recv() {
        if let ch_core::host::StateChange::Plugin {
            state: PluginState::Running { .. },
            ..
        } = c
        {
            saw_running = true;
        }
    }
    assert!(saw_running);

    // Calls on a stopped plugin fail fast.
    let e = h
        .call_raw("echo", "messages.send", json!({}), None)
        .await
        .unwrap_err();
    assert!(matches!(e, HostError::PluginNotRunning { .. }), "{e}");

    // enable_plugin brings it back without accounts; shutdown() stops everything.
    h.enable_plugin("echo").await.unwrap();
    assert!(h.plugin_state("echo").unwrap().is_running());
    h.shutdown().await;
    assert_eq!(h.plugin_state("echo").unwrap(), PluginState::Stopped);
}
