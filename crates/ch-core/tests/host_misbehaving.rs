//! Supervision policy against purpose-built misbehaving plugins (`fixtures/misbehaving.py`):
//! handshake details, version mismatch, timeouts and cancel, crash → restart → resume, crash
//! loop, protocol errors, hangs, floods against a slow sink, shutdown ordering and escalation.

mod host_support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use ch_core::host::{
    AccountPhase, HostError, HostHandle, MemorySecretStore, PluginState, StateChange,
};
use ch_protocol::ErrorCode;
use ch_protocol::events::{AccountState, Event};
use ch_protocol::methods::PagedMessagesResult;
use host_support::*;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

async fn fetch(
    h: &HostHandle,
    plugin: &str,
    account: &str,
    chat: &str,
    extra: Value,
) -> Result<PagedMessagesResult, HostError> {
    let mut params = json!({"account_id": account, "chat_id": chat, "limit": 10});
    for (k, v) in extra.as_object().into_iter().flatten() {
        params[k] = v.clone();
    }
    h.call(plugin, "messages.fetch_recent", &params).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handshake_settings_defaults_secrets_and_single_start() {
    let env = Env::new();
    env.misbehaving(
        "good",
        "good",
        json!({"settings_schema": {"type": "object", "properties": {
            "proxy": {"type": "string", "default": ""},
            "seed": {"type": "integer", "default": 1},
            "token": {"type": "string", "x-secret": true}
        }}}),
    );
    let (h, sink, secrets) = host(&env);

    h.start_account("good", "g#1", spec(json!({"seed": 7})))
        .await
        .unwrap();
    let st = h.plugin_state("good").unwrap();
    let PluginState::Running {
        plugin_version,
        capabilities,
        ..
    } = &st
    else {
        panic!("{st}")
    };
    assert_eq!(plugin_version.as_deref(), Some("0.0.1"));
    assert!(capabilities.has("send.silent"));

    let s = sink
        .wait_status("g#1", secs(5.0), |s| s.state == AccountState::Connected)
        .await;
    let d = detail_json(&s);
    assert_eq!(d["start"], json!(1));
    assert_eq!(
        d["settings"],
        json!({"seed": 7, "proxy": ""}),
        "defaults filled, no secret default"
    );
    assert_eq!(d["secrets"], json!({}));
    assert!(
        d["state_dir"]
            .as_str()
            .unwrap()
            .ends_with("accounts/g#1/state")
    );

    // The plugin stored a runtime secret through secrets.set.
    let t = Instant::now();
    while secrets.snapshot("good", "g#1").is_empty() {
        assert!(t.elapsed() < secs(3.0), "secrets.set never arrived");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        secrets.snapshot("good", "g#1")["session"],
        "session-for-g#1"
    );

    // A second start without a stop is refused by the host; the plugin saw one start.
    let e = h
        .start_account("good", "g#1", spec(json!({})))
        .await
        .unwrap_err();
    assert!(matches!(e, HostError::AccountAlreadyStarted(_)));

    // The gated param is allowed now that the plugin declares send.silent.
    let r: ch_protocol::methods::MessageResult = h
        .call(
            "good",
            "messages.send",
            &json!({"account_id": "g#1", "chat_id": "cc", "text": "hi", "silent": true}),
        )
        .await
        .unwrap();
    assert_eq!(r.message.text.as_deref(), Some("silent:hi"));

    // Stop, then start again: the secret comes back, and it is the plugin's second start.
    h.stop_account("good", "g#1").await.unwrap();
    assert_eq!(
        h.account("good", "g#1").unwrap().state,
        AccountState::Disconnected
    );
    h.start_account("good", "g#1", spec(json!({"seed": 8})))
        .await
        .unwrap();
    let s = sink
        .wait_status("g#1", secs(5.0), |s| {
            s.state == AccountState::Connected && detail_json(s)["start"] == json!(2)
        })
        .await;
    let d = detail_json(&s);
    assert_eq!(d["secrets"], json!({"session": "session-for-g#1"}));
    assert_eq!(d["settings"]["seed"], json!(8));

    // Malformed frames from the plugin get INVALID_REQUEST and the stream continues.
    let r = fetch(&h, "good", "g#1", "malformed", json!({}))
        .await
        .unwrap();
    assert_eq!(r.next_cursor.as_deref(), Some("malformed=INVALID_REQUEST"));
    assert!(h.plugin_state("good").unwrap().is_running());

    // An event for an account that is not started is refused and never reaches the sink.
    let r = fetch(&h, "good", "g#1", "stray", json!({})).await.unwrap();
    assert_eq!(r.next_cursor.as_deref(), Some("stray=UNKNOWN_ACCOUNT"));
    assert!(
        !sink
            .events()
            .iter()
            .any(|e| matches!(e, Event::Status(s) if s.account_id == "nobody"))
    );

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_account_plugin() {
    let env = Env::new();
    env.misbehaving("one", "good", json!({"multi_account": false}));
    let (h, _sink, _s) = host(&env);
    h.start_account("one", "a", spec(json!({}))).await.unwrap();
    let e = h
        .start_account("one", "b", spec(json!({})))
        .await
        .unwrap_err();
    assert!(matches!(e, HostError::SingleAccount(_)), "{e}");
    h.stop_account("one", "a").await.unwrap();
    h.start_account("one", "b", spec(json!({}))).await.unwrap();
    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn version_mismatch_is_incompatible() {
    let env = Env::new();
    env.misbehaving("old", "old_api", json!({}));
    let (h, _sink, _s) = host(&env);
    let e = h
        .start_account("old", "o#1", spec(json!({})))
        .await
        .unwrap_err();
    assert!(
        matches!(&e, HostError::PluginNotRunning { state: PluginState::Incompatible { api_version }, .. } if api_version == "0.9.0"),
        "{e}"
    );
    wait_plugin_state(&h, "old", secs(3.0), |s| {
        matches!(s, PluginState::Incompatible { .. })
    })
    .await;
    // It stays there (no restart loop) and can be shut down.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(matches!(
        h.plugin_state("old").unwrap(),
        PluginState::Incompatible { .. }
    ));
    h.disable_plugin("old").await.unwrap();
    assert_eq!(h.plugin_state("old").unwrap(), PluginState::Stopped);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeouts_cancel_and_progress_reset() {
    let env = Env::new();
    env.misbehaving("tt", "good", json!({}));
    let (h, _sink, _s) = host(&env);
    h.start_account("tt", "t#1", spec(json!({}))).await.unwrap();
    let timeout = h.config().request_timeout("messages.fetch_recent");

    // "crash" would exit the fixture process if it received this frame.
    let revoked = CancellationToken::new();
    revoked.cancel();
    let wire_id = Arc::new(std::sync::OnceLock::new());
    let err = h.call_raw_tracked("tt", "messages.fetch_recent",
        json!({"account_id":"t#1","chat_id":"crash","limit":1}),
        Some(revoked), Some(wire_id.clone())).await.unwrap_err();
    assert_eq!(err.error_code(), Some(ErrorCode::Cancelled));
    assert!(wire_id.get().is_none());
    assert!(h.plugin_state("tt").unwrap().is_running());

    // Never answered: TIMEOUT at the registry timeout, and the plugin stays alive.
    let t = Instant::now();
    let e = fetch(&h, "tt", "t#1", "hang", json!({})).await.unwrap_err();
    assert_eq!(e.error_code(), Some(ErrorCode::Timeout), "{e}");
    let took = t.elapsed();
    assert!(
        took >= timeout && took < timeout * 3,
        "{took:?} vs {timeout:?}"
    );
    assert!(h.plugin_state("tt").unwrap().is_running());

    // Cancel: $/cancelRequest goes out, the plugin answers CANCELLED.
    let token = CancellationToken::new();
    let t2 = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        t2.cancel();
    });
    let t = Instant::now();
    let e: HostError = h
        .call_cancellable::<_, PagedMessagesResult>(
            "tt",
            "messages.fetch_recent",
            &json!({"account_id": "t#1", "chat_id": "slow", "limit": 1}),
            token,
        )
        .await
        .unwrap_err();
    assert_eq!(e.error_code(), Some(ErrorCode::Cancelled), "{e}");
    assert!(
        t.elapsed() < timeout,
        "cancel should beat the timeout: {:?}",
        t.elapsed()
    );

    // Progress: 3 steps each shorter than the timeout, total longer; the reset keeps it alive.
    let step_ms = (timeout.as_millis() as u64) / 2;
    let r = fetch(
        &h,
        "tt",
        "t#1",
        "progress",
        json!({"steps": 3, "step_ms": step_ms}),
    )
    .await
    .unwrap();
    assert_eq!(r.next_cursor.as_deref(), Some("progress=3"));

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_restart_resumes_accounts() {
    let env = Env::new();
    env.misbehaving("cc", "good", json!({}));
    let (h, sink, _s) = host(&env);
    let mut changes = h.subscribe();
    h.start_account("cc", "c#1", spec(json!({"seed": 1})))
        .await
        .unwrap();
    h.start_account("cc", "c#2", spec(json!({"seed": 2})))
        .await
        .unwrap();
    // c#3 is started and stopped: it must not be resumed.
    h.start_account("cc", "c#3", spec(json!({}))).await.unwrap();
    h.stop_account("cc", "c#3").await.unwrap();
    sink.wait_status("c#2", secs(5.0), |s| s.state == AccountState::Connected)
        .await;

    // Another request is in flight while the plugin dies: it is answered INTERNAL.
    let h2 = h.clone();
    let hanging = tokio::spawn(async move { fetch(&h2, "cc", "c#1", "hang", json!({})).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let e = fetch(&h, "cc", "c#1", "crash", json!({}))
        .await
        .unwrap_err();
    assert_eq!(e.error_code(), Some(ErrorCode::Internal), "{e}");
    let e = hanging.await.unwrap().unwrap_err();
    assert_eq!(e.error_code(), Some(ErrorCode::Internal), "{e}");

    // Restarting → Running, and both wanted accounts come back on the new process.
    let mut saw_restarting = false;
    let deadline = Instant::now() + secs(5.0);
    while !saw_restarting {
        assert!(Instant::now() < deadline);
        if let Ok(StateChange::Plugin {
            state: PluginState::Restarting {
                failures, reason, ..
            },
            ..
        }) = tokio::time::timeout(secs(1.0), changes.recv())
            .await
            .unwrap()
        {
            assert_eq!(failures, 1);
            assert!(reason.contains("exited"), "{reason}");
            saw_restarting = true;
        }
    }
    wait_plugin_state(&h, "cc", secs(5.0), PluginState::is_running).await;
    for acc in ["c#1", "c#2"] {
        let a = wait_account(&h, "cc", acc, secs(5.0), |a| {
            a.phase == AccountPhase::Running && a.state == AccountState::Connected
        })
        .await;
        assert!(a.wanted);
    }
    // The new process counts starts from 1 again, and got the original settings.
    let connected: Vec<_> = sink
        .events()
        .into_iter()
        .filter_map(|e| match e {
            Event::Status(s) if s.account_id == "c#2" && s.state == AccountState::Connected => {
                Some(detail_json(&s))
            }
            _ => None,
        })
        .collect();
    assert_eq!(connected.len(), 2, "{connected:?}");
    assert_eq!(connected[1]["start"], json!(1));
    assert_eq!(connected[1]["settings"]["seed"], json!(2));
    let c3 = h.account("cc", "c#3").unwrap();
    assert_eq!((c3.phase, c3.wanted), (AccountPhase::Stopped, false));

    // Requests work again.
    fetch(&h, "cc", "c#1", "ok", json!({})).await.unwrap();
    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_loop_gives_up_and_manual_restart() {
    let env = Env::new();
    env.misbehaving("loop", "crash_on_start", json!({}));
    let mut config = env.config();
    config.max_failures = 4;
    let h = HostHandle::new(
        config,
        Arc::new(ch_core::host::NullSink),
        Arc::new(MemorySecretStore::new()),
    );
    let mut changes = h.subscribe();

    let t = Instant::now();
    let e = h.start_account("loop", "l#1", spec(json!({}))).await;
    // Either refused outright (already Failed) or accepted during a backoff: both are fine.
    let _ = e;
    let st = wait_plugin_state(&h, "loop", secs(10.0), |s| {
        matches!(s, PluginState::Failed { .. })
    })
    .await;
    let PluginState::Failed { reason } = st else {
        unreachable!()
    };
    assert!(reason.contains("exited"), "{reason}");
    assert!(t.elapsed() < secs(5.0), "{:?}", t.elapsed());

    // Backoff grew: 1, 2, 3 restarts with doubling delays.
    let mut delays = Vec::new();
    while let Ok(c) = changes.try_recv() {
        if let StateChange::Plugin {
            state: PluginState::Restarting {
                failures, delay, ..
            },
            ..
        } = c
        {
            delays.push((failures, delay));
        }
    }
    assert_eq!(delays.iter().map(|d| d.0).collect::<Vec<_>>(), [1, 2, 3]);
    assert_eq!(delays[1].1, delays[0].1 * 2);
    assert_eq!(delays[2].1, delays[0].1 * 4);

    // Failed: no more attempts, start is refused.
    let e = h
        .start_account("loop", "l#1", spec(json!({})))
        .await
        .unwrap_err();
    assert!(
        matches!(
            e,
            HostError::PluginNotRunning {
                state: PluginState::Failed { .. },
                ..
            }
        ),
        "{e}"
    );
    if let Some(a) = h.account("loop", "l#1") {
        assert!(!a.wanted);
    }

    // "Restart" tries again from a fresh count (and fails again, 4 more times).
    h.restart_plugin("loop").await.unwrap();
    wait_plugin_state(&h, "loop", secs(10.0), |s| {
        !matches!(s, PluginState::Failed { .. })
    })
    .await;
    wait_plugin_state(&h, "loop", secs(10.0), |s| {
        matches!(s, PluginState::Failed { .. })
    })
    .await;
    h.disable_plugin("loop").await.unwrap();
    assert_eq!(h.plugin_state("loop").unwrap(), PluginState::Stopped);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn garbage_on_stdout_restarts() {
    let env = Env::new();
    env.misbehaving("gg", "garbage", json!({}));
    let (h, _sink, _s) = host(&env);
    let mut changes = h.subscribe();
    let _ = h.enable_plugin("gg").await;
    let deadline = Instant::now() + secs(5.0);
    loop {
        assert!(Instant::now() < deadline, "no restart observed");
        if let Ok(StateChange::Plugin {
            state: PluginState::Restarting { reason, .. },
            ..
        }) = tokio::time::timeout(secs(1.0), changes.recv())
            .await
            .unwrap()
        {
            assert!(
                reason.contains("protocol error") && reason.contains("not JSON"),
                "{reason}"
            );
            break;
        }
    }
    h.disable_plugin("gg").await.unwrap();
    assert_eq!(h.plugin_state("gg").unwrap(), PluginState::Stopped);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hung_plugin_is_restarted() {
    let env = Env::new();
    env.misbehaving("hh", "hang", json!({}));
    let (h, _sink, _s) = host(&env);
    let mut changes = h.subscribe();
    h.start_account("hh", "h#1", spec(json!({}))).await.unwrap();
    let t = Instant::now();
    let deadline = t + secs(10.0);
    loop {
        assert!(Instant::now() < deadline, "hang not detected");
        if let Ok(StateChange::Plugin {
            state: PluginState::Restarting { reason, .. },
            ..
        }) = tokio::time::timeout(secs(5.0), changes.recv())
            .await
            .unwrap()
        {
            assert!(
                reason.contains("hung") && reason.contains("3 pings"),
                "{reason}"
            );
            break;
        }
    }
    // 3 misses × (interval + ping timeout), roughly.
    let expected = (h.config().ping_interval + h.config().request_timeout("ping")) * 3;
    assert!(
        t.elapsed() < expected * 3,
        "{:?} vs {expected:?}",
        t.elapsed()
    );
    assert_eq!(h.account("hh", "h#1").unwrap().phase, AccountPhase::Stopped);
    assert!(
        h.account("hh", "h#1").unwrap().wanted,
        "resumed after the restart"
    );
    h.disable_plugin("hh").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flood_with_slow_sink_stays_bounded_and_ordered() {
    let env = Env::new();
    env.misbehaving("ff", "good", json!({}));
    let mut config = env.config();
    config.event_queue = 16;
    // Keep the fast ping period, but budget RPCs separately from the unscaled 4000-frame
    // workload and forced sink pause. DIV=50 otherwise leaves only 400 ms to drain it.
    config.timeout_scale = 0.1;
    let sink = RecSink::new();
    let h = HostHandle::new(config, sink.clone(), Arc::new(MemorySecretStore::new()));
    let mut changes = h.subscribe();
    h.start_account("ff", "f#1", spec(json!({}))).await.unwrap();
    sink.wait_status("f#1", secs(5.0), |s| s.state == AccountState::Connected)
        .await;

    let hang_window = (h.config().ping_interval + h.config().request_timeout("ping")) * 3;
    let stall_for = hang_window + secs(0.5);
    assert!(
        h.config().request_timeout("messages.fetch_recent") >= stall_for + secs(5.0),
        "the flood RPC needs a drain budget after the intentional sink stall"
    );

    // Block the sink, then make the plugin emit 4000 notifications (~700 KB).
    sink.pause();
    let count = 4000;
    let h2 = h.clone();
    let flood =
        tokio::spawn(
            async move { fetch(&h2, "ff", "f#1", "flood", json!({"count": count})).await },
        );

    // Hold the stall for longer than the hang detection window.
    tokio::time::sleep(stall_for).await;

    // Nothing reached the sink (it is blocked on the first event)...
    assert_eq!(
        sink.count(),
        1,
        "one event is inside deliver, the rest waits"
    );
    // ...the plugin is far from done (its stdout writes block: pipe + queue is all the host holds)...
    let log = std::fs::read_to_string(env.data_dir.join("logs/plugins/ff.log")).unwrap_or_default();
    let max_sent = log
        .lines()
        .filter_map(|l| l.split("flood sent ").nth(1))
        .filter_map(|n| n.trim().parse::<u32>().ok())
        .max()
        .unwrap_or(0);
    eprintln!(
        "flood: the plugin had written {max_sent} of {count} frames while the sink was blocked"
    );
    assert!(
        max_sent < 1000,
        "plugin wrote {max_sent} frames while the sink was blocked; backpressure failed"
    );
    // ...and the plugin was not restarted for missing pings while we were the slow side.
    assert!(h.plugin_state("ff").unwrap().is_running());
    while let Ok(c) = changes.try_recv() {
        assert!(
            !matches!(
                c,
                StateChange::Plugin {
                    state: PluginState::Restarting { .. },
                    ..
                }
            ),
            "{c:?}"
        );
    }

    sink.resume();
    let r = tokio::time::timeout(secs(20.0), flood)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(r.next_cursor.as_deref(), Some(&*format!("sent={count}")));
    sink.wait_for(secs(20.0), |evs| {
        evs.iter()
            .filter(|e| matches!(e, Event::MessagesUpsert(_)))
            .count()
            == count as usize
    })
    .await;
    let ids: Vec<u32> = sink
        .events()
        .into_iter()
        .filter_map(|e| match e {
            Event::MessagesUpsert(u) => u.messages[0].id.parse().ok(),
            _ => None,
        })
        .collect();
    assert!(
        ids.iter().enumerate().all(|(i, id)| *id as usize == i),
        "events out of order"
    );
    assert!(h.plugin_state("ff").unwrap().is_running());

    // Acked events (sent as requests) are answered after ingestion, in order.
    let r = fetch(
        &h,
        "ff",
        "f#1",
        "flood",
        json!({"count": 50, "acked": true}),
    )
    .await
    .unwrap();
    assert_eq!(r.next_cursor.as_deref(), Some("sent=50"));
    let total = sink
        .events()
        .iter()
        .filter(|e| matches!(e, Event::MessagesUpsert(_)))
        .count();
    assert_eq!(total, count as usize + 50);
    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_waits_for_the_reply_before_closing_stdin() {
    let env = Env::new();
    let marker = env.tmp.path().join("marker");
    env.misbehaving(
        "ss",
        "slow_shutdown",
        json!({"env": {"CH_TEST_MARKER": marker.to_str().unwrap()}}),
    );
    let mut config = env.config();
    config.shutdown_grace = secs(2.0); // the plugin takes 300 ms to answer
    let h = HostHandle::new(
        config,
        Arc::new(ch_core::host::NullSink),
        Arc::new(MemorySecretStore::new()),
    );
    h.start_account("ss", "s#1", spec(json!({}))).await.unwrap();
    let t = Instant::now();
    h.disable_plugin("ss").await.unwrap();
    assert!(
        t.elapsed() >= Duration::from_millis(300),
        "{:?}",
        t.elapsed()
    );
    assert!(t.elapsed() < secs(2.0), "{:?}", t.elapsed());
    assert!(
        !marker.exists(),
        "stdin was closed before the shutdown reply was read"
    );
    assert_eq!(h.plugin_state("ss").unwrap(), PluginState::Stopped);
    let a = h.account("ss", "s#1").unwrap();
    assert_eq!(
        (a.phase, a.state, a.wanted),
        (AccountPhase::Stopped, AccountState::Disconnected, false)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_escalates_to_sigterm_then_sigkill() {
    let env = Env::new();
    env.misbehaving("ign", "ignore_shutdown", json!({}));
    env.misbehaving("imm", "immortal", json!({}));
    let (h, _sink, _s) = host(&env);
    h.enable_plugin("ign").await.unwrap();
    h.enable_plugin("imm").await.unwrap();
    let grace = h.config().shutdown_grace + h.config().exit_grace * 2;
    let t = Instant::now();
    h.shutdown().await;
    assert!(t.elapsed() < grace + secs(2.0), "{:?}", t.elapsed());
    assert_eq!(h.plugin_state("ign").unwrap(), PluginState::Stopped);
    assert_eq!(h.plugin_state("imm").unwrap(), PluginState::Stopped);
    // Both processes are gone: no python holds the plugin dir open. Check via /proc: no
    // process has our misbehaving script with these modes in its cmdline.
    let script = misbehaving_script().to_string_lossy().into_owned();
    for mode in ["ignore_shutdown", "immortal"] {
        let alive = std::fs::read_dir("/proc")
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|e| std::fs::read(e.path().join("cmdline")).ok())
            .map(|b| String::from_utf8_lossy(&b).replace('\0', " "))
            .any(|c| c.contains(&script) && c.contains(mode));
        assert!(!alive, "{mode} plugin survived shutdown");
    }
}
