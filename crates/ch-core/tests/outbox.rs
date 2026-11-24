//! Outbox state machine against a scripted transport on a paused tokio clock: no plugin
//! process, no real time, every sleep is virtual.

mod common;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use ch_core::host::{AccountInfo, AccountPhase, HostError, StateChange};
use ch_core::ingest::{Ingest, IngestOptions};
use ch_core::outbox::{
    EchoSender, Outbox, OutboxChange, OutboxItem, OutboxKind, OutboxPolicy, OutboxRequest,
    OutboxState, SendTransport,
};
use ch_protocol::events::{AccountState, TransferDirection, TransferProgressEvent};
use ch_protocol::methods::{
    AlbumItem, MessagesDeleteParams, MessagesEditParams, MessagesForwardParams,
    MessagesSendAlbumParams, MessagesSendFileParams, MessagesSendParams,
};
use ch_protocol::{ErrorCode, ErrorObject};
use serde_json::{Value, json};
use tokio::sync::{broadcast, oneshot};
use tokio_util::sync::CancellationToken;

const PLUGIN: &str = "fake";
const ACC: &str = "acc";

/// One scripted answer.
enum Reply {
    Ok(Value),
    Err(HostError),
    /// Block until released; then answer with the inner reply. Cancelling the token answers
    /// `CANCELLED` unless `ignore_cancel`.
    Hold {
        release: oneshot::Receiver<()>,
        then: Box<Reply>,
        ignore_cancel: bool,
    },
}

#[derive(Debug, Clone)]
struct Call {
    plugin_id: String,
    method: String,
    params: Value,
    at: tokio::time::Instant,
}

struct Fake {
    calls: Mutex<Vec<Call>>,
    script: Mutex<VecDeque<Reply>>,
    online: Mutex<bool>,
    changes: broadcast::Sender<StateChange>,
    seq: Mutex<u64>,
}

impl Fake {
    fn new(online: bool) -> Arc<Self> {
        let (changes, _) = broadcast::channel(64);
        Arc::new(Self {
            calls: Mutex::new(vec![]),
            script: Mutex::new(VecDeque::new()),
            online: Mutex::new(online),
            changes,
            seq: Mutex::new(0),
        })
    }

    fn push(&self, r: Reply) {
        self.script.lock().unwrap().push_back(r);
    }

    fn hold(&self, then: Reply, ignore_cancel: bool) -> oneshot::Sender<()> {
        let (tx, rx) = oneshot::channel();
        self.push(Reply::Hold {
            release: rx,
            then: Box::new(then),
            ignore_cancel,
        });
        tx
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    fn client_ids(&self) -> Vec<String> {
        self.calls()
            .iter()
            .filter_map(|c| {
                c.params
                    .get("client_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .collect()
    }

    fn set_online(&self, online: bool) {
        *self.online.lock().unwrap() = online;
        let _ = self.changes.send(StateChange::Account(self.info()));
    }

    fn info(&self) -> AccountInfo {
        let online = *self.online.lock().unwrap();
        AccountInfo {
            plugin_id: PLUGIN.into(),
            account_id: ACC.into(),
            wanted: true,
            phase: if online {
                AccountPhase::Running
            } else {
                AccountPhase::Stopped
            },
            state: if online {
                AccountState::Connected
            } else {
                AccountState::Disconnected
            },
            detail: None,
            reconnecting: false,
            me: None,
        }
    }

    /// The default answer: a fresh platform message mirroring the request.
    fn default_reply(&self, method: &str, params: &Value) -> Value {
        let mut seq = self.seq.lock().unwrap();
        *seq += 1;
        let id = format!("m{}", *seq);
        let chat = params
            .get("chat_id")
            .or_else(|| params.get("to_chat_id"))
            .and_then(Value::as_str)
            .unwrap_or("c1");
        let msg = |id: &str| {
            json!({
                "id": id, "chat_id": chat, "direction": "out",
                "sent_at": "2026-01-01T10:00:00.000Z",
                "text": params.get("text").or_else(|| params.get("caption")).cloned().unwrap_or(Value::Null),
            })
        };
        match method {
            "messages.edit" => json!({"message": msg(params["message_id"].as_str().unwrap())}),
            "messages.send" | "messages.send_file" => json!({"message": msg(&id)}),
            "messages.send_album" | "messages.forward" => {
                let n = params
                    .get("items")
                    .or_else(|| params.get("message_ids"))
                    .and_then(Value::as_array)
                    .map_or(1, Vec::len);
                let msgs: Vec<Value> = (0..n).map(|i| msg(&format!("{id}-{i}"))).collect();
                json!({"messages": msgs})
            }
            _ => json!({}),
        }
    }
}

#[async_trait]
impl SendTransport for Fake {
    async fn call(
        &self,
        plugin_id: &str,
        method: &str,
        params: Value,
        cancel: CancellationToken,
        request_id: Arc<OnceLock<i64>>,
    ) -> Result<Value, HostError> {
        assert!(matches!(plugin_id, PLUGIN | "replacement"));
        {
            let mut calls = self.calls.lock().unwrap();
            request_id
                .set(i64::try_from(calls.len() + 1).unwrap())
                .unwrap();
            calls.push(Call {
                plugin_id: plugin_id.into(),
                method: method.to_owned(),
                params: params.clone(),
                at: tokio::time::Instant::now(),
            });
        }
        let reply = self.script.lock().unwrap().pop_front();
        let mut reply = match reply {
            Some(r) => r,
            None => return Ok(self.default_reply(method, &params)),
        };
        loop {
            match reply {
                Reply::Ok(v) => return Ok(v),
                Reply::Err(e) => return Err(e),
                Reply::Hold {
                    release,
                    then,
                    ignore_cancel,
                } => {
                    if ignore_cancel {
                        let _ = release.await;
                    } else {
                        tokio::select! {
                            _ = release => {}
                            _ = cancel.cancelled() => {
                                return Err(HostError::code(ErrorCode::Cancelled, "cancelled"));
                            }
                        }
                    }
                    reply = *then;
                }
            }
        }
    }

    fn account(&self, plugin_id: &str, account_id: &str) -> Option<AccountInfo> {
        (matches!(plugin_id, PLUGIN | "replacement") && account_id == ACC).then(|| AccountInfo {
            plugin_id: plugin_id.into(),
            ..self.info()
        })
    }

    fn subscribe(&self) -> broadcast::Receiver<StateChange> {
        self.changes.subscribe()
    }
}

fn rpc(code: ErrorCode) -> Reply {
    Reply::Err(HostError::code(code, "scripted"))
}

struct World {
    ingest: Arc<Mutex<Ingest>>,
    fake: Arc<Fake>,
    outbox: Outbox,
    changes: broadcast::Receiver<OutboxChange>,
}

fn policy() -> OutboxPolicy {
    OutboxPolicy {
        reconcile_timeout: Duration::from_secs(10),
        ..OutboxPolicy::default()
    }
}

fn world_with(online: bool, policy: OutboxPolicy) -> World {
    let ingest = Arc::new(Mutex::new(common::ingest_with_accounts(
        &[(ACC, PLUGIN)],
        IngestOptions::default(),
    )));
    let fake = Fake::new(online);
    let outbox = Outbox::new(Arc::clone(&ingest), fake.clone(), policy);
    let changes = outbox.subscribe();
    outbox.start().unwrap();
    World {
        ingest,
        fake,
        outbox,
        changes,
    }
}

fn world() -> World {
    world_with(true, policy())
}

impl World {
    fn text(&self, chat: &str, text: &str) -> OutboxItem {
        self.outbox
            .enqueue(
                ACC,
                OutboxRequest::SendText(MessagesSendParams {
                    chat_id: chat.into(),
                    text: text.into(),
                    ..Default::default()
                }),
                Some(EchoSender {
                    user_id: "me".into(),
                    name: Some("Me".into()),
                }),
            )
            .unwrap()
    }

    fn file(&self, chat: &str, path: &str) -> OutboxItem {
        self.outbox
            .enqueue(
                ACC,
                OutboxRequest::SendFile(MessagesSendFileParams {
                    chat_id: chat.into(),
                    path: path.into(),
                    ..Default::default()
                }),
                None,
            )
            .unwrap()
    }

    /// Poll until `pred` holds for the item, advancing virtual time; panics after 10 virtual
    /// minutes.
    async fn wait(&self, id: i64, pred: impl Fn(&OutboxItem) -> bool) -> OutboxItem {
        let start = tokio::time::Instant::now();
        loop {
            let item = self.outbox.get(id).unwrap();
            if pred(&item) {
                return item;
            }
            assert!(
                start.elapsed() < Duration::from_secs(600),
                "timed out waiting; item = {item:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn wait_state(&self, id: i64, state: OutboxState) -> OutboxItem {
        self.wait(id, |i| i.state == state).await
    }

    /// Let the workers run without letting virtual time pass.
    async fn settle(&self) {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    fn echo_row(&self, item: &OutboxItem) -> Option<ch_db::MessageRow> {
        let g = self.ingest.lock().unwrap();
        ch_db::get_message_by_id(g.db().conn(), item.echo_message_id.unwrap()).unwrap()
    }

    /// The platform's own echo of a send, carrying our client id in `ext`.
    fn platform_echo(&self, chat: &str, platform_id: &str, client_id: &str) {
        let ev = common::ev(
            "messages.upsert",
            json!({"account_id": ACC, "messages": [{
                "id": platform_id, "chat_id": chat, "direction": "out",
                "sent_at": "2026-01-01T10:00:00.000Z", "text": "hi",
                "ext": {PLUGIN: {"client_id": client_id}}
            }]}),
        );
        self.ingest.lock().unwrap().apply(PLUGIN, ev).unwrap();
    }

    fn drain_changes(&mut self) -> Vec<OutboxChange> {
        let mut out = vec![];
        while let Ok(c) = self.changes.try_recv() {
            out.push(c);
        }
        out
    }

    fn states_seen(&mut self, id: i64) -> Vec<OutboxState> {
        self.drain_changes()
            .into_iter()
            .filter_map(|c| match c {
                OutboxChange::Item(i) if i.id == id => Some(i.state),
                _ => None,
            })
            .collect()
    }
}

fn assert_each_client_id_once(fake: &Fake) {
    let mut ids = fake.client_ids();
    let n = ids.len();
    ids.sort();
    ids.dedup();
    assert_eq!(
        ids.len(),
        n,
        "a client_id was sent more than once: {:?}",
        fake.client_ids()
    );
}

#[tokio::test(start_paused = true)]
async fn happy_path_text() {
    let mut w = world();
    let item = w.text("c1", "hello");
    assert_eq!(item.state, OutboxState::Queued);
    assert_eq!(item.kind, OutboxKind::SendText);
    let echo = w.echo_row(&item).unwrap();
    assert_eq!(echo.message_id, format!("local:{}", item.client_id));
    assert_eq!(echo.delivery.as_deref(), Some("sending"));
    assert_eq!(echo.text.as_deref(), Some("hello"));
    assert_eq!(echo.sender_id.as_deref(), Some("me"));

    let sent = w.wait_state(item.id, OutboxState::Sent).await;
    assert_eq!(sent.result_message_ids, Some(vec!["m1".into()]));
    assert_eq!(sent.attempts, 1);
    let calls = w.fake.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].method, "messages.send");
    assert_eq!(calls[0].params["client_id"], item.client_id);
    assert_eq!(calls[0].params["account_id"], ACC);
    assert!(
        calls[0].params.get("silent").is_none(),
        "unset optionals are omitted"
    );
    // The echo row was re-keyed to the platform id and marked sent.
    let row = w.echo_row(&sent).unwrap();
    assert_eq!(row.message_id, "m1");
    assert_eq!(row.delivery.as_deref(), Some("sent"));
    let states = w.states_seen(item.id);
    assert_eq!(
        states,
        [OutboxState::Queued, OutboxState::Sending, OutboxState::Sent]
    );
    assert_each_client_id_once(&w.fake);
}

#[tokio::test(start_paused = true)]
async fn flood_wait_waits_exactly() {
    let w = world();
    w.fake
        .push(Reply::Err(HostError::rpc(ErrorObject::flood_wait(37))));
    let item = w.text("c1", "x");
    let sent = w.wait_state(item.id, OutboxState::Sent).await;
    assert_eq!(sent.attempts, 2);
    let calls = w.fake.calls();
    assert_eq!(calls.len(), 2);
    let gap = calls[1].at - calls[0].at;
    assert!(
        gap >= Duration::from_secs(37) && gap < Duration::from_secs(38),
        "{gap:?}"
    );
    // While waiting the item stayed queued, not failed, and the echo kept "sending".
    assert!(w.fake.client_ids().iter().all(|id| id == &item.client_id));
}

#[tokio::test(start_paused = true)]
async fn flood_wait_keeps_item_queued_with_error() {
    let w = world();
    w.fake
        .push(Reply::Err(HostError::rpc(ErrorObject::flood_wait(300))));
    let item = w.text("c1", "x");
    let waiting = w.wait(item.id, |i| i.next_attempt_at.is_some()).await;
    assert_eq!(waiting.state, OutboxState::Queued);
    assert_eq!(
        waiting.last_error.as_ref().unwrap().code(),
        ErrorCode::FloodWait
    );
    assert_eq!(
        w.echo_row(&waiting).unwrap().delivery.as_deref(),
        Some("sending")
    );
    tokio::time::sleep(Duration::from_secs(100)).await;
    assert_eq!(
        w.fake.calls().len(),
        1,
        "not retried before the wait elapses"
    );
    w.wait_state(item.id, OutboxState::Sent).await;
    assert_eq!(w.fake.calls().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn unavailable_errors_back_off_exponentially() {
    let w = world();
    for _ in 0..3 {
        w.fake.push(rpc(ErrorCode::Unavailable));
    }
    let item = w.text("c1", "x");
    let sent = w.wait_state(item.id, OutboxState::Sent).await;
    assert_eq!(sent.attempts, 4);
    let calls = w.fake.calls();
    assert_eq!(calls.len(), 4);
    let gaps: Vec<Duration> = calls.windows(2).map(|p| p[1].at - p[0].at).collect();
    for (gap, want) in gaps.iter().zip([1u64, 2, 4]) {
        let want = Duration::from_secs(want);
        assert!(
            *gap >= want && *gap < want + Duration::from_millis(200),
            "gap {gap:?} want {want:?}"
        );
    }
    assert!(w.fake.client_ids().iter().all(|id| id == &item.client_id));
}

#[tokio::test(start_paused = true)]
async fn unavailable_errors_exhaust_into_failed() {
    let w = world_with(
        true,
        OutboxPolicy {
            max_attempts: 3,
            ..policy()
        },
    );
    for _ in 0..5 {
        w.fake.push(rpc(ErrorCode::Unavailable));
    }
    let item = w.text("c1", "x");
    let failed = w.wait_state(item.id, OutboxState::Failed).await;
    assert_eq!(failed.attempts, 3);
    assert_eq!(
        failed.last_error.as_ref().unwrap().code(),
        ErrorCode::Unavailable
    );
    assert_eq!(w.fake.calls().len(), 3);
    assert_eq!(
        w.echo_row(&failed).unwrap().delivery.as_deref(),
        Some("failed")
    );
}

#[tokio::test(start_paused = true)]
async fn permanent_failure_then_user_retry() {
    let mut w = world();
    w.fake.push(rpc(ErrorCode::Forbidden));
    let item = w.text("c1", "x");
    let failed = w.wait_state(item.id, OutboxState::Failed).await;
    assert_eq!(
        failed.last_error.as_ref().unwrap().code(),
        ErrorCode::Forbidden
    );
    assert!(failed.next_attempt_at.is_none());
    assert_eq!(
        w.echo_row(&failed).unwrap().delivery.as_deref(),
        Some("failed")
    );
    tokio::time::sleep(Duration::from_secs(300)).await;
    assert_eq!(
        w.fake.calls().len(),
        1,
        "a permanent failure is never retried by itself"
    );

    // Not cancellable or deletable while... well, failed is deletable; retry re-queues.
    assert!(w.outbox.cancel(item.id).is_err());
    w.drain_changes();
    let again = w.outbox.retry(item.id).unwrap();
    assert_eq!(again.state, OutboxState::Queued);
    assert_eq!(again.attempts, 0);
    assert_eq!(
        w.echo_row(&again).unwrap().delivery.as_deref(),
        Some("sending")
    );
    let sent = w.wait_state(item.id, OutboxState::Sent).await;
    assert_eq!(sent.attempts, 1);
    let ids = w.fake.client_ids();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[0], ids[1], "a user retry re-sends the same client_id");
    assert_eq!(w.echo_row(&sent).unwrap().message_id, "m1");
    assert!(
        w.outbox.retry(item.id).is_err(),
        "only failed items can be retried"
    );
}

#[tokio::test(start_paused = true)]
async fn not_supported_and_invalid_params_fail_permanently() {
    let w = world();
    w.fake.push(rpc(ErrorCode::NotSupported));
    w.fake.push(rpc(ErrorCode::InvalidParams));
    let a = w.text("c1", "a");
    let b = w.text("c2", "b");
    let a = w.wait_state(a.id, OutboxState::Failed).await;
    let b = w.wait_state(b.id, OutboxState::Failed).await;
    assert_eq!(a.last_error.unwrap().code(), ErrorCode::NotSupported);
    assert_eq!(b.last_error.unwrap().code(), ErrorCode::InvalidParams);
    assert_eq!(w.fake.calls().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn cancel_queued_item_removes_echo() {
    let mut w = world_with(false, policy());
    let item = w.text("c1", "x");
    w.settle().await;
    assert!(w.echo_row(&item).is_some());
    let c = w.outbox.cancel(item.id).unwrap();
    assert_eq!(c.state, OutboxState::Cancelled);
    assert!(w.echo_row(&item).is_none(), "the local echo is gone");
    w.fake.set_online(true);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(w.fake.calls().is_empty(), "a cancelled item is never sent");
    let changes = w.drain_changes();
    assert!(
        changes
            .iter()
            .any(|c| matches!(c, OutboxChange::Ingested(a) if a.event == "local.deleted"))
    );
    w.outbox.delete(item.id).unwrap();
    assert!(w.outbox.get(item.id).is_err());
}

#[tokio::test(start_paused = true)]
async fn cancel_in_flight_request() {
    let w = world();
    let _release = w.fake.hold(Reply::Ok(json!({})), false);
    let item = w.text("c1", "x");
    let sending = w.wait_state(item.id, OutboxState::Sending).await;
    assert_eq!(w.fake.calls().len(), 1);
    let r = w.outbox.cancel(sending.id).unwrap();
    assert_eq!(
        r.state,
        OutboxState::Sending,
        "still in flight until the plugin answers"
    );
    let c = w.wait_state(item.id, OutboxState::Cancelled).await;
    assert!(c.last_error.is_none());
    assert!(w.echo_row(&item).is_none());
    assert_eq!(w.fake.calls().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn cancel_in_flight_but_plugin_had_finished() {
    let w = world();
    // The plugin ignores the cancel and answers with the message (§11.0): it was sent.
    let release = w.fake.hold(
        Reply::Ok(json!({"message": {"id": "p9", "chat_id": "c1", "direction": "out", "sent_at": "2026-01-01T10:00:00.000Z", "text": "x"}})),
        true,
    );
    let item = w.text("c1", "x");
    w.wait_state(item.id, OutboxState::Sending).await;
    w.outbox.cancel(item.id).unwrap();
    release.send(()).unwrap();
    let sent = w.wait_state(item.id, OutboxState::Sent).await;
    assert_eq!(sent.result_message_ids, Some(vec!["p9".into()]));
    assert_eq!(w.echo_row(&sent).unwrap().message_id, "p9");
}

#[tokio::test(start_paused = true)]
async fn ordering_per_conversation_parallel_across_conversations() {
    let w = world();
    let release = w.fake.hold(Reply::Ok(json!({"message": {"id": "a1", "chat_id": "A", "direction": "out", "sent_at": "2026-01-01T10:00:00.000Z"}})), false);
    let a1 = w.text("A", "a1");
    let a2 = w.text("A", "a2");
    let b1 = w.text("B", "b1");
    // A1 is held; B1 must go out meanwhile; A2 must wait for A1.
    w.wait_state(b1.id, OutboxState::Sent).await;
    assert_eq!(w.outbox.get(a1.id).unwrap().state, OutboxState::Sending);
    assert_eq!(w.outbox.get(a2.id).unwrap().state, OutboxState::Queued);
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(w.fake.calls().len(), 2, "A2 does not overtake A1");
    release.send(()).unwrap();
    w.wait_state(a2.id, OutboxState::Sent).await;
    let order: Vec<String> = w
        .fake
        .calls()
        .iter()
        .map(|c| c.params["text"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(order, ["a1", "b1", "a2"]);
    assert_each_client_id_once(&w.fake);
}

#[tokio::test(start_paused = true)]
async fn failed_head_does_not_block_its_lane() {
    let w = world();
    w.fake.push(rpc(ErrorCode::Forbidden));
    let a1 = w.text("A", "a1");
    let a2 = w.text("A", "a2");
    w.wait_state(a1.id, OutboxState::Failed).await;
    w.wait_state(a2.id, OutboxState::Sent).await;
}

#[tokio::test(start_paused = true)]
async fn offline_account_waits_then_sends() {
    let w = world_with(false, policy());
    let item = w.text("c1", "x");
    tokio::time::sleep(Duration::from_secs(120)).await;
    assert!(w.fake.calls().is_empty(), "nothing is sent while offline");
    assert_eq!(w.outbox.get(item.id).unwrap().state, OutboxState::Queued);
    w.fake.set_online(true);
    w.wait_state(item.id, OutboxState::Sent).await;
    assert_eq!(w.fake.calls().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn auth_required_pauses_until_reconnected() {
    let w = world();
    w.fake.push(rpc(ErrorCode::AuthRequired));
    let item = w.text("c1", "x");
    let paused = w.wait(item.id, |i| i.last_error.is_some()).await;
    assert_eq!(paused.state, OutboxState::Queued, "paused, not failed");
    assert_eq!(
        w.echo_row(&paused).unwrap().delivery.as_deref(),
        Some("sending")
    );
    tokio::time::sleep(Duration::from_secs(20)).await;
    assert_eq!(
        w.fake.calls().len(),
        1,
        "no retry while the account has not reconnected"
    );
    // The account goes auth_required → connected again.
    w.fake.set_online(false);
    w.fake.set_online(true);
    w.wait_state(item.id, OutboxState::Sent).await;
    assert_eq!(w.fake.calls().len(), 2);
    assert!(w.fake.client_ids().iter().all(|id| id == &item.client_id));
}

#[tokio::test(start_paused = true)]
async fn plugin_not_running_pauses() {
    let w = world();
    w.fake
        .push(Reply::Err(HostError::UnknownAccount(ACC.into())));
    let item = w.text("c1", "x");
    let paused = w.wait(item.id, |i| i.last_error.is_some()).await;
    assert_eq!(paused.state, OutboxState::Queued);
    w.fake.set_online(false);
    w.fake.set_online(true);
    w.wait_state(item.id, OutboxState::Sent).await;
}

#[tokio::test(start_paused = true)]
async fn restart_mid_send_with_echo_arriving() {
    // Process 1: enqueue, the request goes out, the process dies before the reply.
    let w1 = world_with(false, policy());
    let item = w1.text("c1", "hi");
    {
        let g = w1.ingest.lock().unwrap();
        ch_db::set_outbox_state(g.db().conn(), item.id, "sending", None, None, None).unwrap();
    }
    w1.outbox.shutdown().await;
    // Process 2 over the same database.
    let fake2 = Fake::new(true);
    let outbox2 = Outbox::new(Arc::clone(&w1.ingest), fake2.clone(), policy());
    outbox2.start().unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        outbox2.get(item.id).unwrap().state,
        OutboxState::Sending,
        "reconciling, not re-sent"
    );
    assert!(fake2.calls().is_empty());
    w1.platform_echo("c1", "p42", &item.client_id);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let sent = loop {
        assert!(tokio::time::Instant::now() < deadline);
        let i = outbox2.get(item.id).unwrap();
        if i.state == OutboxState::Sent {
            break i;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(sent.result_message_ids, Some(vec!["p42".into()]));
    assert!(fake2.calls().is_empty(), "never re-sent");
    let row = w1.echo_row(&sent).unwrap();
    assert_eq!(row.message_id, "p42");
    assert_eq!(row.delivery.as_deref(), Some("sent"));
}

#[tokio::test(start_paused = true)]
async fn restart_mid_send_without_echo_fails_as_unknown() {
    let w1 = world_with(false, policy());
    let item = w1.text("c1", "hi");
    {
        let g = w1.ingest.lock().unwrap();
        ch_db::set_outbox_state(g.db().conn(), item.id, "sending", None, None, None).unwrap();
    }
    w1.outbox.shutdown().await;
    let fake2 = Fake::new(false);
    let outbox2 = Outbox::new(Arc::clone(&w1.ingest), fake2.clone(), policy());
    outbox2.start().unwrap();
    // Offline: the reconcile clock does not run yet.
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(outbox2.get(item.id).unwrap().state, OutboxState::Sending);
    fake2.set_online(true);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let failed = loop {
        assert!(tokio::time::Instant::now() < deadline);
        let i = outbox2.get(item.id).unwrap();
        if i.state == OutboxState::Failed {
            break i;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let err = failed.last_error.clone().unwrap();
    assert_eq!(err.data.unwrap().reason.as_deref(), Some("outcome_unknown"));
    assert!(fake2.calls().is_empty(), "never re-sent by itself");
    assert_eq!(
        w1.echo_row(&failed).unwrap().delivery.as_deref(),
        Some("failed")
    );
    // The user may retry: one more send, same client_id.
    outbox2.retry(item.id).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        assert!(tokio::time::Instant::now() < deadline);
        if outbox2.get(item.id).unwrap().state == OutboxState::Sent {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(fake2.client_ids(), vec![item.client_id.clone()]);
}

#[tokio::test(start_paused = true)]
async fn restart_requeues_idempotent_edit() {
    let w1 = world_with(false, policy());
    let item = w1
        .outbox
        .enqueue(
            ACC,
            OutboxRequest::Edit(MessagesEditParams {
                chat_id: "c1".into(),
                message_id: "m7".into(),
                text: Some("new".into()),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
    {
        let g = w1.ingest.lock().unwrap();
        ch_db::set_outbox_state(g.db().conn(), item.id, "sending", None, None, None).unwrap();
    }
    w1.outbox.shutdown().await;
    let fake2 = Fake::new(true);
    let outbox2 = Outbox::new(Arc::clone(&w1.ingest), fake2.clone(), policy());
    outbox2.start().unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        assert!(tokio::time::Instant::now() < deadline);
        if outbox2.get(item.id).unwrap().state == OutboxState::Sent {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(fake2.calls().len(), 1);
    assert_eq!(fake2.calls()[0].method, "messages.edit");
}

#[tokio::test(start_paused = true)]
async fn timeout_reconciles_instead_of_resending() {
    let w = world();
    w.fake.push(rpc(ErrorCode::Timeout));
    let item = w.text("c1", "hi");
    let reconciling = w.wait(item.id, |i| i.last_error.is_some()).await;
    assert_eq!(reconciling.state, OutboxState::Sending);
    assert_eq!(reconciling.attempts, 1);
    assert!(
        w.outbox.cancel(item.id).is_err(),
        "cannot cancel what may have been sent"
    );
    // A later message in the same chat waits behind the reconcile.
    let next = w.text("c1", "later");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(w.fake.calls().len(), 1);
    w.platform_echo("c1", "p1", &item.client_id);
    let sent = w.wait_state(item.id, OutboxState::Sent).await;
    assert_eq!(sent.result_message_ids, Some(vec!["p1".into()]));
    w.wait_state(next.id, OutboxState::Sent).await;
    assert_eq!(w.fake.calls().len(), 2);
    assert_each_client_id_once(&w.fake);
}

#[tokio::test(start_paused = true)]
async fn timeout_without_echo_fails_after_reconcile_timeout() {
    let w = world();
    w.fake.push(rpc(ErrorCode::Timeout));
    let item = w.text("c1", "hi");
    let failed = w.wait_state(item.id, OutboxState::Failed).await;
    assert_eq!(
        failed.last_error.unwrap().data.unwrap().reason.as_deref(),
        Some("outcome_unknown")
    );
    assert_eq!(w.fake.calls().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn plugin_restarted_mid_send_reconciles() {
    let w = world();
    w.fake.push(Reply::Err(HostError::code(
        ErrorCode::Internal,
        "plugin restarted",
    )));
    let item = w.text("c1", "hi");
    let r = w.wait(item.id, |i| i.last_error.is_some()).await;
    assert_eq!(r.state, OutboxState::Sending);
    w.platform_echo("c1", "p1", &item.client_id);
    w.wait_state(item.id, OutboxState::Sent).await;
    assert_eq!(w.fake.calls().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn upload_progress_lands_on_the_item() {
    let mut w = world();
    let release = w.fake.hold(Reply::Ok(json!({"message": {"id": "f1", "chat_id": "c1", "direction": "out", "sent_at": "2026-01-01T10:00:00.000Z"}})), false);
    let item = w.file("c1", "/tmp/does-not-matter.bin");
    assert_eq!(item.kind, OutboxKind::SendFile);
    let echo = w.echo_row(&item).unwrap();
    assert_eq!(
        echo.media.as_ref().unwrap().path.as_deref(),
        Some("/tmp/does-not-matter.bin")
    );
    w.wait_state(item.id, OutboxState::Sending).await;
    w.drain_changes();
    let progress = |done: u64, chat: Option<&str>| TransferProgressEvent {
        account_id: ACC.into(),
        request_id: Some(ch_protocol::RequestId::Number(1)),
        direction: TransferDirection::Upload,
        chat_id: chat.map(str::to_owned),
        message_id: None,
        done_bytes: done,
        total_bytes: Some(1000),
    };
    w.outbox
        .on_transfer_progress(PLUGIN, &progress(100, Some("c1")));
    let i = w.outbox.get(item.id).unwrap();
    assert_eq!((i.done_bytes, i.total_bytes), (Some(100), Some(1000)));
    // Without a chat id it still matches: one upload in flight.
    w.outbox.on_transfer_progress(PLUGIN, &progress(500, None));
    assert_eq!(w.outbox.get(item.id).unwrap().done_bytes, Some(500));
    // Another chat's progress does not.
    w.outbox
        .on_transfer_progress(PLUGIN, &progress(900, Some("other")));
    assert_eq!(w.outbox.get(item.id).unwrap().done_bytes, Some(500));
    let mut stale = progress(950, Some("c1"));
    stale.request_id = Some(ch_protocol::RequestId::Number(42));
    w.outbox.on_transfer_progress(PLUGIN, &stale);
    stale.request_id = None;
    w.outbox.on_transfer_progress(PLUGIN, &stale);
    w.outbox
        .on_transfer_progress("another-plugin", &progress(980, Some("c1")));
    assert_eq!(w.outbox.get(item.id).unwrap().done_bytes, Some(500));
    // Downloads are ignored.
    let mut dl = progress(999, Some("c1"));
    dl.direction = TransferDirection::Download;
    w.outbox.on_transfer_progress(PLUGIN, &dl);
    assert_eq!(w.outbox.get(item.id).unwrap().done_bytes, Some(500));
    let progress_changes = w
        .drain_changes()
        .into_iter()
        .filter(|c| matches!(c, OutboxChange::Item(i) if i.done_bytes.is_some()))
        .count();
    assert_eq!(progress_changes, 2);
    release.send(()).unwrap();
    w.wait_state(item.id, OutboxState::Sent).await;
}

#[tokio::test(start_paused = true)]
async fn album_forward_edit_delete_kinds() {
    let w = world();
    // Seed a message to edit/delete/forward.
    let seed = common::ev(
        "messages.upsert",
        json!({"account_id": ACC, "messages": [
            common::msg("c1", "s1", "seed", json!({"direction": "out"})),
            common::msg("c1", "s2", "seed2", json!({"direction": "out"})),
        ]}),
    );
    w.ingest.lock().unwrap().apply(PLUGIN, seed).unwrap();

    let album = w
        .outbox
        .enqueue(
            ACC,
            OutboxRequest::SendAlbum(MessagesSendAlbumParams {
                chat_id: "c1".into(),
                items: vec![
                    AlbumItem {
                        path: "/tmp/a.jpg".into(),
                        ..Default::default()
                    },
                    AlbumItem {
                        path: "/tmp/b.jpg".into(),
                        ..Default::default()
                    },
                ],
                caption: Some("album".into()),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
    assert!(
        w.outbox
            .enqueue(
                ACC,
                OutboxRequest::SendAlbum(MessagesSendAlbumParams {
                    chat_id: "c1".into(),
                    items: vec![AlbumItem {
                        path: "/tmp/a.jpg".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                None,
            )
            .is_err(),
        "an album needs two items"
    );
    let edit = w
        .outbox
        .enqueue(
            ACC,
            OutboxRequest::Edit(MessagesEditParams {
                chat_id: "c1".into(),
                message_id: "s1".into(),
                text: Some("edited".into()),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
    let del = w
        .outbox
        .enqueue(
            ACC,
            OutboxRequest::Delete(MessagesDeleteParams {
                chat_id: "c1".into(),
                message_ids: vec!["s2".into()],
                revoke: true,
                ..Default::default()
            }),
            None,
        )
        .unwrap();
    let fwd = w
        .outbox
        .enqueue(
            ACC,
            OutboxRequest::Forward(MessagesForwardParams {
                from_chat_id: "c1".into(),
                message_ids: vec!["s1".into()],
                to_chat_id: "c2".into(),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
    assert!(
        edit.echo_message_id.is_none()
            && del.echo_message_id.is_none()
            && fwd.echo_message_id.is_none()
    );
    assert!(album.echo_message_id.is_some());

    let album = w.wait_state(album.id, OutboxState::Sent).await;
    assert_eq!(album.result_message_ids.as_ref().unwrap().len(), 2);
    let edit = w.wait_state(edit.id, OutboxState::Sent).await;
    assert_eq!(edit.result_message_ids, Some(vec!["s1".into()]));
    w.wait_state(del.id, OutboxState::Sent).await;
    let fwd = w.wait_state(fwd.id, OutboxState::Sent).await;
    assert_eq!(fwd.result_message_ids.as_ref().unwrap().len(), 1);

    let g = w.ingest.lock().unwrap();
    let conn = g.db().conn();
    let c1 = ch_db::conversation_rowid(conn, ACC, "c1").unwrap().unwrap();
    assert!(
        ch_db::get_message(conn, c1, "s2").unwrap().is_none(),
        "deleted for good"
    );
    let c2 = ch_db::conversation_rowid(conn, ACC, "c2").unwrap().unwrap();
    assert!(
        ch_db::get_message(conn, c2, &fwd.result_message_ids.unwrap()[0])
            .unwrap()
            .is_some(),
        "forward ingested"
    );
    let methods: Vec<String> = w.fake.calls().iter().map(|c| c.method.clone()).collect();
    assert!(methods.contains(&"messages.send_album".to_owned()));
    assert!(methods.contains(&"messages.delete".to_owned()));
    let del_call = w
        .fake
        .calls()
        .into_iter()
        .find(|c| c.method == "messages.delete")
        .unwrap();
    assert_eq!(del_call.params["revoke"], true);
    assert!(del_call.params.get("client_id").is_none());
}

#[tokio::test(start_paused = true)]
async fn delete_failed_item_removes_echo_and_row() {
    let mut w = world();
    w.fake.push(rpc(ErrorCode::Forbidden));
    let item = w.text("c1", "x");
    w.wait_state(item.id, OutboxState::Failed).await;
    w.drain_changes();
    w.outbox.delete(item.id).unwrap();
    assert!(w.echo_row(&item).is_none());
    assert!(w.outbox.get(item.id).is_err());
    assert!(w.outbox.list(ACC).unwrap().is_empty());
    let changes = w.drain_changes();
    assert!(
        changes
            .iter()
            .any(|c| matches!(c, OutboxChange::Removed { id, .. } if *id == item.id))
    );
}

#[tokio::test(start_paused = true)]
async fn unknown_account_is_rejected() {
    let w = world();
    let r = w.outbox.enqueue(
        "nope",
        OutboxRequest::SendText(MessagesSendParams {
            chat_id: "c1".into(),
            text: "x".into(),
            ..Default::default()
        }),
        None,
    );
    assert!(matches!(
        r,
        Err(ch_core::outbox::OutboxError::UnknownAccount(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn many_messages_each_client_id_once() {
    let w = world();
    // A mix of transient failures across several chats; every client id must be sent exactly
    // once (automatic retries re-send the *same* id only after the plugin said it did not go).
    for i in 0..6 {
        if i % 2 == 0 {
            w.fake.push(rpc(ErrorCode::Unavailable));
        }
    }
    let items: Vec<OutboxItem> = (0..12)
        .map(|i| w.text(&format!("c{}", i % 4), &format!("t{i}")))
        .collect();
    for it in &items {
        w.wait_state(it.id, OutboxState::Sent).await;
    }
    let calls = w.fake.calls();
    // 12 sends + 3 network retries.
    assert_eq!(calls.len(), 15);
    let mut ids = w.fake.client_ids();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 12);
    // Per chat, the texts were sent in enqueue order (retries repeat the same text).
    for chat in 0..4 {
        let texts: Vec<String> = calls
            .iter()
            .filter(|c| c.params["chat_id"] == format!("c{chat}"))
            .map(|c| c.params["text"].as_str().unwrap().to_owned())
            .collect();
        let mut dedup = texts.clone();
        dedup.dedup();
        let want: Vec<String> = (0..12)
            .filter(|i| i % 4 == chat)
            .map(|i| format!("t{i}"))
            .collect();
        assert_eq!(dedup, want, "chat c{chat}: {texts:?}");
    }
    assert_eq!(w.outbox.in_flight(), 0);
}

#[tokio::test(start_paused = true)]
async fn ambiguous_network_failure_with_late_echo_never_replays_on_retry() {
    let w = world();
    w.fake.push(rpc(ErrorCode::Network));
    let item = w.text("c", "payload");
    let failed = w.wait_state(item.id, OutboxState::Failed).await;
    assert_eq!(
        failed.last_error.unwrap().data.unwrap().reason.as_deref(),
        Some("outcome_unknown")
    );
    w.platform_echo("c", "p1", &item.client_id);
    assert_eq!(w.outbox.get(item.id).unwrap().state, OutboxState::Sent);
    assert!(w.outbox.retry(item.id).is_err());
    w.settle().await;
    assert_eq!(w.fake.calls().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn enqueue_failure_rolls_back_echo_and_conversation() {
    let w = world_with(false, policy());
    let g = w.ingest.lock().unwrap();
    g.db().conn().execute_batch("CREATE TEMP TRIGGER fail_insert BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT,'fixture'); END;").unwrap();
    drop(g);
    let req = OutboxRequest::SendText(MessagesSendParams {
        chat_id: "new".into(),
        text: "payload".into(),
        ..Default::default()
    });
    assert!(w.outbox.enqueue(ACC, req, None).is_err());
    assert!(w.outbox.list(ACC).unwrap().is_empty());
    let g = w.ingest.lock().unwrap();
    assert!(
        ch_db::conversation_rowid(g.db().conn(), ACC, "new")
            .unwrap()
            .is_none()
    );
}

#[tokio::test(start_paused = true)]
async fn cancellation_with_a_definitive_rejection_never_requeues() {
    let w = world();
    let release = w
        .fake
        .hold(Reply::Err(HostError::rpc(ErrorObject::flood_wait(3))), true);
    let item = w.text("c", "cancel");
    w.wait_state(item.id, OutboxState::Sending).await;
    w.outbox.cancel(item.id).unwrap();
    release.send(()).unwrap();
    w.wait_state(item.id, OutboxState::Cancelled).await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert_eq!(w.fake.calls().len(), 1);
    assert!(w.echo_row(&item).is_none());
}

#[tokio::test(start_paused = true)]
async fn cancelled_attempt_with_ambiguous_result_can_still_be_confirmed() {
    let w = world();
    let release = w.fake.hold(rpc(ErrorCode::Network), true);
    let item = w.text("c", "cancel");
    w.wait_state(item.id, OutboxState::Sending).await;
    w.outbox.cancel(item.id).unwrap();
    release.send(()).unwrap();
    w.wait(item.id, |i| i.last_error.is_some()).await;
    w.platform_echo("c", "delivered", &item.client_id);
    w.wait_state(item.id, OutboxState::Sent).await;
    assert_eq!(w.fake.calls().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn discussion_echo_links_preexisting_canonical_only_in_verified_chat() {
    let w = world();
    let source = common::ev(
        "messages.upsert",
        json!({"account_id":ACC,"messages":[
            common::msg("channel","post","source",json!({"replies_chat_id":"discussion"})),
            common::msg("discussion","comment","sent",json!({"direction":"out"}))
        ]}),
    );
    w.ingest.lock().unwrap().apply(PLUGIN, source).unwrap();
    w.fake.push(rpc(ErrorCode::Timeout));
    let item = w
        .outbox
        .enqueue(
            ACC,
            OutboxRequest::SendText(MessagesSendParams {
                chat_id: "channel".into(),
                comment_to: Some("post".into()),
                text: "sent".into(),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
    w.wait(item.id, |i| i.last_error.is_some()).await;
    w.platform_echo("unrelated", "comment", &item.client_id);
    assert_eq!(w.outbox.get(item.id).unwrap().state, OutboxState::Sending);
    w.platform_echo("discussion", "comment", &item.client_id);
    let sent = w.outbox.get(item.id).unwrap();
    assert_eq!(sent.state, OutboxState::Sent);
    assert_ne!(sent.echo_message_id, item.echo_message_id);
    assert_eq!(sent.result_message_ids, Some(vec!["comment".into()]));
    assert!(w.echo_row(&item).is_none());
    assert_eq!(w.echo_row(&sent).unwrap().delivery.as_deref(), Some("sent"));
    assert!(w.outbox.retry(item.id).is_err());
    assert_eq!(w.fake.calls().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn deletion_and_reuse_ignore_old_reply_and_dispatch_to_new_owner() {
    let w = world();
    let release=w.fake.hold(Reply::Ok(json!({"message":{"id":"old","chat_id":"c","direction":"out","sent_at":"2026-01-01T10:00:00.000Z"}})),true);
    let old = w.text("c", "old");
    w.wait_state(old.id, OutboxState::Sending).await;
    {
        let g = w.ingest.lock().unwrap();
        let conn = g.db().conn();
        let mut replacement = ch_db::get_plugin(conn, PLUGIN).unwrap().unwrap();
        replacement.plugin_id = "replacement".into();
        ch_db::upsert_plugin(conn, &replacement).unwrap();
        ch_db::delete_account(conn, ACC).unwrap();
        ch_db::create_account(conn, ACC, "replacement", None, &Default::default()).unwrap();
        // Force SQLite to reuse the old integer row id while its RPC is still outstanding.
        conn.execute("INSERT INTO outbox(id,account_id,chat_id,client_id,kind,method,params_json,state,created_at,updated_at)
            VALUES(?1,?2,'c','fresh','send_text','messages.send',?3,'sending',1,1)",rusqlite::params![
                old.id,ACC,json!({"account_id":ACC,"chat_id":"c","client_id":"fresh","text":"new"}).to_string()]).unwrap();
    }
    release.send(()).unwrap();
    w.settle().await;
    let fresh = w.outbox.get(old.id).unwrap();
    assert_eq!(fresh.client_id, "fresh");
    assert_eq!(fresh.state, OutboxState::Sending);
    {
        let g = w.ingest.lock().unwrap();
        assert!(
            ch_db::conversation_rowid(g.db().conn(), ACC, "c")
                .unwrap()
                .is_none()
        );
        ch_db::set_outbox_state(g.db().conn(), old.id, "queued", None, None, None).unwrap();
    }
    w.outbox.start_account(ACC).unwrap();
    w.wait_state(old.id, OutboxState::Sent).await;
    let calls = w.fake.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].plugin_id, PLUGIN);
    assert_eq!(calls[1].plugin_id, "replacement");
}
