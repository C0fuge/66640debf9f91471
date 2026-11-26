use super::*;
use crate::host::{AccountInfo, HostError, StateChange};
use crate::ingest::IngestOptions;
use ch_db::{Db, PluginRow};
use serde_json::Value;

struct Offline(broadcast::Sender<StateChange>);
#[async_trait::async_trait]
impl SendTransport for Offline {
    async fn call(&self, _: &str, _: &str, _: Value, _: CancellationToken, _: Arc<OnceLock<i64>>) -> std::result::Result<Value, HostError> {
        panic!("offline text admission must not dispatch")
    }
    fn account(&self, _: &str, _: &str) -> Option<AccountInfo> { None }
    fn subscribe(&self) -> broadcast::Receiver<StateChange> { self.0.subscribe() }
}
fn fixture() -> (Outbox, TextAdmissionTarget, MessagesSendParams, TextAdmissionLifetime) {
    let db = Db::open_in_memory().unwrap();
    ch_db::upsert_plugin(db.conn(), &PluginRow {
        plugin_id:"fixture".into(), name:"Fixture".into(), version:"0.0.1".into(), api_version:"1.1.0".into(),
        install_dir:None, brand_color:None, icon_path:None, manifest_json:None, enabled:true, installed:true,
        first_seen_at:1, last_seen_at:1,
    }).unwrap();
    let owner = ch_db::create_account(db.conn(), "a", "fixture", None, &Default::default()).unwrap();
    let id = ch_db::insert_conversation(db.conn(), &ConversationRow {
        account_id:"a".into(), chat_id:"chat".into(), kind:"group".into(), can_post:Some(true), created_at:1, ..Default::default()
    }).unwrap();
    ch_db::insert_topic(db.conn(), &TopicRow {
        conversation_id:id, topic_id:"topic".into(), title:"Topic".into(), created_at:1, ..Default::default()
    }).unwrap();
    let target = TextAdmissionTarget {
        owner, conversation: ch_db::get_conversation(db.conn(), "a", "chat").unwrap().unwrap(),
        topic:ch_db::get_topic(db.conn(), id, "topic").unwrap(),
    };
    let params = MessagesSendParams { account_id:"a".into(), chat_id:"chat".into(), topic_id:Some("topic".into()), text:" original  😀 ".into(), ..Default::default() };
    let (events, _) = broadcast::channel(1);
    let outbox = Outbox::new(Arc::new(Mutex::new(Ingest::new(db, IngestOptions::default()))), Arc::new(Offline(events)), OutboxPolicy::default());
    (outbox, target, params, TextAdmissionLifetime { owner_revoked:CancellationToken::new(), app_closed:CancellationToken::new() })
}
fn counts(outbox: &Outbox) -> (i64,i64) {
    let g = outbox.inner.ingest();
    let conn = g.db().conn();
    (conn.query_row("SELECT count(*) FROM outbox", [], |r|r.get(0)).unwrap(),
     conn.query_row("SELECT count(*) FROM message", [], |r|r.get(0)).unwrap())
}

#[tokio::test]
async fn text_scope_and_original_body_commit_once_per_explicit_call_even_when_disabled() {
    let (outbox, target, params, lifetime) = fixture();
    outbox.inner.ingest().db().conn().execute("UPDATE account SET enabled=0 WHERE account_id='a'", []).unwrap();
    let first = outbox.enqueue_text_checked(&target, params.clone(), &lifetime, None).unwrap();
    let second = outbox.enqueue_text_checked(&target, params.clone(), &lifetime, None).unwrap();
    assert_ne!(first.client_id, second.client_id);
    assert_eq!(counts(&outbox), (2,2));
    {
    let g = outbox.inner.ingest();
    let stored = ch_db::get_outbox(g.db().conn(), first.id).unwrap().unwrap();
    assert_eq!(stored.params["text"], params.text);
    assert_eq!(stored.params["topic_id"], "topic");
    assert_eq!(stored.params["client_id"], first.client_id);
    assert_eq!(first.state, OutboxState::Queued);
    }
    outbox.shutdown().await;
}

#[tokio::test]
async fn changed_owner_chat_topic_and_posting_metadata_reject_without_echo_or_stub() {
    for sql in [
        "UPDATE account SET created_at=created_at+1 WHERE account_id='a'",
        "UPDATE conversation SET is_gone=1",
        "UPDATE conversation SET created_at=created_at+1",
        "UPDATE conversation SET kind='channel'",
        "UPDATE conversation SET can_post=0",
        "UPDATE topic SET closed=1",
        "UPDATE topic SET is_gone=1",
        "UPDATE topic SET created_at=created_at+1",
        "DELETE FROM topic",
        "DELETE FROM conversation",
    ] {
        let (outbox, target, params, lifetime) = fixture();
        outbox.inner.ingest().db().conn().execute(sql, []).unwrap();
        assert!(matches!(outbox.enqueue_text_checked(&target, params, &lifetime, None), Err(TextAdmissionError::Rejected(OutboxError::AdmissionConflict))), "{sql}");
        assert_eq!(counts(&outbox), (0,0));
        outbox.shutdown().await;
    }
    let (outbox, mut target, mut params, lifetime) = fixture();
    target.topic = None;
    assert!(outbox.enqueue_text_checked(&target, params.clone(), &lifetime, None).is_err());
    params.topic_id = None;
    params.reply_to = Some(ch_protocol::methods::ReplyTarget { message_id:"m".into(), ..Default::default() });
    assert!(outbox.enqueue_text_checked(&target, params, &lifetime, None).is_err());
    assert_eq!(counts(&outbox), (0,0));
    outbox.shutdown().await;
}

#[tokio::test]
async fn revocation_before_the_linearization_rolls_back_but_a_later_revocation_can_finish() {
    for phase in ["before-commit", "linearized"] {
        let (outbox, target, params, lifetime) = fixture();
        let token = lifetime.owner_revoked.clone();
        *outbox.inner.text_checkpoint.lock().unwrap() = Some(Arc::new(move |at| { if at == phase { token.cancel(); } }));
        let result = outbox.enqueue_text_checked(&target, params, &lifetime, None);
        assert_eq!(result.is_ok(), phase == "linearized");
        assert_eq!(counts(&outbox), if phase == "linearized" { (1,1) } else { (0,0) });
        outbox.shutdown().await;
    }
    let (outbox, target, params, lifetime) = fixture();
    lifetime.app_closed.cancel();
    assert!(outbox.enqueue_text_checked(&target, params, &lifetime, None).is_err());
    assert_eq!(counts(&outbox), (0,0));
    outbox.shutdown().await;
}

#[tokio::test]
async fn a_known_commit_returns_its_receipt_even_when_worker_refresh_cannot_read_the_owner() {
    let (outbox, target, params, lifetime) = fixture();
    let inner = outbox.inner.clone();
    *outbox.inner.text_checkpoint.lock().unwrap() = Some(Arc::new(move |at| {
        if at == "committed" { inner.ingest().db().conn().execute("ALTER TABLE account RENAME TO temporarily_unavailable_account", []).unwrap(); }
    }));
    let receipt = outbox.enqueue_text_checked(&target, params, &lifetime, None).unwrap();
    assert_eq!(receipt.state, OutboxState::Queued);
    assert_eq!(counts(&outbox), (1,1));
    outbox.inner.ingest().db().conn().execute("ALTER TABLE temporarily_unavailable_account RENAME TO account", []).unwrap();
    *outbox.inner.text_checkpoint.lock().unwrap() = None;
    outbox.shutdown().await;
}

#[tokio::test]
async fn an_uncertain_commit_never_becomes_a_definite_unsent_result() {
    let (outbox, target, params, lifetime) = fixture();
    outbox.inner.ingest().db().conn().execute_batch("CREATE TABLE absent_parent (id INTEGER PRIMARY KEY); CREATE TABLE deferred_child (parent INTEGER REFERENCES absent_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER reject_commit AFTER INSERT ON outbox BEGIN INSERT INTO deferred_child VALUES (99); END;").unwrap();
    assert!(matches!(outbox.enqueue_text_checked(&target, params, &lifetime, None), Err(TextAdmissionError::CommitUnknown)));
    assert_eq!(counts(&outbox), (0,0));
    outbox.shutdown().await;
}
