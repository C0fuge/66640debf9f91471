//! Exact observed reply confirmation on final results, early events and the Rust seam.
use super::*;
use ch_protocol::dto::Message;
use ch_protocol::methods::ReplyTarget;

fn enqueue(w: &World, topic: Option<&str>) -> OutboxItem {
    w.outbox.enqueue(ACC, OutboxRequest::SendText(MessagesSendParams {
        chat_id: "reply-chat".into(), text: "literal".into(), topic_id: topic.map(str::to_owned),
        reply_to: Some(ReplyTarget { message_id: "parent".into(), ..Default::default() }),
        ..Default::default()
    }), None).unwrap()
}

fn observed(topic: Option<&str>, reply: Value) -> Value {
    common::msg("reply-chat", "platform", "literal", json!({
        "direction":"out", "topic_id":topic, "reply_to":reply
    }))
}

fn event(w: &World, item: &OutboxItem, mut message: Value) {
    message["ext"] = json!({PLUGIN:{"client_id":item.client_id}});
    w.ingest.lock().unwrap().apply(PLUGIN, common::ev("messages.upsert", json!({
        "account_id":ACC,"messages":[message]
    }))).unwrap();
}

#[tokio::test(start_paused = true)]
async fn final_reply_result_requires_actual_id_parent_and_exact_topic() {
    for (topic, reply, result_topic) in [
        (None, Value::Null, None),
        (None, json!({"message_id":"other"}), None),
        (None, json!({"message_id":"parent","chat_id":"foreign"}), None),
        (None, json!({"message_id":"parent"}), Some("1")),
        (Some("topic"), json!({"message_id":"parent"}), None),
        (Some("topic"), json!({"message_id":"parent"}), Some("other-topic")),
    ] {
        let w = world();
        w.fake.push(Reply::Ok(json!({"message":observed(result_topic,reply)})));
        let item = enqueue(&w, topic);
        let failed = w.wait_state(item.id, OutboxState::Failed).await;
        assert_eq!(failed.last_error.unwrap().data.unwrap().reason.as_deref(), Some("outcome_unknown"));
        assert_eq!(w.fake.calls().len(), 1);
        let echo = w.echo_row(&item).unwrap();
        assert_eq!(echo.message_id, format!("local:{}", item.client_id));
        assert_eq!(echo.reply_to.unwrap().message_id, "parent", "requested echo is not observed proof");
        w.outbox.shutdown().await;
    }
    for (topic, reply) in [
        (None, json!({"message_id":"parent"})),
        (None, json!({"message_id":"parent","chat_id":"reply-chat"})),
        (Some("topic"), json!({"message_id":"parent"})),
    ] {
        let w = world();
        w.fake.push(Reply::Ok(json!({"message":observed(topic,reply)})));
        let item = enqueue(&w, topic);
        let sent = w.wait_state(item.id, OutboxState::Sent).await;
        assert_eq!(sent.result_message_ids, Some(vec!["platform".into()]));
        assert_eq!(w.fake.calls().len(), 1);
        w.outbox.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn invalid_early_client_event_cannot_adopt_requested_echo_or_short_circuit_rpc_error() {
    for (topic, reply, result_topic) in [
        (None, Value::Null, None),
        (None, json!({"message_id":"other"}), None),
        (None, json!({"message_id":"parent","chat_id":"foreign"}), None),
        (None, json!({"message_id":"parent"}), Some("1")),
        (Some("topic"), json!({"message_id":"parent"}), None),
    ] {
        let w = world();
        let release = w.fake.hold(rpc(ErrorCode::Network), true);
        let item = enqueue(&w, topic);
        w.wait_state(item.id, OutboxState::Sending).await;
        event(&w, &item, observed(result_topic, reply));
        assert_eq!(w.outbox.get(item.id).unwrap().state, OutboxState::Sending);
        assert_eq!(w.echo_row(&item).unwrap().message_id, format!("local:{}", item.client_id));
        release.send(()).unwrap();
        w.wait(item.id, |row| row.last_error.is_some()).await;
        assert_eq!(w.outbox.get(item.id).unwrap().state, OutboxState::Sending);
        if topic.is_none() && result_topic.is_some() {
            event(&w, &item, observed(None, json!({"message_id":"parent"})));
            assert_eq!(w.outbox.get(item.id).unwrap().state, OutboxState::Sending,
                "sparse omission cannot clear the canonical foreign topic or authorize adoption");
            w.wait_state(item.id, OutboxState::Failed).await;
            assert_eq!(w.fake.calls().len(), 1);
            w.outbox.shutdown().await;
            continue;
        }
        // The same platform row already exists from the rejected event. A later
        // real exact relation can confirm it, without any repeated transport send.
        event(&w, &item, observed(topic, json!({"message_id":"parent","chat_id":"reply-chat"})));
        assert_eq!(w.outbox.get(item.id).unwrap().state, OutboxState::Sent);
        assert_eq!(w.fake.calls().len(), 1);
        w.outbox.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn deleted_echo_id_reuse_is_neither_delivery_proof_nor_status_authority() {
    for invalid in ["account", "chat", "topic", "missing-cid", "wrong-cid", "reply", "delivery", "exact-looking"] {
        let w = world();
        let release = w.fake.hold(rpc(ErrorCode::Network), true);
        let item = enqueue(&w, None);
        w.wait_state(item.id, OutboxState::Sending).await;
        let reused = {
            let g = w.ingest.lock().unwrap();
            let conn = g.db().conn();
            ch_db::delete_message(conn, item.echo_message_id.unwrap()).unwrap();
            assert!(ch_db::get_outbox(conn, item.id).unwrap().unwrap().echo_message_id.is_none());
            let account = if invalid == "account" {
                ch_db::create_account(conn, "foreign-account", PLUGIN, None, &Default::default()).unwrap();
                "foreign-account"
            } else { ACC };
            let chat = if invalid == "chat" { "foreign-chat" } else { "reply-chat" };
            let conv = match ch_db::conversation_rowid(conn, account, chat).unwrap() {
                Some(id) => id,
                None => ch_db::insert_conversation(conn, &ch_db::ConversationRow {
                    account_id:account.into(), chat_id:chat.into(), kind:"dm".into(), ..Default::default()
                }).unwrap(),
            };
            let ext = if invalid == "missing-cid" { None } else {
                let cid = if invalid == "wrong-cid" { "other" } else { item.client_id.as_str() };
                Some(serde_json::from_value(json!({PLUGIN:{"client_id":cid}})).unwrap())
            };
            let reused = ch_db::insert_message(conn, &ch_db::MessageRow {
                conversation_id:conv, message_id:"unrelated".into(), direction:"out".into(),
                topic_id:(invalid == "topic").then(|| "foreign-topic".into()),
                reply_to:Some(ch_protocol::dto::ReplyRef {
                    message_id:if invalid == "reply" {"other".into()} else {"parent".into()},
                    ..Default::default()
                }), ext, delivery:Some(if invalid == "delivery" {"sending".into()} else {"read".into()}), ..Default::default()
            }).unwrap();
            assert_eq!(reused, item.echo_message_id.unwrap(), "fixture must exercise real rowid reuse");
            // exact-looking deliberately matches account/chat/topic/cid/reply/read.
            // None of those fields may recreate the revoked retained pointer.
            assert!(ch_db::get_outbox(conn, item.id).unwrap().unwrap().echo_message_id.is_none());
            reused
        };
        release.send(()).unwrap();
        w.wait_state(item.id, OutboxState::Failed).await;
        assert_eq!(w.fake.calls().len(), 1);
        assert_eq!(w.ingest.lock().unwrap().db().conn().query_row(
            "SELECT delivery FROM message WHERE id=?1", [reused], |row| row.get::<_, String>(0)
        ).unwrap(), if invalid == "delivery" {"sending"} else {"read"},
            "stale failure must not mutate the unrelated reused row");
        w.outbox.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn trusted_apply_sent_rejects_wrong_reply_and_foreign_account_before_any_stub_or_rekey() {
    let w = world_with(false, policy());
    let item = enqueue(&w, None);
    {
        let g = w.ingest.lock().unwrap();
        ch_db::create_account(g.db().conn(), "other-account", PLUGIN, None, &Default::default()).unwrap();
        let valid: Message = serde_json::from_value(observed(None, json!({"message_id":"parent"}))).unwrap();
        let invalid: Message = serde_json::from_value(observed(None, Value::Null)).unwrap();
        assert!(g.apply_sent(PLUGIN, ACC, Some(&item.client_id), &[]).is_err());
        assert!(g.apply_sent(PLUGIN, ACC, Some(&item.client_id), &[valid.clone(), valid.clone()]).is_err());
        assert!(g.apply_sent(PLUGIN, ACC, Some(&item.client_id), &[invalid]).is_err());
        assert!(g.apply_sent(PLUGIN, "other-account", Some(&item.client_id), std::slice::from_ref(&valid)).is_err());
        assert!(ch_db::conversation_rowid(g.db().conn(), "other-account", "reply-chat").unwrap().is_none());
        assert!(ch_db::get_message(g.db().conn(), ch_db::conversation_rowid(g.db().conn(), ACC, "reply-chat").unwrap().unwrap(), "platform").unwrap().is_none());
        assert_eq!(ch_db::get_outbox(g.db().conn(), item.id).unwrap().unwrap().state, "queued");
        g.apply_sent(PLUGIN, ACC, Some(&item.client_id), &[valid]).unwrap();
    }
    assert_eq!(w.echo_row(&item).unwrap().message_id, "platform");
    assert_eq!(w.outbox.get(item.id).unwrap().state, OutboxState::Sent);
    assert!(w.fake.calls().is_empty());
    w.outbox.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn plain_and_quoted_topic_sends_keep_existing_confirmation_behavior() {
    for reply_to in [None, Some(ReplyTarget {
        message_id:"parent".into(), quote_text:Some("quote".into()), ..Default::default()
    })] {
        let w = world();
        // Default fake result omits topic/reply. These paths are outside this amendment.
        let item = w.outbox.enqueue(ACC, OutboxRequest::SendText(MessagesSendParams {
            chat_id:"reply-chat".into(), topic_id:Some("topic".into()), text:"literal".into(),
            reply_to, ..Default::default()
        }), None).unwrap();
        w.wait_state(item.id, OutboxState::Sent).await;
        assert_eq!(w.fake.calls().len(), 1);
        w.outbox.shutdown().await;
    }
}
