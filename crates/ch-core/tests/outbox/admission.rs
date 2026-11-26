use super::*;
use ch_core::outbox::OutboxError;
use ch_db::AccountRow;

const ID: &str = "f1e20123456789abcdef0123456789ab";

fn owner(w: &World) -> AccountRow {
    ch_db::get_account(w.ingest.lock().unwrap().db().conn(), ACC)
        .unwrap()
        .unwrap()
}

fn file_params() -> MessagesSendFileParams {
    MessagesSendFileParams {
        chat_id: "chat".into(),
        topic_id: Some("topic".into()),
        path: "/synthetic/stage/bytes".into(),
        filename: Some("report.txt".into()),
        caption: Some("original".into()),
        ..Default::default()
    }
}

#[tokio::test]
async fn competing_connections_admit_one_row_echo_and_worker() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.db");
    let first_db = ch_db::Db::open(&path).unwrap();
    let second_db = ch_db::Db::open(&path).unwrap();
    let seed = common::fake();
    let plugin = ch_db::get_plugin(seed.db().conn(), PLUGIN).unwrap().unwrap();
    ch_db::upsert_plugin(first_db.conn(), &plugin).unwrap();
    let owner = ch_db::create_account(first_db.conn(), ACC, PLUGIN, None, &Default::default()).unwrap();

    let fake = Fake::new(false);
    let services = [first_db, second_db].map(|db| {
        let ingest = Arc::new(Mutex::new(Ingest::new(db, IngestOptions::default())));
        Outbox::new(ingest, fake.clone(), policy())
    });
    let mut changes = services.each_ref().map(Outbox::subscribe);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let [first, second] = services.each_ref().map(|service| {
        let s = service.clone();
        let owner = owner.clone();
        let barrier = barrier.clone();
        tokio::task::spawn_blocking(move || {
            barrier.wait();
            s.enqueue_with_id(&owner, ID, OutboxRequest::SendFile(file_params()), None)
                .unwrap()
        })
    });
    assert_eq!(first.await.unwrap(), second.await.unwrap());
    // run() subscribes when it starts and then sleeps on the offline account.
    tokio::time::timeout(Duration::from_secs(5), async {
        while fake.changes.receiver_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(fake.changes.receiver_count(), 1);
    let mut echo_changes = 0;
    let mut item_changes = 0;
    for stream in &mut changes {
        while let Ok(change) = stream.try_recv() {
            match change {
                OutboxChange::Ingested(_) => echo_changes += 1,
                OutboxChange::Item(_) => item_changes += 1,
                OutboxChange::Removed { .. } => panic!("unexpected removal"),
            }
        }
    }
    assert_eq!((echo_changes, item_changes), (1, 1));
    let db = ch_db::Db::open(&path).unwrap();
    let conversation = ch_db::conversation_rowid(db.conn(), ACC, "chat").unwrap().unwrap();
    assert_eq!(ch_db::count_messages(db.conn(), conversation).unwrap(), 1);
    assert_eq!(services[0].list(ACC).unwrap().len(), 1);

    // Make the account dispatchable without sending its normal wakeup event.
    // A new worker or a wake from either duplicate/recovery would now send the file.
    *fake.online.lock().unwrap() = true;
    let req = OutboxRequest::SendFile(file_params());
    for service in &services {
        let receipt = service.get_matching_admission(&owner, ID, &req).unwrap().unwrap();
        assert_eq!(service.enqueue_with_id(&owner, ID, req.clone(), None).unwrap(), receipt);
    }
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(fake.calls().is_empty());
    assert_eq!(fake.changes.receiver_count(), 1);
    for service in &services {
        service.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn comparison_includes_owner_scope_method_kind_and_full_wire_params() {
    let mut w = world_with(false, policy());
    let owner = owner(&w);
    let params = file_params();
    let req = OutboxRequest::SendFile(params.clone());
    let receipt = w.outbox.enqueue_with_id(&owner, ID, req.clone(), None).unwrap();
    w.drain_changes();

    let conflict = |owner: &AccountRow, request: OutboxRequest| {
        assert!(matches!(w.outbox.get_matching_admission(owner, ID, &request), Err(OutboxError::AdmissionConflict)));
        assert!(matches!(w.outbox.enqueue_with_id(owner, ID, request, None), Err(OutboxError::AdmissionConflict)));
    };
    for different in [
        MessagesSendFileParams { chat_id: "another".into(), ..params.clone() },
        MessagesSendFileParams { topic_id: None, ..params.clone() },
        MessagesSendFileParams { caption: Some("changed".into()), ..params.clone() },
        MessagesSendFileParams { path: "/synthetic/other".into(), ..params.clone() },
        MessagesSendFileParams { filename: Some("other.txt".into()), ..params.clone() },
        MessagesSendFileParams { as_file: Some(false), ..params.clone() },
        MessagesSendFileParams { spoiler: Some(true), ..params.clone() },
    ] {
        conflict(&owner, OutboxRequest::SendFile(different));
    }
    conflict(&owner, OutboxRequest::SendText(MessagesSendParams {
        chat_id: params.chat_id.clone(),
        topic_id: params.topic_id.clone(),
        text: "original".into(),
        ..Default::default()
    }));
    let other = {
        let g = w.ingest.lock().unwrap();
        ch_db::create_account(g.db().conn(), "other", PLUGIN, None, &Default::default()).unwrap()
    };
    conflict(&other, req.clone());
    let wrong_plugin = AccountRow { plugin_id: "different-plugin".into(), ..owner.clone() };
    let wrong_creation = AccountRow { created_at: owner.created_at + 1, ..owner.clone() };
    conflict(&wrong_plugin, req.clone());
    conflict(&wrong_creation, req.clone());

    for bad in ["", "abc", "F1E20123456789ABCDEF0123456789AB", "f1e20123456789abcdef0123456789az"] {
        assert!(matches!(
            w.outbox.enqueue_with_id(&owner, bad, req.clone(), None),
            Err(OutboxError::InvalidRequest(_))
        ));
        assert!(matches!(
            w.outbox.get_matching_admission(&owner, bad, &req),
            Err(OutboxError::InvalidRequest(_))
        ));
    }

    {
        let g = w.ingest.lock().unwrap();
        let conn = g.db().conn();
        let row = ch_db::get_outbox(conn, receipt.id).unwrap().unwrap();
        let pretty = serde_json::to_string_pretty(&row.params).unwrap();
        conn.execute("UPDATE outbox SET params_json=?1 WHERE id=?2", rusqlite::params![pretty, row.id]).unwrap();
        conn.pragma_update(None, "query_only", true).unwrap();
    }
    let relabelled = AccountRow { label: Some("updated label".into()), ..owner.clone() };
    assert_eq!(w.outbox.get_matching_admission(&relabelled, ID, &req).unwrap(), Some(receipt.clone()));
    {
        let g = w.ingest.lock().unwrap();
        let conn = g.db().conn();
        conn.pragma_update(None, "query_only", false).unwrap();
        conn.execute("UPDATE outbox SET method='messages.send_album' WHERE id=?1", [receipt.id]).unwrap();
    }
    conflict(&owner, req);
    assert!(w.drain_changes().is_empty());
    assert_eq!(w.outbox.list(ACC).unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn retained_failed_cancelled_and_inflight_rows_remain_the_same_admission() {
    let mut w = world_with(false, policy());
    let owner = owner(&w);
    let req = OutboxRequest::Delete(MessagesDeleteParams {
        chat_id: "chat".into(),
        message_ids: vec!["m1".into()],
        ..Default::default()
    });
    let item = w.outbox.enqueue_with_id(&owner, ID, req.clone(), None).unwrap();
    assert!(item.echo_message_id.is_none());
    w.drain_changes();
    for state in [OutboxState::Sending, OutboxState::Failed, OutboxState::Cancelled] {
        {
            let g = w.ingest.lock().unwrap();
            ch_db::set_outbox_state(g.db().conn(), item.id, state.as_str(), None, None, None).unwrap();
        }
        let current = w.outbox.get(item.id).unwrap();
        assert_eq!(current.state, state);
        assert_eq!(w.outbox.enqueue_with_id(&owner, ID, req.clone(), None).unwrap(), current);
        assert_eq!(w.outbox.get_matching_admission(&owner, ID, &req).unwrap(), Some(current));
    }
    assert!(w.drain_changes().is_empty());
    assert!(w.fake.calls().is_empty());
}

#[tokio::test(start_paused = true)]
async fn duplicate_of_sent_file_preserves_adopted_echo_and_platform_call() {
    let mut w = world();
    let owner = owner(&w);
    let req = OutboxRequest::SendFile(file_params());
    let queued = w.outbox.enqueue_with_id(&owner, ID, req.clone(), None).unwrap();
    let sent = w.wait_state(queued.id, OutboxState::Sent).await;
    assert_eq!(w.echo_row(&sent).unwrap().message_id, "m1");
    w.drain_changes();
    assert_eq!(w.outbox.enqueue_with_id(&owner, ID, req.clone(), None).unwrap(), sent);
    assert_eq!(w.outbox.get_matching_admission(&owner, ID, &req).unwrap(), Some(sent));
    w.settle().await;
    assert!(w.drain_changes().is_empty());
    let calls = w.fake.calls();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].params.get("client_id").is_none());
    let g = w.ingest.lock().unwrap();
    let conn = g.db().conn();
    let conv = ch_db::conversation_rowid(conn, ACC, "chat").unwrap().unwrap();
    assert_eq!(ch_db::count_messages(conn, conv).unwrap(), 1);
    assert!(ch_db::get_message(conn, conv, &format!("local:{ID}")).unwrap().is_none());
}

#[tokio::test(start_paused = true)]
async fn speculative_unique_failure_rolls_back_then_checks_for_a_committed_winner() {
    let mut w = world_with(false, policy());
    let owner = owner(&w);
    let req = OutboxRequest::SendFile(file_params());
    {
        let g = w.ingest.lock().unwrap();
        g.db().conn().execute_batch("
            CREATE TEMP TRIGGER collision BEFORE INSERT ON outbox WHEN NEW.method='messages.send_file'
            BEGIN
              INSERT INTO outbox(account_id, chat_id, client_id, kind, method, params_json, created_at, updated_at)
              VALUES (NEW.account_id, NEW.chat_id, NEW.client_id, 'fixture', 'fixture', '{}', 1, 1);
            END;
        ").unwrap();
    }
    assert!(matches!(w.outbox.enqueue_with_id(&owner, ID, req.clone(), None), Err(OutboxError::Db(_))));
    assert!(w.outbox.get_matching_admission(&owner, ID, &req).unwrap().is_none());
    assert!(w.drain_changes().is_empty());
    let g = w.ingest.lock().unwrap();
    assert!(ch_db::conversation_rowid(g.db().conn(), ACC, "chat").unwrap().is_none());
}

#[tokio::test(start_paused = true)]
async fn committed_admission_survives_postcommit_refresh_error_and_missing_row_is_read_only() {
    let w = world_with(false, policy());
    let owner = owner(&w);
    let req = OutboxRequest::SendFile(file_params());
    {
        let g = w.ingest.lock().unwrap();
        // Break the refresh_worker's AccountRow decode only after the outbox row is inserted.
        g.db().conn().execute_batch("
            CREATE TEMP TRIGGER refresh_error AFTER INSERT ON outbox
            BEGIN UPDATE account SET settings_json='broken' WHERE account_id=NEW.account_id; END;
        ").unwrap();
    }
    let receipt = w.outbox.enqueue_with_id(&owner, ID, req.clone(), None).unwrap();
    {
        let g = w.ingest.lock().unwrap();
        g.db().conn().execute("UPDATE account SET settings_json='{}' WHERE account_id=?1", [ACC]).unwrap();
    }
    assert_eq!(w.outbox.get_matching_admission(&owner, ID, &req).unwrap(), Some(receipt.clone()));
    {
        let g = w.ingest.lock().unwrap();
        assert!(ch_db::delete_outbox(g.db().conn(), receipt.id).unwrap());
    }
    assert!(w.outbox.get_matching_admission(&owner, ID, &req).unwrap().is_none());
    assert!(w.outbox.list(ACC).unwrap().is_empty());
    // Recreate the same account ID and plugin with a distinct persisted creation identity.
    {
        let g = w.ingest.lock().unwrap();
        let conn = g.db().conn();
        ch_db::delete_account(conn, ACC).unwrap();
        ch_db::create_account(conn, ACC, PLUGIN, None, &Default::default()).unwrap();
        conn.execute(
            "UPDATE account SET created_at=?1 WHERE account_id=?2",
            rusqlite::params![owner.created_at + 1, ACC],
        )
        .unwrap();
    }
    assert!(matches!(w.outbox.get_matching_admission(&owner, ID, &req), Err(OutboxError::AdmissionConflict)));
    assert!(matches!(w.outbox.enqueue_with_id(&owner, ID, req, None), Err(OutboxError::AdmissionConflict)));
    assert!(w.outbox.list(ACC).unwrap().is_empty());
}

#[test]
fn private_upload_directory_uses_the_existing_account_mapping() {
    let config = ch_core::host::HostConfig::new("/synthetic/data");
    for id in [ACC, "other", "../nested", ".."] {
        let root = config.state_dir(id);
        let parent = root.parent().unwrap();
        assert_eq!(config.cache_dir(id), parent.join("cache"));
        assert_eq!(config.upload_dir(id), parent.join("uploads"));
    }
}
