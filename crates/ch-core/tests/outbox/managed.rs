//! The generic worker cannot dispatch/reconcile a managed row through a raw transport.
use super::*;

fn bind_source(w: &World, item: &OutboxItem) {
    let g = w.ingest.lock().unwrap();
    g.db().conn().execute("INSERT INTO managed_source(stage_id,client_id,account_id,plugin_id,
        account_created_at,chat_id,path,fingerprint_json,intent_json,created_at,updated_at)
        VALUES(?1,?1,?2,?3,0,?4,?5,'{}','{}',0,0)",
        ch_db::rusqlite::params![item.client_id,ACC,PLUGIN,item.chat_id,
            format!("/synthetic/{}/bytes",item.client_id)]).unwrap();
}

#[tokio::test(start_paused = true)]
async fn managed_rows_park_without_registry_while_ordinary_lanes_send() {
    let w = world_with(false, policy());
    let first = w.file("owned", "/synthetic/owned-one");
    let second = w.file("old-owned", "/synthetic/owned-two");
    {
        let g = w.ingest.lock().unwrap();
        let conn = g.db().conn();
        for (item, stage, state) in [
            (&first, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "retained"),
            (&second, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "released"),
        ] {
            // Metadata fixture only: no fake file/registration/producer permit.
            conn.execute("INSERT INTO managed_source(stage_id,client_id,account_id,plugin_id,
                account_created_at,chat_id,path,fingerprint_json,intent_json,state,created_at,updated_at)
                VALUES(?1,?2,?3,?4,0,?5,?6,'{}','{}',?7,0,0)",
                ch_db::rusqlite::params![stage,item.client_id,ACC,PLUGIN,item.chat_id,
                    format!("/synthetic/{stage}/bytes"),state]).unwrap();
        }
        // Historical sending rows must also avoid the generic reconciliation route.
        conn.execute("UPDATE outbox SET state='sending', attempts=1 WHERE id=?1", [second.id]).unwrap();
        assert_eq!(conn.query_row("SELECT count(*) FROM managed_outbox_dispatch", [],
            |r| r.get::<_, i64>(0)).unwrap(), 0);
    }
    let behind = w.text("owned", "wait behind managed head");
    let ordinary = w.text("ordinary", "ordinary still sends");
    w.fake.set_online(true);
    w.wait_state(ordinary.id, OutboxState::Sent).await;
    w.settle().await;
    assert_eq!(w.outbox.get(first.id).unwrap().state, OutboxState::Queued);
    assert_eq!(w.outbox.get(first.id).unwrap().attempts, 0);
    assert_eq!(w.outbox.get(second.id).unwrap().state, OutboxState::Sending);
    assert_eq!(w.outbox.get(second.id).unwrap().attempts, 1);
    assert_eq!(w.outbox.get(behind.id).unwrap().state, OutboxState::Queued);
    assert!(w.fake.calls().iter().all(|call| call.params["client_id"] == ordinary.client_id));
    w.outbox.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn managed_adopted_failed_rows_survive_start_retry_cancel_and_delete() {
    let w = world_with(false, policy());
    let item = w.file("managed", "/synthetic/owned");
    bind_source(&w, &item);
    {
        let g = w.ingest.lock().unwrap();
        let conn = g.db().conn();
        let mut echo = ch_db::get_message_by_id(conn, item.echo_message_id.unwrap()).unwrap().unwrap();
        echo.message_id = "remote-adopted".into();
        echo.delivery = Some("read".into());
        ch_db::update_message(conn, &echo).unwrap();
        conn.execute("UPDATE outbox SET state='failed',attempts=7 WHERE id=?1", [item.id]).unwrap();
    }
    let before = w.outbox.get(item.id).unwrap();
    w.outbox.start().unwrap(); // includes reconcile_all_late before worker::pass
    w.settle().await;
    assert_eq!(w.outbox.get(item.id).unwrap(), before);
    for result in [w.outbox.retry(item.id), w.outbox.cancel(item.id)] {
        assert!(matches!(result, Err(ch_core::outbox::OutboxError::ManagedUnavailable)));
    }
    assert!(matches!(w.outbox.delete(item.id), Err(ch_core::outbox::OutboxError::ManagedUnavailable)));
    assert_eq!(w.outbox.get(item.id).unwrap(), before);
    // Cancel must not turn an adopted queued/sending row into Sent either.
    for state in ["queued", "sending"] {
        w.ingest.lock().unwrap().db().conn().execute(
            "UPDATE outbox SET state=?2 WHERE id=?1", ch_db::rusqlite::params![item.id,state]).unwrap();
        let before = w.outbox.get(item.id).unwrap();
        assert!(matches!(w.outbox.cancel(item.id), Err(ch_core::outbox::OutboxError::ManagedUnavailable)));
        assert_eq!(w.outbox.get(item.id).unwrap(), before);
    }
    assert!(w.fake.calls().is_empty());
    w.outbox.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_late_generic_result_cannot_finish_a_now_managed_attempt() {
    let w = world();
    let release = w.fake.hold(Reply::Ok(json!({"message": {
        "id":"remote", "chat_id":"managed", "direction":"out",
        "sent_at":"2026-01-01T10:00:00.000Z"
    }})), true);
    let item = w.file("managed", "/synthetic/owned");
    while w.fake.calls().is_empty() { tokio::task::yield_now().await; }
    // Defensive synthetic reclassification after the old raw frame: this is not
    // a supported registration route, and cannot grant managed completion authority.
    bind_source(&w, &item);
    let before = w.outbox.get(item.id).unwrap();
    release.send(()).unwrap();
    w.settle().await;
    assert_eq!(w.outbox.get(item.id).unwrap(), before);
    assert_eq!(w.fake.calls().len(), 1);
    assert_eq!(w.echo_row(&item).unwrap().message_id, format!("local:{}", item.client_id));
    w.outbox.shutdown().await;
}
