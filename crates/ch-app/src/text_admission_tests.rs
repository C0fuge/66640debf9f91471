use super::*;
use ch_core::{host::MemorySecretStore, runtime::{CoreConfig, NewAccount}};
use ch_db::{ConversationRow, TopicRow};
use serde_json::json;
use std::{path::{Path, PathBuf}, sync::{Condvar, atomic::{AtomicUsize, Ordering}}, time::Duration};
use tokio::sync::Notify;

fn config(root:&Path)->CoreConfig {
    let mut config=CoreConfig::new(root,Arc::new(MemorySecretStore::new()));
    config.host.env.insert("CH_DATA_DIR".into(),root.to_string_lossy().into_owned());
    config.host.env.insert("CH_ACTION_TRACE".into(),root.join("trace").to_string_lossy().into_owned());
    for key in ["HOME","XDG_CONFIG_HOME","XDG_DATA_HOME","XDG_STATE_HOME","XDG_CACHE_HOME","XDG_RUNTIME_DIR"] {
        let path=root.join(key); std::fs::create_dir_all(&path).unwrap();
        config.host.env.insert(key.into(),path.to_string_lossy().into_owned());
    }
    config
}
async fn fixture()->(PathBuf,Arc<App>,Thread) { fixture_mode("good").await }
async fn fixture_mode(mode:&str)->(PathBuf,Arc<App>,Thread) {
    let temp=tempfile::tempdir().unwrap(); let root=temp.path().to_path_buf();
    let dir=root.join("plugins/text_fixture"); std::fs::create_dir_all(&dir).unwrap();
    let script=Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/action_plugin.py");
    std::fs::write(dir.join("plugin.json"),json!({"id":"text_fixture","name":"Text fixture","version":"0.0.1","api_version":"1.1.0",
        "entry":["python3","-B","-s",script,mode],"brand_color":"#123456","multi_account":true,"auth_methods":["none"]}).to_string()).unwrap();
    let app=Arc::new(App::open(config(&root),Some(temp)).await.unwrap());
    app.core.accounts().add(NewAccount::new("text_fixture").id("owner").no_start()).await.unwrap();
    let thread=Thread { account_id:"owner".into(),chat_id:"chat".into(),topic_id:Some("topic".into()) };
    seed(&app,&thread).await;
    (root,app,thread)
}
async fn seed(app:&App,thread:&Thread) {
    let t=thread.clone();
    app.read(move|db| {
        let id=ch_db::insert_conversation(db.conn(),&ConversationRow {account_id:t.account_id,chat_id:t.chat_id,
            kind:"group".into(),can_post:Some(true),created_at:1,..Default::default()})?;
        ch_db::insert_topic(db.conn(),&TopicRow {conversation_id:id,topic_id:"topic".into(),title:"Topic".into(),created_at:1,..Default::default()})?;
        Ok(())
    }).await.unwrap();
}
async fn send(app:&App,thread:&Thread)->Value { app.request(json!({"method":"enqueueText","params":{"thread":thread,"text":" original 😀 "}})).await }
fn pending(app:Arc<App>,thread:Thread)->JoinHandle<Value> { tokio::spawn(async move{send(&app,&thread).await}) }
async fn counts(app:&App)->(i64,i64) {
    app.read(|db| Ok((db.conn().query_row("SELECT count(*) FROM outbox",[],|r|r.get(0)).map_err(ch_db::Error::from)?,
        db.conn().query_row("SELECT count(*) FROM message",[],|r|r.get(0)).map_err(ch_db::Error::from)?))).await.unwrap()
}
#[derive(Default)]
struct Gate { reached:Notify,count:AtomicUsize,open:Mutex<bool>,released:Condvar }
impl Gate {
    fn hold(&self) {
        self.count.fetch_add(1,Ordering::SeqCst); self.reached.notify_waiters();
        let mut open=lock(&self.open);
        while !*open { open=self.released.wait(open).unwrap(); }
    }
    async fn reached(&self,count:usize) {
        tokio::time::timeout(Duration::from_secs(5),async {
            loop { let reached=self.reached.notified(); if self.count.load(Ordering::SeqCst)>=count { break; } reached.await; }
        }).await.unwrap();
    }
    fn release(&self) { *lock(&self.open)=true; self.released.notify_all(); }
}
fn checkpoint(app:&App,phase:&'static str)->Arc<Gate> {
    let gate=Arc::new(Gate::default()); let captured=gate.clone();
    *lock(&app.text_admissions.checkpoint)=Some(Arc::new(move|at|if at==phase {captured.hold();}));
    gate
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn offline_and_disabled_text_keep_exact_body_and_fresh_nonce_without_a_plugin_run() {
    let (_root,app,thread)=fixture().await;
    let one=send(&app,&thread).await; assert_eq!(one["ok"],true,"{one}");
    app.core.accounts().set_enabled(&thread.account_id,false).await.unwrap();
    let two=send(&app,&thread).await; assert_eq!(two["ok"],true,"{two}");
    assert_ne!(one["value"]["id"],two["value"]["id"]);
    assert_eq!(counts(&app).await,(2,2));
    let owner=thread.account_id.clone();
    app.read(move|db| {
        for row in ch_db::list_outbox(db.conn(),&owner,None)? {
            assert_eq!(row.params["text"]," original 😀 "); assert_eq!(row.params["topic_id"],"topic");
        }
        Ok(())
    }).await.unwrap();
    app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn final_writer_checks_reject_changes_without_recreating_a_stub_or_local_echo() {
    for sql in ["UPDATE conversation SET can_post=0", "UPDATE topic SET closed=1", "DELETE FROM topic", "DELETE FROM conversation",
        "UPDATE conversation SET created_at=created_at+1"] {
        let (_root,app,thread)=fixture().await;
        let gate=checkpoint(&app,"before-writer"); let request=pending(app.clone(),thread.clone()); gate.reached(1).await;
        let db=app.core.open_db().unwrap(); let tx=db.transaction().unwrap(); tx.execute(sql,[]).unwrap();
        gate.release(); tx.commit().unwrap();
        assert_eq!(request.await.unwrap()["ok"],false,"{sql}");
        assert_eq!(counts(&app).await,(0,0));
        app.shutdown().await;
    }
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn removal_revokes_and_waits_for_actual_admission_work_before_same_id_reuse() {
    for phase in ["before-writer","committed"] {
        let (_root,app,thread)=fixture().await;
        let owner=app.core.accounts().get(&thread.account_id).await.unwrap().unwrap();
        let (revoked,guard)=app.core.accounts().storage_lease(&owner).await.unwrap(); drop(guard);
        let gate=checkpoint(&app,phase); let request=pending(app.clone(),thread.clone()); gate.reached(1).await;
        let removing={let app=app.clone();let id=thread.account_id.clone();tokio::spawn(async move{app.core.accounts().remove(&id).await})};
        tokio::time::timeout(Duration::from_secs(5),revoked.cancelled()).await.unwrap();
        assert!(!removing.is_finished(),"the blocking worker still holds the old storage reader");
        gate.release();
        assert_eq!(request.await.unwrap()["ok"],phase=="committed");
        removing.await.unwrap().unwrap();
        *lock(&app.text_admissions.checkpoint)=None;
        app.core.accounts().add(NewAccount::new("text_fixture").id(&thread.account_id).no_start()).await.unwrap();
        seed(&app,&thread).await;
        assert_eq!(counts(&app).await,(0,0),"no old admission/echo can reach the recreated account");
        assert_eq!(send(&app,&thread).await["ok"],true);
        app.shutdown().await;
    }
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn caller_drop_keeps_four_real_blocking_slots_until_settlement() {
    let (_root,app,thread)=fixture().await; let gate=checkpoint(&app,"before-writer");
    let mut requests=Vec::new(); for _ in 0..JOBS { requests.push(pending(app.clone(),thread.clone())); }
    gate.reached(JOBS as usize).await;
    for request in requests {request.abort();let _=request.await;}
    assert_eq!(send(&app,&thread).await["error"]["code"],"conflict");
    assert_eq!(app.text_admissions.slots.available_permits(),0);
    gate.release(); app.text_admissions.drain().await;
    assert_eq!(counts(&app).await,(4,4));
    assert_eq!(app.text_admissions.slots.available_permits(),JOBS as usize);
    app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn shutdown_rejects_unlinearized_work_and_waits_after_a_shutdown_waiter_is_dropped() {
    let (root,app,thread)=fixture().await;
    let gate=checkpoint(&app,"before-writer");let request=pending(app.clone(),thread.clone());gate.reached(1).await;
    let shutting={let app=app.clone();tokio::spawn(async move{app.shutdown().await})};
    tokio::time::timeout(Duration::from_secs(5),app.text_admissions.stop.cancelled()).await.unwrap();
    assert!(!shutting.is_finished()); shutting.abort();let _=shutting.await;
    assert!(root.exists()); assert!(App::open(config(&root),None).await.is_err());
    let second={let app=app.clone();tokio::spawn(async move{app.shutdown().await})};
    assert!(!second.is_finished()); gate.release();
    assert_eq!(request.await.unwrap()["ok"],false);
    second.await.unwrap(); assert_eq!(counts(&app).await,(0,0));
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn a_panic_after_commit_is_unknown_and_never_admits_a_second_row() {
    let (_root,app,thread)=fixture().await;
    *lock(&app.text_admissions.checkpoint)=Some(Arc::new(|phase|if phase=="committed" {panic!("synthetic lost admission result");}));
    let result=send(&app,&thread).await;assert_eq!(result["error"]["outcomeUnknown"],true);
    app.text_admissions.drain().await;
    assert_eq!(counts(&app).await,(1,1));
    app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn drain_waits_for_a_real_writer_even_if_its_async_wrapper_panics() {
    let (_root,app,thread)=fixture().await;
    let gate=Arc::new(Gate::default());let waiting=Arc::new(Notify::new());
    let hold=gate.clone();let signal=waiting.clone();
    *lock(&app.text_admissions.checkpoint)=Some(Arc::new(move|phase| match phase {
        "before-writer"=>hold.hold(), "spawned"=>panic!("synthetic async wrapper failure"),
        "draining"=>signal.notify_one(), _=>{},
    }));
    let result=send(&app,&thread).await;
    assert_eq!(result["error"]["outcomeUnknown"],true);
    gate.reached(1).await;
    assert_eq!(app.text_admissions.slots.available_permits(),3);
    let service=app.text_admissions.clone();let draining=tokio::spawn(async move{service.drain().await});
    tokio::time::timeout(Duration::from_secs(5),waiting.notified()).await.unwrap();
    assert!(!draining.is_finished(),"the detached actual writer still owns a slot");
    gate.release();draining.await.unwrap();
    assert_eq!(counts(&app).await,(1,1));
    *lock(&app.text_admissions.checkpoint)=None;
    app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn formatted_text_preserves_live_capability_and_utf16_validation_gates() {
    for mode in ["good","without-send.entities"] {
        let (_root,app,thread)=fixture_mode(mode).await;
        let request=json!({"method":"enqueueText","params":{"thread":thread,"text":"A😀B","entities":[{"type":"bold","offset":1,"length":2}]}});
        assert_eq!(app.request(request.clone()).await["error"]["code"],"unavailable");
        assert_eq!(counts(&app).await,(0,0));
        app.core.accounts().start(&thread.account_id).await.unwrap();
        let result=app.request(request.clone()).await;
        if mode=="good" {
            assert_eq!(result["ok"],true,"{result}");
            let owner=thread.account_id.clone();
            app.read(move|db| {
                let rows=ch_db::list_outbox(db.conn(),&owner,None)?;
                assert_eq!(rows.len(),1);assert_eq!(rows[0].params["entities"][0]["offset"],1);
                assert_eq!(rows[0].params["entities"][0]["length"],2);Ok(())
            }).await.unwrap();
            let mut bad=request;bad["params"]["entities"][0]["length"]=json!(1);
            assert_eq!(app.request(bad).await["error"]["code"],"invalid_request");
            assert_eq!(counts(&app).await,(1,1));
        } else {
            assert_eq!(result["error"]["code"],"unsupported");assert_eq!(counts(&app).await,(0,0));
        }
        app.shutdown().await;
    }
}
