//! Real app wire over a controllable stdlib plugin and isolated temporary cache.
use super::*;
use ch_core::{host::MemorySecretStore,runtime::{CoreConfig,NewAccount}};
use ch_db::types::{ConversationRow,MessageRow,TopicRow};
use std::{path::Path,sync::atomic::AtomicUsize};

async fn fixture(mode:&str)->(App,Thread) {
    let temp=tempfile::tempdir().unwrap();let dir=temp.path().join("plugins/media_fixture");std::fs::create_dir_all(&dir).unwrap();
    let script=Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/media_plugin.py");
    std::fs::write(dir.join("plugin.json"),json!({"id":"media_fixture","name":"Media fixture","version":"0.0.1","api_version":"1.0.0","entry":["python3","-B","-s",script,mode],"brand_color":"#123456","multi_account":true,"auth_methods":["none"]}).to_string()).unwrap();
    let mut config=CoreConfig::new(temp.path(),Arc::new(MemorySecretStore::new()));
    config.host.env.insert("CH_MEDIA_TRACE".into(),temp.path().join("trace.jsonl").to_string_lossy().into_owned());
    config.host.env.insert("CH_DATA_DIR".into(),temp.path().to_string_lossy().into_owned());
    for key in ["XDG_CONFIG_HOME","XDG_DATA_HOME","XDG_STATE_HOME","XDG_CACHE_HOME","XDG_RUNTIME_DIR"] { let p=temp.path().join(key);std::fs::create_dir(&p).unwrap();config.host.env.insert(key.into(),p.to_string_lossy().into_owned()); }
    let app=App::open(config,Some(temp)).await.unwrap();
    let account=app.core.accounts().add(NewAccount::new("media_fixture")).await.unwrap().account_id;
    let thread=Thread{account_id:account.clone(),chat_id:"chat".into(),topic_id:None};
    let root=app.core.host().config().cache_dir(&account);let cached=root.join("cached.bin");std::fs::write(&cached,b"isolated attachment").unwrap();
    app.read(move|db|{let c=ch_db::insert_conversation(db.conn(),&ConversationRow{account_id:account.clone(),chat_id:"chat".into(),kind:"group".into(),..Default::default()})?;
        ch_db::insert_conversation(db.conn(),&ConversationRow{account_id:account,chat_id:"other".into(),kind:"group".into(),..Default::default()})?;
        ch_db::insert_topic(db.conn(),&TopicRow{conversation_id:c,topic_id:"topic".into(),title:"Topic".into(),..Default::default()})?;
        for (id,topic) in [("message",None),("second",None),("topic-message",Some("topic")),("local:pending",None)] {
            let media:Media=serde_json::from_value(json!({"kind":"document","filename":"fixture.bin","path":cached,"bytes":19})).unwrap();
            ch_db::insert_message(db.conn(),&MessageRow{conversation_id:c,message_id:id.into(),topic_id:topic.map(str::to_owned),direction:"in".into(),sent_at:1,media:Some(media),..Default::default()})?;
        } Ok(())}).await.unwrap();
    (app,thread)
}
async fn call(app:&App,method:&str,params:Value)->Value {app.request(json!({"method":method,"params":params})).await}
async fn page(app:&App,thread:&Thread)->Value {call(app,"listMediaJobs",json!({"thread":thread,"cursor":null,"requestKey":null})).await["value"].clone()}
async fn start(app:&App,thread:&Thread,key:&str,kind:Kind,id:&str)->Value {
    call(app,if kind==Kind::Download{"startMediaDownload"}else{"startMediaSave"},json!({"thread":thread,"messageId":id,"sessionId":app.media.session,"requestKey":key})).await
}
async fn job(app:&App,thread:&Thread,key:&str)->Value {call(app,"listMediaJobs",json!({"thread":thread,"cursor":null,"requestKey":key})).await["value"]["jobs"][0].clone()}
async fn wait_state(app:&App,thread:&Thread,key:&str,state:&str)->Value {
    tokio::time::timeout(Duration::from_secs(8),async {loop {let j=job(app,thread,key).await;if j["state"]==state{return j;}tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap_or_else(|_|panic!("job did not reach {state}"))
}
fn trace(app:&App)->Vec<Value> {std::fs::read_to_string(app.core.data_dir().join("trace.jsonl")).unwrap_or_default().lines().map(|l|serde_json::from_str(l).unwrap()).collect()}
async fn wait_rpc(app:&App) {tokio::time::timeout(Duration::from_secs(5),async{while !trace(app).iter().any(|r|r["method"]=="media.download"){tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap();}
async fn release(app:&App,thread:&Thread) {let _:Value=app.core.host().call("media_fixture","messages.fetch_recent",
    &json!({"account_id":thread.account_id,"chat_id":"fixture-release","limit":1})).await.unwrap();}
async fn cancel(app:&App,thread:&Thread,key:&str)->Value {let j=job(app,thread,key).await;call(app,"cancelMediaJob",json!({"thread":thread,"sessionId":app.media.session,"jobId":j["id"]})).await}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn download_receipt_is_recoverable_idempotent_and_scoped() {
    let (app,thread)=fixture("hold").await;
    assert_eq!(call(&app,"inventory",json!({})).await["value"]["mediaAccess"]["saveCopy"],false);
    let first=start(&app,&thread,"intent",Kind::Download,"message").await;
    assert_eq!(first["ok"],true);
    wait_state(&app,&thread,"intent","running").await;
    assert_eq!(start(&app,&thread,"intent",Kind::Download,"message").await["value"]["id"],first["value"]["id"]);
    assert_eq!(start(&app,&thread,"different",Kind::Download,"message").await["error"]["code"],"conflict");
    assert_eq!(start(&app,&thread,"intent",Kind::Download,"second").await["error"]["code"],"invalid_request");
    assert_eq!(start(&app,&thread,"local",Kind::Download,"local:pending").await["ok"],false);
    let foreign=Thread{chat_id:"other".into(),..thread.clone()};
    assert_eq!(call(&app,"cancelMediaJob",json!({"thread":foreign,"sessionId":app.media.session,"jobId":first["value"]["id"]})).await["ok"],false);
    wait_rpc(&app).await;release(&app,&thread).await;let final_job=wait_state(&app,&thread,"intent","succeeded").await;
    assert_eq!(final_job["doneBytes"],"19");
    assert!(!final_job.to_string().contains(".bin"));assert!(!final_job.to_string().contains(&app.core.data_dir().to_string_lossy().into_owned()));
    assert_eq!(trace(&app).iter().filter(|r|r["method"]=="media.download").count(),1);
    app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn only_matching_tracked_progress_updates_and_event_alone_does_not_confirm() {
    let (app,thread)=fixture("progress-hold").await;start(&app,&thread,"progress",Kind::Download,"message").await;
    tokio::time::timeout(Duration::from_secs(5),async {loop {if job(&app,&thread,"progress").await["doneBytes"]=="4" {break;}tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap();
    let j=job(&app,&thread,"progress").await;assert_eq!(j["totalBytes"],Value::Null);assert_eq!(j["state"],"running");
    release(&app,&thread).await;wait_state(&app,&thread,"progress","succeeded").await;app.shutdown().await;
    let (app,thread)=fixture("event-hold").await;start(&app,&thread,"event",Kind::Download,"message").await;
    wait_state(&app,&thread,"event","running").await;
    wait_rpc(&app).await;
    tokio::time::timeout(Duration::from_secs(5),async {loop {let t=thread.clone();let recorded=app.read(move|db|{let c=ch_db::conversation_rowid(db.conn(),&t.account_id,&t.chat_id)?.unwrap();Ok(ch_db::get_message(db.conn(),c,"message")?.unwrap().media.unwrap().path.unwrap().ends_with("message.bin"))}).await.unwrap();if recorded{break;}tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap();
    assert_ne!(job(&app,&thread,"event").await["state"],"succeeded");
    release(&app,&thread).await;wait_state(&app,&thread,"event","succeeded").await;app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn response_event_orders_converge_and_invalid_cache_or_confirmation_is_not_success() {
    for mode in ["before","after","state-path","malformed"] {
        let (app,thread)=fixture(mode).await;start(&app,&thread,"order",Kind::Download,"message").await;
        let expected=if mode=="state-path"{"failed"}else if mode=="malformed"{"unknown"}else{"succeeded"};
        let final_job=wait_state(&app,&thread,"order",expected).await;
        if mode=="malformed" {assert_eq!(final_job["error"]["outcomeUnknown"],true);}
        assert_eq!(trace(&app).iter().filter(|r|r["method"]=="media.download").count(),1);
        app.shutdown().await;
    }
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn cancellation_keeps_ignored_rpc_reserved_and_completion_can_win() {
    let (app,thread)=fixture("ignore-cancel").await;start(&app,&thread,"cancel",Kind::Download,"message").await;wait_state(&app,&thread,"cancel","running").await;
    wait_rpc(&app).await;
    assert_eq!(cancel(&app,&thread,"cancel").await["value"]["state"],"cancelling");
    assert_eq!(start(&app,&thread,"again",Kind::Download,"message").await["error"]["code"],"conflict");
    release(&app,&thread).await;wait_state(&app,&thread,"cancel","succeeded").await;app.shutdown().await;
    let (app,thread)=fixture("completion-wins").await;start(&app,&thread,"race",Kind::Download,"message").await;wait_state(&app,&thread,"race","running").await;
    wait_rpc(&app).await;cancel(&app,&thread,"race").await;let j=wait_state(&app,&thread,"race","succeeded").await;assert_eq!(j["cancelRequested"],true);app.shutdown().await;
}

#[derive(Default)]
struct HeldDialog {calls:AtomicUsize,callback:Mutex<Option<SaveDialogCallback>>}
impl SaveDialog for HeldDialog {fn choose(&self,_:String,callback:SaveDialogCallback){*lock(&self.callback)=Some(callback);self.calls.fetch_add(1,Ordering::AcqRel);}}
impl HeldDialog {
    async fn answer(&self,value:std::result::Result<Option<PathBuf>,SaveDialogError>) {
        let callback=tokio::time::timeout(Duration::from_secs(5),async {loop {if let Some(callback)=lock(&self.callback).take(){break callback;}tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap();
        callback(value);
    }
}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn cancelled_chooser_keeps_native_slot_and_late_selection_never_saves() {
    let (mut app,thread)=fixture("good").await;let dialog=Arc::new(HeldDialog::default());app.set_save_dialog(dialog.clone());
    assert_eq!(call(&app,"inventory",json!({})).await["value"]["mediaAccess"]["saveCopy"],true);
    start(&app,&thread,"save",Kind::Save,"message").await;wait_state(&app,&thread,"save","choosing").await;
    cancel(&app,&thread,"save").await;
    assert_eq!(start(&app,&thread,"second-save",Kind::Save,"second").await["error"]["code"],"conflict");
    assert_eq!(dialog.calls.load(Ordering::Acquire),1);
    let dest=app.core.data_dir().join("never-saved.bin");dialog.answer(Ok(Some(dest.clone()))).await;
    wait_state(&app,&thread,"save","cancelled").await;assert!(!dest.exists());
    assert_eq!(start(&app,&thread,"second-save",Kind::Save,"second").await["ok"],true);
    wait_state(&app,&thread,"second-save","choosing").await;dialog.answer(Ok(None)).await;
    wait_state(&app,&thread,"second-save","cancelled").await;app.shutdown().await;
}

fn receipt(manager:&Arc<MediaJobs>,thread:Thread,seq:u64,state:State,kind:Kind)->Arc<Job> {
    let mut media:Media=serde_json::from_value(json!({"kind":"document"})).unwrap();media.path=None;
    Arc::new(Job{seq,plugin:"media_fixture".into(),conversation:0,row:0,fingerprint:Mutex::new(Some(media)),
        snapshot:Mutex::new(Snapshot{id:format!("{}-{seq}",manager.session),session_id:manager.session.clone(),request_key:format!("key-{seq}"),thread,message_id:"deleted-message".into(),kind,state,cancel_requested:false,done_bytes:None,total_bytes:None,error:None}),
        cancel:manager.stop.child_token(),wire:Arc::new(OnceLock::new()),phase:AtomicU8::new(0),manager:Arc::downgrade(manager)})
}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn full_receipt_ledger_never_evicts_unknown_keys_and_pages_stay_scoped_and_bounded() {
    let (app,thread)=fixture("good").await;
    {let mut l=lock(&app.media.ledger);for seq in 1..=MAX_KEYS as u64 {let j=receipt(&app.media,thread.clone(),seq,if seq==1{State::Unknown}else{State::Succeeded},Kind::Download);l.keys.insert(format!("key-{seq}"),seq);l.entries.insert(seq,j);}}
    let all=page(&app,&thread).await;assert_eq!(all["remainingRequestKeys"],0);assert_eq!(all["jobs"].as_array().unwrap().len(),16);
    assert_eq!(start(&app,&thread,"new-intent",Kind::Download,"message").await["error"]["code"],"conflict");
    // Receipt lookup precedes current message existence/capability checks.
    let old=start(&app,&thread,"key-1",Kind::Download,"deleted-message").await;
    assert_eq!(old["value"]["state"],"unknown");assert_eq!(old["value"]["id"],format!("{}-1",app.media.session));
    let next=call(&app,"listMediaJobs",json!({"thread":thread,"cursor":all["nextCursor"],"requestKey":null})).await;
    assert_ne!(next["value"]["jobs"][0]["id"],all["jobs"][0]["id"]);
    let foreign=Thread{chat_id:"other".into(),..thread.clone()};
    assert_eq!(call(&app,"listMediaJobs",json!({"thread":foreign,"cursor":all["nextCursor"],"requestKey":null})).await["ok"],false);
    assert!(trace(&app).is_empty());app.shutdown().await;
    // Maximum JSON escaping expansion fits the existing transport without paths.
    let (app,mut thread)=fixture("good").await;thread.chat_id="\u{1}".repeat(256);thread.topic_id=Some("\u{2}".repeat(256));
    {let mut l=lock(&app.media.ledger);for seq in 1..=17 {let j=receipt(&app.media,thread.clone(),seq,State::Succeeded,Kind::Download);lock(&j.snapshot).message_id="\u{3}".repeat(256);l.keys.insert(format!("key-{seq}"),seq);l.entries.insert(seq,j);}}
    let bytes=serde_json::to_vec(&page(&app,&thread).await).unwrap();assert!(bytes.len()<128*1024);
    app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn deleting_target_does_not_hide_or_disable_cancelling_its_receipt() {
    let (app,thread)=fixture("hold").await;start(&app,&thread,"delete",Kind::Download,"message").await;wait_rpc(&app).await;
    let t=thread.clone();app.read(move|db|{let c=ch_db::conversation_rowid(db.conn(),&t.account_id,&t.chat_id)?.unwrap();let row=ch_db::get_message(db.conn(),c,"message")?.unwrap();ch_db::delete_message(db.conn(),row.id)?;Ok(())}).await.unwrap();
    assert_eq!(job(&app,&thread,"delete").await["requestKey"],"delete");
    assert_eq!(cancel(&app,&thread,"delete").await["ok"],true);wait_state(&app,&thread,"delete","cancelled").await;
    assert_eq!(start(&app,&thread,"delete",Kind::Download,"message").await["value"]["state"],"cancelled");app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn missing_capability_foreign_topic_and_stale_session_never_dispatch_or_consume_receipts() {
    let (app,thread)=fixture("unsupported").await;
    assert_eq!(start(&app,&thread,"cap",Kind::Download,"message").await["error"]["code"],"unsupported");
    let foreign=Thread{topic_id:Some("topic".into()),..thread.clone()};
    assert_eq!(start(&app,&foreign,"topic",Kind::Download,"message").await["ok"],false);
    assert_eq!(call(&app,"startMediaDownload",json!({"thread":thread,"messageId":"message","sessionId":"old-session","requestKey":"stale"})).await["error"]["code"],"invalid_request");
    assert_eq!(page(&app,&thread).await["remainingRequestKeys"],MAX_KEYS);
    assert!(trace(&app).is_empty());app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn shutdown_does_not_wait_for_unclosable_chooser_or_allow_late_publish() {
    let (mut app,thread)=fixture("good").await;let dialog=Arc::new(HeldDialog::default());app.set_save_dialog(dialog.clone());
    start(&app,&thread,"exit",Kind::Save,"message").await;wait_state(&app,&thread,"exit","choosing").await;
    tokio::time::timeout(Duration::from_secs(5),app.shutdown()).await.unwrap();
    let dest=app.core.data_dir().join("after-exit.bin");dialog.answer(Ok(Some(dest.clone()))).await;
    assert!(!dest.exists());assert_eq!(start(&app,&thread,"later",Kind::Save,"second").await["ok"],false);
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn cancelled_then_uncertain_native_answer_still_holds_reservation() {
    let (mut app,thread)=fixture("good").await;let dialog=Arc::new(HeldDialog::default());app.set_save_dialog(dialog.clone());
    start(&app,&thread,"uncertain",Kind::Save,"message").await;wait_state(&app,&thread,"uncertain","choosing").await;
    cancel(&app,&thread,"uncertain").await;
    dialog.answer(Err(SaveDialogError::Uncertain)).await;
    let j=wait_state(&app,&thread,"uncertain","unknown").await;assert_eq!(j["error"]["outcomeUnknown"],true);
    assert_eq!(start(&app,&thread,"another",Kind::Save,"second").await["error"]["code"],"conflict");
    assert_eq!(dialog.calls.load(Ordering::Acquire),1);app.shutdown().await;
}

#[cfg(target_os="linux")]
struct FixedDialog(PathBuf);
#[cfg(target_os="linux")]
impl SaveDialog for FixedDialog {fn choose(&self,_:String,callback:SaveDialogCallback){callback(Ok(Some(self.0.clone())));}}
#[cfg(target_os="linux")]
#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn cached_save_works_offline_without_download_and_never_overwrites() {
    let (mut app,thread)=fixture("good").await;
    app.core.host().stop_account("media_fixture",&thread.account_id).await.unwrap();
    let dest=app.core.data_dir().join("saved-copy.bin");app.set_save_dialog(Arc::new(FixedDialog(dest.clone())));
    start(&app,&thread,"offline",Kind::Save,"message").await;
    wait_state(&app,&thread,"offline","succeeded").await;
    assert_eq!(std::fs::read(&dest).unwrap(),b"isolated attachment");assert!(trace(&app).is_empty());
    let original=std::fs::metadata(&dest).unwrap();
    start(&app,&thread,"no-overwrite",Kind::Save,"message").await;
    let j=wait_state(&app,&thread,"no-overwrite","failed").await;
    assert_eq!(j["error"]["reason"],"destinationExists");assert_eq!(std::fs::read(&dest).unwrap(),b"isolated attachment");
    use std::os::unix::fs::MetadataExt;
    assert_eq!(std::fs::metadata(&dest).unwrap().ino(),original.ino());app.shutdown().await;
}

#[cfg(target_os="linux")]
#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn actual_cache_descriptor_survives_waiter_loss_and_closed_core_rejects_new_opens() {
    use std::{io::Write,os::unix::fs::MetadataExt};
    struct PausedWriter {
        entered:Option<tokio::sync::oneshot::Sender<()>>,
        resume:std::sync::mpsc::Receiver<()>,
        bytes:Vec<u8>,
    }
    impl Write for PausedWriter {
        fn write(&mut self,bytes:&[u8])->std::io::Result<usize> {
            if let Some(entered)=self.entered.take() {
                let _=entered.send(());
                self.resume.recv().map_err(std::io::Error::other)?;
            }
            self.bytes.extend_from_slice(bytes);Ok(bytes.len())
        }
        fn flush(&mut self)->std::io::Result<()> {Ok(())}
    }
    let temp=tempfile::tempdir().unwrap();
    let root=temp.path().join("cache");std::fs::create_dir(&root).unwrap();
    let path=root.join("held.bin");std::fs::write(&path,b"isolated held cache bytes").unwrap();
    let meta=std::fs::metadata(&path).unwrap();
    let open_handles=||std::fs::read_dir("/proc/self/fd").unwrap().filter_map(|entry|
        std::fs::metadata(entry.ok()?.path()).ok())
        .filter(|m|m.dev()==meta.dev()&&m.ino()==meta.ino()).count();
    let mut config=CoreConfig::new(temp.path(),Arc::new(MemorySecretStore::new()));
    config.autostart=false;
    let core=Core::open(config).await.unwrap();
    let directory=(Arc::new(ch_core::runtime::DataDirectoryLock::acquire(temp.path()).unwrap()),None);
    let source=CacheRead::open(&core,directory.clone(),&root,&path).unwrap();
    assert_eq!(open_handles(),1);
    let (entered,ready)=tokio::sync::oneshot::channel();
    let (resume,paused)=std::sync::mpsc::channel();
    let (finished,done)=tokio::sync::oneshot::channel();
    let caller=tokio::spawn(async move {
        tokio::task::spawn_blocking(move|| {
            let mut source=source;
            let mut writer=PausedWriter {entered:Some(entered),resume:paused,bytes:vec![]};
            let result=source.file.copy_to(&mut writer,MAX_CACHE_FILE_BYTES,||false);
            drop(source);
            let _=finished.send((result,writer.bytes));
        }).await.unwrap();
    });
    tokio::time::timeout(Duration::from_secs(5),ready).await.unwrap().unwrap();
    caller.abort();assert!(caller.await.unwrap_err().is_cancelled());
    core.shutdown().await;
    assert_eq!(open_handles(),1);
    assert!(matches!(CacheRead::open(&core,directory.clone(),&root,&path),Err(CacheFileError::Cancelled)));
    assert!(matches!(CacheRead::open(&core,directory.clone(),&root,&root.join("missing")),Err(CacheFileError::Cancelled)));
    drop(directory);
    assert!(ch_core::runtime::DataDirectoryLock::acquire(temp.path()).is_err());
    resume.send(()).unwrap();
    let (copied,bytes)=tokio::time::timeout(Duration::from_secs(5),done).await.unwrap().unwrap();
    assert_eq!(copied.unwrap(),25);
    assert_eq!(bytes,b"isolated held cache bytes");
    assert_eq!(open_handles(),0);
    assert!(ch_core::runtime::DataDirectoryLock::acquire(temp.path()).is_ok());
}

#[cfg(target_os="linux")]
#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn replaced_real_directory_lock_refuses_cache_open_and_late_save_choice() {
    for after_choice in [false,true] {
        let (mut app,thread)=fixture("good").await;
        let dialog=Arc::new(HeldDialog::default());app.set_save_dialog(dialog.clone());
        if after_choice {
            start(&app,&thread,"stale-lock",Kind::Save,"message").await;
            wait_state(&app,&thread,"stale-lock","choosing").await;
        }
        let root=app.core.data_dir();
        std::fs::rename(root.join("app.lock"),root.join("previous.lock")).unwrap();
        let replacement=ch_core::runtime::DataDirectoryLock::acquire(root).unwrap();
        let dest=root.join("must-not-publish.bin");
        if after_choice {dialog.answer(Ok(Some(dest.clone()))).await;}
        else {start(&app,&thread,"stale-lock",Kind::Save,"message").await;}
        let result=wait_state(&app,&thread,"stale-lock","failed").await;
        assert_eq!(result["error"]["reason"],"changed");
        assert_eq!(dialog.calls.load(Ordering::Acquire),usize::from(after_choice));
        assert!(!dest.exists());assert!(trace(&app).is_empty());
        app.shutdown().await;drop(replacement);
    }
}

#[cfg(target_os="linux")]
#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn dropped_app_and_shutdown_waiter_keep_real_directory_lock_until_ingest_fd_closes() {
    let (app,thread)=fixture("good").await;let app=Arc::new(app);
    let _temporary=app._demo.clone().unwrap(); // Keep only the isolated directory, not its lock.
    let root=app.core.data_dir().to_owned();
    let path=app.core.host().config().cache_dir(&thread.account_id).join("cached.bin");
    let mutations=app.core.stage_mutations();
    let (entered,ready)=tokio::sync::oneshot::channel();let (resume,paused)=std::sync::mpsc::channel();
    let core=app.core.clone();
    let caller=tokio::spawn(async move {
        core.ingest().with_ingest(move|_| {
            let readers=mutations.read_scope().unwrap();let file=std::fs::File::open(path).unwrap();
            entered.send(()).unwrap();let _=paused.recv();
            assert_eq!(file.metadata().unwrap().len(),19);
            drop(file);drop(readers);
        }).await
    });
    tokio::time::timeout(Duration::from_secs(5),ready).await.unwrap().unwrap();
    caller.abort();assert!(caller.await.unwrap_err().is_cancelled());
    let closing=app.clone();let waiter=tokio::spawn(async move {closing.shutdown().await;});
    let stopped=app.shutdown_signal();
    tokio::time::timeout(Duration::from_secs(5),stopped.cancelled()).await.unwrap();
    waiter.abort();assert!(waiter.await.unwrap_err().is_cancelled());drop(app);
    assert!(ch_core::runtime::DataDirectoryLock::acquire(&root).is_err());
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10),async {
        loop {
            if let Ok(lock)=ch_core::runtime::DataDirectoryLock::acquire(&root) {drop(lock);break;}
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.unwrap();
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn changed_attachment_and_disabled_account_cannot_publish_late_selection() {
    for disabled in [false,true] {
        let (mut app,thread)=fixture("good").await;let dialog=Arc::new(HeldDialog::default());app.set_save_dialog(dialog.clone());
        start(&app,&thread,"change",Kind::Save,"message").await;wait_state(&app,&thread,"change","choosing").await;
        if disabled {
            app.core.accounts().set_enabled(&thread.account_id,false).await.unwrap();
            wait_state(&app,&thread,"change","cancelling").await;
        } else {
            let t=thread.clone();app.read(move|db|{let c=ch_db::conversation_rowid(db.conn(),&t.account_id,&t.chat_id)?.unwrap();let mut row=ch_db::get_message(db.conn(),c,"message")?.unwrap();row.media.as_mut().unwrap().mime=Some("changed-attachment".into());ch_db::update_message(db.conn(),&row)?;Ok(())}).await.unwrap();
        }
        let dest=app.core.data_dir().join("blocked-selection.bin");dialog.answer(Ok(Some(dest.clone()))).await;
        let j=wait_state(&app,&thread,"change",if disabled{"cancelled"}else{"failed"}).await;
        if !disabled {assert_eq!(j["error"]["reason"],"changed");}
        assert!(!dest.exists());app.shutdown().await;
    }
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn active_admission_and_queued_cancellation_bound_actual_rpc_count() {
    let (app,thread)=fixture("hold").await;
    let t=thread.clone();app.read(move|db|{let c=ch_db::conversation_rowid(db.conn(),&t.account_id,&t.chat_id)?.unwrap();let row=ch_db::get_message(db.conn(),c,"message")?.unwrap();for n in 0..9 {let mut r=row.clone();r.id=0;r.message_id=format!("item-{n}");ch_db::insert_message(db.conn(),&r)?;}Ok(())}).await.unwrap();
    for n in 0..8 {assert_eq!(start(&app,&thread,&format!("active-{n}"),Kind::Download,&format!("item-{n}")).await["ok"],true);}
    tokio::time::timeout(Duration::from_secs(5),async{while trace(&app).iter().filter(|r|r["method"]=="media.download").count()<2 {tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap();
    assert_eq!(start(&app,&thread,"overflow",Kind::Download,"item-8").await["error"]["code"],"conflict");
    let all=page(&app,&thread).await;let queued=all["jobs"].as_array().unwrap().iter().find(|j|j["state"]=="queued").unwrap()["requestKey"].as_str().unwrap().to_owned();
    cancel(&app,&thread,&queued).await;wait_state(&app,&thread,&queued,"cancelled").await;
    assert_eq!(trace(&app).iter().filter(|r|r["method"]=="media.download").count(),2,"cancelled queued work never dispatches");
    app.media.cancel_account(&thread.account_id);
    for n in 0..8 {wait_state(&app,&thread,&format!("active-{n}"),"cancelled").await;}
    app.shutdown().await;
}

#[cfg(target_os="linux")]
#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn genuinely_held_foreign_directory_lock_cannot_open_this_cores_cached_file() {
    use std::os::unix::fs::MetadataExt;
    use ch_core::runtime::DataDirectoryLock;
    let temp=tempfile::tempdir().unwrap();let cache=temp.path().join("cache");
    std::fs::create_dir(&cache).unwrap();let path=cache.join("held.bin");
    std::fs::write(&path,b"isolated root binding").unwrap();let meta=std::fs::metadata(&path).unwrap();
    let open_handles=||std::fs::read_dir("/proc/self/fd").unwrap().filter_map(|entry|
        std::fs::metadata(entry.ok()?.path()).ok())
        .filter(|m|m.dev()==meta.dev()&&m.ino()==meta.ino()).count();
    let mut config=CoreConfig::new(temp.path(),Arc::new(MemorySecretStore::new()));config.autostart=false;
    let core=Core::open(config).await.unwrap();
    let foreign=(Arc::new(DataDirectoryLock::acquire(&temp.path().join("foreign")).unwrap()),None);
    foreign.0.check_current().unwrap();
    assert!(matches!(CacheRead::open(&core,foreign.clone(),&cache,&path),Err(CacheFileError::Changed)));
    assert_eq!(open_handles(),0,"a genuine foreign lock never authorizes opening the cached byte FD");
    let directory=(Arc::new(DataDirectoryLock::acquire(temp.path()).unwrap()),None);
    let source=CacheRead::open(&core,directory,&cache,&path).unwrap();assert_eq!(open_handles(),1);
    drop(source);assert_eq!(open_handles(),0);foreign.0.check_current().unwrap();core.shutdown().await;
}
