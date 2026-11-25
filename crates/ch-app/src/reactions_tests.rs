//! Full app-wire regressions over an isolated, controllable stdlib plugin.
use super::*;
use ch_core::{host::MemorySecretStore,runtime::{CoreConfig,NewAccount}};
use ch_db::types::{ConversationRow,TopicRow};
use ch_protocol::Empty;
use std::path::Path;

async fn fixture(mode:&str)->(App,Thread){
    let temp=tempfile::tempdir().unwrap();let dir=temp.path().join("plugins/reaction_fixture");std::fs::create_dir_all(&dir).unwrap();
    let script=Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/reactions_plugin.py");
    std::fs::write(dir.join("plugin.json"),json!({"id":"reaction_fixture","name":"Reaction fixture","version":"0.0.1","api_version":"1.0.0","entry":["python3","-B","-s",script,mode],"brand_color":"#123456","multi_account":true,"auth_methods":["none"]}).to_string()).unwrap();
    let mut config=CoreConfig::new(temp.path(),Arc::new(MemorySecretStore::new()));
    config.host.env.insert("CH_REACTION_TRACE".into(),temp.path().join("trace.jsonl").to_string_lossy().into_owned());config.host.env.insert("CH_DATA_DIR".into(),temp.path().to_string_lossy().into_owned());
    for key in ["XDG_CONFIG_HOME","XDG_DATA_HOME","XDG_STATE_HOME","XDG_CACHE_HOME","XDG_RUNTIME_DIR"]{let path=temp.path().join(key);std::fs::create_dir(&path).unwrap();config.host.env.insert(key.into(),path.to_string_lossy().into_owned());}
    let app=App::open(config,Some(temp)).await.unwrap();let account=app.core.accounts().add(NewAccount::new("reaction_fixture")).await.unwrap().account_id;
    let thread=Thread{account_id:account.clone(),chat_id:"chat".into(),topic_id:None};
    app.read(move|db|{for chat in ["chat","other","c0","c1","c2","c3","c4"]{let row=ch_db::insert_conversation(db.conn(),&ConversationRow{account_id:account.clone(),chat_id:chat.into(),kind:"group".into(),..Default::default()})?;
        ch_db::insert_message(db.conn(),&MessageRow{conversation_id:row,message_id:"message".into(),direction:"in".into(),sent_at:1793491200000,..Default::default()})?;
        if chat=="chat"{ch_db::insert_topic(db.conn(),&TopicRow{conversation_id:row,topic_id:"topic".into(),title:"Topic".into(),..Default::default()})?;ch_db::insert_message(db.conn(),&MessageRow{conversation_id:row,message_id:"topic-message".into(),topic_id:Some("topic".into()),direction:"in".into(),sent_at:1793491200000,..Default::default()})?;}
    }Ok(())}).await.unwrap();(app,thread)
}
async fn request(app:&App,method:&str,params:Value)->Value{app.request(json!({"method":method,"params":params})).await}
async fn refresh(app:&App,t:&Thread)->Value{request(app,"refreshMessageReactions",json!({"thread":t,"messageId":"message"})).await}
async fn set(app:&App,t:&Thread,c:&Value,keys:Value)->Value{request(app,"setMessageReactions",json!({"thread":t,"messageId":"message","sessionId":c["sessionId"],"contextToken":c["contextToken"],"keys":keys})).await}
async fn who(app:&App,t:&Thread,c:&Value,key:Value,cursor:Value)->Value{request(app,"listMessageReactors",json!({"thread":t,"messageId":"message","sessionId":c["sessionId"],"contextToken":c["contextToken"],"key":key,"cursor":cursor})).await}
fn trace(app:&App)->Vec<Value>{std::fs::read_to_string(app.core.data_dir().join("trace.jsonl")).unwrap_or_default().lines().map(|l|serde_json::from_str(l).unwrap()).collect()}
fn calls(app:&App,method:&str)->usize{trace(app).iter().filter(|v|v["method"]==method).count()}
async fn baseline(app:&App,t:&Thread)->Vec<ReactionRow>{let t=t.clone();app.read(move|db|{let chat=ch_db::conversation_rowid(db.conn(),&t.account_id,&t.chat_id)?.unwrap();let row=ch_db::get_message(db.conn(),chat,"message")?.unwrap();Ok(ch_db::list_reactions(db.conn(),row.id)?)}).await.unwrap()}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn cached_unknowns_raw_precision_full_membership_and_paid_preservation(){
    let (app,t)=fixture("precision").await;
    let initial=request(&app,"history",json!({"thread":t,"window":{"kind":"latest"},"limit":20})).await;
    assert!(initial["value"]["items"][0]["message"]["reactions"].is_null());
    let c=refresh(&app,&t).await;assert_eq!(c["ok"],true);let c=&c["value"];
    assert_eq!(c["snapshot"]["items"][0]["count"],"18446744073709551615");assert_eq!(c["snapshot"]["items"][0]["mine"],false);
    assert_eq!(c["snapshot"]["items"].as_array().unwrap().len(),3,"paid metadata not an ordinary chip");
    let cached=request(&app,"history",json!({"thread":t,"window":{"kind":"latest"},"limit":20})).await;
    assert!(cached["value"]["items"][0]["message"]["reactions"]["items"][0]["count"].is_null());
    assert!(cached["value"]["items"][0]["message"]["reactions"]["items"][0]["mine"].is_null());
    let keys=json!([{"kind":"emoji","emoji":"❤️"},{"kind":"custom","id":"9007199254740993"}]);
    let result=set(&app,&t,c,keys.clone()).await;assert_eq!(result["ok"],true);assert_eq!(result["value"]["snapshot"]["items"][0]["mine"],true);
    assert_eq!(baseline(&app,&t).await.iter().find(|r|r.key=="paid").unwrap().count,5);
    let sent=trace(&app).into_iter().find(|f|f["method"]=="reactions.set").unwrap();
    assert_eq!(sent["params"]["reactions"],json!([{"emoji":"❤️"},{"custom_emoji_id":"9007199254740993"}]));assert_eq!(sent["params"]["big"],false);
    assert_eq!(set(&app,&t,c,keys).await["ok"],false);assert_eq!(calls(&app,"reactions.set"),1);app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn absent_empty_policy_unknowns_and_newest_proof_are_distinct(){
    for mode in ["sparse","empty","missing","policy-unavailable"]{let (app,t)=fixture(mode).await;let c=refresh(&app,&t).await;assert_eq!(c["ok"],true,"{mode}: {c}");let c=&c["value"];
        match mode{"empty"=>assert_eq!(c["snapshot"],json!({"items":[]})),"policy-unavailable"=>{assert!(c["availability"].is_null());assert_eq!(c["canSet"],false);},_=>{assert!(c["snapshot"].is_null());assert_eq!(c["canSet"],false);}}
        if mode=="missing"{assert!(c["contextToken"].is_null());}app.shutdown().await;
    }
    let (app,t)=fixture("good").await;let old=refresh(&app,&t).await["value"].clone();let current=refresh(&app,&t).await["value"].clone();assert_ne!(old["contextToken"],current["contextToken"]);
    assert_eq!(set(&app,&t,&old,json!([{"kind":"emoji","emoji":"❤️"}])).await["ok"],false);
    assert_eq!(set(&app,&t,&current,json!([{"kind":"emoji","emoji":"❤️"}])).await["ok"],true);assert_eq!(calls(&app,"reactions.set"),1);app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn optional_or_ambiguous_write_reply_consumes_proof_without_fake_confirmation(){
    for mode in ["write-missing","write-sparse","unknown","mismatch","write-wrong-chat","bad-key"]{let (app,t)=fixture(mode).await;let c=refresh(&app,&t).await["value"].clone();let result=set(&app,&t,&c,json!([{"kind":"emoji","emoji":"❤️"}])).await;
        if matches!(mode,"write-missing"|"write-sparse"){assert_eq!(result["ok"],true,"{mode}: {result}");assert!(result["value"]["snapshot"].is_null());}else{assert_eq!(result["error"]["outcomeUnknown"],true,"{mode}: {result}");}
        assert!(!result.to_string().contains("sensitive"));assert_eq!(set(&app,&t,&c,json!([{"kind":"emoji","emoji":"❤️"}])).await["ok"],false);assert_eq!(calls(&app,"reactions.set"),1);app.shutdown().await;
    }
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn reactor_pages_are_scoped_cached_and_keep_short_empty_continuations(){
    let (app,t)=fixture("good").await;let c=refresh(&app,&t).await["value"].clone();let first=who(&app,&t,&c,Value::Null,Value::Null).await;assert_eq!(first["ok"],true);
    let first=&first["value"];assert!(first["items"][0]["name"].as_str().unwrap().len()<=256);assert!(first["items"][0]["name"].as_str().unwrap().ends_with('…'));
    assert_ne!(first["nextCursor"],"A");assert_eq!(who(&app,&t,&c,Value::Null,Value::Null).await["value"],*first);assert_eq!(calls(&app,"reactions.who"),1);
    assert_eq!(who(&app,&t,&c,json!({"kind":"emoji","emoji":"❤️"}),first["nextCursor"].clone()).await["ok"],false);
    let second=who(&app,&t,&c,Value::Null,first["nextCursor"].clone()).await;assert_eq!(second["value"]["items"],json!([]));assert!(second["value"]["nextCursor"].is_string());
    let last=who(&app,&t,&c,Value::Null,second["value"]["nextCursor"].clone()).await;assert_eq!(last["value"]["items"][0]["key"]["kind"],"custom");assert!(last["value"]["nextCursor"].is_null());assert_eq!(calls(&app,"reactions.who"),3);app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn expiry_and_changed_baseline_or_scope_block_before_dispatch(){
    let (app,t)=fixture("good").await;let c=refresh(&app,&t).await["value"].clone();let foreign=Thread{chat_id:"other".into(),..t.clone()};
    assert_eq!(set(&app,&foreign,&c,json!([{"kind":"emoji","emoji":"❤️"}])).await["ok"],false);
    let id=c["contextToken"].as_str().unwrap();lock(&app.reactions.store).contexts.get_mut(id).unwrap().expires=Instant::now()-Duration::from_secs(1);
    assert_eq!(set(&app,&t,&c,json!([{"kind":"emoji","emoji":"❤️"}])).await["ok"],false);
    let c=refresh(&app,&t).await["value"].clone();let target=t.clone();app.read(move|db|{let conv=ch_db::conversation_rowid(db.conn(),&target.account_id,&target.chat_id)?.unwrap();let row=ch_db::get_message(db.conn(),conv,"message")?.unwrap();ch_db::set_reactions(db.conn(),row.id,&[])?;Ok(())}).await.unwrap();
    assert_eq!(set(&app,&t,&c,json!([{"kind":"emoji","emoji":"❤️"}])).await["ok"],false);assert_eq!(calls(&app,"reactions.set"),0);app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn custom_catalog_and_handshake_limits_are_independent_of_runtime_unknowns(){
    for mode in ["good","without-reactions.custom","max-default","cap-null","cap-zero"]{
        let (app,t)=fixture(mode).await;let c=refresh(&app,&t).await;assert_eq!(c["ok"],true,"{mode}: {c}");let c=&c["value"];
        if matches!(mode,"cap-null"|"cap-zero"){assert_eq!(c["canSet"],false);assert_eq!(set(&app,&t,c,json!([{"kind":"emoji","emoji":"❤️"}])).await["error"]["code"],"unsupported");}
        else if mode=="max-default"{assert!(c["availability"]["maxPerMessage"].is_null());assert_eq!(set(&app,&t,c,json!([{"kind":"emoji","emoji":"❤️"},{"kind":"emoji","emoji":"👍"}])).await["error"]["code"],"invalid_request");}
        else{assert_eq!(c["availability"]["allowCustom"],false);let result=set(&app,&t,c,json!([{"kind":"custom","id":"9007199254740993"}])).await;if mode=="good"{assert_eq!(result["ok"],true);}else{assert_eq!(result["error"]["code"],"unsupported");}}
        assert_eq!(calls(&app,"reactions.set"),usize::from(mode=="good"));app.shutdown().await;
    }
    let (app,t)=fixture("unrestricted").await;let c=refresh(&app,&t).await["value"].clone();assert!(c["availability"]["keys"].is_null());
    assert_eq!(set(&app,&t,&c,json!([{"kind":"custom","id":"unlisted"}])).await["ok"],false);assert_eq!(calls(&app,"reactions.set"),0);app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn clear_known_selection_works_with_empty_policy_but_noop_and_paid_inputs_do_not_dispatch(){
    let (app,t)=fixture("policy-empty").await;let c=refresh(&app,&t).await["value"].clone();assert_eq!(c["availability"]["keys"],json!([]));assert_eq!(c["availability"]["maxPerMessage"],"0");
    assert_eq!(set(&app,&t,&c,json!([])).await["ok"],true);let raw=trace(&app).into_iter().find(|v|v["method"]=="reactions.set").unwrap();assert_eq!(raw["params"]["reactions"],json!([]));assert_eq!(baseline(&app,&t).await.iter().find(|r|r.key=="paid").unwrap().count,5);app.shutdown().await;
    let (app,t)=fixture("good").await;let c=refresh(&app,&t).await["value"].clone();
    assert_eq!(set(&app,&t,&c,json!([{"kind":"paid","paid":true}])).await["error"]["code"],"invalid_request");
    assert_eq!(set(&app,&t,&c,json!([])).await["ok"],false);
    assert_eq!(set(&app,&t,&c,json!([{"kind":"emoji","emoji":"❤️"}])).await["ok"],false,"a no-op consumed its one-use proof");assert_eq!(calls(&app,"reactions.set"),0);app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn reactor_attempt_budget_and_cycles_never_fake_end_or_replay_requests(){
    let (app,t)=fixture("infinite").await;let c=refresh(&app,&t).await["value"].clone();let mut cursor=Value::Null;
    for _ in 0..8{let result=who(&app,&t,&c,Value::Null,cursor).await;assert_eq!(result["ok"],true);assert_eq!(result["value"]["items"],json!([]));cursor=result["value"]["nextCursor"].clone();assert!(cursor.is_string());}
    assert_eq!(who(&app,&t,&c,Value::Null,cursor).await["error"]["code"],"conflict");assert_eq!(calls(&app,"reactions.who"),8);app.shutdown().await;
    let (app,t)=fixture("cycle").await;let c=refresh(&app,&t).await["value"].clone();let a=who(&app,&t,&c,Value::Null,Value::Null).await;let b=who(&app,&t,&c,Value::Null,a["value"]["nextCursor"].clone()).await;let cursor=b["value"]["nextCursor"].clone();
    assert_eq!(who(&app,&t,&c,Value::Null,cursor.clone()).await["ok"],false);assert_eq!(who(&app,&t,&c,Value::Null,cursor).await["ok"],false);assert_eq!(calls(&app,"reactions.who"),3);app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn contexts_and_page_receipts_are_finite_and_account_or_message_invalidation_rejects_old_proofs(){
    let (app,t)=fixture("good").await;let c=refresh(&app,&t).await["value"].clone();let id=c["contextToken"].as_str().unwrap().to_owned();
    {let mut store=lock(&app.reactions.store);let seed=store.contexts[&id].clone();store.contexts.clear();for n in 0..64{let mut item=seed.clone();item.target.id=format!("holder-{n}");store.contexts.insert(format!("context-{n}"),item);}}
    assert_eq!(refresh(&app,&t).await["error"]["code"],"conflict");assert_eq!(lock(&app.reactions.store).contexts.len(),64);
    {let mut store=lock(&app.reactions.store);for context in store.contexts.values_mut(){context.expires=Instant::now()-Duration::from_secs(1);}}
    let c=refresh(&app,&t).await["value"].clone();assert!(c["contextToken"].is_string());
    {let id=c["contextToken"].as_str().unwrap();let mut store=lock(&app.reactions.store);for n in 0..128{store.pages.insert(format!("page-{n}"),PageReceipt{context:id.into(),key:Some(Key::Emoji{emoji:format!("item-{n}")}),native:Some("offset".into()),reply:None,busy:false});}}
    assert_eq!(who(&app,&t,&c,Value::Null,Value::Null).await["error"]["code"],"conflict");assert_eq!(calls(&app,"reactions.who"),0);
    app.core.accounts().set_enabled(&t.account_id,false).await.unwrap();
    assert_eq!(set(&app,&t,&c,json!([{"kind":"emoji","emoji":"❤️"}])).await["ok"],false);assert_eq!(calls(&app,"reactions.set"),0);app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn dropped_callers_keep_rpc_thread_guards_and_four_task_quota_until_settlement(){
    let (app,base)=fixture("hold-set").await;let app=Arc::new(app);let mut tasks=Vec::new();
    for n in 0..4{let t=Thread{chat_id:format!("c{n}"),..base.clone()};let c=refresh(&app,&t).await["value"].clone();let owner=app.clone();tasks.push(tokio::spawn(async move{set(&owner,&t,&c,json!([{"kind":"emoji","emoji":"❤️"}])).await}));}
    tokio::time::timeout(Duration::from_secs(5),async{while calls(&app,"reactions.set")<4{tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap();
    for task in tasks{task.abort();let _=task.await;}
    let fifth=Thread{chat_id:"c4".into(),..base.clone()};assert_eq!(refresh(&app,&fifth).await["error"]["code"],"conflict");assert_eq!(app.reactions.slots.available_permits(),0);
    let _:Empty=app.core.host().call("reaction_fixture","x-reaction_fixture.release",&json!({})).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5),async{while app.reactions.slots.available_permits()!=4{tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap();
    for n in 0..4{let t=Thread{chat_id:format!("c{n}"),..base.clone()};assert!(baseline(&app,&t).await.iter().any(|r|r.key=="e:❤️"&&r.mine));}
    assert_eq!(refresh(&app,&fifth).await["ok"],true);assert_eq!(calls(&app,"reactions.set"),4);app.shutdown().await;
}

#[test]
fn bounded_payloads_and_strict_keys_do_not_create_partial_proofs(){
    let malformed=json!({"message":{"reactions":[{"key":{"emoji":"❤️","custom_emoji_id":"7"}}]}});assert!(wire_message(&malformed).is_err());
    assert!(serde_json::from_value::<Key>(json!({"kind":"emoji","emoji":"❤️","id":"7"})).is_err());
    let rows:Vec<_>=(0..129).map(|n|Reaction{key:ReactionKey::custom(n.to_string()),count:0,mine:false,my_count:None}).collect();assert!(fresh_snapshot(Some(&rows)).is_err());
    assert!(bounded(json!({"oversize":"x".repeat(BUDGET)})).is_err());
}

#[test]
fn raw_paid_only_arrays_obey_complete_snapshot_and_policy_bounds(){
    let paid=Reaction{key:ReactionKey::paid(),count:7,mine:false,my_count:Some(0)};
    assert!(matches!(fresh_snapshot(Some(&vec![paid.clone();129])),Err(Failure::Unsupported)));
    assert!(matches!(fresh_snapshot(Some(&[paid.clone(),paid])),Err(Failure::Internal)),"duplicate full paid rows cannot be summed or ingested");
    let policy=ReactionsAvailableResult{reactions:Some(vec![ReactionKey::paid();257]),allow_custom:false,max_per_message:None};
    assert!(matches!(Policy::from_result(policy),Err(Failure::Unsupported)));
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn missing_refresh_supersedes_older_write_proof_and_reactor_cursors(){
    let (app,t)=fixture("good").await;let old=refresh(&app,&t).await["value"].clone();let first=who(&app,&t,&old,Value::Null,Value::Null).await;
    let _:Empty=app.core.host().call("reaction_fixture","x-reaction_fixture.missing",&json!({})).await.unwrap();
    let missing=refresh(&app,&t).await;assert_eq!(missing["ok"],true);assert!(missing["value"]["contextToken"].is_null());
    assert_eq!(set(&app,&t,&old,json!([{"kind":"emoji","emoji":"❤️"}])).await["ok"],false);
    assert_eq!(who(&app,&t,&old,Value::Null,first["value"]["nextCursor"].clone()).await["ok"],false);
    assert_eq!(calls(&app,"reactions.set"),0);assert_eq!(calls(&app,"reactions.who"),1);app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn exact_topic_and_returned_identity_are_checked_before_any_effect(){
    let (app,t)=fixture("read-wrong-chat").await;assert_eq!(refresh(&app,&t).await["ok"],false);assert!(lock(&app.reactions.store).contexts.is_empty());assert!(baseline(&app,&t).await.is_empty());app.shutdown().await;
    let (app,t)=fixture("good").await;
    assert_eq!(request(&app,"refreshMessageReactions",json!({"thread":t,"messageId":"topic-message"})).await["ok"],false);assert_eq!(calls(&app,"messages.get"),0);
    let topic=Thread{topic_id:Some("topic".into()),..t.clone()};
    let c=request(&app,"refreshMessageReactions",json!({"thread":topic,"messageId":"topic-message"})).await;assert_eq!(c["ok"],true);let c=&c["value"];
    assert_eq!(request(&app,"setMessageReactions",json!({"thread":topic,"messageId":"topic-message","sessionId":c["sessionId"],"contextToken":c["contextToken"],"keys":[{"kind":"emoji","emoji":"❤️"}]})).await["ok"],true);
    let sent=trace(&app).into_iter().find(|v|v["method"]=="reactions.set").unwrap();assert_eq!(sent["params"]["chat_id"],"chat");assert_eq!(sent["params"]["message_id"],"topic-message");app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn frozen_method_gates_keep_custom_metadata_reads_independent_of_custom_write_cap(){
    let (app,t)=fixture("without-messages.get").await;assert_eq!(refresh(&app,&t).await["error"]["code"],"unsupported");assert!(trace(&app).is_empty());app.shutdown().await;
    let (app,t)=fixture("without-reactions.set").await;let c=refresh(&app,&t).await["value"].clone();assert_eq!(c["canSet"],false);assert!(c["availability"].is_null());assert_eq!(calls(&app,"reactions.available"),0);
    assert_eq!(set(&app,&t,&c,json!([{"kind":"emoji","emoji":"❤️"}])).await["error"]["code"],"unsupported");assert_eq!(calls(&app,"reactions.set"),0);app.shutdown().await;
    let (app,t)=fixture("without-reactions.who").await;let c=refresh(&app,&t).await["value"].clone();assert_eq!(who(&app,&t,&c,Value::Null,Value::Null).await["error"]["code"],"unsupported");assert_eq!(calls(&app,"reactions.who"),0);app.shutdown().await;
    let (app,t)=fixture("without-reactions.custom").await;let c=refresh(&app,&t).await["value"].clone();let result=who(&app,&t,&c,json!({"kind":"custom","id":"9007199254740993"}),Value::Null).await;
    assert_eq!(result["ok"],true);assert_eq!(result["value"]["items"][0]["key"]["kind"],"custom");assert_eq!(calls(&app,"reactions.set"),0);app.shutdown().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn foreign_session_and_recreated_message_cannot_reuse_an_old_read_proof(){
    let (app,t)=fixture("good").await;let c=refresh(&app,&t).await["value"].clone();
    let mut foreign=c.clone();foreign["sessionId"]=json!("old-session");assert_eq!(set(&app,&t,&foreign,json!([{"kind":"emoji","emoji":"❤️"}])).await["error"]["code"],"invalid_request");
    let target=t.clone();app.read(move|db|{let conv=ch_db::conversation_rowid(db.conn(),&target.account_id,&target.chat_id)?.unwrap();let mut row=ch_db::get_message(db.conn(),conv,"message")?.unwrap();ch_db::delete_message(db.conn(),row.id)?;row.id=0;ch_db::insert_message(db.conn(),&row)?;Ok(())}).await.unwrap();
    assert_eq!(set(&app,&t,&c,json!([{"kind":"emoji","emoji":"❤️"}])).await["error"]["code"],"not_found");assert_eq!(calls(&app,"reactions.set"),0);app.shutdown().await;
}
