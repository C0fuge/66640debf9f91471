//! Scoped ordinary membership, never personal chosen-order or paid mutations.
use crate::{App, events, wire::{Failure, Result, Thread}};
use ch_core::{host::{AccountPhase, HostError}, runtime::Core};
use ch_db::{Db, types::{AccountRow, MessageRow, ReactionRow}};
use ch_protocol::{ErrorCode, capabilities::{Capabilities, Flag}, common::is_valid_id,
    dto::{Message, Reaction, ReactionKey}, methods::{MessagesGetParams, OptionalMessageResult,
        ReactionsAvailableParams, ReactionsAvailableResult, ReactionsSetParams, ReactionsWhoParams, ReactionsWhoResult}};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{collections::{BTreeMap, BTreeSet}, sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}}, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use tokio::{sync::{Semaphore, broadcast, oneshot}, task::JoinHandle};
use tokio_util::sync::CancellationToken;

const CONTEXTS:usize=64;
const PAGES:usize=128;
const TTL:Duration=Duration::from_secs(60);
const BUDGET:usize=128*1024;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(tag="kind", rename_all="camelCase", deny_unknown_fields)]
pub(crate) enum Key { Emoji { emoji:String }, Custom { id:String } }
impl Key {
    fn valid(&self)->bool {match self {Self::Emoji{emoji}=>!emoji.is_empty()&&emoji.len()<=128,Self::Custom{id}=>is_valid_id(id)}}
    fn protocol(&self)->ReactionKey {match self {Self::Emoji{emoji}=>ReactionKey::emoji(emoji),Self::Custom{id}=>ReactionKey::custom(id)}}
    fn from_protocol(key:&ReactionKey)->Result<Option<Self>> {
        let key=match key {ReactionKey::Emoji{emoji}=>Self::Emoji{emoji:emoji.clone()},ReactionKey::CustomEmoji{custom_emoji_id}=>Self::Custom{id:custom_emoji_id.clone()},ReactionKey::Paid{paid:true}=>return Ok(None),_=>return Err(Failure::Internal)};
        if !key.valid(){return Err(Failure::Internal);}Ok(Some(key))
    }
}
#[derive(Clone)]
struct Policy {keys:Option<Vec<Key>>,allow_custom:bool,max:Option<u64>}
impl Policy {fn project(&self)->Value{json!({"keys":self.keys,"allowCustom":self.allow_custom,"maxPerMessage":self.max.map(|m|m.to_string())})}}
#[derive(Clone)]
struct Target {owner:AccountRow,thread:Thread,id:String,row:i64,conversation:i64}
#[derive(Clone)]
struct Context {
    target:Target,expires:Instant,selected:Option<BTreeSet<Key>>,policy:Option<Policy>,baseline:Vec<ReactionRow>,
    busy:bool,attempts:usize,rows:usize,seen:BTreeSet<(Option<Key>,String)>,
}
#[derive(Clone)]
struct PageReceipt {context:String,key:Option<Key>,native:Option<String>,reply:Option<Result<Value>>,busy:bool}
type Lease=(tokio::sync::OwnedMutexGuard<()>,tokio::sync::OwnedSemaphorePermit);
struct Store {contexts:BTreeMap<String,Context>,pages:BTreeMap<String,PageReceipt>,epoch:u64,quarantine:Vec<Lease>}
pub(crate) struct Service {
    session:String,store:Arc<Mutex<Store>>,slots:Arc<Semaphore>,closed:Arc<AtomicBool>,stop:CancellationToken,tasks:Mutex<Vec<JoinHandle<()>>>,
}
fn lock<T>(m:&Mutex<T>)->std::sync::MutexGuard<'_,T>{m.lock().unwrap_or_else(|e|e.into_inner())}
fn random()->std::io::Result<String>{let mut bytes=[0u8;16];getrandom::fill(&mut bytes).map_err(|_|std::io::Error::other("Could not initialize reaction state."))?;Ok(bytes.iter().map(|b|format!("{b:02x}")).collect())}
fn token()->Result<String>{random().map_err(|_|Failure::Internal)}
fn bounded(value:Value)->Result<Value>{if serde_json::to_vec(&value).map_err(|_|Failure::Internal)?.len()>BUDGET{Err(Failure::Unsupported)}else{Ok(value)}}
fn expires_at()->String{let now=SystemTime::now().duration_since(UNIX_EPOCH).map_or_else(|e|-(e.duration().as_millis() as i128),|d|d.as_millis() as i128);(now+TTL.as_millis() as i128).to_string()}
impl Store {
    fn prune(&mut self){let now=Instant::now();self.contexts.retain(|_,c|c.busy||c.expires>now);let contexts=&self.contexts;self.pages.retain(|_,p|contexts.contains_key(&p.context));}
    fn remove(&mut self,id:&str){self.contexts.remove(id);self.pages.retain(|_,p|p.context!=id);}
    fn supersede(&mut self,target:&Target){let old:Vec<_>=self.contexts.iter().filter(|(_,c)|c.target.thread==target.thread&&c.target.id==target.id).map(|(id,_)|id.clone()).collect();for id in old{self.remove(&id);}}
}
impl Service {
    pub(crate) fn new()->std::io::Result<Self>{Ok(Self{session:random()?,store:Arc::new(Mutex::new(Store{contexts:BTreeMap::new(),pages:BTreeMap::new(),epoch:0,quarantine:Vec::new()})),slots:Arc::new(Semaphore::new(4)),closed:Arc::new(AtomicBool::new(false)),stop:CancellationToken::new(),tasks:Mutex::new(Vec::new())})}
    pub(crate) fn invalidate_account(&self,id:&str){let mut s=lock(&self.store);s.epoch=s.epoch.wrapping_add(1);s.contexts.retain(|_,c|c.target.thread.account_id!=id);let live:BTreeSet<_>=s.contexts.keys().cloned().collect();s.pages.retain(|_,p|live.contains(&p.context));}
    pub(crate) fn invalidate_plugin(&self,id:&str){let mut s=lock(&self.store);s.epoch=s.epoch.wrapping_add(1);s.contexts.retain(|_,c|c.target.owner.plugin_id!=id);let live:BTreeSet<_>=s.contexts.keys().cloned().collect();s.pages.retain(|_,p|live.contains(&p.context));}
    pub(crate) fn invalidate_all(&self){let mut s=lock(&self.store);s.epoch=s.epoch.wrapping_add(1);s.contexts.clear();s.pages.clear();}
    pub(crate) fn close(&self){self.closed.store(true,Ordering::Release);self.stop.cancel();self.invalidate_all();}
    pub(crate) async fn drain(&self){let tasks=std::mem::take(&mut *lock(&self.tasks));let deadline=tokio::time::Instant::now()+Duration::from_secs(2);for mut task in tasks{if tokio::time::timeout_at(deadline,&mut task).await.is_err(){task.abort();}}lock(&self.store).quarantine.clear();}
}

pub(crate) enum Operation { Refresh, Set {session:String,context:String,keys:Vec<Key>}, Who {session:String,context:String,key:Option<Key>,cursor:Option<String>} }
impl App {
    pub(crate) async fn reaction_operation(&self,thread:Thread,id:String,op:Operation)->Result<Value>{
        let permit=self.reactions.slots.clone().try_acquire_owned().map_err(|_|Failure::Conflict)?;
        let guard=self.action_lock(&thread).await?;
        let (owner,rows)=self.action_targets(&thread,std::slice::from_ref(&id)).await?;
        let row=&rows[0];if row.deleted_at.is_some()||row.is_scheduled{return Err(Failure::Conflict);}
        let target=Target{owner,thread:thread.clone(),id,row:row.id,conversation:row.conversation_id};
        let write=matches!(op,Operation::Set{..});
        let worker=Worker{core:self.core.clone(),db:self.db.clone(),changes:self.changes.clone(),store:self.reactions.store.clone(),closed:self.reactions.closed.clone(),cancel:self.reactions.stop.child_token(),session:self.reactions.session.clone(),target,epoch:lock(&self.reactions.store).epoch};
        let (tx,rx)=oneshot::channel();
        let task=tokio::spawn(async move{
            use futures_util::FutureExt;
            let account=worker.target.thread.account_id.clone();let changes=worker.changes.clone();let state=worker.store.clone();
            let result=match std::panic::AssertUnwindSafe(worker.run(op)).catch_unwind().await{
                Ok(result)=>{drop(guard);drop(permit);result},
                // A panic may have dropped an in-flight host future. Quarantine
                // its bounded lease until host shutdown proves it has settled.
                Err(_)=>{lock(&state).quarantine.push((guard,permit));Err(if write{Failure::OutcomeUnknown}else{Failure::Internal})},
            };
            if matches!(&result,Err(Failure::OutcomeUnknown)){events::notify(&changes,&account);}
            let _=tx.send(result);
        });
        {let mut tasks=lock(&self.reactions.tasks);tasks.retain(|t|!t.is_finished());tasks.push(task);}
        rx.await.map_err(|_|if write{Failure::OutcomeUnknown}else{Failure::Unavailable})?
    }
    pub(crate) async fn project_reaction_history(&self,page:ch_core::history::Page)->Result<Value>{
        self.read(move|db|{
            let mut cached=BTreeMap::new();
            for message in page.messages(){cached.insert(message.id.to_string(),cached_snapshot(db,message)?);}
            let mut value=crate::projection::history(page);
            if let Some(items)=value["items"].as_array_mut(){for item in items{if let Some(message)=item.get_mut("message"){let key=message["rowKey"].as_str().unwrap_or("");let reactions=cached.get(key).cloned().unwrap_or(Value::Null);message["reactions"]=reactions;}}}
            Ok(value)
        }).await
    }
}

struct Worker {core:Arc<Core>,db:Arc<Mutex<Db>>,changes:broadcast::Sender<events::Invalidation>,store:Arc<Mutex<Store>>,closed:Arc<AtomicBool>,cancel:CancellationToken,session:String,target:Target,epoch:u64}
impl Worker {
    fn caps(&self)->Result<Arc<Capabilities>>{
        if self.closed.load(Ordering::Acquire)||self.cancel.is_cancelled(){return Err(Failure::Unavailable);}
        if !self.core.host().account(&self.target.owner.plugin_id,&self.target.thread.account_id).is_some_and(|a|matches!(a.phase,AccountPhase::Running)){return Err(Failure::Unavailable);}
        self.core.host().plugin_state(&self.target.owner.plugin_id).and_then(|s|s.capabilities().cloned()).ok_or(Failure::Unavailable)
    }
    fn checked(db:&Db,target:&Target)->Result<MessageRow>{
        let owner=ch_db::get_account(db.conn(),&target.thread.account_id)?.ok_or(Failure::Missing)?;
        if !owner.enabled||owner.plugin_id!=target.owner.plugin_id{return Err(Failure::Unavailable);}
        let conv=ch_db::get_conversation(db.conn(),&target.thread.account_id,&target.thread.chat_id)?.filter(|c|!c.is_gone&&c.id==target.conversation).ok_or(Failure::Missing)?;
        if let Some(topic)=&target.thread.topic_id{ch_db::get_topic(db.conn(),conv.id,topic)?.filter(|t|!t.is_gone).ok_or(Failure::Missing)?;}
        let row=ch_db::get_message(db.conn(),conv.id,&target.id)?.filter(|r|r.id==target.row&&r.topic_id==target.thread.topic_id).ok_or(Failure::Missing)?;
        if row.deleted_at.is_some()||row.is_scheduled||row.message_id.starts_with("local:")||matches!(row.delivery.as_deref(),Some("sending"|"failed")){return Err(Failure::Conflict);}Ok(row)
    }
    async fn check(&self)->Result<Vec<ReactionRow>>{let target=self.target.clone();let db=self.db.clone();crate::projection::blocking(move||{Self::checked(&lock(&db),&target)?;Ok(ch_db::list_reactions(lock(&db).conn(),target.row)?)}).await}
    async fn call<P:Serialize,R:DeserializeOwned>(&self,method:&str,params:&P,write:bool)->Result<(R,Value)>{
        let raw=self.core.host().call_raw(&self.target.owner.plugin_id,method,serde_json::to_value(params).map_err(|_|Failure::Invalid)?,Some(self.cancel.clone())).await.map_err(|error|if write&&!definite(&error){Failure::OutcomeUnknown}else{Failure::from(&error)})?;
        let decoded=serde_json::from_value(raw.clone()).map_err(|_|if write{Failure::OutcomeUnknown}else{Failure::Internal})?;Ok((decoded,raw))
    }
    fn match_message(&self,message:&mut Message)->Result<()>{if message.id!=self.target.id||message.chat_id!=self.target.thread.chat_id||message.topic_id.as_ref().is_some_and(|t|Some(t)!=self.target.thread.topic_id.as_ref()){return Err(Failure::Internal);}message.topic_id.clone_from(&self.target.thread.topic_id);Ok(())}
    async fn apply(&self,message:Message)->Result<Vec<ReactionRow>>{
        let target=self.target.clone();let closed=self.closed.clone();
        let result=self.core.ingest().with_ingest(move|ing|->Result<_>{if closed.load(Ordering::Acquire){return Err(Failure::Unavailable);}Self::checked(ing.db(),&target)?;let applied=ing.apply_edited(&target.owner.plugin_id,&target.thread.account_id,&message).map_err(|_|Failure::Internal)?;let baseline=ch_db::list_reactions(ing.db().conn(),target.row)?;Ok((applied,baseline))}).await?;
        let (applied,baseline)=result?;self.core.bus().publish_local("reaction-actions",applied);Ok(baseline)
    }
    fn context(&self,store:&Store,id:&str,session:&str)->Result<Context>{
        if session!=self.session||!is_valid_id(id){return Err(Failure::Invalid);}
        let c=store.contexts.get(id).filter(|c|c.expires>Instant::now()).ok_or(Failure::Conflict)?;
        if c.target.thread!=self.target.thread||c.target.id!=self.target.id||c.target.row!=self.target.row||c.target.owner.plugin_id!=self.target.owner.plugin_id{return Err(Failure::Missing);}Ok(c.clone())
    }
    async fn run(self,op:Operation)->Result<Value>{match op{Operation::Refresh=>self.refresh().await,Operation::Set{session,context,keys}=>self.set(&session,&context,keys).await,Operation::Who{session,context,key,cursor}=>self.who(&session,&context,key,cursor).await}}
    async fn refresh(&self)->Result<Value>{
        let caps=self.caps()?;if !caps.has("messages.get"){return Err(Failure::Unsupported);}self.check().await?;
        let get=MessagesGetParams{account_id:self.target.thread.account_id.clone(),chat_id:self.target.thread.chat_id.clone(),message_id:self.target.id.clone()};
        let available=ReactionsAvailableParams{account_id:self.target.thread.account_id.clone(),chat_id:Some(self.target.thread.chat_id.clone())};
        let read=self.call::<_,OptionalMessageResult>("messages.get",&get,false);
        let policy=async{if !caps.has("reactions.set"){return None;}let (p,raw)=self.call::<_,ReactionsAvailableResult>("reactions.available",&available,false).await.ok()?;wire_keys(&raw,"reactions",false).ok()?;Policy::from_result(p).ok()};
        let (result,policy)=tokio::join!(read,policy);let (result,raw)=result?;
        wire_message(&raw)?;
        let Some(mut message)=result.message else{
            self.check().await?;self.caps()?;
            let answer=bounded(json!({"sessionId":self.session,"contextToken":null,"expiresAt":null,"snapshot":null,"availability":policy.as_ref().map(Policy::project),"canSet":false}))?;
            let mut s=lock(&self.store);s.prune();if self.closed.load(Ordering::Acquire)||s.epoch!=self.epoch{return Err(Failure::Conflict);}s.supersede(&self.target);return Ok(answer);
        };
        self.match_message(&mut message)?;let (snapshot,selected)=fresh_snapshot(message.reactions.as_deref())?;
        let baseline=self.apply(message).await?;self.check().await?;let live_caps=self.caps()?;
        let can_set=selected.is_some()&&policy.is_some()&&handshake_limit(&live_caps).is_ok();let id=token()?;
        let answer=bounded(json!({"sessionId":self.session,"contextToken":id,"expiresAt":expires_at(),"snapshot":snapshot,"availability":policy.as_ref().map(Policy::project),"canSet":can_set}))?;
        let mut s=lock(&self.store);s.prune();if self.closed.load(Ordering::Acquire)||s.epoch!=self.epoch{return Err(Failure::Conflict);}
        s.supersede(&self.target);
        if s.contexts.len()>=CONTEXTS{return Err(Failure::Conflict);}if s.contexts.contains_key(&id){return Err(Failure::Internal);}
        s.contexts.insert(id,Context{target:self.target.clone(),expires:Instant::now()+TTL,selected,policy,baseline,busy:false,attempts:0,rows:0,seen:BTreeSet::new()});Ok(answer)
    }
    async fn set(&self,session:&str,id:&str,keys:Vec<Key>)->Result<Value>{
        let caps=self.caps()?;
        let context={let mut s=lock(&self.store);s.prune();let context=self.context(&s,id,session)?;if context.busy{return Err(Failure::Conflict);}s.remove(id);context};
        let selected=context.selected.as_ref().ok_or(Failure::Conflict)?;let policy=context.policy.as_ref().ok_or(Failure::Unsupported)?;
        let requested:BTreeSet<_>=keys.iter().cloned().collect();
        if keys.len()>16||requested.len()!=keys.len()||keys.iter().any(|k|!k.valid()){return Err(Failure::Invalid);}
        if &requested==selected{return Err(Failure::Conflict);}
        if !caps.has("reactions.set")||keys.iter().any(|k|matches!(k,Key::Custom{..})&&!caps.has("reactions.custom")){return Err(Failure::Unsupported);}
        let cap_limit=Some(handshake_limit(&caps)?);
        if [cap_limit,policy.max].into_iter().flatten().any(|m|keys.len() as u64>m){return Err(Failure::Invalid);}
        if keys.iter().any(|key|match &policy.keys{Some(allowed)=>!allowed.contains(key),None=>matches!(key,Key::Custom{..})}){return Err(Failure::Invalid);}
        if self.check().await?!=context.baseline{return Err(Failure::Conflict);}
        let params=ReactionsSetParams{account_id:self.target.thread.account_id.clone(),chat_id:self.target.thread.chat_id.clone(),message_id:self.target.id.clone(),reactions:keys.iter().map(Key::protocol).collect(),big:Some(false)};
        let (reply,raw)=self.call::<_,OptionalMessageResult>("reactions.set",&params,true).await?;
        wire_message(&raw).map_err(|_|Failure::OutcomeUnknown)?;
        let Some(mut message)=reply.message else{events::notify(&self.changes,&self.target.thread.account_id);return Ok(json!({"snapshot":null}));};
        self.match_message(&mut message).map_err(|_|Failure::OutcomeUnknown)?;
        let (snapshot,confirmed)=fresh_snapshot(message.reactions.as_deref()).map_err(|_|Failure::OutcomeUnknown)?;
        if confirmed.as_ref().is_some_and(|c|c!=&requested){return Err(Failure::OutcomeUnknown);}
        let answer=bounded(json!({"snapshot":snapshot})).map_err(|_|Failure::OutcomeUnknown)?;
        self.apply(message).await.map_err(|_|Failure::OutcomeUnknown)?;Ok(answer)
    }
    async fn who(&self,session:&str,id:&str,key:Option<Key>,cursor:Option<String>)->Result<Value>{
        if key.as_ref().is_some_and(|k|!k.valid())||cursor.as_deref().is_some_and(|c|!is_valid_id(c)){return Err(Failure::Invalid);}
        if !self.caps()?.has("reactions.who"){return Err(Failure::Unsupported);}self.check().await?;
        let (page_id,native)={let mut s=lock(&self.store);s.prune();let c=self.context(&s,id,session)?;
            let existing=if let Some(cursor)=&cursor{let p=s.pages.get(cursor).filter(|p|p.context==id&&p.key==key).ok_or(Failure::Invalid)?;Some((cursor.clone(),p.clone()))}else{s.pages.iter().find(|(_,p)|p.context==id&&p.key==key&&p.native.is_none()).map(|(id,p)|(id.clone(),p.clone()))};
            if let Some((_,page))=&existing{if let Some(reply)=&page.reply{return reply.clone();}if page.busy{return Err(Failure::Conflict);}}
            if c.busy||c.attempts>=8||c.rows>=256{return Err(Failure::Conflict);}
            let (page_id,native)=match existing{Some((id,p))=>(id,p.native),None=>{if s.pages.len()>=PAGES{return Err(Failure::Conflict);}let page_id=token()?;if s.pages.contains_key(&page_id){return Err(Failure::Internal);}s.pages.insert(page_id.clone(),PageReceipt{context:id.into(),key:key.clone(),native:None,reply:None,busy:false});(page_id,None)}};
            let c=s.contexts.get_mut(id).ok_or(Failure::Conflict)?;c.busy=true;c.attempts+=1;s.pages.get_mut(&page_id).ok_or(Failure::Internal)?.busy=true;(page_id,native)
        };
        let _usage=Usage{store:self.store.clone(),context:id.into(),page:page_id.clone()};
        let result=self.who_page(id,key.clone(),native).await;
        let mut s=lock(&self.store);let _=self.context(&s,id,session)?;
        if let Some(page)=s.pages.get_mut(&page_id){page.reply=Some(result.clone());}result
    }
    async fn who_page(&self,id:&str,key:Option<Key>,native:Option<String>)->Result<Value>{
        let params=ReactionsWhoParams{account_id:self.target.thread.account_id.clone(),chat_id:self.target.thread.chat_id.clone(),message_id:self.target.id.clone(),key:key.as_ref().map(Key::protocol),limit:32,cursor:native.clone()};
        let (reply,raw)=self.call::<_,ReactionsWhoResult>("reactions.who",&params,false).await?;wire_keys(&raw,"reactors",true)?;
        if reply.reactors.len()>32||reply.next_cursor.as_ref().is_some_and(|c|c.is_empty()||c.len()>4096){return Err(Failure::Internal);}
        let mut items=Vec::new();for row in &reply.reactors{let ordinary=Key::from_protocol(&row.key)?;
            if !is_valid_id(&row.user_id)||key.as_ref().is_some_and(|filter|ordinary.as_ref()!=Some(filter)){return Err(Failure::Internal);}
            if let Some(key)=ordinary{items.push(json!({"userId":row.user_id,"key":key,"name":row.name.as_deref().map(short_name),"at":row.at.map(|t|t.unix_millis().to_string())}));}
        }
        self.check().await?;self.caps()?;
        let mut s=lock(&self.store);let c=s.contexts.get(id).filter(|c|c.expires>Instant::now()).ok_or(Failure::Conflict)?;
        if c.rows+reply.reactors.len()>256{return Err(Failure::Conflict);}
        if let Some(next)=&reply.next_cursor{if Some(next)==native.as_ref()||c.seen.contains(&(key.clone(),next.clone())){return Err(Failure::Internal);}}
        let next=if let Some(native)=&reply.next_cursor{if s.pages.len()>=PAGES{return Err(Failure::Conflict);}let next=token()?;if s.pages.contains_key(&next){return Err(Failure::Internal);}Some((next,native.clone()))}else{None};
        let answer=bounded(json!({"items":items,"nextCursor":next.as_ref().map(|(id,_)|id)}))?;
        let c=s.contexts.get_mut(id).ok_or(Failure::Conflict)?;c.rows+=reply.reactors.len();
        if let Some((next,native))=next{c.seen.insert((key.clone(),native.clone()));s.pages.insert(next,PageReceipt{context:id.into(),key,native:Some(native),reply:None,busy:false});}Ok(answer)
    }
}
struct Usage {store:Arc<Mutex<Store>>,context:String,page:String}
impl Drop for Usage {fn drop(&mut self){let mut s=lock(&self.store);if let Some(c)=s.contexts.get_mut(&self.context){c.busy=false;}if let Some(p)=s.pages.get_mut(&self.page){p.busy=false;}}}
impl Policy {
    fn from_result(result:ReactionsAvailableResult)->Result<Self>{
        let keys=if let Some(keys)=result.reactions{if keys.len()>256{return Err(Failure::Unsupported);}let mut values=Vec::new();let mut unique=BTreeSet::new();for key in keys{if let Some(key)=Key::from_protocol(&key)?{if !unique.insert(key.clone()){return Err(Failure::Internal);}values.push(key);}}if values.len()>256{return Err(Failure::Unsupported);}Some(values)}else{None};
        Ok(Self{keys,allow_custom:result.allow_custom,max:result.max_per_message})
    }
}
fn fresh_snapshot(reactions:Option<&[Reaction]>)->Result<(Option<Value>,Option<BTreeSet<Key>>)>{
    let Some(reactions)=reactions else{return Ok((None,None));};if reactions.len()>128{return Err(Failure::Unsupported);}let mut items=Vec::new();let mut unique=BTreeSet::new();let mut selected=BTreeSet::new();let mut paid_seen=false;
    for reaction in reactions{if let Some(key)=Key::from_protocol(&reaction.key)?{if !unique.insert(key.clone()){return Err(Failure::Internal);}if reaction.mine{selected.insert(key.clone());}items.push(json!({"key":key,"count":reaction.count.to_string(),"mine":reaction.mine}));}else{if paid_seen{return Err(Failure::Internal);}paid_seen=true;}}
    if items.len()>128{return Err(Failure::Unsupported);}Ok((Some(json!({"items":items})),Some(selected)))
}
fn wire_key(value:&Value)->Result<()>{let object=value.as_object().ok_or(Failure::Internal)?;let keys=["emoji","custom_emoji_id","paid"];if keys.into_iter().filter(|k|object.contains_key(*k)).count()!=1||object.get("paid").is_some_and(|v|v.as_bool()!=Some(true)){return Err(Failure::Internal);}Ok(())}
fn wire_keys(value:&Value,field:&str,nested:bool)->Result<()>{if let Some(values)=value.get(field).filter(|v|!v.is_null()){for value in values.as_array().ok_or(Failure::Internal)?{wire_key(if nested{value.get("key").ok_or(Failure::Internal)?}else{value})?;}}Ok(())}
fn wire_message(value:&Value)->Result<()>{if let Some(message)=value.get("message").filter(|v|!v.is_null()){wire_keys(message,"reactions",true)?;}Ok(())}
fn short_name(name:&str)->String{if name.len()<=256{return name.into();}let mut end=253;while !name.is_char_boundary(end){end-=1;}format!("{}…",&name[..end])}
fn definite(error:&HostError)->bool{match error {
    HostError::UnknownPlugin(_)|HostError::UnknownAccount(_)|HostError::PluginNotRunning{..}|HostError::InvalidMethod(..)|HostError::AccountBusy(..)|HostError::AccountAlreadyStarted(_)|HostError::SingleAccount(_)=>true,
    HostError::Rpc(e)=>matches!(e.code(),ErrorCode::InvalidParams|ErrorCode::InvalidRequest|ErrorCode::NotFound|ErrorCode::UnknownAccount|ErrorCode::MethodNotFound|ErrorCode::NotSupported|ErrorCode::AuthRequired|ErrorCode::Forbidden),_=>false,
}}
fn handshake_limit(caps:&Capabilities)->Result<u64>{match caps.get("reactions.set"){
    Some(Flag::Bool(true))=>Ok(1),Some(Flag::Params(p))=>match p.get("max"){None=>Ok(1),Some(v)=>v.as_u64().filter(|m|*m>=1).ok_or(Failure::Unsupported)},_=>Err(Failure::Unsupported),
}}
fn cached_snapshot(db:&Db,row:&MessageRow)->Result<Value>{
    if row.deleted_at.is_some()||row.message_id.starts_with("local:")||matches!(row.delivery.as_deref(),Some("sending"|"failed")){return Ok(Value::Null);}
    let mut items=Vec::new();for reaction in ch_db::list_reactions(db.conn(),row.id)?{let key=match Key::from_protocol(&reaction.key_json){Ok(key)=>key,Err(_)=>return Ok(Value::Null)};if let Some(key)=key{items.push(json!({"key":key,"count":if reaction.count>=0{Some(reaction.count.to_string())}else{None},"mine":null}));if items.len()==8{break;}}}
    Ok(if items.is_empty(){Value::Null}else{json!({"items":items})})
}

#[cfg(test)]
#[path="reactions_tests.rs"]
mod tests;
