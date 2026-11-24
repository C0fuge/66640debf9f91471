use ch_core::{
    host::{AccountPhase, StateChange},
    ingest::{Applied, Change},
    outbox::OutboxChange,
    runtime::{AccountChange, CoreChange},
};
use ch_protocol::dto::AuthStep;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast::{self, error::RecvError};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Invalidation {
    Reset,
    Inventory,
    Account { account_id: String },
}
pub(crate) type Steps = Arc<Mutex<BTreeMap<String, (String, AuthStep)>>>;

pub(crate) fn notify(tx: &broadcast::Sender<Invalidation>, id: &str) {
    let _ = tx.send(Invalidation::Account {
        account_id: id.into(),
    });
}

fn applied(steps: &Steps, plugin: &str, a: &Applied) {
    let mut cache = steps.lock().unwrap_or_else(|e| e.into_inner());
    for change in &a.changes {
        match change {
            Change::AuthStep(s) if s.kind.as_str() != "done" => {
                cache.insert(a.account_id.clone(), (plugin.into(), s.clone()));
            }
            Change::AuthStep(_) => {
                cache.remove(&a.account_id);
            }
            Change::AccountStatus { state, .. }
                if matches!(
                    state.as_str(),
                    "connected" | "disconnected" | "unavailable" | "error"
                ) =>
            {
                cache.remove(&a.account_id);
            }
            _ => {}
        }
    }
}

pub(crate) async fn pump(
    mut core: broadcast::Receiver<CoreChange>,
    mut outbox: broadcast::Receiver<OutboxChange>,
    steps: Steps,
    tx: broadcast::Sender<Invalidation>,
    stop: CancellationToken,
) {
    loop {
        tokio::select! {
            _=stop.cancelled()=>break,
            event=core.recv()=>match event {
                Ok(CoreChange::Ingested{plugin_id,applied:a})=>{applied(&steps,&plugin_id,&a);notify(&tx,&a.account_id);}
                Ok(CoreChange::Local{applied,..})=>notify(&tx,&applied.account_id),
                Ok(CoreChange::Host(StateChange::Account(a)))=>{
                    if matches!(a.phase,AccountPhase::Stopped|AccountPhase::Stopping) {
                        steps.lock().unwrap_or_else(|e|e.into_inner()).remove(&a.account_id);
                    }
                    notify(&tx,&a.account_id);
                }
                Ok(CoreChange::Host(StateChange::Plugin{plugin_id,state}))=>{
                    if !state.is_running() {
                        steps.lock().unwrap_or_else(|e|e.into_inner()).retain(|_,(owner,_)|owner!=&plugin_id);
                        let _=tx.send(Invalidation::Reset);
                    } else {let _=tx.send(Invalidation::Inventory);}
                }
                Ok(CoreChange::Account(change))=>{
                    let id=match change {
                        AccountChange::Added{account_id,..}|AccountChange::Enabled{account_id,..}|AccountChange::SettingsChanged{account_id}=>account_id,
                        AccountChange::Removed{account_id,..}=>{
                            steps.lock().unwrap_or_else(|e|e.into_inner()).remove(&account_id);
                            account_id
                        }
                    };
                    notify(&tx,&id);
                }
                Ok(CoreChange::Host(StateChange::PluginsRescanned))=>{let _=tx.send(Invalidation::Inventory);}
                Ok(_) | Err(RecvError::Lagged(_))=>{
                    steps.lock().unwrap_or_else(|e|e.into_inner()).clear();
                    let _=tx.send(Invalidation::Reset);
                }
                Err(RecvError::Closed)=>break,
            },
            event=outbox.recv()=>match event{
                Ok(OutboxChange::Item(item))=>notify(&tx,&item.account_id),
                Ok(OutboxChange::Removed{account_id,..})=>notify(&tx,&account_id),
                Ok(OutboxChange::Ingested(a))=>notify(&tx,&a.account_id),
                Err(RecvError::Lagged(_))=>{let _=tx.send(Invalidation::Reset);}
                Err(RecvError::Closed)=>break,
            }
        }
    }
}
