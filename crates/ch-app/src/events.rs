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

#[allow(clippy::too_many_arguments)] // The two event streams fan out to independent scoped app services.
pub(crate) async fn pump(
    mut core: broadcast::Receiver<CoreChange>,
    mut outbox: broadcast::Receiver<OutboxChange>,
    steps: Steps,
    media: Arc<crate::media::MediaJobs>,
    reactions: Arc<crate::reactions::Service>,
    ingress: Arc<crate::ingress::Service>,
    tx: broadcast::Sender<Invalidation>,
    stop: CancellationToken,
) {
    let mut media_tick=tokio::time::interval(std::time::Duration::from_millis(250));
    media_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _=stop.cancelled()=>break,
            _=media_tick.tick()=>{media.flush_progress();ingress.flush();},
            event=core.recv()=>match event {
                Ok(CoreChange::Ingested{plugin_id,applied:a})=>{
                    media.applied(&plugin_id,&a);applied(&steps,&plugin_id,&a);
                    if a.changes.iter().any(|c|!matches!(c,Change::TransferProgress(_))) {notify(&tx,&a.account_id);}
                }
                Ok(CoreChange::Local{applied,..})=>notify(&tx,&applied.account_id),
                Ok(CoreChange::Host(StateChange::Account(a)))=>{
                    ingress.inventory_changed();
                    if matches!(a.phase,AccountPhase::Stopped|AccountPhase::Stopping) {
                        ingress.invalidate(Some(&a.account_id),None);
                        steps.lock().unwrap_or_else(|e|e.into_inner()).remove(&a.account_id);
                    }
                    notify(&tx,&a.account_id);
                }
                Ok(CoreChange::Host(StateChange::Plugin{plugin_id,state}))=>{
                    if !state.is_running() {
                        ingress.invalidate(None,Some(&plugin_id));
                        reactions.invalidate_plugin(&plugin_id);
                        steps.lock().unwrap_or_else(|e|e.into_inner()).retain(|_,(owner,_)|owner!=&plugin_id);
                        let _=tx.send(Invalidation::Reset);
                    } else {let _=tx.send(Invalidation::Inventory);}
                }
                Ok(CoreChange::Account(change))=>{
                    ingress.inventory_changed();
                    let id=match change {
                        AccountChange::Added{account_id,..}|AccountChange::SettingsChanged{account_id}=>account_id,
                        AccountChange::Enabled{account_id,enabled}=>{if !enabled {ingress.invalidate(Some(&account_id),None);media.cancel_account(&account_id);reactions.invalidate_account(&account_id);}account_id},
                        AccountChange::Removed{account_id,..}=>{
                            ingress.invalidate(Some(&account_id),None);
                            media.cancel_account(&account_id);
                            reactions.invalidate_account(&account_id);
                            steps.lock().unwrap_or_else(|e|e.into_inner()).remove(&account_id);
                            account_id
                        }
                    };
                    notify(&tx,&id);
                }
                Ok(CoreChange::Host(StateChange::PluginsRescanned))=>{let _=tx.send(Invalidation::Inventory);}
                Ok(_) | Err(RecvError::Lagged(_))=>{
                    ingress.inventory_changed();
                    ingress.invalidate(None,None);
                    reactions.invalidate_all();
                    steps.lock().unwrap_or_else(|e|e.into_inner()).clear();
                    let _=tx.send(Invalidation::Reset);
                }
                Err(RecvError::Closed)=>break,
            },
            event=outbox.recv()=>match event{
                Ok(OutboxChange::Item(item))=>{if !ingress.progress_only(&item){notify(&tx,&item.account_id);}},
                Ok(OutboxChange::Removed{account_id,client_id,..})=>{ingress.forget_progress(&client_id);notify(&tx,&account_id);},
                Ok(OutboxChange::Ingested(a))=>notify(&tx,&a.account_id),
                Err(RecvError::Lagged(_))=>{let _=tx.send(Invalidation::Reset);}
                Err(RecvError::Closed)=>break,
            }
        }
    }
}
