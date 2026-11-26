//! One-use proof of an exact dialog observation, scoped to the active account run.
use crate::{
    App, events,
    wire::{Failure, Result, Thread},
};
use ch_core::runtime::Core;
use ch_db::{
    Db,
    types::{AccountRow, ConversationRow},
};
use ch_protocol::{
    capabilities::Capabilities,
    methods::{ChatsGetResult, ChatsSetFlagsParams},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};
use tokio::{
    sync::{Semaphore, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

const LIMIT: usize = 64;
const TTL: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum Flag {
    Pinned,
    MarkedUnread,
    Archived,
}

pub(crate) struct Set {
    pub session: String,
    pub token: String,
    pub flag: Flag,
    pub value: bool,
}

struct Target {
    thread: Thread,
    owner: AccountRow,
    row: ConversationRow,
    caps: Arc<Capabilities>,
    run: CancellationToken,
}

impl Target {
    fn live(&self, core: &Core) -> Result<()> {
        if self.run.is_cancelled()
            || !core
                .host()
                .plugin_state(&self.owner.plugin_id)
                .and_then(|s| s.capabilities().cloned())
                .is_some_and(|c| Arc::ptr_eq(&self.caps, &c))
        {
            return Err(Failure::Unavailable);
        }
        Ok(())
    }

    fn exists(&self, db: &Db) -> Result<()> {
        if !ch_db::get_account(db.conn(), &self.owner.account_id)?.is_some_and(|a| {
            a.enabled
                && a.plugin_id == self.owner.plugin_id
                && a.created_at == self.owner.created_at
        }) {
            return Err(Failure::Unavailable);
        }
        if !ch_db::get_conversation(db.conn(), &self.thread.account_id, &self.thread.chat_id)?
            .is_some_and(|c| {
                !c.is_gone
                    && c.id == self.row.id
                    && c.created_at == self.row.created_at
                    && c.kind == self.row.kind
            })
        {
            return Err(Failure::Missing);
        }
        Ok(())
    }
}

struct Proof {
    target: Arc<Target>,
    expires: Instant,
    pinned: Option<bool>,
    marked_unread: Option<bool>,
    archived: Option<bool>,
}

struct Task {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

#[derive(Default)]
struct State {
    closed: bool,
    proofs: BTreeMap<String, Proof>,
    tasks: Vec<Task>,
}

pub(crate) struct Service {
    session: String,
    state: Mutex<State>,
    slots: Arc<Semaphore>,
    #[cfg(test)]
    after_reply: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

fn token() -> std::io::Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes)
        .map_err(|_| std::io::Error::other("Cannot initialize chat preference context"))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

impl Service {
    pub(crate) fn new() -> std::io::Result<Self> {
        Ok(Self {
            session: token()?,
            state: Mutex::default(),
            slots: Arc::new(Semaphore::new(4)),
            #[cfg(test)]
            after_reply: Mutex::default(),
        })
    }
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub(crate) fn close(&self) {
        let mut s = self.state();
        s.closed = true;
        s.proofs.clear();
        for task in &s.tasks {
            task.cancel.cancel();
        }
    }
    pub(crate) async fn drain(&self) {
        let tasks = std::mem::take(&mut self.state().tasks);
        for task in tasks {
            let _ = task.handle.await;
        }
    }
    fn review(
        &self,
        target: Arc<Target>,
        pinned: Option<bool>,
        marked_unread: Option<bool>,
        archived: Option<bool>,
    ) -> Result<Value> {
        let mut s = self.state();
        if s.closed || target.run.is_cancelled() {
            return Err(Failure::Unavailable);
        }
        let now = Instant::now();
        s.proofs.retain(|_, p| {
            p.expires > now && !p.target.run.is_cancelled() && p.target.thread != target.thread
        });
        let token = if pinned.is_some() || marked_unread.is_some() || archived.is_some() {
            if s.proofs.len() >= LIMIT {
                return Err(Failure::Conflict);
            }
            let id = token().map_err(|_| Failure::Internal)?;
            if s.proofs.contains_key(&id) {
                return Err(Failure::Internal);
            }
            s.proofs.insert(
                id.clone(),
                Proof {
                    target: target.clone(),
                    expires: now + TTL,
                    pinned,
                    marked_unread,
                    archived,
                },
            );
            Some(id)
        } else {
            None
        };
        Ok(
            json!({"thread":target.thread,"sessionId":self.session,"contextToken":token,"pinned":pinned,"markedUnread":marked_unread,"archived":archived}),
        )
    }
    fn consume(&self, thread: &Thread, write: &Set) -> Result<Arc<Target>> {
        if write.session != self.session {
            return Err(Failure::Conflict);
        }
        let mut s = self.state();
        let proof = s
            .proofs
            .get(&write.token)
            .filter(|p| &p.target.thread == thread)
            .ok_or(Failure::Conflict)?;
        let known = match write.flag {
            Flag::Pinned => proof.pinned,
            Flag::MarkedUnread => proof.marked_unread,
            Flag::Archived => proof.archived,
        };
        let proof = s.proofs.remove(&write.token).ok_or(Failure::Conflict)?;
        if known.is_none() || proof.expires <= Instant::now() {
            return Err(Failure::Conflict);
        }
        Ok(proof.target)
    }
}

impl App {
    pub(crate) async fn chat_flags(&self, thread: Thread, write: Option<Set>) -> Result<Value> {
        if thread.topic_id.is_some() || thread.chat_id.starts_with("local:") {
            return Err(Failure::Invalid);
        }
        let svc = self.chat_flags.clone();
        let permit = svc
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Failure::Conflict)?;
        let guard = self.action_lock(&thread).await?;
        let target = if let Some(params) = &write {
            svc.consume(&thread, params)?
        } else {
            let owner = self.account(&thread.account_id).await?;
            if !owner.enabled {
                return Err(Failure::Unavailable);
            }
            let run = self
                .core
                .host()
                .running_account_token(&owner.plugin_id, &owner.account_id)
                .ok_or(Failure::Unavailable)?;
            let caps = self
                .core
                .host()
                .plugin_state(&owner.plugin_id)
                .and_then(|s| s.capabilities().cloned())
                .ok_or(Failure::Unavailable)?;
            let (row, _) = self.thread(&thread).await?;
            Arc::new(Target {
                thread,
                owner,
                row,
                caps,
                run,
            })
        };
        target.live(&self.core)?;
        let t = target.clone();
        self.read(move |db| t.exists(db)).await?;
        let cancel = target.run.child_token();
        let worker_cancel = cancel.clone();
        let core = self.core.clone();
        let service = svc.clone();
        let changes = self.changes.clone();
        let wire_id = Arc::new(OnceLock::new());
        let tracked = wire_id.clone();
        let is_write = write.is_some();
        let (tx, rx) = oneshot::channel();
        {
            let mut state = svc.state();
            if state.closed {
                return Err(Failure::Unavailable);
            }
            state.tasks.retain(|t| !t.handle.is_finished());
            let handle = tokio::spawn(async move {
                let result = operate(&core, &service, &target, write, worker_cancel, tracked).await;
                if is_write && (result.is_ok() || matches!(result, Err(Failure::OutcomeUnknown))) {
                    events::notify(&changes, &target.thread.account_id);
                }
                // A completed request must release its thread/slot before waking
                // a caller which may immediately request a fresh observation.
                drop((guard, permit));
                let _ = tx.send(result);
                #[cfg(test)]
                if is_write {
                    let hook = service.after_reply.lock().unwrap_or_else(|e| e.into_inner()).take();
                    if let Some(hook) = hook { hook(); }
                }
            });
            state.tasks.push(Task { cancel, handle });
        }
        rx.await.map_err(|_| {
            if is_write && wire_id.get().is_some() {
                Failure::OutcomeUnknown
            } else {
                Failure::Unavailable
            }
        })?
    }
}

async fn operate(
    core: &Core,
    svc: &Service,
    target: &Arc<Target>,
    write: Option<Set>,
    cancel: CancellationToken,
    id: Arc<OnceLock<i64>>,
) -> Result<Value> {
    target.live(core)?;
    let t = target.clone();
    core.ingest()
        .with_ingest(move |ing| t.exists(ing.db()))
        .await??;
    target.live(core)?;
    if cancel.is_cancelled() {
        return Err(Failure::Unavailable);
    }
    if !target.caps.has("chat.get") {
        return Err(Failure::Unsupported);
    }
    if let Some(write) = write {
        let (cap, pinned, marked_unread, archived) = match write.flag {
            Flag::Pinned => ("chat.pin", Some(write.value), None, None),
            Flag::MarkedUnread => ("chat.mark_unread", None, Some(write.value), None),
            Flag::Archived => ("chat.archive", None, None, Some(write.value)),
        };
        if !target.caps.has("chat.flags") || !target.caps.has(cap) {
            return Err(Failure::Unsupported);
        }
        let params = ChatsSetFlagsParams {
            account_id: target.thread.account_id.clone(),
            chat_id: target.thread.chat_id.clone(),
            pinned,
            marked_unread,
            archived,
            ..Default::default()
        };
        let raw = core
            .host()
            .call_raw_tracked(
                &target.owner.plugin_id,
                "chats.set_flags",
                serde_json::to_value(params).map_err(|_| Failure::Invalid)?,
                Some(cancel.clone()),
                Some(id.clone()),
            )
            .await
            .map_err(|err| {
                if id.get().is_some() {
                    Failure::OutcomeUnknown
                } else {
                    Failure::from(&err)
                }
            })?;
        if !raw.is_object() || cancel.is_cancelled() {
            return Err(Failure::OutcomeUnknown);
        }
        let t = target.clone();
        core.ingest()
            .with_ingest(move |ing| t.exists(ing.db()))
            .await
            .map_err(|_| Failure::OutcomeUnknown)?
            .map_err(|_| Failure::OutcomeUnknown)?;
        target.live(core).map_err(|_| Failure::OutcomeUnknown)?;
        return Ok(Value::Null);
    }
    let raw = core
        .host()
        .call_raw(
            &target.owner.plugin_id,
            "chats.get",
            json!({"account_id":target.thread.account_id,"chat_id":target.thread.chat_id}),
            Some(cancel.clone()),
        )
        .await
        .map_err(|err| Failure::from(&err))?;
    let obs = serde_json::from_value::<ChatsGetResult>(raw)
        .map_err(|_| Failure::Internal)?
        .conversation;
    if obs.id != target.thread.chat_id || obs.kind.as_str() != target.row.kind {
        return Err(Failure::Conflict);
    }
    target.live(core)?;
    let (pinned, marked_unread, archived) = (obs.pinned, obs.marked_unread, obs.archived);
    let t = target.clone();
    let applied = core
        .ingest()
        .with_ingest(move |ing| {
            if cancel.is_cancelled() {
                return Err(Failure::Unavailable);
            }
            ing.apply_observed_chat_flags(&t.owner, &t.row, pinned, marked_unread, archived, || {
                !cancel.is_cancelled()
            })
            .map_err(|_| Failure::Internal)?
            .ok_or(Failure::Conflict)
        })
        .await??;
    core.bus().publish_local("chat-flags", applied);
    target.live(core)?;
    svc.review(target.clone(), pinned, marked_unread, archived)
}

#[cfg(test)]
#[path = "chat_flags_tests.rs"]
mod tests;
