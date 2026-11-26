//! No validated native upload boundary on this platform.
use crate::{
    OpenFileDialog,
    events::Invalidation,
    wire::{Failure, Result, Thread},
};
use ch_core::{
    outbox::{Outbox, OutboxItem},
    runtime::Core,
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

pub(crate) struct Service {
    session: String,
    core: Arc<Core>,
}
impl Service {
    pub(crate) async fn new(
        core: Arc<Core>,
        _: Arc<Mutex<ch_db::Db>>,
        _: Outbox,
        _: broadcast::Sender<Invalidation>,
    ) -> std::io::Result<Arc<Self>> {
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes)
            .map_err(|_| std::io::Error::other("Cannot generate an ingress identifier"))?;
        Ok(Arc::new(Self {
            core,
            session: format!("{:032x}", u128::from_be_bytes(bytes)),
        }))
    }
    pub(crate) fn install(&self, _: Arc<dyn OpenFileDialog>) {}
    pub(crate) fn available(&self) -> bool {
        false
    }
    pub(crate) fn close(&self) {}
    pub(crate) async fn drain(&self) {}
    pub(crate) fn inventory_changed(&self) {}
    pub(crate) fn flush(&self) {}
    pub(crate) fn reset_progress(&self) {}
    pub(crate) fn forget_progress(&self, _: &str) {}
    pub(crate) fn progress_only(&self, _: &OutboxItem) -> bool {
        false
    }
    pub(crate) async fn list(
        &self,
        t: Thread,
        k: Option<String>,
        c: Option<String>,
    ) -> Result<Value> {
        use ch_protocol::common::is_valid_id;
        if !is_valid_id(&t.account_id)
            || !is_valid_id(&t.chat_id)
            || t.topic_id.as_deref().is_some_and(|id| !is_valid_id(id))
            || c.is_some()
            || k.as_deref().is_some_and(|s| {
                s.is_empty()
                    || s.len() > 64
                    || !s
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
            })
        {
            return Err(Failure::Invalid);
        }
        self.core
            .accounts()
            .get(&t.account_id)
            .await?
            .ok_or(Failure::Missing)?;
        Ok(
            json!({"sessionId":self.session,"items":[],"nextCursor":null,"remainingRequestKeys":4096}),
        )
    }
    pub(crate) async fn start(&self, _: Thread, _: String, _: String) -> Result<Value> {
        Err(Failure::Unsupported)
    }
    pub(crate) async fn send(
        &self,
        _: Thread,
        _: String,
        _: String,
        _: String,
        _: bool,
        _: bool,
    ) -> Result<Value> {
        Err(Failure::Unsupported)
    }
    pub(crate) async fn cancel(&self, _: Thread, _: String, _: String) -> Result<Value> {
        Err(Failure::Missing)
    }
}
