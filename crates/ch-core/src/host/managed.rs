//! Native producer metadata. No file descriptor, process handle or source grant.
use crate::runtime::LogicalOwnerObservation;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub(crate) fn nonce() -> Option<String> {
    let mut bytes=[0u8;16];
    // SAFETY: bytes is valid for its exact length. Never block the Host loop on
    // entropy; failure disables managed capture, not ordinary plugin operation.
    let count=unsafe {libc::getrandom(bytes.as_mut_ptr().cast(),bytes.len(),libc::GRND_NONBLOCK)};
    if count!=bytes.len() as isize {return None;}
    let hex=b"0123456789abcdef";let mut nonce=String::with_capacity(32);
    for byte in bytes {nonce.push(hex[(byte>>4) as usize] as char);nonce.push(hex[(byte&15) as usize] as char);}
    Some(nonce)
}

pub(super) struct Instance { nonce:String,pid:u32 }
impl Instance {
    // Only the actual Host process-spawn path supplies its Child::id().
    pub(super) fn spawned(pid:Option<u32>) -> Option<Arc<Self>> {
        Some(Arc::new(Self {nonce:nonce()?,pid:pid?}))
    }
}

/// Original initialized connection + accepted account interval, not caller PID.
#[derive(Clone)]
pub(crate) struct NativeProducer {
    instance:Arc<Instance>,
    origin:LogicalOwnerObservation,
    run:CancellationToken,
}
impl NativeProducer {
    pub(super) fn capture(instance:&Option<Arc<Instance>>,origin:LogicalOwnerObservation,
        run:CancellationToken,capabilities:&ch_protocol::Capabilities) -> Option<Self> {
        // This is the LIVE raw initialize metadata of this exact connection.
        let promised=matches!(capabilities.get("send.file"),Some(ch_protocol::Flag::Params(params))
            if params.get("source_release").and_then(serde_json::Value::as_str)==Some("on_success"));
        if !promised || origin.is_revoked() || run.is_cancelled() {return None;}
        Some(Self {instance:instance.clone()?,origin,run})
    }
    pub(crate) fn nonce(&self) -> &str {&self.instance.nonce}
    pub(crate) fn native_pid(&self) -> u32 {self.instance.pid}
    pub(crate) fn account(&self) -> &str {self.origin.account_id()}
    pub(crate) fn plugin(&self) -> &str {self.origin.plugin_id()}
    pub(crate) fn live(&self) -> bool {!self.origin.is_revoked() && !self.run.is_cancelled()}
    pub(crate) fn origin(&self) -> LogicalOwnerObservation {self.origin.clone()}
    pub(super) fn same_connection(&self,instance:&Option<Arc<Instance>>) -> bool {
        self.live() && instance.as_ref().is_some_and(|actual|Arc::ptr_eq(actual,&self.instance))
    }
}
