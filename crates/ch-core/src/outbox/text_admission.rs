//! Atomic admission of an existing application text destination, without creating stubs.
use super::*;
use ch_db::{ConversationRow, TopicRow};
use ch_protocol::common::is_valid_id;

/// Persisted destination identities captured while the caller retains its owner storage lease.
#[derive(Clone)]
pub struct TextAdmissionTarget {
    /// Exact persisted account incarnation, including plugin and creation time.
    pub owner: AccountRow,
    /// Existing whole-chat row. It cannot be recreated by the local echo helper.
    pub conversation: ConversationRow,
    /// Existing topic row when sending into a topic.
    pub topic: Option<TopicRow>,
}

/// Revocation authorities held by the owned application job through actual settlement.
pub struct TextAdmissionLifetime {
    /// The exact account storage generation; removal revokes it before awaiting its writer.
    pub owner_revoked: CancellationToken,
    /// Closed application admission service. This is not a network run token.
    pub app_closed: CancellationToken,
}

/// Definite rejection or a commit whose outcome could not be established.
#[derive(Debug, thiserror::Error)]
pub enum TextAdmissionError {
    /// No local admission; the transaction was never committed.
    #[error(transparent)]
    Rejected(#[from] OutboxError),
    /// A commit may have succeeded. The caller must never repeat admission automatically.
    #[error("text admission outcome is unknown")]
    CommitUnknown,
}
impl From<ch_db::Error> for TextAdmissionError {
    fn from(error: ch_db::Error) -> Self { Self::Rejected(OutboxError::Db(error)) }
}

impl TextAdmissionTarget {
    fn validate(&self, conn: &rusqlite::Connection, params: &MessagesSendParams) -> Result<()> {
        let owner = &self.owner;
        let expected = &self.conversation;
        if !is_valid_id(&params.account_id) || !is_valid_id(&params.chat_id)
            || params.topic_id.as_deref().is_some_and(|id| !is_valid_id(id))
            || params.account_id != owner.account_id || expected.account_id != owner.account_id
            || params.chat_id != expected.chat_id || params.text.trim().is_empty()
            || params.reply_to.is_some() || params.comment_to.is_some() || params.silent.is_some()
            || params.schedule_at.is_some() || params.no_webpage.is_some() || params.effect_id.is_some()
        {
            return Err(OutboxError::InvalidRequest("invalid checked text destination or options".into()));
        }
        if !ch_db::get_account(conn, &owner.account_id)?.is_some_and(|a|
            a.plugin_id == owner.plugin_id && a.created_at == owner.created_at)
        {
            return Err(OutboxError::AdmissionConflict);
        }
        let current = ch_db::get_conversation(conn, &owner.account_id, &params.chat_id)?
            .filter(|c| !c.is_gone && c.id == expected.id && c.created_at == expected.created_at && c.kind == expected.kind)
            .ok_or(OutboxError::AdmissionConflict)?;
        if current.can_post == Some(false) { return Err(OutboxError::AdmissionConflict); }
        match (&self.topic, &params.topic_id) {
            (None, None) => {}
            (Some(expected), Some(id)) if expected.conversation_id == current.id && &expected.topic_id == id => {
                if !ch_db::get_topic(conn, current.id, id)?.is_some_and(|t|
                    !t.is_gone && !t.closed && t.id == expected.id && t.created_at == expected.created_at)
                { return Err(OutboxError::AdmissionConflict); }
            }
            _ => return Err(OutboxError::AdmissionConflict),
        }
        Ok(())
    }
}
impl TextAdmissionLifetime {
    fn check(&self, outbox: &Outbox) -> Result<()> {
        if self.owner_revoked.is_cancelled() || self.app_closed.is_cancelled() || outbox.inner.stopping.is_cancelled() {
            return Err(OutboxError::Stopped);
        }
        Ok(())
    }
}
impl Outbox {
    /// Atomically recheck a text destination and insert one fresh echo/queued item.
    /// The caller must retain the exact owner's storage read guard until this function settles.
    /// The last successful lifetime check is the logical admission point: a later remover
    /// waits for that guard before deleting old rows/namespace and allowing ID reuse.
    pub fn enqueue_text_checked(
        &self,
        target: &TextAdmissionTarget,
        mut params: MessagesSendParams,
        lifetime: &TextAdmissionLifetime,
        sender: Option<EchoSender>,
    ) -> std::result::Result<OutboxItem, TextAdmissionError> {
        lifetime.check(self)?;
        params.client_id = None;
        let account = &target.owner.account_id;
        let request = OutboxRequest::SendText(params.clone());
        let (applied, item) = {
            let ingest = self.inner.ingest();
            let tx = ingest.db().transaction()?;
            target.validate(&tx, &params)?;
            lifetime.check(self)?;
            let client_id: String = tx.query_row("SELECT lower(hex(randomblob(16)))", [], |row| row.get(0))
                .map_err(ch_db::Error::from)?;
            let inserted = self.insert_in(&ingest, &tx, account, &client_id, &request, sender.as_ref())?;
            #[cfg(test)] self.at_text("before-commit");
            lifetime.check(self)?;
            #[cfg(test)] self.at_text("linearized");
            tx.commit().map_err(|_| TextAdmissionError::CommitUnknown)?;
            inserted
        };
        #[cfg(test)] self.at_text("committed");
        if let Some(applied) = applied { self.inner.publish(OutboxChange::Ingested(applied)); }
        self.inner.publish(OutboxChange::Item(item.clone()));
        // A scheduling failure cannot turn a known durable admission into a refusal.
        if Self::refresh_worker(&self.inner, account).is_err() {
            tracing::warn!(account, id=item.id, "outbox: text admitted but worker refresh failed");
        }
        Ok(item)
    }
    #[cfg(test)]
    fn at_text(&self, phase: &str) {
        let checkpoint = self.inner.text_checkpoint.lock().unwrap().clone();
        if let Some(checkpoint) = checkpoint { checkpoint(phase); }
    }
}

#[cfg(test)]
#[path = "text_admission_tests.rs"]
mod tests;
