//! Isolated WIP: native local admission, with no managed dispatch/reader grants.
//! Not promotable until every Host input/alias/catalog boundary is coherent.

use super::*;
use crate::host::HostHandle;
use crate::managed_sources::{self, ManagedSources};
use crate::managed_stage::{LiveStageClaim, StageIdentity};
use crate::runtime::{Accounts, AccountStorageObservation, ManagedProofError};

#[derive(Clone)]
pub(super) struct Context {
    pub(super) sources: Arc<ManagedSources>,
    pub(super) host: HostHandle,
    pub(super) accounts: Accounts,
}

// Native composition identity, not a filesystem/read or posting permission.
// Only the actual for_core context can derive it; no public factory or booleans.
#[derive(Clone)]
struct Domain { accounts: Accounts, expected: std::path::PathBuf }

impl Domain {
    fn capture(context: &Context, observed: &AccountStorageObservation, stage: &str,
        request: &OutboxRequest) -> Result<Self> {
        let domain = Self { accounts: context.accounts.clone(),
            expected: context.host.config().upload_dir(&observed.owner().account_id).join(stage).join("bytes") };
        domain.check(observed, request)?;
        Ok(domain)
    }
    fn check(&self, observed: &AccountStorageObservation, request: &OutboxRequest) -> Result<()> {
        let OutboxRequest::SendFile(params) = request else { return Err(OutboxError::AdmissionConflict); };
        if !self.accounts.owns_storage_observation(observed)
            || std::path::Path::new(&params.path) != self.expected.as_path()
            || params.account_id != observed.owner().account_id
        { return Err(OutboxError::AdmissionConflict); }
        Ok(())
    }
    fn check_in(&self, conn: &rusqlite::Connection, observed: &AccountStorageObservation,
        request: &OutboxRequest) -> Result<()> {
        self.check(observed, request)?;
        observed.check(conn).map_err(|_| OutboxError::AdmissionConflict)?;
        self.check(observed, request)
    }
}

/// Local admission result only; no platform delivery or source-release assertion.
#[derive(Debug, thiserror::Error)]
pub enum ManagedAdmissionError {
    /// Definite local refusal, with no new source/outbox commit by this operation.
    #[error(transparent)]
    Rejected(#[from] OutboxError),
    /// The bounded local admission service is unavailable or full.
    #[error("managed admission is unavailable")]
    Unavailable,
    /// A local commit may exist. Only an exact retained lookup is allowed next.
    #[error("managed admission outcome is unknown")]
    Unknown,
}

type AdmissionResult = std::result::Result<OutboxItem, ManagedAdmissionError>;

// Snapshot captured internally from existing DB rows, never supplied by a caller.
struct Target { conversation: ch_db::ConversationRow, topic: Option<ch_db::TopicRow> }

impl Target {
    fn capture(conn: &rusqlite::Connection, owner: &AccountRow, request: &OutboxRequest) -> Result<Self> {
        let conversation = ch_db::get_conversation(conn, &owner.account_id, request.chat_id())?
            .ok_or(OutboxError::AdmissionConflict)?;
        let topic = request.topic_id().map(|id| ch_db::get_topic(conn, conversation.id, id))
            .transpose()?.flatten();
        Ok(Self { conversation, topic })
    }

    fn check(&self, conn: &rusqlite::Connection, observation: &AccountStorageObservation,
        request: &OutboxRequest) -> Result<()> {
        observation.check(conn).map_err(|_| OutboxError::AdmissionConflict)?;
        let owner = observation.owner();
        if !ch_db::get_account(conn, &owner.account_id)?.is_some_and(|row| row.enabled) {
            return Err(OutboxError::ManagedUnavailable);
        }
        let old = &self.conversation;
        let current = ch_db::get_conversation(conn, &owner.account_id, request.chat_id())?
            .filter(|row| row.id == old.id && row.created_at == old.created_at
                && row.account_id == owner.account_id && row.chat_id == old.chat_id
                && row.kind == old.kind && !row.is_gone && row.can_post != Some(false))
            .ok_or(OutboxError::AdmissionConflict)?;
        match (&self.topic, request.topic_id()) {
            (None, None) => {}
            (Some(old), Some(id)) if old.conversation_id == current.id && &old.topic_id == id => {
                if !ch_db::get_topic(conn, current.id, id)?.is_some_and(|row|
                    row.id == old.id && row.created_at == old.created_at && !row.closed && !row.is_gone)
                { return Err(OutboxError::AdmissionConflict); }
            }
            _ => return Err(OutboxError::AdmissionConflict),
        }
        Ok(())
    }
}

struct Written {
    item: OutboxItem,
    applied: Option<Applied>,
    source: Option<(i64, StageIdentity)>,
}

fn matching_observed(conn: &rusqlite::Connection, domain: &Domain, observed: &AccountStorageObservation,
    client: &str, request: &OutboxRequest) -> Result<Option<OutboxItem>> {
    domain.check(observed, request)?;
    let tx = rusqlite::Transaction::new_unchecked(conn,
        rusqlite::TransactionBehavior::Deferred).map_err(ch_db::Error::from)?;
    domain.check_in(&tx, observed, request)?;
    let item = matching_admission(&tx, observed.owner(), client, request)?;
    domain.check_in(&tx, observed, request)?;
    tx.commit().map_err(ch_db::Error::from)?;
    domain.check(observed, request)?;
    Ok(item)
}

impl Outbox {
    /// WIP native owned local admission from a real one-use Core claim.
    /// Requires this Core's actual Accounts issuer and configured upload namespace.
    /// Plain Outbox::new retains its separate descriptive get_matching_admission API.
    /// No App/RPC route calls this. Generic workers park its rows until the
    /// complete managed Host/catalog/dispatch chain is implemented and reviewed.
    pub async fn enqueue_managed_file_checked(&self, claim: LiveStageClaim) -> AdmissionResult {
        // Intrinsic composition identity precedes every lookup. This is not a
        // fresh run/posting/byte eligibility check that could hide an old receipt.
        let context = self.inner.managed.clone().ok_or(ManagedAdmissionError::Unavailable)?;
        let observation = claim.owner_binding().observation();
        let client = claim.manifest().client_id.clone();
        let request = claim.canonical_request().map_err(|_| OutboxError::AdmissionConflict)?;
        let domain = Domain::capture(&context, &observation, &claim.manifest().stage_id, &request)?;
        // Retained receipts precede fresh service/run/rights/bytes/used-state checks.
        if let Some(item) = self.lookup_managed_receipt(&context.sources, &domain, &observation, &client, &request).await? { return Ok(item); }
        let outbox = self.clone();
        let sources = Arc::clone(&context.sources);
        let host = context.host;
        let owned_domain = domain.clone();
        let runtime = tokio::runtime::Handle::current();
        match context.sources.run(move || outbox.admit_managed_owned(claim, &sources, &host, owned_domain, &runtime)).await {
            Ok(result) => result,
            Err(managed_sources::Error::Task) => Err(ManagedAdmissionError::Unknown),
            Err(_) => {
                // A reservation/slot refusal can only reconcile a retained receipt.
                self.lookup_managed_receipt(&context.sources, &domain, &observation, &client, &request).await?
                    .ok_or(ManagedAdmissionError::Unavailable)
            }
        }
    }

    // Lookup jobs capture bounded descriptive intent only. A cancelled waiter
    // drops its claim immediately; no detached metadata reader retains its FDs.
    async fn lookup_managed_receipt(&self, sources: &ManagedSources, original_domain: &Domain,
        original: &AccountStorageObservation, client: &str,
        request: &OutboxRequest) -> std::result::Result<Option<OutboxItem>, ManagedAdmissionError> {
        original_domain.check(original, request)?;
        let domain = original_domain.clone();
        let observed = original.clone();
        let client = client.to_owned();
        let request = request.clone();
        let ingest = Arc::clone(&self.inner.ingest);
        #[cfg(test)]
        let checkpoint = self.inner.managed_checkpoint.lock().unwrap().clone();
        let returned_request = request.clone();
        let result = sources.lookups().run(move || -> Result<Option<OutboxItem>> {
            domain.check(&observed, &request)?;
            #[cfg(test)]
            if let Some(checkpoint) = checkpoint { checkpoint("lookup"); }
            let g = ingest.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            matching_observed(g.db().conn(), &domain, &observed, &client, &request)
        }).await;
        original_domain.check(original, &returned_request)?;
        match result {
            Ok(result) => Ok(result?),
            Err(managed_sources::Error::Task) => Err(ManagedAdmissionError::Unknown),
            Err(_) => Err(ManagedAdmissionError::Unavailable),
        }
    }

    fn admit_managed_owned(&self, claim: LiveStageClaim, sources: &Arc<ManagedSources>,
        host: &HostHandle, domain: Domain, runtime: &tokio::runtime::Handle) -> AdmissionResult {
        let owner = claim.owner_binding().owner().clone();
        let client = claim.manifest().client_id.clone();
        let stage = claim.manifest().stage_id.clone();
        let request = claim.canonical_request().map_err(|_| OutboxError::AdmissionConflict)?;
        let observation = claim.owner_binding().observation();
        let retained = |outbox: &Outbox| {
            let g = outbox.inner.ingest();
            matching_observed(g.db().conn(), &domain, &observation, &client, &request)
        };
        if let Some(item) = retained(self)? { return Ok(item); }
        let mut reservation = match sources.reserve(observation.clone(), stage.clone()) {
            Ok(reservation) => reservation,
            Err(_) => return retained(self)?
                .ok_or(ManagedAdmissionError::Unavailable),
        };
        // All actual FS work is outside SQLite. This blocking owned job retains
        // the real claim/binding/FD/flock across the FULL transaction's settlement.
        let bytes = claim.manifest().size;
        let expected_manifest = claim.manifest().clone();
        let prepared: crate::managed_stage::PreparedStageClaim = claim.prepare_admission();
        let identity = prepared.identity().cloned();
        let prepared_request = prepared.request().cloned();
        let prepared_observation = prepared.owner_observation();
        let params = request.params(&owner.account_id, &client);
        let scope = host.file_admission_scope(&owner.plugin_id, &owner.account_id, &params, bytes);
        let target = {
            let g = self.inner.ingest();
            Target::capture(g.db().conn(), &owner, &request)
        };
        // Preserve preparation/validation errors until AFTER exact lookup inside writer.
        let material = identity.clone().map_err(|_| OutboxError::AdmissionConflict)
            .and_then(|identity| {
                let prepared_request = prepared_request.map_err(|_| OutboxError::AdmissionConflict)?;
                if identity.manifest() != &expected_manifest || prepared_observation != observation
                    || prepared_request.params(&owner.account_id, &client) != params
                { return Err(OutboxError::AdmissionConflict); }
                let fingerprint = identity.to_persisted_json().map_err(|_| OutboxError::AdmissionConflict)?;
                let intent = serde_json::to_string(&params)?;
                Ok((identity, fingerprint, intent))
            });
        let stopped = sources.stopped();
        #[cfg(test)] self.at_managed("before-writer");
        let outbox = self.clone();
        let result = runtime.block_on(sources.proofs().with_transaction(move |full| {
            let tx = full.transaction();
            // ALL managed reads and returned receipts use this explicit FULL tx.
            // Original owner generation is identity, never a replaceable row timestamp;
            // fresh posting/run/bytes checks still follow the exact retained lookup.
            domain.check_in(tx, &observation, &request)?;
            if let Some(item) = matching_admission(tx, &owner, &client, &request)? {
                domain.check_in(tx, &observation, &request)?;
                return Ok(Written { item, applied: None, source: None });
            }
            let (identity, fingerprint, intent) = material?;
            let target = target?;
            let scope = scope.map_err(|_| OutboxError::ManagedUnavailable)?;
            let fresh = || -> Result<()> {
                if stopped.is_cancelled() || outbox.inner.stopping.is_cancelled() {
                    return Err(OutboxError::Stopped);
                }
                scope.check().map_err(|_| OutboxError::ManagedUnavailable)?;
                domain.check_in(tx, &observation, &request)?;
                target.check(tx, &observation, &request)
            };
            request.validate()?;
            fresh()?;
            let now = outbox.inner.now();
            tx.execute("INSERT INTO managed_source(stage_id,client_id,account_id,plugin_id,
                account_created_at,chat_id,topic_id,path,fingerprint_json,intent_json,created_at,updated_at)
                VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?11)",
                rusqlite::params![stage,client,owner.account_id,owner.plugin_id,owner.created_at,
                    request.chat_id(),request.topic_id(),identity.path().to_str(),fingerprint,intent,now])
                .map_err(ch_db::Error::from)?;
            let source = tx.last_insert_rowid();
            let (applied, item) = outbox.insert_with_echo_in(tx, &owner.account_id, &client,
                &request, None, |echo| Ok(full.apply_local_echo(&owner.account_id, echo)?))?;
            #[cfg(test)] outbox.at_managed("in-writer");
            if ch_db::managed_source_id(tx, &stage)?.map(|id| id.as_i64()) != Some(source) {
                return Err(OutboxError::AdmissionConflict);
            }
            let stored: String = tx.query_row("SELECT fingerprint_json FROM managed_source WHERE id=?1",
                [source], |r| r.get(0)).map_err(ch_db::Error::from)?;
            if StageIdentity::from_persisted_json(&stored).map_err(|_| OutboxError::AdmissionConflict)? != identity {
                return Err(OutboxError::AdmissionConflict);
            }
            // Last DB rights/owner/run check is the admission linearization point.
            fresh()?;
            Ok(Written { item, applied, source: Some((source, identity)) })
        }));
        let written = match result {
            Ok(Ok(written)) => written,
            Ok(Err(error)) => {
                reservation.no_new_source();
                return Err(error.into());
            }
            Err(ManagedProofError::CommitUnknown(_) | ManagedProofError::RollbackUnknown(_)
                | ManagedProofError::TaskFailed) => {
                reservation.quarantine(identity.ok());
                return Err(ManagedAdmissionError::Unknown);
            }
            Err(_) => {
                reservation.no_new_source();
                return Err(ManagedAdmissionError::Unavailable);
            }
        };
        if let Some((id, identity)) = written.source {
            if reservation.publish(id, identity.clone()).is_err() {
                reservation.quarantine(Some(identity));
                tracing::warn!("managed admission committed; gate publication incomplete");
            }
        } else {
            // An identical legacy/other admission is never retro-registered.
            reservation.no_new_source();
        }
        #[cfg(test)] self.at_managed("committed");
        // A known receipt survives local bookkeeping failures. consume() performs
        // its own exact retained lookup; all physical handles drop before events.
        if prepared.into_claim().consume(self).is_err() {
            tracing::warn!("managed admission committed; local bookkeeping incomplete");
        }
        drop(reservation);
        if let Some(applied) = written.applied { self.inner.publish(OutboxChange::Ingested(applied)); }
        self.inner.publish(OutboxChange::Item(written.item.clone()));
        // Intentionally no managed scheduling. Even manual start is fail-closed
        // at the shared generic managed-row guard, without relying on this omission.
        Ok(written.item)
    }

    #[cfg(test)]
    fn at_managed(&self, phase: &str) {
        let checkpoint = self.inner.managed_checkpoint.lock().unwrap().clone();
        if let Some(checkpoint) = checkpoint { checkpoint(phase); }
    }
}

#[cfg(test)]
#[path = "managed_admission_tests.rs"]
mod tests;
