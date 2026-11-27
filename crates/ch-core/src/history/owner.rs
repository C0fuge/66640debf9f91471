//! Account generation capture and actual blocking DB-work ownership.
use super::*;
use crate::runtime::RuntimeError;

/// A bounded, owned projection lease. Move it into the actual blocking reader;
/// never retain it across platform/network work. Not a write or filesystem grant.
pub struct HistoryReadLease {
    binding: crate::runtime::AccountStorageBinding,
    _permit: tokio::sync::OwnedSemaphorePermit,
    stop: CancellationToken,
}
impl HistoryReadLease {
    pub fn check(&self, conn: &rusqlite::Connection) -> Result<()> {
        let owner = self.binding.owner();
        if self.stop.is_cancelled() { return Err(HistoryError::UnknownAccount(owner.account_id.clone())); }
        self.binding.observation().check(conn).map_err(|error| runtime(error, &owner.account_id))
    }
}

fn runtime(error: RuntimeError, account: &str) -> HistoryError {
    match error {
        RuntimeError::Db(error) => error.into(),
        _ => HistoryError::UnknownAccount(account.to_owned()),
    }
}

impl History {
    /// Refuse new work and revoke unlinearized DB writes. Never abort blocking work.
    pub fn close(&self) {
        *lock(&self.0.closed) = true;
        self.0.stop.cancel();
        self.prune();
    }

    /// Wait for actual DB closures, including ones detached by a dropped caller.
    pub async fn drain(&self) {
        // The semaphore is never closed: an acquire error cannot count as settlement.
        let _settled = self.0.db_slots.clone().acquire_many_owned(DB_JOBS).await
            .expect("history DB semaphore is not closed");
    }

    pub(super) fn active(&self, ctx: &Ctx) -> Result<()> {
        if self.0.stop.is_cancelled() || ctx.owner.is_revoked() {
            return Err(HistoryError::UnknownAccount(ctx.owner.owner().account_id.clone()));
        }
        Ok(())
    }

    pub(super) fn prune(&self) {
        lock(&self.0.chat_locks).retain(|(owner, _), value| !owner.is_revoked() && value.strong_count() > 0);
        lock(&self.0.inflight).retain(|((owner, _), _), cell| !owner.is_revoked() && Arc::strong_count(cell) > 1);
        let now = Instant::now();
        lock(&self.0.flood).retain(|(owner, _), until| !owner.is_revoked() && *until > now);
        lock(&self.0.gap_errors).retain(|(owner, _), _| !owner.is_revoked());
    }

    pub(super) async fn ctx(&self, ck: &ChatKey) -> Result<Ctx> {
        self.prune();
        if self.0.stop.is_cancelled() { return Err(HistoryError::UnknownAccount(ck.account_id.clone())); }
        // Accounts metadata readers can themselves be blocking. An owned async wrapper
        // keeps their drain permit until actual settlement if the request disappears.
        let permit = self.db_permit()?;
        let accounts = self.0.accounts.clone();
        let id = ck.account_id.clone();
        let owner = tokio::spawn(async move {
            let _permit = permit;
            accounts.storage_observation_for(&id).await.map_err(|error| runtime(error, &id))
        }).await.map_err(|_| worker_failed())??;
        let plugin_id = owner.owner().plugin_id.clone();
        let ctx = Ctx { owner, caps:self.0.api.capabilities(&plugin_id), plugin_id, serial:None };
        self.active(&ctx)?;
        Ok(ctx)
    }

    pub(super) async fn serial(&self, ctx: &Ctx, ck: &ChatKey) -> Result<Ctx> {
        let mutex = self.chat_lock(ctx, ck);
        let revoked = ctx.owner.revocation();
        let guard = tokio::select! {
            biased;
            _ = self.0.stop.cancelled() => return Err(HistoryError::UnknownAccount(ck.account_id.clone())),
            _ = revoked.cancelled() => return Err(HistoryError::UnknownAccount(ck.account_id.clone())),
            guard = mutex.lock_owned() => guard,
        };
        let mut ctx = ctx.clone();
        ctx.serial = Some(Arc::new(guard));
        self.active(&ctx)?;
        Ok(ctx)
    }

    pub(super) fn chat_lock(&self, ctx: &Ctx, ck: &ChatKey) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = lock(&self.0.chat_locks);
        locks.retain(|(owner, _), value| !owner.is_revoked() && value.strong_count() > 0);
        let scope = (ctx.owner.clone(), ck.clone());
        if let Some(mutex) = locks.get(&scope).and_then(Weak::upgrade) { return mutex; }
        let mutex = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(scope, Arc::downgrade(&mutex));
        mutex
    }

    pub(super) fn page(&self, ctx: &Ctx, mut page: Page) -> Result<Page> {
        self.active(ctx)?;
        page.owner = Some(ctx.owner.clone());
        Ok(page)
    }

    /// Extend exact-generation read ownership through a caller's bounded projection.
    pub async fn projection_lease(&self, observed: &AccountStorageObservation) -> Result<HistoryReadLease> {
        if self.0.stop.is_cancelled() || observed.is_revoked() {
            return Err(HistoryError::UnknownAccount(observed.owner().account_id.clone()));
        }
        let permit = self.db_permit()?;
        let accounts = self.0.accounts.clone();
        let observed = observed.clone();
        let stop = self.0.stop.clone();
        tokio::spawn(async move {
            let binding = accounts.storage_binding_for_observation(&observed).await
                .map_err(|error| runtime(error, &observed.owner().account_id))?;
            if stop.is_cancelled() || observed.is_revoked() {
                return Err(HistoryError::UnknownAccount(observed.owner().account_id.clone()));
            }
            Ok(HistoryReadLease { binding, _permit:permit, stop })
        }).await.map_err(|_| worker_failed())?
    }

    fn db_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit> {
        let closed = lock(&self.0.closed);
        if *closed { return Err(HistoryError::Storage(Arc::new(ch_db::Error::Invalid("history is closed".into())))); }
        self.0.db_slots.clone().try_acquire_owned()
            .map_err(|_| HistoryError::Storage(Arc::new(ch_db::Error::Invalid("history DB work is busy".into()))))
    }

    pub(super) async fn read<T, F>(&self, ctx: &Ctx, f: F) -> Result<T>
    where T:Send+'static, F:FnOnce(&Self, &rusqlite::Connection)->Result<T>+Send+'static {
        self.work(ctx, move |this, ingest, _owner, _stop| f(this, ingest.db().conn())).await
    }

    pub(super) async fn work<T, F>(&self, ctx: &Ctx, f: F) -> Result<T>
    where T:Send+'static, F:FnOnce(&Self, &Ingest, &AccountStorageObservation, &CancellationToken)->Result<T>+Send+'static {
        self.active(ctx)?;
        // Never acquire storage while holding Ingest or an SQLite writer.
        let lease = self.projection_lease(&ctx.owner).await?;
        let this = self.clone();
        let ctx = ctx.clone();
        tokio::task::spawn_blocking(move || {
            let _serial = ctx.serial.clone();
            #[cfg(target_os="linux")]
            let _references=this.0.references.read();
            this.active(&ctx)?;
            let ingest = lock(&this.0.ingest);
            lease.check(ingest.db().conn())?;
            let result = f(&this, &ingest, &ctx.owner, &this.0.stop);
            lease.check(ingest.db().conn())?;
            this.active(&ctx)?;
            result
        }).await.map_err(|_| HistoryError::Storage(Arc::new(ch_db::Error::Invalid("history DB worker failed".into()))))?
    }

    pub(super) async fn global_read<T, F>(&self, f: F) -> Result<T>
    where T:Send+'static, F:FnOnce(&rusqlite::Connection)->Result<T>+Send+'static {
        let permit = self.db_permit()?;
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            #[cfg(target_os="linux")]
            let _references=this.0.references.read();
            if this.0.stop.is_cancelled() {return Err(worker_failed());}
            let ingest = lock(&this.0.ingest);
            f(ingest.db().conn())
        }).await.map_err(|_| HistoryError::Storage(Arc::new(ch_db::Error::Invalid("history DB worker failed".into()))))?
    }
}

fn worker_failed() -> HistoryError {
    HistoryError::Storage(Arc::new(ch_db::Error::Invalid("history DB worker failed".into())))
}
