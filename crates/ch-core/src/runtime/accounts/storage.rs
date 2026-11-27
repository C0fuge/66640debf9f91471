//! In-process authority over the actual account storage directory, including ID aliases.
//! A chooser keeps only the generation token. Storage users retain the read guard.
use super::{AccountRow, Accounts, Arc, HashMap, Mutex, PoisonError, Result, RuntimeError};
use crate::host::HostConfig;
use rusqlite::Connection;
use std::{hash::{Hash, Hasher}, fmt};
use std::path::{Path, PathBuf};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use tokio_util::sync::CancellationToken;

// Only Accounts can construct this actual-issuer view. A Host retains Weak, not
// its Accounts->Host backreference, and upgrades only during bounded local work.
#[cfg(target_os = "linux")]
#[derive(Clone)]
pub(crate) struct HostStorageView(std::sync::Weak<super::Inner>);

#[cfg(target_os = "linux")]
impl HostStorageView {
    pub(crate) fn same_issuer(&self, other: &Self) -> bool { std::sync::Weak::ptr_eq(&self.0, &other.0) }
    pub(crate) fn belongs_to(&self, host: &crate::host::HostHandle) -> bool {
        self.0.upgrade().is_some_and(|inner| inner.stage_mutations.is_some()
            && inner.owner_jobs.is_some() && inner.host.same_instance(host))
    }
    pub(crate) fn current(&self, observed: &AccountStorageObservation) -> bool {
        self.0.upgrade().is_some_and(|inner| Accounts(inner).owns_storage_observation(observed))
    }
    // Called only on the actual counted directory metadata worker. No read
    // lease is acquired, so a removal writer can never strand this capture.
    pub(crate) fn capture(&self, plugin: &str, account: &str) -> Result<(AccountStorageObservation, super::LogicalOwnerObservation)> {
        let inner = self.0.upgrade().ok_or_else(|| unknown(account))?;
        let db = inner.db.lock().unwrap_or_else(PoisonError::into_inner);
        let observed = observation_at(&inner, &db, account)?;
        if observed.owner.plugin_id != plugin || observed.is_revoked()
            || !exclusive(db.conn(), inner.host.config(), &observed.owner, &observed.root)?
            || !inner.storage.current(&observed.root, &observed.gate)
        { return Err(unknown(account)); }
        let logical = inner.logical_owners.capture(&observed.owner).ok_or_else(|| unknown(account))?;
        Ok((observed, logical))
    }
    pub(crate) async fn reacquire(&self, observed: &AccountStorageObservation, stopped: &CancellationToken)
        -> Result<AccountStorageBinding> {
        let inner = self.0.upgrade().ok_or_else(|| unknown(&observed.owner.account_id))?;
        Accounts(inner).binding_for_observation_until(observed, stopped).await
    }
}

fn observation_at(inner: &super::Inner, db: &ch_db::Db, account: &str) -> Result<AccountStorageObservation> {
    #[cfg(test)] {
        let hook = inner.storage.2.lock().unwrap_or_else(PoisonError::into_inner).take();
        if let Some(hook) = hook { hook(); }
    }
    let owner = ch_db::get_account(db.conn(), account)?.ok_or_else(|| unknown(account))?;
    let root = directory(inner.host.config(), account);
    let gate = inner.storage.get(&root);
    Ok(AccountStorageObservation { owner, root, gate })
}

#[derive(Default)]
pub(super) struct Gates(Mutex<HashMap<PathBuf, Arc<Gate>>>,
    #[cfg(test)] Mutex<Option<Arc<CaptureHook>>>,
    // Separate one-shot hook inside an actual metadata worker. The ABA hook
    // above deliberately remains outside the DB mutex so removal can proceed.
    #[cfg(test)] Mutex<Option<Arc<CaptureHook>>>,
    // One-shot observation of the actual exact-Gate read wait, independent
    // from the original capture/metadata callback barriers above.
    #[cfg(test)] Mutex<Option<Arc<CaptureHook>>>);
#[cfg(test)]
type CaptureHook = dyn Fn() + Send + Sync;
#[derive(Default)]
pub(super) struct Gate {
    pub(super) drain: Arc<RwLock<()>>,
    pub(super) token: CancellationToken,
}

impl Gates {
    #[cfg(target_os = "linux")]
    fn replace_revoked(&self,root:&Path,old:&Arc<Gate>) -> Option<Arc<Gate>> {
        let mut gates=self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if !old.token.is_cancelled() || !gates.get(root).is_some_and(|g|Arc::ptr_eq(g,old)) { return None; }
        let fresh=Arc::new(Gate::default());gates.insert(root.to_owned(),fresh.clone());Some(fresh)
    }
    pub(super) fn get(&self, root: &Path) -> Arc<Gate> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(root.to_owned())
            .or_default()
            .clone()
    }
    pub(super) fn revoke_existing(&self, root: &Path) {
        if let Some(gate) = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(root)
        {
            gate.token.cancel();
        }
    }
    pub(super) fn retire(&self, root: &Path, old: &Arc<Gate>) {
        let mut gates = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if gates.get(root).is_some_and(|gate| Arc::ptr_eq(gate, old)) {
            gates.remove(root);
        }
    }
    fn current(&self, root: &Path, expected: &Arc<Gate>) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(root)
            .is_some_and(|g| Arc::ptr_eq(g, expected))
    }
}
pub(super) fn directory(config: &HostConfig, id: &str) -> PathBuf {
    let mut path = config.state_dir(id);
    path.pop(); // HostConfig appends the fixed child "state".
    path
}
fn valid_root(config: &HostConfig, root: &Path) -> bool {
    root.parent() == Some(config.data_dir.join("accounts").as_path())
        && root.file_name().is_some_and(|name| name.len() <= 255)
}
pub(super) fn require_absent(config: &HostConfig, id: &str, root: &Path) -> Result<()> {
    if valid_root(config, root)
        && std::fs::symlink_metadata(root).is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound)
    {
        return Ok(());
    }
    Err(RuntimeError::AccountExists(id.into()))
}
pub(super) fn same(row: &AccountRow, expected: &AccountRow) -> bool {
    row.account_id == expected.account_id
        && row.plugin_id == expected.plugin_id
        && row.created_at == expected.created_at
}
fn unknown(id: &str) -> RuntimeError {
    RuntimeError::UnknownAccount(id.into())
}
pub(super) fn check_owner(conn: &Connection, expected: &AccountRow) -> Result<()> {
    if ch_db::get_account(conn, &expected.account_id)?.is_some_and(|row| same(&row, expected)) {
        Ok(())
    } else {
        Err(unknown(&expected.account_id))
    }
}

// Temporary unsupported-state policy: history has no reviewed ownership/closure
// implementation yet, so ANY dispatch/retirement conservatively blocks removal.
// These are constant-size/indexed probes, never unbounded orphan/JSON scans.
pub(super) fn managed_in_use(conn:&Connection,account:&str) -> Result<bool> {
    Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM managed_source WHERE account_id=?1)
        OR EXISTS(SELECT 1 FROM managed_dispatch LIMIT 1)
        OR EXISTS(SELECT 1 FROM managed_retirement LIMIT 1)",[account],|row|row.get(0))
        .map_err(ch_db::Error::from)?)
}
// Bounded examination of identifiers only. A source ID longer than 256 bytes
// cannot sanitize to a <=255-byte component (replacements never shorten it).
pub(super) fn exclusive(
    conn: &Connection,
    config: &HostConfig,
    expected: &AccountRow,
    root: &Path,
) -> Result<bool> {
    if !valid_root(config, root) {
        return Ok(false);
    }
    let mut stmt=conn.prepare("SELECT substr(CAST(account_id AS BLOB), 1, 257) FROM account WHERE account_id != ?1 LIMIT 4097")
        .map_err(ch_db::Error::from)?;
    let mut rows = stmt
        .query([&expected.account_id])
        .map_err(ch_db::Error::from)?;
    let mut count = 0;
    while let Some(row) = rows.next().map_err(ch_db::Error::from)? {
        count += 1;
        if count > 4096 {
            return Ok(false);
        }
        let id: Vec<u8> = row.get(0).map_err(ch_db::Error::from)?;
        if id.len() > 256 {
            continue;
        }
        let Ok(id) = std::str::from_utf8(&id) else {
            return Ok(false);
        };
        if directory(config, id) == root {
            return Ok(false);
        }
    }
    Ok(true)
}
/// Exact persisted owner and published storage generation, issued only by Accounts.
/// It owns a read lease through actual bounded filesystem work; it is not Clone.
///
/// ```compile_fail
/// use ch_core::runtime::AccountStorageBinding;
/// let fabricated = AccountStorageBinding {};
/// ```
pub struct AccountStorageBinding {
    #[cfg(target_os = "linux")]
    mutations: Option<crate::managed_stage::StageMutations>,
    owner: AccountRow,
    root: PathBuf,
    gate: Arc<Gate>,
    guard: Arc<OwnedRwLockReadGuard<()>>,
}

/// Lease-free identity of one Accounts-issued owner generation. It retains no
/// storage read guard and cannot authorize I/O without fresh exact reacquisition.
#[derive(Clone)]
pub struct AccountStorageObservation {
    owner: AccountRow,
    root: PathBuf,
    gate: Arc<Gate>,
}
impl fmt::Debug for AccountStorageObservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccountStorageObservation").finish_non_exhaustive()
    }
}
impl PartialEq for AccountStorageObservation {
    fn eq(&self, other: &Self) -> bool {
        self.owner.account_id == other.owner.account_id && Arc::ptr_eq(&self.gate, &other.gate)
    }
}
impl Eq for AccountStorageObservation {}
impl Hash for AccountStorageObservation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.owner.account_id.hash(state);
        Arc::as_ptr(&self.gate).hash(state);
    }
}
impl AccountStorageObservation {
    // Descriptive configured namespace key only; this is not a filesystem proof.
    #[cfg(target_os = "linux")]
    pub(crate) fn namespace(&self) -> &Path { &self.root }
    pub fn owner(&self) -> &AccountRow { &self.owner }
    pub fn revocation(&self) -> CancellationToken { self.gate.token.clone() }
    pub fn is_revoked(&self) -> bool { self.gate.token.is_cancelled() }
    /// Recheck the original generation and persisted owner inside a DB snapshot.
    pub(crate) fn check(&self, conn: &Connection) -> Result<()> {
        if self.is_revoked() { return Err(unknown(&self.owner.account_id)); }
        check_owner(conn, &self.owner)?;
        if self.is_revoked() { return Err(unknown(&self.owner.account_id)); }
        Ok(())
    }
}

impl AccountStorageBinding {
    pub fn owner(&self) -> &AccountRow { &self.owner }
    pub fn revocation(&self) -> CancellationToken { self.gate.token.clone() }
    pub fn is_revoked(&self) -> bool { self.gate.token.is_cancelled() }
    pub fn observation(&self) -> AccountStorageObservation {
        AccountStorageObservation { owner:self.owner.clone(), root:self.root.clone(), gate:self.gate.clone() }
    }
    #[cfg(target_os = "linux")]
    pub(crate) fn uploads(&self) -> PathBuf { self.root.join("uploads") }
    #[cfg(target_os = "linux")]
    pub(crate) fn retain_binding(&self) -> Self {
        Self { owner: self.owner.clone(), root: self.root.clone(), gate: self.gate.clone(), guard: self.guard.clone(), mutations: self.mutations.clone() }
    }
    #[cfg(target_os = "linux")]
    pub(crate) fn stage_mutations(&self) -> crate::managed_stage::Result<&crate::managed_stage::StageMutations> {
        self.mutations.as_ref().ok_or(crate::managed_stage::Error::TargetChanged)
    }
}

impl Accounts {
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn host_capture_checkpoint(&self, hook: Arc<CaptureHook>) {
        *self.0.storage.2.lock().unwrap() = Some(hook);
    }
    #[cfg(target_os = "linux")]
    pub(crate) fn host_storage_view(&self) -> Result<HostStorageView> {
        if self.0.stage_mutations.is_none() || self.0.owner_jobs.is_none() { return Err(unknown("")); }
        Ok(HostStorageView(Arc::downgrade(&self.0)))
    }
    #[cfg(target_os = "linux")]
    pub(super) fn removal_observation(&self,row:&AccountRow,root:&Path,gate:&Arc<Gate>) -> Result<AccountStorageObservation> {
        if root!=directory(self.0.host.config(),&row.account_id)
            || !self.0.storage.current(root,gate)
        { return Err(unknown(&row.account_id)); }
        Ok(AccountStorageObservation {owner:row.clone(),root:root.to_owned(),gate:gate.clone()})
    }

    // Intrinsic issuer/current-generation check only, not a read lease or posting
    // permission. A matching row/root from another Accounts factory is insufficient.
    #[cfg(target_os = "linux")]
    pub(crate) fn owns_storage_observation(&self, observed: &AccountStorageObservation) -> bool {
        !observed.is_revoked()
            && observed.root == directory(self.0.host.config(), &observed.owner.account_id)
            && self.0.storage.current(&observed.root, &observed.gate)
            && !observed.is_revoked()
    }

    /// Capture owner and Gate together, then release the short capture lease.
    /// Callers may retain this observation across network I/O without blocking removal.
    pub async fn storage_observation_for(&self, account_id: &str) -> Result<AccountStorageObservation> {
        Ok(self.storage_binding_for(account_id).await?.observation())
    }

    /// Acquire a bounded-use lease for exactly the originally observed Gate.
    /// No matching timestamp or replacement generation can substitute for it.
    pub async fn storage_binding_for_observation(&self, observed: &AccountStorageObservation) -> Result<AccountStorageBinding> {
        self.binding_for_observation_until(observed, &CancellationToken::new()).await
    }

    async fn binding_for_observation_until(&self, observed: &AccountStorageObservation, stopped: &CancellationToken)
        -> Result<AccountStorageBinding> {
        if stopped.is_cancelled() { return Err(unknown(&observed.owner.account_id)); }
        if observed.is_revoked() || !self.0.storage.current(&observed.root, &observed.gate) {
            return Err(unknown(&observed.owner.account_id));
        }
        let guard = tokio::select! {
            biased;
            _ = stopped.cancelled() => return Err(unknown(&observed.owner.account_id)),
            _ = observed.gate.token.cancelled() => return Err(unknown(&observed.owner.account_id)),
            guard = async {
                #[cfg(test)] {
                    let hook = self.0.storage.3.lock().unwrap_or_else(PoisonError::into_inner).take();
                    if let Some(hook) = hook { hook(); }
                }
                observed.gate.drain.clone().read_owned().await
            } => Arc::new(guard),
        };
        let owner = observed.owner.clone();
        let root = observed.root.clone();
        let inner = self.0.clone();
        let lease = guard.clone();
        self.db(move |db| {
            let _lease = lease;
            check_owner(db.conn(), &owner)?;
            if !exclusive(db.conn(), inner.host.config(), &owner, &root)? {
                return Err(unknown(&owner.account_id));
            }
            Ok(())
        }).await?;
        if stopped.is_cancelled() || observed.is_revoked() || !self.0.storage.current(&observed.root, &observed.gate) {
            return Err(unknown(&observed.owner.account_id));
        }
        Ok(AccountStorageBinding { owner:observed.owner.clone(), root:observed.root.clone(), gate:observed.gate.clone(), guard,
            #[cfg(target_os = "linux")]
            mutations: self.0.stage_mutations.clone() })
    }

    /// Pin a persisted account's configured storage against destructive teardown.
    /// The token revokes queued work. Retain the guard through actual I/O settlement.
    /// Native choosers keep only the token and recheck it after acquiring a fresh guard.
    pub async fn storage_lease(
        &self,
        expected: &AccountRow,
    ) -> Result<(CancellationToken, OwnedRwLockReadGuard<()>)> {
        let root = directory(self.0.host.config(), &expected.account_id);
        let owner = expected.clone();
        let inner = self.0.clone();
        let path = root.clone();
        let gate = self
            .db(move |db| {
                check_owner(db.conn(), &owner)?;
                Ok(inner.storage.get(&path))
            })
            .await?;
        if gate.token.is_cancelled() {
            return Err(unknown(&expected.account_id));
        }
        let guard = gate.drain.clone().read_owned().await;
        let owner = expected.clone();
        let inner = self.0.clone();
        let path = root.clone();
        self.db(move |db| {
            check_owner(db.conn(), &owner)?;
            if !exclusive(db.conn(), inner.host.config(), &owner, &path)? {
                return Err(unknown(&owner.account_id));
            }
            Ok(())
        })
        .await?;
        if gate.token.is_cancelled() || !self.0.storage.current(&root, &gate) {
            return Err(unknown(&expected.account_id));
        }
        Ok((gate.token.clone(), guard))
    }

    /// Capture a current owner and its published storage generation in one account-DB read.
    /// Unlike matching a previously read row by millisecond creation time, this cannot
    /// substitute a recreated owner's gate when account metadata happens to be identical.
    pub async fn storage_lease_for(
        &self,
        account_id: &str,
    ) -> Result<(AccountRow, CancellationToken, OwnedRwLockReadGuard<()>)> {
        let (owner, _, gate, guard) = self.capture_storage(account_id).await?;
        Ok((owner, gate.token.clone(), guard))
    }

    /// Accounts-owned nonforgeable scope for the managed-stage verifier.
    pub async fn storage_binding_for(&self, account_id: &str) -> Result<AccountStorageBinding> {
        let (owner, root, gate, guard) = self.capture_storage(account_id).await?;
        Ok(AccountStorageBinding { owner, root, gate, guard: Arc::new(guard),
            #[cfg(target_os = "linux")]
            mutations: self.0.stage_mutations.clone() })
    }

    async fn capture_storage(
        &self, account_id: &str,
    ) -> Result<(AccountRow, PathBuf, Arc<Gate>, OwnedRwLockReadGuard<()>)> {
        let id = account_id.to_owned();
        let inner = self.0.clone();
        let (owner, root, gate) = self.db(move |db| {
            let observed = observation_at(&inner, db, &id)?;
            Ok((observed.owner, observed.root, observed.gate))
        }).await?;
        #[cfg(test)] {
            let hook = self.0.storage.1.lock().unwrap_or_else(PoisonError::into_inner).clone();
            if let Some(hook) = hook { hook(); }
        }
        if gate.token.is_cancelled() { return Err(unknown(&owner.account_id)); }
        let guard = gate.drain.clone().read_owned().await;
        let expected = owner.clone();
        let path = root.clone();
        let inner = self.0.clone();
        self.db(move |db| {
            check_owner(db.conn(), &expected)?;
            if !exclusive(db.conn(), inner.host.config(), &expected, &path)? {
                return Err(unknown(&expected.account_id));
            }
            Ok(())
        }).await?;
        if gate.token.is_cancelled() || !self.0.storage.current(&root, &gate) {
            return Err(unknown(&owner.account_id));
        }
        Ok((owner, root, gate, guard))
    }

    // Called with the lifecycle and membership guards and the revoked storage writer.
    // A definite, explicitly confirmed rollback can authorize a fresh token.
    // A failed rollback, unknown commit or task failure leaves the gate cancelled.
    pub(super) async fn delete_owned_row(
        &self,
        row: &AccountRow,
        root: &Path,
        gate: &Arc<Gate>,
    ) -> Result<bool> {
        let owner = row.clone();
        let path = root.to_owned();
        let expected=gate.clone();
        let me=self.clone();
        let result = self
            .db(move |db| me.delete_row_at(db,&owner,&path,&expected))
            .await?;
        if result.is_err() {
            self.0.storage.retire(root, gate);
        }
        result
    }

    // Inner Err is a positively completed rollback; outer Err is not rearm proof.
    pub(super) fn delete_row_at(&self,db:&ch_db::Db,row:&AccountRow,root:&Path,gate:&Arc<Gate>) -> Result<Result<bool>> {
        self.delete_row_checked(db,row,root,gate,||Ok(()))
    }

    // Core removal supplies the actual fresh catalog veto. Setup rollback's
    // legacy helper supplies no catalog permission and retains all SQL guards.
    pub(super) fn delete_row_checked(&self,db:&ch_db::Db,row:&AccountRow,root:&Path,gate:&Arc<Gate>,
        veto:impl FnOnce()->Result<()>) -> Result<Result<bool>> {
        let tx=db.transaction()?;
        check_owner(&tx,row)?;
        if !self.0.storage.current(root,gate) { return Err(unknown(&row.account_id)); }
        let sole=exclusive(&tx,self.0.host.config(),row,root)?;
        // Reserve the actual deleting writer before the LAST negative-source
        // check. Preflight alone cannot protect against a concurrent raw writer.
        if managed_in_use(&tx,&row.account_id)? {
            self.rollback_delete(tx, db, row)?;
            return Ok(Err(RuntimeError::SourceInUse));
        }
        if let Err(error)=veto() {
            self.rollback_delete(tx, db, row)?;
            return Ok(Err(error));
        }
        match ch_db::delete_account(&tx,&row.account_id) {
            Ok(true)=>{
                #[cfg(all(test, target_os = "linux"))]
                if let Some(hook) = self.0.delete_commit_checkpoint.lock().unwrap().take() { hook(&tx); }
                if let Err(error) = tx.commit() {
                    self.0.logical_owners.uncertain(&row.account_id);
                    return Err(ch_db::Error::from(error).into());
                }
                self.0.logical_owners.deleted(&row.account_id);
                Ok(Ok(sole))
            }
            result=>{
                let error=match result {Err(e)=>RuntimeError::Db(e),_=>unknown(&row.account_id)};
                self.rollback_delete(tx, db, row)?;
                Ok(Err(error))
            }
        }
    }

    fn rollback_delete(&self, tx: rusqlite::Transaction<'_>, db: &ch_db::Db, row: &AccountRow) -> Result<()> {
        if let Err(error) = tx.rollback() {
            self.0.logical_owners.uncertain(&row.account_id);
            return Err(ch_db::Error::from(error).into());
        }
        if let Err(error) = check_owner(db.conn(),row) {
            self.0.logical_owners.uncertain(&row.account_id);
            return Err(error);
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    pub(super) fn rearm_refused(&self,scope:&crate::managed_sources::BlockingWork<'_>,row:&AccountRow,
        root:&Path,gate:&Arc<Gate>,anchor:&crate::managed_stage::AccountRootAnchor,
        close:&crate::managed_sources::OwnerClose) -> Result<()> {
        if !close.matches_anchor(anchor) || anchor.check().is_err() { return Err(RuntimeError::SourceInUse); }
        let fresh=self.owner_db(scope,|db| {
            let tx=db.transaction()?;
            check_owner(&tx,row)?;
            if !exclusive(&tx,self.0.host.config(),row,root)? || !self.0.storage.current(root,gate) {
                return Err(RuntimeError::SourceInUse);
            }
            tx.commit().map_err(ch_db::Error::from)?;
            let next=self.0.storage.replace_revoked(root,gate).ok_or(RuntimeError::SourceInUse)?;
            Ok(AccountStorageObservation {owner:row.clone(),root:root.to_owned(),gate:next})
        })?;
        if anchor.check().is_err() || !self.owns_storage_observation(&fresh) || close.rearmed(&fresh,anchor).is_err() {
            fresh.revocation().cancel();
            return Err(RuntimeError::SourceInUse);
        }
        Ok(())
    }
}

#[cfg(test)]
mod capture_tests {
    use super::*;
    use crate::{host::MemorySecretStore, runtime::{Core, CoreConfig, NewAccount}};
    use std::sync::Condvar;
    use tokio::sync::Notify;

    #[cfg(target_os = "linux")]
    async fn directory_core() -> (tempfile::TempDir, Core) {
        let temp = tempfile::tempdir().unwrap(); let root = temp.path().join("data");
        let plugin = root.join("plugins/fixture"); std::fs::create_dir_all(&plugin).unwrap();
        std::fs::write(plugin.join("peer.py"), include_bytes!("../../../tests/fixtures/account_directories.py")).unwrap();
        std::fs::write(plugin.join("plugin.json"), serde_json::json!({"id":"fixture","name":"Fixture",
            "version":"0.0.1","api_version":"1.2.0","entry":["python3","-I","./peer.py"],
            "brand_color":"#123456","multi_account":true,"auth_methods":["none"]}).to_string()).unwrap();
        let mut config = CoreConfig::new(&root, Arc::new(MemorySecretStore::new()));
        config.autostart = false; config.host = HostConfig::fast(&root, 50);
        for key in ["HOME", "CH_DATA_DIR", "XDG_DATA_HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME"] {
            config.host.env.insert(key.into(), temp.path().join(key).to_string_lossy().into_owned());
        }
        config.host.env.insert("CH_TEST_ACCOUNT_STARTS".into(), temp.path().join("starts").to_string_lossy().into_owned());
        let core = Core::open(config).await.unwrap();
        core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
        (temp, core)
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exact_host_read_wait_is_cancelled_before_the_removal_writer_is_released() {
        for close_core in [false, true] {
            let (_temp, core) = directory_core().await;
            let root = directory(core.host().config(), "owner");
            let gate = core.accounts().0.storage.get(&root);
            let writer = gate.drain.write().await;
            let (send, recv) = tokio::sync::oneshot::channel(); let send = Mutex::new(Some(send));
            *core.accounts().0.storage.3.lock().unwrap() = Some(Arc::new(move || {
                let _ = send.lock().unwrap().take().unwrap().send(());
            }));
            let host = core.host().clone();
            let start = tokio::spawn(async move { host.start_account("fixture", "owner", crate::host::AccountSpec::default()).await });
            tokio::time::timeout(std::time::Duration::from_secs(5), recv).await.unwrap().unwrap();
            if close_core { core.managed_sources().close(); } else { gate.token.cancel(); }
            // Host has to process this while the SAME original writer is still
            // held, rather than wait for a guard/removal deadlock to disappear.
            tokio::time::timeout(std::time::Duration::from_secs(5), core.host().stop_account("fixture", "owner")).await.unwrap().unwrap();
            assert!(matches!(tokio::time::timeout(std::time::Duration::from_secs(5), start).await.unwrap().unwrap(),
                Err(crate::host::HostError::ShuttingDown)));
            assert!(!root.exists());
            assert_eq!(core.managed_sources().catalog().view().writers(), 0);
            drop(writer); core.shutdown().await;
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn host_weak_view_cannot_bind_a_standalone_or_second_same_root_host() {
        let (_temp, core) = directory_core().await;
        let view = core.accounts().host_storage_view().unwrap();
        assert!(view.belongs_to(core.host()));
        assert!(core.host().bind_storage(view.clone()).is_err()); // Already once-bound.
        let foreign = crate::host::HostHandle::for_core(core.host().config().clone(), core.ingest().clone(),
            Arc::new(MemorySecretStore::new()), &core.managed_sources());
        assert!(!view.belongs_to(&foreign));
        assert!(foreign.bind_storage(view).is_err());
        let standalone = Accounts::new(core.open_db().unwrap(), core.host().clone(), Arc::new(MemorySecretStore::new()),
            crate::runtime::ChangeBus::new(8));
        assert!(standalone.host_storage_view().is_err());
        core.shutdown().await;
    }

    #[tokio::test(flavor="multi_thread", worker_threads=3)]
    async fn dropped_history_capture_keeps_drain_pending_until_actual_metadata_worker_settles() {
        use crate::history::{ChatKey, History, HistoryConfig};
        use std::time::Duration;
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("plugins/fixture");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("plugin.json"), serde_json::json!({"id":"fixture","name":"Fixture","version":"0.0.1","api_version":"1.2.0",
            "entry":["python3","-B","-c","pass"],"brand_color":"#123456","multi_account":true,"auth_methods":["none"]}).to_string()).unwrap();
        let mut config=CoreConfig::new(temp.path(),Arc::new(MemorySecretStore::new())); config.autostart=false;
        let core=Core::open(config).await.unwrap();
        core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
        let arrived=Arc::new(Notify::new());
        let latch=Arc::new((Mutex::new(false),Condvar::new()));
        let signal=arrived.clone();let blocked=latch.clone();
        *core.accounts().0.storage.2.lock().unwrap()=Some(Arc::new(move|| {
            signal.notify_one();
            let mut released=blocked.0.lock().unwrap();
            while !*released {released=blocked.1.wait(released).unwrap();}
        }));
        let history=History::from_core(&core,HistoryConfig::default());
        let request={let history=history.clone();tokio::spawn(async move {
            history.refresh(&ChatKey::new("owner","chat"),10).await
        })};
        tokio::time::timeout(Duration::from_secs(5),arrived.notified()).await.unwrap();
        // The hook is running inside the actual Accounts DB closure, before its
        // read has finished. Cancelling the outer request must not settle it.
        request.abort();assert!(request.await.unwrap_err().is_cancelled());
        history.close();
        let mut draining={let history=history.clone();tokio::spawn(async move {history.drain().await})};
        let waited=tokio::time::timeout(Duration::from_millis(50),&mut draining).await.is_err();
        *latch.0.lock().unwrap()=true;latch.1.notify_all();
        assert!(waited,"drain returned while actual account metadata work remained blocked");
        tokio::time::timeout(Duration::from_secs(5),draining).await.unwrap().unwrap();
        assert!(history.refresh(&ChatKey::new("owner","chat"),10).await.is_err());
        let db=core.open_db().unwrap();
        assert!(ch_db::get_account(db.conn(),"owner").unwrap().is_some());
        for table in ["conversation","message","history_state","history_gap"] {
            let count:i64=db.conn().query_row(&format!("SELECT COUNT(*) FROM {table}"),[],|row|row.get(0)).unwrap();
            assert_eq!(count,0,"closed History must not write {table}");
        }
        drop(db);
        core.shutdown().await;
    }

    #[tokio::test(flavor="multi_thread", worker_threads=3)]
    async fn lease_free_observation_survives_no_guard_and_cannot_rebind_equal_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("plugins/fixture");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("plugin.json"), serde_json::json!({"id":"fixture","name":"Fixture","version":"0.0.1","api_version":"1.2.0",
            "entry":["python3","-B","-c","pass"],"brand_color":"#123456","multi_account":true,"auth_methods":["none"]}).to_string()).unwrap();
        let mut config=CoreConfig::new(temp.path(),Arc::new(MemorySecretStore::new())); config.autostart=false;
        let core=Core::open(config).await.unwrap();
        core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
        core.open_db().unwrap().conn().execute("UPDATE account SET created_at=1 WHERE account_id='owner'",[]).unwrap();
        let old=core.accounts().storage_observation_for("owner").await.unwrap();
        assert_eq!(old,old.clone());
        // A retained observation alone must not hold removal behind a network wait.
        tokio::time::timeout(std::time::Duration::from_secs(5),core.accounts().remove("owner")).await.unwrap().unwrap();
        assert!(old.is_revoked());
        core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
        core.open_db().unwrap().conn().execute("UPDATE account SET created_at=1 WHERE account_id='owner'",[]).unwrap();
        let fresh=core.accounts().storage_observation_for("owner").await.unwrap();
        assert_ne!(old,fresh);
        assert!(matches!(core.accounts().storage_binding_for_observation(&old).await,Err(RuntimeError::UnknownAccount(_))));
        let binding=core.accounts().storage_binding_for_observation(&fresh).await.unwrap();
        assert_eq!(binding.observation(),fresh);
        let accounts=core.accounts().clone();
        let removing=tokio::spawn(async move{accounts.remove("owner").await});
        tokio::time::timeout(std::time::Duration::from_secs(5),fresh.revocation().cancelled()).await.unwrap();
        assert!(!removing.is_finished(),"only the newly acquired binding owns a storage lease");
        drop(binding);
        tokio::time::timeout(std::time::Duration::from_secs(5),removing).await.unwrap().unwrap().unwrap();
        core.shutdown().await;
    }

    #[tokio::test(flavor="multi_thread", worker_threads=4)]
    async fn captured_gate_cannot_follow_same_id_recreation_with_identical_owner_metadata() {
        assert_same_metadata_recreation(false).await;
    }

    #[tokio::test(flavor="multi_thread", worker_threads=4)]
    async fn opaque_stage_binding_cannot_follow_same_metadata_recreation() {
        assert_same_metadata_recreation(true).await;
    }

    async fn assert_same_metadata_recreation(opaque: bool) {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("plugins/fixture");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("plugin.json"), serde_json::json!({"id":"fixture","name":"Fixture","version":"0.0.1","api_version":"1.1.0",
            "entry":["python3","-B","-c","pass"],"brand_color":"#123456","multi_account":true,"auth_methods":["none"]}).to_string()).unwrap();
        let mut config=CoreConfig::new(temp.path(),Arc::new(MemorySecretStore::new())); config.autostart=false;
        let core=Arc::new(Core::open(config).await.unwrap());
        core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
        core.open_db().unwrap().conn().execute("UPDATE account SET created_at=1 WHERE account_id='owner'",[]).unwrap();
        let arrived=Arc::new(Notify::new()); let state=Arc::new((Mutex::new(false),Condvar::new()));
        let signal=arrived.clone();let latch=state.clone();
        *core.accounts().0.storage.1.lock().unwrap()=Some(Arc::new(move|| {
            signal.notify_one();let mut released=latch.0.lock().unwrap();while !*released {released=latch.1.wait(released).unwrap();}
        }));
        let accounts=core.accounts().clone();
        let pending=tokio::spawn(async move {
            if opaque { accounts.storage_binding_for("owner").await.map(|_| ()) }
            else { accounts.storage_lease_for("owner").await.map(|_| ()) }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5),arrived.notified()).await.unwrap();
        core.accounts().remove("owner").await.unwrap();
        core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
        core.open_db().unwrap().conn().execute("UPDATE account SET created_at=1 WHERE account_id='owner'",[]).unwrap();
        *state.0.lock().unwrap()=true;state.1.notify_all();
        assert!(matches!(pending.await.unwrap(),Err(RuntimeError::UnknownAccount(_))));
        *core.accounts().0.storage.1.lock().unwrap()=None;
        let (_,token,guard)=core.accounts().storage_lease_for("owner").await.unwrap();assert!(!token.is_cancelled());drop(guard);
        core.shutdown().await;
    }

    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    #[cfg(target_os="linux")]
    async fn refusal_does_not_replace_a_different_published_gate_with_identical_owner_metadata() {
        let temp=tempfile::tempdir().unwrap();let dir=temp.path().join("plugins/fixture");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("plugin.json"),serde_json::json!({"id":"fixture","name":"Fixture",
            "version":"0.0.1","api_version":"1.2.0","entry":["python3","-I","-c","pass"],
            "brand_color":"#123456","multi_account":true,"auth_methods":["none"]}).to_string()).unwrap();
        let mut config=CoreConfig::new(temp.path(),Arc::new(MemorySecretStore::new()));config.autostart=false;
        let core=Core::open(config).await.unwrap();
        let row=core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
        let root=directory(core.host().config(),"owner");std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("bytes"),b"preserve").unwrap();
        core.open_db().unwrap().conn().execute("INSERT INTO managed_source(stage_id,client_id,account_id,
            plugin_id,account_created_at,chat_id,path,fingerprint_json,intent_json,created_at,updated_at)
            VALUES('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
            'owner','fixture',?1,'chat',?2,'{}','{}',0,0)",
            rusqlite::params![row.created_at,root.join("bytes").to_str()]).unwrap();
        let original=core.accounts().0.storage.get(&root);
        let replacement=Arc::new(Mutex::new(None));let saved=replacement.clone();
        let accounts=core.accounts().clone();let path=root.clone();
        // Fault injection uses the real private map publisher, not a fabricated
        // observation/root proof. Persisted owner fields stay exactly identical.
        core.accounts().owner_close_checkpoint(Arc::new(move|phase| {
            if phase=="before-rearm" {
                let previous=accounts.0.storage.get(&path);
                accounts.0.storage.retire(&path,&previous);
                *saved.lock().unwrap()=Some(accounts.0.storage.get(&path));
            }
        }));
        assert!(matches!(core.accounts().remove("owner").await,Err(RuntimeError::SourceInUse)));
        let fresh=replacement.lock().unwrap().clone().unwrap();
        assert!(original.token.is_cancelled());
        assert!(Arc::ptr_eq(&core.accounts().0.storage.get(&root),&fresh));
        assert!(core.accounts().get("owner").await.unwrap().is_some_and(|owner|owner.created_at==row.created_at));
        assert_eq!(std::fs::read(root.join("bytes")).unwrap(),b"preserve");
        assert_eq!(core.managed_sources().owner_barriers().closing_count(),1);
        assert!(matches!(core.accounts().remove("owner").await,Err(RuntimeError::SourceInUse)));
        assert!(fresh.token.is_cancelled());
        assert_eq!(core.managed_sources().owner_barriers().closing_count(),1);
        core.accounts().owner_close_checkpoint(Arc::new(|_|{}));
        core.shutdown().await;
    }
}
