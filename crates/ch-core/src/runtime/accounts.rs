//! The accounts service: the `account` table, per-account settings and `x-secret` routing,
//! and starting accounts on the host.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};

mod storage;
mod owner;
#[cfg(all(test, target_os = "linux"))]
mod metadata_tests;
pub(crate) use owner::{LogicalOwnerObservation, LogicalOwners};
pub use storage::{AccountStorageBinding, AccountStorageObservation};
#[cfg(target_os = "linux")]
pub(crate) use storage::HostStorageView;

use ch_db::types::{AccountRow, PluginRow};
use ch_db::{Db, now_millis};
use ch_protocol::methods::AutoDownload;
use serde_json::{Map, Value};
use tracing::{debug, info, warn};

use super::bus::{AccountChange, ChangeBus, CoreChange};
use super::error::{Result, RuntimeError, SettingsProblem};
use crate::host::settings::{fill_defaults, secret_keys};
use crate::host::{
    AccountInfo, AccountPhase, AccountSpec, HostError, HostHandle, PluginSnapshot, SecretStore,
    StateChange,
};

type Schema = Map<String, Value>;

/// The [`SecretStore`] "plugin" namespace that holds an account's `x-secret` setting values.
///
/// Runtime secrets (`secrets.set`) live under the real plugin id and go to the plugin in
/// `account.start.secrets`; setting secrets must not mix with them (the plugin could overwrite
/// them, and they would arrive twice), so they live under `<plugin_id>/settings`. Plugin ids
/// cannot contain `/`, so this never collides with a real plugin.
pub fn settings_secret_namespace(plugin_id: &str) -> String {
    format!("{plugin_id}/settings")
}

/// What to add.
#[derive(Debug, Clone)]
pub struct NewAccount {
    /// The plugin that serves the account.
    pub plugin_id: String,
    /// A chosen id; `None` = generate `<plugin_id>-<n>`.
    pub account_id: Option<String>,
    /// Display label.
    pub label: Option<String>,
    /// Settings, `x-secret` values included (they go to the secret store).
    pub settings: Map<String, Value>,
    /// Start the account right away (default `true`).
    pub start: bool,
}

impl NewAccount {
    /// An account on `plugin_id` with no settings, started on add.
    pub fn new(plugin_id: impl Into<String>) -> Self {
        Self {
            plugin_id: plugin_id.into(),
            account_id: None,
            label: None,
            settings: Map::new(),
            start: true,
        }
    }

    /// Use this id.
    pub fn id(mut self, account_id: impl Into<String>) -> Self {
        self.account_id = Some(account_id.into());
        self
    }

    /// Set the label.
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Set the settings (a JSON object; anything else means none).
    pub fn settings(mut self, settings: Value) -> Self {
        self.settings = match settings {
            Value::Object(m) => m,
            _ => Map::new(),
        };
        self
    }

    /// Persist only; do not start.
    pub fn no_start(mut self) -> Self {
        self.start = false;
        self
    }
}

/// An account's settings as the settings form sees them.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AccountSettings {
    /// Stored values without secrets and without schema defaults.
    pub values: Map<String, Value>,
    /// `x-secret` keys that have a stored value (the values are never returned).
    pub secrets_set: BTreeSet<String>,
}

/// Global inputs to every `account.start` that are not per-account settings. The settings
/// service sets them; changing them takes effect on the next start.
#[derive(Debug, Clone, Default)]
pub struct SpecDefaults {
    /// Ghost mode (§17).
    pub ghost: bool,
    /// Auto-download limits (§11.1).
    pub auto_download: AutoDownload,
}

#[cfg(all(test,target_os="linux"))]
type OwnerCloseCheckpoint = dyn Fn(&str)+Send+Sync;
#[cfg(all(test,target_os="linux"))]
type DeleteCommitCheckpoint = dyn Fn(&rusqlite::Connection)+Send+Sync;

struct Inner {
    logical_owners: LogicalOwners,
    #[cfg(target_os = "linux")]
    stage_mutations: Option<crate::managed_stage::StageMutations>,
    #[cfg(target_os = "linux")]
    owner_barriers: Option<crate::managed_sources::OwnerBarriers>,
    #[cfg(target_os = "linux")]
    owner_jobs: Option<Arc<crate::managed_sources::OwnedJobs>>,
    #[cfg(all(test,target_os="linux"))]
    owner_checkpoint: Mutex<Option<Arc<OwnerCloseCheckpoint>>>,
    #[cfg(all(test,target_os="linux"))]
    delete_commit_checkpoint: Mutex<Option<Arc<DeleteCommitCheckpoint>>>,
    db: Arc<Mutex<Db>>,
    host: HostHandle,
    secrets: Arc<dyn SecretStore>,
    bus: ChangeBus,
    defaults: RwLock<SpecDefaults>,
    /// One lifecycle operation at a time per account (start/stop pairs must not interleave).
    locks: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
    membership: tokio::sync::Mutex<()>,
    storage: storage::Gates,
}

/// The accounts service. Cheap to clone. Owns its own database connection.
#[derive(Clone)]
pub struct Accounts(Arc<Inner>);

impl std::fmt::Debug for Accounts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Accounts").finish_non_exhaustive()
    }
}

impl Accounts {
    /// A service over `db`, driving `host`.
    pub fn new(db: Db, host: HostHandle, secrets: Arc<dyn SecretStore>, bus: ChangeBus) -> Self {
        Self(Arc::new(Inner {
            logical_owners: LogicalOwners::new(),
            #[cfg(target_os = "linux")]
            stage_mutations: None,
            #[cfg(target_os = "linux")]
            owner_barriers: None,
            #[cfg(target_os = "linux")]
            owner_jobs: None,
            #[cfg(all(test,target_os="linux"))]
            owner_checkpoint: Mutex::default(),
            #[cfg(all(test,target_os="linux"))]
            delete_commit_checkpoint: Mutex::default(),
            db: Arc::new(Mutex::new(db)),
            host,
            secrets,
            bus,
            defaults: RwLock::new(SpecDefaults::default()),
            locks: Mutex::new(HashMap::new()),
            membership: Default::default(),
            storage: Default::default(),
        }))
    }

    // Standalone public Accounts construction never manufactures a Core hook.
    #[cfg(target_os = "linux")]
    pub(crate) fn for_core(db: Db, host: HostHandle, secrets: Arc<dyn SecretStore>, bus: ChangeBus,
        sources: &crate::managed_sources::ManagedSources, logical_owners: LogicalOwners) -> Self {
        let mut accounts = Self::new(db, host, secrets, bus);
        let inner = Arc::get_mut(&mut accounts.0).expect("new Accounts is uniquely owned");
        inner.stage_mutations = Some(sources.stage_mutations());
        inner.owner_barriers = Some(sources.owner_barriers());
        inner.owner_jobs = Some(sources.owner_jobs());
        inner.logical_owners = logical_owners;
        accounts
    }

    /// Run `f` on this service's connection on a blocking task.
    async fn db<R, F>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&Db) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let db = Arc::clone(&self.0.db);
        #[cfg(target_os="linux")]
        {
            self.0.logical_owners.run_unowned_metadata(move|| {
                let g=db.lock().unwrap_or_else(PoisonError::into_inner);f(&g)
            }).await.map_err(|_|RuntimeError::Task("account metadata work unavailable".into()))?
        }
        #[cfg(not(target_os="linux"))]
        {
        tokio::task::spawn_blocking(move || {
            let g = db.lock().unwrap_or_else(PoisonError::into_inner);
            f(&g)
        })
        .await?
        }
    }

    fn lock_for(&self, account_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.0.locks.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(lock) = locks.get(account_id).and_then(Weak::upgrade) {
            return lock;
        }
        locks.retain(|_, lock| lock.strong_count() > 0);
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(account_id.into(), Arc::downgrade(&lock));
        lock
    }

    // ---- reads ------------------------------------------------------------------------------

    /// Every account in display order.
    pub async fn list(&self) -> Result<Vec<AccountRow>> {
        self.db(|db| Ok(ch_db::list_accounts(db.conn())?)).await
    }

    /// Rows and the matching native Host metadata captured under this service's
    /// DB mutex. The returned values are descriptive, not reusable authority.
    pub async fn inventory(&self) -> Result<Vec<(AccountRow, Option<AccountInfo>)>> {
        let inner = self.0.clone();
        self.db(move |db| {
            let mut out = Vec::new();
            for row in ch_db::list_accounts(db.conn())? {
                #[cfg(target_os = "linux")]
                let info = if inner.stage_mutations.is_some() {
                    inner.logical_owners.capture(&row).and_then(|origin|
                        inner.host.account_for_origin(&origin).map(|snapshot| snapshot.info().clone()))
                } else { inner.host.account(&row.plugin_id, &row.account_id) };
                #[cfg(not(target_os = "linux"))]
                let info = inner.host.account(&row.plugin_id, &row.account_id);
                out.push((row, info));
            }
            Ok(out)
        }).await
    }

    /// One account.
    pub async fn get(&self, account_id: &str) -> Result<Option<AccountRow>> {
        let id = account_id.to_owned();
        self.db(move |db| Ok(ch_db::get_account(db.conn(), &id)?))
            .await
    }

    async fn row(&self, account_id: &str) -> Result<AccountRow> {
        self.get(account_id)
            .await?
            .ok_or_else(|| RuntimeError::UnknownAccount(account_id.to_owned()))
    }

    /// The account's stored settings and which secret settings are set.
    pub async fn settings(&self, account_id: &str) -> Result<AccountSettings> {
        let row = self.row(account_id).await?;
        let secrets = self
            .0
            .secrets
            .get_all(&settings_secret_namespace(&row.plugin_id), account_id)
            .await?;
        Ok(AccountSettings {
            values: row.settings,
            secrets_set: secrets.into_keys().collect(),
        })
    }

    /// What `account.start` gets (before the host adds defaults, runtime secrets and dirs):
    /// stored settings with the secret settings merged in, plus the global [`SpecDefaults`].
    pub async fn spec(&self, account_id: &str) -> Result<AccountSpec> {
        let row = self.row(account_id).await?;
        self.spec_for(&row).await
    }

    async fn spec_for(&self, row: &AccountRow) -> Result<AccountSpec> {
        let mut settings = row.settings.clone();
        let secrets = self
            .0
            .secrets
            .get_all(&settings_secret_namespace(&row.plugin_id), &row.account_id)
            .await?;
        for (k, v) in secrets {
            settings.insert(k, Value::String(v));
        }
        let d = self
            .0
            .defaults
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        Ok(AccountSpec {
            settings,
            ghost: d.ghost,
            auto_download: d.auto_download,
        })
    }

    /// Replace the global start inputs (ghost mode, auto-download). Running accounts pick
    /// them up on their next start.
    pub fn set_spec_defaults(&self, defaults: SpecDefaults) {
        *self
            .0
            .defaults
            .write()
            .unwrap_or_else(PoisonError::into_inner) = defaults;
    }

    // ---- plugins ----------------------------------------------------------------------------

    fn plugin(&self, plugin_id: &str) -> Result<(PluginSnapshot, Option<Schema>)> {
        let snap = self
            .0
            .host
            .plugin(plugin_id)
            .ok_or_else(|| RuntimeError::UnknownPlugin(plugin_id.to_owned()))?;
        let Some(m) = &snap.info.manifest else {
            return Err(RuntimeError::PluginUnavailable {
                plugin_id: plugin_id.to_owned(),
                reason: snap.state.to_string(),
            });
        };
        let schema = m.settings_schema.clone();
        Ok((snap, schema))
    }

    /// Mirror the host's discovered plugins into the `plugin` table (manifest facts and
    /// `installed`). Plugins no longer found are marked not installed; rows are never deleted
    /// (accounts reference them).
    pub async fn sync_plugins(&self) -> Result<()> {
        let snaps = self.0.host.plugins();
        self.db(move |db| {
            let tx = db.transaction()?;
            let now = now_millis();
            let mut seen = BTreeSet::new();
            for s in &snaps {
                if let Some(row) = plugin_row(s, now) {
                    seen.insert(row.plugin_id.clone());
                    ch_db::upsert_plugin(&tx, &row)?;
                }
            }
            for mut p in ch_db::list_plugins(&tx)? {
                if !seen.contains(&p.plugin_id) && p.installed {
                    p.installed = false;
                    ch_db::upsert_plugin(&tx, &p)?;
                }
            }
            tx.commit().map_err(ch_db::Error::from)?;
            Ok(())
        })
        .await
    }

    // ---- writes -----------------------------------------------------------------------------

    /// Persist a new account (wanted) and, unless [`NewAccount::no_start`], start it.
    ///
    /// Settings are validated against the plugin's `settings_schema` (defaults filled in
    /// first); `x-secret` values go to the secret store, never the database. If the start
    /// fails the error is returned but the account stays persisted and wanted: it starts on
    /// the next launch or [`Accounts::set_enabled`].
    pub async fn add(&self, new: NewAccount) -> Result<AccountRow> {
        let me = self.clone();
        tokio::spawn(async move { me.add_owned(new).await }).await?
    }
    async fn add_owned(&self, new: NewAccount) -> Result<AccountRow> {
        let membership = self.0.membership.lock().await;
        let (snap, schema) = self.plugin(&new.plugin_id)?;
        let split = split_settings(
            &new.plugin_id,
            schema.as_ref(),
            new.settings,
            &BTreeMap::new(),
        )?;
        let plugin_id = new.plugin_id.clone();
        let label = new.label.clone();
        let wanted_id = new.account_id.clone();
        let prow = plugin_row(&snap, now_millis());
        let plain = split.plain.clone();
        let inner = self.0.clone();
        let row = self
            .db(move |db| {
                let tx = db.transaction()?;
                let id = match wanted_id {
                    Some(id) => {
                        if ch_db::get_account(&tx, &id)?.is_some() {
                            return Err(RuntimeError::AccountExists(id));
                        }
                        id
                    }
                    None => generate_id(&tx, &plugin_id)?,
                };
                let root = storage::directory(inner.host.config(), &id);
                storage::require_absent(inner.host.config(), &id, &root)?;
                if let Some(p) = &prow {
                    ch_db::upsert_plugin(&tx, p)?;
                }
                let row = ch_db::create_account(&tx, &id, &plugin_id, label.as_deref(), &plain)?;
                if let Err(error) = tx.commit() {
                    inner.logical_owners.uncertain(&id);
                    return Err(ch_db::Error::from(error).into());
                }
                inner.logical_owners.created(&row);
                // An existing gate for this newly inserted ID's directory may
                // belong to a different ID with the same sanitized spelling.
                inner.storage.revoke_existing(&root);
                Ok(row)
            })
            .await?;
        let lock = self.lock_for(&row.account_id);
        let _lifecycle = lock.lock().await;
        let ns = settings_secret_namespace(&row.plugin_id);
        for (k, v) in &split.set {
            if let Err(e) = self.0.secrets.set(&ns, &row.account_id, k, v).await {
                // Do not leave a half-configured account behind.
                let root = storage::directory(self.0.host.config(), &row.account_id);
                let gate = self.0.storage.get(&root);
                gate.token.cancel();
                let _storage = gate.drain.write().await;
                self.delete_owned_row(&row, &root, &gate).await?;
                for k in split.set.keys() {
                    let _ = self.0.secrets.delete(&ns, &row.account_id, k).await;
                }
                self.0.storage.retire(&root, &gate);
                return Err(e.into());
            }
        }
        info!(account = %row.account_id, plugin = %row.plugin_id, "account added");
        self.0
            .bus
            .publish(CoreChange::Account(AccountChange::Added {
                account_id: row.account_id.clone(),
                plugin_id: row.plugin_id.clone(),
            }));
        drop(membership);
        if new.start {
            // The held lifecycle guard still pins this exact added row.
            self.start_row(&row).await?;
        }
        Ok(row)
    }

    /// Stop the account, delete it with everything it owns (DB rows cascade; secret settings,
    /// runtime secrets and `accounts/<id>/` on disk). Persons and categories survive.
    pub async fn remove(&self, account_id: &str) -> Result<()> {
        let me = self.clone();
        let id = account_id.to_owned();
        #[cfg(target_os = "linux")]
        {
            // A hookless native factory cannot see Core quarantine/pending state.
            // Linux destructive removal requires the actual Core-issued context.
            let jobs=self.0.owner_jobs.as_ref().ok_or(RuntimeError::Host(HostError::ShuttingDown))?;
            let runtime=tokio::runtime::Handle::current();
            return jobs.run_scoped(move |scope|me.remove_core_owned(&id,&scope,&runtime))
                .await.map_err(|_|RuntimeError::Host(HostError::ShuttingDown))?;
        }
        #[cfg(not(target_os = "linux"))]
        {tokio::spawn(async move { me.remove_owned(&id).await }).await?}
    }

    #[cfg(target_os="linux")]
    fn owner_db<R>(&self,scope:&crate::managed_sources::BlockingWork<'_>,
        work:impl FnOnce(&Db)->Result<R>) -> Result<R> {
        self.0.owner_jobs.as_ref().ok_or(RuntimeError::Host(HostError::ShuttingDown))?
            .check(scope).map_err(|_|RuntimeError::Host(HostError::ShuttingDown))?;
        let db=self.0.db.lock().unwrap_or_else(PoisonError::into_inner);
        work(&db)
    }

    // The whole function executes on the actual counted blocking worker. Neither
    // destructive SQLite nor std::fs deletion is a detached async sub-task.
    #[cfg(target_os="linux")]
    fn remove_core_owned(&self,account_id:&str,scope:&crate::managed_sources::BlockingWork<'_>,
        runtime:&tokio::runtime::Handle) -> Result<()> {
        let _membership=runtime.block_on(self.0.membership.lock());
        let lock=self.lock_for(account_id);let _lifecycle=runtime.block_on(lock.lock());
        let row=self.owner_db(scope,|db|ch_db::get_account(db.conn(),account_id)?
            .ok_or_else(||RuntimeError::UnknownAccount(account_id.into())))?;
        let root=storage::directory(self.0.host.config(),account_id);
        let gate=self.0.storage.get(&root);
        let anchor=crate::managed_stage::AccountRootAnchor::capture(self.0.host.config(),account_id).ok();
        let observed=self.removal_observation(&row,&root,&gate)?;
        let barriers=self.0.owner_barriers.as_ref().ok_or(RuntimeError::Host(HostError::ShuttingDown))?;
        let close=match barriers.begin_close(observed,anchor.as_ref()) {
            Ok(close)=>close,
            Err(_)=>{gate.token.cancel();return Err(RuntimeError::SourceInUse);}
        };
        gate.token.cancel();
        #[cfg(test)] self.at_owner_close("revoked");
        let _storage=runtime.block_on(gate.drain.write());
        let anchor=anchor.as_ref().ok_or(RuntimeError::SourceInUse)?;
        if !close.matches_anchor(anchor) || anchor.check().is_err() { return Err(RuntimeError::SourceInUse); }
        // First slice: every known source is retained; no settlement certificate
        // or inactive catalog snapshot is interpreted as whole-reader closure.
        let durable=self.owner_db(scope,|db|storage::managed_in_use(db.conn(),account_id))?;
        if close.in_use() || durable {
            #[cfg(test)] self.at_owner_close("before-rearm");
            let _=self.rearm_refused(scope,&row,&root,&gate,anchor,&close);
            return Err(RuntimeError::SourceInUse);
        }
        let catalog=match close.fresh_catalog() {
            Ok(check)=>check,
            Err(_)=>{
                let _=self.rearm_refused(scope,&row,&root,&gate,anchor,&close);
                return Err(RuntimeError::SourceInUse);
            }
        };
        if runtime.block_on(self.0.host.stop_for_removal(&row.plugin_id, &row.account_id))
            != crate::host::RemovalStop::Confirmed {
            let _=self.rearm_refused(scope,&row,&root,&gate,anchor,&close);
            return Err(RuntimeError::SourceInUse);
        }
        #[cfg(test)] self.at_owner_close("before-delete-tx");
        let sole=match self.owner_db(scope,|db|self.delete_row_checked(db,&row,&root,&gate,||
            catalog.verify().map_err(|_|RuntimeError::SourceInUse)))? {
            Ok(sole)=>sole,
            Err(error)=>{
                let _=self.rearm_refused(scope,&row,&root,&gate,anchor,&close);
                return Err(error);
            }
        };
        // This clears only the controlled close reservation after a known delete,
        // never source/unknown records and never from an absent-row inference.
        close.deleted().map_err(|_|RuntimeError::Task("account removal bookkeeping incomplete".into()))?;
        runtime.block_on(self.delete_account_secrets(&row));
        let settled=if sole {
            // Observe-only invalidation; a closed/poisoned catalog already denies
            // completeness. It does not supply source-lifetime/deletion authority.
            let _mutation=self.0.stage_mutations.as_ref().and_then(|hook|hook.begin().ok());
            #[cfg(test)] self.at_owner_close("before-root-delete");
            if anchor.check().is_err() {
                warn!("account directory identity changed before cleanup");false
            } else {
                match std::fs::remove_dir_all(&root) {
                    Ok(())=>true,
                    Err(error) if error.kind()==std::io::ErrorKind::NotFound=>true,
                    Err(_)=>{warn!("account directory cleanup incomplete");false}
                }
            }
        } else {true};
        if settled {self.0.storage.retire(&root,&gate);}
        self.0.bus.publish(CoreChange::Account(AccountChange::Removed {
            account_id:account_id.into(),plugin_id:row.plugin_id,
        }));
        Ok(())
    }

    async fn delete_account_secrets(&self,row:&AccountRow) {
        for ns in [row.plugin_id.clone(),settings_secret_namespace(&row.plugin_id)] {
            match self.0.secrets.get_all(&ns,&row.account_id).await {
                Ok(all)=>for key in all.keys() {
                    if self.0.secrets.delete(&ns,&row.account_id,key).await.is_err() {warn!("deleting account secret failed");}
                },
                Err(_)=>warn!("listing account secrets failed"),
            }
        }
    }

    #[cfg(all(test,target_os="linux"))]
    pub(crate) fn owner_close_checkpoint(&self,hook:Arc<OwnerCloseCheckpoint>) {
        *self.0.owner_checkpoint.lock().unwrap()=Some(hook);
    }
    #[cfg(all(test,target_os="linux"))]
    fn at_owner_close(&self,phase:&str) {
        let hook=self.0.owner_checkpoint.lock().unwrap().clone();
        if let Some(hook)=hook {hook(phase);}
    }

    #[cfg(not(target_os = "linux"))]
    async fn remove_owned(&self, account_id: &str) -> Result<()> {
        let _membership = self.0.membership.lock().await;
        let lock = self.lock_for(account_id);
        let _g = lock.lock().await;
        let row = self.row(account_id).await?;
        let root = storage::directory(self.0.host.config(), account_id);
        let gate = self.0.storage.get(&root);
        gate.token.cancel();
        let _storage = gate.drain.write().await;
        let check_id=account_id.to_owned();
        if self.db(move|db|storage::managed_in_use(db.conn(),&check_id)).await? {
            // Standalone native Accounts has no Core close/root proof to rearm.
            return Err(RuntimeError::SourceInUse);
        }
        self.stop_on_host(&row).await;
        let sole = self.delete_owned_row(&row, &root, &gate).await?;
        self.delete_account_secrets(&row).await;
        let settled = if sole {
            match tokio::fs::remove_dir_all(&root).await {
                Ok(()) => true,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
                Err(_) => {
                    warn!(account = account_id, "account directory cleanup incomplete");
                    false
                }
            }
        } else {
            // A shared or noncanonical root must survive this owner's removal.
            true
        };
        if settled {
            self.0.storage.retire(&root, &gate);
        }
        info!(account = account_id, plugin = %row.plugin_id, "account removed");
        self.0
            .bus
            .publish(CoreChange::Account(AccountChange::Removed {
                account_id: account_id.to_owned(),
                plugin_id: row.plugin_id,
            }));
        Ok(())
    }

    /// Switch "wanted": persist it, then start or stop the account.
    pub async fn set_enabled(&self, account_id: &str, enabled: bool) -> Result<()> {
        let me = self.clone();
        let id = account_id.to_owned();
        tokio::spawn(async move { me.set_enabled_owned(&id, enabled).await }).await?
    }

    async fn set_enabled_owned(&self, account_id: &str, enabled: bool) -> Result<()> {
        let lock = self.lock_for(account_id);
        let _guard = lock.lock().await;
        let row = self.row(account_id).await?;
        if row.enabled != enabled {
            let owner = row.clone();
            let host = self.0.host.clone();
            self.db(move |db| {
                let tx = db.transaction()?;
                storage::check_owner(&tx, &owner)?;
                ch_db::update_account_meta(
                    &tx,
                    &owner.account_id,
                    owner.label.as_deref(),
                    owner.position,
                    enabled,
                )?;
                tx.commit().map_err(ch_db::Error::from)?;
                if !enabled {
                    host.revoke_account_run(&owner.plugin_id, &owner.account_id);
                }
                Ok(())
            })
            .await?;
            self.0
                .bus
                .publish(CoreChange::Account(AccountChange::Enabled {
                    account_id: account_id.to_owned(),
                    enabled,
                }));
        }
        if enabled {
            self.start_row(&row).await
        } else {
            // Also revoke a directly started host account on repeat disable.
            self.0.host.revoke_account_run(&row.plugin_id, account_id);
            self.stop_on_host(&row).await;
            Ok(())
        }
    }

    /// Save the settings form. `values` replaces the stored non-secret settings. For `x-secret`
    /// keys: a string stores the value, `null` deletes it, absent keeps what is stored (the form
    /// never receives secret values). The merged result, with schema defaults, must satisfy
    /// `settings_schema`. A started account is restarted with the new values (§6.2).
    pub async fn set_settings(&self, account_id: &str, values: Map<String, Value>) -> Result<()> {
        let lock = self.lock_for(account_id);
        let _g = lock.lock().await;
        let row = self.row(account_id).await?;
        let (_, schema) = self.plugin(&row.plugin_id)?;
        let ns = settings_secret_namespace(&row.plugin_id);
        let existing = self.0.secrets.get_all(&ns, account_id).await?;
        let split = split_settings(&row.plugin_id, schema.as_ref(), values, &existing)?;
        let id = account_id.to_owned();
        let plain = split.plain;
        self.db(move |db| Ok(ch_db::set_account_settings(db.conn(), &id, &plain)?))
            .await?;
        for (k, v) in &split.set {
            self.0.secrets.set(&ns, account_id, k, v).await?;
        }
        for k in &split.delete {
            self.0.secrets.delete(&ns, account_id, k).await?;
        }
        self.0
            .bus
            .publish(CoreChange::Account(AccountChange::SettingsChanged {
                account_id: account_id.to_owned(),
            }));
        let started = self
            .0
            .host
            .account(&row.plugin_id, account_id)
            .is_some_and(|a| a.phase != AccountPhase::Stopped);
        if started {
            debug!(account = account_id, "settings changed; restarting");
            self.stop_on_host(&row).await;
            let row = self.row(account_id).await?;
            self.start_row(&row).await?;
        }
        Ok(())
    }

    /// Start the account on the host with its persisted spec. Does not change "wanted". An
    /// account that is already started is left alone.
    pub async fn start(&self, account_id: &str) -> Result<()> {
        let lock = self.lock_for(account_id);
        let _g = lock.lock().await;
        let row = self.row(account_id).await?;
        self.start_row(&row).await
    }

    async fn start_row(&self, row: &AccountRow) -> Result<()> {
        let spec = self.spec_for(row).await?;
        match self
            .0
            .host
            .start_account(&row.plugin_id, &row.account_id, spec)
            .await
        {
            Ok(()) | Err(HostError::AccountAlreadyStarted(_)) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn stop_on_host(&self, row: &AccountRow) {
        match self
            .0
            .host
            .stop_account(&row.plugin_id, &row.account_id)
            .await
        {
            Ok(()) | Err(HostError::UnknownAccount(_) | HostError::UnknownPlugin(_)) => {}
            Err(e) => warn!(account = %row.account_id, "stopping: {e}"),
        }
    }

    /// Start every wanted account, concurrently. Returns the ones that failed (also logged);
    /// accounts whose plugin is not discovered are skipped with a warning.
    pub async fn start_wanted(&self) -> Result<Vec<(String, RuntimeError)>> {
        let mut set = tokio::task::JoinSet::new();
        for row in self.list().await? {
            if !row.enabled {
                continue;
            }
            if self.0.host.plugin(&row.plugin_id).is_none() {
                warn!(account = %row.account_id, plugin = %row.plugin_id, "plugin not found; account not started");
                continue;
            }
            let me = self.clone();
            set.spawn(async move {
                let r = me.start(&row.account_id).await;
                (row.account_id, r)
            });
        }
        let mut failed = Vec::new();
        while let Some(r) = set.join_next().await {
            let (id, r) = r?;
            if let Err(e) = r {
                warn!(account = %id, "start failed: {e}");
                failed.push((id, e));
            }
        }
        Ok(failed)
    }

    // ---- host state -------------------------------------------------------------------------

    /// Legacy unbound native notification hook; rescans refresh the plugin table.
    /// Linux Core account metadata instead requires its private source-bound
    /// snapshot stream. A raw `AccountInfo` cannot authorize those writes.
    pub async fn on_host_change(&self, change: &StateChange) -> Result<()> {
        match change {
            StateChange::Account(info) => {
                #[cfg(target_os = "linux")]
                if self.0.stage_mutations.is_some() {
                    return Err(RuntimeError::Task("native account metadata provenance required".into()));
                }
                self.persist_account(info.clone()).await
            },
            StateChange::PluginsRescanned => self.sync_plugins().await,
            StateChange::Plugin { .. } => Ok(()),
        }
    }

    /// Re-persist every account from the host's snapshot (after a lagged subscription).
    pub async fn resync_from_host(&self) -> Result<()> {
        #[cfg(target_os = "linux")]
        if self.0.stage_mutations.is_some() {
            for snapshot in self.0.host.accounts_owned() { self.persist_owned(snapshot).await?; }
            return Ok(());
        }
        for info in self.0.host.accounts() { self.persist_account(info).await?; }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn on_owned_host_change(&self, change: &crate::host::OwnedStateChange) -> Result<Option<StateChange>> {
        match change {
            crate::host::OwnedStateChange::Account(snapshot) => {
                let origin = snapshot.origin().clone();
                if self.persist_owned(snapshot.clone()).await? && !origin.is_revoked() {
                    Ok(Some(StateChange::Account(snapshot.info().clone())))
                } else { Ok(None) }
            }
            crate::host::OwnedStateChange::Other(change) => {
                self.on_host_change(change).await?;
                Ok(Some(change.as_ref().clone()))
            }
        }
    }

    #[cfg(target_os = "linux")]
    async fn persist_owned(&self, snapshot: crate::host::OwnedAccountSnapshot) -> Result<bool> {
        let owners = self.0.logical_owners.clone();
        let db = self.0.db.clone();
        let domain = owners.clone();
        let origin = snapshot.origin().clone();
        #[cfg(test)]
        let checkpoint = owners.take_checkpoint("snapshot");
        match owners.run_metadata(&origin, move || {
            #[cfg(test)]
            let _completion = checkpoint.map(|checkpoint| checkpoint.enter());
            let db = db.lock().unwrap_or_else(PoisonError::into_inner);
            let (info, origin) = snapshot.into_parts();
            let tx = db.transaction()?;
            if origin.account_id() != info.account_id || origin.plugin_id() != info.plugin_id
                || !domain.validate(&tx, &origin)? { return Ok(false); }
            let row = ch_db::get_account(&tx, &info.account_id)?
                .ok_or_else(|| RuntimeError::UnknownAccount(info.account_id.clone()))?;
            write_account_info(&tx, &row, &info)?;
            if !domain.validate(&tx, &origin)? { tx.rollback().map_err(ch_db::Error::from)?; return Ok(false); }
            tx.commit().map_err(ch_db::Error::from)?;
            Ok(!origin.is_revoked())
        }).await {
            Ok(result) => result,
            Err(crate::managed_sources::Error::Closed) => Ok(false),
            Err(_) => Err(RuntimeError::Task("native metadata worker failed".into())),
        }
    }

    async fn persist_account(&self, info: AccountInfo) -> Result<()> {
        self.db(move |db| {
            let conn = db.conn();
            let Some(row) = ch_db::get_account(conn, &info.account_id)? else {
                return Ok(());
            };
            if row.plugin_id != info.plugin_id {
                return Ok(());
            }
            write_account_info(conn, &row, &info)?;
            Ok(())
        })
        .await
    }
}

// Both native guarded writes use their explicit transaction connection. The
// legacy unbound native service retains its existing descriptive helper.
fn write_account_info(conn: &rusqlite::Connection, row: &AccountRow, info: &AccountInfo) -> Result<()> {
            let state = info.state.as_str();
            if row.state != state
                || row.state_detail != info.detail
                || row.reconnecting != info.reconnecting
            {
                ch_db::set_account_state(
                    conn,
                    &info.account_id,
                    state,
                    info.detail.as_deref(),
                    info.reconnecting,
                    now_millis(),
                )?;
            }
            if let Some(me) = &info.me
                && row.me_user_id.as_deref() != Some(me.id.as_str())
            {
                ch_db::set_account_me(conn, &info.account_id, &me.id)?;
            }
    Ok(())
}

/// Settings split for storage.
struct Split {
    /// Non-secret values (stored in `account.settings_json`).
    plain: Map<String, Value>,
    /// Secret values to store.
    set: BTreeMap<String, String>,
    /// Secret keys to delete.
    delete: Vec<String>,
}

/// Separate `x-secret` values from the rest and validate the merged result.
fn split_settings(
    plugin_id: &str,
    schema: Option<&Schema>,
    values: Map<String, Value>,
    existing_secrets: &BTreeMap<String, String>,
) -> Result<Split> {
    let secret = secret_keys(schema);
    let mut plain = Map::new();
    let mut set = BTreeMap::new();
    let mut delete = Vec::new();
    let mut problems = Vec::new();
    for (k, v) in values {
        if secret.contains(&k) {
            match v {
                Value::Null => delete.push(k),
                Value::String(s) => {
                    set.insert(k, s);
                }
                _ => problems.push(SettingsProblem {
                    path: format!("/{}", pointer_escape(&k)),
                    message: "a secret setting must be a string".into(),
                }),
            }
        } else {
            plain.insert(k, v);
        }
    }
    if !problems.is_empty() {
        return Err(RuntimeError::InvalidSettings(problems));
    }
    let mut effective = plain.clone();
    for (k, v) in existing_secrets {
        if secret.contains(k) && !delete.contains(k) {
            effective.insert(k.clone(), Value::String(v.clone()));
        }
    }
    for (k, v) in &set {
        effective.insert(k.clone(), Value::String(v.clone()));
    }
    validate_settings(plugin_id, schema, &effective)?;
    Ok(Split { plain, set, delete })
}

/// Validate `settings` (secrets merged in) against a plugin's `settings_schema` (draft 2020-12,
/// formats checked), after filling the schema's top-level defaults. A plugin without a schema
/// takes no settings. Messages never include values.
pub fn validate_settings(
    plugin_id: &str,
    schema: Option<&Schema>,
    settings: &Map<String, Value>,
) -> Result<()> {
    let Some(schema) = schema else {
        if settings.is_empty() {
            return Ok(());
        }
        return Err(RuntimeError::InvalidSettings(vec![SettingsProblem {
            path: String::new(),
            message: "this plugin takes no settings".into(),
        }]));
    };
    let validator = jsonschema::draft202012::options()
        .should_validate_formats(true)
        .build(&Value::Object(schema.clone()))
        .map_err(|e| RuntimeError::InvalidSchema {
            plugin_id: plugin_id.to_owned(),
            message: e.to_string(),
        })?;
    let mut filled = settings.clone();
    fill_defaults(Some(schema), &mut filled);
    let instance = Value::Object(filled);
    let problems: Vec<SettingsProblem> = validator
        .iter_errors(&instance)
        .map(|e| SettingsProblem {
            path: e.instance_path().to_string(),
            message: e.masked().to_string(),
        })
        .collect();
    if problems.is_empty() {
        Ok(())
    } else {
        Err(RuntimeError::InvalidSettings(problems))
    }
}

fn pointer_escape(k: &str) -> String {
    k.replace('~', "~0").replace('/', "~1")
}

/// `<plugin_id>-<n>` with the smallest `n ≥ 1` never used (ids are never reused: the
/// plugin's `state_dir` and secrets of a removed account must not resurface).
fn generate_id(conn: &rusqlite::Connection, plugin_id: &str) -> Result<String> {
    const KEY: &str = "runtime.account_seq";
    let mut seq: BTreeMap<String, u64> = ch_db::get_setting(conn, KEY)?.unwrap_or_default();
    let mut n = seq.get(plugin_id).copied().unwrap_or(0);
    loop {
        n += 1;
        let id = format!("{plugin_id}-{n}");
        if ch_db::get_account(conn, &id)?.is_none() {
            seq.insert(plugin_id.to_owned(), n);
            ch_db::set_setting(conn, KEY, &seq)?;
            return Ok(id);
        }
    }
}

fn plugin_row(s: &PluginSnapshot, now: i64) -> Option<PluginRow> {
    let m = s.info.manifest.as_ref()?;
    Some(PluginRow {
        plugin_id: m.id.clone(),
        name: m.name.clone(),
        version: m.version.clone(),
        api_version: m.api_version.clone(),
        install_dir: Some(s.info.dir.to_string_lossy().into_owned()),
        brand_color: Some(m.brand_color.clone()),
        icon_path: m
            .icon
            .as_ref()
            .map(|i| s.info.dir.join(i).to_string_lossy().into_owned()),
        manifest_json: serde_json::to_string(m).ok(),
        enabled: true,
        installed: s.info.executable.is_some()
            && !matches!(s.state, crate::host::PluginState::Unavailable(_)),
        first_seen_at: now,
        last_seen_at: now,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema() -> Schema {
        json!({"type": "object", "properties": {
            "seed": {"type": "integer", "minimum": 0, "default": 1},
            "proxy": {"type": "string", "format": "hostname"},
            "token": {"type": "string", "x-secret": true, "minLength": 4}
        }, "required": ["token"]})
        .as_object()
        .unwrap()
        .clone()
    }

    fn obj(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn split_and_validate() {
        let s = schema();
        let sp = split_settings(
            "p",
            Some(&s),
            obj(json!({"seed": 2, "token": "abcd"})),
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(Value::Object(sp.plain), json!({"seed": 2}));
        assert_eq!(sp.set.get("token").map(String::as_str), Some("abcd"));

        // Missing required secret.
        let e = split_settings("p", Some(&s), obj(json!({"seed": 2})), &BTreeMap::new())
            .err()
            .unwrap();
        assert!(matches!(e, RuntimeError::InvalidSettings(_)), "{e}");
        // ... but an already stored secret satisfies it.
        let stored = BTreeMap::from([("token".to_owned(), "wxyz".to_owned())]);
        split_settings("p", Some(&s), obj(json!({"seed": 2})), &stored).unwrap();
        // Deleting it does not.
        assert!(split_settings("p", Some(&s), obj(json!({"token": null})), &stored).is_err());

        // A bad secret value is reported without its value.
        let e = split_settings(
            "p",
            Some(&s),
            obj(json!({"token": "hun"})),
            &BTreeMap::new(),
        )
        .err()
        .unwrap();
        let msg = e.to_string();
        assert!(msg.contains("/token"), "{msg}");
        assert!(!msg.contains("hun"), "secret leaked: {msg}");

        // Types, ranges and formats.
        for bad in [
            json!({"seed": -1, "token": "abcd"}),
            json!({"seed": "x", "token": "abcd"}),
            json!({"proxy": "not a host!", "token": "abcd"}),
            json!({"token": 5}),
        ] {
            assert!(
                split_settings("p", Some(&s), obj(bad.clone()), &BTreeMap::new()).is_err(),
                "{bad}"
            );
        }
        // No schema, no settings.
        assert!(validate_settings("p", None, &Map::new()).is_ok());
        assert!(validate_settings("p", None, &obj(json!({"a": 1}))).is_err());
    }
}
