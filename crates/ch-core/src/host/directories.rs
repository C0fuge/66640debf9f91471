//! Core-bound account directory preparation only. No owner, reader or network grant.
use super::{HostConfig, HostError};
use crate::{managed_sources::OwnedJobs, managed_stage::StageMutations};
use std::{io, path::PathBuf, sync::{Arc, OnceLock}};
use super::start_scope::{StartOwner, StartScope};

#[cfg(test)]
type DirectoryCheckpoint = Arc<dyn Fn(&str) + Send + Sync>;

pub(super) struct AccountDirectories {
    mutations: StageMutations,
    jobs: Arc<OwnedJobs>,
    captures: Arc<OwnedJobs>,
    issuer: OnceLock<crate::runtime::HostStorageView>,
    #[cfg(test)]
    checkpoint: std::sync::Mutex<Option<DirectoryCheckpoint>>,
}
impl AccountDirectories {
    pub(super) fn new(mutations: StageMutations, jobs: Arc<OwnedJobs>, captures: Arc<OwnedJobs>) -> Arc<Self> {
        Arc::new(Self { mutations, jobs, captures, issuer: OnceLock::new(), #[cfg(test)] checkpoint: Default::default() })
    }

    pub(super) fn bind(&self, host: &super::HostHandle, issuer: crate::runtime::HostStorageView) -> Result<(), HostError> {
        if !issuer.belongs_to(host) { return Err(HostError::ShuttingDown); }
        self.issuer.set(issuer).map_err(|_| HostError::ShuttingDown)
    }

    pub(super) async fn capture(&self, plugin: &str, account: &str) -> Result<StartScope, HostError> {
        let issuer = self.issuer.get().ok_or(HostError::ShuttingDown)?.clone();
        let plugin = plugin.to_owned(); let account = account.to_owned();
        let capture = issuer.clone();
        let stopped = self.jobs.stopped();
        let (observed, logical_origin) = self.captures.run_wait_until(&stopped, move || capture.capture(&plugin, &account))
            .await.map_err(|_| HostError::ShuttingDown)?.map_err(|_| HostError::ShuttingDown)?;
        let scope = StartScope::bound(StartOwner { issuer, observed, logical_origin, stopped: self.jobs.stopped() });
        if !scope.live() { return Err(HostError::ShuttingDown); }
        Ok(scope)
    }

    pub(super) async fn prepare(&self, config: &HostConfig, account: &str, scope: &StartScope)
        -> Result<Vec<(PathBuf, io::Error)>, HostError>
    {
        self.mutations.check_accounts_root(&config.data_dir.join("accounts"))
            .map_err(|_| HostError::ShuttingDown)?;
        let paths = [config.state_dir(account), config.cache_dir(account)];
        let owner = scope.owner.as_ref().ok_or(HostError::ShuttingDown)?.clone();
        if !self.issuer.get().is_some_and(|issuer| issuer.same_issuer(&owner.issuer))
            || owner.observed.owner().account_id != account || !scope.live()
        { return Err(HostError::ShuttingDown); }
        let revoked = owner.observed.revocation();
        let scope = scope.clone();
        let runtime = tokio::runtime::Handle::current();
        #[cfg(test)]
        let checkpoint = self.checkpoint.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        #[cfg(test)]
        if let Some(checkpoint) = &checkpoint { checkpoint("waiting"); }
        let result = self.jobs.run_wait_until(&revoked, move || {
            // Both the slot and token belong to this actual blocking closure.
            // No waiter cancellation can settle its mkdirs or permit a new scan.
            let binding = runtime.block_on(owner.issuer.reacquire(&owner.observed, &owner.stopped))
                .map_err(|_| HostError::ShuttingDown)?;
            let uploads = binding.uploads();
            let root = uploads.parent().ok_or(HostError::ShuttingDown)?;
            if paths.iter().any(|path| path.parent() != Some(root)) { return Err(HostError::ShuttingDown); }
            let _mutation = binding.stage_mutations().map_err(|_| HostError::ShuttingDown)?
                .begin().map_err(|_| HostError::ShuttingDown)?;
            #[cfg(test)]
            if let Some(checkpoint) = checkpoint { checkpoint("writing"); }
            let mut errors = Vec::new();
            for path in paths {
                if !scope.live() { return Err(HostError::ShuttingDown); }
                if let Err(error) = std::fs::create_dir_all(&path) { errors.push((path, error)); }
            }
            Ok(errors)
        }).await;
        match result {
            Ok(result) => result,
            Err(_) => Err(HostError::ShuttingDown),
        }
    }

    #[cfg(test)]
    pub(super) fn checkpoint(&self, hook: Option<DirectoryCheckpoint>) {
        *self.checkpoint.lock().unwrap() = hook;
    }
    #[cfg(test)]
    pub(super) fn checkpoint_at(&self, phase: &str) {
        let hook = self.checkpoint.lock().unwrap().clone();
        if let Some(hook) = hook { hook(phase); }
    }
}
