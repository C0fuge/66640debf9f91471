//! Core-bound account directory preparation only. No owner, reader or network grant.
use super::{HostConfig, HostError};
use crate::{managed_sources::OwnedJobs, managed_stage::StageMutations};
use std::{io, path::PathBuf, sync::Arc};

#[cfg(test)]
type DirectoryCheckpoint = Arc<dyn Fn(&str) + Send + Sync>;

pub(super) struct AccountDirectories {
    mutations: StageMutations,
    jobs: Arc<OwnedJobs>,
    #[cfg(test)]
    checkpoint: std::sync::Mutex<Option<DirectoryCheckpoint>>,
}
impl AccountDirectories {
    pub(super) fn new(mutations: StageMutations, jobs: Arc<OwnedJobs>) -> Arc<Self> {
        Arc::new(Self { mutations, jobs, #[cfg(test)] checkpoint: Default::default() })
    }

    pub(super) async fn prepare(&self, config: &HostConfig, account: &str)
        -> Result<Vec<(PathBuf, io::Error)>, HostError>
    {
        self.mutations.check_accounts_root(&config.data_dir.join("accounts"))
            .map_err(|_| HostError::ShuttingDown)?;
        let paths = [config.state_dir(account), config.cache_dir(account)];
        let mutations = self.mutations.clone();
        #[cfg(test)]
        let checkpoint = self.checkpoint.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        #[cfg(test)]
        if let Some(checkpoint) = &checkpoint { checkpoint("waiting"); }
        let result = self.jobs.run_wait(move || {
            // Both the slot and token belong to this actual blocking closure.
            // No waiter cancellation can settle its mkdirs or permit a new scan.
            let _mutation = mutations.begin().map_err(|_| HostError::ShuttingDown)?;
            #[cfg(test)]
            if let Some(checkpoint) = checkpoint { checkpoint("writing"); }
            let mut errors = Vec::new();
            for path in paths {
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
}
