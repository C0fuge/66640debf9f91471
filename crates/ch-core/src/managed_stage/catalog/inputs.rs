//! Bounded metadata-only outside-input observation. Never a managed-source grant.
use super::{Inode, PrivateByteCatalog};
use crate::managed_stage::{Error, Result, fd};
use std::{ffi::CString, fs::File, os::{fd::{AsRawFd, FromRawFd}, unix::fs::MetadataExt}, path::Path, sync::Arc};

pub(crate) struct ExternalInputs {
    issuer: Arc<()>,
    epoch: u64,
    paths: Vec<String>,
}

fn target(path: &str) -> Result<(File, std::path::PathBuf, Inode)> {
    fd::parts(Path::new(path))?; // Absolute, bounded, no ambiguous components.
    let name = CString::new(path).map_err(|_| Error::Integrity)?;
    // SAFETY: name is terminated; successful open transfers one O_PATH handle.
    // Follow external symlinks, but never read bytes or open a device/FIFO reader.
    let raw = unsafe { libc::open(name.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
    if raw < 0 { return Err(Error::Storage); }
    let file = unsafe { File::from_raw_fd(raw) };
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.ino() == 0 { return Err(Error::UnsafeSource); }
    let resolved = std::fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd()))?;
    fd::parts(&resolved)?;
    Ok((file, resolved, Inode { dev: metadata.dev(), ino: metadata.ino() }))
}

impl PrivateByteCatalog {
    // Called ONLY by the counted blocking input worker; every O_PATH/directory
    // handle drops before returning. The immutable result contains no FD/Gate.
    pub(crate) fn external_inputs(&self, paths: Vec<String>) -> Result<ExternalInputs> {
        let _readers=self.read_scope()?;
        if paths.is_empty() || paths.len() > 10 { return Err(Error::Integrity); }
        let data = fd::data_dir(&self.data_dir)?;
        let epoch = {
            let state=self.lock();
            (state.complete && !state.closed && !state.poisoned && state.active==0).then_some(state.epoch)
        };
        let epoch=match epoch { Some(epoch)=>epoch,None=>self.refresh()?.epoch() };
        let private = self.data_dir.join("accounts");
        for path in &paths {
            let (file, resolved, inode) = target(path)?;
            if Path::new(path).starts_with(&private) || resolved.starts_with(&private) {
                return Err(Error::UnsafeSource);
            }
            {
                let state = self.lock();
                if state.inodes.contains(&inode) { return Err(Error::UnsafeSource); }
            }
            // Re-open the supplied name while the original metadata handle is
            // held; reject a disappeared/rebound name, including deleted aliases.
            let (_current, current_path, current_inode) = target(path)?;
            if inode != current_inode || resolved != current_path { return Err(Error::SourceChanged); }
            drop(file);
        }
        data.check()?;
        let state = self.lock();
        if state.closed || state.poisoned || !state.complete || state.active != 0
            || state.epoch != epoch
            || state.root != Some(Inode { dev: data.identity.dev, ino: data.identity.ino })
        { return Err(Error::Busy); }
        Ok(ExternalInputs { issuer: self.issuer.clone(), epoch: state.epoch, paths })
    }

    // The caller already owns the source-reservation lock. Only the actual
    // nonblocking frame publication may run while this catalog lock is held.
    pub(crate) fn external_frame<R>(&self, observed: &ExternalInputs, paths: &[String],
        publish: impl FnOnce() -> R) -> Result<R> {
        let state = self.lock();
        if !Arc::ptr_eq(&self.issuer, &observed.issuer) || observed.paths != paths
            || state.closed || state.poisoned || !state.complete || state.active != 0
            || state.epoch != observed.epoch
        { return Err(Error::Busy); }
        let result = publish();
        drop(state);
        Ok(result)
    }
}
