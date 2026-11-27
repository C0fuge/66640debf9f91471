//! Actual application directory lock. Namespace/reader/cleanup proofs are separate.
use std::{fs::{File,OpenOptions},io,path::{Path,PathBuf}};

/// Holds the real kernel lock used by the application. Fields are private; a
/// caller cannot substitute a path, boolean or deserialized marker for the lock.
/// Sharing an `Arc` extends the actual descriptor's lifetime through owned work.
/// This lock alone does not certify current namespace identity or reader closure.
///
/// ```compile_fail
/// use ch_core::runtime::DataDirectoryLock;
/// fn duplicate(lock:&DataDirectoryLock)->DataDirectoryLock {lock.clone()}
/// ```
pub struct DataDirectoryLock {
    _file:File,
    directory:PathBuf,
}
impl DataDirectoryLock {
    /// Acquire the same nontruncating `app.lock` kernel lock as desktop startup.
    /// This is real IO, not a constructor accepting a caller-vouched locked flag.
    pub fn acquire(directory:&Path)->io::Result<Self> {
        std::fs::create_dir_all(directory)?;
        let file=OpenOptions::new().read(true).write(true).create(true).truncate(false)
            .open(directory.join("app.lock"))?;
        file.try_lock().map_err(|_|io::Error::other("cannot lock the application data directory"))?;
        Ok(Self {_file:file,directory:directory.to_owned()})
    }

    /// Descriptive configured spelling only. Independent current namespace
    /// verification is mandatory before this can accompany any filesystem proof.
    pub fn directory(&self)->&Path {&self.directory}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn real_kernel_lock_survives_shared_owner_loss_and_never_truncates_existing_content() {
        let temp=tempfile::tempdir().unwrap();
        let path=temp.path().join("app.lock");
        std::fs::write(&path,b"synthetic retained lock marker").unwrap();
        let original=Arc::new(DataDirectoryLock::acquire(temp.path()).unwrap());
        assert_eq!(original.directory(),temp.path());
        assert_eq!(std::fs::read(&path).unwrap(),b"synthetic retained lock marker");
        let held=original.clone();drop(original);
        // Distinct configured spelling still names the same actual lock inode.
        assert!(DataDirectoryLock::acquire(&temp.path().join(".")).is_err());
        drop(held);
        let next=DataDirectoryLock::acquire(temp.path()).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(),b"synthetic retained lock marker");
        drop(next);
    }

    #[test]
    fn independent_directories_keep_independent_real_lock_ownership() {
        let first=tempfile::tempdir().unwrap();let second=tempfile::tempdir().unwrap();
        let one=DataDirectoryLock::acquire(first.path()).unwrap();
        let two=DataDirectoryLock::acquire(second.path()).unwrap();
        assert!(DataDirectoryLock::acquire(first.path()).is_err());
        assert!(DataDirectoryLock::acquire(second.path()).is_err());
        drop(one);assert!(DataDirectoryLock::acquire(first.path()).is_ok());
        drop(two);assert!(DataDirectoryLock::acquire(second.path()).is_ok());
    }
}
