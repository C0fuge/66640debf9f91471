//! Actual application directory lock. Namespace/reader/cleanup proofs are separate.
use std::{io,path::{Path,PathBuf}};
#[cfg(not(target_os="linux"))]
use std::fs::{File,OpenOptions};
#[cfg(target_os="linux")]
mod native;

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
    #[cfg(target_os="linux")]
    native:native::Held,
    #[cfg(not(target_os="linux"))]
    _file:File,
    directory:PathBuf,
}
impl DataDirectoryLock {
    /// Acquire the same nontruncating `app.lock` kernel lock as desktop startup.
    /// This is real IO, not a constructor accepting a caller-vouched locked flag.
    pub fn acquire(directory:&Path)->io::Result<Self> {
        std::fs::create_dir_all(directory)?;
        #[cfg(target_os="linux")]
        let native=native::Held::acquire(directory)?;
        #[cfg(not(target_os="linux"))]
        let file=OpenOptions::new().read(true).write(true).create(true).truncate(false)
            .open(directory.join("app.lock"))?;
        #[cfg(not(target_os="linux"))]
        file.try_lock().map_err(|_|io::Error::other("cannot lock the application data directory"))?;
        Ok(Self {#[cfg(target_os="linux")] native,
            #[cfg(not(target_os="linux"))] _file:file,directory:directory.to_owned()})
    }

    /// Descriptive configured spelling only. Independent current namespace
    /// verification is mandatory before this can accompany any filesystem proof.
    pub fn directory(&self)->&Path {&self.directory}

    /// Recheck this actual lock and its original parent against their current
    /// names. Failure is a negative veto. Success is an immediate observation,
    /// not a reusable namespace, account, reader-closure or cleanup permission.
    #[cfg(target_os="linux")]
    pub fn check_current(&self)->io::Result<()> {self.native.check_current()}
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

    #[cfg(target_os="linux")]
    #[test]
    fn original_parent_replacement_is_refused_even_when_original_lock_inode_is_moved_back() {
        use std::os::unix::fs::MetadataExt;
        let temp=tempfile::tempdir().unwrap();let root=temp.path().join("data");
        let held=DataDirectoryLock::acquire(&root).unwrap();held.check_current().unwrap();
        let inode=std::fs::metadata(root.join("app.lock")).unwrap().ino();
        let old=temp.path().join("previous");std::fs::rename(&root,&old).unwrap();
        std::fs::create_dir(&root).unwrap();
        std::fs::rename(old.join("app.lock"),root.join("app.lock")).unwrap();
        assert_eq!(std::fs::metadata(root.join("app.lock")).unwrap().ino(),inode);
        assert!(held.check_current().is_err());
        assert!(DataDirectoryLock::acquire(&root).is_err(),"original kernel lock is still held");
        drop(held);
        DataDirectoryLock::acquire(&root).unwrap().check_current().unwrap();
    }

    #[cfg(target_os="linux")]
    #[test]
    fn replacement_lock_name_cannot_renew_the_old_descriptor() {
        let temp=tempfile::tempdir().unwrap();
        let old=DataDirectoryLock::acquire(temp.path()).unwrap();
        std::fs::rename(temp.path().join("app.lock"),temp.path().join("previous.lock")).unwrap();
        let current=DataDirectoryLock::acquire(temp.path()).unwrap();
        assert!(old.check_current().is_err());current.check_current().unwrap();
        drop(old);
        assert!(DataDirectoryLock::acquire(temp.path()).is_err());
        drop(current);assert!(DataDirectoryLock::acquire(temp.path()).is_ok());
    }

    #[cfg(target_os="linux")]
    #[test]
    fn unsafe_lock_entries_are_rejected_and_directory_aliases_keep_original_target_identity() {
        use std::os::unix::fs::symlink;
        let temp=tempfile::tempdir().unwrap();let root=temp.path().join("data");
        std::fs::create_dir(&root).unwrap();
        let target=temp.path().join("original");std::fs::write(&target,b"synthetic original bytes").unwrap();
        let lock=root.join("app.lock");symlink(&target,&lock).unwrap();
        assert!(DataDirectoryLock::acquire(&root).is_err());
        std::fs::remove_file(&lock).unwrap();std::fs::hard_link(&target,&lock).unwrap();
        assert!(DataDirectoryLock::acquire(&root).is_err());
        std::fs::remove_file(&lock).unwrap();std::fs::create_dir(&lock).unwrap();
        assert!(DataDirectoryLock::acquire(&root).is_err());
        assert_eq!(std::fs::read(&target).unwrap(),b"synthetic original bytes");
        std::fs::remove_dir(&lock).unwrap();
        let alias=temp.path().join("data-alias");symlink(&root,&alias).unwrap();
        let held=DataDirectoryLock::acquire(&alias).unwrap();held.check_current().unwrap();
        assert!(DataDirectoryLock::acquire(&root).is_err());
        assert!(DataDirectoryLock::acquire(&alias.join("")).is_err());
        let other=temp.path().join("other");std::fs::create_dir(&other).unwrap();
        std::fs::remove_file(&alias).unwrap();symlink(&other,&alias).unwrap();
        assert!(held.check_current().is_err());
        DataDirectoryLock::acquire(&alias).unwrap().check_current().unwrap();
        assert_eq!(std::fs::read(&target).unwrap(),b"synthetic original bytes");
    }
}
