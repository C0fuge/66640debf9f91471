//! Original parent and kernel-lock descriptors. No managed-file authority.
use std::{ffi::CStr,fs::{File,OpenOptions},io,
    os::{fd::{AsRawFd,FromRawFd},unix::fs::{MetadataExt,OpenOptionsExt}},
    path::{Path,PathBuf}};

pub(super) struct Held { file:File,root:File,directory:PathBuf }

fn changed()->io::Error {io::Error::other("application data directory lock changed")}
fn directory(path:&Path)->io::Result<File> {
    // Preserve configured directory aliases. The actual held target, rather
    // than the spelling or symlink, is compared again before every use. Managed
    // namespace verification remains an independent, stricter boundary.
    OpenOptions::new().read(true).custom_flags(libc::O_DIRECTORY|libc::O_CLOEXEC|libc::O_NONBLOCK)
        .open(path)
}
fn at(root:&File,name:&CStr,flags:i32)->io::Result<File> {
    let mode:libc::mode_t=0o600;
    // SAFETY: root stays open and name is terminated; the new FD is owned once.
    let fd=unsafe {libc::openat(root.as_raw_fd(),name.as_ptr(),
        flags|libc::O_CLOEXEC|libc::O_NOFOLLOW|libc::O_NONBLOCK,mode)};
    if fd<0 {return Err(io::Error::last_os_error());}
    // SAFETY: successful openat returned a new descriptor, transferred once.
    Ok(unsafe {File::from_raw_fd(fd)})
}
fn regular(file:&File)->io::Result<std::fs::Metadata> {
    let meta=file.metadata()?;
    // SAFETY: geteuid has no memory preconditions.
    if !meta.is_file() || meta.nlink()!=1 || meta.uid()!=unsafe {libc::geteuid()} {return Err(changed());}
    Ok(meta)
}

impl Held {
    pub(super) fn acquire(path:&Path)->io::Result<Self> {
        let directory=std::path::absolute(path)?.components().collect::<PathBuf>();
        // Keep an actual root descriptor so a later same-spelling replacement
        // cannot renew this lock, even if its lock file is moved to the new root.
        let root=self::directory(&directory)?;
        let file=at(&root,c"app.lock",libc::O_RDWR|libc::O_CREAT)?;
        regular(&file)?;
        file.try_lock().map_err(|_|io::Error::other("cannot lock the application data directory"))?;
        let held=Self {file,root,directory};held.check_current()?;Ok(held)
    }

    pub(super) fn check_for(&self,expected:&Path)->io::Result<()> {
        self.check_current()?;
        let expected=directory(expected)?;
        let actual=expected.metadata()?;let original=self.root.metadata()?;
        if (actual.dev(),actual.ino())!=(original.dev(),original.ino()) {return Err(changed());}
        self.check_current()
    }

    pub(super) fn check_current(&self)->io::Result<()> {
        let current=directory(&self.directory)?;
        let original=self.root.metadata()?;let named=current.metadata()?;
        if (original.dev(),original.ino())!=(named.dev(),named.ino()) {return Err(changed());}
        let lock=regular(&self.file)?;
        // Metadata-only, no symlink following and no device/FIFO byte reader.
        let named=at(&current,c"app.lock",libc::O_PATH)?;
        let named=regular(&named)?;
        if (lock.dev(),lock.ino())!=(named.dev(),named.ino()) {return Err(changed());}
        Ok(())
    }
}
