//! Native-selected destination publication; no renderer path or overwrite API.
//!
//! Copies the held cache descriptor to an unnamed inode, then publishes exactly
//! that inode once. The caller owns destination selection, job state, and the
//! final commit/cancel CAS. An OutcomeUnknown must never trigger replay or unlink.

use crate::cache_file::{CacheFile, CacheFileError};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SaveFileError {
    Cancelled,
    Conflict,
    SourceChanged,
    TooLarge,
    Unavailable,
    Io,
    OutcomeUnknown,
}

type Result<T> = std::result::Result<T, SaveFileError>;

pub(crate) fn publish(
    source: &mut CacheFile,
    destination: &Path,
    limit: u64,
    cancelled: impl FnMut() -> bool,
    progress: impl FnMut(u64, u64),
    begin_commit: impl FnOnce() -> bool,
) -> Result<u64> {
    #[cfg(target_os = "linux")]
    { linux::publish_with_sync(source, destination, limit, cancelled, progress, begin_commit, |file, _| file.sync_all()) }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (source, destination, limit, cancelled, progress, begin_commit);
        Err(SaveFileError::Unavailable)
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::{
        ffi::{CStr, CString},
        fs::{File, Metadata},
        io::{self, Write},
        os::{fd::{AsRawFd, FromRawFd, RawFd}, unix::{ffi::OsStrExt, fs::MetadataExt}},
    };

    const MAX_COMPONENTS: usize = 64;

    struct Directory {
        file: File,
        name: Option<CString>,
        identity: (u64, u64),
    }
    struct Destination {
        directories: Vec<Directory>,
        name: CString,
    }

    fn stat_at(parent: RawFd, name: &CStr, flags: libc::c_int) -> io::Result<libc::stat> {
        let mut info = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: parent is held, name is NUL-terminated, output storage is valid.
        let result = unsafe { libc::fstatat(parent, name.as_ptr(), info.as_mut_ptr(), flags) };
        if result != 0 { return Err(io::Error::last_os_error()); }
        // SAFETY: successful fstatat initialized the full structure.
        Ok(unsafe { info.assume_init() })
    }

    #[allow(clippy::unnecessary_cast)] // libc dev_t/ino_t widths differ across Linux ABIs.
    fn identity(info: &libc::stat) -> (u64, u64) { (info.st_dev as u64, info.st_ino as u64) }
    fn kind(mode: libc::mode_t, expected: libc::mode_t) -> bool { mode & libc::S_IFMT == expected }

    fn open_at(parent: RawFd, name: &CStr, flags: libc::c_int, mode: libc::mode_t) -> io::Result<File> {
        // SAFETY: parent/name are valid; mode is supplied for O_TMPFILE and ignored otherwise.
        let fd = unsafe { libc::openat(parent, name.as_ptr(), flags, mode) };
        if fd < 0 { return Err(io::Error::last_os_error()); }
        // SAFETY: the newly returned fd is transferred exactly once.
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    impl Destination {
        fn open(path: &Path) -> Result<Self> {
            let bytes = path.as_os_str().as_bytes();
            if !bytes.starts_with(b"/") { return Err(SaveFileError::Unavailable); }
            let parts: Vec<_> = bytes[1..].split(|byte| *byte == b'/').collect();
            if parts.is_empty() || parts.len() > MAX_COMPONENTS
                || parts.iter().any(|part| part.is_empty() || *part == b"." || *part == b".." || part.contains(&0)) {
                return Err(SaveFileError::Unavailable);
            }
            let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK;
            let file = open_at(libc::AT_FDCWD, c"/", flags, 0).map_err(|_| SaveFileError::Unavailable)?;
            let meta = file.metadata().map_err(|_| SaveFileError::Io)?;
            let mut directories = vec![Directory { file, name: None, identity: (meta.dev(), meta.ino()) }];
            for part in &parts[..parts.len() - 1] {
                let name = CString::new(*part).map_err(|_| SaveFileError::Unavailable)?;
                let parent = directories.last().ok_or(SaveFileError::Unavailable)?.file.as_raw_fd();
                let file = open_at(parent, &name, flags, 0).map_err(|_| SaveFileError::Unavailable)?;
                let meta = file.metadata().map_err(|_| SaveFileError::Io)?;
                directories.push(Directory { file, name: Some(name), identity: (meta.dev(), meta.ino()) });
            }
            let name = CString::new(*parts.last().ok_or(SaveFileError::Unavailable)?).map_err(|_| SaveFileError::Unavailable)?;
            let destination = Self { directories, name };
            destination.validate()?;
            destination.require_absent()?;
            Ok(destination)
        }

        fn parent(&self) -> Result<&File> {
            Ok(&self.directories.last().ok_or(SaveFileError::Unavailable)?.file)
        }

        fn validate(&self) -> Result<()> {
            for pair in self.directories.windows(2) {
                let stat = stat_at(pair[0].file.as_raw_fd(), pair[1].name.as_ref().ok_or(SaveFileError::Unavailable)?, libc::AT_SYMLINK_NOFOLLOW)
                    .map_err(|_| SaveFileError::Unavailable)?;
                if !kind(stat.st_mode, libc::S_IFDIR) || identity(&stat) != pair[1].identity {
                    return Err(SaveFileError::Unavailable);
                }
            }
            Ok(())
        }

        fn require_absent(&self) -> Result<()> {
            match stat_at(self.parent()?.as_raw_fd(), &self.name, libc::AT_SYMLINK_NOFOLLOW) {
                Ok(_) => Err(SaveFileError::Conflict),
                Err(error) if error.raw_os_error() == Some(libc::ENOENT) => Ok(()),
                Err(_) => Err(SaveFileError::Unavailable),
            }
        }
    }

    fn source_error(error: CacheFileError) -> SaveFileError {
        match error {
            CacheFileError::Cancelled => SaveFileError::Cancelled,
            CacheFileError::TooLarge => SaveFileError::TooLarge,
            CacheFileError::Unsupported => SaveFileError::Unavailable,
            CacheFileError::Io => SaveFileError::Io,
            CacheFileError::Changed | CacheFileError::InvalidPath | CacheFileError::UnsafeFile => SaveFileError::SourceChanged,
        }
    }

    fn unsupported(error: &io::Error) -> bool {
        matches!(error.raw_os_error(), Some(libc::EOPNOTSUPP | libc::EINVAL | libc::ENOSYS | libc::EISDIR | libc::EXDEV | libc::ENOENT | libc::EPERM))
    }

    struct ProgressWriter<'a, P> {
        file: &'a mut File,
        progress: P,
        done: u64,
        total: u64,
    }
    impl<P: FnMut(u64, u64)> Write for ProgressWriter<'_, P> {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let count = self.file.write(bytes)?;
            self.done += count as u64;
            (self.progress)(self.done, self.total);
            Ok(count)
        }
        fn flush(&mut self) -> io::Result<()> { self.file.flush() }
    }

    fn temp_metadata(file: &File, bytes: u64, links: u64) -> Result<Metadata> {
        let meta = file.metadata().map_err(|_| SaveFileError::Io)?;
        // SAFETY: geteuid has no memory/argument preconditions.
        let uid = unsafe { libc::geteuid() };
        if !meta.is_file() || meta.len() != bytes || meta.nlink() != links || meta.uid() != uid
            || meta.mode() & 0o777 != 0o600 {
            return Err(SaveFileError::Io);
        }
        Ok(meta)
    }

    pub(super) fn publish_with_sync(
        source: &mut CacheFile,
        path: &Path,
        limit: u64,
        mut cancelled: impl FnMut() -> bool,
        mut progress: impl FnMut(u64, u64),
        begin_commit: impl FnOnce() -> bool,
        sync: impl Fn(&File, bool) -> io::Result<()>,
    ) -> Result<u64> {
        if cancelled() { return Err(SaveFileError::Cancelled); }
        let destination = Destination::open(path)?;
        let mut temporary = open_at(destination.parent()?.as_raw_fd(), c".",
            libc::O_TMPFILE | libc::O_RDWR | libc::O_CLOEXEC, 0o600)
            .map_err(|error| if unsupported(&error) { SaveFileError::Unavailable } else { SaveFileError::Io })?;
        let initial = temp_metadata(&temporary, 0, 0)?;
        let total = source.metadata().byte_len;
        progress(0, total);
        let copied = source.copy_to(&mut ProgressWriter { file: &mut temporary, progress, done: 0, total }, limit, &mut cancelled)
            .map_err(source_error)?;
        if copied != total { return Err(SaveFileError::SourceChanged); }
        sync(&temporary, false).map_err(|_| SaveFileError::Io)?;
        source.check_unchanged().map_err(source_error)?;
        let before = temp_metadata(&temporary, copied, 0)?;
        if (initial.dev(), initial.ino()) != (before.dev(), before.ino()) { return Err(SaveFileError::Io); }
        destination.validate()?;
        destination.require_absent()?;
        let proc_name = CString::new(format!("/proc/self/fd/{}", temporary.as_raw_fd())).map_err(|_| SaveFileError::Unavailable)?;
        let proc_info = stat_at(libc::AT_FDCWD, &proc_name, 0).map_err(|_| SaveFileError::Unavailable)?;
        if identity(&proc_info) != (before.dev(), before.ino()) { return Err(SaveFileError::Unavailable); }
        let parent = destination.parent()?;
        if cancelled() || !begin_commit() { return Err(SaveFileError::Cancelled); }
        // The claim excludes cancellation; final identity checks can still fail
        // safely before visibility. Do no further copying/syncing before link.
        source.check_unchanged().map_err(source_error)?;
        destination.validate()?;
        // No cancellation check after the job's atomic commit claim. Link once;
        // O_TMPFILE plus a generated proc-fd path publishes exactly this inode.
        // SAFETY: both names are NUL-terminated; fd/name remain held through fsync.
        let linked = unsafe { libc::linkat(libc::AT_FDCWD, proc_name.as_ptr(), parent.as_raw_fd(), destination.name.as_ptr(), libc::AT_SYMLINK_FOLLOW) };
        if linked != 0 {
            let error = io::Error::last_os_error();
            return Err(if error.raw_os_error() == Some(libc::EEXIST) { SaveFileError::Conflict }
                else if unsupported(&error) { SaveFileError::Unavailable } else { SaveFileError::OutcomeUnknown });
        }
        // Every error after successful publication is ambiguous. Never unlink a
        // final pathname: it could already have been replaced by another actor.
        let finish = || -> Result<()> {
            destination.validate()?;
            let published = stat_at(parent.as_raw_fd(), &destination.name, libc::AT_SYMLINK_NOFOLLOW).map_err(|_| SaveFileError::Io)?;
            let after = temp_metadata(&temporary, copied, 1)?;
            if identity(&published) != (before.dev(), before.ino())
                || (after.dev(), after.ino(), after.mtime(), after.mtime_nsec()) != (before.dev(), before.ino(), before.mtime(), before.mtime_nsec()) {
                return Err(SaveFileError::Io);
            }
            sync(parent, true).map_err(|_| SaveFileError::Io)?;
            destination.validate()?;
            source.check_unchanged().map_err(source_error)?;
            Ok(())
        };
        finish().map_err(|_| SaveFileError::OutcomeUnknown)?;
        Ok(copied)
    }
}

#[cfg(all(test, target_os = "linux"))]
#[path = "../tests/support/save_file_tests.rs"]
mod tests;
