//! Internal held-descriptor boundary for account-cache reads.
//!
//! The caller must derive `cache_root` from the host's account configuration and
//! `candidate` from validated core state, never renderer-supplied paths. This
//! module neither chooses a destination nor publishes a copied file. On any copy
//! error the caller must discard its temporary destination before publication.
//! This checks inode/path stability, not messenger content authenticity. Blocking
//! filesystem/write syscalls are not preempted; cancellation is checked between
//! bounded chunks. Production registration belongs to the later save integration.

use std::{fmt, io::Write, path::Path};

/// Local safety budget, independent of any platform's advertised attachment limit.
pub(crate) const MAX_CACHE_FILE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CacheFileError {
    #[allow(dead_code)] // Reserved for the non-Unix implementation.
    Unsupported,
    InvalidPath,
    UnsafeFile,
    Changed,
    TooLarge,
    Cancelled,
    Io,
}

impl fmt::Display for CacheFileError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::Unsupported => "Cache-file access is unsupported on this platform.",
            Self::InvalidPath => "The cache-file location is invalid.",
            Self::UnsafeFile => "The cache-file location is unsafe.",
            Self::Changed => "The cache file changed or is no longer available.",
            Self::TooLarge => "The cache file exceeds the copy limit.",
            Self::Cancelled => "The cache-file copy was cancelled.",
            Self::Io => "The cache file could not be copied.",
        })
    }
}
impl std::error::Error for CacheFileError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CacheMetadata {
    pub byte_len: u64,
}

#[cfg(unix)]
mod platform {
    use super::*;
    use std::{
        ffi::{CStr, CString, OsStr},
        fs::{File, Metadata},
        io::{Read, Seek, SeekFrom},
        os::{fd::{AsRawFd, FromRawFd, RawFd}, unix::{ffi::OsStrExt, fs::MetadataExt}},
    };

    const MAX_COMPONENTS: usize = 64;
    const CHUNK: usize = 64 * 1024;
    type Result<T> = std::result::Result<T, CacheFileError>;

    #[derive(Clone, Copy, PartialEq, Eq)]
    struct Fingerprint {
        dev: u64, ino: u64, size: u64, mode: u32, uid: u32, nlink: u64,
        mtime: (i64, i64), ctime: (i64, i64),
    }
    impl Fingerprint {
        fn of(meta: &Metadata) -> Self {
            Self { dev: meta.dev(), ino: meta.ino(), size: meta.size(), mode: meta.mode(),
                uid: meta.uid(), nlink: meta.nlink(), mtime: (meta.mtime(), meta.mtime_nsec()),
                ctime: (meta.ctime(), meta.ctime_nsec()) }
        }
    }

    struct Directory {
        file: File,
        // Name relative to the preceding held directory; None denotes "/".
        name: Option<CString>,
        identity: (u64, u64),
    }

    pub(crate) struct CacheFile {
        file: File,
        directories: Vec<Directory>,
        name: CString,
        fingerprint: Fingerprint,
    }

    fn components(path: &Path) -> Result<Vec<&OsStr>> {
        let bytes = path.as_os_str().as_bytes();
        if !bytes.starts_with(b"/") { return Err(CacheFileError::InvalidPath); }
        let parts: Vec<_> = bytes[1..].split(|byte| *byte == b'/').collect();
        if parts.is_empty() || parts.len() > MAX_COMPONENTS
            || parts.iter().any(|p| p.is_empty() || *p == b"." || *p == b".." || p.contains(&0)) {
            return Err(CacheFileError::InvalidPath);
        }
        Ok(parts.into_iter().map(OsStr::from_bytes).collect())
    }

    fn name(value: &OsStr) -> Result<CString> {
        CString::new(value.as_bytes()).map_err(|_| CacheFileError::InvalidPath)
    }

    fn open_at(parent: RawFd, name: &CStr, flags: libc::c_int) -> Result<File> {
        // SAFETY: name is NUL-terminated, and parent remains a held descriptor
        // for the call (or the explicit AT_FDCWD used for the absolute "/").
        let fd = unsafe { libc::openat(parent, name.as_ptr(), flags) };
        if fd < 0 { return Err(CacheFileError::UnsafeFile); }
        // SAFETY: openat returned a new descriptor, transferred exactly once.
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    fn stat_at(parent: RawFd, name: &CStr) -> Result<libc::stat> {
        let mut info = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: parent/name are valid for the call; info is writable storage.
        let result = unsafe { libc::fstatat(parent, name.as_ptr(), info.as_mut_ptr(), libc::AT_SYMLINK_NOFOLLOW) };
        if result != 0 { return Err(CacheFileError::Changed); }
        // SAFETY: a successful fstatat initialized the complete stat structure.
        Ok(unsafe { info.assume_init() })
    }

    #[allow(clippy::unnecessary_cast)] // libc dev_t/ino_t widths differ across Unix ABIs.
    fn identity(info: &libc::stat) -> (u64, u64) {
        (info.st_dev as u64, info.st_ino as u64)
    }

    fn is_kind(mode: libc::mode_t, kind: libc::mode_t) -> bool {
        mode & libc::S_IFMT == kind
    }

    impl CacheFile {
        pub(crate) fn open(cache_root: &Path, candidate: &Path) -> Result<Self> {
            let root = components(cache_root)?;
            let selected = components(candidate)?;
            if selected.len() <= root.len() || !selected.starts_with(&root) {
                return Err(CacheFileError::InvalidPath);
            }
            let flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY
                | libc::O_NOFOLLOW | libc::O_NONBLOCK;
            let file = open_at(libc::AT_FDCWD, c"/", flags)?;
            let meta = file.metadata().map_err(|_| CacheFileError::Io)?;
            let mut directories = vec![Directory { file, name: None, identity: (meta.dev(), meta.ino()) }];
            for component in &selected[..selected.len() - 1] {
                let part = name(component)?;
                let parent = directories.last().ok_or(CacheFileError::InvalidPath)?.file.as_raw_fd();
                let file = open_at(parent, &part, flags)?;
                let meta = file.metadata().map_err(|_| CacheFileError::Io)?;
                directories.push(Directory { file, name: Some(part), identity: (meta.dev(), meta.ino()) });
            }
            let basename = name(selected.last().ok_or(CacheFileError::InvalidPath)?)?;
            let parent = directories.last().ok_or(CacheFileError::InvalidPath)?.file.as_raw_fd();
            // Preflight avoids deliberately opening known devices/FIFOs. The
            // final open is still nonblocking and fstat-validated against races.
            let before = stat_at(parent, &basename)?;
            // SAFETY: geteuid has no arguments or memory preconditions.
            let uid = unsafe { libc::geteuid() };
            if !is_kind(before.st_mode, libc::S_IFREG) || before.st_nlink != 1 || before.st_uid != uid {
                return Err(CacheFileError::UnsafeFile);
            }
            let file = open_at(parent, &basename,
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)?;
            let meta = file.metadata().map_err(|_| CacheFileError::Io)?;
            if !meta.is_file() || meta.nlink() != 1 || meta.uid() != uid {
                return Err(CacheFileError::UnsafeFile);
            }
            if identity(&before) != (meta.dev(), meta.ino()) {
                return Err(CacheFileError::Changed);
            }
            if meta.len() > MAX_CACHE_FILE_BYTES { return Err(CacheFileError::TooLarge); }
            let result = Self { file, directories, name: basename, fingerprint: Fingerprint::of(&meta) };
            result.validate()?;
            Ok(result)
        }

        pub(crate) fn metadata(&self) -> CacheMetadata {
            CacheMetadata { byte_len: self.fingerprint.size }
        }

        /// Recheck the held descriptor and original anchored pathname before a
        /// separate publication step; no source bytes are reopened by pathname.
        pub(crate) fn check_unchanged(&self) -> Result<()> { self.validate() }

        fn validate_file(&self) -> Result<()> {
            let meta = self.file.metadata().map_err(|_| CacheFileError::Io)?;
            if Fingerprint::of(&meta) != self.fingerprint || !meta.is_file() || meta.nlink() != 1 {
                return Err(CacheFileError::Changed);
            }
            Ok(())
        }

        fn validate(&self) -> Result<()> {
            self.validate_file()?;
            // Held ancestor descriptors prevent redirection; identity checks also
            // reject a root/component which has since been renamed or replaced.
            for pair in self.directories.windows(2) {
                let info = stat_at(pair[0].file.as_raw_fd(), pair[1].name.as_ref().ok_or(CacheFileError::InvalidPath)?)?;
                if !is_kind(info.st_mode, libc::S_IFDIR)
                    || identity(&info) != pair[1].identity {
                    return Err(CacheFileError::Changed);
                }
            }
            let parent = self.directories.last().ok_or(CacheFileError::InvalidPath)?.file.as_raw_fd();
            let info = stat_at(parent, &self.name)?;
            if !is_kind(info.st_mode, libc::S_IFREG)
                || identity(&info) != (self.fingerprint.dev, self.fingerprint.ino) {
                return Err(CacheFileError::Changed);
            }
            Ok(())
        }

        /// Copy exactly the opened file's bytes, bounded by both caps. A caller
        /// must discard the destination on any error; this method never publishes.
        pub(crate) fn copy_to<W: Write>(
            &mut self, destination: &mut W, limit: u64, mut cancelled: impl FnMut() -> bool,
        ) -> Result<u64> {
            if self.fingerprint.size > limit { return Err(CacheFileError::TooLarge); }
            if cancelled() { return Err(CacheFileError::Cancelled); }
            self.validate()?;
            self.file.seek(SeekFrom::Start(0)).map_err(|_| CacheFileError::Io)?;
            let mut buffer = [0_u8; CHUNK];
            let mut copied = 0;
            while copied < self.fingerprint.size {
                if cancelled() { return Err(CacheFileError::Cancelled); }
                self.validate_file()?;
                let count = (self.fingerprint.size - copied).min(CHUNK as u64) as usize;
                let count = self.file.read(&mut buffer[..count]).map_err(|_| CacheFileError::Io)?;
                if count == 0 { return Err(CacheFileError::Changed); }
                destination.write_all(&buffer[..count]).map_err(|_| CacheFileError::Io)?;
                copied += count as u64;
                self.validate_file()?;
            }
            if cancelled() { return Err(CacheFileError::Cancelled); }
            let mut extra = [0_u8; 1];
            if self.file.read(&mut extra).map_err(|_| CacheFileError::Io)? != 0 {
                return Err(CacheFileError::Changed);
            }
            self.validate()?;
            Ok(copied)
        }
    }
}

#[cfg(not(unix))]
mod platform {
    use super::*;
    pub(crate) struct CacheFile;
    impl CacheFile {
        pub(crate) fn open(_: &Path, _: &Path) -> Result<Self, CacheFileError> { Err(CacheFileError::Unsupported) }
        pub(crate) fn metadata(&self) -> CacheMetadata { CacheMetadata { byte_len: 0 } }
        pub(crate) fn check_unchanged(&self) -> Result<(), CacheFileError> { Err(CacheFileError::Unsupported) }
        pub(crate) fn copy_to<W: Write>(&mut self, _: &mut W, _: u64, _: impl FnMut() -> bool) -> Result<u64, CacheFileError> {
            Err(CacheFileError::Unsupported)
        }
    }
}

pub(crate) use platform::CacheFile;
