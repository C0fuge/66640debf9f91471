use super::{Error, Result};
use std::{
    ffi::{CStr, CString, OsStr},
    fs::{File, Metadata},
    io,
    os::{
        fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, RawFd},
        unix::{ffi::OsStrExt, fs::MetadataExt},
    },
    path::{Path, PathBuf},
    sync::Arc,
};

pub(super) fn uid() -> u32 {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() }
}

pub(super) fn parts(path: &Path) -> Result<Vec<&[u8]>> {
    let bytes = path.as_os_str().as_bytes();
    if bytes == b"/" {
        return Ok(Vec::new());
    }
    if bytes.len() > 4096 || !bytes.starts_with(b"/") {
        return Err(Error::Integrity);
    }
    let parts: Vec<_> = bytes[1..].split(|b| *b == b'/').collect();
    if parts.len() > 64
        || parts
            .iter()
            .any(|p| p.is_empty() || p.len() > 255 || *p == b"." || *p == b".." || p.contains(&0))
    {
        return Err(Error::Integrity);
    }
    Ok(parts)
}
pub(super) fn cname(bytes: &[u8]) -> Result<CString> {
    CString::new(bytes).map_err(|_| Error::Integrity)
}

pub(super) fn openat(
    parent: RawFd,
    name: &CStr,
    flags: i32,
    mode: libc::mode_t,
) -> io::Result<File> {
    // SAFETY: name is terminated and the successful call transfers one descriptor.
    let fd = unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            mode,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Identity {
    pub dev: u64,
    pub ino: u64,
    pub mount: u64,
}

fn statx(parent: RawFd, name: &CStr, flags: i32) -> io::Result<Identity> {
    let mut st = std::mem::MaybeUninit::uninit();
    let mask = libc::STATX_INO | libc::STATX_MNT_ID;
    // SAFETY: the successful syscall initializes st and name is valid.
    if unsafe { libc::statx(parent, name.as_ptr(), flags, mask, st.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let st = unsafe { st.assume_init() };
    if st.stx_mask & mask != mask {
        return Err(io::ErrorKind::Unsupported.into());
    }
    Ok(Identity {
        dev: libc::makedev(st.stx_dev_major, st.stx_dev_minor),
        ino: st.stx_ino,
        mount: st.stx_mnt_id,
    })
}
pub(super) fn id(file: &File) -> io::Result<Identity> {
    id_fd(file.as_fd())
}
pub(super) fn id_fd(file: BorrowedFd<'_>) -> io::Result<Identity> {
    statx(file.as_raw_fd(), c"", libc::AT_EMPTY_PATH)
}
pub(super) fn named(parent: &Dir, name: &CStr) -> io::Result<Identity> {
    statx(parent.fd(), name, libc::AT_SYMLINK_NOFOLLOW)
}

#[derive(Clone, Copy)]
pub(super) enum Policy {
    Any,
    Owner,
    Private,
}
impl Policy {
    pub(super) fn permits(self, m: &Metadata) -> bool {
        m.is_dir()
            && match self {
                Self::Any => true,
                Self::Owner => m.uid() == uid() && m.mode() & 0o022 == 0,
                Self::Private => m.uid() == uid() && m.mode() & 0o7777 == 0o700,
            }
    }
}
pub(super) struct Dir {
    pub file: File,
    pub path: PathBuf,
    pub identity: Identity,
    parent: Option<(Arc<Dir>, CString)>,
    policy: Policy,
}
impl Dir {
    pub fn root() -> Result<Arc<Self>> {
        let file = openat(libc::AT_FDCWD, c"/", libc::O_PATH | libc::O_DIRECTORY, 0)?;
        Ok(Arc::new(Self {
            identity: id(&file)?,
            file,
            path: "/".into(),
            parent: None,
            policy: Policy::Any,
        }))
    }
    pub fn fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
    pub fn child(self: &Arc<Self>, name: &CStr, policy: Policy) -> io::Result<Arc<Self>> {
        let access = if matches!(policy, Policy::Any) {
            libc::O_PATH
        } else {
            libc::O_RDONLY
        };
        let file = openat(self.fd(), name, access | libc::O_DIRECTORY, 0)?;
        if !policy.permits(&file.metadata()?) {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        Ok(Arc::new(Self {
            identity: id(&file)?,
            file,
            path: self.path.join(OsStr::from_bytes(name.to_bytes())),
            parent: Some((self.clone(), name.to_owned())),
            policy,
        }))
    }
    pub fn check(&self) -> Result<()> {
        if let Some((parent, name)) = &self.parent {
            parent.check()?;
            if named(parent, name)? != self.identity {
                return Err(Error::StageChanged);
            }
        }
        if !self.policy.permits(&self.file.metadata()?) {
            return Err(Error::StageChanged);
        }
        Ok(())
    }
    pub(super) fn identity_for_ancestor(&self, path: &Path) -> Result<Identity> {
        let mut current = Some(self);
        while let Some(dir) = current {
            if dir.path == path { return Ok(dir.identity); }
            current = dir.parent.as_ref().map(|(parent, _)| parent.as_ref());
        }
        Err(Error::Integrity)
    }
    pub fn lock(&self) -> Result<File> {
        // New open description: concurrent users of the same Dir also contend.
        let f = openat(self.fd(), c".", libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
        // SAFETY: f is valid, and the kernel releases the flock on drop.
        if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(
                if io::Error::last_os_error().kind() == io::ErrorKind::WouldBlock {
                    Error::Busy
                } else {
                    Error::Storage
                },
            );
        }
        Ok(f)
    }
    pub fn bind(&self, name: &CStr, f: &File) -> Result<()> {
        self.check()?;
        if id(f)? != named(self, name)? {
            return Err(Error::StageChanged);
        }
        Ok(())
    }
}

pub(super) fn temp(dir: &Dir) -> Result<File> {
    Ok(openat(
        dir.fd(),
        c".",
        libc::O_RDWR | libc::O_TMPFILE,
        0o600,
    )?)
}
pub(super) fn link(dir: &Dir, f: &File, name: &CStr) -> Result<()> {
    let source = cname(format!("/proc/self/fd/{}", f.as_raw_fd()).as_bytes())?;
    // SAFETY: link exactly the held, unnamed inode without replacing any name.
    if unsafe {
        libc::linkat(
            libc::AT_FDCWD,
            source.as_ptr(),
            dir.fd(),
            name.as_ptr(),
            libc::AT_SYMLINK_FOLLOW,
        )
    } != 0
    {
        return Err(Error::Storage);
    }
    Ok(())
}
pub(super) fn unlink(dir: &Dir, name: &CStr, expected: Identity, directory: bool) -> Result<()> {
    dir.check()?;
    if named(dir, name)? != expected {
        return Err(Error::StageChanged);
    }
    let flags = if directory { libc::AT_REMOVEDIR } else { 0 };
    // SAFETY: directory is held and only an identity-checked fixed entry is removed.
    if unsafe { libc::unlinkat(dir.fd(), name.as_ptr(), flags) } != 0 {
        return Err(Error::Storage);
    }
    Ok(())
}
pub(super) fn replace(dir: &Dir, from: &CStr, to: &CStr) -> Result<()> {
    // SAFETY: both names are constants under the same held directory.
    if unsafe { libc::renameat(dir.fd(), from.as_ptr(), dir.fd(), to.as_ptr()) } != 0 {
        return Err(Error::Storage);
    }
    Ok(())
}
