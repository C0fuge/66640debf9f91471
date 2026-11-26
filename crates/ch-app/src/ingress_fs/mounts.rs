//! Read the calling thread's mount namespace at selection and publication. Held mount IDs,
//! including the leaf's, must still match their named entries on subsequent checks.
use super::{Error, Result};
use std::{
    collections::HashMap,
    ffi::OsStr,
    fs::File,
    io::Read,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
};

const MAX_TABLE: usize = 2 * 1024 * 1024;
const MAX_RECORDS: usize = 8192;

struct Mount {
    parent: u64,
    device: u64,
    root: PathBuf,
    point: PathBuf,
    fs: Vec<u8>,
    subvolume: Option<PathBuf>,
    subvolume_id: Option<u64>,
}
pub(super) struct Mounts(HashMap<u64, Mount>);
fn number(bytes: &[u8]) -> Result<u64> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return Err(Error::UnsafeSource);
    }
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(Error::UnsafeSource)
}
fn path(bytes: &[u8]) -> Result<PathBuf> {
    let mut result = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            let escaped = match bytes.get(i + 1..i + 4) {
                Some(b"040") => b' ',
                Some(b"011") => b'\t',
                Some(b"012") => b'\n',
                Some(b"134") => b'\\',
                _ => return Err(Error::UnsafeSource),
            };
            result.push(escaped);
            i += 4;
        } else {
            result.push(bytes[i]);
            i += 1;
        }
    }
    let path = PathBuf::from(OsStr::from_bytes(&result));
    if !path.is_absolute() {
        return Err(Error::UnsafeSource);
    }
    Ok(path)
}
impl Mounts {
    pub fn read() -> Result<Self> {
        let mut bytes = Vec::new();
        File::open("/proc/thread-self/mountinfo")?
            .take(MAX_TABLE as u64 + 1)
            .read_to_end(&mut bytes)?;
        Self::parse(&bytes)
    }
    pub(super) fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_TABLE || !bytes.ends_with(b"\n") {
            return Err(Error::UnsafeSource);
        }
        let mut mounts = HashMap::new();
        for line in bytes[..bytes.len() - 1].split(|b| *b == b'\n') {
            if mounts.len() >= MAX_RECORDS {
                return Err(Error::UnsafeSource);
            }
            let mut fields = line.split(|b| *b == b' ');
            let mut field = || fields.next().ok_or(Error::UnsafeSource);
            let id = number(field()?)?;
            let parent = number(field()?)?;
            let mut dev = field()?.split(|b| *b == b':');
            let major = number(dev.next().ok_or(Error::UnsafeSource)?)?;
            let minor = number(dev.next().ok_or(Error::UnsafeSource)?)?;
            if dev.next().is_some() || major > u64::from(u32::MAX) || minor > u64::from(u32::MAX) {
                return Err(Error::UnsafeSource);
            }
            let device = libc::makedev(major as u32, minor as u32);
            let root = path(field()?)?;
            let point = path(field()?)?;
            field()?; // per-mount options
            if !fields.any(|f| f == b"-") {
                return Err(Error::UnsafeSource);
            }
            let fs = fields.next().ok_or(Error::UnsafeSource)?.to_vec();
            fields.next().ok_or(Error::UnsafeSource)?; // kernel mount source
            let options = fields.next().ok_or(Error::UnsafeSource)?;
            if fields.next().is_some() {
                return Err(Error::UnsafeSource);
            }
            let mut subvolume = None;
            let mut subvolume_id = None;
            for option in options.split(|c| *c == b',') {
                if let Some(value) = option.strip_prefix(b"subvol=")
                    && subvolume.replace(path(value)?).is_some()
                {
                    return Err(Error::UnsafeSource);
                }
                if let Some(value) = option.strip_prefix(b"subvolid=")
                    && subvolume_id.replace(number(value)?).is_some()
                {
                    return Err(Error::UnsafeSource);
                }
            }
            if mounts
                .insert(
                    id,
                    Mount {
                        parent,
                        device,
                        root,
                        point,
                        fs,
                        subvolume,
                        subvolume_id,
                    },
                )
                .is_some()
            {
                return Err(Error::UnsafeSource);
            }
        }
        Ok(Self(mounts))
    }

    // Include every mounted descendant of configured roots without walking their contents.
    pub fn protect_descendants(&self, roots: &mut Vec<(u64, PathBuf)>) {
        let additional: Vec<_> = self
            .0
            .iter()
            .filter(|(_, m)| roots.iter().any(|(_, p)| m.point.starts_with(p)))
            .map(|(id, m)| (*id, m.point.clone()))
            .collect();
        roots.extend(additional);
    }

    pub fn check(
        &self,
        id: u64,
        device: u64,
        parent: Option<u64>,
        at: &Path,
        private: &[(u64, PathBuf)],
    ) -> Result<()> {
        let m = self.0.get(&id).ok_or(Error::UnsafeSource)?;
        if m.device != device {
            return Err(Error::UnsafeSource);
        }
        let boundary = parent != Some(id);
        if boundary && (m.point != at || parent.is_some_and(|p| p != m.parent)) {
            return Err(Error::UnsafeSource);
        }
        let verified = match m.fs.as_slice() {
            b"ext2" | b"ext3" | b"ext4" | b"xfs" | b"f2fs" | b"vfat" | b"exfat" | b"ntfs3"
            | b"tmpfs" => m.root == Path::new("/"),
            b"btrfs" => {
                m.subvolume.as_ref() == Some(&m.root) && m.subvolume_id.is_some_and(|v| v > 0)
            }
            _ => false,
        };
        if !verified {
            return Err(Error::UnsafeSource);
        }
        for (private_id, private_path) in private {
            let pm = self.0.get(private_id).ok_or(Error::UnsafeSource)?;
            if id == *private_id {
                if at.starts_with(private_path) {
                    return Err(Error::UnsafeSource);
                }
            } else if !private_path.starts_with(at) {
                // For different proven Btrfs subvolumes, mountinfo gives their
                // paths relative to the filesystem's top-level tree. Siblings
                // are distinct. A mounted private descendant remains forbidden.
                if m.fs == b"btrfs"
                    && pm.fs == b"btrfs"
                    && m.subvolume_id != pm.subvolume_id
                    && pm.subvolume.as_ref() == Some(&pm.root)
                    && pm.subvolume_id.is_some_and(|v| v > 0)
                {
                    let physical = m
                        .root
                        .join(at.strip_prefix(&m.point).map_err(|_| Error::UnsafeSource)?);
                    let protected = pm.root.join(
                        private_path
                            .strip_prefix(&pm.point)
                            .map_err(|_| Error::UnsafeSource)?,
                    );
                    if physical.starts_with(protected) {
                        return Err(Error::UnsafeSource);
                    }
                } else if m.device == pm.device || (m.fs == b"btrfs" && pm.fs == b"btrfs") {
                    return Err(Error::UnsafeSource);
                }
            }
        }
        Ok(())
    }
}
