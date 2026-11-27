use super::{
    Error, Result, check_size,
    fd::{self, Dir, Identity, Policy},
    manifest::{Fingerprint, sanitize},
    mounts::Mounts,
};
use std::{
    ffi::CString,
    fs::File,
    io,
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::{Path, PathBuf},
    sync::Arc,
};

struct Private {
    parent: Arc<Dir>,
    leaf: Option<(CString, File, Identity)>,
    path: PathBuf,
}
impl Private {
    // Configured roots may not exist yet (e.g. optional plugin logs). Anchor the
    // nearest existing directory and still exclude its full lexical destination.
    fn open(path: &Path) -> Result<Self> {
        let parts = fd::parts(path)?;
        let mut dir = Dir::root()?;
        let mut leaf = None;
        for (i, component) in parts.iter().enumerate() {
            let name = fd::cname(component)?;
            if i + 1 == parts.len() {
                match fd::openat(dir.fd(), &name, libc::O_PATH, 0) {
                    Ok(f) if !f.metadata()?.is_symlink() => {
                        let id = fd::id(&f)?;
                        leaf = Some((name, f, id));
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    _ => return Err(Error::UnsafeSource),
                }
            } else {
                match dir.child(&name, Policy::Any) {
                    Ok(child) => dir = child,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => break,
                    Err(_) => return Err(Error::UnsafeSource),
                }
            }
        }
        Ok(Self {
            parent: dir,
            leaf,
            path: path.into(),
        })
    }
    fn binding(&self) -> Result<(u64, PathBuf)> {
        self.parent.check()?;
        let mount = if let Some((name, f, id)) = &self.leaf {
            self.parent.bind(name, f)?;
            id.mount
        } else {
            self.parent.identity.mount
        };
        Ok((mount, self.path.clone()))
    }
    fn excludes(&self, id: Identity) -> bool {
        self.leaf
            .as_ref()
            .is_some_and(|(_, _, root)| (root.dev, root.ino) == (id.dev, id.ino))
    }
}

pub(crate) struct Source {
    parent: Arc<Dir>,
    name: CString,
    pub(super) file: File,
    fingerprint: Fingerprint,
    private: Vec<Private>,
    pub(super) display_name: String,
    #[cfg(test)]
    pub(super) test_mounts: Option<Mounts>,
}
impl Source {
    pub fn open(path: &Path, private_roots: &[PathBuf], limit: u64) -> Result<Self> {
        Self::open_with(
            path,
            private_roots,
            limit,
            &Mounts::read().map_err(|_| Error::UnsafeSource)?,
        )
    }
    pub(super) fn open_with(
        path: &Path,
        roots: &[PathBuf],
        limit: u64,
        mounts: &Mounts,
    ) -> Result<Self> {
        if roots.is_empty() || roots.len() > 64 {
            return Err(Error::UnsafeSource);
        }
        let components = fd::parts(path).map_err(|_| Error::UnsafeSource)?;
        let (last, dirs) = components.split_last().ok_or(Error::UnsafeSource)?;
        if roots.iter().any(|root| path.starts_with(root)) {
            return Err(Error::UnsafeSource);
        }
        let private = roots
            .iter()
            .map(|p| Private::open(p))
            .collect::<Result<Vec<_>>>()
            .map_err(|_| Error::UnsafeSource)?;
        let mut bindings = private
            .iter()
            .map(Private::binding)
            .collect::<Result<Vec<_>>>()
            .map_err(|_| Error::UnsafeSource)?;
        mounts.protect_descendants(&mut bindings);
        let check = |identity: Identity, parent: Option<u64>, at: &Path| {
            if private.iter().any(|p| p.excludes(identity)) {
                return Err(Error::UnsafeSource);
            }
            mounts.check(identity.mount, identity.dev, parent, at, &bindings)
        };
        let mut parent = Dir::root()?;
        check(parent.identity, None, &parent.path)?;
        for part in dirs {
            let next = parent
                .child(&fd::cname(part)?, Policy::Any)
                .map_err(|_| Error::UnsafeSource)?;
            check(next.identity, Some(parent.identity.mount), &next.path)?;
            parent = next;
        }
        let name = fd::cname(last)?;
        // Inspect the type with O_PATH first, so even opening a device for reading
        // is unnecessary. The I/O descriptor must identify precisely that same entry.
        let probe =
            fd::openat(parent.fd(), &name, libc::O_PATH, 0).map_err(|_| Error::UnsafeSource)?;
        let initial = probe.metadata()?;
        if !initial.is_file() || initial.nlink() != 1 || initial.len() == 0 {
            return Err(Error::UnsafeSource);
        }
        check_size(initial.len(), limit)?;
        let identity = fd::id(&probe)?;
        check(identity, Some(parent.identity.mount), path)?;
        let file =
            fd::openat(parent.fd(), &name, libc::O_RDONLY, 0).map_err(|_| Error::UnsafeSource)?;
        if fd::id(&file)? != identity {
            return Err(Error::SourceChanged);
        }
        let source = Self {
            parent,
            name,
            file,
            fingerprint: initial.into(),
            private,
            display_name: sanitize(std::str::from_utf8(last).ok()),
            #[cfg(test)]
            test_mounts: None,
        };
        source.check()?;
        Ok(source)
    }
    pub fn display_name(&self)->&str {&self.display_name}
    pub fn len(&self) -> u64 {
        self.fingerprint.size
    }
    pub(super) fn check(&self) -> Result<()> {
        self.parent
            .bind(&self.name, &self.file)
            .map_err(|_| Error::SourceChanged)?;
        if Fingerprint::from(self.file.metadata()?) != self.fingerprint {
            return Err(Error::SourceChanged);
        }
        Ok(())
    }
    pub(super) fn publication_check(&self) -> Result<()> {
        self.check()?;
        #[cfg(test)]
        if let Some(mounts) = &self.test_mounts {
            return self.check_private(mounts);
        }
        let mounts = Mounts::read().map_err(|_| Error::SourceChanged)?;
        self.check_private(&mounts)?;
        Ok(())
    }
    fn check_private(&self, mounts: &Mounts) -> Result<()> {
        let mut bindings = Vec::new();
        for root in &self.private {
            bindings.push(root.binding().map_err(|_| Error::SourceChanged)?);
            // Also protect an optional root that appeared while copying.
            let now = Private::open(&root.path).map_err(|_| Error::SourceChanged)?;
            bindings.push(now.binding().map_err(|_| Error::SourceChanged)?);
        }
        mounts.protect_descendants(&mut bindings);
        let mut cursor = Some(&self.parent);
        while let Some(dir) = cursor {
            let parent = dir.parent();
            mounts.check(
                dir.identity.mount,
                dir.identity.dev,
                parent.map(|p| p.identity.mount),
                &dir.path,
                &bindings,
            )?;
            cursor = parent;
        }
        let leaf = fd::id(&self.file)?;
        let path = self
            .parent
            .path
            .join(std::ffi::OsStr::from_bytes(self.name.to_bytes()));
        mounts.check(
            leaf.mount,
            leaf.dev,
            Some(self.parent.identity.mount),
            &path,
            &bindings,
        )?;
        Ok(())
    }
}

impl std::io::Read for Source {
    fn read(&mut self,buffer:&mut [u8])->std::io::Result<usize> {
        std::io::Read::read(&mut self.file,buffer)
    }
}
impl ch_core::managed_stage::CopyInput for Source {
    fn check(&self)->Result<()> {Source::check(self)}
    fn publication_check(&self)->Result<()> {Source::publication_check(self)}
}
