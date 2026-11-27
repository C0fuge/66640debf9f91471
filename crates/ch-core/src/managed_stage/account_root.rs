//! Bounded directory-only removal anchor, never a byte reader or deletion grant.
use super::{Error, Result, fd::{self, Dir, Policy}};
use crate::host::HostConfig;
use std::{ffi::CString, io, os::unix::ffi::OsStrExt, path::PathBuf, sync::Arc};

// Kept only in this Core's in-memory close barrier. No serde or persisted mount ID.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct AccountRootIdentity {
    root:PathBuf,
    ancestry:Vec<(PathBuf,fd::Identity)>,
    missing:Option<Vec<u8>>,
}

enum Held { Present(Arc<Dir>), Absent { parent:Arc<Dir>,name:CString } }

pub(crate) struct AccountRootAnchor { held:Held,identity:AccountRootIdentity }

impl AccountRootAnchor {
    pub(crate) fn capture(config:&HostConfig, account:&str) -> Result<Self> {
        let root=config.upload_dir(account).parent().ok_or(Error::Integrity)?.to_owned();
        let configured=config.data_dir.join("accounts");
        if root.parent()!=Some(configured.as_path()) { return Err(Error::Integrity); }
        let component=root.file_name().ok_or(Error::Integrity)?.as_bytes();
        fd::parts(&root)?;
        let data=fd::data_dir(&config.data_dir)?;
        let accounts=match data.child(c"accounts",Policy::Owner) {
            Err(error) if error.kind()==io::ErrorKind::NotFound => {
                return Self::absent(root,data,c"accounts".to_owned());
            }
            other=>other?,
        };
        if accounts.identity.mount!=data.identity.mount { return Err(Error::Integrity); }
        let name=fd::cname(component)?;
        let account=match accounts.child(&name,Policy::Owner) {
            Err(error) if error.kind()==io::ErrorKind::NotFound => return Self::absent(root,accounts,name),
            other=>other?,
        };
        if account.identity.mount!=accounts.identity.mount { return Err(Error::Integrity); }
        let identity=AccountRootIdentity {root,ancestry:account.ancestry_identity(),missing:None};
        let anchor=Self {held:Held::Present(account),identity};
        anchor.check()?;Ok(anchor)
    }

    fn absent(root:PathBuf,parent:Arc<Dir>,name:CString) -> Result<Self> {
        let identity=AccountRootIdentity {root,ancestry:parent.ancestry_identity(),missing:Some(name.as_bytes().to_vec())};
        let anchor=Self {held:Held::Absent {parent,name},identity};
        anchor.check()?;Ok(anchor)
    }

    pub(crate) fn identity(&self) -> &AccountRootIdentity { &self.identity }

    pub(crate) fn check(&self) -> Result<()> {
        match &self.held {
            Held::Present(dir)=>dir.check(),
            Held::Absent {parent,name}=>{
                parent.check()?;
                match fd::named(parent,name) {
                    Err(error) if error.kind()==io::ErrorKind::NotFound=>Ok(()),
                    _=>Err(Error::StageChanged),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs,os::unix::fs::PermissionsExt};

    fn directory(path:&std::path::Path) {
        fs::create_dir_all(path).unwrap();
        fs::set_permissions(path,fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[test]
    fn held_present_root_rejects_named_root_and_ancestry_replacement() {
        let temp=tempfile::tempdir().unwrap();let data=temp.path().join("data");
        let config=HostConfig::new(&data);let root=data.join("accounts/owner");
        directory(&root);
        let anchor=AccountRootAnchor::capture(&config,"owner").unwrap();
        anchor.check().unwrap();
        fs::rename(&root,data.join("accounts/previous")).unwrap();directory(&root);
        assert!(anchor.check().is_err());
        let replacement=AccountRootAnchor::capture(&config,"owner").unwrap();
        assert!(replacement.identity()!=anchor.identity());
        fs::rename(&data,temp.path().join("previous-data")).unwrap();directory(&root);
        assert!(replacement.check().is_err());
    }

    #[test]
    fn missing_accounts_or_owner_is_anchored_and_never_accepted_after_appearance() {
        let temp=tempfile::tempdir().unwrap();let config=HostConfig::new(temp.path());
        let accounts=temp.path().join("accounts");let root=accounts.join("owner");
        let absent=AccountRootAnchor::capture(&config,"owner").unwrap();absent.check().unwrap();
        directory(&accounts);assert!(absent.check().is_err());
        let absent=AccountRootAnchor::capture(&config,"owner").unwrap();absent.check().unwrap();
        let elsewhere=temp.path().join("elsewhere");directory(&elsewhere);
        std::os::unix::fs::symlink(&elsewhere,&root).unwrap();
        assert!(absent.check().is_err());
        assert!(AccountRootAnchor::capture(&config,"owner").is_err());
    }
}
