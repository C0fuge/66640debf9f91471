//! Actual exclusive local byte creation. No admission or reader-closure claim.
use super::{Error,Result,MAX_BYTES,fd,manifest::{Manifest,Scope,Phase,Fingerprint,Format,hex},verify::open_uploads};
use crate::runtime::AccountStorageBinding;
use sha2::{Digest,Sha256};
use std::{fs::Permissions,io::{Read,Write},os::unix::fs::PermissionsExt};

/// An owned input stream. Input checks do not assert output birth or grant IO.
pub trait CopyInput:Read+Send+'static {
    fn check(&self)->Result<()>;
    fn publication_check(&self)->Result<()>;
}

/// Descriptive copy input. These values cannot designate a pre-existing byte inode.
pub struct CopyDescription {
    pub stage_id:String,
    pub scope:Scope,
    pub created:i64,
    pub name:String,
    pub size:u64,
    pub limit:u64,
}

/// Create a new private inode and publish a v1 Ready manifest under the actual
/// Accounts binding. Call only inside the counted local FS job. The checkpoint
/// validates the original input and cancellation at each chunk/publication edge.
/// All created byte descriptors/flocks drop before returning metadata. This is
/// exclusive creation; the Core publisher records protected birth before Ready.
pub(super) fn copy_ready_stage(owner:&AccountStorageBinding,description:CopyDescription,
    reader:&mut impl CopyInput,mut checkpoint:impl FnMut(u64)->Result<()>,
    before_ready:impl FnOnce(&fd::Dir,&Manifest)->std::result::Result<(),super::publisher::PublicationWriteError>)
    ->Result<Manifest> {
    let CopyDescription {stage_id,scope,created,name,size,limit}=description;
    let account=owner.owner();
    if !hex(&stage_id,32) || created<0 || scope.account!=account.account_id
        || scope.plugin!=account.plugin_id || scope.account_created!=account.created_at {
        return Err(Error::Integrity);
    }
    if size>limit.min(MAX_BYTES) {return Err(Error::TooLarge);}
    if size==0 {return Err(Error::Integrity);}
    let client_id=crate::host::managed_nonce().ok_or(Error::Storage)?;
    if client_id==stage_id || super::manifest::sanitize(Some(&name))!=name
        || !super::manifest::opaque(&scope.chat)
        || scope.topic.as_ref().is_some_and(|id|!super::manifest::opaque(id)) {
        return Err(Error::Integrity);
    }
    reader.check()?;checkpoint(0)?;
    let _mutation=owner.stage_mutations()?.begin()?;
    let uploads=open_uploads(owner,true)?;
    let display_name=name;
    let name=fd::cname(stage_id.as_bytes())?;
    uploads.mkdir(&name)?;
    let dir=uploads.child(&name,fd::Policy::Private)?;
    if dir.identity.mount!=uploads.identity.mount {return Err(Error::Integrity);}
    let _flock=dir.lock()?;
    let mut published=None;let mut birth_committed=false;
    let operation=(|| {
        uploads.file.sync_all()?;
        let mut bytes=fd::temp(&dir)?;
        let mut hash=Sha256::new();let mut prefix=Vec::with_capacity(8);
        let mut buffer=[0u8;64*1024];let mut count=0;
        while count<size {
            let cap=(size-count).min(buffer.len() as u64) as usize;
            let n=reader.read(&mut buffer[..cap])?;
            if n==0 {return Err(Error::Integrity);}
            reader.check()?;
            bytes.write_all(&buffer[..n])?;count+=n as u64;
            hash.update(&buffer[..n]);prefix.extend_from_slice(&buffer[..n.min(8-prefix.len())]);
            checkpoint(count)?;
            if owner.is_revoked() {return Err(Error::TargetChanged);}
        }
        if reader.read(&mut buffer[..1])?!=0 {return Err(Error::Integrity);}
        bytes.flush()?;bytes.sync_all()?;
        reader.publication_check()?;checkpoint(size)?;dir.check()?;
        if owner.is_revoked() {return Err(Error::TargetChanged);}
        if bytes.metadata()?.len()!=size {return Err(Error::Storage);}
        fd::link(&dir,&bytes,c"bytes")?;published=Some(fd::id(&bytes)?);
        bytes.set_permissions(Permissions::from_mode(0o400))?;bytes.sync_all()?;
        let manifest=Manifest {version:1,stage_id:stage_id.clone(),client_id,scope,created,
            name:display_name,size,format:Format::detect(&prefix),
            fingerprint:Fingerprint::from(bytes.metadata()?),sha256:format!("{:x}",hash.finalize()),phase:Phase::Ready};
        manifest.check()?;dir.bind(c"bytes",&bytes)?;dir.file.sync_all()?;
        match before_ready(&dir,&manifest) {
            Ok(())=>birth_committed=true,
            Err(super::publisher::PublicationWriteError::Refused(error))=>return Err(error),
            Err(super::publisher::PublicationWriteError::Unknown)=>{birth_committed=true;return Err(Error::Unknown);},
        }
        let encoded=serde_json::to_vec(&manifest).map_err(|_|Error::Integrity)?;
        if encoded.len()>64*1024 {return Err(Error::Integrity);}
        let mut metadata=fd::temp(&dir)?;metadata.write_all(&encoded)?;
        metadata.flush()?;metadata.sync_all()?;
        checkpoint(size)?;reader.publication_check()?;dir.bind(c"bytes",&bytes)?;
        if owner.is_revoked() {return Err(Error::TargetChanged);}
        fd::link(&dir,&metadata,c"manifest.json")?;
        // Ready may survive any following error. Never roll this publication back.
        dir.file.sync_all().map_err(|_|Error::Unknown)?;
        dir.bind(c"manifest.json",&metadata).map_err(|_|Error::Unknown)?;
        Ok(manifest)
    })();
    // Once the birth may have committed, retain the exact bytes/fact for local
    // recovery. A lost/closed waiter cannot manufacture quota-free cancellation.
    if birth_committed && operation.is_err() {return Err(Error::Unknown);}
    if matches!(&operation,Err(error) if *error!=Error::Unknown) {
        if let Some(identity)=published {fd::unlink(&dir,c"bytes",identity,false)?;}
        dir.file.sync_all()?;fd::unlink(&uploads,&name,dir.identity,true)?;
        uploads.file.sync_all().map_err(|_|Error::Unknown)?;
    }
    operation
}
