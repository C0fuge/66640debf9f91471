//! Bounded negative observation history. An association never certifies unlink,
//! descriptor closure or a right to forget an inode that moved outside a stage.
use super::{Inode,StageKey,MAX_INODES};
use crate::managed_stage::fd::Identity;
use std::collections::BTreeMap;

const MAX_RECORD_BYTES:usize=2048;
const MAX_BYTES:usize=2*1024*1024;

#[derive(Clone,PartialEq,Eq)]
pub(super) struct Location {
    pub(super) namespace:[Identity;5], // actual data/accounts/account/uploads/stage
    pub(super) key:StageKey,
    pub(super) name:Vec<u8>,
    pub(super) file:Identity,
}
#[derive(Clone,PartialEq,Eq)]
pub(super) enum Observation { At(Location),Ambiguous }
impl Observation {
    fn charge(&self)->Option<usize> {
        match self {
            Self::Ambiguous=>Some(64),
            Self::At(location)=>512usize.checked_add(location.key.account.len())?
                .checked_add(location.key.stage.len())?.checked_add(location.name.len())
                .filter(|bytes|*bytes<=MAX_RECORD_BYTES),
        }
    }
}

#[derive(Default)]
pub(super) struct Origins { entries:BTreeMap<Inode,Observation>,bytes:usize }
impl Origins {
    // Called only while retaining the corresponding actual scan inode. Returns
    // negative uncertainty when evidence could not fit, never a settlement grant.
    pub(super) fn record(&mut self,inode:Inode,observed:Option<Observation>)->bool {
        let mut observed=observed.unwrap_or(Observation::Ambiguous);
        if let Some(old)=self.entries.get(&inode) {
            if old!=&observed {observed=Observation::Ambiguous;}
        } else if self.entries.len()>=MAX_INODES {return true;}
        let old=self.entries.get(&inode).and_then(Observation::charge).unwrap_or(0);
        let charge=observed.charge();
        let next=charge.and_then(|charge|self.bytes.checked_sub(old)?.checked_add(charge));
        let lost=next.is_none_or(|bytes|bytes>MAX_BYTES);
        if lost {observed=Observation::Ambiguous;}
        self.bytes=self.bytes-old+observed.charge().expect("bounded ambiguity record");
        self.entries.insert(inode,observed);lost
    }
    #[cfg(test)]
    pub(super) fn get(&self,inode:&Inode)->Option<&Observation> {self.entries.get(inode)}
    #[cfg(test)]
    pub(super) fn len(&self)->usize {self.entries.len()}
}
