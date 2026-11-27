//! Metadata-only, fixed-depth accounting. Partial totals are lower bounds when
//! complete is false. The writer must refuse new reservations in that case.
use super::{
    Error, Manifest, Phase, Result, Stage, Store,
    fd::{self, Dir, Policy},
    manifest::hex,
    store::read_manifest,
};
use std::{
    collections::{BTreeMap, HashSet},
    ffi::OsString,
    fs, io,
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::PathBuf,
};

#[derive(Default)]
pub(crate) struct Charge {
    pub slots: u64,
    pub bytes: u64,
}
pub(crate) struct Inventory {
    pub complete: bool,
    pub total: Charge,
    pub per_directory: BTreeMap<OsString, Charge>,
    pub manifests: Vec<Manifest>,
}
struct Budget {
    accounts: usize,
    managed: usize,
    exhausted: bool,
}
fn entries(dir: &Dir, remaining: &mut usize, exhausted: &mut bool) -> Result<Vec<OsString>> {
    // procfs names only the held descriptor. Iterator/metadata errors never
    // become an empty directory; no guessed child paths or recursive traversal.
    dir.check()?;
    let path = PathBuf::from(format!("/proc/self/fd/{}", dir.fd()));
    let mut names = Vec::new();
    for item in fs::read_dir(path)? {
        if *remaining == 0 {
            *exhausted = true;
            return Err(Error::Integrity);
        }
        *remaining -= 1;
        names.push(item?.file_name());
    }
    dir.check()?;
    names.sort_unstable();
    Ok(names)
}
impl Store {
    pub fn inventory(&self) -> Inventory {
        let mut scan = Inventory {
            complete: true,
            total: Charge::default(),
            per_directory: BTreeMap::new(),
            manifests: Vec::new(),
        };
        // Actual metadata descriptors can pin unlinked storage too. Caller loss
        // cannot release this scope while the synchronous scan still owns them.
        let Ok(_readers)=self.mutations.as_ref().map(|hook|hook.read_scope()).transpose() else {
            scan.complete=false;return scan;
        };
        let mut budget = Budget {
            accounts: 4096,
            managed: 256,
            exhausted: false,
        };
        let run = (|| {
            let names = entries(&self.accounts, &mut budget.accounts, &mut budget.exhausted)?;
            for key in names {
                if budget.exhausted {
                    break;
                }
                if self.scan_account(&key, &mut budget, &mut scan).is_err() {
                    scan.complete = false;
                }
            }
            self.accounts.check()
        })();
        scan.complete &= run.is_ok() && !budget.exhausted;
        // A duplicated fixed client or stage identity cannot be safely imported,
        // even across otherwise valid manifests. Retain and charge their bytes.
        let mut seen_stages = HashSet::new();
        let mut seen_clients = HashSet::new();
        let mut duplicates = HashSet::new();
        for m in &scan.manifests {
            if !seen_stages.insert(m.stage_id.clone()) {
                duplicates.insert(m.stage_id.clone());
            }
            if !seen_clients.insert(m.client_id.clone()) {
                duplicates.insert(m.client_id.clone());
            }
        }
        if !duplicates.is_empty() {
            scan.complete = false;
            scan.manifests.retain(|m| {
                !duplicates.contains(&m.stage_id) && !duplicates.contains(&m.client_id)
            });
        }
        scan
    }
    fn scan_account(
        &self,
        key: &OsString,
        budget: &mut Budget,
        scan: &mut Inventory,
    ) -> Result<()> {
        let account = self
            .accounts
            .child(&fd::cname(key.as_bytes())?, Policy::Owner)?;
        if account.identity.mount != self.accounts.identity.mount {
            return Err(Error::Integrity);
        }
        let uploads = match account.child(c"uploads", Policy::Private) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            result => result?,
        };
        if uploads.identity.mount != account.identity.mount {
            return Err(Error::Integrity);
        }
        let names = entries(&uploads, &mut budget.managed, &mut budget.exhausted)?;
        let owner = self.owners.get(key).and_then(Option::as_ref);
        for name in names {
            if budget.exhausted {
                break;
            }
            // Every stage directory, including an incomplete/orphan allocation,
            // reserves a slot until explicitly removed and durably settled.
            scan.total.slots += 1;
            let charge = scan.per_directory.entry(key.clone()).or_default();
            charge.slots += 1;
            let measured = (|| {
                let stage_name = name
                    .to_str()
                    .filter(|s| hex(s, 32))
                    .ok_or(Error::Integrity)?;
                let dir = uploads.child(&fd::cname(name.as_bytes())?, Policy::Private)?;
                if dir.identity.mount != uploads.identity.mount {
                    return Err(Error::Integrity);
                }
                let children = entries(&dir, &mut budget.managed, &mut budget.exhausted)?;
                let mut has_bytes = false;
                let mut valid = true;
                for child in children {
                    let file =
                        fd::openat(dir.fd(), &fd::cname(child.as_bytes())?, libc::O_PATH, 0)?;
                    let m = file.metadata()?;
                    if !m.is_file()
                        || fd::id(&file)?.mount != dir.identity.mount
                        || m.uid() != fd::uid()
                        || m.nlink() != 1
                    {
                        valid = false;
                    }
                    match child.as_bytes() {
                        b"bytes" => {
                            has_bytes = true;
                            scan.total.bytes = scan
                                .total
                                .bytes
                                .checked_add(m.len())
                                .ok_or(Error::Integrity)?;
                            charge.bytes =
                                charge.bytes.checked_add(m.len()).ok_or(Error::Integrity)?;
                        }
                        b"manifest.json" => {}
                        _ => {
                            valid = false;
                        } // e.g. interrupted transition's .next
                    }
                }
                let (m, _) = read_manifest(&dir)?;
                if m.stage_id != stage_name {
                    return Err(Error::Integrity);
                }
                // Keep proven local admission/claim evidence visible even when
                // damage to the retained bytes freezes further allocations.
                let stage = Stage::from_inventory(dir.clone(), m, self.mutations.clone())?;
                valid &= if has_bytes {
                    stage.check_bytes_metadata().is_ok()
                } else {
                    stage.manifest().phase == Phase::Discarded
                };
                let phase = &stage.manifest().phase;
                if owner.is_some_and(|o| o.matches(&stage.manifest().scope))
                    && (matches!(phase, Phase::Claimed(_) | Phase::Consumed(_))
                        || (*phase == Phase::Ready && valid))
                {
                    scan.manifests.push(stage.manifest().clone());
                }
                if !valid {
                    return Err(Error::Integrity);
                }
                dir.check()
            })();
            if measured.is_err() {
                scan.complete = false;
            }
        }
        uploads.check()
    }
}
