use super::{
    Error, Format, Intent, Inventory, LiveClaim, MAX_BYTES, Owner, Phase, Result, Scope, Source,
    Stage, Store, Verified, fd, manifest::sanitize, mounts::Mounts, store,
};
use std::{
    cell::Cell,
    fs::{self, File, Permissions},
    io,
    os::unix::{
        ffi::OsStrExt,
        fs::{MetadataExt, PermissionsExt, symlink},
    },
    path::{Path, PathBuf},
};

const ID: &str = "11223344556677889900aabbccddeeff";
struct Fixture {
    _temp: tempfile::TempDir,
    accounts: PathBuf,
    private: PathBuf,
    source: PathBuf,
    owner: Owner,
    store: Store,
}
impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let base = temp.path().canonicalize().unwrap();
        let private = base.join("data");
        let accounts = private.join("accounts");
        let account = accounts.join("test-account");
        fs::create_dir_all(&account).unwrap();
        let source = base.join("synthetic.bin");
        fs::write(&source, b"\x89PNG\r\n\x1a\nfixture").unwrap();
        let owner = Owner {
            account: "test-account".into(),
            plugin: "test-plugin".into(),
            account_created: 1,
            uploads: account.join("uploads"),
        };
        let store = Store::open(&accounts, vec![owner.clone()]).unwrap();
        Self {
            _temp: temp,
            accounts,
            private,
            source,
            owner,
            store,
        }
    }
    fn scope(&self) -> Scope {
        Scope {
            account: self.owner.account.clone(),
            plugin: self.owner.plugin.clone(),
            account_created: 1,
            chat: "chat".into(),
            topic: Some("topic".into()),
        }
    }
    fn mounts(&self) -> Mounts {
        // The CI sandbox itself may be an overlay or container bind. Supply
        // synthetic ordinary mount records keyed to the actual temporary tree's
        // held statx identities; no mount privileges or /proc changes.
        let mut dir = fd::Dir::root().unwrap();
        let mut text = String::new();
        let mut record = |dir: &fd::Dir, parent: u64| {
            let id = dir.identity;
            text.push_str(&format!(
                "{} {} {}:{} / {} rw - ext4 /dev/test rw\n",
                id.mount,
                parent,
                libc::major(id.dev),
                libc::minor(id.dev),
                dir.path.display()
            ));
        };
        record(&dir, 0);
        for part in fd::parts(self.source.parent().unwrap()).unwrap() {
            let next = dir
                .child(&fd::cname(part).unwrap(), fd::Policy::Any)
                .unwrap();
            if dir.identity.mount != next.identity.mount {
                record(&next, dir.identity.mount);
            }
            dir = next;
        }
        // Do not use the same synthetic device across a mount crossing unless the OS
        // really uses it. This preserves private-alias checks on real descriptors.
        Mounts::parse(text.as_bytes()).unwrap()
    }
    fn open_path(&self, path: &Path, limit: u64) -> Result<Source> {
        let mounts = self.mounts();
        let mut source =
            Source::open_with(path, std::slice::from_ref(&self.private), limit, &mounts)?;
        source.test_mounts = Some(mounts);
        Ok(source)
    }
    fn open(&self) -> Source {
        self.open_path(&self.source, MAX_BYTES).unwrap()
    }
    fn stage(&self) -> Stage {
        self.store
            .stage(
                &self.owner,
                ID,
                self.scope(),
                2,
                self.open(),
                MAX_BYTES,
                |_| Ok(()),
            )
            .unwrap()
    }
    fn reload(&self) -> Store {
        Store::open(&self.accounts, vec![self.owner.clone()]).unwrap()
    }
}
fn intent() -> Intent {
    Intent {
        caption: "  💬\n".into(),
        as_file: true,
        spoiler: false,
    }
}

async fn core_for_catalog(f: &Fixture) -> ch_core::runtime::Core {
    let mut config = ch_core::runtime::CoreConfig::new(&f.private,
        std::sync::Arc::new(ch_core::host::MemorySecretStore::new()));
    config.autostart = false;
    ch_core::runtime::Core::open(config).await.unwrap()
}

#[tokio::test]
async fn core_hook_keeps_v1_copy_quota_and_recovered_discard_and_vetoes_closed_writes() {
    let mut f = Fixture::new();
    let core = core_for_catalog(&f).await;
    f.store = Store::open_with_mutations(&f.accounts, vec![f.owner.clone()], core.stage_mutations()).unwrap();
    // A failed copy still follows the existing bounded cleanup/quota behavior.
    let failed = f.store.stage(&f.owner, ID, f.scope(), 2, f.open(), MAX_BYTES,
        |copied| if copied > 0 { Err(Error::Cancelled) } else { Ok(()) });
    assert!(matches!(failed, Err(Error::Cancelled)));
    assert_eq!(f.store.inventory().total.slots, 0);
    let stage = f.stage();
    let json = fs::read(stage.managed_path().with_file_name("manifest.json")).unwrap();
    let fields = serde_json::from_slice::<serde_json::Value>(&json).unwrap();
    let expected: std::collections::BTreeSet<_> = ["version", "stage_id", "client_id", "scope", "created",
        "name", "size", "format", "fingerprint", "sha256", "phase"].into_iter().collect();
    assert_eq!(fields.as_object().unwrap().keys().map(String::as_str).collect::<std::collections::BTreeSet<_>>(), expected);
    assert!(stage.manifest().phase == Phase::Ready);
    let recovered = f.store.recover(&f.owner, ID).unwrap().clone();
    assert!(recovered.discard_ready(|_, _| Ok(false)).is_err());
    assert_eq!(fs::read(stage.managed_path().with_file_name("manifest.json")).unwrap(), json);
    let inventory = f.store.inventory();
    assert!(inventory.complete);
    assert_eq!(inventory.total.slots, 1);
    assert_eq!(inventory.total.bytes, f.open().len());
    core.shutdown().await;
    let called = Cell::new(false);
    assert!(matches!(recovered.discard_ready(|_, _| { called.set(true); Ok(true) }), Err(Error::Cancelled)));
    assert!(!called.get());
    assert_eq!(fs::read(stage.managed_path().with_file_name("manifest.json")).unwrap(), json);
    let another = "22223344556677889900aabbccddeeff";
    assert!(matches!(f.store.stage(&f.owner, another, f.scope(), 2, f.open(), MAX_BYTES, |_| Ok(())), Err(Error::Cancelled)));
    assert!(!f.owner.uploads.join(another).exists());
    assert_eq!(f.store.inventory().total.slots, 1);
}

#[tokio::test]
async fn hooked_discard_sync_failures_retain_honest_phase_and_existing_byte_charge() {
    for fail_at_directory in [false, true] {
        let mut f = Fixture::new();
        let core = core_for_catalog(&f).await;
        f.store = Store::open_with_mutations(&f.accounts, vec![f.owner.clone()], core.stage_mutations()).unwrap();
        let stage = f.stage();
        let result = stage.discard_ready_with(|_, _| Ok(true), |file, directory| {
            if directory == fail_at_directory { Err(io::Error::other("synthetic sync failure")) }
            else { file.sync_all() }
        });
        assert_eq!(result.unwrap_err(), if fail_at_directory { Error::Unknown } else { Error::Storage });
        assert!(stage.managed_path().exists());
        let recovered = f.store.recover(&f.owner, ID).unwrap();
        assert!(recovered.manifest().phase == if fail_at_directory { Phase::Discarded } else { Phase::Ready });
        let inventory = f.store.inventory();
        assert_eq!(inventory.total.slots, 1);
        assert_eq!(inventory.total.bytes, f.open().len());
        core.shutdown().await;
    }
}

#[tokio::test]
async fn store_rejects_a_real_hook_for_another_accounts_namespace_without_allocating() {
    let mut f = Fixture::new();
    let other = Fixture::new();
    let core = core_for_catalog(&f).await;
    let hook = core.stage_mutations();
    assert!(matches!(Store::open_with_mutations(&other.accounts, vec![other.owner.clone()], hook.clone()),
        Err(Error::Integrity)));
    assert!(!other.owner.uploads.exists());
    assert_eq!(other.store.inventory().total.slots, 0);
    f.store = Store::open_with_mutations(&f.accounts, vec![f.owner.clone()], hook).unwrap();
    f.stage();
    assert_eq!(f.store.inventory().total.slots, 1);
    assert_eq!(f.store.inventory().total.bytes, f.open().len());
    core.shutdown().await;
}

#[tokio::test]
async fn local_claim_and_consumed_publishers_keep_the_cores_permanent_writer_veto() {
    for consuming in [false, true] {
        let mut f = Fixture::new();
        let core = core_for_catalog(&f).await;
        f.store = Store::open_with_mutations(&f.accounts, vec![f.owner.clone()], core.stage_mutations()).unwrap();
        let stage = f.stage();
        let verified = stage.verify(MAX_BYTES, |_| Ok(())).unwrap();
        let claim = if consuming {
            let mut claim = verified.claim(intent()).unwrap();
            claim.checked_path().unwrap();
            Some(claim)
        } else {
            core.shutdown().await;
            assert!(matches!(verified.claim(intent()), Err(Error::Cancelled)));
            None
        };
        if let Some(claim) = claim {
            core.shutdown().await;
            assert!(matches!(claim.consume_for_test(), Err(Error::Cancelled)));
        }
        let recovered = f.store.recover(&f.owner, ID).unwrap();
        assert!(recovered.manifest().phase == if consuming { Phase::Claimed(intent()) } else { Phase::Ready });
        let inventory = f.store.inventory();
        assert!(inventory.complete);
        assert_eq!(inventory.total.slots, 1);
        assert_eq!(inventory.total.bytes, f.open().len());
    }
}

#[test]
fn inventory_validates_metadata_and_charges_all_phases_without_opening_payload_readers() {
    use std::io::Read;
    use std::os::fd::{AsRawFd, FromRawFd};
    for phase in 0..3 {
        let f = Fixture::new();
        let stage = f.stage();
        let expected = match phase {
            0 => Phase::Ready,
            1 => {
                let claim = stage.verify(MAX_BYTES, |_| Ok(())).unwrap().claim(intent()).unwrap();
                drop(claim);
                Phase::Claimed(intent())
            },
            _ => {
                let mut claim = stage.verify(MAX_BYTES, |_| Ok(())).unwrap().claim(intent()).unwrap();
                claim.checked_path().unwrap();
                drop(claim.consume_for_test().unwrap());
                Phase::Consumed(intent())
            },
        };
        // O_PATH has no payload open/read events. This detects the old O_RDONLY
        // inventory probe even if it never consumes bytes and closes immediately.
        // SAFETY: inotify_init1 takes only these supported flag bits, no pointers.
        let raw = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        assert!(raw >= 0);
        // SAFETY: successful inotify_init1 transferred this sole descriptor.
        let mut notifications = unsafe { File::from_raw_fd(raw) };
        let path = fd::cname(stage.managed_path().as_os_str().as_bytes()).unwrap();
        // SAFETY: descriptor and terminated synthetic fixture path are valid.
        assert!(unsafe { libc::inotify_add_watch(notifications.as_raw_fd(), path.as_ptr(),
            libc::IN_OPEN | libc::IN_ACCESS | libc::IN_CLOSE_NOWRITE) } >= 0);
        let inventory = f.store.inventory();
        assert!(inventory.complete);
        assert_eq!(inventory.total.slots, 1);
        assert_eq!(inventory.total.bytes, f.open().len());
        assert_eq!(inventory.manifests.len(), 1);
        assert!(inventory.manifests[0].phase == expected);
        let mut events = [0; 1024];
        assert_eq!(notifications.read(&mut events).unwrap_err().kind(), io::ErrorKind::WouldBlock);
        // Positive control: a genuine payload reader must be observed.
        drop(File::open(stage.managed_path()).unwrap());
        assert!(notifications.read(&mut events).unwrap() > 0);
    }
}

#[test]
fn actual_file_source_type_name_binding_and_limits() {
    let f = Fixture::new();
    let base = f.source.parent().unwrap();
    let private_file = f.private.join("secret");
    fs::write(&private_file, b"private").unwrap();
    assert!(f.open_path(&private_file, MAX_BYTES).is_err());
    symlink(&f.private, base.join("alias")).unwrap();
    assert!(f.open_path(&base.join("alias/secret"), MAX_BYTES).is_err());
    symlink(&f.source, base.join("link")).unwrap();
    assert!(f.open_path(&base.join("link"), MAX_BYTES).is_err());
    fs::hard_link(&f.source, base.join("hard")).unwrap();
    assert!(f.open_path(&f.source, MAX_BYTES).is_err());
    fs::remove_file(base.join("hard")).unwrap();
    let fifo = base.join("fifo");
    let n = fd::cname(fifo.as_os_str().as_bytes()).unwrap();
    // SAFETY: the target is a fresh name under the synthetic fixture.
    assert_eq!(unsafe { libc::mkfifo(n.as_ptr(), 0o600) }, 0);
    assert!(f.open_path(&fifo, MAX_BYTES).is_err());
    for path in [
        base.to_owned(),
        base.join("./synthetic.bin"),
        base.join("dir/../synthetic.bin"),
        PathBuf::from("file"),
    ] {
        assert!(f.open_path(&path, MAX_BYTES).is_err());
    }
    assert!(Source::open(&private_file, std::slice::from_ref(&f.private), MAX_BYTES).is_err());
    let sparse = File::create(&f.source).unwrap();
    assert!(f.open_path(&f.source, MAX_BYTES).is_err());
    sparse.set_len(MAX_BYTES).unwrap();
    assert_eq!(f.open().len(), MAX_BYTES);
    assert!(matches!(
        f.open_path(&f.source, MAX_BYTES - 1),
        Err(Error::TooLarge)
    ));
    sparse.set_len(MAX_BYTES + 1).unwrap();
    assert!(matches!(
        f.open_path(&f.source, u64::MAX),
        Err(Error::TooLarge)
    ));
    assert_eq!(sanitize(Some("../\u{202e}a/b\\c\n")), "..abc");
    assert_eq!(sanitize(Some("..")), "attachment");
    assert_eq!(sanitize(Some(&"Ж".repeat(130))).len(), 254);
}

#[test]
fn mount_boundaries_check_every_crossing_and_leaf_id() {
    let table=b"1 0 8:1 / / rw - ext4 /dev/a rw\n2 1 8:1 /private/account /alias rw - ext4 /dev/a rw\n3 1 8:2 /@home /home rw - btrfs /dev/b rw,subvolid=256,subvol=/@home\n4 1 8:2 /@home/sub /bad rw - btrfs /dev/b rw,subvolid=256,subvol=/@home\n5 1 8:1 / /duplicate rw - ext4 /dev/a rw\n6 1 0:67 / /docs rw - fuse.portal portal rw\n7 1 8:3 / /private/mount rw - ext4 /dev/c rw\n8 1 8:3 / /other rw - ext4 /dev/c rw\n";
    let m = Mounts::parse(table).unwrap();
    let mut p = vec![(1, "/private".into())];
    m.protect_descendants(&mut p);
    let d = libc::makedev;
    assert!(m.check(1, d(8, 1), None, Path::new("/"), &p).is_ok());
    assert!(m.check(3, d(8, 2), Some(1), Path::new("/home"), &p).is_ok());
    assert!(
        m.check(3, d(8, 2), Some(3), Path::new("/home/file"), &p)
            .is_ok()
    );
    assert!(
        m.check(3, d(8, 2), Some(99), Path::new("/home"), &p)
            .is_err()
    );
    assert!(
        m.check(3, d(8, 4), Some(1), Path::new("/home"), &p)
            .is_err()
    );
    for (id, dev, path) in [
        (2, d(8, 1), "/alias"),
        (4, d(8, 2), "/bad"),
        (5, d(8, 1), "/duplicate"),
        (6, d(0, 67), "/docs"),
        (8, d(8, 3), "/other"),
    ] {
        assert!(m.check(id, dev, Some(1), Path::new(path), &p).is_err());
    }
    assert!(
        m.check(2, d(8, 1), Some(1), Path::new("/selected-file"), &p)
            .is_err()
    );
    let mut too_many = String::new();
    for i in 1..=8193 {
        too_many.push_str(&format!("{i} 0 8:1 / / rw - ext4 /dev/a rw\n"));
    }
    assert!(Mounts::parse(too_many.as_bytes()).is_err());
    assert!(Mounts::parse(&vec![b'x'; 2 * 1024 * 1024 + 1]).is_err());
    assert!(Mounts::parse(b"1 0 8:1 / /bad\\377 rw - ext4 /dev/a rw\n").is_err());
    assert!(fd::parts(Path::new(&format!("/{}", "a/".repeat(65)))).is_err());
}

#[test]
fn cancellation_source_rename_and_mutation_never_publish_ready() {
    for action in 0..4 {
        let f = Fixture::new();
        fs::write(&f.source, vec![7; 128 * 1024]).unwrap();
        let open = f.open();
        let changed = Cell::new(false);
        let r = f
            .store
            .stage(&f.owner, ID, f.scope(), 2, open, MAX_BYTES, |n| {
                if n > 0 && !changed.replace(true) {
                    match action {
                        0 => return Err(Error::Cancelled),
                        1 => {
                            fs::rename(&f.source, f.source.with_extension("old")).unwrap();
                            fs::write(&f.source, b"other").unwrap();
                        }
                        2 => {
                            fs::write(&f.source, b"changed").unwrap();
                        }
                        _ => return Err(Error::TargetChanged),
                    }
                }
                Ok(())
            });
        assert!(r.is_err());
        assert!(!f.owner.uploads.join(ID).exists());
        let inv = f.store.inventory();
        assert!(inv.complete);
        assert_eq!(inv.total.slots, 0);
    }
}

#[test]
fn restart_exact_scoping_one_live_claim_and_permanent_charge() {
    let f = Fixture::new();
    let ready = f.stage();
    assert_eq!(ready.manifest().format, Format::Png);
    assert_eq!(
        fs::metadata(ready.managed_path()).unwrap().mode() & 0o777,
        0o400
    );
    assert_eq!(
        fs::metadata(&f.owner.uploads).unwrap().mode() & 0o777,
        0o700
    );
    let raw = fs::read_to_string(f.owner.uploads.join(ID).join("manifest.json")).unwrap();
    assert!(!raw.contains(f.source.parent().unwrap().to_str().unwrap()));
    assert!(raw.contains("synthetic.bin"));
    let store = f.reload();
    let inv: Inventory = store.inventory();
    assert!(inv.complete);
    assert_eq!(inv.manifests.len(), 1);
    assert_eq!(inv.total.bytes, f.open().len());
    assert_eq!(inv.total.slots, 1);
    let recovered = store.recover(&f.owner, ID).unwrap();
    assert!(recovered.manifest().scope == f.scope());
    let verified: Verified = recovered.verify(MAX_BYTES, |_| Ok(())).unwrap();
    assert!(matches!(
        ready.discard_ready(|_, _| Ok(true)),
        Err(Error::Busy)
    ));
    let mut claimed: LiveClaim = verified.claim(intent()).unwrap();
    assert!(claimed.manifest().phase == Phase::Claimed(intent()));
    assert_eq!(claimed.checked_path().unwrap(), recovered.managed_path());
    assert!(claimed.checked_path().is_err());
    let consumed = claimed.consume_for_test().unwrap();
    assert!(consumed.manifest().phase == Phase::Consumed(intent()));
    assert!(consumed.verify(MAX_BYTES, |_| Ok(())).is_err());
    assert!(consumed.discard_ready(|_, _| Ok(true)).is_err());
    assert!(recovered.verify(MAX_BYTES, |_| Ok(())).is_err());
    let inv = f.reload().inventory();
    assert!(inv.complete);
    assert_eq!(inv.total.bytes, f.open().len());
    let mut other = f.owner.clone();
    other.account_created += 1;
    let replaced = Store::open(&f.accounts, vec![other.clone()]).unwrap();
    assert!(replaced.recover(&other, ID).is_err());
    let inv = replaced.inventory();
    assert!(inv.complete);
    assert!(inv.manifests.is_empty());
    assert_eq!(inv.total.slots, 1);
    let mut colliding = f.owner.clone();
    colliding.account = "different".into();
    let conflicted = Store::open(&f.accounts, vec![f.owner.clone(), colliding]).unwrap();
    assert!(conflicted.recover(&f.owner, ID).is_err());
    let orphan = Store::open(&f.accounts, vec![]).unwrap().inventory();
    assert!(orphan.complete);
    assert!(orphan.manifests.is_empty());
    assert_eq!(orphan.total.bytes, f.open().len());
}

#[test]
fn exact_stage_bytes_and_sha_required_for_claim_and_discard() {
    for change in 0..4 {
        let f = Fixture::new();
        let stage = f.stage();
        let bytes = stage.managed_path();
        match change {
            0 => {
                fs::rename(&bytes, bytes.with_extension("old")).unwrap();
                fs::write(&bytes, b"replacement").unwrap();
            }
            1 => {
                fs::hard_link(&bytes, bytes.with_extension("alias")).unwrap();
            }
            2 => {
                fs::set_permissions(&bytes, Permissions::from_mode(0o600)).unwrap();
            }
            _ => {
                fs::rename(bytes.parent().unwrap(), f.owner.uploads.join("moved")).unwrap();
                fs::create_dir(f.owner.uploads.join(ID)).unwrap();
            }
        }
        assert!(stage.verify(MAX_BYTES, |_| Ok(())).is_err());
        assert!(stage.discard_ready(|_, _| Ok(true)).is_err());
    }
    let f = Fixture::new();
    let stage = f.stage();
    let manifest_path = stage.managed_path().with_file_name("manifest.json");
    let mut m: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    m["sha256"] = serde_json::json!("0".repeat(64));
    fs::write(manifest_path, serde_json::to_vec(&m).unwrap()).unwrap();
    let recovered = f.reload().recover(&f.owner, ID).unwrap();
    assert!(matches!(
        recovered.verify(MAX_BYTES, |_| Ok(())),
        Err(Error::StageChanged)
    ));
    assert!(recovered.discard_ready(|_, _| Ok(true)).is_err());
    assert!(recovered.managed_path().exists());
}

#[test]
fn discard_requires_proof_and_preserves_claimed_even_after_restart() {
    let f = Fixture::new();
    let ready = f.stage();
    assert!(ready.discard_ready(|_, _| Ok(false)).is_err());
    let recovered = f.reload().recover(&f.owner, ID).unwrap();
    recovered
        .discard_ready(|m, p| Ok(m.stage_id == ID && p == ready.managed_path()))
        .unwrap();
    assert!(!f.owner.uploads.join(ID).exists());
    assert!(f.reload().recover(&f.owner, ID).is_err());
    assert_eq!(f.store.inventory().total.slots, 0);
    let stage = f.stage();
    let claim = stage
        .verify(MAX_BYTES, |_| Ok(()))
        .unwrap()
        .claim(intent())
        .unwrap();
    drop(claim);
    let after = f.reload().recover(&f.owner, ID).unwrap();
    assert!(after.manifest().phase == Phase::Claimed(intent()));
    assert!(after.verify(MAX_BYTES, |_| Ok(())).is_err());
    assert!(after.discard_ready(|_, _| Ok(true)).is_err());
    assert_eq!(f.reload().inventory().total.bytes, f.open().len());
}

#[test]
fn manifest_sync_failure_before_and_after_rename() {
    for post in [false, true] {
        let f = Fixture::new();
        let stage = f.stage();
        let (mut m, old) = store::read_manifest(&stage.dir).unwrap();
        m.phase = Phase::Claimed(intent());
        let result = stage.publish_with(
            Some(&old),
            &m,
            || Ok(()),
            |fd, is_directory| {
                if is_directory == post {
                    Err(io::ErrorKind::StorageFull.into())
                } else {
                    fd.sync_all()
                }
            },
        );
        assert_eq!(
            result,
            Err(if post { Error::Unknown } else { Error::Storage })
        );
        let recovered = f.reload().recover(&f.owner, ID).unwrap();
        if post {
            assert!(recovered.manifest().phase == Phase::Claimed(intent()));
            assert!(recovered.verify(MAX_BYTES, |_| Ok(())).is_err());
        } else {
            assert!(recovered.manifest().phase == Phase::Ready);
        }
    }
}
#[test]
fn strict_manifest_decode_and_bounded_inventory_do_not_modify_anomalies() {
    let f = Fixture::new();
    let stage = f.stage();
    let path = stage.managed_path().with_file_name("manifest.json");
    let original = fs::read_to_string(&path).unwrap();
    for bad in [
        original.replacen('{', "{\"version\":1,", 1),
        original.replacen('{', "{\"path\":\"unexpected\",", 1),
        original.replace("\"ready\"", "\"unrecognised\""),
        original.replace("\"topic\":\"topic\"", "\"bad\":null"),
        original.replace(&stage.manifest().client_id, "bad"),
        " ".repeat(65537),
    ] {
        fs::write(&path, &bad).unwrap();
        assert!(f.reload().recover(&f.owner, ID).is_err());
        let inv = f.store.inventory();
        assert!(!inv.complete);
        assert!(inv.manifests.is_empty());
        assert_eq!(inv.total.bytes, f.open().len());
        assert_eq!(fs::read(&path).unwrap(), bad.as_bytes());
    }
    fs::write(&path, original).unwrap();
    // A link to an unrelated subtree cannot make the scan walk it.
    symlink(&f.private, f.owner.uploads.join("alias")).unwrap();
    assert!(!f.store.inventory().complete);
    for i in 0..257 {
        File::create(f.owner.uploads.join(format!("other-{i}"))).unwrap();
    }
    assert!(!f.store.inventory().complete);
    assert!(stage.managed_path().exists());
}

#[test]
fn verified_btrfs_sibling_roots_and_private_subvolume_aliases() {
    let records=b"1 0 0:31 /@ / rw - btrfs /dev/d rw,subvolid=256,subvol=/@\n2 1 0:32 /@home /home rw - btrfs /dev/d rw,subvolid=257,subvol=/@home\n3 1 0:33 /@/var/private/nested /alias rw - btrfs /dev/d rw,subvolid=258,subvol=/@/var/private/nested\n";
    let m = Mounts::parse(records).unwrap();
    let private = vec![(1, "/var/private".into())];
    assert!(
        m.check(
            2,
            libc::makedev(0, 32),
            Some(1),
            Path::new("/home"),
            &private
        )
        .is_ok()
    );
    assert!(
        m.check(
            2,
            libc::makedev(0, 32),
            Some(2),
            Path::new("/home/file"),
            &private
        )
        .is_ok()
    );
    assert!(
        m.check(
            3,
            libc::makedev(0, 33),
            Some(1),
            Path::new("/alias"),
            &private
        )
        .is_err()
    );
}

#[test]
fn corrupt_ready_is_retained_and_damaged_claim_still_supplies_evidence() {
    let f = Fixture::new();
    let stage = f.stage();
    let extra = stage.managed_path().with_file_name("extra");
    File::create(&extra).unwrap();
    assert!(stage.discard_ready(|_, _| Ok(true)).is_err());
    assert!(stage.managed_path().exists());
    fs::remove_file(extra).unwrap();
    let claim = stage
        .verify(MAX_BYTES, |_| Ok(()))
        .unwrap()
        .claim(intent())
        .unwrap();
    drop(claim);
    fs::remove_file(stage.managed_path()).unwrap();
    let inventory = f.reload().inventory();
    assert!(!inventory.complete);
    assert_eq!(inventory.manifests.len(), 1);
    assert!(inventory.manifests[0].phase == Phase::Claimed(intent()));
}

#[test]
fn literal_caption_limits_and_no_clobber_stage_id() {
    let f = Fixture::new();
    let stage = f.stage();
    assert!(
        f.store
            .stage(&f.owner, ID, f.scope(), 3, f.open(), MAX_BYTES, |_| Ok(()))
            .is_err()
    );
    let manifest = stage.manifest();
    assert_eq!(manifest.created, 2);
    let too_long = Intent {
        caption: "a".repeat(1025),
        ..intent()
    };
    assert!(
        stage
            .verify(MAX_BYTES, |_| Ok(()))
            .unwrap()
            .claim(too_long)
            .is_err()
    );
    let valid = Intent {
        caption: "🧪".repeat(1024),
        ..intent()
    };
    let claim = stage
        .verify(MAX_BYTES, |_| Ok(()))
        .unwrap()
        .claim(valid.clone())
        .unwrap();
    assert!(claim.manifest().phase == Phase::Claimed(valid));
}
