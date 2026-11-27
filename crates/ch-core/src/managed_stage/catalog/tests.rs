use super::*;
use crate::managed_stage::manifest::{Fingerprint, Format, Intent, Manifest, Phase, Scope};
use sha2::{Digest, Sha256};
use std::{fs, os::unix::fs::{MetadataExt, PermissionsExt}, path::{Path, PathBuf}};

const FIRST: &str = "11111111111111111111111111111111";
const SECOND: &str = "22222222222222222222222222222222";
const CLIENT1: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CLIENT2: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

struct Fixture { _temp: tempfile::TempDir, data: PathBuf, catalog: Arc<PrivateByteCatalog> }
impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("data");
        fs::create_dir(&data).unwrap();
        fs::set_permissions(&data, fs::Permissions::from_mode(0o700)).unwrap();
        let catalog = PrivateByteCatalog::for_core(&HostConfig::new(&data));
        Self { _temp: temp, data, catalog }
    }
    fn stage(&self, account: &str, stage: &str, client: &str, phase: Phase) -> PathBuf {
        stage_at(&self.data, account, stage, client, phase)
    }
}
fn claimed() -> Phase { Phase::Claimed(Intent { caption: String::new(), as_file: false, spoiler: false }) }
fn consumed() -> Phase { Phase::Consumed(Intent { caption: String::new(), as_file: false, spoiler: false }) }

fn stage_at(data: &Path, account: &str, stage: &str, client: &str, phase: Phase) -> PathBuf {
    let uploads = data.join("accounts").join(account).join("uploads");
    let directory = uploads.join(stage);
    fs::create_dir_all(&directory).unwrap();
    for path in [&uploads, &directory] { fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap(); }
    let bytes = directory.join("bytes");
    fs::write(&bytes, b"private").unwrap();
    fs::set_permissions(&bytes, fs::Permissions::from_mode(0o400)).unwrap();
    let manifest = Manifest { version: 1, stage_id: stage.into(), client_id: client.into(),
        scope: Scope { account: account.into(), plugin: "fixture".into(), account_created: 1,
            chat: "chat".into(), topic: None }, created: 1, name: "synthetic.bin".into(), size: 7,
        format: Format::Other, fingerprint: Fingerprint::from(fs::metadata(&bytes).unwrap()),
        sha256: format!("{:x}", Sha256::digest(b"private")), phase };
    let metadata = directory.join("manifest.json");
    fs::write(&metadata, serde_json::to_vec(&manifest).unwrap()).unwrap();
    fs::set_permissions(&metadata, fs::Permissions::from_mode(0o600)).unwrap();
    bytes
}
fn known(catalog: &PrivateByteCatalog, path: &Path) -> bool {
    let metadata = fs::metadata(path).unwrap();
    catalog.lookup_inode(metadata.dev(), metadata.ino()).matched() == InodeMatch::KnownPrivate
}

#[test]
fn outside_observation_is_exact_issuer_bound_and_revoked_by_actual_supported_writer() {
    let f=Fixture::new();
    let private=f.stage("owner",FIRST,CLIENT1,Phase::Ready);
    let ordinary=f._temp.path().join("ordinary");
    fs::write(&ordinary,b"external").unwrap();
    let paths=vec![ordinary.to_str().unwrap().to_owned()];
    let observed=f.catalog.external_inputs(paths.clone()).unwrap();
    assert_eq!(f.catalog.external_frame(&observed,&paths,||19).unwrap(),19);
    assert!(f.catalog.external_frame(&observed,&[private.to_str().unwrap().into()],||()).is_err());
    let foreign=PrivateByteCatalog::for_core(&HostConfig::new(&f.data));
    foreign.refresh().unwrap();
    assert!(foreign.external_frame(&observed,&paths,||()).is_err());
    let mutation=f.catalog.mutations().begin().unwrap();
    assert!(f.catalog.external_frame(&observed,&paths,||()).is_err());
    drop(mutation);
    f.catalog.refresh().unwrap();
    assert!(f.catalog.external_frame(&observed,&paths,||()).is_err(),"a new complete epoch cannot renew old evidence");
    let fresh=f.catalog.external_inputs(paths.clone()).unwrap();
    assert!(f.catalog.external_frame(&fresh,&paths,||()).is_ok());
    f.catalog.close();
    assert!(f.catalog.external_frame(&fresh,&paths,||()).is_err());
}

#[test]
fn outside_classifier_denies_private_names_aliases_and_nonregular_or_ambiguous_targets() {
    let f=Fixture::new();
    let private=f.stage("owner",FIRST,CLIENT1,Phase::Ready);
    let symlink=f._temp.path().join("private-link");
    std::os::unix::fs::symlink(&private,&symlink).unwrap();
    for path in [&private,&private.with_file_name("manifest.json"),&symlink,&f.data] {
        assert!(f.catalog.external_inputs(vec![path.to_str().unwrap().into()]).is_err());
    }
    for path in ["relative","/missing/synthetic","/tmp/../synthetic"] {
        assert!(f.catalog.external_inputs(vec![path.into()]).is_err());
    }
    // An alias outside the namespace invalidates private manifest link evidence;
    // even an unrelated input then remains denied by incomplete catalog state.
    let hardlink=f._temp.path().join("private-hardlink");
    fs::hard_link(&private,&hardlink).unwrap();
    assert!(f.catalog.refresh().is_err());
    let ordinary=f._temp.path().join("ordinary");fs::write(&ordinary,b"outside").unwrap();
    assert!(f.catalog.external_inputs(vec![hardlink.to_str().unwrap().into()]).is_err());
    assert!(f.catalog.external_inputs(vec![ordinary.to_str().unwrap().into()]).is_err());
}

#[test]
fn unknown_default_and_real_scan_cover_ready_and_regular_metadata_without_granting_read() {
    let f = Fixture::new();
    assert!(!f.catalog.view().complete());
    let bytes = f.stage("owner", FIRST, CLIENT1, Phase::Ready);
    assert!(f.catalog.refresh().unwrap().complete());
    assert!(known(&f.catalog, &bytes));
    assert!(known(&f.catalog, &bytes.with_file_name("manifest.json")));
    assert_eq!(f.catalog.view().inode_count(), 2);
    assert!(f.catalog.legacy_candidates().iter().any(|c|c.kind()==StageKind::Ready),
        "Ready metadata never proves an older reader has closed");
    let ordinary = f._temp.path().join("ordinary");
    fs::write(&ordinary, b"external").unwrap();
    let metadata = fs::metadata(ordinary).unwrap();
    let lookup = f.catalog.lookup_inode(metadata.dev(), metadata.ino());
    assert!(lookup.matched() == InodeMatch::NotListed);
    assert!(lookup.complete()); // NotListed still contains no safe-to-read permit.
    assert_eq!(lookup.epoch(), f.catalog.view().epoch());
}

#[test]
fn claimed_consumed_and_unknown_artifacts_are_negative_owner_close_candidates() {
    let f = Fixture::new();
    f.stage("owner", FIRST, CLIENT1, claimed());
    f.stage("other", SECOND, CLIENT2, consumed());
    f.catalog.refresh().unwrap();
    let candidates = f.catalog.legacy_candidates();
    assert_eq!(candidates.len(), 2);
    assert!(candidates.iter().any(|c| c.kind() == StageKind::Claimed && c.account_folder() == b"owner"
        && c.stage_folder() == FIRST.as_bytes() && c.client_hint() == Some(CLIENT1)
        && c.scope_hint().is_some_and(|s| s.account == "owner")));
    assert!(candidates.iter().any(|c| c.kind() == StageKind::Consumed));
    let mutation = f.catalog.begin_mutation().unwrap();
    fs::write(f.data.join("accounts/owner/uploads/orphan-bytes"), b"orphan").unwrap();
    drop(mutation);
    assert!(f.catalog.refresh().is_err());
    assert!(known(&f.catalog, &f.data.join("accounts/owner/uploads/orphan-bytes")));
    assert!(f.catalog.legacy_candidates().iter().any(|c| c.kind() == StageKind::Unknown));
}

#[test]
fn external_hardlink_is_known_private_even_when_integrity_and_completeness_fail() {
    let f = Fixture::new();
    let bytes = f.stage("owner", FIRST, CLIENT1, Phase::Ready);
    let alias = f._temp.path().join("outside-hardlink");
    fs::hard_link(&bytes, &alias).unwrap();
    assert!(f.catalog.refresh().is_err());
    assert!(!f.catalog.view().complete());
    assert!(known(&f.catalog, &alias));
}

#[test]
fn errors_and_absence_never_prune_previous_inodes_or_unproved_candidates() {
    let f = Fixture::new();
    let bytes = f.stage("owner", FIRST, CLIENT1, consumed());
    f.catalog.refresh().unwrap();
    let before = fs::metadata(&bytes).unwrap();
    let mutation = f.catalog.begin_mutation().unwrap();
    fs::write(bytes.with_file_name("manifest.json"), b"malformed").unwrap();
    drop(mutation);
    assert!(f.catalog.refresh().is_err());
    assert!(f.catalog.lookup_inode(before.dev(), before.ino()).matched() == InodeMatch::KnownPrivate);
    assert!(!f.catalog.legacy_candidates().is_empty());
    let mutation = f.catalog.begin_mutation().unwrap();
    fs::remove_dir_all(bytes.parent().unwrap()).unwrap(); // Synthetic fault, not cleanup authority.
    drop(mutation);
    f.catalog.refresh().unwrap();
    assert!(f.catalog.lookup_inode(before.dev(), before.ino()).matched() == InodeMatch::KnownPrivate);
    assert_eq!(f.catalog.legacy_candidates().len(), 1);
}

#[test]
fn duplicate_ids_and_rebound_namespace_fail_closed() {
    let f = Fixture::new();
    f.stage("owner", FIRST, CLIENT1, Phase::Ready);
    f.stage("other", FIRST, CLIENT2, Phase::Ready);
    assert!(f.catalog.refresh().is_err());
    assert_eq!(f.catalog.legacy_candidates().len(), 2);
    let g = Fixture::new();
    let bytes = g.stage("owner", FIRST, CLIENT1, consumed());
    g.catalog.refresh().unwrap();
    let inode = fs::metadata(&bytes).unwrap();
    fs::rename(&g.data, g._temp.path().join("previous-data")).unwrap();
    fs::create_dir(&g.data).unwrap();
    fs::set_permissions(&g.data, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(g.catalog.refresh().is_err());
    assert!(g.catalog.lookup_inode(inode.dev(), inode.ino()).matched() == InodeMatch::KnownPrivate);
    assert!(!g.catalog.legacy_candidates().is_empty());
}

#[test]
fn mutation_between_scan_and_publication_cannot_mark_complete() {
    let f = Fixture::new();
    let old = f.stage("owner", FIRST, CLIENT1, consumed());
    f.catalog.refresh().unwrap();
    let epoch = f.catalog.view().epoch();
    let result = f.catalog.refresh_with(|| {}, || {
        let mutation = f.catalog.begin_mutation().unwrap();
        f.stage("owner", SECOND, CLIENT2, Phase::Ready);
        drop(mutation);
    });
    assert!(matches!(result, Err(Error::Busy)));
    assert!(!f.catalog.view().complete());
    assert!(f.catalog.view().epoch() > epoch);
    assert!(known(&f.catalog, &old));
    f.catalog.refresh().unwrap();
    assert!(known(&f.catalog, &f.data.join("accounts/owner/uploads").join(SECOND).join("bytes")));
}

#[test]
fn racing_empty_copy_directory_never_becomes_sticky_unproved_legacy_evidence() {
    let f = Fixture::new();
    f.stage("owner", FIRST, CLIENT1, consumed());
    f.catalog.refresh().unwrap();
    let mutation = std::cell::RefCell::new(None);
    let result = f.catalog.refresh_with(|| {
        *mutation.borrow_mut() = Some(f.catalog.begin_mutation().unwrap());
        let directory = f.data.join("accounts/owner/uploads").join(SECOND);
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
    }, || {
        f.stage("owner", SECOND, CLIENT2, Phase::Ready);
        drop(mutation.borrow_mut().take());
    });
    assert!(matches!(result, Err(Error::Busy)));
    // Existing stable Consumed evidence survives; the racing empty directory
    // has no stable provenance and must not become a permanent close barrier.
    assert_eq!(f.catalog.legacy_candidates().len(), 1);
    f.catalog.refresh().unwrap();
    let candidates = f.catalog.legacy_candidates();
    assert_eq!(candidates.len(), 2);
    assert!(candidates.iter().any(|c|c.kind()==StageKind::Consumed));
    assert!(candidates.iter().any(|c|c.kind()==StageKind::Ready),
        "the later stable Ready is independently unproved, unlike the transient empty directory");
    assert!(known(&f.catalog, &f.data.join("accounts/owner/uploads").join(SECOND).join("bytes")));
}

#[test]
fn bounded_entry_scan_and_epoch_or_writer_overflow_never_enable_completeness() {
    let f = Fixture::new();
    let bytes = f.stage("owner", FIRST, CLIENT1, Phase::Ready);
    f.catalog.refresh().unwrap();
    let mutation = f.catalog.begin_mutation().unwrap();
    let uploads = bytes.parent().unwrap().parent().unwrap();
    for id in 0..257 { fs::write(uploads.join(format!("orphan-{id}")), b"x").unwrap(); }
    drop(mutation);
    assert!(f.catalog.refresh().is_err());
    assert!(known(&f.catalog, &bytes));
    assert!(!f.catalog.view().complete());
    f.catalog.lock().epoch = u64::MAX;
    assert!(f.catalog.refresh().is_err());
    assert!(f.catalog.begin_mutation().is_err());
    let g = Fixture::new();
    let guards: Vec<_> = (0..MAX_WRITERS).map(|_| g.catalog.begin_mutation().unwrap()).collect();
    assert!(g.catalog.begin_mutation().is_err());
    assert_eq!(g.catalog.view().writers(), MAX_WRITERS);
    drop(guards);
    assert_eq!(g.catalog.view().writers(), 0);
    assert!(!g.catalog.view().complete());
}

#[test]
fn observe_only_poison_preserves_file_work_but_close_permanently_vetoes_start() {
    let f = Fixture::new();
    let hook = f.catalog.mutations();
    let guards: Vec<_> = (0..MAX_WRITERS).map(|_| hook.begin().unwrap()).collect();
    let overflow = hook.begin().unwrap(); // Not a file error while enforcement is inactive.
    assert_eq!(f.catalog.view().writers(), MAX_WRITERS);
    drop((guards, overflow));
    assert_eq!(f.catalog.view().writers(), 0);
    assert!(f.catalog.refresh().is_err()); // Poison cannot be healed by absence/scan.
    let already_started = hook.begin().unwrap();
    f.catalog.close();
    assert!(matches!(hook.begin(), Err(Error::Cancelled)));
    drop(already_started);
    assert!(matches!(hook.begin(), Err(Error::Cancelled)));
    assert!(f.catalog.refresh().is_err());
    assert!(!f.catalog.view().complete());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_caller_does_not_settle_the_actual_owned_mutation() {
    let f = Fixture::new();
    f.stage("owner", FIRST, CLIENT1, Phase::Ready);
    f.catalog.refresh().unwrap();
    let mutation = f.catalog.begin_mutation().unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (settled_tx, settled_rx) = tokio::sync::oneshot::channel();
    let data = f.data.clone();
    let caller = tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            let _ = entered_tx.send(());
            release_rx.recv().unwrap();
            stage_at(&data, "owner", SECOND, CLIENT2, Phase::Ready);
            drop(mutation); // Actual worker settlement, not waiter cancellation.
            let _ = settled_tx.send(());
        }).await.unwrap();
    });
    entered_rx.await.unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert_eq!(f.catalog.view().writers(), 1);
    assert!(matches!(f.catalog.refresh(), Err(Error::Busy)));
    release_tx.send(()).unwrap();
    settled_rx.await.unwrap();
    assert_eq!(f.catalog.view().writers(), 0);
    assert!(!f.catalog.view().complete(), "token drop cannot publish complete");
    f.catalog.refresh().unwrap();
}
