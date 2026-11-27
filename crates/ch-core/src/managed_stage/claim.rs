use super::{Error, Result, VerifiedStageBinding, fd, manifest::{Intent, Manifest, Phase}, verify::{check_layout, read_manifest_file}};
use crate::outbox::{Outbox, OutboxError, OutboxRequest};
use crate::runtime::AccountStorageBinding;
use ch_protocol::methods::{FileKind, MessagesSendFileParams};
use std::{fs::File, io::{self, Write}, path::PathBuf};

mod preparation;
pub(crate) use preparation::PreparedStageClaim;

const MANIFEST_CAP: usize = 64 * 1024;

/// One actual durable Ready -> Claimed transition. Recovered JSON cannot mint this.
/// This bounded local handle is not upload settlement or source-release authority.
///
/// ```compile_fail
/// use ch_core::managed_stage::LiveStageClaim;
/// fn duplicate(v: &LiveStageClaim) -> LiveStageClaim { v.clone() }
/// ```
pub struct LiveStageClaim {
    proof: VerifiedStageBinding,
    used: bool,
}

impl VerifiedStageBinding {
    pub fn claim(self, intent: Intent) -> Result<LiveStageClaim> {
        self.claim_with(intent, |file, _directory| file.sync_all())
    }

    fn claim_with(mut self, intent: Intent, sync: impl Fn(&File, bool) -> io::Result<()>) -> Result<LiveStageClaim> {
        intent.check()?;
        self.recheck_ready()?;
        let mut next = self.manifest.clone();
        next.phase = Phase::Claimed(intent);
        self.publish_phase(next, sync)?;
        Ok(LiveStageClaim { proof: self, used: false })
    }

    fn publish_phase(&mut self, next: Manifest, sync: impl Fn(&File, bool) -> io::Result<()>) -> Result<()> {
        next.check()?;
        check_layout(&self.dir)?;
        let (current, previous) = read_manifest_file(&self.dir)?;
        if current != self.manifest { return Err(Error::StageChanged); }
        let claiming = match (&current.phase, &next.phase) {
            (Phase::Ready, Phase::Claimed(_)) => true,
            (Phase::Claimed(old), Phase::Consumed(new)) if old == new => false,
            _ => return Err(Error::Integrity),
        };
        let bytes = serde_json::to_vec(&next).map_err(|_| Error::Integrity)?;
        if bytes.len() > MANIFEST_CAP { return Err(Error::Integrity); }
        let _mutation = self.owner.stage_mutations()?.begin()?;
        let mut temp = fd::temp(&self.dir)?;
        temp.write_all(&bytes)?;
        temp.flush()?;
        sync(&temp, false)?;
        self.dir.check()?;
        fd::link(&self.dir, &temp, c".next")?;
        let replaced = (|| {
            if claiming { self.check_bytes()?; }
            self.dir.bind(c".next", &temp)?;
            self.dir.bind(c"manifest.json", &previous)?;
            fd::replace(&self.dir, c".next", c"manifest.json")
        })();
        if replaced.is_err() { fd::unlink(&self.dir, c".next", fd::id(&temp)?, false)?; }
        replaced?;
        // Replacement may already be durable. No subsequent failure grants a live claim.
        sync(&self.dir.file, true).map_err(|_| Error::Unknown)?;
        self.dir.bind(c"manifest.json", &temp).map_err(|_| Error::Unknown)?;
        self.manifest = next;
        Ok(())
    }
}

impl LiveStageClaim {
    pub fn manifest(&self) -> &Manifest { &self.proof.manifest }

    // The original Accounts-issued scope; never reconstructed from row/token pieces.
    pub(crate) fn owner_binding(&self) -> &AccountStorageBinding { &self.proof.owner }

    /// Initial admission may take this path exactly once, including failed checks.
    pub fn checked_path(&mut self) -> Result<PathBuf> {
        self.burn_unused()?;
        self.validate_claimed()?;
        Ok(self.proof.dir.path.join("bytes"))
    }

    fn burn_unused(&mut self) -> Result<()> {
        let used = self.used;
        self.used = true;
        if used { Err(Error::Integrity) } else { Ok(()) }
    }

    fn validate_claimed(&self) -> Result<()> {
        self.proof.check_bytes()?;
        check_layout(&self.proof.dir)?;
        if read_manifest_file(&self.proof.dir)?.0 != self.proof.manifest {
            return Err(Error::StageChanged);
        }
        Ok(())
    }

    /// Bookkeep local admission only after Core independently matches its exact retained row.
    /// No current network run, capability, posting rights or success response is required.
    /// The held storage lease bounds this local work; there is no network await here.
    pub fn consume(self, outbox: &Outbox) -> Result<Manifest> {
        self.consume_with(outbox, |file, _directory| file.sync_all())
    }

    fn consume_with(mut self, outbox: &Outbox, sync: impl Fn(&File, bool) -> io::Result<()>) -> Result<Manifest> {
        if !self.used { return Err(Error::Integrity); }
        let request = self.canonical_request()?;
        let admitted = outbox.get_matching_admission(self.owner_binding().owner(),
            &self.proof.manifest.client_id, &request).map_err(|error| match error {
                OutboxError::AdmissionConflict => Error::Integrity,
                _ => Error::Storage,
            })?;
        if admitted.is_none() { return Err(Error::Integrity); }
        let mut next = self.proof.manifest.clone();
        let Phase::Claimed(intent) = &next.phase else { return Err(Error::Integrity); };
        next.phase = Phase::Consumed(intent.clone());
        self.proof.publish_phase(next, sync)?;
        // Returning metadata drops every owned byte handle, flock and storage lease.
        Ok(self.proof.manifest.clone())
    }

    pub(crate) fn canonical_request(&self) -> Result<OutboxRequest> {
        canonical_request(&self.proof.manifest, &self.proof.dir.path.join("bytes"))
    }
}

pub(super) fn canonical_request(m: &Manifest, path: &std::path::Path) -> Result<OutboxRequest> {
    let Phase::Claimed(intent) = &m.phase else { return Err(Error::Integrity); };
    let photo = !intent.as_file && matches!(m.format, super::manifest::Format::Jpeg | super::manifest::Format::Png);
    Ok(OutboxRequest::SendFile(MessagesSendFileParams {
        account_id: m.scope.account.clone(), chat_id: m.scope.chat.clone(),
        topic_id: m.scope.topic.clone(), path: path.to_str().ok_or(Error::Integrity)?.into(),
        filename: Some(m.name.clone()), kind: Some(if photo { FileKind::Photo } else { FileKind::Document }),
        caption: (!intent.caption.is_empty()).then(|| intent.caption.clone()),
        as_file: intent.as_file.then_some(true), spoiler: intent.spoiler.then_some(true),
        ..Default::default()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{host::MemorySecretStore, outbox::OutboxPolicy, runtime::{Core, CoreConfig, NewAccount}};
    use super::super::manifest::{Fingerprint, Format, Scope};
    use sha2::{Digest, Sha256};
    use std::{os::unix::fs::PermissionsExt, sync::Arc};

    const STAGE: &str = "11111111111111111111111111111111";
    const CLIENT: &str = "22222222222222222222222222222222";

    struct Fixture { _temp: tempfile::TempDir, core: Core, outbox: Outbox }
    impl Fixture {
        async fn new() -> Self { Self::with_format(Format::Other).await }
        async fn with_format(format: Format) -> Self {
            let content: &[u8] = match format {
                Format::Jpeg => b"\xff\xd8\xfffixture",
                Format::Png => b"\x89PNG\r\n\x1a\nfixture",
                Format::Other => b"synthetic bytes",
            };
            let temp = tempfile::tempdir().unwrap();
            let plugin = temp.path().join("plugins/fixture");
            std::fs::create_dir_all(&plugin).unwrap();
            std::fs::write(plugin.join("plugin.json"), serde_json::json!({
                "id":"fixture","name":"Fixture","version":"0.0.1","api_version":"1.2.0",
                "entry":["python3","-B","-c","pass"],"brand_color":"#123456",
                "multi_account":true,"auth_methods":["none"]
            }).to_string()).unwrap();
            let mut config = CoreConfig::new(temp.path(), Arc::new(MemorySecretStore::new()));
            config.autostart = false;
            let core = Core::open(config).await.unwrap();
            let owner = core.accounts().add(NewAccount::new("fixture").id("owner").no_start()).await.unwrap();
            let db = core.open_db().unwrap();
            db.conn().execute("UPDATE account SET enabled=0 WHERE account_id='owner'", []).unwrap();
            let conversation = ch_db::insert_conversation(db.conn(), &ch_db::ConversationRow {
                account_id:"owner".into(), chat_id:"chat".into(),kind:"group".into(),can_post:Some(true),
                ..Default::default()
            }).unwrap();
            ch_db::insert_topic(db.conn(), &ch_db::TopicRow {
                conversation_id:conversation, topic_id:"topic".into(),title:"Topic".into(),..Default::default()
            }).unwrap();
            let uploads = core.host().config().upload_dir("owner");
            let stage = uploads.join(STAGE);
            std::fs::create_dir_all(&stage).unwrap();
            for directory in [&uploads, &stage] {
                std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700)).unwrap();
            }
            let bytes = stage.join("bytes");
            std::fs::write(&bytes, content).unwrap();
            std::fs::set_permissions(&bytes, std::fs::Permissions::from_mode(0o400)).unwrap();
            let manifest = Manifest {
                version:1,stage_id:STAGE.into(),client_id:CLIENT.into(),
                scope:Scope { account:"owner".into(),plugin:"fixture".into(),account_created:owner.created_at,
                    chat:"chat".into(),topic:Some("topic".into()) },
                created:1,name:"synthetic.bin".into(),size:content.len() as u64,format,
                fingerprint:Fingerprint::from(std::fs::metadata(&bytes).unwrap()),
                sha256:format!("{:x}",Sha256::digest(content)),phase:Phase::Ready,
            };
            let path = stage.join("manifest.json");
            std::fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
            let outbox = Outbox::for_core(&core, OutboxPolicy::default());
            Self { _temp:temp, core, outbox }
        }
        async fn proof(&self) -> VerifiedStageBinding {
            let binding = self.core.accounts().storage_binding_for("owner").await.unwrap();
            VerifiedStageBinding::verify_ready(&binding, STAGE, super::super::MAX_BYTES, |_| Ok(())).unwrap()
        }
        fn disk(&self) -> Manifest {
            serde_json::from_slice(&std::fs::read(self.core.host().config().upload_dir("owner")
                .join(STAGE).join("manifest.json")).unwrap()).unwrap()
        }
        fn admit(&self, claim: &LiveStageClaim) {
            self.outbox.enqueue_with_id(claim.owner_binding().owner(), CLIENT, claim.canonical_request().unwrap(), None).unwrap();
        }
        async fn close(self) { self.outbox.shutdown().await; self.core.shutdown().await; }
    }
    fn intent() -> Intent { Intent { caption:" literal ".into(),as_file:true,spoiler:true } }
    fn fail_directory(file:&File,directory:bool)->io::Result<()> {
        if directory { Err(io::Error::other("synthetic sync failure")) } else { file.sync_all() }
    }

    #[tokio::test]
    async fn actual_phase_publishers_invalidate_before_temp_and_settle_after_sync_failures() {
        for consuming in [false, true] {
            for fail_at_directory in [false, true] {
                let f = Fixture::new().await;
                let catalog = f.core.managed_sources().catalog();
                let mut proof = Some(f.proof().await);
                let claim = if consuming {
                    let mut claim = proof.take().unwrap().claim(intent()).unwrap();
                    claim.checked_path().unwrap();
                    f.admit(&claim);
                    Some(claim)
                } else { None };
                catalog.refresh().unwrap();
                let initial = catalog.view().epoch();
                let sync = |file: &File, directory| {
                    let state = catalog.view();
                    assert!(!state.complete());
                    assert_eq!(state.writers(), 1);
                    assert!(state.epoch() > initial);
                    if directory == fail_at_directory { Err(io::Error::other("synthetic sync failure")) }
                    else { file.sync_all() }
                };
                let failed = if let Some(claim) = claim {
                    claim.consume_with(&f.outbox, sync).map(|_| ())
                } else {
                    proof.unwrap().claim_with(intent(), sync).map(|_| ())
                };
                assert_eq!(failed.unwrap_err(), if fail_at_directory { Error::Unknown } else { Error::Storage });
                assert_eq!(catalog.view().writers(), 0);
                assert!(!catalog.view().complete());
                let expected = match (consuming, fail_at_directory) {
                    (false, false) => Phase::Ready,
                    (false, true) | (true, false) => Phase::Claimed(intent()),
                    (true, true) => Phase::Consumed(intent()),
                };
                assert!(f.disk().phase == expected);
                f.close().await;
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn caller_loss_keeps_actual_phase_writer_token_until_worker_settles() {
        let f = Fixture::new().await;
        let catalog = f.core.managed_sources().catalog();
        catalog.refresh().unwrap();
        let proof = f.proof().await;
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (settled_tx, settled_rx) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn(async move {
            tokio::task::spawn_blocking(move || {
                let gate = std::sync::Mutex::new(Some((entered_tx, release_rx)));
                let result = proof.claim_with(intent(), |file, directory| {
                    if !directory {
                        let (entered, release) = gate.lock().unwrap().take().unwrap();
                        let _ = entered.send(());
                        release.recv().unwrap();
                    }
                    file.sync_all()
                });
                // Drop actual proof/storage/byte handles before announcing settlement.
                let phase = result.map(|claim| claim.manifest().phase.clone());
                let _ = settled_tx.send(phase);
            }).await.unwrap();
        });
        entered_rx.await.unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert_eq!(catalog.view().writers(), 1);
        assert!(matches!(catalog.refresh(), Err(Error::Busy)));
        release_tx.send(()).unwrap();
        assert!(settled_rx.await.unwrap().unwrap() == Phase::Claimed(intent()));
        assert_eq!(catalog.view().writers(), 0);
        assert!(!catalog.view().complete());
        f.close().await;
    }

    #[tokio::test]
    async fn retained_and_fresh_bindings_keep_core_hook_but_standalone_accounts_cannot_mint_it() {
        let f = Fixture::new().await;
        let binding = f.core.accounts().storage_binding_for("owner").await.unwrap();
        let observation = binding.observation();
        drop(binding);
        let rebound = f.core.accounts().storage_binding_for_observation(&observation).await.unwrap();
        let proof = VerifiedStageBinding::verify_ready(&rebound, STAGE, super::super::MAX_BYTES, |_| Ok(())).unwrap();
        drop(rebound);
        let standalone = crate::runtime::Accounts::new(f.core.open_db().unwrap(), f.core.host().clone(),
            Arc::new(MemorySecretStore::new()), f.core.bus().clone());
        let other = standalone.storage_binding_for("owner").await.unwrap();
        assert!(matches!(other.stage_mutations(), Err(Error::TargetChanged)));
        drop(other);
        f.core.managed_sources().close();
        assert!(matches!(proof.claim(intent()), Err(Error::Cancelled)));
        assert!(f.disk().phase == Phase::Ready);
        assert!(matches!(f.core.stage_mutations().begin(), Err(Error::Cancelled)));
        f.close().await;
    }

    #[tokio::test]
    async fn real_claim_is_one_use_and_consumes_only_the_exact_retained_request_offline() {
        let f = Fixture::new().await;
        let original = f.disk();
        let mut claim = f.proof().await.claim(intent()).unwrap();
        assert!(f.disk().phase == Phase::Claimed(intent()));
        assert_eq!(claim.checked_path().unwrap(), f.core.host().config().upload_dir("owner").join(STAGE).join("bytes"));
        assert!(matches!(claim.checked_path(), Err(Error::Integrity)));
        f.admit(&claim);
        // Already-retained admission needs neither current posting rights nor a running plugin.
        let db = f.core.open_db().unwrap();
        db.conn().execute("UPDATE conversation SET can_post=0,is_gone=1", []).unwrap();
        db.conn().execute("UPDATE topic SET closed=1,is_gone=1", []).unwrap();
        let consumed = claim.consume(&f.outbox).unwrap();
        assert!(consumed.phase == Phase::Consumed(intent()));
        assert_eq!(consumed.sha256, original.sha256);
        assert!(consumed.fingerprint == original.fingerprint);
        assert!(f.disk().phase == consumed.phase);
        assert_eq!(f.outbox.get_by_client_id(CLIENT).unwrap().unwrap().client_id, CLIENT);
        f.close().await;
    }

    #[tokio::test]
    async fn canonical_file_request_matrix_matches_actual_retained_params() {
        for format in [Format::Jpeg, Format::Png, Format::Other] {
            for as_file in [false, true] {
                for caption in ["", " literal 🦀 "] {
                    let f = Fixture::with_format(format).await;
                    let spoiler = !caption.is_empty();
                    let options = Intent { caption: caption.into(), as_file, spoiler };
                    let mut claim = f.proof().await.claim(options.clone()).unwrap();
                    let path = claim.checked_path().unwrap();
                    let photo = !as_file && format != Format::Other;
                    let mut expected = serde_json::json!({
                        "account_id":"owner", "chat_id":"chat", "topic_id":"topic",
                        "path":path.to_str().unwrap(), "filename":"synthetic.bin",
                        "kind":if photo { "photo" } else { "document" },
                    });
                    if as_file { expected["as_file"] = true.into(); }
                    if spoiler { expected["spoiler"] = true.into(); }
                    if !caption.is_empty() { expected["caption"] = caption.into(); }
                    let OutboxRequest::SendFile(params) = claim.canonical_request().unwrap() else { panic!("wrong request kind") };
                    assert_eq!(serde_json::to_value(&params).unwrap(), expected);
                    f.admit(&claim);
                    let db = f.core.open_db().unwrap();
                    let retained = ch_db::get_outbox_by_client_id(db.conn(), CLIENT).unwrap().unwrap();
                    assert_eq!(retained.params, expected);
                    assert!(claim.consume(&f.outbox).unwrap().phase == Phase::Consumed(options));
                    f.close().await;
                }
            }
        }
    }

    #[tokio::test]
    async fn preparation_owns_original_binding_and_flock_and_burns_admission_once() {
        let f = Fixture::new().await;
        // Fixture::proof already drops the caller binding; only the real claim retains it.
        let claim = f.proof().await.claim(intent()).unwrap();
        let directory = claim.proof.dir.clone();
        let prepared = claim.prepare_admission();
        let observed = prepared.owner_observation();
        assert!(matches!(directory.lock(), Err(Error::Busy)));
        let identity = prepared.identity().unwrap().clone();
        let json = identity.to_persisted_json().unwrap();
        assert!(super::super::StageIdentity::from_persisted_json(&json).unwrap() == identity);
        assert!(super::super::StageIdentity::from_persisted_json(&json.replacen("\"version\":1", "\"version\":2", 1)).is_err());
        let mut legacy: serde_json::Value = serde_json::from_str(&json).unwrap();
        let namespace = legacy.as_object_mut().unwrap().remove("namespace").unwrap();
        legacy["ancestry"] = namespace;
        assert!(super::super::StageIdentity::from_persisted_json(&legacy.to_string()).is_err());
        let mut extra: serde_json::Value = serde_json::from_str(&json).unwrap();
        extra["namespace"][0]["mount"] = 100.into();
        assert!(super::super::StageIdentity::from_persisted_json(&extra.to_string()).is_err());
        let mut spent = prepared.into_claim();
        assert!(matches!(spent.checked_path(), Err(Error::Integrity)));
        let prepared_again = spent.prepare_admission();
        assert!(matches!(prepared_again.identity(), Err(Error::Integrity)));
        assert!(prepared_again.request().is_ok(), "spent authority must preserve comparison intent");
        let accounts = f.core.accounts().clone();
        let removing = tokio::spawn(async move { accounts.remove("owner").await });
        tokio::time::timeout(std::time::Duration::from_secs(5), observed.revocation().cancelled()).await.unwrap();
        assert!(!removing.is_finished(), "prepared claim alone retains the actual storage lease");
        drop(prepared_again);
        tokio::time::timeout(std::time::Duration::from_secs(5), removing).await.unwrap().unwrap().unwrap();
        // A directory descriptor alone has no storage lease or flock authority.
        drop(directory);
        assert!(observed.is_revoked());
        drop(identity);
        f.close().await;
    }

    #[tokio::test]
    async fn current_held_mount_checks_remain_strict_with_stable_namespace_metadata() {
        let f = Fixture::new().await;
        let mut claim = f.proof().await.claim(intent()).unwrap();
        let identity = super::super::StageIdentity::capture(&claim.proof).unwrap();
        let actual = claim.proof.dir.identity;
        // Synthetic observation fault only; no OS mount manipulation or fake proof.
        Arc::get_mut(&mut claim.proof.dir).unwrap().identity.mount = actual.mount.wrapping_add(1);
        assert!(matches!(claim.validate_claimed(), Err(Error::StageChanged)));
        Arc::get_mut(&mut claim.proof.dir).unwrap().identity = actual;
        claim.validate_claimed().unwrap();
        assert!(super::super::StageIdentity::capture(&claim.proof).unwrap() == identity);
        let json: serde_json::Value = serde_json::from_str(&identity.to_persisted_json().unwrap()).unwrap();
        assert_eq!(json["namespace"].as_array().unwrap().len(), 5);
        drop(claim);
        f.close().await;
    }

    #[tokio::test]
    async fn failed_preparation_keeps_exact_original_intent_for_retained_lookup_first() {
        let f = Fixture::new().await;
        let claim = f.proof().await.claim(intent()).unwrap();
        f.admit(&claim);
        let directory = claim.proof.dir.clone();
        let mut changed = claim.manifest().clone();
        let Phase::Claimed(changed_intent) = &mut changed.phase else { panic!("missing real claim") };
        changed_intent.caption = "foreign disk caption".into();
        std::fs::write(directory.path.join("manifest.json"), serde_json::to_vec(&changed).unwrap()).unwrap();
        let prepared = claim.prepare_admission();
        assert!(matches!(prepared.identity(), Err(Error::StageChanged)));
        let request = prepared.request().unwrap();
        let OutboxRequest::SendFile(params) = request else { panic!("wrong comparison request") };
        assert_eq!(params.caption.as_deref(), Some(" literal "));
        assert!(f.outbox.get_matching_admission(prepared.owner_binding().owner(), CLIENT, request).unwrap().is_some(),
            "an exact retained admission is still readable despite failed FS preparation");
        assert!(matches!(directory.lock(), Err(Error::Busy)), "failure retains the original claim until the decision settles");
        let mut spent = prepared.into_claim();
        assert!(matches!(spent.checked_path(), Err(Error::Integrity)));
        drop(spent);
        drop(directory.lock().unwrap());
        assert!(f.disk() == changed, "preparation never rewrites observed/requested intent");
        let count:i64 = f.core.open_db().unwrap().conn().query_row("SELECT count(*) FROM outbox", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 1);
        f.close().await;
    }

    #[tokio::test]
    async fn missing_conflicting_or_unchecked_admission_never_publishes_consumed() {
        for case in 0..3 {
            let f = Fixture::new().await;
            let mut claim = f.proof().await.claim(intent()).unwrap();
            if case != 2 { claim.checked_path().unwrap(); }
            if case != 0 { f.admit(&claim); }
            if case == 1 {
                f.core.open_db().unwrap().conn().execute(
                    "UPDATE outbox SET params_json=json_set(params_json,'$.caption','foreign')", []).unwrap();
            }
            assert!(matches!(claim.consume(&f.outbox), Err(Error::Integrity)));
            assert!(f.disk().phase == Phase::Claimed(intent()));
            let binding = f.core.accounts().storage_binding_for("owner").await.unwrap();
            assert!(matches!(VerifiedStageBinding::verify_ready(&binding, STAGE, super::super::MAX_BYTES, |_| Ok(())), Err(Error::Integrity)));
            drop(binding);
            f.close().await;
        }
    }

    #[tokio::test]
    async fn claim_directory_sync_failure_mints_no_claim_and_recovery_cannot_reclaim() {
        let f = Fixture::new().await;
        assert!(matches!(f.proof().await.claim_with(intent(), fail_directory), Err(Error::Unknown)));
        assert!(f.disk().phase == Phase::Claimed(intent()));
        assert!(f.outbox.get_by_client_id(CLIENT).unwrap().is_none());
        let binding = f.core.accounts().storage_binding_for("owner").await.unwrap();
        assert!(matches!(VerifiedStageBinding::verify_ready(&binding, STAGE, super::super::MAX_BYTES, |_| Ok(())), Err(Error::Integrity)));
        drop(binding);
        f.close().await;
    }

    #[tokio::test]
    async fn claim_file_sync_failure_before_link_preserves_ready_layout_and_no_authority() {
        let f = Fixture::new().await;
        let original = f.disk();
        let failed = f.proof().await.claim_with(intent(), |_file, directory| {
            assert!(!directory, "file failure must precede directory publication");
            Err(io::Error::other("synthetic file sync failure"))
        });
        assert!(matches!(failed, Err(Error::Storage)));
        assert!(f.disk() == original);
        let mut entries = std::fs::read_dir(f.core.host().config().upload_dir("owner").join(STAGE))
            .unwrap().map(|entry| entry.unwrap().file_name()).collect::<Vec<_>>();
        entries.sort();
        assert_eq!(entries, vec![std::ffi::OsString::from("bytes"), std::ffi::OsString::from("manifest.json")]);
        assert!(f.outbox.get_by_client_id(CLIENT).unwrap().is_none());
        // A fresh complete verification confirms actual Ready bytes, not row-absence inference.
        let proof = f.proof().await;
        assert!(proof.manifest().phase == Phase::Ready);
        drop(proof);
        f.close().await;
    }

    #[tokio::test]
    async fn consumed_sync_failure_keeps_canonical_admission_without_replay() {
        let f = Fixture::new().await;
        let mut claim = f.proof().await.claim(intent()).unwrap();
        claim.checked_path().unwrap();
        f.admit(&claim);
        assert!(matches!(claim.consume_with(&f.outbox, fail_directory), Err(Error::Unknown)));
        assert!(f.disk().phase == Phase::Consumed(intent()));
        let count:i64 = f.core.open_db().unwrap().conn().query_row("SELECT count(*) FROM outbox", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 1);
        f.close().await;
    }

    #[tokio::test]
    async fn unavailable_retained_lookup_never_publishes_consumed() {
        let f = Fixture::new().await;
        let mut claim = f.proof().await.claim(intent()).unwrap();
        claim.checked_path().unwrap();
        f.admit(&claim);
        let db = f.core.open_db().unwrap();
        db.conn().execute("ALTER TABLE outbox RENAME TO unavailable_outbox", []).unwrap();
        assert!(matches!(claim.consume(&f.outbox), Err(Error::Storage)));
        assert!(f.disk().phase == Phase::Claimed(intent()));
        db.conn().execute("ALTER TABLE unavailable_outbox RENAME TO outbox", []).unwrap();
        assert!(f.outbox.get_by_client_id(CLIENT).unwrap().is_some());
        f.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn owner_revoked_before_claim_never_publishes_a_transition() {
        let f = Fixture::new().await;
        let binding = f.core.accounts().storage_binding_for("owner").await.unwrap();
        let revoked = binding.revocation();
        let proof = VerifiedStageBinding::verify_ready(&binding, STAGE, super::super::MAX_BYTES, |_| Ok(())).unwrap();
        let accounts = f.core.accounts().clone();
        let remove = tokio::spawn(async move { accounts.remove("owner").await });
        tokio::time::timeout(std::time::Duration::from_secs(5), revoked.cancelled()).await.unwrap();
        assert!(matches!(proof.claim(intent()), Err(Error::TargetChanged)));
        assert!(f.disk().phase == Phase::Ready);
        assert!(f.outbox.get_by_client_id(CLIENT).unwrap().is_none());
        drop(binding);
        tokio::time::timeout(std::time::Duration::from_secs(5), remove).await.unwrap().unwrap().unwrap();
        f.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn live_claim_alone_holds_storage_and_stage_flock_until_drop_or_consume() {
        use std::os::fd::AsRawFd;
        fn try_flock(file: &File) -> io::Result<()> {
            // SAFETY: file is a live independently opened directory descriptor.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                Ok(())
            } else { Err(io::Error::last_os_error()) }
        }
        for consume in [false, true] {
            let f = Fixture::new().await;
            let binding = f.core.accounts().storage_binding_for("owner").await.unwrap();
            let revoked = binding.revocation();
            let mut claim = VerifiedStageBinding::verify_ready(&binding, STAGE, super::super::MAX_BYTES, |_| Ok(())).unwrap()
                .claim(intent()).unwrap();
            // Probe owns no storage guard or flock; keep its open description across removal.
            let probe = fd::openat(claim.proof.dir.fd(), c".", libc::O_RDONLY | libc::O_DIRECTORY, 0).unwrap();
            assert_eq!(try_flock(&probe).unwrap_err().kind(), io::ErrorKind::WouldBlock);
            if consume { claim.checked_path().unwrap(); f.admit(&claim); }
            drop(binding); // The live claim is now the ONLY storage read-guard owner.
            let accounts = f.core.accounts().clone();
            let remove = tokio::spawn(async move { accounts.remove("owner").await });
            tokio::time::timeout(std::time::Duration::from_secs(5), revoked.cancelled()).await.unwrap();
            assert!(!remove.is_finished());
            if consume {
                assert!(claim.consume(&f.outbox).unwrap().phase == Phase::Consumed(intent()));
            } else { drop(claim); }
            // Even a directory already unlinked by removal remains a valid flock probe.
            try_flock(&probe).unwrap();
            drop(probe);
            tokio::time::timeout(std::time::Duration::from_secs(5), remove).await.unwrap().unwrap().unwrap();
            f.close().await;
        }
    }
}
