use super::*;
use ch_core::{host::AccountPhase, outbox::OutboxRequest};
use ch_protocol::{
    Flag,
    methods::{FileKind, MessagesSendFileParams},
};
use std::path::Path;

impl Service {
    pub(super) fn fs_owner(&self, a: &AccountRow) -> fs::Owner {
        fs::Owner {
            account: a.account_id.clone(),
            plugin: a.plugin_id.clone(),
            account_created: a.created_at,
            uploads: self.core.host().config().upload_dir(&a.account_id),
        }
    }
    pub(super) fn current_owner(&self, a: &AccountRow) -> Result<AccountRow> {
        ch_db::get_account(lock(&self.db).conn(), &a.account_id)?
            .filter(|v| v.plugin_id == a.plugin_id && v.created_at == a.created_at)
            .ok_or(Failure::Missing)
    }
    pub(super) fn eligible(
        &self,
        a: &AccountRow,
        thread: &Thread,
        intent: Option<&fs::Intent>,
    ) -> Result<(u64, Arc<ch_protocol::Capabilities>)> {
        if !self.current_owner(a)?.enabled {
            return Err(Failure::Unavailable);
        }
        let host = self.core.host();
        if !host
            .account(&a.plugin_id, &a.account_id)
            .is_some_and(|v| v.phase == AccountPhase::Running)
        {
            return Err(Failure::Unavailable);
        }
        {
            let db = lock(&self.db);
            let c = ch_db::get_conversation(db.conn(), &a.account_id, &thread.chat_id)?
                .filter(|c| !c.is_gone)
                .ok_or(Failure::Missing)?;
            if c.can_post == Some(false) {
                return Err(Failure::Conflict);
            }
            if let Some(topic) = &thread.topic_id {
                let topic = ch_db::get_topic(db.conn(), c.id, topic)?
                    .filter(|t| !t.is_gone)
                    .ok_or(Failure::Missing)?;
                if topic.closed {
                    return Err(Failure::Conflict);
                }
            }
        }
        let state = host
            .plugin_state(&a.plugin_id)
            .ok_or(Failure::Unavailable)?;
        let caps = state.capabilities().ok_or(Failure::Unavailable)?;
        if intent.is_some_and(|i| {
            (i.as_file && !caps.has("send.as_file")) || (i.spoiler && !caps.has("send.spoiler"))
        }) {
            return Err(Failure::Unsupported);
        }
        let limit = match caps.get("send.file") {
            Some(Flag::Bool(true)) => Ok(fs::MAX_BYTES),
            Some(Flag::Params(m)) => match m.get("max_bytes") {
                None => Ok(fs::MAX_BYTES),
                Some(n) => n
                    .as_u64()
                    .filter(|&n| n > 0)
                    .map(|n| n.min(fs::MAX_BYTES))
                    .ok_or(Failure::Unsupported),
            },
            _ => Err(Failure::Unsupported),
        }?;
        Ok((limit, caps.clone()))
    }
    pub(super) fn store(&self) -> std::result::Result<fs::Store, fs::Error> {
        let db = lock(&self.db);
        let mut stmt = db
            .conn()
            .prepare("SELECT account_id,plugin_id,created_at FROM account LIMIT 4097")
            .map_err(|_| fs::Error::Storage)?;
        let owners = stmt
            .query_map([], |r| {
                let account: String = r.get(0)?;
                Ok(fs::Owner {
                    uploads: self.core.host().config().upload_dir(&account),
                    account,
                    plugin: r.get(1)?,
                    account_created: r.get(2)?,
                })
            })
            .map_err(|_| fs::Error::Storage)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| fs::Error::Storage)?;
        if owners.len() > 4096 {
            return Err(fs::Error::Integrity);
        }
        drop(stmt);
        drop(db);
        fs::Store::open(&self.core.data_dir().join("accounts"), owners)
    }
    pub(super) fn private_roots(&self) -> Vec<PathBuf> {
        let c = self.core.host().config();
        let mut roots = vec![c.data_dir.clone()];
        for p in [
            Some(&c.user_plugins_dir),
            c.builtin_plugins_dir.as_ref(),
            c.plugin_log_dir.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if !roots.iter().any(|r| p.starts_with(r)) {
                roots.push(p.clone());
            }
        }
        roots
    }
    fn import_candidates(&self) -> (bool, Vec<(AccountRow, Binding)>) {
        let Ok(store) = self.store() else {
            return (false, vec![]);
        };
        let mut inv = store.inventory();
        inv.manifests
            .sort_by(|a, b| (a.created, &a.stage_id).cmp(&(b.created, &b.stage_id)));
        let mut entries = Vec::new();
        for m in inv.manifests {
            let Ok(Some(owner)) = ch_db::get_account(lock(&self.db).conn(), &m.scope.account)
            else {
                inv.complete = false;
                continue;
            };
            let Ok(stage) = store.recover(&self.fs_owner(&owner), &m.stage_id) else {
                inv.complete = false;
                continue;
            };
            if stage.manifest() != &m {
                inv.complete = false;
                continue;
            }
            entries.push((
                owner,
                Binding {
                    manifest: m,
                    path: stage.managed_path(),
                },
            ));
        }
        (inv.complete, entries)
    }
    pub(super) async fn refresh(self: &Arc<Self>) {
        let epoch = {
            let l = lock(&self.ledger);
            if l.closed
                || l.scanned_epoch == Some(l.inventory_epoch)
                || l.next_refresh
                    .is_some_and(|t| std::time::Instant::now() < t)
            {
                return;
            }
            l.inventory_epoch
        };
        let Ok(slot) = self.scan.clone().try_acquire_owned() else {
            return;
        };
        let s = self.clone();
        // A read request may be dropped during metadata I/O; the blocking task retains the
        // scan permit. No path is published, deleted or enqueued by this operation.
        let Ok((complete, candidates)) = tokio::task::spawn_blocking(move || {
            let _slot = slot;
            s.import_candidates()
        })
        .await
        else {
            return;
        };
        let mut imported = Vec::new();
        for (owner, b) in candidates {
            let Ok((generation, _guard)) = self.core.accounts().storage_lease(&owner).await else {
                continue;
            };
            let m = &b.manifest;
            let mut e = Entry {
                stage: m.stage_id.clone(),
                key: None,
                thread: Thread {
                    account_id: m.scope.account.clone(),
                    chat_id: m.scope.chat.clone(),
                    topic_id: m.scope.topic.clone(),
                },
                plugin: owner.plugin_id.clone(),
                birth: owner.created_at,
                generation,
                state: State::Ready,
                name: None,
                bytes: None,
                format: None,
                copied: None,
                bound: None,
                intent: None,
                proven: false,
                outbox_row: None,
                problem: None,
                active: None,
            };
            e.bind(b);
            imported.push((owner, e));
        }
        let mut l = lock(&self.ledger);
        if l.closed || l.inventory_epoch != epoch {
            return;
        }
        l.scanned_epoch = complete.then_some(epoch);
        l.next_refresh = Some(std::time::Instant::now() + std::time::Duration::from_millis(250));
        for (_, e) in imported {
            if e.generation.is_cancelled() || l.ids.contains_key(&e.stage) {
                continue;
            }
            let account = e.thread.account_id.clone();
            l.insert(e);
            events::notify(&self.changes, &account);
        }
    }
    pub(super) fn reconcile(&self, n: u64, e: &Entry, owner: &AccountRow) {
        if e.active.is_some()
            || !matches!(
                e.state,
                State::Ready | State::Enqueued | State::AdmissionUnknown
            )
        {
            return;
        }
        let Some(b) = &e.bound else {
            return;
        };
        if matches!(b.manifest.phase, fs::Phase::Ready) && e.outbox_row.is_none() {
            if matches!(
                self.outbox.get_by_client_id(&b.manifest.client_id),
                Ok(None)
            ) {
                return;
            }
            let mut l = lock(&self.ledger);
            if let Some(current) = l
                .entries
                .get_mut(&n)
                .filter(|r| r.active.is_none() && r.state == State::Ready)
            {
                current.state = State::Failed;
                current.problem = Some(Problem::new("conflict", "integrity"));
            }
            return;
        }
        let Some(i) = &e.intent else {
            return;
        };
        let matching = request(&b.manifest, i, &b.path)
            .map_err(|_| Failure::Invalid)
            .and_then(|r| {
                self.outbox
                    .get_matching_admission(owner, &b.manifest.client_id, &r)
                    .map_err(Failure::from)
            });
        let mut l = lock(&self.ledger);
        let Some(current) = l.entries.get_mut(&n).filter(|r| {
            r.active.is_none() && matches!(r.state, State::Enqueued | State::AdmissionUnknown)
        }) else {
            return;
        };
        let reason = match matching {
            Ok(Some(_)) => {
                current.proven = true;
                current.outbox_row = Some("present");
                None
            }
            Ok(None) => {
                current.outbox_row = Some("missing");
                (!current.proven).then(Problem::unknown)
            }
            Err(error) => {
                current.outbox_row = Some("unchecked");
                Some(Problem {
                    code: "unavailable",
                    reason: if matches!(error, Failure::Conflict) {
                        "integrity"
                    } else {
                        "storage"
                    },
                    outcome_unknown: !current.proven,
                })
            }
        };
        current.state = if current.proven {
            State::Enqueued
        } else {
            State::AdmissionUnknown
        };
        current.problem = reason;
    }
}
pub(super) fn request(
    m: &fs::Manifest,
    i: &fs::Intent,
    path: &Path,
) -> std::result::Result<OutboxRequest, fs::Error> {
    let photo = !i.as_file && matches!(m.format, fs::Format::Jpeg | fs::Format::Png);
    Ok(OutboxRequest::SendFile(MessagesSendFileParams {
        account_id: m.scope.account.clone(),
        chat_id: m.scope.chat.clone(),
        topic_id: m.scope.topic.clone(),
        path: path.to_str().ok_or(fs::Error::Integrity)?.to_owned(),
        filename: Some(m.name.clone()),
        kind: Some(if photo {
            FileKind::Photo
        } else {
            FileKind::Document
        }),
        caption: (!i.caption.is_empty()).then(|| i.caption.clone()),
        as_file: i.as_file.then_some(true),
        spoiler: i.spoiler.then_some(true),
        ..Default::default()
    }))
}
pub(super) fn budget(
    inv: &fs::Inventory,
    a: &fs::Owner,
    size: u64,
) -> std::result::Result<(), Problem> {
    let key = a
        .uploads
        .parent()
        .and_then(Path::file_name)
        .ok_or(fs::Error::Integrity)?;
    let used = inv.per_directory.get(key).map_or(0, |c| c.bytes);
    if !inv.complete
        || inv.total.slots >= 16
        || size > (8u64 << 30).saturating_sub(inv.total.bytes)
        || size > (4u64 << 30).saturating_sub(used)
    {
        return Err(Problem::capacity());
    }
    Ok(())
}
// Resolve only cached, symlink-free existing bindings. The fd is metadata-only:
// an unresolved/malformed reference, cache miss or unsupported syscall keeps the stage.
fn distinct_file(raw: &str, identity: (u64, u64), budget: &mut usize) -> bool {
    use std::{
        ffi::CString,
        fs::File,
        os::{fd::FromRawFd, unix::fs::MetadataExt},
    };
    if !raw.starts_with('/')
        || raw.len() > 16384
        || *budget == 0
        || raw.split('/').any(|c| c == "." || c == "..")
    {
        return false;
    }
    *budget -= 1;
    let Ok(raw) = CString::new(raw) else {
        return false;
    };
    // SAFETY: open_how contains only integer fields and its reserved fields must be zero.
    let mut how: libc::open_how = unsafe { std::mem::zeroed() };
    how.flags = (libc::O_PATH | libc::O_CLOEXEC) as u64;
    how.resolve = libc::RESOLVE_NO_SYMLINKS | libc::RESOLVE_NO_MAGICLINKS | libc::RESOLVE_CACHED;
    // SAFETY: both pointers are valid for the call. Success yields a new owned fd.
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            libc::AT_FDCWD,
            raw.as_ptr(),
            &how,
            std::mem::size_of_val(&how),
        )
    };
    if fd < 0 {
        return false;
    }
    let file = unsafe { File::from_raw_fd(fd as i32) };
    file.metadata()
        .is_ok_and(|m| m.is_file() && (m.dev(), m.ino()) != identity)
}
fn distinct_refs(v: &Value, identity: (u64, u64), budget: &mut usize) -> bool {
    match v {
        Value::Array(a) => a.iter().all(|v| distinct_refs(v, identity, budget)),
        Value::Object(m) => m.iter().all(|(k, v)| {
            if k == "path" || k.ends_with("_path") {
                v.is_null()
                    || v.as_str()
                        .is_some_and(|s| distinct_file(s, identity, budget))
            } else {
                distinct_refs(v, identity, budget)
            }
        }),
        _ => true,
    }
}
// Only the ready phase, private namespace and an absence proof under the SQLite writer
// lock authorize discard. Leave that lock in the caller through actual fs settlement.
pub(super) fn unreferenced(
    db: &Db,
    m: &fs::Manifest,
    path: &Path,
) -> std::result::Result<bool, fs::Error> {
    use std::os::unix::fs::MetadataExt;
    let byte = std::fs::symlink_metadata(path).map_err(|_| fs::Error::StageChanged)?;
    if !byte.is_file() || byte.nlink() != 1 {
        return Ok(false);
    }
    let identity = (byte.dev(), byte.ino());
    let mut path_budget = 128;
    if ch_db::get_outbox_by_client_id(db.conn(), &m.client_id)
        .map_err(|_| fs::Error::Storage)?
        .is_some()
    {
        return Ok(false);
    }
    let mut remaining = 4096usize;
    let mut bytes = 1024 * 1024usize;
    for (table, col, json) in [
        ("outbox", "params_json", true),
        ("message", "media_json", true),
        ("message_version", "media_json", true),
        ("media_file", "path", false),
        ("conversation", "avatar_path", false),
        ("platform_handle", "avatar_path", false),
        ("plugin", "icon_path", false),
    ] {
        let sql = format!(
            "SELECT length(cast({col} AS blob)), CASE WHEN length(cast({col} AS blob)) <= 65536 THEN {col} END FROM {table} LIMIT ?1"
        );
        let mut stmt = db.conn().prepare(&sql).map_err(|_| fs::Error::Storage)?;
        let mut rows = stmt
            .query([i64::try_from(remaining + 1).map_err(|_| fs::Error::Integrity)?])
            .map_err(|_| fs::Error::Storage)?;
        while let Some(row) = rows.next().map_err(|_| fs::Error::Storage)? {
            let Some(left) = remaining.checked_sub(1) else {
                return Ok(false);
            };
            remaining = left;
            let Some(len) = row
                .get::<_, Option<usize>>(0)
                .map_err(|_| fs::Error::Integrity)?
            else {
                continue;
            };
            if len > 65536 || len > bytes {
                return Ok(false);
            }
            bytes -= len;
            let text: String = row.get(1).map_err(|_| fs::Error::Integrity)?;
            let distinct = if json {
                distinct_refs(
                    &serde_json::from_str(&text).map_err(|_| fs::Error::Integrity)?,
                    identity,
                    &mut path_budget,
                )
            } else {
                distinct_file(&text, identity, &mut path_budget)
            };
            if !distinct {
                return Ok(false);
            }
        }
    }
    Ok(true)
}
