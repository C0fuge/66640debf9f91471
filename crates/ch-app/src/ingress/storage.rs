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
    fn import_candidates(&self) -> Option<(Arc<fs::Store>, fs::Inventory)> {
        let store = Arc::new(self.store().ok()?);
        let mut inventory = store.inventory();
        inventory
            .manifests
            .sort_by(|a, b| (a.created, &a.stage_id).cmp(&(b.created, &b.stage_id)));
        Some((store, inventory))
    }
    pub(super) async fn refresh(self: &Arc<Self>) {
        let Ok(_refresh) = self.refresh_gate.try_lock() else {
            return;
        };
        let epoch = {
            let mut l = lock(&self.ledger);
            l.notice_revocations();
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
        let Ok(reader) = self.readers.clone().try_read_owned() else {
            return;
        };
        if self.stop.is_cancelled() {
            return;
        }
        let s = self.clone();
        // A read request may be dropped during metadata I/O; the blocking task retains the
        // scan permit. No path is published, deleted or enqueued by this operation.
        let Ok(result) = tokio::task::spawn_blocking(move || {
            let (_reader, _slot) = (reader, slot);
            s.import_candidates()
        })
        .await
        else {
            return;
        };
        let mut imported = Vec::new();
        let Some((store, inventory)) = result else {
            let mut l = lock(&self.ledger);
            if l.inventory_epoch == epoch {
                l.next_refresh =
                    Some(std::time::Instant::now() + std::time::Duration::from_millis(250));
            }
            return;
        };
        let mut complete = inventory.complete;
        let visible: BTreeSet<_> = inventory
            .manifests
            .iter()
            .map(|m| m.stage_id.clone())
            .collect();
        for m in inventory.manifests {
            {
                let l = lock(&self.ledger);
                if l.ids
                    .get(&m.stage_id)
                    .is_some_and(|n| !l.entries[n].generation.is_cancelled())
                {
                    continue;
                }
            }
            let Ok(Some(owner)) = self.core.accounts().get(&m.scope.account).await else {
                complete = false;
                continue;
            };
            let Ok((generation, guard)) = self.core.accounts().storage_lease(&owner).await else {
                complete = false;
                continue;
            };
            // Resolve the current binding under the captured generation. The owner may
            // have been removed and recreated with the same timestamp since inventory.
            let Ok(slot) = self.scan.clone().try_acquire_owned() else {
                complete = false;
                break;
            };
            let Ok(reader) = self.readers.clone().try_read_owned() else {
                return;
            };
            if self.stop.is_cancelled() {
                return;
            }
            let stage_store = store.clone();
            let fs_owner = self.fs_owner(&owner);
            let binding = tokio::task::spawn_blocking(move || {
                let (_guard, _reader, _slot) = (guard, reader, slot);
                let stage = stage_store.recover(&fs_owner, &m.stage_id).ok()?;
                (stage.manifest() == &m).then(|| Binding {
                    manifest: m,
                    path: stage.managed_path(),
                })
            })
            .await;
            let Ok(Some(b)) = binding else {
                complete = false;
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
                retired: false,
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
            imported.push(e);
        }
        let mut l = lock(&self.ledger);
        if l.closed || l.inventory_epoch != epoch {
            return;
        }
        l.scanned_epoch = complete.then_some(epoch);
        l.next_refresh = Some(std::time::Instant::now() + std::time::Duration::from_millis(250));
        if complete {
            // A retained key still names its old ordinal even if its authorized
            // receipt disappeared. A lookup of that key returns scoped missing.
            let gone: Vec<_> = l
                .entries
                .iter()
                .filter(|(_, e)| e.retired && e.active.is_none() && !visible.contains(&e.stage))
                .map(|(n, e)| (*n, e.stage.clone()))
                .collect();
            for (n, id) in gone {
                l.ids.remove(&id);
                l.entries.remove(&n);
            }
        }
        for e in imported {
            if e.generation.is_cancelled() {
                continue;
            }
            let account = e.thread.account_id.clone();
            if let Some(&n) = l.ids.get(&e.stage) {
                let old = &l.entries[&n];
                if !old.generation.is_cancelled() {
                    continue;
                }
                if old.active.is_some() {
                    l.scanned_epoch = None;
                    continue;
                }
                // Rebind at the same ordinal so an existing session cursor keeps
                // its original ordering. The retired start key cannot authorize e.
                l.entries.insert(n, e);
            } else {
                if l.entries.values().filter(|row| row.key.is_none()).count() >= 256 {
                    l.scanned_epoch = None;
                    break;
                }
                l.insert(e);
            }
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
            let problem = match self.outbox.get_by_client_id(&b.manifest.client_id) {
                Ok(None) => return,
                Ok(Some(_)) => Problem::new("conflict", "integrity"),
                Err(_) => Problem::new("unavailable", "storage"),
            };
            let mut l = lock(&self.ledger);
            if let Some(current) = l
                .entries
                .get_mut(&n)
                .filter(|r| r.active.is_none() && r.state == State::Ready)
            {
                if problem.reason == "integrity" {
                    current.state = State::Failed;
                }
                current.problem = Some(problem);
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
fn valid_outbox_shape(kind: &str, method: &str, raw: &str) -> bool {
    use ch_protocol::methods::*;
    let Some(kind) = OutboxKind::parse(kind) else {
        return false;
    };
    if method != kind.method() {
        return false;
    }
    // Decode original text so repeated typed fields and missing required paths
    // fail the proof. The subsequent tree walk also checks extension path slots.
    match kind {
        OutboxKind::SendText => serde_json::from_str::<MessagesSendParams>(raw).is_ok(),
        OutboxKind::SendFile => serde_json::from_str::<MessagesSendFileParams>(raw).is_ok(),
        OutboxKind::SendAlbum => serde_json::from_str::<MessagesSendAlbumParams>(raw).is_ok(),
        OutboxKind::Forward => serde_json::from_str::<MessagesForwardParams>(raw).is_ok(),
        OutboxKind::Edit => serde_json::from_str::<MessagesEditParams>(raw).is_ok(),
        OutboxKind::Delete => serde_json::from_str::<MessagesDeleteParams>(raw).is_ok(),
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
    let fixed_row: bool = db
        .conn()
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM outbox WHERE client_id=?1)",
            [&m.client_id],
            |row| row.get(0),
        )
        .map_err(|_| fs::Error::Storage)?;
    if fixed_row {
        return Ok(false);
    }
    let mut remaining = 256usize;
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
        let tags = if table == "outbox" {
            ", CASE WHEN length(cast(kind AS blob)) < 24 THEN kind END, CASE WHEN length(cast(method AS blob)) < 64 THEN method END"
        } else {
            ""
        };
        let sql = format!(
            "SELECT length(cast({col} AS blob)), CASE WHEN length(cast({col} AS blob)) <= 65536 THEN {col} END{tags} FROM {table} LIMIT ?1"
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
                .get::<_, Option<i64>>(0)
                .map_err(|_| fs::Error::Integrity)?
            else {
                continue;
            };
            let Ok(len) = usize::try_from(len) else {
                return Ok(false);
            };
            if len > 65536 || len > bytes {
                return Ok(false);
            }
            bytes -= len;
            let text: String = row.get(1).map_err(|_| fs::Error::Integrity)?;
            let distinct = if json {
                if table == "outbox" {
                    let (Ok(kind), Ok(method)) = (row.get::<_, String>(2), row.get::<_, String>(3))
                    else {
                        return Ok(false);
                    };
                    if !valid_outbox_shape(&kind, &method, &text) {
                        return Ok(false);
                    }
                } else if serde_json::from_str::<ch_protocol::dto::Media>(&text).is_err() {
                    return Ok(false);
                }
                let Ok(value) = serde_json::from_str(&text) else {
                    return Ok(false);
                };
                distinct_refs(&value, identity, &mut path_budget)
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
