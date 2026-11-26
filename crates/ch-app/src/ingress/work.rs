use super::storage::{budget, request, unreferenced};
use super::*;
use crate::SaveDialogError;
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;

enum Action {
    Choose(Arc<dyn OpenFileDialog>),
    Send(fs::Intent),
    Discard,
}
type Work<T> = std::result::Result<T, Problem>;

struct Job {
    s: Arc<Service>,
    seq: u64,
    owner: AccountRow,
    thread: Thread,
    stage: String,
    cancel: CancellationToken,
    generation: CancellationToken,
    handshake: Option<Arc<ch_protocol::Capabilities>>,
}
impl Service {
    async fn gates(
        self: &Arc<Self>,
        owner: AccountRow,
        t: Thread,
        intent: Option<fs::Intent>,
    ) -> Result<Arc<ch_protocol::Capabilities>> {
        let s = self.clone();
        blocking(move || {
            s.eligible(&owner, &t, intent.as_ref())
                .map(|(_, caps)| caps)
        })
        .await
    }
    pub(crate) async fn start(
        self: &Arc<Self>,
        thread: Thread,
        session: String,
        key: String,
    ) -> Result<Value> {
        self.session(&session)?;
        if !valid_key(&key) {
            return Err(Failure::Invalid);
        }
        let owner = self.owner(&thread).await?;
        let existing = |l: &Ledger| -> Result<Option<Value>> {
            if let Some(seq) = l.keys.get(&key) {
                let e = &l.entries[seq];
                if !e.owned(&thread, &owner) {
                    return Err(Failure::Missing);
                }
                Ok(Some(e.render(&self.session)))
            } else {
                Ok(None)
            }
        };
        if let Some(v) = existing(&lock(&self.ledger))? {
            return Ok(v);
        }
        let _start = self.starts.lock().await;
        if let Some(v) = existing(&lock(&self.ledger))? {
            return Ok(v);
        }
        let handshake = self.gates(owner.clone(), thread.clone(), None).await?;
        let (generation, guard) = self.core.accounts().storage_lease(&owner).await?;
        if lock(&self.ledger).unsettled_charge {
            return Err(Failure::Conflict);
        }
        let slot = self
            .scan
            .clone()
            .try_acquire_owned()
            .map_err(|_| Failure::Conflict)?;
        let service = self.clone();
        let owner_copy = owner.clone();
        let (_slot, _guard) = blocking(move || {
            let inv = service
                .store()
                .map_err(|_| Failure::Unavailable)?
                .inventory();
            budget(&inv, &service.fs_owner(&owner_copy), 1).map_err(|_| Failure::Conflict)?;
            Ok((slot, guard))
        })
        .await?;
        let provider = lock(&self.provider).clone().ok_or(Failure::Unsupported)?;
        let mut l = lock(&self.ledger);
        if let Some(v) = existing(&l)? {
            return Ok(v);
        }
        l.capacity()?;
        if l.keys.len() >= KEYS || l.unsettled_charge || generation.is_cancelled() {
            return Err(Failure::Conflict);
        }
        let stage = token().map_err(|_| Failure::Internal)?;
        if l.ids.contains_key(&stage) {
            return Err(Failure::Conflict);
        }
        let seq = l.insert(Entry {
            stage,
            key: Some(key),
            thread,
            plugin: owner.plugin_id.clone(),
            birth: owner.created_at,
            generation,
            state: State::Choosing,
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
        });
        Ok(self.launch(
            &mut l,
            seq,
            owner,
            Action::Choose(provider),
            Some(handshake),
        ))
    }
    pub(crate) async fn send(
        self: &Arc<Self>,
        thread: Thread,
        session: String,
        id: String,
        caption: String,
        as_file: bool,
        spoiler: bool,
    ) -> Result<Value> {
        self.session(&session)?;
        if caption.len() > 4096 || caption.chars().count() > 1024 {
            return Err(Failure::Invalid);
        }
        let owner = self.owner(&thread).await?;
        let intent = fs::Intent {
            caption,
            as_file,
            spoiler,
        };
        let existing = |l: &Ledger| -> Result<Option<Value>> {
            let n = Self::sequence(l, &id, &thread, &owner)?;
            let e = &l.entries[&n];
            if let Some(frozen) = &e.intent {
                if frozen != &intent {
                    return Err(Failure::Conflict);
                }
                return Ok(Some(e.render(&self.session)));
            }
            if e.state != State::Ready || e.active.is_some() {
                return Err(Failure::Conflict);
            }
            Ok(None)
        };
        if let Some(v) = existing(&lock(&self.ledger))? {
            return Ok(v);
        }
        let handshake = self
            .gates(owner.clone(), thread.clone(), Some(intent.clone()))
            .await?;
        let mut l = lock(&self.ledger);
        if let Some(v) = existing(&l)? {
            return Ok(v);
        }
        l.capacity()?;
        let n = Self::sequence(&l, &id, &thread, &owner)?;
        let e = l.entries.get_mut(&n).ok_or(Failure::Missing)?;
        e.state = State::Verifying;
        e.intent = Some(intent.clone());
        e.problem = None;
        Ok(self.launch(&mut l, n, owner, Action::Send(intent), Some(handshake)))
    }
    pub(crate) async fn cancel(
        self: &Arc<Self>,
        t: Thread,
        session: String,
        id: String,
    ) -> Result<Value> {
        self.session(&session)?;
        let owner = self.owner(&t).await?;
        let mut l = lock(&self.ledger);
        let n = Self::sequence(&l, &id, &t, &owner)?;
        let e = l.entries.get_mut(&n).ok_or(Failure::Missing)?;
        match e.state {
            State::Choosing | State::Copying | State::Verifying => {
                if let Some(c) = &e.active {
                    c.cancel();
                }
                e.state = State::Cancelling;
                events::notify(&self.changes, &t.account_id);
                Ok(e.render(&self.session))
            }
            State::Ready if e.active.is_none() => {
                l.capacity()?;
                l.entries.get_mut(&n).ok_or(Failure::Missing)?.state = State::Cancelling;
                Ok(self.launch(&mut l, n, owner, Action::Discard, None))
            }
            State::Enqueuing | State::Enqueued | State::AdmissionUnknown => Err(Failure::Conflict),
            _ => Ok(e.render(&self.session)),
        }
    }
    fn launch(
        self: &Arc<Self>,
        l: &mut Ledger,
        n: u64,
        owner: AccountRow,
        action: Action,
        handshake: Option<Arc<ch_protocol::Capabilities>>,
    ) -> Value {
        // Called while admission/close and the four-job limit are locked together.
        let e = l.entries.get_mut(&n).expect("registered ingress receipt");
        let cancel = self.stop.child_token();
        e.active = Some(cancel.clone());
        let j = Arc::new(Job {
            s: self.clone(),
            seq: n,
            owner,
            thread: e.thread.clone(),
            stage: e.stage.clone(),
            cancel,
            generation: e.generation.clone(),
            handshake,
        });
        let value = e.render(&self.session);
        events::notify(&self.changes, &e.thread.account_id);
        l.tasks.push(tokio::spawn(async move {
            let result = AssertUnwindSafe(j.run(action))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| Err(Problem::new("internal", "storage")));
            j.finish(result);
        }));
        value
    }
}
impl Job {
    fn entry(&self) -> Entry {
        lock(&self.s.ledger).entries[&self.seq].clone()
    }
    fn update(&self, f: impl FnOnce(&mut Entry)) {
        if let Some(e) = lock(&self.s.ledger).entries.get_mut(&self.seq) {
            f(e);
        }
        self.s.notify(self.seq);
    }
    fn check(&self, life: &CancellationToken) -> std::result::Result<(), fs::Error> {
        if self.cancel.is_cancelled() || self.generation.is_cancelled() || life.is_cancelled() {
            Err(fs::Error::Cancelled)
        } else {
            Ok(())
        }
    }
    fn effective(&self, intent: Option<&fs::Intent>) -> std::result::Result<u64, fs::Error> {
        let (limit, live) = self
            .s
            .eligible(&self.owner, &self.thread, intent)
            .map_err(|_| fs::Error::TargetChanged)?;
        if !self
            .handshake
            .as_ref()
            .is_some_and(|original| Arc::ptr_eq(original, &live))
        {
            return Err(fs::Error::TargetChanged);
        }
        Ok(limit)
    }
    async fn run(self: &Arc<Self>, action: Action) -> Work<()> {
        let lease = self
            .s
            .core
            .accounts()
            .storage_lease(&self.owner)
            .await
            .map_err(|_| fs::Error::TargetChanged)?;
        let (life, guard) = lease;
        self.check(&life)?;
        let source = if let Action::Choose(provider) = &action {
            let slot = tokio::select! {
                _=self.cancel.cancelled()=>return Err(fs::Error::Cancelled.into()),
                _=life.cancelled()=>return Err(fs::Error::TargetChanged.into()),
                slot=self.s.scan.clone().acquire_owned()=>slot.map_err(|_|fs::Error::Cancelled)?,
            };
            let j = self.clone();
            let life_before = life.clone();
            tokio::task::spawn_blocking(move || -> Work<()> {
                let (_guard, _slot) = (guard, slot);
                j.check(&life_before)?;
                j.effective(None)?;
                budget(&j.s.store()?.inventory(), &j.s.fs_owner(&j.owner), 1)
            })
            .await
            .map_err(|_| fs::Error::Storage)??;
            let (tx, rx) = tokio::sync::oneshot::channel();
            // The callback holds only the channel, never an app, task, fd, or directory lease.
            self.check(&life)?;
            provider.choose_open(Box::new(move |r| {
                let _ = tx.send(r);
            }));
            let answer = tokio::select! {
                biased;
                _=self.cancel.cancelled()=>return Err(fs::Error::Cancelled.into()),
                _=life.cancelled()=>return Err(fs::Error::TargetChanged.into()),
                answer=rx=>answer,
            };
            match answer {
                Ok(Ok(Some(path))) => Some(path),
                Ok(Ok(None)) => {
                    self.update(|e| e.state = State::Cancelled);
                    return Ok(());
                }
                Ok(Err(SaveDialogError::Uncertain)) | Err(_) => {
                    return Err(Problem::new("unavailable", "chooserUncertain"));
                }
                Ok(Err(_)) => return Err(Problem::new("unavailable", "unavailable")),
            }
        } else {
            drop(guard);
            None
        };
        // Acquire the directory lease after the chooser; a once-revoked original generation never revives.
        let (current, guard) = self
            .s
            .core
            .accounts()
            .storage_lease(&self.owner)
            .await
            .map_err(|_| fs::Error::TargetChanged)?;
        self.check(&life)?;
        self.check(&current)?;
        let slot = tokio::select! {
            biased;
            _=self.cancel.cancelled()=>return Err(fs::Error::Cancelled.into()),
            _=life.cancelled()=>return Err(fs::Error::TargetChanged.into()),
            s=self.s.scan.clone().acquire_owned()=>s.map_err(|_|fs::Error::Cancelled)?,
        };
        let j = self.clone();
        tokio::task::spawn_blocking(move || {
            // Detached request futures cannot drop these guards while blocking filesystem work still runs.
            let (_guard, _slot) = (guard, slot);
            j.check(&life)?;
            j.check(&current)?;
            let store = j.s.store()?;
            let a = j.s.fs_owner(&j.owner);
            match action {
                Action::Choose(_) => {
                    j.copy(&store, &a, source.ok_or(fs::Error::UnsafeSource)?, &life)
                }
                Action::Send(i) => j.admit(&store, &a, &i, &life),
                Action::Discard => j.discard(&store, &a, &life),
            }
        })
        .await
        .map_err(|_| fs::Error::Storage)?
    }
    fn copy(
        &self,
        store: &fs::Store,
        owner: &fs::Owner,
        path: PathBuf,
        life: &CancellationToken,
    ) -> Work<()> {
        let limit = self.effective(None)?;
        let source = fs::Source::open(&path, &self.s.private_roots(), limit)?;
        drop(path);
        let size = source.len();
        // The one scan permit covers inventory, reservation, temporary allocation, and settlement.
        // No other ingress allocation can start before the next complete inventory.
        if lock(&self.s.ledger).unsettled_charge {
            return Err(Problem::capacity());
        }
        budget(&store.inventory(), owner, size)?;
        self.check(life)?;
        self.update(|e| {
            if e.state != State::Cancelling {
                e.state = State::Copying;
            }
            e.bytes = Some(size);
            e.copied = Some(0);
        });
        let scope = fs::Scope {
            account: owner.account.clone(),
            plugin: owner.plugin.clone(),
            account_created: owner.account_created,
            chat: self.thread.chat_id.clone(),
            topic: self.thread.topic_id.clone(),
        };
        let staged = store.stage(
            owner,
            &self.stage,
            scope,
            ch_db::now_millis(),
            source,
            limit,
            |n| {
                self.check(life)?;
                if n == size && size > self.effective(None)? {
                    return Err(fs::Error::TooLarge);
                }
                let mut l = lock(&self.s.ledger);
                if let Some(e) = l.entries.get_mut(&self.seq) {
                    e.copied = Some(n);
                }
                l.dirty.insert(self.thread.account_id.clone());
                Ok(())
            },
        );
        if matches!(staged, Err(fs::Error::Unknown)) {
            lock(&self.s.ledger).unsettled_charge = true;
        }
        let stage = staged?;
        self.s.inventory_changed();
        let b = Binding {
            manifest: stage.manifest().clone(),
            path: stage.managed_path(),
        };
        self.update(|e| {
            e.problem = None;
            e.bind(b);
        });
        Ok(())
    }
    fn stage(&self, store: &fs::Store, owner: &fs::Owner) -> Work<fs::Stage> {
        let entry = self.entry();
        let b = entry.bound.ok_or(fs::Error::Integrity)?;
        let stage = store.recover(owner, &self.stage)?;
        if stage.manifest() != &b.manifest || stage.managed_path() != b.path {
            return Err(fs::Error::StageChanged.into());
        }
        Ok(stage)
    }
    fn admit(
        &self,
        store: &fs::Store,
        owner: &fs::Owner,
        i: &fs::Intent,
        life: &CancellationToken,
    ) -> Work<()> {
        let stage = self.stage(store, owner)?;
        let limit = self.effective(Some(i))?;
        let m = stage.manifest();
        if !matches!(m.phase, fs::Phase::Ready)
            || self
                .s
                .outbox
                .get_by_client_id(&m.client_id)
                .map_err(|_| fs::Error::Storage)?
                .is_some()
        {
            return Err(fs::Error::Integrity.into());
        }
        let verified: fs::Verified = stage.verify(limit, |_| self.check(life))?;
        if m.size > self.effective(Some(i))? {
            return Err(fs::Error::TooLarge.into());
        }
        // The same lock as cancel decides which side wins the hand-off.
        {
            let mut l = lock(&self.s.ledger);
            self.check(life)?;
            let e = l.entries.get_mut(&self.seq).ok_or(fs::Error::Integrity)?;
            if e.state != State::Verifying {
                return Err(fs::Error::Cancelled.into());
            }
            e.state = State::Enqueuing;
        }
        self.s.notify(self.seq);
        let mut claim: fs::LiveClaim = verified.claim(i.clone())?;
        self.s.inventory_changed();
        self.update(|e| {
            e.bound = Some(Binding {
                manifest: claim.manifest().clone(),
                path: stage.managed_path(),
            });
            e.outbox_row = Some("unchecked");
        });
        // The live claim is consumed exactly once, even if the reply/receipt update fails.
        if life.is_cancelled() || m.size > self.effective(Some(i))? {
            return Err(fs::Error::TargetChanged.into());
        }
        let checked = claim.checked_path()?;
        let req = request(claim.manifest(), i, &checked)?;
        self.s
            .outbox
            .enqueue_with_id(&self.owner, &m.client_id, req, None)
            .map_err(|_| Problem::unknown())?;
        self.update(|e| {
            e.proven = true;
            e.state = State::Enqueued;
            e.outbox_row = Some("present");
            e.problem = None;
        });
        let consumed = claim.consume()?;
        self.s.inventory_changed();
        self.update(|e| {
            e.bind(Binding {
                manifest: consumed.manifest().clone(),
                path: consumed.managed_path(),
            })
        });
        // consume recorded local proof; return the last matching row observation to this caller.
        self.update(|e| e.outbox_row = Some("present"));
        Ok(())
    }
    fn discard(&self, store: &fs::Store, owner: &fs::Owner, life: &CancellationToken) -> Work<()> {
        let stage = self.stage(store, owner)?;
        let db = self.s.core.open_db().map_err(|_| fs::Error::Storage)?;
        let mut tx = None;
        let mut refused = false;
        let result = stage.discard_ready(|m, path| {
            self.check(life)?;
            self.s
                .current_owner(&self.owner)
                .map_err(|_| fs::Error::TargetChanged)?;
            tx = Some(db.transaction().map_err(|_| fs::Error::Storage)?);
            let safe = unreferenced(&db, m, path)?;
            refused = !safe;
            Ok(safe)
        });
        drop(tx);
        self.s.inventory_changed();
        if matches!(result, Err(fs::Error::Unknown)) {
            lock(&self.s.ledger).unsettled_charge = true;
        }
        if refused {
            return Err(Problem::new("conflict", "unavailable"));
        }
        result?;
        self.update(|e| {
            e.state = State::Cancelled;
            e.problem = None;
            e.bound = None;
            e.intent = None;
        });
        Ok(())
    }
    fn finish(&self, res: Work<()>) {
        self.update(|e| {
            if let Err(mut problem) = res {
                if matches!(e.state, State::Enqueuing | State::AdmissionUnknown) {
                    e.state = State::AdmissionUnknown;
                    e.outbox_row = Some("unchecked");
                    problem.outcome_unknown = true;
                } else if e.proven {
                    e.state = State::Enqueued;
                } else if e.bound.is_some() {
                    if matches!(
                        problem.reason,
                        "sourceChanged" | "stageChanged" | "integrity" | "storage"
                    ) {
                        e.state = State::Failed;
                    } else {
                        e.state = State::Ready;
                    }
                    e.intent = None;
                } else if e.state == State::Cancelling || problem.reason == "interrupted" {
                    e.state = State::Cancelled;
                } else {
                    e.state = State::Failed;
                }
                e.problem = Some(problem);
            }
            e.active = None;
        });
    }
}
