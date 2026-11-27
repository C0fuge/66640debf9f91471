//! History: paging and gap filling against plugins.
//!
//! [`History`] serves a thread from the database first and talks to the plugin only when the
//! stored history runs out or has a hole:
//!
//! - [`History::open`] — the newest stored page, instantly, no plugin call.
//! - [`History::refresh`] — `messages.fetch_recent` (newest page), ingest, gap detection, then
//!   the newest page again. The UI calls it right after `open`.
//! - [`History::older`] / [`History::newer`] — scroll. Stored messages first; at a gap the gap
//!   is filled on demand; at the stored bottom older pages are fetched with the
//!   `history_state.older_cursor` chain until the page is full or the chat start is reached.
//! - [`History::jump_to_date`] — a page around a date (`history.around` when declared).
//! - [`History::fill_gap`] — fill one gap on demand.
//! - [`History::spawn_background`] — low-priority, rate-limited filling of every open gap.
//!
//! Pages are chronological ([`Page::items`], oldest first) and carry `more_older` /
//! `more_newer` flags and [`Item::Gap`] markers where stored history is known to be incomplete.
//!
//! # Gap policy
//!
//! A gap is a `history_gap` row `(after_message_id, before_message_id)`: messages strictly
//! between the two (by `(sent_at, rowid)`) may be missing. They are hidden from pages until the
//! gap closes; the page shows a marker instead.
//!
//! - **Detection** (`refresh`): the newest stored platform message *before* the fetch is
//!   compared with the fresh newest page. If the page does not contain it, is entirely newer
//!   than it, and has a `next_cursor` (more history below), a gap
//!   `(stored newest, page oldest)` is recorded. `jump_to_date` also records the hole between a
//!   fetched `history.around` island and the stored history above it.
//! - **Filling**, one page per step: with the `history.fill_gap` capability, bottom-up —
//!   `history.fill_gap(after, before, cursor)`, `after_message_id` advances to the newest message
//!   received and the cursor is stored as `fill:<cursor>`. Without it, top-down via
//!   `messages.fetch_recent` paging from the page below the gap's upper edge
//!   (`recent:<cursor>`), `before_message_id` moving down. A gap closes when a page reaches the
//!   other edge or comes back without `next_cursor`. A stored cursor of the other mode is ignored.
//! - **On demand**: `older`/`newer`/`jump_to_date` fill up to `on_demand_fill_pages` pages of a
//!   gap they run into; if it is still open the page ends at its marker.
//! - **Background**: one page at a time, `background_interval` apart, skipping chats a
//!   foreground request is working on, accounts in a `FLOOD_WAIT`, plugins that are not running,
//!   and gaps whose last attempt failed (30 s × 2ⁿ, capped at 1 h).
//!
//! # Errors and rate limits
//!
//! `FLOOD_WAIT` blocks that `(account, method)` for `seconds`: nothing is sent before it
//! elapses. A foreground call waits inline when the remaining time is at most
//! `max_inline_flood_wait`, else it fails with [`HistoryError::FloodWait`] (the UI shows a
//! countdown). Every other plugin error is returned as [`HistoryError::Plugin`].
//!
//! # Concurrency
//!
//! All plugin-facing work on one thread (chat or topic) is serialised by a per-thread async
//! lock, and identical concurrent requests (same thread, same window) share one execution and
//! its result. The database is reached through [`SharedIngest`]; the lock is held only for
//! short synchronous sections, never across an `await`.

mod api;
mod store;
mod owner;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use ch_db::Millis;
use ch_db::types::MessageRow;
use ch_protocol::capabilities::Capabilities;
use ch_protocol::methods::{
    HistoryAroundParams, HistoryAroundResult, HistoryFillGapParams, MessagesFetchRecentParams,
    PagedMessagesResult,
};
use ch_protocol::{ErrorCode, Id, Timestamp};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{Notify, OnceCell, Semaphore, OwnedMutexGuard};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub use api::PluginApi;
pub use owner::HistoryReadLease;
use store::{Key, ResolvedGap};

use crate::host::HostError;
use crate::ingest::{Applied, Ingest, IngestError};
use crate::runtime::{AccountStorageObservation, Accounts, ChangeBus, Core};

/// The ingest/database handle the services share (`Db` is `!Sync`).
pub type SharedIngest = Arc<Mutex<Ingest>>;

const FETCH_RECENT: &str = "messages.fetch_recent";
const FILL_GAP: &str = "history.fill_gap";
const AROUND: &str = "history.around";
const CAP_FILL_GAP: &str = "history.fill_gap";
const CAP_JUMP: &str = "history.jump_to_date";
const FILL_PREFIX: &str = "fill:";
const RECENT_PREFIX: &str = "recent:";

/// One thread: a chat, or a topic of a forum chat.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChatKey {
    pub account_id: String,
    pub chat_id: Id,
    /// `None` = the whole chat.
    pub topic_id: Option<Id>,
}

impl ChatKey {
    /// A whole chat.
    pub fn new(account_id: impl Into<String>, chat_id: impl Into<Id>) -> Self {
        Self {
            account_id: account_id.into(),
            chat_id: chat_id.into(),
            topic_id: None,
        }
    }

    /// A topic of this chat.
    pub fn with_topic(mut self, topic_id: impl Into<Id>) -> Self {
        self.topic_id = Some(topic_id.into());
        self
    }

    fn topic(&self) -> Option<&str> {
        self.topic_id.as_deref()
    }

    /// `history_state` / `history_gap` key: `""` for the chat.
    fn topic_key(&self) -> &str {
        self.topic_id.as_deref().unwrap_or("")
    }
}

/// Tuning. The defaults suit the UI.
#[derive(Debug, Clone)]
pub struct HistoryConfig {
    /// `limit` sent to the plugin per page (clamped to 1–200).
    pub page_size: u32,
    /// Plugin pages one `older`/`jump_to_date` call may fetch before returning what it has.
    pub max_fetch_pages: u32,
    /// Gap pages one foreground call may fill before returning a page that ends at the gap.
    pub on_demand_fill_pages: u32,
    /// A foreground call waits out a `FLOOD_WAIT` up to this long; longer ones are returned.
    pub max_inline_flood_wait: Duration,
    /// Pause between two background gap pages (the background rate limit).
    pub background_interval: Duration,
    /// How long the background filler sleeps when there is nothing to do (it is also woken
    /// when a gap is recorded).
    pub background_idle: Duration,
    /// Show tombstones (kept-deleted messages) in pages.
    pub include_deleted: bool,
}

impl Default for HistoryConfig {
    fn default() -> Self {
        Self {
            page_size: 50,
            max_fetch_pages: 8,
            on_demand_fill_pages: 4,
            max_inline_flood_wait: Duration::from_secs(5),
            background_interval: Duration::from_secs(2),
            background_idle: Duration::from_secs(60),
            include_deleted: true,
        }
    }
}

/// Where stored history is incomplete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapMarker {
    /// `history_gap.id`, for [`History::fill_gap`].
    pub gap_id: i64,
    /// The newest message known to precede the hole.
    pub after_message_id: Id,
    /// The oldest message known to follow it (`None` = unknown, up to now).
    pub before_message_id: Option<Id>,
}

/// One row of a page.
#[derive(Debug, Clone, PartialEq)]
pub enum Item {
    Message(Box<MessageRow>),
    Gap(GapMarker),
}

/// A window of a thread, oldest first.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Page {
    pub items: Vec<Item>,
    /// Older history exists: stored, behind a gap, or still on the platform.
    pub more_older: bool,
    /// Newer stored history exists (or a gap sits above the page).
    pub more_newer: bool,
    /// `jump_to_date`: the message to scroll to (the first one at or after the date, else the
    /// newest one before it).
    pub target: Option<Id>,
    owner: Option<AccountStorageObservation>,
}

impl Page {
    /// The exact account generation of this snapshot; never a wire field.
    pub fn owner_observation(&self) -> Option<&AccountStorageObservation> { self.owner.as_ref() }
    /// The messages, oldest first.
    pub fn messages(&self) -> impl Iterator<Item = &MessageRow> {
        self.items.iter().filter_map(|i| match i {
            Item::Message(m) => Some(m.as_ref()),
            Item::Gap(_) => None,
        })
    }

    /// Platform ids of the messages, oldest first.
    pub fn message_ids(&self) -> Vec<&str> {
        self.messages().map(|m| m.message_id.as_str()).collect()
    }

    /// The gap markers on the page.
    pub fn gaps(&self) -> impl Iterator<Item = &GapMarker> {
        self.items.iter().filter_map(|i| match i {
            Item::Gap(g) => Some(g),
            Item::Message(_) => None,
        })
    }
}

/// What [`History::fill_gap`] achieved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GapFill {
    pub closed: bool,
    pub pages: u32,
}

/// History errors. Cloneable so deduplicated callers can all receive one.
#[derive(Debug, Clone, thiserror::Error)]
pub enum HistoryError {
    #[error("unknown account {0:?}")]
    UnknownAccount(String),
    #[error("unknown message {0:?}")]
    UnknownMessage(String),
    #[error("plugin {0:?} is not running")]
    PluginNotRunning(String),
    /// Rate-limited: nothing is sent for `method` before `retry_after` elapses.
    #[error("{method}: rate-limited, retry in {}s", retry_after.as_secs())]
    FloodWait {
        method: String,
        retry_after: Duration,
    },
    /// The plugin (or the host on its behalf) failed the call.
    #[error(transparent)]
    Plugin(Arc<HostError>),
    #[error(transparent)]
    Storage(Arc<ch_db::Error>),
}

impl From<ch_db::Error> for HistoryError {
    fn from(e: ch_db::Error) -> Self {
        Self::Storage(Arc::new(e))
    }
}

impl From<IngestError> for HistoryError {
    fn from(e: IngestError) -> Self {
        match e {
            IngestError::UnknownAccount { account_id }
            | IngestError::PluginMismatch { account_id, .. } => Self::UnknownAccount(account_id),
            IngestError::Db(e) => e.into(),
        }
    }
}

/// Result alias.
pub type Result<T> = std::result::Result<T, HistoryError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Op {
    Refresh(u32),
    Older(Option<Id>, u32),
    Newer(Id, u32),
    Jump(Millis, u32),
}

type Scope = (AccountStorageObservation, ChatKey);
type Inflight = HashMap<(Scope, Op), Arc<OnceCell<Result<Page>>>>;
const DB_JOBS: u32 = 64;

struct Inner {
    ingest: SharedIngest,
    #[cfg(target_os="linux")]
    references:crate::runtime::reference_queue::ReferenceQueue,
    bus: Option<ChangeBus>,
    api: Arc<dyn PluginApi>,
    accounts: Accounts,
    cfg: HistoryConfig,
    chat_locks: Mutex<HashMap<Scope, Weak<tokio::sync::Mutex<()>>>>,
    inflight: Mutex<Inflight>,
    /// `(account_id, method)` → blocked until.
    flood: Mutex<HashMap<(AccountStorageObservation, String), Instant>>,
    /// gap id → (failures, retry not before).
    gap_errors: Mutex<HashMap<(AccountStorageObservation, i64), (u32, Instant)>>,
    wake: Notify,
    stop: CancellationToken,
    closed: Mutex<bool>,
    db_slots: Arc<Semaphore>,
    #[cfg(test)]
    background_settled: Notify,
}

/// The history service. Cheap to clone.
#[derive(Clone)]
pub struct History(Arc<Inner>);

impl std::fmt::Debug for History {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("History")
            .field("cfg", &self.0.cfg)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct Ctx {
    owner: AccountStorageObservation,
    plugin_id: String,
    caps: Option<Arc<Capabilities>>,
    // Actual blocking DB work retains serialization if its async caller disappears.
    serial: Option<Arc<OwnedMutexGuard<()>>>,
}

impl Ctx {
    fn fill_mode(&self) -> bool {
        self.caps.as_ref().is_some_and(|caps| caps.has(CAP_FILL_GAP))
    }
}

/// What lies at a date: a gap, or the part below the stored history.
enum Hole {
    Gap(ResolvedGap),
    Bottom(Option<(Key, String)>),
}

struct Window {
    page: Page,
    full: bool,
    reached_start: bool,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

fn marker(g: &ResolvedGap) -> Item {
    Item::Gap(GapMarker {
        gap_id: g.row.id,
        after_message_id: g.row.after_message_id.clone(),
        before_message_id: g.row.before_message_id.clone(),
    })
}

fn tagged(prefix: &str, c: Option<&str>) -> Option<String> {
    c.map(|c| format!("{prefix}{c}"))
}

fn untag<'a>(prefix: &str, c: &'a Option<String>) -> Option<&'a str> {
    c.as_deref().and_then(|c| c.strip_prefix(prefix))
}

impl History {
    /// A history service over `ingest`, calling plugins through `api`.
    pub fn new(core: &Core, api: Arc<dyn PluginApi>, cfg: HistoryConfig) -> Self {
        Self::build(core, api, cfg, None)
    }

    /// As [`History::new`], publishing committed message changes on the runtime bus.
    pub fn with_bus(
        core: &Core,
        api: Arc<dyn PluginApi>,
        cfg: HistoryConfig,
        bus: ChangeBus,
    ) -> Self {
        Self::build(core, api, cfg, Some(bus))
    }

    /// Use the running core's writer, plugin host and change bus. Ingest policy updates on
    /// the core also apply to subsequent history fetches.
    pub fn from_core(core: &Core, cfg: HistoryConfig) -> Self {
        Self::with_bus(
            core,
            Arc::new(core.host().clone()),
            cfg,
            core.bus().clone(),
        )
    }

    fn build(
        core: &Core,
        api: Arc<dyn PluginApi>,
        mut cfg: HistoryConfig,
        bus: Option<ChangeBus>,
    ) -> Self {
        cfg.page_size = cfg.page_size.clamp(1, 200);
        Self(Arc::new(Inner {
            ingest: core.ingest().shared_ingest(),
            #[cfg(target_os="linux")]
            references:core.ingest().reference_queue(),
            bus,
            api,
            accounts: core.accounts().clone(),
            cfg,
            chat_locks: Mutex::default(),
            inflight: Mutex::default(),
            flood: Mutex::default(),
            gap_errors: Mutex::default(),
            wake: Notify::new(),
            stop: CancellationToken::new(),
            closed: Mutex::new(false),
            db_slots: Arc::new(Semaphore::new(DB_JOBS as usize)),
            #[cfg(test)]
            background_settled: Notify::new(),
        }))
    }

    /// The configuration in use.
    pub fn config(&self) -> &HistoryConfig {
        &self.0.cfg
    }

    // ---- public API ---------------------------------------------------------------------

    /// The newest `limit` stored messages. Database only, never blocks on a plugin.
    pub async fn open(&self, ck: &ChatKey, limit: u32) -> Result<Page> {
        let limit = self.limit(limit);
        let ctx = self.ctx(ck).await?;
        let key = ck.clone();
        let page = self.read(&ctx, move |this, conn| Ok(this.older_window(conn, &key, None, limit)?.page)).await?;
        self.page(&ctx, page)
    }

    /// Fetch the newest page from the plugin, record a gap if the stored history does not reach
    /// it, and return the newest `limit` stored messages.
    pub async fn refresh(&self, ck: &ChatKey, limit: u32) -> Result<Page> {
        let limit = self.limit(limit);
        let ctx = self.ctx(ck).await?;
        self.dedup(&ctx, ck, Op::Refresh(limit), || async {
            let ctx = self.serial(&ctx, ck).await?;
            self.refresh_inner(&ctx, ck).await?;
            let key = ck.clone();
            self.read(&ctx, move |this, conn| Ok(this.older_window(conn, &key, None, limit)?.page)).await
        })
        .await
    }

    /// `limit` messages older than `before_message_id` (`None` = from the newest). Fills a gap
    /// it runs into and fetches older pages from the plugin when the stored history ends.
    pub async fn older(
        &self,
        ck: &ChatKey,
        before_message_id: Option<&str>,
        limit: u32,
    ) -> Result<Page> {
        let limit = self.limit(limit);
        let ctx = self.ctx(ck).await?;
        let op = Op::Older(before_message_id.map(str::to_owned), limit);
        self.dedup(&ctx, ck, op, || async {
            let ctx = self.serial(&ctx, ck).await?;
            let before = match before_message_id {
                Some(id) => Some(self.key_of(&ctx, ck, id).await?),
                None => None,
            };
            let (mut fetched, mut filled) = (0, 0);
            loop {
                let key = ck.clone();
                let w = self.read(&ctx, move |this, conn| this.older_window(conn, &key, before, limit)).await?;
                if let Some(Item::Gap(g)) = w.page.items.first() {
                    if filled >= self.0.cfg.on_demand_fill_pages {
                        return Ok(w.page);
                    }
                    self.fill_page(&ctx, ck, g.gap_id, self.0.cfg.max_inline_flood_wait)
                        .await?;
                    filled += 1;
                    continue;
                }
                if w.full || w.reached_start || fetched >= self.0.cfg.max_fetch_pages {
                    return Ok(w.page);
                }
                self.fetch_older_page(&ctx, ck).await?;
                fetched += 1;
            }
        })
        .await
    }

    /// `limit` messages newer than `after_message_id`, filling a gap it runs into.
    pub async fn newer(&self, ck: &ChatKey, after_message_id: &str, limit: u32) -> Result<Page> {
        let limit = self.limit(limit);
        let ctx = self.ctx(ck).await?;
        let op = Op::Newer(after_message_id.to_owned(), limit);
        self.dedup(&ctx, ck, op, || async {
            let ctx = self.serial(&ctx, ck).await?;
            let after = self.key_of(&ctx, ck, after_message_id).await?;
            let mut filled = 0;
            loop {
                let key = ck.clone();
                let w = self.read(&ctx, move |this, conn| this.newer_window(conn, &key, after, limit)).await?;
                let Some(Item::Gap(g)) = w.page.items.last() else {
                    return Ok(w.page);
                };
                if filled >= self.0.cfg.on_demand_fill_pages {
                    return Ok(w.page);
                }
                self.fill_page(&ctx, ck, g.gap_id, self.0.cfg.max_inline_flood_wait)
                    .await?;
                filled += 1;
            }
        })
        .await
    }

    /// A page of about `limit` messages around `at` (unix ms), [`Page::target`] set. Uses
    /// `history.around` when the plugin declares `history.jump_to_date`; otherwise fills the gap
    /// or pages older history until the date is covered (bounded by `max_fetch_pages`).
    pub async fn jump_to_date(&self, ck: &ChatKey, at: Millis, limit: u32) -> Result<Page> {
        let limit = self.limit(limit);
        let ctx = self.ctx(ck).await?;
        self.dedup(&ctx, ck, Op::Jump(at, limit), || async {
            let ctx = self.serial(&ctx, ck).await?;
            let mut around_done = false;
            for _ in 0..self.0.cfg.max_fetch_pages.max(1) {
                let key = ck.clone();
                let Some(hole) = self.read(&ctx, move |this, conn| this.locate_hole(conn, &key, at)).await? else {
                    break;
                };
                if !around_done && ctx.caps.as_ref().is_some_and(|caps| caps.has(CAP_JUMP)) {
                    self.around_inner(&ctx, ck, at, hole).await?;
                    around_done = true;
                    continue;
                }
                match hole {
                    Hole::Gap(g) => {
                        self.fill_page(&ctx, ck, g.row.id, self.0.cfg.max_inline_flood_wait)
                            .await?;
                    }
                    Hole::Bottom(_) => self.fetch_older_page(&ctx, ck).await?,
                }
            }
            let key = ck.clone();
            self.read(&ctx, move |this, conn| this.around_window(conn, &key, at, limit)).await
        })
        .await
    }

    /// Fill a gap on demand, up to `on_demand_fill_pages` pages.
    pub async fn fill_gap(&self, ck: &ChatKey, gap_id: i64) -> Result<GapFill> {
        let captured = self.ctx(ck).await?;
        let ctx = self.serial(&captured, ck).await?;
        let mut pages = 0;
        while pages < self.0.cfg.on_demand_fill_pages.max(1) {
            let key = ck.clone();
            if self.read(&ctx, move |_this, conn| {
                Ok(
                    store::resolved_gap(conn, gap_id, Self::conv(conn, &key)?, key.topic_key())?
                        .is_none(),
                )
            }).await? {
                return Ok(GapFill {
                    closed: true,
                    pages,
                });
            }
            pages += 1;
            if self
                .fill_page(&ctx, ck, gap_id, self.0.cfg.max_inline_flood_wait)
                .await?
            {
                return Ok(GapFill {
                    closed: true,
                    pages,
                });
            }
        }
        Ok(GapFill {
            closed: false,
            pages,
        })
    }

    /// Run the background gap filler until `cancel` fires.
    pub fn spawn_background(&self, cancel: CancellationToken) -> JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move { this.background_loop(cancel).await })
    }

    /// How long `method` is blocked for `account_id` by a `FLOOD_WAIT`, if at all.
    pub fn flood_wait_remaining(&self, account_id: &str, method: &str) -> Option<Duration> {
        self.prune();
        let until = lock(&self.0.flood).iter().find_map(|((owner, key), until)|
            (owner.owner().account_id == account_id && key == method).then_some(*until))?;
        let now = Instant::now();
        (until > now).then(|| until - now)
    }

    // ---- plumbing -----------------------------------------------------------------------

    fn limit(&self, requested: u32) -> u32 {
        if requested == 0 {
            self.0.cfg.page_size
        } else {
            requested.min(200)
        }
    }

    fn publish(&self, applied: Applied) {
        if let Some(bus) = &self.0.bus {
            bus.publish_local("history", applied);
        }
    }

    async fn dedup<F, Fut>(&self, ctx: &Ctx, ck: &ChatKey, op: Op, f: F) -> Result<Page>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Page>>,
    {
        self.active(ctx)?;
        let key = ((ctx.owner.clone(), ck.clone()), op);
        let cell = lock(&self.0.inflight)
            .entry(key.clone())
            .or_default()
            .clone();
        let res = cell.get_or_init(f).await.clone();
        let mut m = lock(&self.0.inflight);
        if m.get(&key).is_some_and(|c| Arc::ptr_eq(c, &cell)) {
            m.remove(&key);
        }
        drop(m);
        self.page(ctx, res?)
    }

    fn conv(conn: &rusqlite::Connection, ck: &ChatKey) -> Result<Option<i64>> {
        Ok(ch_db::conversation_rowid(
            conn,
            &ck.account_id,
            &ck.chat_id,
        )?)
    }

    async fn key_of(&self, ctx: &Ctx, ck: &ChatKey, message_id: &str) -> Result<Key> {
        let key = ck.clone();
        let id = message_id.to_owned();
        self.read(ctx, move |_this, conn| {
            let conv = Self::conv(conn, &key)?;
            Ok(match conv {
                Some(c) => store::key_of(conn, c, key.topic(), &id)?,
                None => None,
            })
        }).await?
        .ok_or_else(|| HistoryError::UnknownMessage(message_id.to_owned()))
    }

    /// Call a plugin method, honouring and recording `FLOOD_WAIT`.
    async fn call<P: Serialize, R: DeserializeOwned>(
        &self,
        ctx: &Ctx,
        account_id: &str,
        method: &str,
        params: &P,
        inline_wait: Duration,
    ) -> Result<R> {
        self.active(ctx)?;
        if ctx.caps.is_none() { return Err(HistoryError::PluginNotRunning(ctx.plugin_id.clone())); }
        let key = (ctx.owner.clone(), method.to_owned());
        let params = serde_json::to_value(params)
            .map_err(|e| HistoryError::Plugin(Arc::new(HostError::Json(e))))?;
        for attempt in 0.. {
            self.active(ctx)?;
            let until = lock(&self.0.flood).get(&key).copied();
            if let Some(until) = until.filter(|u| *u > Instant::now()) {
                let wait = until.saturating_duration_since(Instant::now());
                if wait > inline_wait || attempt >= 3 {
                    return Err(HistoryError::FloodWait {
                        method: method.to_owned(),
                        retry_after: wait,
                    });
                }
                let revoked = ctx.owner.revocation();
                tokio::select! {
                    biased;
                    _ = self.0.stop.cancelled() => return Err(HistoryError::UnknownAccount(account_id.to_owned())),
                    _ = revoked.cancelled() => return Err(HistoryError::UnknownAccount(account_id.to_owned())),
                    _ = tokio::time::sleep_until(until) => {},
                }
            }
            self.active(ctx)?;
            let revoked = ctx.owner.revocation();
            let response = tokio::select! {
                biased;
                _ = self.0.stop.cancelled() => return Err(HistoryError::UnknownAccount(account_id.to_owned())),
                _ = revoked.cancelled() => return Err(HistoryError::UnknownAccount(account_id.to_owned())),
                result = self.0.api.call(&ctx.plugin_id, method, params.clone()) => result,
            };
            self.active(ctx)?;
            match response {
                Ok(v) => {
                    return serde_json::from_value(v)
                        .map_err(|e| HistoryError::Plugin(Arc::new(HostError::Json(e))));
                }
                Err(HostError::Rpc(e)) if e.code() == ErrorCode::FloodWait => {
                    let secs = e.data.as_ref().and_then(|d| d.seconds).unwrap_or(30).max(1);
                    tracing::info!(account_id, method, secs, "FLOOD_WAIT");
                    lock(&self.0.flood)
                        .insert(key.clone(), Instant::now() + Duration::from_secs(secs));
                }
                Err(e) => return Err(HistoryError::Plugin(Arc::new(e))),
            }
        }
        unreachable!("the loop returns")
    }

    // ---- windows (database only) --------------------------------------------------------

    fn older_window(
        &self,
        conn: &rusqlite::Connection,
        ck: &ChatKey,
        before: Option<Key>,
        limit: u32,
    ) -> Result<Window> {
        let Some(conv) = Self::conv(conn, ck)? else {
            return Ok(Window {
                page: Page {
                    more_older: true,
                    ..Default::default()
                },
                full: false,
                reached_start: false,
            });
        };
        let incl = self.0.cfg.include_deleted;
        let reached_start =
            ch_db::get_history_state(conn, conv, ck.topic_key())?.is_some_and(|s| s.reached_start);
        let anchor = before.unwrap_or(Key(i64::MAX, i64::MAX));
        let blocking = store::resolved_gaps(conn, conv, ck.topic_key())?
            .into_iter()
            .filter(|g| g.lo < anchor)
            .max_by_key(|g| g.hi);
        let floor = blocking.as_ref().map(|g| g.hi);
        let mut rows = store::rows_older(conn, conv, ck.topic(), before, floor, limit + 1, incl)?;
        let has_more = rows.len() > limit as usize;
        let full = limit > 0 && rows.len() >= limit as usize;
        rows.truncate(limit as usize);
        rows.reverse();
        let mut items = Vec::with_capacity(rows.len() + 1);
        if !has_more && let Some(g) = &blocking {
            items.push(marker(g));
        }
        items.extend(rows.into_iter().map(|m| Item::Message(Box::new(m))));
        Ok(Window {
            page: Page {
                items,
                more_older: has_more || blocking.is_some() || !reached_start,
                more_newer: before.is_some(),
                target: None,
                owner: None,
            },
            full,
            reached_start,
        })
    }

    fn newer_window(
        &self,
        conn: &rusqlite::Connection,
        ck: &ChatKey,
        after: Key,
        limit: u32,
    ) -> Result<Window> {
        let Some(conv) = Self::conv(conn, ck)? else {
            return Ok(Window {
                page: Page::default(),
                full: false,
                reached_start: false,
            });
        };
        let incl = self.0.cfg.include_deleted;
        let blocking = store::resolved_gaps(conn, conv, ck.topic_key())?
            .into_iter()
            .filter(|g| g.hi > after)
            .min_by_key(|g| g.lo);
        let ceil = blocking.as_ref().map(|g| g.lo);
        let mut rows = store::rows_newer(conn, conv, ck.topic(), after, ceil, limit + 1, incl)?;
        let has_more = rows.len() > limit as usize;
        let full = limit > 0 && rows.len() >= limit as usize;
        rows.truncate(limit as usize);
        let mut items: Vec<Item> = rows
            .into_iter()
            .map(|m| Item::Message(Box::new(m)))
            .collect();
        if !has_more && let Some(g) = &blocking {
            items.push(marker(g));
        }
        Ok(Window {
            page: Page {
                items,
                more_older: true,
                more_newer: has_more || blocking.is_some(),
                target: None,
                owner: None,
            },
            full,
            reached_start: false,
        })
    }

    fn around_window(
        &self,
        conn: &rusqlite::Connection,
        ck: &ChatKey,
        at: Millis,
        limit: u32,
    ) -> Result<Page> {
        let k = Key::at(at);
        let below = limit / 2;
        let lower = self.older_window(conn, ck, Some(k), below)?;
        let upper = self.newer_window(conn, ck, k, limit - below)?;
        let target = upper
            .page
            .messages()
            .next()
            .or_else(|| lower.page.messages().last())
            .map(|m| m.message_id.clone());
        let mut items = lower.page.items;
        items.extend(upper.page.items);
        Ok(Page {
            items,
            more_older: lower.page.more_older,
            more_newer: upper.page.more_newer,
            target,
            owner: None,
        })
    }

    fn locate_hole(
        &self,
        conn: &rusqlite::Connection,
        ck: &ChatKey,
        at: Millis,
    ) -> Result<Option<Hole>> {
        let Some(conv) = Self::conv(conn, ck)? else {
            return Ok(Some(Hole::Bottom(None)));
        };
        let k = Key::at(at);
        if let Some(g) = store::resolved_gaps(conn, conv, ck.topic_key())?
            .into_iter()
            .find(|g| g.lo < k && k < g.hi)
        {
            return Ok(Some(Hole::Gap(g)));
        }
        let reached_start =
            ch_db::get_history_state(conn, conv, ck.topic_key())?.is_some_and(|s| s.reached_start);
        if reached_start {
            return Ok(None);
        }
        let oldest = store::oldest_platform(conn, conv, ck.topic())?;
        Ok(oldest
            .as_ref()
            .is_none_or(|(o, _)| *o > k)
            .then_some(Hole::Bottom(oldest)))
    }

    // ---- plugin-facing steps (chat lock held) -------------------------------------------

    fn fetch_params(&self, ck: &ChatKey, cursor: Option<&str>) -> MessagesFetchRecentParams {
        MessagesFetchRecentParams {
            account_id: ck.account_id.clone(),
            chat_id: ck.chat_id.clone(),
            topic_id: ck.topic_id.clone(),
            limit: self.0.cfg.page_size,
            cursor: cursor.map(str::to_owned),
        }
    }

    /// Newest page + gap detection.
    async fn refresh_inner(&self, ctx: &Ctx, ck: &ChatKey) -> Result<()> {
        let key = ck.clone();
        let (prior, state) = self.read(ctx, move |_this, conn| {
            Ok(match Self::conv(conn, &key)? {
                Some(c) => (
                    store::newest_platform(conn, c, key.topic())?,
                    ch_db::get_history_state(conn, c, key.topic_key())?,
                ),
                None => (None, None),
            })
        }).await?;
        let res: PagedMessagesResult = self
            .call(
                ctx,
                &ck.account_id,
                FETCH_RECENT,
                &self.fetch_params(ck, None),
                self.0.cfg.max_inline_flood_wait,
            )
            .await?;
        let fill_mode = ctx.fill_mode();
        let scope = ck.clone();
        let plugin = ctx.plugin_id.clone();
        let recorded = self.work(ctx, move |this, ing, owner, stop| {
            let ck = &scope;
            let never_paged = state
                .as_ref()
                .is_none_or(|s| s.older_cursor.is_none() && !s.reached_start);
            let older_arg = match &res.next_cursor {
                None => Some(None),
                Some(c) if never_paged => Some(Some(c.as_str())),
                Some(_) => None,
            };
            let (applied, recorded) = ing.apply_history_page_owned(
                &plugin,
                &ck.account_id,
                &ck.chat_id,
                ck.topic(),
                &res.messages,
                older_arg,
                owner, stop,
                |conn, conv, now| {
                    let (Some((prior_key, prior_id)), Some(next)) = (&prior, &res.next_cursor)
                    else {
                        return Ok(false);
                    };
                    if res.messages.iter().any(|m| &m.id == prior_id) {
                        return Ok(false);
                    }
                    let keys = Self::page_keys(conn, conv, ck.topic(), &res.messages)?;
                    let Some((oldest_key, oldest_id)) = keys.iter().min_by_key(|(key, _)| *key)
                    else {
                        return Ok(false);
                    };
                    // Timestamp ties still have a stable order: use persisted rowids, exactly
                    // as thread paging does. A page of new ids at the old head's time can
                    // have an unfetched hole below it too.
                    if oldest_key <= prior_key {
                        return Ok(false);
                    }
                    ch_db::insert_history_gap(
                        conn,
                        &ch_db::types::HistoryGapRow {
                            conversation_id: conv,
                            topic_id: ck.topic_key().to_owned(),
                            after_message_id: prior_id.clone(),
                            before_message_id: Some(oldest_id.clone()),
                            cursor: (!fill_mode).then(|| format!("{RECENT_PREFIX}{next}")),
                            created_at: now,
                            ..Default::default()
                        },
                    )?;
                    Ok(true)
                },
            )?;
            this.publish(applied);
            Ok(recorded)
        }).await?;
        if recorded {
            self.0.wake.notify_one();
        }
        Ok(())
    }

    /// One page further back along the `older_cursor` chain (the newest page if the chain was
    /// never started).
    async fn fetch_older_page(&self, ctx: &Ctx, ck: &ChatKey) -> Result<()> {
        let key = ck.clone();
        let cursor = self.read(ctx, move |_this, conn| {
            Ok(match Self::conv(conn, &key)? {
                Some(c) => {
                    ch_db::get_history_state(conn, c, key.topic_key())?.and_then(|s| s.older_cursor)
                }
                None => None,
            })
        }).await?;
        let Some(cursor) = cursor else {
            return self.refresh_inner(ctx, ck).await;
        };
        let res: PagedMessagesResult = self
            .call(
                ctx,
                &ck.account_id,
                FETCH_RECENT,
                &self.fetch_params(ck, Some(&cursor)),
                self.0.cfg.max_inline_flood_wait,
            )
            .await?;
        let scope = ck.clone();
        let plugin = ctx.plugin_id.clone();
        self.work(ctx, move |this, ing, owner, stop| {
            let ck = &scope;
            let (applied, ()) = ing.apply_history_page_owned(
                &plugin,
                &ck.account_id,
                &ck.chat_id,
                ck.topic(),
                &res.messages,
                Some(res.next_cursor.as_deref()),
                owner, stop, |_, _, _| Ok(()),
            )?;
            this.publish(applied);
            Ok(())
        }).await
    }

    /// Keys of a page's messages after ingest, with their ids.
    fn page_keys(
        conn: &rusqlite::Connection,
        conv: i64,
        topic: Option<&str>,
        msgs: &[ch_protocol::dto::Message],
    ) -> ch_db::Result<Vec<(Key, String)>> {
        let mut out = Vec::with_capacity(msgs.len());
        for m in msgs {
            if let Some(k) = store::key_of(conn, conv, topic, &m.id)? {
                out.push((k, m.id.clone()));
            }
        }
        Ok(out)
    }

    /// Fill one page of a gap. Returns whether the gap is closed.
    async fn fill_page(
        &self,
        ctx: &Ctx,
        ck: &ChatKey,
        gap_id: i64,
        inline_wait: Duration,
    ) -> Result<bool> {
        let key = ck.clone();
        let Some(g) = self.read(ctx, move |_this, conn| {
            Ok(store::resolved_gap(
                conn,
                gap_id,
                Self::conv(conn, &key)?,
                key.topic_key(),
            )?)
        }).await?
        else {
            return Ok(true);
        };
        let fill_mode = ctx.fill_mode();
        let res: PagedMessagesResult = if fill_mode {
            let p = HistoryFillGapParams {
                account_id: ck.account_id.clone(),
                chat_id: ck.chat_id.clone(),
                topic_id: ck.topic_id.clone(),
                after_message_id: g.row.after_message_id.clone(),
                before_message_id: g.row.before_message_id.clone(),
                limit: self.0.cfg.page_size,
                cursor: untag(FILL_PREFIX, &g.row.cursor).map(str::to_owned),
            };
            self.call(ctx, &ck.account_id, FILL_GAP, &p, inline_wait)
                .await?
        } else {
            let p = self.fetch_params(ck, untag(RECENT_PREFIX, &g.row.cursor));
            self.call(ctx, &ck.account_id, FETCH_RECENT, &p, inline_wait)
                .await?
        };
        let scope = ck.clone();
        let plugin = ctx.plugin_id.clone();
        self.work(ctx, move |this, ing, owner, stop| {
            let ck = &scope;
            let (applied, closed) = ing.apply_history_page_owned(
                &plugin,
                &ck.account_id,
                &ck.chat_id,
                ck.topic(),
                &res.messages,
                None,
                owner, stop,
                |conn, conv, now| {
                    // The request awaited plugin I/O. Recheck ownership inside the write
                    // transaction so removal/replacement of a gap cannot target another row.
                    let Some(current) =
                        store::resolved_gap(conn, gap_id, Some(conv), ck.topic_key())?
                    else {
                        return Ok(true);
                    };
                    if current.row != g.row {
                        return Err(ch_db::Error::Invalid(
                            "history gap changed during fill".into(),
                        )
                        .into());
                    }
                    let g = current;
                    let open_ended = g.row.before_message_id.is_none();
                    let keys = Self::page_keys(conn, conv, ck.topic(), &res.messages)?;
                    let contains = |id: &str| keys.iter().any(|(_, i)| i == id);
                    let closed = if fill_mode {
                        let reached = !open_ended
                            && (g.row.before_message_id.as_deref().is_some_and(contains)
                                || keys.iter().any(|(k, _)| *k >= g.hi));
                        if res.next_cursor.is_none() || reached {
                            true
                        } else {
                            let after = keys
                                .iter()
                                .filter(|(k, _)| *k > g.lo)
                                .max_by_key(|(k, _)| *k)
                                .map_or(g.row.after_message_id.as_str(), |(_, id)| id.as_str());
                            store::update_gap(
                                conn,
                                gap_id,
                                after,
                                g.row.before_message_id.as_deref(),
                                tagged(FILL_PREFIX, res.next_cursor.as_deref()).as_deref(),
                                now,
                            )?;
                            false
                        }
                    } else {
                        let reached = contains(&g.row.after_message_id)
                            || keys.iter().any(|(k, _)| *k <= g.lo);
                        if res.next_cursor.is_none() || reached {
                            true
                        } else {
                            let before = keys
                                .iter()
                                .filter(|(k, _)| *k < g.hi && !open_ended)
                                .min_by_key(|(k, _)| *k)
                                .map(|(_, id)| id.as_str())
                                .or(g.row.before_message_id.as_deref());
                            store::update_gap(
                                conn,
                                gap_id,
                                &g.row.after_message_id,
                                before,
                                tagged(RECENT_PREFIX, res.next_cursor.as_deref()).as_deref(),
                                now,
                            )?;
                            false
                        }
                    };
                    if closed {
                        ch_db::delete_history_gap(conn, gap_id)?;
                        tracing::debug!(gap_id, "history gap closed");
                    }
                    Ok(closed)
                },
            )?;
            this.publish(applied);
            Ok(closed)
        }).await
    }

    /// `history.around` into a hole; records the holes left on either side of the island.
    async fn around_inner(&self, ctx: &Ctx, ck: &ChatKey, at: Millis, hole: Hole) -> Result<()> {
        let p = HistoryAroundParams {
            account_id: ck.account_id.clone(),
            chat_id: ck.chat_id.clone(),
            topic_id: ck.topic_id.clone(),
            at: Timestamp::from_unix_millis(at)
                .unwrap_or_else(|| Timestamp::from_unix_millis(0).expect("epoch")),
            limit: self.0.cfg.page_size,
        };
        let res: HistoryAroundResult = self
            .call(
                ctx,
                &ck.account_id,
                AROUND,
                &p,
                self.0.cfg.max_inline_flood_wait,
            )
            .await?;
        let fill_mode = ctx.fill_mode();
        let scope = ck.clone();
        let plugin = ctx.plugin_id.clone();
        let recorded = self.work(ctx, move |this, ing, owner, stop| {
            let ck = &scope;
            let island_min = res.messages.iter().map(|m| m.sent_at.unix_millis()).min();
            let (older_arg, prev_older) = match &hole {
                Hole::Bottom(oldest) => {
                    let below =
                        island_min.is_some_and(|m| oldest.as_ref().is_none_or(|(o, _)| m < o.0));
                    let prev = match Self::conv(ing.db().conn(), ck)? {
                        Some(c) => ch_db::get_history_state(ing.db().conn(), c, ck.topic_key())?
                            .and_then(|s| s.older_cursor),
                        None => None,
                    };
                    (below.then_some(res.older_cursor.as_deref()), prev)
                }
                Hole::Gap(_) => (None, None),
            };
            let (applied, recorded) = ing.apply_history_page_owned(
                &plugin,
                &ck.account_id,
                &ck.chat_id,
                ck.topic(),
                &res.messages,
                older_arg,
                owner, stop,
                |conn, conv, now| {
                    let current_gap = match &hole {
                        Hole::Gap(g) => Some(
                            store::resolved_gap(conn, g.row.id, Some(conv), ck.topic_key())?
                                .ok_or_else(|| {
                                    ch_db::Error::Invalid(
                                        "history gap changed during date jump".into(),
                                    )
                                })?,
                        ),
                        Hole::Bottom(_) => None,
                    };
                    let current_hole = match &hole {
                        Hole::Gap(original) => {
                            let current = current_gap.expect("gap checked above");
                            if current.row != original.row {
                                return Err(ch_db::Error::Invalid(
                                    "history gap changed during date jump".into(),
                                )
                                .into());
                            }
                            Hole::Gap(current)
                        }
                        Hole::Bottom(oldest) => Hole::Bottom(oldest.clone()),
                    };
                    let keys = Self::page_keys(conn, conv, ck.topic(), &res.messages)?;
                    let (Some(lo_i), Some(hi_i)) = (
                        keys.iter().min_by_key(|(k, _)| *k),
                        keys.iter().max_by_key(|(k, _)| *k),
                    ) else {
                        return Ok(false);
                    };
                    let contains = |id: &str| keys.iter().any(|(_, i)| i == id);
                    let new_gap = |after: &str, before: Option<&str>, cursor: Option<String>| {
                        ch_db::insert_history_gap(
                            conn,
                            &ch_db::types::HistoryGapRow {
                                conversation_id: conv,
                                topic_id: ck.topic_key().to_owned(),
                                after_message_id: after.to_owned(),
                                before_message_id: before.map(str::to_owned),
                                cursor,
                                created_at: now,
                                ..Default::default()
                            },
                        )
                    };
                    let mut recorded = false;
                    match &current_hole {
                        Hole::Gap(g) => {
                            if g.row.before_message_id.is_none() && res.newer_cursor.is_some() {
                                let cursor = if fill_mode {
                                    tagged(FILL_PREFIX, res.newer_cursor.as_deref())
                                } else {
                                    None
                                };
                                new_gap(&hi_i.1, None, cursor)?;
                                recorded = true;
                            }
                            if let Some(b) = g.row.before_message_id.as_deref()
                                && hi_i.0 < g.hi
                                && !contains(b)
                            {
                                let cursor = if fill_mode {
                                    tagged(FILL_PREFIX, res.newer_cursor.as_deref())
                                } else {
                                    g.row
                                        .cursor
                                        .clone()
                                        .filter(|c| c.starts_with(RECENT_PREFIX))
                                };
                                new_gap(&hi_i.1, Some(b), cursor)?;
                                recorded = true;
                            }
                            if lo_i.0 > g.lo && !contains(&g.row.after_message_id) {
                                let cursor = if fill_mode {
                                    g.row.cursor.clone().filter(|c| c.starts_with(FILL_PREFIX))
                                } else {
                                    tagged(RECENT_PREFIX, res.older_cursor.as_deref())
                                };
                                store::update_gap(
                                    conn,
                                    g.row.id,
                                    &g.row.after_message_id,
                                    Some(&lo_i.1),
                                    cursor.as_deref(),
                                    now,
                                )?;
                            } else {
                                ch_db::delete_history_gap(conn, g.row.id)?;
                            }
                        }
                        Hole::Bottom(oldest) => {
                            if oldest.is_none() && res.newer_cursor.is_some() {
                                let cursor = if fill_mode {
                                    tagged(FILL_PREFIX, res.newer_cursor.as_deref())
                                } else {
                                    None
                                };
                                new_gap(&hi_i.1, None, cursor)?;
                                recorded = true;
                            }
                            if let Some((ok, oid)) = oldest
                                && hi_i.0 < *ok
                                && !contains(oid)
                            {
                                let cursor = if fill_mode {
                                    tagged(FILL_PREFIX, res.newer_cursor.as_deref())
                                } else {
                                    tagged(RECENT_PREFIX, prev_older.as_deref())
                                };
                                new_gap(&hi_i.1, Some(oid), cursor)?;
                                recorded = true;
                            }
                        }
                    }
                    Ok(recorded)
                },
            )?;
            this.publish(applied);
            Ok(recorded)
        }).await?;
        if recorded {
            self.0.wake.notify_one();
        }
        Ok(())
    }

    // ---- background ---------------------------------------------------------------------

    async fn background_loop(&self, cancel: CancellationToken) {
        loop {
            tokio::select! {
                _ = self.0.stop.cancelled() => return,
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(self.0.cfg.background_interval) => {}
            }
            let next = self.background_step().await;
            #[cfg(test)]
            self.0.background_settled.notify_one();
            if let Some(next) = next {
                let idle = Instant::now() + self.0.cfg.background_idle;
                let deadline = next.map_or(idle, |n| n.min(idle));
                tokio::select! {
                    _ = self.0.stop.cancelled() => return,
                    _ = cancel.cancelled() => return,
                    _ = self.0.wake.notified() => {}
                    _ = tokio::time::sleep_until(deadline) => {}
                }
            }
        }
    }

    /// Fill one page of some gap. `None` = worked; `Some(t)` = idle, something may be ready at `t`.
    async fn background_step(&self) -> Option<Option<Instant>> {
        self.prune();
        let gaps = match self.global_read(|conn| Ok(store::all_gaps(conn)?)).await {
            Ok(g) => g,
            Err(e) => {
                tracing::warn!(error = %e, "background history: listing gaps failed");
                return Some(None);
            }
        };
        let now = Instant::now();
        let mut earliest: Option<Instant> = None;
        let mut note = |t: Instant| earliest = Some(earliest.map_or(t, |e| e.min(t)));
        for (gap_id, account_id, chat_id, topic) in gaps {
            let ck = ChatKey {
                account_id,
                chat_id,
                topic_id: (!topic.is_empty()).then_some(topic),
            };
            let Ok(mut ctx) = self.ctx(&ck).await else { continue };
            if ctx.caps.is_none() { continue; }
            let error_key = (ctx.owner.clone(), gap_id);
            if let Some(&(_, until)) = lock(&self.0.gap_errors).get(&error_key)
                && until > now
            {
                note(until);
                continue;
            }
            let method = if ctx.fill_mode() {
                FILL_GAP
            } else {
                FETCH_RECENT
            };
            if let Some(&until) =
                lock(&self.0.flood).get(&(ctx.owner.clone(), method.to_owned()))
                && until > now
            {
                note(until);
                continue;
            }
            let Ok(guard) = self.chat_lock(&ctx, &ck).try_lock_owned() else {
                continue;
            };
            ctx.serial = Some(Arc::new(guard));
            match self.fill_page(&ctx, &ck, gap_id, Duration::ZERO).await {
                Ok(_) => {
                    lock(&self.0.gap_errors).remove(&error_key);
                }
                Err(HistoryError::FloodWait { .. }) => {}
                Err(e) => {
                    if self.active(&ctx).is_err() { self.prune(); continue; }
                    let mut errs = lock(&self.0.gap_errors);
                    let n = errs.get(&error_key).map_or(0, |(n, _)| *n);
                    let backoff =
                        Duration::from_secs(30 << n.min(7)).min(Duration::from_secs(3600));
                    tracing::warn!(gap_id, error = %e, ?backoff, "background gap fill failed");
                    errs.insert(error_key, (n + 1, Instant::now() + backoff));
                }
            }
            return None;
        }
        Some(earliest)
    }
}
