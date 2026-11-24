//! History service against a scripted platform (no plugin process).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use ch_core::history::{
    ChatKey, History, HistoryConfig, HistoryError, Item, PluginApi, SharedIngest,
};
use ch_core::host::HostError;
use ch_core::ingest::{Ingest, IngestOptions};
use ch_db::{Db, now_millis};
use ch_protocol::capabilities::Capabilities;
use ch_protocol::dto::Message;
use ch_protocol::{ErrorObject, Event, Timestamp};
use serde_json::{Value, json};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const BASE: i64 = 1_767_225_600_000; // 2026-01-01T00:00:00Z
const MIN: i64 = 60_000;

fn at(i: usize) -> i64 {
    BASE + i as i64 * MIN
}

fn mid(i: usize) -> String {
    format!("m{i:04}")
}

fn message(chat: &str, i: usize) -> Message {
    serde_json::from_value(json!({
        "id": mid(i),
        "chat_id": chat,
        "direction": "in",
        "sent_at": Timestamp::from_unix_millis(at(i)).unwrap(),
        "text": format!("message {i}"),
    }))
    .unwrap()
}

/// A platform with one chat `c1` whose messages are `m0000..` one minute apart. `visible`
/// controls how many exist "now" (the chat grows while the app is offline).
struct Platform {
    caps: Arc<Capabilities>,
    visible: Mutex<usize>,
    calls: Mutex<Vec<(String, Value, Instant)>>,
    floods: Mutex<VecDeque<(String, u64)>>,
    delay: Mutex<Duration>,
}

impl Platform {
    fn new(total: usize, caps: &[&str]) -> Arc<Self> {
        let mut c = Capabilities::new();
        for k in caps {
            c.enable(k);
        }
        Arc::new(Self {
            caps: Arc::new(c),

            visible: Mutex::new(total),
            calls: Mutex::default(),
            floods: Mutex::default(),
            delay: Mutex::new(Duration::ZERO),
        })
    }

    fn calls(&self, method: &str) -> Vec<Value> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, _, _)| m == method)
            .map(|(_, p, _)| p.clone())
            .collect()
    }

    fn call_times(&self, method: &str) -> Vec<Instant> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, _, _)| m == method)
            .map(|(_, _, t)| *t)
            .collect()
    }

    fn flood_next(&self, method: &str, seconds: u64) {
        self.floods
            .lock()
            .unwrap()
            .push_back((method.to_owned(), seconds));
    }

    fn index(&self, id: &str) -> usize {
        id[1..].parse().unwrap()
    }

    fn page(&self, lo: usize, hi: usize, newest_first: bool) -> Vec<Message> {
        let mut v: Vec<Message> = (lo..hi).map(|i| message("c1", i)).collect();
        if newest_first {
            v.reverse();
        }
        v
    }

    fn answer(&self, method: &str, p: &Value) -> Value {
        let n = *self.visible.lock().unwrap();
        let limit = p["limit"].as_u64().unwrap() as usize;
        let cursor = p["cursor"].as_str().map(|c| c.parse::<usize>().unwrap());
        match method {
            // cursor = exclusive upper index; pages go older.
            "messages.fetch_recent" => {
                let hi = cursor.unwrap_or(n).min(n);
                let lo = hi.saturating_sub(limit);
                let mut r = json!({"messages": self.page(lo, hi, true)});
                if lo > 0 {
                    r["next_cursor"] = json!(lo.to_string());
                }
                r
            }
            // cursor = start index; pages go newer, oldest first.
            "history.fill_gap" => {
                let start = cursor
                    .unwrap_or_else(|| self.index(p["after_message_id"].as_str().unwrap()) + 1);
                let end_bound = p["before_message_id"].as_str().map_or(n, |b| self.index(b));
                let end = (start + limit).min(end_bound);
                let mut r = json!({"messages": self.page(start, end, false)});
                if end < end_bound {
                    r["next_cursor"] = json!(end.to_string());
                }
                r
            }
            "history.around" => {
                let ts: Timestamp = serde_json::from_value(p["at"].clone()).unwrap();
                let t = ts.unix_millis();
                let i = (0..n).find(|&i| at(i) >= t).unwrap_or(n);
                let lo = i.saturating_sub(limit / 2);
                let hi = (i + limit / 2).min(n);
                let mut r = json!({"messages": self.page(lo, hi, true)});
                if lo > 0 {
                    r["older_cursor"] = json!(lo.to_string());
                }
                if hi < n {
                    r["newer_cursor"] = json!(hi.to_string());
                }
                r
            }
            other => panic!("unexpected method {other}"),
        }
    }
}

#[async_trait]
impl PluginApi for Platform {
    fn capabilities(&self, plugin_id: &str) -> Option<Arc<Capabilities>> {
        assert_eq!(plugin_id, "fake");
        Some(self.caps.clone())
    }

    async fn call(&self, _plugin: &str, method: &str, params: Value) -> Result<Value, HostError> {
        // Respect capabilities like the host does.
        let cap = match method {
            "history.fill_gap" => Some("history.fill_gap"),
            "history.around" => Some("history.jump_to_date"),
            _ => None,
        };
        if let Some(c) = cap {
            assert!(self.caps.has(c), "{method} called without capability {c}");
        }
        self.calls
            .lock()
            .unwrap()
            .push((method.to_owned(), params.clone(), Instant::now()));
        let delay = *self.delay.lock().unwrap();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let flood = {
            let mut f = self.floods.lock().unwrap();
            match f.front() {
                Some((m, _)) if m == method => f.pop_front().map(|(_, s)| s),
                _ => None,
            }
        };
        if let Some(s) = flood {
            return Err(HostError::rpc(ErrorObject::flood_wait(s)));
        }
        Ok(self.answer(method, &params))
    }
}

fn shared_ingest() -> SharedIngest {
    let db = Db::open_in_memory().unwrap();
    ch_db::upsert_plugin(
        db.conn(),
        &ch_db::PluginRow {
            plugin_id: "fake".into(),
            name: "fake".into(),
            version: "0.1.0".into(),
            api_version: "1.0.0".into(),
            install_dir: None,
            brand_color: None,
            icon_path: None,
            manifest_json: None,
            enabled: true,
            installed: true,
            first_seen_at: now_millis(),
            last_seen_at: now_millis(),
        },
    )
    .unwrap();
    ch_db::create_account(db.conn(), "acc", "fake", None, &Default::default()).unwrap();
    Arc::new(Mutex::new(Ingest::new(db, IngestOptions::default())))
}

fn live(ing: &SharedIngest, range: std::ops::Range<usize>) {
    let msgs: Vec<Message> = range.map(|i| message("c1", i)).collect();
    let ev = Event::from_params(
        "messages.upsert",
        json!({"account_id": "acc", "messages": msgs}),
    )
    .unwrap()
    .unwrap();
    ing.lock().unwrap().apply("fake", ev).unwrap();
}

fn cfg(page: u32) -> HistoryConfig {
    HistoryConfig {
        page_size: page,
        max_fetch_pages: 20,
        on_demand_fill_pages: 20,
        ..Default::default()
    }
}

fn ids(range: std::ops::Range<usize>) -> Vec<String> {
    range.map(mid).collect()
}

fn page_ids(p: &ch_core::history::Page) -> Vec<String> {
    p.message_ids().into_iter().map(str::to_owned).collect()
}

fn gap_count(ing: &SharedIngest) -> i64 {
    ing.lock()
        .unwrap()
        .db()
        .conn()
        .query_row("SELECT COUNT(*) FROM history_gap", [], |r| r.get(0))
        .unwrap()
}

fn chat() -> ChatKey {
    ChatKey::new("acc", "c1")
}

#[tokio::test]
async fn open_serves_the_database_first_then_refresh_fetches() {
    let ing = shared_ingest();
    live(&ing, 97..100);
    let p = Platform::new(100, &[]);
    let h = History::new(ing.clone(), p.clone(), cfg(50));

    let page = h.open(&chat(), 20).unwrap();
    assert_eq!(page_ids(&page), ids(97..100));
    assert!(page.more_older, "start not reached yet");
    assert!(
        p.calls("messages.fetch_recent").is_empty(),
        "open never calls the plugin"
    );

    let page = h.refresh(&chat(), 20).await.unwrap();
    assert_eq!(page_ids(&page), ids(80..100));
    assert!(page.more_older && !page.more_newer);
    assert_eq!(p.calls("messages.fetch_recent").len(), 1);
    assert!(p.calls("messages.fetch_recent")[0].get("cursor").is_none());
    assert_eq!(
        gap_count(&ing),
        0,
        "the fresh page contains the stored newest"
    );
}

#[tokio::test]
async fn page_older_until_the_start() {
    let ing = shared_ingest();
    let p = Platform::new(120, &[]);
    let h = History::new(ing.clone(), p.clone(), cfg(50));
    let mut page = h.refresh(&chat(), 30).await.unwrap();
    let mut all = page_ids(&page);
    let mut rounds = 0;
    while page.more_older {
        let oldest = all[0].clone();
        page = h.older(&chat(), Some(&oldest), 30).await.unwrap();
        assert!(page.more_newer);
        let mut got = page_ids(&page);
        got.extend(all);
        all = got;
        rounds += 1;
        assert!(rounds < 10);
    }
    assert_eq!(
        all,
        ids(0..120),
        "contiguous, no duplicates, down to the first message"
    );
    // 1 newest page + 2 older pages (50, 50, 20); the last one has no next_cursor.
    let calls = p.calls("messages.fetch_recent");
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[1]["cursor"], "70");
    assert_eq!(calls[2]["cursor"], "20");
    // Once the start is reached, nothing more is fetched.
    let again = h.older(&chat(), Some("m0000"), 30).await.unwrap();
    assert!(again.items.is_empty() && !again.more_older);
    assert_eq!(p.calls("messages.fetch_recent").len(), 3);
}

#[tokio::test]
async fn gap_after_offline_is_detected_and_filled_with_a_multi_page_cursor() {
    let ing = shared_ingest();
    let p = Platform::new(200, &["history.fill_gap"]);
    *p.visible.lock().unwrap() = 60;
    let h = History::new(ing.clone(), p.clone(), cfg(20));
    h.refresh(&chat(), 20).await.unwrap();
    // Offline while 140 messages arrive.
    *p.visible.lock().unwrap() = 200;
    let page = h.refresh(&chat(), 100).await.unwrap();
    assert_eq!(gap_count(&ing), 1);
    let gap = page
        .gaps()
        .next()
        .expect("the newest page ends at a gap marker")
        .clone();
    assert!(
        matches!(page.items[0], Item::Gap(_)),
        "marker at the old end"
    );
    assert_eq!(gap.after_message_id, "m0059");
    assert_eq!(gap.before_message_id.as_deref(), Some("m0180"));
    assert_eq!(page_ids(&page), ids(180..200));
    assert!(page.more_older);

    // The stored messages below the gap are hidden from the newest window, but the window
    // below it is reachable once the gap closes. Fill on demand: 120 hidden at 20 per page.
    let fill = h.fill_gap(&chat(), gap.gap_id).await.unwrap();
    assert!(fill.closed);
    assert_eq!(fill.pages, 6);
    let calls = p.calls("history.fill_gap");
    assert_eq!(calls.len(), 6);
    assert!(calls[0].get("cursor").is_none());
    assert_eq!(calls[0]["after_message_id"], "m0059");
    assert_eq!(calls[0]["before_message_id"], "m0180");
    assert_eq!(calls[1]["cursor"], "80");
    assert_eq!(
        calls[1]["after_message_id"], "m0079",
        "the lower edge advances"
    );
    assert_eq!(calls[5]["cursor"], "160");
    assert_eq!(gap_count(&ing), 0);

    let page = h.open(&chat(), 200).unwrap();
    assert_eq!(page_ids(&page), ids(40..200), "contiguous after the fill");
    assert_eq!(page.gaps().count(), 0);
}

#[tokio::test]
async fn scrolling_into_a_gap_fills_it_top_down_without_fill_gap() {
    let ing = shared_ingest();
    let p = Platform::new(150, &[]);
    *p.visible.lock().unwrap() = 30;
    let h = History::new(ing.clone(), p.clone(), cfg(20));
    h.refresh(&chat(), 20).await.unwrap();
    *p.visible.lock().unwrap() = 150;
    let page = h.refresh(&chat(), 20).await.unwrap();
    assert_eq!(page_ids(&page), ids(130..150));
    assert_eq!(gap_count(&ing), 1);

    // Scroll up past the fresh page: the gap is filled via fetch_recent paging (cursor tagged
    // `recent:`), never history.fill_gap.
    let mut all = page_ids(&page);
    let mut page = page;
    while page.more_older {
        page = h.older(&chat(), Some(&all[0].clone()), 25).await.unwrap();
        assert_eq!(page.gaps().count(), 0, "filled on demand");
        let mut got = page_ids(&page);
        got.extend(all);
        all = got;
    }
    assert_eq!(all, ids(0..150));
    assert!(p.calls("history.fill_gap").is_empty());
    assert_eq!(gap_count(&ing), 0);
    let cursors: Vec<Value> = p
        .calls("messages.fetch_recent")
        .iter()
        .map(|c| c.get("cursor").cloned().unwrap_or(Value::Null))
        .collect();
    // refresh, refresh, then the gap from "130" down to the stored edge (m0029),
    // then the older chain ("10" from the first refresh).
    assert_eq!(
        cursors[..4],
        [Value::Null, Value::Null, json!("130"), json!("110")]
    );
    assert!(cursors.contains(&json!("10")));
}

#[tokio::test]
async fn scrolling_newer_at_an_exact_gap_edge_respects_the_fill_budget() {
    let ing = shared_ingest();
    let p = Platform::new(30, &[]);
    let h = History::new(
        ing.clone(),
        p.clone(),
        HistoryConfig {
            on_demand_fill_pages: 1,
            ..cfg(20)
        },
    );
    h.refresh(&chat(), 20).await.unwrap(); // m0010..m0029
    *p.visible.lock().unwrap() = 70;
    h.refresh(&chat(), 20).await.unwrap(); // m0050..m0069

    let page = h.newer(&chat(), "m0010", 18).await.unwrap();
    assert_eq!(page_ids(&page), ids(11..29));
    assert_eq!(page.gaps().count(), 0);
    assert_eq!(p.calls("messages.fetch_recent").len(), 2);

    // Exactly 19 stored messages reach the marker. One top-down fetch per call
    // moves the upper edge to m0030, then overlaps the stored lower edge m0029.
    let page = h.newer(&chat(), "m0010", 19).await.unwrap();
    assert_eq!(page_ids(&page), ids(11..30));
    let gap = page.gaps().next().unwrap();
    assert_eq!(gap.before_message_id.as_deref(), Some("m0030"));
    assert_eq!(p.calls("messages.fetch_recent").len(), 3);

    let page = h.newer(&chat(), "m0010", 19).await.unwrap();
    assert_eq!(page_ids(&page), ids(11..30));
    assert_eq!(page.gaps().count(), 0);
    assert_eq!(gap_count(&ing), 0);
    assert_eq!(p.calls("messages.fetch_recent").len(), 4);
}

#[tokio::test]
async fn jump_to_date_with_history_around_records_the_hole_above_the_island() {
    let ing = shared_ingest();
    let p = Platform::new(300, &["history.jump_to_date", "history.fill_gap"]);
    let h = History::new(ing.clone(), p.clone(), cfg(20));
    h.refresh(&chat(), 20).await.unwrap(); // m0280..m0299 stored

    let page = h.jump_to_date(&chat(), at(100), 10).await.unwrap();
    assert_eq!(page.target.as_deref(), Some("m0100"));
    assert_eq!(page_ids(&page), ids(95..105));
    assert_eq!(p.calls("history.around").len(), 1);
    assert_eq!(
        gap_count(&ing),
        1,
        "between the island and the stored history"
    );

    // Scrolling down from the island fills the hole bottom-up from the around page's
    // newer_cursor.
    let page = h.newer(&chat(), "m0109", 200).await.unwrap();
    assert_eq!(page_ids(&page), ids(110..300));
    assert!(!page.more_newer);
    let fills = p.calls("history.fill_gap");
    assert_eq!(fills[0]["cursor"], "110");
    assert_eq!(gap_count(&ing), 0);

    // Older than the island continues from the around page's older_cursor.
    let page = h.older(&chat(), Some("m0090"), 5).await.unwrap();
    assert_eq!(page_ids(&page), ids(85..90));
    assert_eq!(
        p.calls("messages.fetch_recent").last().unwrap()["cursor"],
        "90"
    );

    // A date the stored history covers is served without the plugin.
    let n = p.calls.lock().unwrap().len();
    let page = h.jump_to_date(&chat(), at(200) + 1, 4).await.unwrap();
    assert_eq!(page_ids(&page), ids(199..203));
    assert_eq!(page.target.as_deref(), Some("m0201"));
    assert_eq!(p.calls.lock().unwrap().len(), n);
}

#[tokio::test]
async fn jump_to_date_without_capability_pages_older() {
    let ing = shared_ingest();
    let p = Platform::new(100, &[]);
    let h = History::new(ing.clone(), p.clone(), cfg(20));
    let page = h.jump_to_date(&chat(), at(30), 6).await.unwrap();
    assert_eq!(page.target.as_deref(), Some("m0030"));
    assert_eq!(page_ids(&page), ids(27..33));
    assert!(p.calls("history.around").is_empty());
    // newest page, then cursors 80, 60, 40.
    assert_eq!(p.calls("messages.fetch_recent").len(), 4);
}

#[tokio::test(start_paused = true)]
async fn concurrent_identical_requests_share_one_call() {
    let ing = shared_ingest();
    let p = Platform::new(100, &[]);
    *p.delay.lock().unwrap() = Duration::from_secs(1);
    let h = History::new(ing.clone(), p.clone(), cfg(20));
    let ck = chat();
    let (a, b, c) = tokio::join!(h.refresh(&ck, 20), h.refresh(&ck, 20), h.refresh(&ck, 20));
    assert_eq!(a.unwrap(), b.unwrap());
    assert!(c.is_ok());
    assert_eq!(p.calls("messages.fetch_recent").len(), 1);

    let (a, b) = tokio::join!(
        h.older(&ck, Some("m0080"), 15),
        h.older(&ck, Some("m0080"), 15)
    );
    assert_eq!(page_ids(&a.unwrap()), ids(65..80));
    assert_eq!(page_ids(&b.unwrap()), ids(65..80));
    assert_eq!(p.calls("messages.fetch_recent").len(), 2);

    // A later identical request is not served from a stale result.
    h.refresh(&chat(), 20).await.unwrap();
    assert_eq!(p.calls("messages.fetch_recent").len(), 3);
}

#[tokio::test(start_paused = true)]
async fn flood_wait_backs_off_for_the_given_seconds() {
    let ing = shared_ingest();
    let p = Platform::new(100, &[]);
    let h = History::new(ing.clone(), p.clone(), cfg(20));

    p.flood_next("messages.fetch_recent", 30);
    let err = h.refresh(&chat(), 20).await.unwrap_err();
    let HistoryError::FloodWait {
        method,
        retry_after,
    } = err
    else {
        panic!("{err:?}")
    };
    assert_eq!(method, "messages.fetch_recent");
    assert!(retry_after > Duration::from_secs(29) && retry_after <= Duration::from_secs(30));
    assert_eq!(p.calls("messages.fetch_recent").len(), 1);

    // Nothing is sent before it elapses.
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(matches!(
        h.refresh(&chat(), 20).await,
        Err(HistoryError::FloodWait { .. })
    ));
    assert_eq!(p.calls("messages.fetch_recent").len(), 1);
    let left = h
        .flood_wait_remaining("acc", "messages.fetch_recent")
        .unwrap();
    assert!(left <= Duration::from_secs(20));

    tokio::time::advance(Duration::from_secs(21)).await;
    h.refresh(&chat(), 20).await.unwrap();
    assert_eq!(p.calls("messages.fetch_recent").len(), 2);

    // A short wait is absorbed inline.
    p.flood_next("messages.fetch_recent", 3);
    let t0 = Instant::now();
    h.older(&chat(), Some("m0080"), 30).await.unwrap();
    assert!(t0.elapsed() >= Duration::from_secs(3));
    let times = p.call_times("messages.fetch_recent");
    assert_eq!(times.len(), 5);
    assert!(times[3] - times[2] >= Duration::from_secs(3));
}

#[tokio::test(start_paused = true)]
async fn background_filler_is_rate_limited_and_waits_out_flood() {
    let ing = shared_ingest();
    let p = Platform::new(200, &["history.fill_gap"]);
    *p.visible.lock().unwrap() = 50;
    let config = HistoryConfig {
        background_interval: Duration::from_secs(2),
        ..cfg(20)
    };
    let h = History::new(ing.clone(), p.clone(), config);
    h.refresh(&chat(), 20).await.unwrap();
    *p.visible.lock().unwrap() = 200;
    h.refresh(&chat(), 20).await.unwrap();
    assert_eq!(gap_count(&ing), 1); // m0050..m0179 hidden: 130 = 7 pages of 20

    p.flood_next("history.fill_gap", 15);
    let cancel = CancellationToken::new();
    let task = h.spawn_background(cancel.clone());
    tokio::time::sleep(Duration::from_secs(120)).await;
    assert_eq!(gap_count(&ing), 0, "background closed the gap");
    let times = p.call_times("history.fill_gap");
    assert_eq!(times.len(), 8, "one flooded attempt + 7 pages");
    assert!(
        times[1] - times[0] >= Duration::from_secs(15),
        "waited out FLOOD_WAIT"
    );
    for w in times[1..].windows(2) {
        assert!(w[1] - w[0] >= Duration::from_secs(2), "rate limit");
    }
    let page = h.open(&chat(), 300).unwrap();
    assert_eq!(page_ids(&page), ids(30..200));
    cancel.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn unknown_account_and_message_are_errors() {
    let ing = shared_ingest();
    let p = Platform::new(10, &[]);
    let h = History::new(ing, p, cfg(20));
    assert!(matches!(
        h.refresh(&ChatKey::new("nope", "c1"), 10).await,
        Err(HistoryError::UnknownAccount(_))
    ));
    assert!(matches!(
        h.older(&chat(), Some("m9999"), 10).await,
        Err(HistoryError::UnknownMessage(_))
    ));
}

#[tokio::test]
async fn a_gap_cannot_be_filled_through_another_chat_or_topic() {
    let ingest = shared_ingest();
    live(&ingest, 0..4);
    let gap_id = {
        let guard = ingest.lock().unwrap();
        let conn = guard.db().conn();
        let conversation_id = ch_db::conversation_rowid(conn, "acc", "c1")
            .unwrap()
            .unwrap();
        ch_db::insert_history_gap(
            conn,
            &ch_db::HistoryGapRow {
                conversation_id,
                after_message_id: mid(0),
                before_message_id: Some(mid(3)),
                ..Default::default()
            },
        )
        .unwrap()
    };
    let platform = Platform::new(4, &["history.fill_gap"]);
    let history = History::new(ingest.clone(), platform.clone(), cfg(2));
    for wrong in [
        ChatKey::new("acc", "other"),
        chat().with_topic("other-topic"),
    ] {
        assert!(history.fill_gap(&wrong, gap_id).await.is_err());
    }
    assert!(platform.calls("history.fill_gap").is_empty());
    assert_eq!(gap_count(&ingest), 1);
}

#[tokio::test]
async fn service_changes_are_published_after_message_commit() {
    let ingest = shared_ingest();
    let platform = Platform::new(3, &[]);
    let bus = ch_core::runtime::ChangeBus::default();
    let mut changes = bus.subscribe();
    let history = History::with_bus(ingest.clone(), platform, cfg(3), bus);
    history.refresh(&chat(), 3).await.unwrap();
    let change = changes.try_recv().unwrap();
    assert!(matches!(
        change,
        ch_core::runtime::CoreChange::Local {
            service: "history",
            ..
        }
    ));
    assert_eq!(page_ids(&history.open(&chat(), 0).unwrap()), ids(0..3));
    assert_eq!(
        history.open(&chat(), u32::MAX).unwrap().messages().count(),
        3
    );
}

#[tokio::test]
async fn scrolling_returns_an_exactly_full_stored_window_without_plugin_io() {
    let ingest = shared_ingest();
    live(&ingest, 0..3);
    let platform = Platform::new(100, &[]);
    let history = History::new(ingest, platform.clone(), cfg(20));
    assert_eq!(
        page_ids(&history.older(&chat(), None, 3).await.unwrap()),
        ids(0..3)
    );
    assert!(platform.calls("messages.fetch_recent").is_empty());
}

struct FixedPage(Value);

#[async_trait]
impl PluginApi for FixedPage {
    fn capabilities(&self, _: &str) -> Option<Arc<Capabilities>> {
        Some(Arc::new(Capabilities::new()))
    }
    async fn call(&self, _: &str, method: &str, _: Value) -> Result<Value, HostError> {
        assert_eq!(method, "messages.fetch_recent");
        Ok(self.0.clone())
    }
}

#[test]
fn history_pages_validate_every_effective_message_before_writing() {
    let ingest = shared_ingest();
    let guard = ingest.lock().unwrap();
    let ing = &*guard;
    let conn = ing.db().conn();
    let valid = message("c1", 10);
    let foreign = message("another-chat", 11);
    assert!(
        ing.apply_history_page(
            "fake",
            "acc",
            "c1",
            Some("topic-a"),
            &[valid.clone(), foreign],
            Some(Some("next"))
        )
        .is_err()
    );
    assert!(
        ch_db::conversation_rowid(conn, "acc", "c1")
            .unwrap()
            .is_none()
    );
    assert!(
        ch_db::conversation_rowid(conn, "acc", "another-chat")
            .unwrap()
            .is_none()
    );

    // Whole-chat pages can carry messages from all topics.
    let mut sibling = message("c1", 20);
    sibling.topic_id = Some("topic-b".into());
    ing.apply_history_page("fake", "acc", "c1", None, &[sibling.clone()], None)
        .unwrap();
    let conv = ch_db::conversation_rowid(conn, "acc", "c1")
        .unwrap()
        .unwrap();
    sibling.topic_id = None; // absence must retain the known sibling topic
    assert!(
        ing.apply_history_page(
            "fake",
            "acc",
            "c1",
            Some("topic-a"),
            &[valid.clone(), sibling],
            Some(Some("bad-cursor"))
        )
        .is_err()
    );
    assert!(ch_db::get_message(conn, conv, &valid.id).unwrap().is_none());
    assert!(
        ch_db::get_history_state(conn, conv, "topic-a")
            .unwrap()
            .is_none()
    );

    let mut explicit_foreign = message("c1", 21);
    explicit_foreign.topic_id = Some("topic-b".into());
    assert!(
        ing.apply_history_page(
            "fake",
            "acc",
            "c1",
            Some("topic-a"),
            &[valid.clone(), explicit_foreign],
            None
        )
        .is_err()
    );

    // New/unknown messages inherit topic context. Later omissions keep their merged fields.
    ing.apply_history_page(
        "fake",
        "acc",
        "c1",
        Some("topic-a"),
        std::slice::from_ref(&valid),
        None,
    )
        .unwrap();
    let mut partial = valid.clone();
    partial.text = None;
    ing.apply_history_page("fake", "acc", "c1", Some("topic-a"), &[partial], None)
        .unwrap();
    let row = ch_db::get_message(conn, conv, &valid.id).unwrap().unwrap();
    assert_eq!(row.topic_id.as_deref(), Some("topic-a"));
    assert_eq!(row.text.as_deref(), Some("message 10"));
}

#[tokio::test]
async fn sibling_topic_anchors_and_foreign_corrupt_gaps_are_rejected_without_mutation() {
    let ingest = shared_ingest();
    let gap = {
        let ing = ingest.lock().unwrap();
        let mut m = message("c1", 0);
        m.topic_id = Some("topic-b".into());
        ing.apply_history_page("fake", "acc", "c1", None, &[m], None)
            .unwrap();
        let conn = ing.db().conn();
        let conv = ch_db::conversation_rowid(conn, "acc", "c1")
            .unwrap()
            .unwrap();
        ch_db::insert_history_gap(
            conn,
            &ch_db::HistoryGapRow {
                conversation_id: conv,
                topic_id: "topic-b".into(),
                after_message_id: "not-stored".into(),
                ..Default::default()
            },
        )
        .unwrap()
    };
    let platform = Platform::new(50, &["history.fill_gap"]);
    let history = History::new(ingest.clone(), platform.clone(), cfg(10));
    let topic_a = chat().with_topic("topic-a");
    assert!(matches!(
        history.older(&topic_a, Some(&mid(0)), 10).await,
        Err(HistoryError::UnknownMessage(_))
    ));
    assert!(matches!(
        history.newer(&topic_a, &mid(0), 10).await,
        Err(HistoryError::UnknownMessage(_))
    ));
    assert!(history.fill_gap(&topic_a, gap).await.is_err());
    let remaining: i64 = ingest
        .lock()
        .unwrap()
        .db()
        .conn()
        .query_row("SELECT count(*) FROM history_gap WHERE id=?1", [gap], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(
        remaining, 1,
        "foreign broken gaps must not be repaired/deleted by lookup"
    );
    assert!(platform.calls("history.fill_gap").is_empty());
    assert!(platform.calls("messages.fetch_recent").is_empty());
}

#[tokio::test]
async fn refresh_detects_missing_history_when_new_head_has_the_same_timestamp() {
    let ingest = shared_ingest();
    live(&ingest, 0..1);
    let mut first = message("c1", 2);
    let mut second = message("c1", 1);
    first.sent_at = Timestamp::from_unix_millis(BASE).unwrap();
    second.sent_at = first.sent_at;
    let api = Arc::new(FixedPage(
        json!({"messages":[first,second],"next_cursor":"older"}),
    ));
    let history = History::new(ingest.clone(), api, cfg(10));
    let page = history.refresh(&chat(), 10).await.unwrap();
    let gap = page.gaps().next().unwrap();
    assert_eq!(gap.after_message_id, mid(0));
    assert_eq!(
        gap.before_message_id,
        Some(mid(2)),
        "persisted (timestamp,rowid) order is authoritative"
    );
    assert_eq!(gap_count(&ingest), 1);
}

#[tokio::test]
async fn failed_gap_insertion_rolls_back_messages_cursor_and_notifications() {
    let ingest = shared_ingest();
    live(&ingest, 0..1);
    ingest
        .lock()
        .unwrap()
        .db()
        .conn()
        .execute_batch(
            "CREATE TEMP TRIGGER reject_gap BEFORE INSERT ON history_gap
         BEGIN SELECT RAISE(ABORT,'injected gap failure'); END;",
        )
        .unwrap();
    let bus = ch_core::runtime::ChangeBus::default();
    let mut changes = bus.subscribe();
    let history = History::with_bus(ingest.clone(), Platform::new(100, &[]), cfg(10), bus);
    assert!(history.refresh(&chat(), 10).await.is_err());
    {
        let ing = ingest.lock().unwrap();
        let conn = ing.db().conn();
        let conv = ch_db::conversation_rowid(conn, "acc", "c1")
            .unwrap()
            .unwrap();
        assert!(ch_db::get_message(conn, conv, &mid(99)).unwrap().is_none());
        assert!(ch_db::get_history_state(conn, conv, "").unwrap().is_none());
        assert_eq!(
            ch_db::get_conversation(conn, "acc", "c1")
                .unwrap()
                .unwrap()
                .last_message_at,
            Some(at(0))
        );
        conn.execute_batch("DROP TRIGGER reject_gap;").unwrap();
    }
    assert!(changes.try_recv().is_err());
    assert_eq!(gap_count(&ingest), 0);
    history.refresh(&chat(), 10).await.unwrap();
    assert_eq!(
        gap_count(&ingest),
        1,
        "a retry must still detect the original hole"
    );
}

#[tokio::test]
async fn failed_gap_close_rolls_back_the_fetched_fill_page() {
    let ingest = shared_ingest();
    live(&ingest, 0..1);
    let history = History::new(
        ingest.clone(),
        Platform::new(20, &["history.fill_gap"]),
        cfg(10),
    );
    let page = history.refresh(&chat(), 10).await.unwrap();
    let gap = page.gaps().next().unwrap().gap_id;
    ingest
        .lock()
        .unwrap()
        .db()
        .conn()
        .execute_batch(
            "CREATE TEMP TRIGGER reject_close BEFORE DELETE ON history_gap
         BEGIN SELECT RAISE(ABORT,'injected close failure'); END;",
        )
        .unwrap();
    assert!(history.fill_gap(&chat(), gap).await.is_err());
    {
        let ing = ingest.lock().unwrap();
        let conn = ing.db().conn();
        let conv = ch_db::conversation_rowid(conn, "acc", "c1")
            .unwrap()
            .unwrap();
        assert!(ch_db::get_message(conn, conv, &mid(1)).unwrap().is_none());
        assert_eq!(
            ch_db::list_history_gaps(conn, conv, "").unwrap()[0].after_message_id,
            mid(0)
        );
        conn.execute_batch("DROP TRIGGER reject_close;").unwrap();
    }
    assert!(history.fill_gap(&chat(), gap).await.unwrap().closed);
}

#[tokio::test]
async fn failed_island_split_rolls_back_new_messages_and_preserves_the_old_gap() {
    let ingest = shared_ingest();
    live(&ingest, 0..10);
    let history = History::new(
        ingest.clone(),
        Platform::new(200, &["history.fill_gap", "history.jump_to_date"]),
        cfg(20),
    );
    history.refresh(&chat(), 20).await.unwrap();
    let before = {
        let ing = ingest.lock().unwrap();
        let conn = ing.db().conn();
        let conv = ch_db::conversation_rowid(conn, "acc", "c1")
            .unwrap()
            .unwrap();
        conn.execute_batch(
            "CREATE TEMP TRIGGER reject_split BEFORE INSERT ON history_gap
            BEGIN SELECT RAISE(ABORT,'injected split failure'); END;",
        )
        .unwrap();
        ch_db::list_history_gaps(conn, conv, "").unwrap()
    };
    assert!(history.jump_to_date(&chat(), at(100), 20).await.is_err());
    let ing = ingest.lock().unwrap();
    let conn = ing.db().conn();
    let conv = ch_db::conversation_rowid(conn, "acc", "c1")
        .unwrap()
        .unwrap();
    assert_eq!(ch_db::list_history_gaps(conn, conv, "").unwrap(), before);
    assert!(ch_db::get_message(conn, conv, &mid(100)).unwrap().is_none());
}

#[tokio::test]
async fn first_date_jump_keeps_newer_history_reachable_with_or_without_fill_capability() {
    for fill in [false, true] {
        let ingest = shared_ingest();
        let caps = if fill {
            vec!["history.jump_to_date", "history.fill_gap"]
        } else {
            vec!["history.jump_to_date"]
        };
        let platform = Platform::new(100, &caps);
        let history = History::new(ingest.clone(), platform.clone(), cfg(20));
        let page = history.jump_to_date(&chat(), at(40), 20).await.unwrap();
        assert_eq!(page.target, Some(mid(40)));
        assert_eq!(gap_count(&ingest), 1);
        let next = history.newer(&chat(), &mid(49), 50).await.unwrap();
        assert_eq!(page_ids(&next), ids(50..100));
        assert_eq!(gap_count(&ingest), 0);
        if fill {
            assert_eq!(platform.calls("history.fill_gap")[0]["cursor"], "50");
        }
    }
}
