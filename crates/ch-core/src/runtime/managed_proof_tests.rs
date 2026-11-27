//! Real WAL transactions in isolated temporary directories; SQL rows are metadata fixtures,
//! never managed-file registration/FD proofs. No host requests or real source files.

use super::*;
use crate::host::MemorySecretStore;
use crate::ingest::IngestOptions;
use crate::runtime::{Core, CoreConfig};
use ch_db::OutboxRow;
use ch_protocol::dto::{Media, MediaKind};
use std::sync::Condvar;
use std::time::Duration;

const SOURCE: &str = "/synthetic/private-upload/bytes";
const STAGE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CLIENT: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn seed(db: &Db) {
    db.conn().execute_batch("INSERT INTO plugin(plugin_id,name,first_seen_at,last_seen_at)
        VALUES('fixture','Fixture',0,0);
        INSERT INTO account(account_id,plugin_id,enabled,created_at,updated_at)
        VALUES('owner','fixture',0,0,0);
        INSERT INTO conversation(account_id,chat_id,kind,created_at,updated_at)
        VALUES('owner','chat','dm',0,0);").unwrap();
}

fn writer() -> (tempfile::TempDir, ManagedProofWriter) {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("hub.db");
    let normal = Db::open(&path).unwrap();
    seed(&normal);
    let ingest = Arc::new(Mutex::new(Ingest::new(normal, IngestOptions::default())));
    let proof = ManagedProofWriter::new(ingest, Db::open_full_sync(path).unwrap());
    (tmp, proof)
}

fn count(conn: &rusqlite::Connection, table: &str) -> i64 {
    // Only fixed test table names reach this helper.
    conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0)).unwrap()
}

fn assert_empty(conn: &rusqlite::Connection) {
    for table in ["managed_source", "outbox", "message", "managed_ref"] {
        assert_eq!(count(conn, table), 0, "{table}");
    }
}

fn write_bundle(scope: &ManagedProofTransaction<'_, '_>) -> (i64, i64, i64) {
    let tx = scope.transaction();
    tx.execute("INSERT INTO managed_source(stage_id,client_id,account_id,plugin_id,
        account_created_at,chat_id,path,fingerprint_json,intent_json,created_at,updated_at)
        VALUES(?1,?2,'owner','fixture',0,'chat',?3,'{}','{}',0,0)",
        rusqlite::params![STAGE, CLIENT, SOURCE]).unwrap();
    let source = tx.last_insert_rowid();
    let (_, echo) = scope.apply_local_echo("owner", &LocalEcho {
        chat_id: "chat".into(), client_id: CLIENT.into(), text: Some("caption".into()),
        media: Some(Media {
            kind: MediaKind::Document, path: Some(SOURCE.into()),
            filename: Some("original.bin".into()), bytes: Some(7), mime: None,
            width: None, height: None, duration: None, thumb_path: None,
            waveform: None, title: None, performer: None, has_spoiler: false, sticker: None,
        }),
        ..Default::default()
    }).unwrap();
    let outbox = ch_db::insert_outbox(tx, &OutboxRow {
        account_id: "owner".into(), chat_id: "chat".into(), client_id: CLIENT.into(),
        kind: "send_file".into(), method: "messages.send_file".into(),
        params: serde_json::json!({"account_id":"owner","chat_id":"chat","path":SOURCE}),
        echo_message_id: Some(echo), ..Default::default()
    }).unwrap();
    let source_id = ch_db::managed_source_id(tx, STAGE).unwrap().unwrap();
    let refs = ch_db::managed_references(tx, source_id).unwrap();
    assert!(!refs.truncated);
    assert_eq!(refs.entries.len(), 2);
    assert!(refs.entries.iter().all(|r| r.account_id.as_deref() == Some("owner")));
    let stale: i64 = tx.query_row("SELECT count(*) FROM managed_ref r
        LEFT JOIN managed_ref_row c USING(consumer_kind,consumer_key)
        WHERE c.revision IS NULL OR c.revision != r.revision OR c.state != 'clean'", [], |r| r.get(0)).unwrap();
    assert_eq!(stale, 0);
    assert_eq!(ch_db::get_outbox(tx, outbox).unwrap().unwrap().echo_message_id, Some(echo));
    (source, outbox, echo)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_receipt_reads_own_bundle_while_normal_snapshot_does_not() {
    let (tmp, writer) = writer();
    let reader = Db::open(tmp.path().join("hub.db")).unwrap();
    let receipt = writer.with_transaction(move |scope| {
        let tx = scope.transaction();
        let sync: i64 = tx.query_row("PRAGMA synchronous", [], |r| r.get(0)).unwrap();
        let mode: String = tx.query_row("PRAGMA journal_mode", [], |r| r.get(0)).unwrap();
        assert_eq!(sync, 2);
        assert_eq!(mode, "wal");
        assert_eq!(reader.conn().query_row("PRAGMA synchronous", [], |r| r.get::<_, i64>(0)).unwrap(), 1);
        // A separate WAL reader, never the shared ingest connection, establishes
        // its old snapshot before the first FULL write. This cannot self-deadlock.
        reader.conn().execute_batch("BEGIN DEFERRED").unwrap();
        assert_empty(reader.conn());
        let receipt = write_bundle(scope);
        assert_eq!(count(tx, "managed_source"), 1);
        assert_empty(reader.conn());
        reader.conn().execute_batch("ROLLBACK").unwrap();
        Ok::<_, ()>(receipt)
    }).await.unwrap().unwrap();
    let db = Db::open(tmp.path().join("hub.db")).unwrap();
    assert_eq!(count(db.conn(), "managed_source"), 1);
    assert_eq!(count(db.conn(), "managed_ref"), 2);
    assert_eq!(ch_db::get_outbox(db.conn(), receipt.1).unwrap().unwrap().echo_message_id, Some(receipt.2));
    assert_eq!(db.conn().query_row("SELECT id FROM managed_source", [], |r| r.get::<_, i64>(0)).unwrap(), receipt.0);
    writer.drain().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn domain_refusal_and_panic_roll_back_all_rows_and_leave_writer_usable() {
    let (tmp, writer) = writer();
    let refused = writer.with_transaction(|scope| {
        write_bundle(scope);
        Err::<(), _>("refused")
    }).await.unwrap();
    assert_eq!(refused, Err("refused"));
    assert_empty(Db::open(tmp.path().join("hub.db")).unwrap().conn());
    let panicked = writer.with_transaction(|scope| -> Result<(), ()> {
        write_bundle(scope);
        panic!("synthetic callback failure");
    }).await;
    assert!(matches!(panicked, Err(ManagedProofError::TaskFailed)));
    assert_empty(Db::open(tmp.path().join("hub.db")).unwrap().conn());
    // Acquiring every slot proves the panic released its actual worker permit.
    let all = Arc::clone(&writer.inner.slots).try_acquire_many_owned(JOB_LIMIT).unwrap();
    drop(all);
    writer.with_transaction(|scope| Ok::<_, ()>(write_bundle(scope))).await.unwrap().unwrap();
    // A recorded panic can make the first drain report failure, but never early drain.
    let _ = writer.drain().await;
    writer.drain().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_normal_transaction_and_changed_durability_refuse_before_callback() {
    let (_tmp, writer) = writer();
    for begin in ["BEGIN IMMEDIATE", "BEGIN DEFERRED"] {
        writer.inner.ingest.lock().unwrap().db().conn().execute_batch(begin).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1),
            writer.with_transaction(|_| -> Result<(), ()> { panic!("must not run") })).await.unwrap();
        assert!(matches!(result, Err(ManagedProofError::ActiveTransaction)));
        writer.inner.ingest.lock().unwrap().db().conn().execute_batch("ROLLBACK").unwrap();
    }
    writer.inner.db.lock().unwrap().conn().pragma_update(None, "synchronous", "NORMAL").unwrap();
    let result = writer.with_transaction(|_| -> Result<(), ()> { panic!("must not run") }).await;
    assert!(matches!(result, Err(ManagedProofError::Database(_))));
    writer.drain().await.unwrap();
    assert!(Db::open_full_sync(":memory:").is_err());
    assert!(Db::open_full_sync("").is_err());
}

struct Release(Arc<(Mutex<bool>, Condvar)>);

impl Release {
    fn release(&self) {
        *self.0.0.lock().unwrap() = true;
        self.0.1.notify_all();
    }
}

impl Drop for Release {
    fn drop(&mut self) { self.release(); }
}

fn pause() -> (Release, impl FnOnce() + Send + 'static, oneshot::Receiver<()>) {
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let release = Release(Arc::clone(&gate));
    let (send, recv) = oneshot::channel();
    (release, move || {
        let _ = send.send(());
        let (lock, cond) = &*gate;
        let mut released = lock.lock().unwrap();
        while !*released { released = cond.wait(released).unwrap(); }
    }, recv)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_admission_rejects_overflow_and_close_without_queuing() {
    let (_tmp, writer) = writer();
    let (release, block, entered) = pause();
    let first_writer = writer.clone();
    let first = tokio::spawn(async move {
        first_writer.with_transaction(move |_| { block(); Ok::<_, ()>(()) }).await
    });
    entered.await.unwrap();
    let mut queued = Vec::new();
    for _ in 1..JOB_LIMIT {
        let writer = writer.clone();
        queued.push(tokio::spawn(async move {
            writer.with_transaction(|_| Ok::<_, ()>(())).await
        }));
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while writer.inner.slots.available_permits() != 0 { tokio::task::yield_now().await; }
    }).await.unwrap();
    assert!(matches!(writer.with_transaction(|_| Ok::<_, ()>(())).await, Err(ManagedProofError::Busy)));
    writer.close();
    assert!(matches!(writer.with_transaction(|_| Ok::<_, ()>(())).await, Err(ManagedProofError::Closed)));
    release.release();
    first.await.unwrap().unwrap().unwrap();
    for job in queued { job.await.unwrap().unwrap().unwrap(); }
    writer.drain().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_caller_and_shutdown_still_retain_actual_blocking_work() {
    let tmp = tempfile::tempdir().unwrap();
    seed(&Db::open(tmp.path().join("hub.db")).unwrap());
    let mut config = CoreConfig::new(tmp.path(), Arc::new(MemorySecretStore::new()));
    config.autostart = false;
    let core = Arc::new(Core::open(config).await.unwrap());
    let writer = core.managed_proofs().clone();
    let (release, block, entered) = pause();
    let caller_writer = writer.clone();
    let caller = tokio::spawn(async move {
        caller_writer.with_transaction(move |scope| {
            let receipt = write_bundle(scope);
            block();
            Ok::<_, ()>(receipt)
        }).await
    });
    entered.await.unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let shutdown_core = Arc::clone(&core);
    let shutdown = tokio::spawn(async move { shutdown_core.shutdown().await });
    // Empty jobs means shutdown reached drain and took ownership of the handle.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let draining = {
                let state = writer.inner.state.lock().unwrap();
                state.closed && state.jobs.is_empty()
            };
            if draining { break; }
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    assert!(!shutdown.is_finished());
    shutdown.abort();
    assert!(shutdown.await.unwrap_err().is_cancelled());
    assert!(matches!(writer.with_transaction(|_| Ok::<_, ()>(())).await, Err(ManagedProofError::Closed)));
    let draining_writer = writer.clone();
    let mut drain = tokio::spawn(async move { draining_writer.drain().await });
    assert!(tokio::time::timeout(Duration::from_millis(25), &mut drain).await.is_err());
    release.release();
    tokio::time::timeout(Duration::from_secs(2), drain).await.unwrap().unwrap().unwrap();
    let db = Db::open(tmp.path().join("hub.db")).unwrap();
    assert_eq!(count(db.conn(), "managed_source"), 1);
    assert_eq!(count(db.conn(), "managed_ref"), 2);
    core.shutdown().await;
}
