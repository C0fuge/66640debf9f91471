//! Conservative managed-source metadata and reverse references.
//!
//! These records are not filesystem, source-release or unlink authority. Every literal
//! reference remains unresolved; later Core code must independently validate anchored
//! FDs, owner leases and settlement. There is deliberately no source/proof insertion API.

use rusqlite::{Connection, OptionalExtension, params};

use crate::Result;

/// A managed source identity read from the journal, never constructed from a pathname.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManagedSourceId(i64);

impl ManagedSourceId {
    /// The persisted key; possession of this key grants no authority.
    pub fn as_i64(self) -> i64 { self.0 }
}

/// A persisted dispatch identity, independent of retry-reset attempt counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManagedDispatchId(i64);

impl ManagedDispatchId {
    /// The persisted key; this is not a settlement certificate.
    pub fn as_i64(self) -> i64 { self.0 }
}

/// The supported reverse-reference consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefConsumer {
    /// Message media, previews and conservative extension paths.
    Message,
    /// Same-message historical media.
    MessageVersion,
    /// Canonical outbox request paths.
    Outbox,
    /// The media-file lookup index (not a reverse multimap).
    MediaFile,
    /// Conversation avatars and extension paths.
    Conversation,
    /// Contact avatars and extension paths.
    PlatformHandle,
    /// Installed plugin icons.
    Plugin,
}

const CONSUMERS: [RefConsumer; 7] = [RefConsumer::Message, RefConsumer::MessageVersion,
    RefConsumer::Outbox, RefConsumer::MediaFile, RefConsumer::Conversation,
    RefConsumer::PlatformHandle, RefConsumer::Plugin];

impl RefConsumer {
    /// Stable journal/table name; never supplied as unchecked SQL by a caller.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message", Self::MessageVersion => "message_version",
            Self::Outbox => "outbox", Self::MediaFile => "media_file",
            Self::Conversation => "conversation", Self::PlatformHandle => "platform_handle",
            Self::Plugin => "plugin",
        }
    }
    fn parse(value: &str) -> rusqlite::Result<Self> {
        CONSUMERS.into_iter().find(|c| c.as_str() == value).ok_or(rusqlite::Error::InvalidQuery)
    }
    fn key(self) -> &'static str {
        match self { Self::MediaFile => "path", Self::Plugin => "plugin_id", _ => "id" }
    }
    fn text_key(self) -> bool { matches!(self, Self::MediaFile | Self::Plugin) }
    fn columns(self) -> &'static [&'static str] {
        match self {
            Self::Message => &["media_json","link_preview_json","ext_json","reply_to_json",
                "forward_from_json","poll_json","location_json","contact_card_json","dice_json",
                "buttons_json","action_json"],
            Self::MessageVersion => &["media_json"], Self::Outbox => &["params_json"],
            Self::MediaFile => &["path"], Self::Conversation => &["avatar_path","ext_json","wallpaper"],
            Self::PlatformHandle => &["avatar_path","ext_json","status_json"], Self::Plugin => &["icon_path"],
        }
    }
}

/// One conservative literal slot. No inode/alias or detachability assertion is made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedReference {
    /// Owning SQL table.
    pub consumer: RefConsumer,
    /// Exact persisted row key.
    pub key: String,
    /// Exact JSON/scalar column.
    pub column: String,
    /// SQLite JSON path, or `$scalar`; ambiguous JSON is blocked separately.
    pub slot: String,
    /// Literal path, not canonical filesystem identity.
    pub path: String,
    /// Denormalized owner hint; re-read actual consumer ownership before any grant.
    pub account_id: Option<String>,
    /// Denormalized message hint; never sufficient ownership proof.
    pub message_id: Option<i64>,
    /// Exact extraction revision.
    pub revision: i64,
}

/// A bounded indexed result; an incomplete result never means no other references.
#[derive(Debug)]
pub struct ManagedReferences {
    /// Literal entries within the fixed release budget.
    pub entries: Vec<ManagedReference>,
    /// More entries or bytes exist than the reviewed 256-reference / 1 MiB budget.
    pub truncated: bool,
}

/// Index coverage only. Complete does not certify filesystem identity or release eligibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManagedCoverage {
    /// Every original table cursor reached its end.
    pub complete: bool,
    /// A direct/unacknowledged SQL write still requires bounded revalidation.
    pub dirty: bool,
    /// An unsupported/invalid/oversized/ambiguous consumer is retained as a blocker.
    pub blocked: bool,
}

/// Bounded cursor progress, independent of unrelated history size.
#[derive(Debug)]
pub struct ManagedBackfill {
    /// Rows refreshed in this transaction, at most 128.
    pub rows: usize,
    /// Charged input bytes, at most 1 MiB (an oversize blocked row charges one page).
    pub bytes: usize,
    /// Current coverage after this step.
    pub coverage: ManagedCoverage,
}

/// Current literal-index coverage. These constant-sized probes use the state index.
pub fn managed_reference_coverage(conn: &Connection) -> Result<ManagedCoverage> {
    Ok(conn.query_row(
        "SELECT NOT EXISTS(SELECT 1 FROM managed_ref_coverage WHERE complete=0),
          EXISTS(SELECT 1 FROM managed_ref_row WHERE state='dirty'),
          EXISTS(SELECT 1 FROM managed_ref_row WHERE state='blocked')", [],
        |r| Ok(ManagedCoverage { complete:r.get(0)?,dirty:r.get(1)?,blocked:r.get(2)? }))?)
}

/// Look up a source metadata identity only; there is no registration/grant seam yet.
pub fn managed_source_id(conn: &Connection, stage_id: &str) -> Result<Option<ManagedSourceId>> {
    Ok(conn.query_row("SELECT id FROM managed_source WHERE stage_id=?1", [stage_id],
        |r| Ok(ManagedSourceId(r.get(0)?))).optional()?)
}

/// Read a current dispatch identity without asserting any successful settlement.
pub fn managed_current_dispatch(conn: &Connection, outbox_id: i64) -> Result<Option<ManagedDispatchId>> {
    Ok(conn.query_row("SELECT dispatch_id FROM managed_outbox_dispatch WHERE outbox_id=?1",[outbox_id],
        |r| Ok(ManagedDispatchId(r.get(0)?))).optional()?)
}

/// Fetch a bounded indexed source candidate set, including pre-registration literal refs.
/// Aliases cannot be inferred absent from this query; all results are unresolved.
pub fn managed_references(conn: &Connection, source: ManagedSourceId) -> Result<ManagedReferences> {
    let mut stmt=conn.prepare_cached(
        "SELECT consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision
         FROM managed_ref WHERE source_id=?1
         UNION ALL
         SELECT consumer_kind,consumer_key,column_name,slot,path,account_id,message_id,revision
         FROM managed_ref WHERE path=(SELECT path FROM managed_source WHERE id=?1)
           AND (source_id IS NULL OR source_id!=?1) LIMIT 257")?;
    let mut rows=stmt.query([source.0])?;
    let (mut entries,mut bytes)=(Vec::new(),0usize);
    while let Some(row)=rows.next()? {
        let entry=ManagedReference { consumer:RefConsumer::parse(&row.get::<_,String>(0)?)?,
            key:row.get(1)?,column:row.get(2)?,slot:row.get(3)?,path:row.get(4)?,
            account_id:row.get(5)?,message_id:row.get(6)?,revision:row.get(7)? };
        bytes=bytes.saturating_add(entry.key.len()+entry.column.len()+entry.slot.len()+entry.path.len()
            +entry.account_id.as_ref().map_or(0,String::len));
        if entries.len()==256 || bytes>1_048_576 {
            return Ok(ManagedReferences {entries,truncated:true});
        }
        entries.push(entry);
    }
    Ok(ManagedReferences {entries,truncated:false})
}

/// Resume literal extraction in pages of at most 128 rows / 1 MiB input. The first
/// write reserves SQLite's writer before reading cursors. Invalid rows advance but
/// stay blocked. Dirty rows behind completed cursors are revalidated separately.
pub fn backfill_managed_references(conn: &Connection) -> Result<ManagedBackfill> {
    let write=RefWrite::begin(conn)?;
    conn.execute("UPDATE managed_ref_coverage SET format_version=format_version WHERE consumer_kind='message'",[])?;
    let (mut count,mut bytes)=(0usize,0usize);
    for consumer in CONSUMERS {
        if count==128 || bytes==1_048_576 { break; }
        let (complete,integer,has_text):(bool,Option<i64>,bool)=conn.query_row(
            "SELECT complete,integer_cursor,text_cursor IS NOT NULL FROM managed_ref_coverage WHERE consumer_kind=?1",
            [consumer.as_str()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
        if complete { continue; }
        let key=consumer.key(); let table=consumer.as_str();
        let payload_weight=consumer.columns().iter().map(|col|format!("coalesce(length(CAST({col} AS BLOB)),0)"))
            .collect::<Vec<_>>().join("+");
        let weight=format!("{payload_weight}+length(CAST({key} AS BLOB))");
        // Avoid `(?1 IS NULL OR key>?1)`: SQLite may scan from the beginning for
        // every later page instead of using the primary-key range.
        let filter=if consumer.text_key() && has_text {
            format!("WHERE {key}>(SELECT text_cursor FROM managed_ref_coverage WHERE consumer_kind=?1)")
        } else if !consumer.text_key() && integer.is_some() {
            format!("WHERE {key}>?1")
        } else {String::new()};
        // Malformed raw SQL can create enormous text primary keys. Keep their
        // exact cursor inside SQLite and never load that key into an application Vec.
        let selected_key=if consumer.text_key() {
            format!("CASE WHEN length(CAST({key} AS BLOB))<=4096 THEN {key} ELSE NULL END")
        } else {format!("CAST({key} AS TEXT)")};
        let sql=format!("SELECT {selected_key},{weight} FROM {table} {filter} ORDER BY {key} LIMIT ?2");
        let mut stmt=conn.prepare_cached(&sql)?;
        let cursor=if consumer.text_key() {rusqlite::types::Value::Text(table.into())} else {integer.map_or(rusqlite::types::Value::Null, rusqlite::types::Value::Integer)};
        let page=stmt.query_map(params![cursor,(128-count) as i64],|r|Ok((
            r.get::<_,Option<String>>(0)?,r.get::<_,i64>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if page.is_empty() {
            conn.execute("UPDATE managed_ref_coverage SET complete=1 WHERE consumer_kind=?1",[table])?;
        }
        for (key,weight) in page {
            let charge=usize::try_from(weight).unwrap_or(usize::MAX).min(1_048_576);
            if count>0 && bytes.saturating_add(charge)>1_048_576 { break; }
            if let Some(key)=key {
                refresh(conn,consumer,&key)?;
                conn.execute(if consumer.text_key() {
                    "UPDATE managed_ref_coverage SET text_cursor=?2 WHERE consumer_kind=?1"
                } else { "UPDATE managed_ref_coverage SET integer_cursor=CAST(?2 AS INTEGER) WHERE consumer_kind=?1" },params![table,key])?;
            } else {
                advance_oversized_key(conn,consumer)?;
            }
            count+=1;bytes+=charge;
        }
    }
    // Revalidation is bounded and ordered; blocked rows require an actual writer fix,
    // rather than spinning forever on an invalid row before later cursor pages.
    if count<128 && bytes<1_048_576 {
        let mut stmt=conn.prepare_cached("SELECT consumer_kind,consumer_key FROM managed_ref_row WHERE state='dirty' ORDER BY consumer_kind,consumer_key LIMIT ?1")?;
        let pending=stmt.query_map([(128-count) as i64],|r|Ok((RefConsumer::parse(&r.get::<_,String>(0)?)?,r.get::<_,String>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for (consumer,key) in pending {
            // Each refresh independently caps parsing at 64 KiB per value. Charge
            // the full conservative row cap so this auxiliary pass is one row/page.
            if count>0 { break; }
            refresh(conn,consumer,&key)?;
            count+=1;bytes=1_048_576;
        }
    }
    let coverage=managed_reference_coverage(conn)?;
    write.commit()?;
    Ok(ManagedBackfill {rows:count,bytes,coverage})
}

fn advance_oversized_key(conn:&Connection, consumer:RefConsumer)->Result<()> {
    let table=consumer.as_str(); let key=consumer.key(); let column=consumer.columns()[0];
    let has_cursor:bool=conn.query_row("SELECT text_cursor IS NOT NULL FROM managed_ref_coverage WHERE consumer_kind=?1",
        [table],|r|r.get(0))?;
    let filter=if has_cursor {
        format!("AND {key}>(SELECT text_cursor FROM managed_ref_coverage WHERE consumer_kind=?1)")
    } else {String::new()};
    let next=format!("SELECT {key} FROM {table} WHERE ?1=?1 {filter} ORDER BY {key} LIMIT 1");
    conn.execute(&format!("UPDATE {table} SET {column}={column} WHERE {key}=({next})"),[table])?;
    // The extraction trigger marks this key blocked. Do not acknowledge or read it.
    conn.execute(&format!("UPDATE managed_ref_coverage SET text_cursor=({next}) WHERE consumer_kind=?1"),[table])?;
    Ok(())
}

fn refresh(conn:&Connection, consumer:RefConsumer, key:&str)->Result<()> {
    // No DTO roundtrip: keep every original/unknown JSON value byte-for-byte.
    // The no-op updates only a path column (not message text/updated_at), invoking
    // the same bounded extraction trigger as ordinary writes, without FTS churn.
    let column=consumer.columns()[0];
    conn.execute(&format!("UPDATE {} SET {column}={column} WHERE {}=?1",consumer.as_str(),consumer.key()),[key])?;
    acknowledge(conn,consumer,key)
}

fn acknowledge(conn:&Connection, consumer:RefConsumer,key:&str)->Result<()> {
    let revision:Option<i64>=conn.query_row(
        "SELECT revision FROM managed_ref_row WHERE consumer_kind=?1 AND consumer_key=?2 AND state='dirty'",
        params![consumer.as_str(),key],|r|r.get(0)).optional()?;
    if let Some(revision)=revision {
        conn.execute("UPDATE managed_ref_row SET state='clean' WHERE consumer_kind=?1 AND consumer_key=?2 AND revision=?3 AND state='dirty'",
            params![consumer.as_str(),key,revision])?;
    }
    Ok(())
}

/// Query helpers use one savepoint for their write and exact revision acknowledgement.
/// Direct SQL cannot call this private acknowledgement and therefore stays dirty.
pub(crate) struct RefWrite<'a> { conn:&'a Connection, active:bool }

impl<'a> RefWrite<'a> {
    pub(crate) fn begin(conn:&'a Connection)->Result<Self> {
        conn.execute_batch("SAVEPOINT managed_ref_writer")?;
        Ok(Self {conn,active:true})
    }
    pub(crate) fn finish(self,consumer:RefConsumer,key:impl ToString)->Result<()> {
        acknowledge(self.conn,consumer,&key.to_string())?;
        self.commit()
    }
    fn commit(mut self)->Result<()> {
        self.conn.execute_batch("RELEASE managed_ref_writer")?;
        self.active=false; Ok(())
    }
}
impl Drop for RefWrite<'_> {
    fn drop(&mut self) {
        if self.active {
            let _=self.conn.execute_batch("ROLLBACK TO managed_ref_writer; RELEASE managed_ref_writer");
        }
    }
}

