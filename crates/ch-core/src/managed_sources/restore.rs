//! Bounded negative restoration only. No replay, source reader or closure grant.
use crate::{host::HostConfig, runtime::{Result, RuntimeError}};
use rusqlite::{Connection, params};
use std::{collections::{HashMap, HashSet}, path::PathBuf};

const SOURCES:usize=256;
const ATTEMPTS:usize=1024;
const PAGE:i64=64;

pub(super) struct Recovered { pub(super) roots:HashMap<String,PathBuf> }
fn unavailable() -> RuntimeError { RuntimeError::ManagedLifetimeRecoveryUnavailable }

pub(super) fn load(conn:&Connection,config:&HostConfig) -> Result<Recovered> {
    let tx=rusqlite::Transaction::new_unchecked(conn,rusqlite::TransactionBehavior::Deferred)
        .map_err(ch_db::Error::from)?;
    let noncanonical:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM managed_source WHERE id<=0)
        OR EXISTS(SELECT 1 FROM managed_dispatch WHERE id<=0)
        OR EXISTS(SELECT 1 FROM managed_retirement WHERE source_id<=0)",[],|row|row.get(0))
        .map_err(ch_db::Error::from)?;
    if noncanonical { return Err(unavailable()); }
    let mut roots=HashMap::new();let mut ids=HashSet::new();let mut cursor=0i64;let mut metadata=0usize;
    loop {
        // Do not load attacker-sized raw SQL strings. One page is at most 64
        // bounded scalar identities; fingerprint/intent/response JSON stays in DB.
        let mut query=tx.prepare_cached("SELECT id,
            CASE WHEN length(CAST(stage_id AS BLOB))=32 THEN stage_id END,
            CASE WHEN length(CAST(account_id AS BLOB)) BETWEEN 1 AND 4096 THEN account_id END,
            CASE WHEN length(CAST(path AS BLOB)) BETWEEN 1 AND 4096 THEN path END
            FROM managed_source WHERE id>?1 ORDER BY id LIMIT ?2").map_err(ch_db::Error::from)?;
        let page=query.query_map(params![cursor,PAGE],|row|Ok((row.get::<_,i64>(0)?,
            row.get::<_,Option<String>>(1)?,row.get::<_,Option<String>>(2)?,row.get::<_,Option<String>>(3)?)))
            .map_err(ch_db::Error::from)?.collect::<rusqlite::Result<Vec<_>>>().map_err(ch_db::Error::from)?;
        if page.is_empty() { break; }
        for (id,stage,account,path) in page {
            if ids.len()==SOURCES { return Err(unavailable()); }
            let stage=stage.ok_or_else(unavailable)?;
            let account=account.ok_or_else(unavailable)?;
            let path=path.ok_or_else(unavailable)?;
            metadata+=stage.len()+account.len()+path.len();
            if metadata>1_048_576 { return Err(unavailable()); }
            if !crate::managed_stage::manifest::hex(&stage,32)
                || PathBuf::from(path)!=config.upload_dir(&account).join(&stage).join("bytes")
            { return Err(unavailable()); }
            let root=config.state_dir(&account).parent().ok_or_else(unavailable)?.to_owned();
            if roots.insert(stage,root).is_some() { return Err(unavailable()); }
            ids.insert(id);cursor=id;
        }
    }
    // Inspect ALL historical attempts, regardless of current binding, source
    // label, outbox state or a newer positive reply. Never infer physical closure
    // from producer exit, missing pointers or a released/positive SQL label.
    for (table,key,budget) in [("managed_dispatch","id",ATTEMPTS),("managed_retirement","source_id",SOURCES)] {
        let mut cursor=0i64;let mut count=0usize;
        loop {
            let sql=format!("SELECT {key},source_id FROM {table} WHERE {key}>?1 ORDER BY {key} LIMIT ?2");
            let mut query=tx.prepare_cached(&sql).map_err(ch_db::Error::from)?;
            let page=query.query_map(params![cursor,PAGE],|row|Ok((row.get::<_,i64>(0)?,row.get::<_,i64>(1)?)))
                .map_err(ch_db::Error::from)?.collect::<rusqlite::Result<Vec<_>>>().map_err(ch_db::Error::from)?;
            if page.is_empty() { break; }
            for (id,source) in page {
                count+=1;
                if count>budget || !ids.contains(&source) { return Err(unavailable()); }
                cursor=id;
            }
        }
    }
    tx.commit().map_err(ch_db::Error::from)?;
    Ok(Recovered { roots })
}
