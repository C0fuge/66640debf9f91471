//! In-process authority over the actual account storage directory, including ID aliases.
//! A chooser keeps only the generation token. Storage users retain the read guard.
use super::{AccountRow, Accounts, Arc, HashMap, Mutex, PoisonError, Result, RuntimeError};
use crate::host::HostConfig;
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(super) struct Gates(Mutex<HashMap<PathBuf, Arc<Gate>>>);
#[derive(Default)]
pub(super) struct Gate {
    pub(super) drain: Arc<RwLock<()>>,
    pub(super) token: CancellationToken,
}

impl Gates {
    pub(super) fn get(&self, root: &Path) -> Arc<Gate> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(root.to_owned())
            .or_default()
            .clone()
    }
    pub(super) fn revoke_existing(&self, root: &Path) {
        if let Some(gate) = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(root)
        {
            gate.token.cancel();
        }
    }
    pub(super) fn retire(&self, root: &Path, old: &Arc<Gate>) {
        let mut gates = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if gates.get(root).is_some_and(|gate| Arc::ptr_eq(gate, old)) {
            gates.remove(root);
        }
    }
    fn current(&self, root: &Path, expected: &Arc<Gate>) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(root)
            .is_some_and(|g| Arc::ptr_eq(g, expected))
    }
}
pub(super) fn directory(config: &HostConfig, id: &str) -> PathBuf {
    let mut path = config.state_dir(id);
    path.pop(); // HostConfig appends the fixed child "state".
    path
}
fn valid_root(config: &HostConfig, root: &Path) -> bool {
    root.parent() == Some(config.data_dir.join("accounts").as_path())
        && root.file_name().is_some_and(|name| name.len() <= 255)
}
pub(super) fn require_absent(config: &HostConfig, id: &str, root: &Path) -> Result<()> {
    if valid_root(config, root)
        && std::fs::symlink_metadata(root).is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound)
    {
        return Ok(());
    }
    Err(RuntimeError::AccountExists(id.into()))
}
pub(super) fn same(row: &AccountRow, expected: &AccountRow) -> bool {
    row.account_id == expected.account_id
        && row.plugin_id == expected.plugin_id
        && row.created_at == expected.created_at
}
fn unknown(id: &str) -> RuntimeError {
    RuntimeError::UnknownAccount(id.into())
}
pub(super) fn check_owner(conn: &Connection, expected: &AccountRow) -> Result<()> {
    if ch_db::get_account(conn, &expected.account_id)?.is_some_and(|row| same(&row, expected)) {
        Ok(())
    } else {
        Err(unknown(&expected.account_id))
    }
}
// Bounded examination of identifiers only. A source ID longer than 256 bytes
// cannot sanitize to a <=255-byte component (replacements never shorten it).
pub(super) fn exclusive(
    conn: &Connection,
    config: &HostConfig,
    expected: &AccountRow,
    root: &Path,
) -> Result<bool> {
    if !valid_root(config, root) {
        return Ok(false);
    }
    let mut stmt=conn.prepare("SELECT substr(CAST(account_id AS BLOB), 1, 257) FROM account WHERE account_id != ?1 LIMIT 4097")
        .map_err(ch_db::Error::from)?;
    let mut rows = stmt
        .query([&expected.account_id])
        .map_err(ch_db::Error::from)?;
    let mut count = 0;
    while let Some(row) = rows.next().map_err(ch_db::Error::from)? {
        count += 1;
        if count > 4096 {
            return Ok(false);
        }
        let id: Vec<u8> = row.get(0).map_err(ch_db::Error::from)?;
        if id.len() > 256 {
            continue;
        }
        let Ok(id) = std::str::from_utf8(&id) else {
            return Ok(false);
        };
        if directory(config, id) == root {
            return Ok(false);
        }
    }
    Ok(true)
}
impl Accounts {
    /// Pin a persisted account's configured storage against destructive teardown.
    /// The token revokes queued work. Retain the guard through actual I/O settlement.
    /// Native choosers keep only the token and recheck it after acquiring a fresh guard.
    pub async fn storage_lease(
        &self,
        expected: &AccountRow,
    ) -> Result<(CancellationToken, OwnedRwLockReadGuard<()>)> {
        let root = directory(self.0.host.config(), &expected.account_id);
        let owner = expected.clone();
        let inner = self.0.clone();
        let path = root.clone();
        let gate = self
            .db(move |db| {
                check_owner(db.conn(), &owner)?;
                Ok(inner.storage.get(&path))
            })
            .await?;
        if gate.token.is_cancelled() {
            return Err(unknown(&expected.account_id));
        }
        let guard = gate.drain.clone().read_owned().await;
        let owner = expected.clone();
        let inner = self.0.clone();
        let path = root.clone();
        self.db(move |db| {
            check_owner(db.conn(), &owner)?;
            if !exclusive(db.conn(), inner.host.config(), &owner, &path)? {
                return Err(unknown(&owner.account_id));
            }
            Ok(())
        })
        .await?;
        if gate.token.is_cancelled() || !self.0.storage.current(&root, &gate) {
            return Err(unknown(&expected.account_id));
        }
        Ok((gate.token.clone(), guard))
    }

    // Called with the lifecycle and membership guards and the revoked storage writer.
    // A definite, explicitly confirmed rollback can authorize a fresh token.
    // A failed rollback, unknown commit or task failure leaves the gate cancelled.
    pub(super) async fn delete_owned_row(
        &self,
        row: &AccountRow,
        root: &Path,
        gate: &Arc<Gate>,
    ) -> Result<bool> {
        let owner = row.clone();
        let inner = self.0.clone();
        let path = root.to_owned();
        let result = self
            .db(move |db| {
                let tx = db.transaction()?;
                check_owner(&tx, &owner)?;
                let sole = exclusive(&tx, inner.host.config(), &owner, &path)?;
                match ch_db::delete_account(&tx, &owner.account_id) {
                    Ok(true) => {
                        tx.commit().map_err(ch_db::Error::from)?;
                        Ok(Ok(sole))
                    }
                    result => {
                        let error = match result {
                            Err(e) => RuntimeError::Db(e),
                            _ => unknown(&owner.account_id),
                        };
                        tx.rollback().map_err(ch_db::Error::from)?;
                        check_owner(db.conn(), &owner)?;
                        Ok(Err(error))
                    }
                }
            })
            .await?;
        if result.is_err() {
            self.0.storage.retire(root, gate);
        }
        result
    }
}
