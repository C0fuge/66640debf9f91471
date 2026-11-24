//! OS keyring implementation of the host's SecretStore. One credential per (plugin namespace,
//! account) contains a JSON map. Replacing the credential changes its values and key list
//! together. Runtime secrets and x-secret settings use the runtime's distinct namespaces.
//! All keyring calls run on blocking threads. The desktop core owns writes to its namespace.

#[cfg(all(target_os = "linux", feature = "keyring"))]
mod os;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::host::{SecretError, SecretStore};

/// Secret Service application name.
pub const SERVICE: &str = "contact-hub-ng";

/// Synchronous operations on one credential. Implementations must never log secret values.
pub trait KeyringBackend: Send + Sync + 'static {
    fn get(&self, service: &str, user: &str) -> Result<Option<String>, SecretError>;
    fn set(&self, service: &str, user: &str, value: &str) -> Result<(), SecretError>;
    fn delete(&self, service: &str, user: &str) -> Result<(), SecretError>;
}

type Secrets = BTreeMap<String, String>;

// Multiple store handles in the same process may share a backend. The blocking operation owns
// this guard even if the async caller is cancelled while Secret Service is answering.
static KEYRING_LOCK: Mutex<()> = Mutex::new(());

fn account_user(plugin: &str, account: &str) -> String {
    let enc = |s: &str| s.replace('%', "%25").replace(':', "%3A");
    format!("account:{}:{}", enc(plugin), enc(account))
}

fn unavailable() -> SecretError {
    SecretError("OS keyring unavailable or locked".into())
}

/// Persistent SecretStore backed by the system credential store.
pub struct KeyringSecretStore {
    service: String,
    backend: Arc<dyn KeyringBackend>,
}

impl std::fmt::Debug for KeyringSecretStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyringSecretStore").finish_non_exhaustive()
    }
}

impl KeyringSecretStore {
    /// Verify Secret Service is reachable before starting an account. No files or credentials
    /// are created by this probe. Missing OS support and access failures are explicit errors.
    pub async fn open() -> Result<Self, SecretError> {
        #[cfg(all(target_os = "linux", feature = "keyring"))]
        {
            let store = Self::with_backend(SERVICE, Arc::new(os::OsKeyring));
            store
                .run(|b, s| b.get(s, "service-probe").map(|_| ()))
                .await?;
            Ok(store)
        }
        #[cfg(not(all(target_os = "linux", feature = "keyring")))]
        Err(unavailable())
    }

    /// Supply an isolated credential backend (e.g. in tests).
    pub fn with_backend(service: &str, backend: Arc<dyn KeyringBackend>) -> Self {
        Self {
            service: service.into(),
            backend,
        }
    }

    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&dyn KeyringBackend, &str) -> Result<T, SecretError> + Send + 'static,
    ) -> Result<T, SecretError> {
        let backend = Arc::clone(&self.backend);
        let service = self.service.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = KEYRING_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            // Backends may include invalid input or credential bytes in diagnostics.
            f(&*backend, &service).map_err(|_| unavailable())
        })
        .await
        .map_err(|_| unavailable())?
    }

    fn read(b: &dyn KeyringBackend, service: &str, user: &str) -> Result<Secrets, SecretError> {
        match b.get(service, user)? {
            Some(encoded) => serde_json::from_str(&encoded).map_err(|_| unavailable()),
            None => Ok(Secrets::new()),
        }
    }

    async fn update(
        &self,
        plugin: &str,
        account: &str,
        key: String,
        value: Option<String>,
    ) -> Result<(), SecretError> {
        let user = account_user(plugin, account);
        self.run(move |b, service| {
            let mut secrets = Self::read(b, service, &user)?;
            match value {
                Some(v) => {
                    secrets.insert(key, v);
                }
                None if secrets.remove(&key).is_none() => return Ok(()),
                None => {}
            }
            if secrets.is_empty() {
                return b.delete(service, &user);
            }
            let encoded = serde_json::to_string(&secrets).map_err(|_| unavailable())?;
            b.set(service, &user, &encoded)
        })
        .await
    }
}

#[async_trait]
impl SecretStore for KeyringSecretStore {
    async fn get_all(&self, plugin: &str, account: &str) -> Result<Secrets, SecretError> {
        let user = account_user(plugin, account);
        self.run(move |b, s| Self::read(b, s, &user)).await
    }
    async fn set(
        &self,
        plugin: &str,
        account: &str,
        key: &str,
        value: &str,
    ) -> Result<(), SecretError> {
        self.update(plugin, account, key.into(), Some(value.into()))
            .await
    }
    async fn delete(&self, plugin: &str, account: &str, key: &str) -> Result<(), SecretError> {
        self.update(plugin, account, key.into(), None).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Fake(Mutex<BTreeMap<(String, String), String>>);

    impl KeyringBackend for Fake {
        fn get(&self, service: &str, user: &str) -> Result<Option<String>, SecretError> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .get(&(service.into(), user.into()))
                .cloned())
        }
        fn set(&self, service: &str, user: &str, value: &str) -> Result<(), SecretError> {
            if value.contains("simulate-failure") {
                return Err(SecretError(value.into()));
            }
            self.0
                .lock()
                .unwrap()
                .insert((service.into(), user.into()), value.into());
            Ok(())
        }
        fn delete(&self, service: &str, user: &str) -> Result<(), SecretError> {
            self.0
                .lock()
                .unwrap()
                .remove(&(service.into(), user.into()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn secret_map_is_isolated_atomic_and_redacted() {
        let backend = Arc::new(Fake::default());
        let s = KeyringSecretStore::with_backend("test", backend.clone());
        let s2 = KeyringSecretStore::with_backend("test", backend);
        let (a, b) = tokio::join!(
            s.set("p:a", "b", "one", "x"),
            s2.set("p:a", "b", "two", "y")
        );
        a.unwrap();
        b.unwrap();
        assert_eq!(s.get_all("p:a", "b").await.unwrap().len(), 2);
        for (p, a) in [("p", "a:b"), ("p%3Aa", "b"), ("p:a/settings", "b")] {
            assert!(s.get_all(p, a).await.unwrap().is_empty());
        }
        let err = s
            .set("p:a", "b", "one", "simulate-failure")
            .await
            .unwrap_err();
        assert!(!format!("{s:?} {err:?} {err}").contains("simulate-failure"));
        assert_eq!(s.get_all("p:a", "b").await.unwrap()["one"], "x");
        s.delete("p:a", "b", "one").await.unwrap();
        s.delete("p:a", "b", "two").await.unwrap();
        s.delete("p:a", "b", "two").await.unwrap();
        assert!(s.get_all("p:a", "b").await.unwrap().is_empty());
    }
}
