//! The Linux Secret Service backend. Construct a concrete credential so even an application
//! override of keyring's global default builder cannot redirect secrets to a mock.
use super::{KeyringBackend, SecretError, unavailable};
use keyring::secret_service::SsCredential;
use keyring::{Entry, Error};

pub(super) struct OsKeyring;

fn entry(service: &str, user: &str) -> Result<Entry, SecretError> {
    let cred = SsCredential::new_with_target(None, service, user).map_err(|_| unavailable())?;
    Ok(Entry::new_with_credential(Box::new(cred)))
}

impl KeyringBackend for OsKeyring {
    fn get(&self, service: &str, user: &str) -> Result<Option<String>, SecretError> {
        match entry(service, user)?.get_password() {
            Ok(v) => Ok(Some(v)),
            Err(Error::NoEntry) => Ok(None),
            Err(_) => Err(unavailable()),
        }
    }
    fn set(&self, service: &str, user: &str, value: &str) -> Result<(), SecretError> {
        entry(service, user)?
            .set_password(value)
            .map_err(|_| unavailable())
    }
    fn delete(&self, service: &str, user: &str) -> Result<(), SecretError> {
        match entry(service, user)?.delete_credential() {
            Ok(()) | Err(Error::NoEntry) => Ok(()),
            Err(_) => Err(unavailable()),
        }
    }
}
