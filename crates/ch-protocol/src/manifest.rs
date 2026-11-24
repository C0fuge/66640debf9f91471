//! `plugin.json` (spec §3.2).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

str_enum! {
    /// Known sign-in methods for `auth_methods`. Unknown values are shown verbatim.
    AuthMethod {
        PhoneCode = "phone_code",
        Password2fa = "password_2fa",
        Qr = "qr",
        EmailCode = "email_code",
        Token = "token",
        Oauth = "oauth",
        UsernamePassword = "username_password",
        /// Signs in with nothing but its settings; never emits steps.
        None = "none",
    }
}

/// The manifest at the top of a plugin directory. Unknown fields are ignored.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    /// Platform id, `^[a-z][a-z0-9_]{1,31}$`. Immutable once published.
    pub id: String,
    /// Human name of the platform.
    pub name: String,
    /// The plugin's own version (semver).
    pub version: String,
    /// The API version it implements (semver; compared by major).
    pub api_version: String,
    /// argv to spawn; `entry[0]` starting with `./` is relative to the plugin directory.
    pub entry: Vec<String>,
    /// SVG or PNG path relative to the plugin directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    /// `#RRGGBB` for the platform badge.
    pub brand_color: String,
    /// May the core start more than one account on this process?
    pub multi_account: bool,
    /// Sign-in methods the user may pick in `auth.begin`.
    pub auth_methods: Vec<AuthMethod>,
    /// JSON Schema (draft 2020-12) of the per-account settings form (§6).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settings_schema: Option<serde_json::Map<String, serde_json::Value>>,
    /// Shown in Settings → Accounts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Shown in Settings → Accounts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub homepage: Option<String>,
    /// Shown in Settings → Accounts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    /// Shown in Settings → Accounts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    /// Extra environment variables for the process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<BTreeMap<String, String>>,
}

/// Is `id` a well-formed plugin id (`^[a-z][a-z0-9_]{1,31}$`)?
pub fn is_valid_plugin_id(id: &str) -> bool {
    let b = id.as_bytes();
    (2..=32).contains(&b.len())
        && b[0].is_ascii_lowercase()
        && b[1..]
            .iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_')
}

/// Is `c` a `#RRGGBB` colour?
pub fn is_valid_brand_color(c: &str) -> bool {
    let b = c.as_bytes();
    b.len() == 7 && b[0] == b'#' && b[1..].iter().all(u8::is_ascii_hexdigit)
}

impl Manifest {
    /// Structural checks beyond what serde enforces: id pattern, non-empty entry, colour
    /// format, semver fields. Returns the first problem as a message.
    pub fn validate(&self) -> Result<(), String> {
        if !is_valid_plugin_id(&self.id) {
            return Err(format!(
                "id {:?} does not match ^[a-z][a-z0-9_]{{1,31}}$",
                self.id
            ));
        }
        if self.name.is_empty() {
            return Err("name is empty".into());
        }
        if self.entry.is_empty() || self.entry.iter().any(String::is_empty) {
            return Err("entry must be a non-empty array of non-empty strings".into());
        }
        if !is_valid_brand_color(&self.brand_color) {
            return Err(format!("brand_color {:?} is not #RRGGBB", self.brand_color));
        }
        crate::ApiVersion::parse(&self.version).map_err(|e| format!("version: {e}"))?;
        crate::ApiVersion::parse(&self.api_version).map_err(|e| format!("api_version: {e}"))?;
        Ok(())
    }

    /// Does this plugin speak our major version (§18)?
    pub fn is_compatible(&self) -> bool {
        crate::versions_compatible(&self.api_version, crate::API_VERSION)
    }
}
