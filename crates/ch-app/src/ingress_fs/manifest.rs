use super::{Error, MAX_BYTES, Result, fd::uid};
use serde::{Deserialize, Serialize};
use std::{fs::Metadata, os::unix::fs::MetadataExt, path::PathBuf};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Scope {
    pub account: String,
    pub plugin: String,
    pub account_created: i64,
    pub chat: String,
    #[serde(deserialize_with = "Deserialize::deserialize")]
    pub topic: Option<String>,
}
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Owner {
    pub account: String,
    pub plugin: String,
    pub account_created: i64,
    pub uploads: PathBuf,
}
impl Owner {
    pub(super) fn matches(&self, scope: &Scope) -> bool {
        self.account == scope.account
            && self.plugin == scope.plugin
            && self.account_created == scope.account_created
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Intent {
    pub caption: String,
    pub as_file: bool,
    pub spoiler: bool,
}
impl Intent {
    pub(super) fn check(&self) -> Result<()> {
        if self.caption.len() > 4096 || self.caption.chars().count() > 1024 {
            Err(Error::Integrity)
        } else {
            Ok(())
        }
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum Phase {
    Ready,
    Claimed(Intent),
    Consumed(Intent),
    Discarded,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Format {
    Jpeg,
    Png,
    Other,
}
impl Format {
    pub fn detect(prefix: &[u8]) -> Self {
        if prefix.starts_with(b"\xff\xd8\xff") {
            Self::Jpeg
        } else if prefix.starts_with(b"\x89PNG\r\n\x1a\n") {
            Self::Png
        } else {
            Self::Other
        }
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Fingerprint {
    pub dev: u64,
    pub ino: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    links: u64,
    pub size: u64,
    mtime: i64,
    mtime_nsec: i64,
    ctime: i64,
    ctime_nsec: i64,
}
impl From<Metadata> for Fingerprint {
    fn from(m: Metadata) -> Self {
        Self {
            dev: m.dev(),
            ino: m.ino(),
            mode: m.mode(),
            uid: m.uid(),
            gid: m.gid(),
            links: m.nlink(),
            size: m.len(),
            mtime: m.mtime(),
            mtime_nsec: m.mtime_nsec(),
            ctime: m.ctime(),
            ctime_nsec: m.ctime_nsec(),
        }
    }
}
impl Fingerprint {
    pub fn staged(&self) -> bool {
        self.mode == libc::S_IFREG | 0o400
            && self.uid == uid()
            && self.links == 1
            && self.ino != 0
            && (0..1_000_000_000).contains(&self.mtime_nsec)
            && (0..1_000_000_000).contains(&self.ctime_nsec)
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    pub version: u8,
    pub stage_id: String,
    pub client_id: String,
    pub scope: Scope,
    pub created: i64,
    pub name: String,
    pub size: u64,
    pub format: Format,
    pub(super) fingerprint: Fingerprint,
    pub(super) sha256: String,
    pub phase: Phase,
}
pub(super) fn hex(value: &str, size: usize) -> bool {
    value.len() == size
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub(super) fn opaque(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256
}
pub(super) fn sanitize(name: Option<&str>) -> String {
    let mut out = String::new();
    if let Some(s) = name {
        for c in s.chars().filter(|c| !c.is_control() && !matches!(c, '/' | '\\' | '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')) {
            if out.len() + c.len_utf8() > 255 { break; }
            out.push(c);
        }
    }
    if out.trim().is_empty() || out == "." || out == ".." {
        "attachment".into()
    } else {
        out
    }
}
impl Manifest {
    pub(super) fn check(&self) -> Result<()> {
        if self.version != 1
            || !hex(&self.stage_id, 32)
            || !hex(&self.client_id, 32)
            || self.client_id == self.stage_id
            || !opaque(&self.scope.account)
            || !opaque(&self.scope.plugin)
            || !opaque(&self.scope.chat)
            || self.scope.topic.as_ref().is_some_and(|s| !opaque(s))
            || self.scope.account_created < 0
            || self.created < 0
            || self.size == 0
            || self.size > MAX_BYTES
            || self.size != self.fingerprint.size
            || !self.fingerprint.staged()
            || self.name != sanitize(Some(&self.name))
            || !hex(&self.sha256, 64)
        {
            return Err(Error::Integrity);
        }
        if let Phase::Claimed(intent) | Phase::Consumed(intent) = &self.phase {
            intent.check()?;
        }
        Ok(())
    }
}
