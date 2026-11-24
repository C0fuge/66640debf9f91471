//! Plugin discovery (spec §3): scan the built-in and user plugin directories, parse and
//! validate `plugin.json`, resolve `entry[0]`, and report "not installed" when it is missing.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use ch_protocol::Manifest;

/// Why a discovered directory is not runnable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryProblem {
    /// `plugin.json` could not be read or parsed, or failed validation (§3.2).
    InvalidManifest(String),
    /// `entry[0]` does not exist or is not executable (§3.4). The core never installs it.
    NotInstalled {
        /// The path that was looked for (resolved against the plugin directory, or the
        /// bare command when it was looked up on `PATH`).
        path: PathBuf,
    },
}

impl std::fmt::Display for DiscoveryProblem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidManifest(m) => write!(f, "invalid manifest: {m}"),
            Self::NotInstalled { path } => {
                write!(f, "not installed: {} is missing", path.display())
            }
        }
    }
}

/// One plugin directory found on disk.
#[derive(Debug, Clone)]
pub struct Discovered {
    /// The directory containing `plugin.json`.
    pub dir: PathBuf,
    /// Found under the built-in directory (as opposed to the user directory).
    pub builtin: bool,
    /// The parsed manifest, when it parsed.
    pub manifest: Option<Manifest>,
    /// The resolved executable, when `entry[0]` was found.
    pub executable: Option<PathBuf>,
    /// What stops this plugin from running, if anything.
    pub problem: Option<DiscoveryProblem>,
}

impl Discovered {
    /// The manifest `id`, or the directory name when the manifest is unusable.
    pub fn id(&self) -> String {
        self.manifest.as_ref().map_or_else(
            || {
                self.dir
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default()
            },
            |m| m.id.clone(),
        )
    }
}

/// Scan `builtin` (if any) then `user`; a user plugin shadows a built-in one with the same
/// `id` (§3.1). Directories without `plugin.json` are skipped silently. The result is sorted
/// by id.
pub fn discover(builtin: Option<&Path>, user: &Path) -> Vec<Discovered> {
    let mut by_id: BTreeMap<String, Discovered> = BTreeMap::new();
    for (dir, is_builtin) in builtin
        .map(|b| (b, true))
        .into_iter()
        .chain(std::iter::once((user, false)))
    {
        for d in scan_dir(dir, is_builtin) {
            by_id.insert(d.id(), d);
        }
    }
    by_id.into_values().collect()
}

fn scan_dir(root: &Path, builtin: bool) -> Vec<Discovered> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut out: Vec<Discovered> = entries
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.join("plugin.json").is_file())
        .map(|p| inspect(&p, builtin))
        .collect();
    out.sort_by(|a, b| a.dir.cmp(&b.dir));
    out
}

/// Inspect one plugin directory.
pub fn inspect(dir: &Path, builtin: bool) -> Discovered {
    let dir = dir.to_path_buf();
    let manifest = std::fs::read(dir.join("plugin.json"))
        .map_err(|e| e.to_string())
        .and_then(|bytes| serde_json::from_slice::<Manifest>(&bytes).map_err(|e| e.to_string()))
        .and_then(|m| m.validate().map(|()| m));
    match manifest {
        Err(msg) => Discovered {
            dir,
            builtin,
            manifest: None,
            executable: None,
            problem: Some(DiscoveryProblem::InvalidManifest(msg)),
        },
        Ok(m) => {
            let (executable, problem) = match resolve_entry(&dir, &m.entry[0]) {
                Ok(p) => (Some(p), None),
                Err(path) => (None, Some(DiscoveryProblem::NotInstalled { path })),
            };
            Discovered {
                dir,
                builtin,
                manifest: Some(m),
                executable,
                problem,
            }
        }
    }
}

/// Resolve `entry[0]` (§3.2): `./x` against the plugin directory (also accepts an absolute
/// path), anything else on `PATH`. `Err(path)` carries what was looked for.
pub fn resolve_entry(dir: &Path, entry0: &str) -> Result<PathBuf, PathBuf> {
    let candidate = Path::new(entry0);
    if entry0.starts_with("./") || entry0.starts_with("../") || candidate.is_absolute() {
        let p = if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            dir.join(candidate)
        };
        return if is_executable(&p) { Ok(p) } else { Err(p) };
    }
    if entry0.contains('/') {
        // A relative path that is not ./-prefixed: the spec says PATH lookup, but a path
        // with a slash can never be found on PATH; resolve against the directory instead.
        let p = dir.join(candidate);
        return if is_executable(&p) { Ok(p) } else { Err(p) };
    }
    let path_var = std::env::var_os("PATH").unwrap_or_default();
    std::env::split_paths(&path_var)
        .map(|d| d.join(entry0))
        .find(|p| is_executable(p))
        .ok_or_else(|| candidate.to_path_buf())
}

fn is_executable(p: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(p) else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}
