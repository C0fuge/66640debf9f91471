//! One desktop or development-server session over the Rust core.
mod bridge;
mod actions;
mod polls;
mod reactions;
mod cache_file;
mod save_file;
mod media;
pub use media::{SaveDialog, SaveDialogCallback, SaveDialogError};
#[cfg(target_os = "linux")]
mod ingress_fs;
#[cfg(target_os = "linux")]
mod ingress;
#[cfg(not(target_os = "linux"))]
#[path = "ingress_unsupported.rs"]
mod ingress;
mod file_dialog;
pub use file_dialog::OpenFileDialog;
mod events;
mod projection;
mod wire;

use ch_core::{
    history::{History, HistoryConfig},
    host::MemorySecretStore,
    outbox::{Outbox, OutboxPolicy},
    runtime::{Core, CoreConfig, NewAccount, default_data_dir},
    search::Search,
    secrets::KeyringSecretStore,
};
pub use events::Invalidation;
use events::Steps;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
};
use tempfile::TempDir;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use wire::{Failure, Request};

pub struct App {
    core: Arc<Core>,
    media: Arc<media::MediaJobs>,
    ingress: Arc<ingress::Service>,
    reactions: Arc<reactions::Service>,
    history: History,
    search: Search,
    db: Arc<Mutex<ch_db::Db>>,
    outbox: Outbox,
    steps: Steps,
    auth_locks: Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<()>>>>,
    action_locks: Mutex<BTreeMap<String, Weak<tokio::sync::Mutex<()>>>>,
    changes: broadcast::Sender<Invalidation>,
    stop: CancellationToken,
    tasks: tokio::sync::Mutex<AppTasks>,
    _lock: Arc<std::fs::File>,
    _demo: Option<Arc<TempDir>>,
}

struct AppTasks {
    pump: Option<tokio::task::JoinHandle<()>>,
    teardown: Option<tokio::task::JoinHandle<()>>,
}

type Startup<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

impl App {
    pub async fn desktop(builtin_plugins: PathBuf) -> Startup<Self> {
        let data =
            default_data_dir().ok_or_else(|| io::Error::other("no application data directory"))?;
        let mut config = CoreConfig::new(data, Arc::new(KeyringSecretStore::open().await?));
        config.builtin_plugins_dir = Some(builtin_plugins);
        Self::open(config, None).await
    }

    /// The demo exposes only the fake manifest. All writes and secrets are disposable.
    pub async fn demo(repo: &Path) -> Startup<Self> {
        let root = repo.canonicalize()?;
        let temp = tempfile::Builder::new()
            .prefix("contact-hub-demo-")
            .tempdir()?;
        let dir = temp.path().join("plugins/fake");
        std::fs::create_dir_all(&dir)?;
        let fake = root.join("plugins/fake");
        let sdk = root.join("sdk/python/src");
        if !sdk.join("contact_hub_sdk").is_dir() || !fake.join("fake_plugin").is_dir() {
            return Err(io::Error::other(
                "expected the rebuild source tree with the fake plugin and SDK",
            )
            .into());
        }
        let mut manifest: Value =
            serde_json::from_slice(&std::fs::read(fake.join("plugin.json"))?)?;
        if manifest["id"] != "fake" {
            return Err(io::Error::other("invalid fake manifest").into());
        }
        manifest["entry"] = json!(["python3", "-B", "-s", "-m", "fake_plugin"]);
        std::fs::write(dir.join("plugin.json"), serde_json::to_vec(&manifest)?)?;
        std::fs::copy(fake.join("icon.svg"), dir.join("icon.svg"))?;
        let mut config = CoreConfig::new(
            temp.path().to_path_buf(),
            Arc::new(MemorySecretStore::new()),
        );
        let python_path = std::env::join_paths([fake, sdk])?
            .into_string()
            .map_err(|_| io::Error::other("invalid plugin path"))?;
        config.host.env.insert("PYTHONPATH".into(), python_path);
        config.host.env.insert(
            "CH_DATA_DIR".into(),
            temp.path().to_string_lossy().into_owned(),
        );
        for key in [
            "XDG_CONFIG_HOME",
            "XDG_DATA_HOME",
            "XDG_CACHE_HOME",
            "XDG_STATE_HOME",
            "XDG_RUNTIME_DIR",
        ] {
            let dir = temp.path().join(key);
            std::fs::create_dir(&dir)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
            }
            config
                .host
                .env
                .insert(key.into(), dir.to_string_lossy().into_owned());
        }
        let app = Self::open(config, Some(temp)).await?;
        let account = NewAccount::new("fake")
            .label("Demo")
            .settings(json!({"start_state":"connected","seed":7}));
        if let Err(e) = app.core.accounts().add(account).await {
            app.shutdown().await;
            return Err(e.into());
        }
        Ok(app)
    }

    async fn open(mut config: CoreConfig, demo: Option<TempDir>) -> Startup<Self> {
        std::fs::create_dir_all(&config.data_dir)?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(config.data_dir.join("app.lock"))?;
        file.try_lock()
            .map_err(|_| io::Error::other("cannot lock the application data directory"))?;
        config.autostart = false;
        let core = Arc::new(Core::open(config).await?);
        let c = core.clone();
        let (db, search) = tokio::task::spawn_blocking(move || -> Startup<_> {
            Ok((c.open_db()?, Search::from_core(&c)?))
        })
        .await??;
        let history = History::from_core(&core, HistoryConfig::default());
        let outbox = Outbox::for_core(&core, OutboxPolicy::default());
        let (changes, _) = broadcast::channel(256);
        let steps = Steps::default();
        let stop = CancellationToken::new();
        let db = Arc::new(Mutex::new(db));
        let media = media::MediaJobs::new(core.clone(), db.clone(), changes.clone())?;
        let reactions = Arc::new(reactions::Service::new()?);
        let ingress = ingress::Service::new(core.clone(), db.clone(), outbox.clone(), changes.clone()).await?;
        let pump = tokio::spawn(events::pump(
            core.subscribe(),
            outbox.subscribe(),
            steps.clone(),
            media.clone(),
            reactions.clone(),
            ingress.clone(),
            changes.clone(),
            stop.clone(),
        ));
        let app = Self {
            core,
            ingress,
            media,
            reactions,
            history,
            outbox,
            search,
            db,
            steps,
            auth_locks: Mutex::default(),
            action_locks: Mutex::default(),
            changes,
            stop,
            tasks: tokio::sync::Mutex::new(AppTasks { pump: Some(pump), teardown: None }),
            _lock: Arc::new(file),
            _demo: demo.map(Arc::new),
        };
        if let Err(e) = app.outbox.start() {
            app.shutdown().await;
            return Err(e.into());
        }
        if let Err(e) = app.core.accounts().start_wanted().await {
            app.shutdown().await;
            return Err(e.into());
        }
        Ok(app)
    }

    /// Install the native open chooser before sharing the application with clients.
    pub fn set_open_file_dialog(&mut self, provider: Arc<dyn OpenFileDialog>) {
        self.ingress.install(provider);
    }

    pub fn shutdown_signal(&self) -> CancellationToken {
        self.stop.child_token()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Invalidation> {
        self.changes.subscribe()
    }

    pub async fn request(&self, value: Value) -> Value {
        let Ok(request) = serde_json::from_value::<Request>(value) else {
            return Failure::Invalid.reply();
        };
        if self.stop.is_cancelled() {
            return Failure::Unavailable.reply();
        }
        match self.dispatch(request).await {
            Ok(value) => json!({"ok":true,"value":value}),
            Err(e) => e.reply(),
        }
    }

    pub async fn shutdown(&self) {
        let mut tasks = self.tasks.lock().await;
        if let Some(pump) = tasks.pump.take() {
            self.stop.cancel();
            self.ingress.close();
            self.media.close();
            self.reactions.close();
            let (ingress, outbox, core, reactions) = (self.ingress.clone(), self.outbox.clone(), self.core.clone(), self.reactions.clone());
            let directory = (self._lock.clone(), self._demo.clone());
            // Teardown owns the directory and all drains. Cancelling any waiter
            // leaves the same task running; another caller joins its completion.
            tasks.teardown = Some(tokio::spawn(async move {
                let _directory = directory;
                ingress.drain().await;
                outbox.shutdown().await;
                core.shutdown().await;
                reactions.drain().await;
                let _ = pump.await;
            }));
        }
        if let Some(task) = tasks.teardown.as_mut() {
            let _ = task.await;
            tasks.teardown = None;
        }
    }
}
