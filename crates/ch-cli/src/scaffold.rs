//! Render the shipped templates, including when the CLI runs outside the checkout.
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use clap::{Args, ValueEnum};

#[derive(Clone, Copy, ValueEnum)]
pub enum Language {
    Python,
    Rust,
}

#[derive(Args)]
pub struct New {
    plugin_id: String,
    #[arg(long, value_enum, default_value = "python")]
    lang: Language,
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    author: Option<String>,
    #[arg(long)]
    dir: Option<PathBuf>,
    #[arg(long)]
    sdk_path: Option<PathBuf>,
    /// Allow a built-in plugin id.
    #[arg(long)]
    force: bool,
    /// Only write the scaffold.
    #[arg(long)]
    no_install: bool,
}

macro_rules! files {
    ($lang:literal; $($file:literal),+ $(,)?) => {
        &[$(($file, include_str!(concat!("../../../templates/", $lang, "/", $file)))),+]
    };
}

const PYTHON: &[(&str, &str)] = files!("python";
    "plugin.json", "pyproject.toml", "README.md", "tests/test_plugin.py",
    "src/{{module_name}}/__init__.py", "src/{{module_name}}/__main__.py", "src/{{module_name}}/plugin.py"
);
const RUST: &[(&str, &str)] = files!("rust";
    "plugin.json", "Cargo.toml", "README.md", "src/main.rs", "tests/plugin.rs"
);

fn default_author() -> String {
    Command::new("git")
        .args(["config", "user.name"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_owned())
        .unwrap_or_else(|| std::env::var("USER").unwrap_or_default())
}

fn checked(value: &str) -> Result<(), Box<dyn Error>> {
    if value.contains(['"', '\\']) || value.contains("{{") || value.chars().any(char::is_control) {
        return Err("template values must be single-line text without quotes, backslashes or template markers".into());
    }
    Ok(())
}

fn local_checkout() -> Option<PathBuf> {
    // Look alongside the running invocation, so a distributed binary keeps its git dependency.
    let cwd = std::env::current_dir().ok()?;
    let exe = std::env::current_exe().ok()?;
    cwd.ancestors()
        .chain(exe.ancestors())
        .find(|root| {
            root.join("sdk/python/pyproject.toml").is_file()
                && root.join("crates/ch-plugin-sdk/Cargo.toml").is_file()
        })
        .map(Path::to_path_buf)
}

pub fn create(args: New) -> Result<(), Box<dyn Error>> {
    let id = &args.plugin_id;
    if !ch_protocol::manifest::is_valid_plugin_id(id) {
        return Err("plugin id must match ^[a-z][a-z0-9_]{1,31}$".into());
    }
    if !args.force && matches!(id.as_str(), "fake" | "telegram" | "whatsapp" | "max") {
        return Err("built-in plugin id (use --force to select it)".into());
    }
    let display = args.name.unwrap_or_else(|| {
        id.split('_')
            .filter(|s| !s.is_empty())
            .map(|s| format!("{}{}", s[..1].to_ascii_uppercase(), &s[1..]))
            .collect::<Vec<_>>()
            .join(" ")
    });
    let author = args.author.unwrap_or_else(default_author);
    checked(&display)?;
    checked(&author)?;
    if display.trim().is_empty() {
        return Err("display name must contain text".into());
    }
    let dest = args.dir.unwrap_or_else(|| PathBuf::from(id));
    if dest.exists() && fs::read_dir(&dest)?.next().is_some() {
        return Err(format!("destination {} is not empty", dest.display()).into());
    }
    let (files, dep, sdk_dir, sdk_manifest) = match args.lang {
        Language::Python => (PYTHON, "contact-hub-sdk", "sdk/python", "pyproject.toml"),
        Language::Rust => (RUST, "ch-plugin-sdk", "crates/ch-plugin-sdk", "Cargo.toml"),
    };
    let sdk = args
        .sdk_path
        .or_else(local_checkout)
        .map(|root| -> Result<String, Box<dyn Error>> {
            let path = root.join(sdk_dir).canonicalize()?;
            if !path.join(sdk_manifest).is_file() {
                return Err(format!("missing {sdk_manifest} in {}", path.display()).into());
            }
            Ok(serde_json::to_string(
                path.to_str().ok_or("SDK path must be UTF-8")?,
            )?)
        })
        .transpose()?;
    let module = format!("{id}_plugin");
    let krate = format!("{}-plugin", id.replace('_', "-"));
    let render = |s: &str| {
        s.replace("{{plugin_id}}", id)
            .replace("{{display_name}}", &display)
            .replace("{{author}}", &author)
            .replace("{{module_name}}", &module)
            .replace("{{crate_name}}", &krate)
    };
    for &(file, source) in files {
        let path = dest.join(render(file));
        let mut contents = String::new();
        for line in source.split_inclusive('\n') {
            if let Some((git, _)) = line.split_once("# ch:sdk-source") {
                match &sdk {
                    Some(s) => contents.push_str(&format!("{dep} = {{ path = {s} }}\n")),
                    None => contents.push_str(&format!("{}\n", git.trim_end())),
                }
            } else {
                contents.push_str(&render(line));
            }
        }
        if contents.contains("{{") {
            return Err(format!("unresolved placeholder in {file}").into());
        }
        fs::create_dir_all(path.parent().ok_or("invalid template path")?)?;
        use std::io::Write;
        fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?
            .write_all(contents.as_bytes())?;
    }
    println!("Created {}", dest.display());
    if !args.no_install {
        let (tool, commands): (&str, &[&[&str]]) = match args.lang {
            Language::Python => ("uv", &[&["sync"], &["run", "pytest"]]),
            Language::Rust => ("cargo", &[&["build"], &["test"]]),
        };
        let profile = tempfile::tempdir()?;
        for command in commands {
            let mut process = Command::new(tool);
            for key in [
                "CH_DATA_DIR",
                "XDG_DATA_HOME",
                "XDG_CONFIG_HOME",
                "XDG_STATE_HOME",
                "XDG_CACHE_HOME",
                "XDG_RUNTIME_DIR",
            ] {
                let path = profile.path().join(key);
                fs::create_dir_all(&path)?;
                process.env(key, path);
            }
            process.env(
                "DBUS_SESSION_BUS_ADDRESS",
                format!("unix:path={}/no-bus", profile.path().display()),
            );
            process.env("PYTHON_KEYRING_BACKEND", "keyring.backends.null.Keyring");
            let status = process.args(*command).current_dir(&dest).status()?;
            if !status.success() {
                return Err(format!("{tool} {} exited with {status}", command.join(" ")).into());
            }
        }
    }
    match args.lang {
        Language::Rust => println!(
            "Run cargo build --release in the plugin directory before testing its manifest entry."
        ),
        Language::Python if args.no_install => {
            println!("Run uv sync in the plugin directory to prepare its manifest entry.")
        }
        _ => {}
    }
    println!(
        "Run ch plugin test {:?}\nInstall in $XDG_DATA_HOME/contact-hub-ng/plugins/{id}/",
        dest
    );
    Ok(())
}
