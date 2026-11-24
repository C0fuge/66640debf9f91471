mod scaffold;

use ch_conformance::Options;
use clap::{Args, Parser, Subcommand};
use std::{error::Error, path::PathBuf, process::ExitCode, time::Duration};

#[derive(Parser)]
#[command(name = "ch", version, about = "Contact Hub plugin tools")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Plugin {
        #[command(subcommand)]
        command: PluginCommand,
    },
}
#[derive(Subcommand)]
enum PluginCommand {
    /// Exercise a plugin's manifest entry with a disposable test account.
    Test(Test),
    /// Generate a plugin from an official SDK template.
    New(scaffold::New),
}
#[derive(Args)]
struct Test {
    dir: PathBuf,
    #[arg(long)]
    json: bool,
    #[arg(short, long)]
    verbose: bool,
    /// Offline fake or recording account id. Enables interactive auth and mutating probes.
    #[arg(long)]
    fixture_account: Option<String>,
    /// Test setting, KEY=JSON (plain strings are accepted).
    #[arg(long, value_parser=pair)]
    setting: Vec<(String, String)>,
    /// Disposable test secret, KEY=VALUE.
    #[arg(long, value_parser=pair)]
    secret: Vec<(String, String)>,
    /// Answer to a test auth step, e.g. code=1234.
    #[arg(long, value_parser=pair)]
    auth: Vec<(String, String)>,
    /// Seconds to wait for the account to connect or request sign-in.
    #[arg(long, default_value_t = 15)]
    state_timeout: u32,
}

fn pair(raw: &str) -> Result<(String, String), String> {
    let (key, value) = raw
        .split_once('=')
        .filter(|(k, _)| !k.is_empty())
        .ok_or("expected KEY=VALUE")?;
    Ok((key.to_owned(), value.to_owned()))
}

async fn execute() -> Result<bool, Box<dyn Error>> {
    let Command::Plugin { command } = Cli::parse().command;
    match command {
        PluginCommand::New(args) => {
            scaffold::create(args)?;
            Ok(true)
        }
        PluginCommand::Test(args) => {
            let options = Options {
                fixture: args.fixture_account.is_some(),
                account_id: args
                    .fixture_account
                    .unwrap_or_else(|| "ch-conformance".into()),
                state_timeout: Duration::from_secs(args.state_timeout.into()),
                settings: args
                    .setting
                    .into_iter()
                    .map(|(k, v)| (k, serde_json::from_str(&v).unwrap_or(v.into())))
                    .collect(),
                secrets: args
                    .secret
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
                auth_answers: args.auth.into_iter().collect(),
            };
            let report = ch_conformance::run(&args.dir, &options).await?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print!("{}", report.render(args.verbose));
            }
            Ok(report.ok())
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match execute().await {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("ch: {e}");
            ExitCode::FAILURE
        }
    }
}
