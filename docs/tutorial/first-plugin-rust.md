# Write your first plugin in Rust

This is the Rust version of [Write your first plugin](first-plugin.md). It builds
the same Parrot plugin (one chat with Polly, who repeats what you say) with the Rust
SDK, `crates/ch-plugin-sdk`. Read the Python tutorial first for the ideas: what a
plugin is, the manifest, capabilities, errors and sign-in. This page only covers
what is different in Rust.

The finished crate is in [`example/rust/`](example/rust/). Every snippet below is
copied from it, and `cargo test` passes.

## 1. The crate

```toml
[package]
name = "parrot"
version = "0.1.0"
edition = "2024"
publish = false

[dependencies]
ch-plugin-sdk = { path = "../../../../crates/ch-plugin-sdk" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }

[dev-dependencies]
serde_json = "1"
tempfile = "3"

# A standalone crate, not a member of the repository's workspace.
[workspace]
```

Point the `path` at `crates/ch-plugin-sdk` in your checkout. `plugin.json` is the
same as the Python one except for the command that starts the plugin:
`"entry": ["./target/release/parrot"]`. Building it is the job of whoever installs the
plugin directory (you, or an installer running `cargo build --release`): the core only
starts `entry`, and shows the plugin as "not installed" while that file is missing.

## 2. The plugin definition

In Rust you don't subclass anything. `PluginBuilder` collects the version, the
capabilities, the lifecycle hooks and one handler per method, and then serves stdio:

```rust
#[tokio::main]
async fn main() -> Result<(), SdkError> {
    ch_plugin_sdk::init_logging();
    PluginBuilder::new()
        .version(env!("CARGO_PKG_VERSION"))
        .capability("messages.get")
        .capability("send.silent")
        .capability("auth.interactive")
        .on_account_start(on_start)
        .method("auth.begin", auth_begin)
        .method("auth.submit", auth_submit)
        .method("auth.cancel", auth_cancel)
        .method("messages.fetch_recent", fetch_recent)
        .method("messages.get", get)
        .method("messages.send", send)
        .build()?
        .run_stdio()
        .await
}
```

- `init_logging()` sends `tracing` output to stderr, at the level in `CH_LOG_LEVEL`.
  Never print to stdout.
- `build()` checks the definition. It fails if a required method has no handler,
  or if a declared capability is missing one of the methods it covers.
- `auth.interactive` is declared by hand, and the three `auth.*` methods are
  registered like any others.

Each handler is an `async fn` that takes a `Call<Params>` and returns
`HandlerResult<Result>` (that is, `Result<Result, PluginError>`), using the generated
types from `ch_protocol`
(`use ch_plugin_sdk::prelude::*` brings in all of them). `c.params` is the typed
params, and `c.account()?` is the started account.

## 3. Per-account state

There is no `self`, so state lives on the account. `Account::set_data` stores one
value of any type, and `Account::data::<T>()` gets it back as an `Arc<T>`:

```rust
/// Everything one account knows, kept with the account (`Account::set_data`).
struct State {
    history: Vec<Message>, // oldest first; message id = position + 1
    sends: Vec<Instant>,   // recent sends, for the rate limit
    auth: AuthFlow,
    phone: Option<String>,
}

fn state(acct: &Account) -> Arc<Mutex<State>> {
    acct.data::<Mutex<State>>().expect("set in account.start")
}
```

`on_start` fills it, then either connects or asks the user to sign in:

```rust
    acct.set_data(Mutex::new(st));
    if acct.secret("session").is_some() {
        connect(&acct)
    } else {
        Ok(acct.status(AccountState::AuthRequired)?)
    }
```

Event emitters such as `acct.connected(me)`, `acct.conversations_upsert(...)` and
`acct.messages_upsert(...)` are plain (not `async`) methods that queue the frame.
The `?` turns an `EmitError` into an error response.

## 4. History and sending

Paging works the same way as in Python: no cursor means the newest page, messages
come newest first, and `next_cursor` is left out on the last page:

```rust
async fn fetch_recent(c: Call<MessagesFetchRecentParams>) -> HandlerResult<PagedMessagesResult> {
    chat(&c.params.chat_id)?;
    let st = state(c.account()?);
    let st = st.lock().unwrap();
    let end = match &c.params.cursor {
        Some(cur) => cur
            .parse()
            .map_err(|_| error::invalid_params("cursor", "bad cursor"))?,
        None => st.history.len(),
    };
    let start = end.saturating_sub(c.params.limit as usize);
    let messages = st.history[start..end].iter().rev().cloned().collect(); // newest first
    Ok(PagedMessagesResult {
        messages,
        next_cursor: (start > 0).then(|| start.to_string()),
    })
}
```

Errors are `PluginError`s (a spec `ErrorObject` in a `Box`, so results stay small)
built by the functions in `ch_plugin_sdk::error`: `not_found`, `flood_wait`,
`not_supported`, `invalid_params`, `forbidden`, `network`, and so on. `send` returns
`flood_wait` when you talk too fast (and `chat`, which it calls first, `not_found`):

```rust
async fn send(c: Call<MessagesSendParams>) -> HandlerResult<MessageResult> {
    chat(&c.params.chat_id)?;
    let acct = c.account()?;
    let limit: usize = acct.settings().require("max_per_minute")?; // default: plugin.json
    let st = state(acct);
    let mut st = st.lock().unwrap();
    let minute = Duration::from_secs(60);
    st.sends.retain(|t| t.elapsed() < minute);
    if st.sends.len() >= limit {
        let wait = minute - st.sends[0].elapsed();
        return Err(error::flood_wait(wait.as_secs() + 1));
    }
```

What the SDK does for you, as in Python:

- **Parameter capabilities.** Some parameters need a capability of their own
  (`schedule_at` needs `send.schedule`, `silent` needs `send.silent`). The registry
  lists them (`gated_params`), and the SDK answers `NOT_SUPPORTED` for one you did not
  declare before your handler runs. So `send` never sees a `schedule_at`, and Parrot
  gets `messages.edit` refused the same way, for its undeclared method capability.
- **Settings defaults.** The SDK reads `plugin.json` and fills each `settings_schema`
  `default` the user did not set, as the core does. That is why `send` can `require`
  `max_per_minute`.

Chats, topics and messages are *merge-updated*: a field you leave out means
"unchanged", so an update carries only what changed (a later read sends just
`unread_count`). The prelude gives `Conversation`, `Message`, `Topic`, `Contact`,
`Folder` and `Draft` a `new` with the required fields, every other field unset, and
`with_*` setters. `connect` builds Polly's chat that way:

```rust
    let mut conv = Conversation::new(CHAT, ConversationKind::Dm)
        .with_title("Polly")
        .with_peer_user_id("polly");
    conv.last_message_at = last;
    acct.conversations_upsert(vec![conv])?;
```

## 5. Interactive sign-in

The Rust `AuthFlow` doesn't wait for answers. Each `auth.submit` is a separate
call, so the plugin keeps the flow in its state and decides what to do from the
step that is on screen:

```rust
async fn auth_submit(c: Call<AuthSubmitParams>) -> HandlerResult<Empty> {
    let acct = c.account()?;
    let shared = state(acct);
    let phone = {
        let mut st = shared.lock().unwrap();
        if !st.auth.accepts(&c.params) {
            return Ok(Empty {}); // a stale step: answer {} and ignore it (spec 7.2)
        }
        match st.auth.current().map(|s| s.kind.clone()) {
            Some(AuthStepKind::Phone) => {
                st.phone = Some(c.params.value.clone());
                st.auth
                    .ask(AuthStepKind::Code)
                    .title("Enter the code")
                    .hint(format!("Polly's code is always {DEMO_CODE}."))
                    .length(Some(4), Some(4))
                    .send()?;
                return Ok(Empty {});
            }
            _ if c.params.value != DEMO_CODE => {
                st.auth.reject("Wrong code, try again.")?;
                return Ok(Empty {});
            }
            _ => st.phone.clone().unwrap_or_default(),
        }
    };
    acct.set_secret("session", format!("parrot:{phone}"))
        .await?;
    shared.lock().unwrap().auth.done()?;
    connect(acct)?;
    Ok(Empty {})
}
```

- `reject(...)` asks for the same step again with an error message. A wrong code is
  never an error response.
- The mutex guard is dropped before `set_secret(...).await`. A `std::sync::MutexGuard`
  held across an `.await` makes the future not `Send`, and the code won't compile.
- `auth_cancel` calls `AuthFlow::cancel()`, which reports `auth_required` again.

## 6. Test it

The same hand session works against the binary:

```sh
cargo build
while read -r line; do echo "$line"; sleep 0.2; done < ../python/session.ndjson | target/debug/parrot
```

[`tests/parrot.rs`](example/rust/tests/parrot.rs) is the Rust version of the
pytest file. It starts the binary (`env!("CARGO_BIN_EXE_parrot")`) with
`CH_DATA_DIR` and `XDG_DATA_HOME` in a temporary folder, and lets the SDK's
`ch_plugin_sdk::testing::FakeCore` play the core over stdio. Like the real core it
fills settings defaults from `plugin.json`, acknowledges events, and fails the test
on a malformed frame or an event for an account that is not started. The tests check
sign-in, paging, the error codes and shutdown:

```rust
    let mut later = send("polly", "x");
    later["schedule_at"] = json!("2030-01-01T00:00:00.000Z");
    assert_eq!(
        code(core.call("messages.send", later).await),
        ErrorCode::NotSupported
    );
```

```sh
cargo test
```

```
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

`cargo clippy --all-targets -- -D warnings` is clean too: handlers fail with the
small, boxed `PluginError`, so the `result_large_err` lint has nothing to say.

To try it in the app, put the built directory in `~/.local/share/contact-hub-ng/plugins/`
(`$XDG_DATA_HOME/contact-hub-ng/plugins/parrot/`), as in
[the Python tutorial](first-plugin.md).

## Where to go next

The list at the end of [the Python tutorial](first-plugin.md#14-where-to-go-next)
applies here too. For Rust in particular: the crate docs (`cargo doc -p
ch-plugin-sdk --open` from the repository root), `crates/ch-plugin-sdk/examples/echo.rs`,
and the end-to-end tests in `crates/ch-plugin-sdk/tests/e2e.rs`, which use
`FakeCore` to show cancellation, progress, acked events and the EOF rules.
