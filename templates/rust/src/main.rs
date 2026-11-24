//! {{display_name}} plugin for Contact Hub.
//!
//! `ch-plugin-sdk` does the protocol work: `initialize`, `ping`, `shutdown`,
//! `account.start`/`account.stop`, capability checks (methods and gated params), settings
//! defaults from `plugin.json`, and error mapping. You write the platform code.
//! Spec: `docs/plugin-api.md` in the Contact Hub repo.
//!
//! Logs go to stderr (`tracing`); stdout belongs to the protocol, so never `println!`.

use std::sync::atomic::{AtomicU64, Ordering};

use ch_plugin_sdk::prelude::*;

#[tokio::main]
async fn main() -> Result<(), SdkError> {
    ch_plugin_sdk::init_logging();
    PluginBuilder::new()
        .version(env!("CARGO_PKG_VERSION"))
        // Capabilities (spec section 14). The core only calls optional methods and only
        // sends gated params (e.g. `silent` on messages.send) that are declared here; the
        // SDK answers NOT_SUPPORTED for any other.
        // TODO: declare what {{display_name}} supports, e.g. .capability("send.silent").
        .on_account_start(on_account_start)
        .on_account_stop(on_account_stop)
        .method("messages.fetch_recent", fetch_recent)
        .method("messages.send", send)
        // Interactive sign-in (spec 7): uncomment this block, the block at the end of the
        // file and the check in connect(), and set "auth_methods": ["token"] in plugin.json.
        // BEGIN auth example
        // .capability("auth.interactive")
        // .method("auth.begin", auth_begin)
        // .method("auth.submit", auth_submit)
        // .method("auth.cancel", auth_cancel)
        // END auth example
        .build()?
        .run_stdio()
        .await
}

/// Must return fast, without waiting for the network (spec 4.2): connect on a task and
/// report progress through `status`.
async fn on_account_start(acct: Account) -> HandlerResult<()> {
    let server: String = acct.settings().require("server")?; // default: plugin.json
    tokio::spawn(async move {
        tokio::select! {
            _ = acct.cancelled() => {} // account.stop or shutdown
            r = connect(&acct, &server) => {
                if let Err(e) = r {
                    tracing::debug!("connect stopped: {e}");
                }
            }
        }
    });
    Ok(())
}

async fn connect(acct: &Account, server: &str) -> Result<(), EmitError> {
    // TODO: connect to {{display_name}} at `server` with your client library.
    //   unreachable   -> acct.status(AccountState::Unavailable) and keep retrying with backoff
    //   unrecoverable -> acct.status_with(AccountState::Error, Some("why".into()), false, None)
    // BEGIN auth example
    // if acct.secret("token").is_none() {
    //     return acct.status(AccountState::AuthRequired); // the core offers "Sign in"
    // }
    // END auth example
    tracing::info!("connected to {server}");
    acct.connected(None)?;
    // TODO: then push the chat list and keep it fresh:
    //   acct.conversations_upsert(vec![Conversation::new(id, ConversationKind::Dm).with_title(..)])?;
    //   acct.conversations_synced(vec![...])?;
    //   and on every incoming message: acct.messages_upsert(vec![...])?;
    // Chats, topics and messages are merge-updated: later updates carry only the fields
    // that changed (Conversation::new(id, kind).with_unread_count(n)).
    Ok(())
}

async fn on_account_stop(_acct: Account) {
    // TODO: close sockets and flush the session. The account's token is already cancelled.
}

async fn fetch_recent(_c: Call<MessagesFetchRecentParams>) -> HandlerResult<PagedMessagesResult> {
    // TODO: return the newest `c.params.limit` messages of `c.params.chat_id` (newest
    // first), and a `next_cursor` for the page before them, or None at the start of history.
    Ok(PagedMessagesResult::default())
}

async fn send(c: Call<MessagesSendParams>) -> HandlerResult<MessageResult> {
    // TODO: send `c.params.text` to `c.params.chat_id` on {{display_name}} and return the
    // message as the platform stored it (its real id and timestamp). Fail with the
    // `error::*` helpers: error::flood_wait(secs), error::not_found(..), error::network(..).
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    let id = format!("local-{}", NEXT_ID.fetch_add(1, Ordering::Relaxed));
    let message = Message::new(id, c.params.chat_id, Direction::Out, Timestamp::now())
        .with_text(c.params.text)
        .with_delivery(Delivery::Sent);
    Ok(MessageResult { message })
}

// BEGIN auth example
// use std::sync::{Arc, Mutex};
//
// /// The account's sign-in flow, kept in the account's data slot.
// fn sign_in_flow(acct: &Account) -> Arc<Mutex<AuthFlow>> {
//     acct.data::<Mutex<AuthFlow>>()
//         .unwrap_or_else(|| acct.set_data(Mutex::new(AuthFlow::sign_in(acct.clone()))))
// }
//
// async fn auth_begin(c: Call<AuthBeginParams>) -> HandlerResult<Empty> {
//     let flow = sign_in_flow(c.account()?);
//     let mut flow = flow.lock().unwrap();
//     flow.ask(AuthStepKind::Text).title("API token").hint("From your {{display_name}} settings").send()?;
//     Ok(Empty::default())
// }
//
// async fn auth_submit(c: Call<AuthSubmitParams>) -> HandlerResult<Empty> {
//     let acct = c.account()?.clone();
//     let flow = sign_in_flow(&acct);
//     if !flow.lock().unwrap().accepts(&c.params) {
//         return Ok(Empty::default()); // a stale step: answer {} and ignore it (spec 7.2)
//     }
//     let token = c.params.value.trim().to_owned();
//     if token.is_empty() {
//         // TODO: check the token against the platform
//         flow.lock().unwrap().reject("That token was rejected")?;
//         return Ok(Empty::default());
//     }
//     acct.set_secret("token", token).await?; // comes back in acct.secrets() on every start
//     flow.lock().unwrap().signed_in(None)?;
//     Ok(Empty::default())
// }
//
// async fn auth_cancel(c: Call<AuthCancelParams>) -> HandlerResult<Empty> {
//     sign_in_flow(c.account()?).lock().unwrap().cancel()?;
//     Ok(Empty::default())
// }
// END auth example
