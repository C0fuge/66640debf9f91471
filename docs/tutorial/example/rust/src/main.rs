//! Parrot: a toy Contact Hub plugin. One chat with Polly, who repeats what you say.
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ch_plugin_sdk::prelude::*;

const CHAT: &str = "polly";
const DEMO_CODE: &str = "1234";

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

fn nickname(acct: &Account) -> String {
    acct.settings()
        .get_or("nickname", "Me".to_string())
        .unwrap_or_default()
}

/// Append a message to the history and return it.
fn add(
    acct: &Account,
    st: &mut State,
    direction: Direction,
    text: &str,
    silent: Option<bool>,
) -> Message {
    let (sender_id, sender_name) = match direction {
        Direction::In => ("polly".to_string(), "Polly".to_string()),
        _ => ("me".to_string(), nickname(acct)),
    };
    let id = (st.history.len() + 1).to_string();
    let mut msg = Message::new(id, CHAT, direction, Timestamp::now())
        .with_text(text)
        .with_sender_id(sender_id)
        .with_sender_name(sender_name);
    msg.silent = silent;
    st.history.push(msg.clone());
    msg
}

fn chat(chat_id: &str) -> HandlerResult<()> {
    if chat_id == CHAT {
        Ok(())
    } else {
        Err(error::not_found("chat", format!("no chat {chat_id:?}")))
    }
}

fn connect(acct: &Account) -> HandlerResult<()> {
    let me = Contact::new("me").with_display_name(nickname(acct));
    acct.connected(Some(me))?;
    let polly = Contact::new("polly")
        .with_display_name("Polly")
        .with_is_bot(true);
    acct.contacts_upsert(vec![polly])?;
    let last = state(acct)
        .lock()
        .unwrap()
        .history
        .last()
        .map(|m| m.sent_at);
    let mut conv = Conversation::new(CHAT, ConversationKind::Dm)
        .with_title("Polly")
        .with_peer_user_id("polly");
    conv.last_message_at = last;
    acct.conversations_upsert(vec![conv])?;
    acct.conversations_synced(vec![CHAT.into()])?;
    Ok(())
}

async fn on_start(acct: Account) -> HandlerResult<()> {
    let mut st = State {
        history: vec![],
        sends: vec![],
        auth: AuthFlow::sign_in(acct.clone()),
        phone: None,
    };
    let start = Timestamp::now().unix_millis() - 3_600_000;
    for i in 0..50 {
        add(
            &acct,
            &mut st,
            Direction::In,
            &format!("Squawk #{}!", i + 1),
            None,
        );
        let at = Timestamp::from_unix_millis(start + 60_000 * i as i64).expect("in range");
        st.history[i].sent_at = at;
    }
    acct.set_data(Mutex::new(st));
    if acct.secret("session").is_some() {
        connect(&acct)
    } else {
        Ok(acct.status(AccountState::AuthRequired)?)
    }
}

async fn auth_begin(c: Call<AuthBeginParams>) -> HandlerResult<Empty> {
    let st = state(c.account()?);
    let mut st = st.lock().unwrap();
    st.auth
        .ask(AuthStepKind::Phone)
        .title("Your phone number")
        .placeholder("+15550100")
        .send()?;
    Ok(Empty {})
}

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

async fn auth_cancel(c: Call<AuthCancelParams>) -> HandlerResult<Empty> {
    state(c.account()?).lock().unwrap().auth.cancel()?;
    Ok(Empty {})
}

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

async fn get(c: Call<MessagesGetParams>) -> HandlerResult<OptionalMessageResult> {
    chat(&c.params.chat_id)?;
    let st = state(c.account()?);
    let st = st.lock().unwrap();
    let i = c.params.message_id.parse::<usize>().unwrap_or(0);
    Ok(OptionalMessageResult {
        message: i.checked_sub(1).and_then(|i| st.history.get(i)).cloned(),
    })
}

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
    st.sends.push(Instant::now());
    let sent = add(
        acct,
        &mut st,
        Direction::Out,
        &c.params.text,
        c.params.silent,
    );
    let reply = add(
        acct,
        &mut st,
        Direction::In,
        &format!("Squawk! {}", c.params.text),
        None,
    );
    acct.messages_upsert(vec![reply])?;
    Ok(MessageResult { message: sent })
}

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
