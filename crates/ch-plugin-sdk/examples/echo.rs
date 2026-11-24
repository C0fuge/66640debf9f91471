//! A minimal Contact Hub plugin: one chat, `echo`, that answers every message with itself.
//!
//! Run it under the core, or by hand (`cargo run --example echo`) typing NDJSON frames.
use std::sync::atomic::{AtomicU64, Ordering};

use ch_plugin_sdk::prelude::*;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn message(direction: Direction, text: &str) -> Message {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed).to_string();
    Message::new(id, "echo", direction, Timestamp::now()).with_text(text)
}

#[tokio::main]
async fn main() -> Result<(), SdkError> {
    ch_plugin_sdk::init_logging();
    PluginBuilder::new()
        .version(env!("CARGO_PKG_VERSION"))
        .on_account_start(|acct: Account| async move {
            acct.connected(None)?;
            let chat = Conversation::new("echo", ConversationKind::Dm).with_title("Echo");
            acct.conversations_upsert(vec![chat])?;
            acct.conversations_synced(vec!["echo".into()])?;
            Ok(())
        })
        .method(
            "messages.fetch_recent",
            |_c: Call<MessagesFetchRecentParams>| async { Ok(PagedMessagesResult::default()) },
        )
        .method("messages.send", |c: Call<MessagesSendParams>| async move {
            if c.params.chat_id != "echo" {
                return Err(error::not_found("chat", "only the echo chat exists"));
            }
            let sent = message(Direction::Out, &c.params.text);
            c.account()?
                .messages_upsert(vec![message(Direction::In, &c.params.text)])?;
            Ok(MessageResult { message: sent })
        })
        .build()?
        .run_stdio()
        .await
}
