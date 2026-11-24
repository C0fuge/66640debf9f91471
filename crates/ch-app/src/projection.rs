use crate::{
    App,
    wire::{self, Failure, Result, Span, Thread},
};
use ch_core::{
    history::{Item, Page},
    host::AccountPhase,
    outbox::OutboxItem,
    search::SearchPage,
};
use ch_db::types::ConversationRow;
use ch_protocol::dto::AuthStep;
use serde_json::{Value, json};
use std::collections::BTreeMap;

const JS_MAX: u64 = (1 << 53) - 1;

pub(crate) async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|_| Failure::Internal)?
}

impl App {
    pub(crate) async fn read<T: Send + 'static>(
        &self,
        f: impl FnOnce(&ch_db::Db) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let db = self.db.clone();
        blocking(move || {
            let db = db.lock().unwrap_or_else(|e| e.into_inner());
            f(&db)
        })
        .await
    }
    pub(crate) async fn inventory(&self) -> Result<Value> {
        let accounts = self.core.accounts().list().await?;
        let stored = self.read(|db| Ok(ch_db::list_plugins(db.conn())?)).await?;
        let mut plugins = BTreeMap::new();
        for p in self.core.host().plugins() {
            let id = p.info.id;
            let m = p.info.manifest.as_ref();
            plugins.insert(
                id.clone(),
                json!({
                    "pluginId":id,"name":m.map_or(id.as_str(),|m|m.name.as_str()),
                    "brandColor":m.map(|m|&m.brand_color),"phase":wire::phase(&p.state),
                    "capabilities":p.state.capabilities().map(|c|c.as_ref()),
                    "authMethods":m.map(|m|m.auth_methods.as_slice()).unwrap_or_default(),
                    "settingsSchema":m.and_then(|m|m.settings_schema.as_ref())
                }),
            );
        }
        let mut rows = Vec::with_capacity(accounts.len());
        for a in accounts {
            plugins.entry(a.plugin_id.clone()).or_insert_with(||{
                let old=stored.iter().find(|p|p.plugin_id==a.plugin_id);
                json!({"pluginId":a.plugin_id,"name":old.map_or(&a.plugin_id,|p|&p.name),
                    "brandColor":old.and_then(|p|p.brand_color.as_ref()),"phase":"unavailable","capabilities":null,
                    "authMethods":[],"settingsSchema":null})
            });
            let info = self.core.host().account(&a.plugin_id, &a.account_id);
            let phase = match info.as_ref().map(|i| &i.phase) {
                Some(AccountPhase::Starting) => "starting",
                Some(AccountPhase::Running) => "running",
                Some(AccountPhase::Stopping) => "stopping",
                _ => "stopped",
            };
            rows.push(json!({
                "accountId":a.account_id,"pluginId":a.plugin_id,"label":a.label,"enabled":a.enabled,
                "phase":phase,"state":info.as_ref().map_or("disconnected",|i|i.state.as_str()),
                "reconnecting":info.is_some_and(|i|i.reconnecting)
            }));
        }
        Ok(json!({"plugins":plugins.into_values().collect::<Vec<_>>(),"accounts":rows}))
    }
}

pub(crate) fn chat(c: ConversationRow) -> Value {
    json!({"thread":Thread{account_id:c.account_id,chat_id:c.chat_id,topic_id:None},
        "title":c.title,"kind":c.kind,"archived":c.archived,"pinned":c.pinned.unwrap_or(false),
        "muted":c.muted,"canPost":c.can_post,"unreadCount":c.unread_count.clamp(0,JS_MAX as i64),
        "markedUnread":c.marked_unread.unwrap_or(false),"lastMessageAt":c.last_message_at.map(|t|t.to_string()),
        "lastMessagePreview":c.last_message_preview,"lastMessageKind":c.last_message_kind})
}

pub(crate) fn history(page: Page) -> Value {
    let items=page.items.into_iter().map(|i|match i{
        Item::Gap(g)=>json!({"kind":"gap","gapId":g.gap_id.to_string()}),
        Item::Message(m)=>{
            let media=m.media_kind();
            let entities:Vec<_>=m.entities.unwrap_or_default().into_iter()
                .filter(|e|e.offset<=JS_MAX && e.length<=JS_MAX).map(Span::from).collect();
            json!({"kind":"message","message":{"rowKey":m.id.to_string(),"messageId":m.message_id,
                "topicId":m.topic_id,"direction":m.direction,"sentAt":m.sent_at.to_string(),
                "senderName":m.sender_name.or(m.sender_title),"text":m.text,"entities":entities,
                "mediaKind":media,"edited":m.edited,"deleted":m.deleted_at.is_some(),"delivery":m.delivery,"isPinned":m.is_pinned.unwrap_or(false)}})
        }
    }).collect::<Vec<_>>();
    json!({"items":items,"moreOlder":page.more_older,"moreNewer":page.more_newer})
}

pub(crate) fn outbox(i: OutboxItem) -> Value {
    let unknown = i
        .last_error
        .as_ref()
        .and_then(|e| e.data.as_ref())
        .and_then(|d| d.reason.as_deref())
        == Some("outcome_unknown");
    json!({"id":i.client_id,"thread":Thread{account_id:i.account_id,chat_id:i.chat_id,topic_id:i.topic_id},
        "echoRowKey":i.echo_message_id.map(|id|id.to_string()),"state":i.state.as_str(),
        "nextAttemptAt":i.next_attempt_at.map(|t|t.to_string()),"outcomeUnknown":unknown})
}

pub(crate) fn search(p: SearchPage) -> Value {
    let hits=p.hits.into_iter().map(|h|{
        let spans=h.snippet.highlights.into_iter().map(|s|json!({"offset":s.offset,"length":s.length})).collect::<Vec<_>>();
        json!({"thread":Thread{account_id:h.account_id,chat_id:h.chat_id,topic_id:h.topic_id},
            "messageId":h.message_id,"sentAt":h.sent_at.to_string(),"snippet":h.snippet.text,"snippetHighlights":spans})
    }).collect::<Vec<_>>();
    json!({"hits":hits,"nextOffset":p.next_offset})
}

pub(crate) fn auth(step: &AuthStep) -> Value {
    json!({"flow":step.flow,"stepId":step.step_id,"kind":step.kind,"title":step.title,"hint":step.hint,
        "placeholder":step.placeholder,"data":step.data,"expiresAt":step.expires_at.map(|t|t.unix_millis().to_string()),
        "error":step.error,"minLength":step.min_length.map(|n|n.min(JS_MAX)),"maxLength":step.max_length.map(|n|n.min(JS_MAX)),"canResend":step.can_resend})
}
