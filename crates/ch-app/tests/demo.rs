//! Uses isolated temporary app state and the source-tree Python fake plugin.
use ch_app::App;
use serde_json::{Value, json};
use std::{path::Path, time::Duration};

async fn call(app: &App, method: &str, params: Value) -> Value {
    let reply = app.request(json!({"method":method,"params":params})).await;
    assert_eq!(reply["ok"], true, "{method}: {reply}");
    reply["value"].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fake_chat_send_and_scope() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let app = App::demo(&repo).await.expect("isolated fake runtime");
    let mut changes = app.subscribe();
    let inventory = call(&app, "inventory", json!({})).await;
    let id = &inventory["accounts"][0]["accountId"];
    let chats = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let rows = call(&app, "listChats", json!({"accountId":id,"archived":false})).await;
            if rows.as_array().is_some_and(|a| a.len() >= 2) {
                break rows;
            }
            let _ = changes.recv().await;
        }
    })
    .await
    .expect("fake initial sync");
    let thread = &chats
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["kind"] == "dm" && r["canPost"] != false)
        .expect("writable direct chat")["thread"];
    let other = &chats
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["thread"] != *thread)
        .unwrap()["thread"];
    let mut incomplete = thread.clone();
    assert_eq!(
        incomplete.as_object_mut().unwrap().remove("topicId"),
        Some(Value::Null)
    );
    let rejected=app.request(json!({"method":"enqueueText","params":{"thread":incomplete,"text":"must not reach the main chat"}})).await;
    assert_eq!(rejected["error"]["code"], "invalid_request");
    assert!(
        call(&app, "listOutbox", json!({"thread":thread}))
            .await
            .as_array()
            .unwrap()
            .is_empty()
    );
    let sent = call(
        &app,
        "enqueueText",
        json!({"thread":thread,"text":"phase three demo 📨"}),
    )
    .await;
    assert!(sent["id"].is_string());
    let foreign = app
        .request(json!({"method":"cancelOutbox","params":{"thread":other,"id":sent["id"]}}))
        .await;
    assert_eq!(foreign["error"]["code"], "not_found");
    let page = call(
        &app,
        "history",
        json!({"thread":thread,"window":{"kind":"latest"},"limit":50}),
    )
    .await;
    let message = page["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| &i["message"])
        .find(|m| m["text"] == "phase three demo 📨")
        .expect("committed echo");
    assert!(message["sentAt"].is_string());
    assert_eq!(message["direction"], "out");
    assert!(message["rowKey"].is_string());
    let unknown = app
        .request(json!({"method":"plugin.call","params":{}}))
        .await;
    assert_eq!(unknown["error"]["code"], "invalid_request");
    app.shutdown().await;
}
