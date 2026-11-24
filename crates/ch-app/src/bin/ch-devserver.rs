//! Browser development transport. This entry point only opens an isolated demo.
use ch_app::{App, Invalidation};
use futures_util::{SinkExt, StreamExt, stream::FuturesUnordered};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    ffi::OsString,
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, broadcast::error::RecvError},
    task::JoinSet,
    time::timeout,
};
use tokio_tungstenite::{
    WebSocketStream, accept_hdr_async_with_config,
    tungstenite::{
        Message,
        handshake::server::{ErrorResponse, Request, Response},
        http::{HeaderValue, StatusCode},
        protocol::WebSocketConfig,
    },
};

const ORIGIN: &str = "http://127.0.0.1:5173";
const HOST: &str = "127.0.0.1:43127";
const PROTOCOL: &str = "ch-core-v1";
const MAX_FRAME: usize = 256 * 1024;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn options() -> Result<(PathBuf, PathBuf)> {
    let args = std::env::args_os().skip(1).collect::<Vec<OsString>>();
    match args.as_slice() {
        [first, root, second, file] if first == "--demo" && second == "--token-file" => {
            Ok((root.into(), file.into()))
        }
        _ => Err(io::Error::other(
            "usage: ch-devserver --demo <rebuild-root> --token-file <new-private-file>",
        )
        .into()),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let (repo, path) = options()?;
    let mut key = [0u8; 32];
    getrandom::fill(&mut key)
        .map_err(|_| io::Error::other("cannot initialize the development token"))?;
    let token: Arc<str> = key
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>()
        .into();
    let listener = TcpListener::bind(HOST).await?;
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.write_all(token.as_bytes())?;
    let app = Arc::new(App::demo(&repo).await?);
    if let Err(error) = tmp.persist_noclobber(&path) {
        app.shutdown().await;
        return Err(error.into());
    }
    eprintln!("Demo ready at ws://{HOST}/core (origin {ORIGIN}).");
    let slots = Arc::new(Semaphore::new(8));
    let operations = Operations::new();
    let mut peers = JoinSet::new();
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    let result = loop {
        tokio::select! {
            _=&mut ctrl_c=>break Ok(()),
            Some(_)=peers.join_next(),if !peers.is_empty()=>{},
            next=listener.accept()=>{
                let (socket,peer)=match next{Ok(pair)=>pair,Err(e)=>break Err(e)};
                if !peer.ip().is_loopback(){continue;}
                let Ok(permit)=slots.clone().try_acquire_owned() else{continue;};
                let app=app.clone();let token=token.clone();let operations=operations.clone();
                peers.spawn(async move {let _permit=permit;let _=serve(socket,app,token,operations).await;});
            }
        }
    };
    peers.shutdown().await;
    operations.drain().await;
    app.shutdown().await;
    result?;
    Ok(())
}

#[allow(clippy::result_large_err)]
fn headers(req: &Request, mut res: Response) -> std::result::Result<Response, ErrorResponse> {
    let single = |name: &str, value: &str| {
        let mut all = req.headers().get_all(name).iter();
        all.next().is_some_and(|v| v.as_bytes() == value.as_bytes()) && all.next().is_none()
    };
    if req.uri() != "/core"
        || !single("Host", HOST)
        || !single("Origin", ORIGIN)
        || !single("Sec-WebSocket-Protocol", PROTOCOL)
    {
        return Err(Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(None)
            .expect("valid response"));
    }
    res.headers_mut()
        .insert("sec-websocket-protocol", HeaderValue::from_static(PROTOCOL));
    Ok(res)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Auth {
    token: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    id: String,
    request: Value,
}

fn verify(s: &str, token: &str) -> bool {
    s.len() == token.len()
        && s.as_bytes()
            .iter()
            .zip(token.as_bytes())
            .fold(0u8, |n, (a, b)| n | (a ^ b))
            == 0
}

// The server owns accepted operations across peer disconnects. A permit stays with
// each task through completion, including unwinding. Core service calls retain their
// bounded host/SQLite timeouts. Draining after peer shutdown cannot admit new work.
#[derive(Clone)]
struct Operations(Arc<Semaphore>);
impl Operations {
    const LIMIT: u32 = 64;

    fn new() -> Self {
        Self(Arc::new(Semaphore::new(Self::LIMIT as usize)))
    }

    fn start(
        &self,
        request: impl Future<Output = Value> + Send + 'static,
    ) -> Option<tokio::task::JoinHandle<Value>> {
        let permit = self.0.clone().try_acquire_owned().ok()?;
        Some(tokio::spawn(async move {
            let _permit = permit;
            request.await
        }))
    }
    async fn drain(&self) {
        let _all = self
            .0
            .acquire_many(Self::LIMIT)
            .await
            .expect("operation semaphore stays open");
    }
}

async fn serve(
    socket: TcpStream,
    app: Arc<App>,
    token: Arc<str>,
    operations: Operations,
) -> Result<()> {
    let cfg = WebSocketConfig::default()
        .max_frame_size(Some(MAX_FRAME))
        .max_message_size(Some(MAX_FRAME));
    let mut ws = timeout(
        Duration::from_secs(5),
        accept_hdr_async_with_config(socket, headers, Some(cfg)),
    )
    .await??;
    let Some(Ok(Message::Text(msg))) = timeout(Duration::from_secs(5), ws.next()).await? else {
        return Ok(());
    };
    if !serde_json::from_str::<Auth>(&msg).is_ok_and(|auth| verify(&auth.token, &token)) {
        return Ok(());
    }
    let events = app.subscribe();
    ws.send(Message::text(r#"{"ready":true}"#)).await?;
    dispatch_frames(ws, events, operations, move |request| {
        let app = app.clone();
        async move { app.request(request).await }
    })
    .await
}

async fn dispatch_frames<F: Future<Output = Value> + Send + 'static>(
    mut ws: WebSocketStream<TcpStream>,
    mut events: tokio::sync::broadcast::Receiver<Invalidation>,
    operations: Operations,
    dispatch: impl Fn(Value) -> F,
) -> Result<()> {
    let mut pending = FuturesUnordered::new();
    let mut active = std::collections::BTreeSet::new();
    loop {
        let frame = tokio::select! {
            Some((id,reply))=pending.next(),if !pending.is_empty()=>{
                active.remove(&id);
                json!({"id":id,"reply":reply})
            }
            next=ws.next()=>{
                match next {
                    Some(Ok(Message::Text(data)))=>{
                        let frame:Envelope=serde_json::from_str(&data)?;
                        if frame.id.is_empty() || frame.id.len()>128 || active.len()>=16 || !active.insert(frame.id.clone()){break;}
                        let mutation = matches!(frame.request["method"].as_str(), Some("editText" | "deleteMessages" | "pinMessage" | "unpinMessage"));
                        if let Some(task)=operations.start(dispatch(frame.request)){
                            pending.push(async move {
                                let reply=task.await.unwrap_or_else(|_| {
                                    let mut reply = json!({"ok":false,"error":{"code":"internal","message":"The operation could not be completed."}});
                                    if mutation { reply["error"]["outcomeUnknown"] = json!(true); }
                                    reply
                                });
                                (frame.id,reply)
                            });
                            continue;
                        }
                        active.remove(&frame.id);
                        json!({"id":frame.id,"reply":{"ok":false,"error":{"code":"unavailable","message":"The service is busy."}}})
                    }
                    Some(Ok(Message::Ping(data)))=>{ws.send(Message::Pong(data)).await?;continue;}
                    Some(Ok(Message::Pong(_)))=>continue,
                    _=>break,
                }
            }
            event=events.recv()=>{
                let change=match event{Ok(e)=>e,Err(RecvError::Lagged(_))=>Invalidation::Reset,Err(RecvError::Closed)=>break};
                json!({"change":change})
            }
        };
        timeout(
            Duration::from_secs(5),
            ws.send(Message::text(frame.to_string())),
        )
        .await??;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn upgrade(host: &str, origin: &str, path: &str) -> Request {
        Request::builder()
            .uri(path)
            .header("host", host)
            .header("origin", origin)
            .header("sec-websocket-protocol", PROTOCOL)
            .body(())
            .unwrap()
    }
    #[test]
    fn upgrade_is_scoped_to_loopback_origin_and_exact_path() {
        let response = || Response::builder().body(()).unwrap();
        assert!(headers(&upgrade(HOST, ORIGIN, "/core"), response()).is_ok());
        assert!(headers(&upgrade("example.invalid", ORIGIN, "/core"), response()).is_err());
        assert!(headers(&upgrade(HOST, "null", "/core"), response()).is_err());
        assert!(headers(&upgrade(HOST, ORIGIN, "/core?token=x"), response()).is_err());
    }

    #[tokio::test]
    async fn accepted_mutation_finishes_after_socket_disconnect() {
        for method in ["enqueueText", "editText", "deleteMessages", "pinMessage", "unpinMessage"] {
            disconnected_mutation(method).await;
        }
    }

    async fn disconnected_mutation(method: &'static str) {
        use std::sync::atomic::{AtomicU8, Ordering};
        use tokio::sync::Notify;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let operations = Operations::new();
        let stage = Arc::new(AtomicU8::new(0));
        let started = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let (events, rx) = tokio::sync::broadcast::channel(1);
        let peer = tokio::spawn({
            let operations = operations.clone();
            let stage = stage.clone();
            let started = started.clone();
            let resume = resume.clone();
            async move {
                let (socket, _) = listener.accept().await.unwrap();
                let ws = tokio_tungstenite::accept_async(socket).await.unwrap();
                dispatch_frames(ws, rx, operations, move |request| {
                    assert_eq!(request["method"], method);
                    let (stage, started, resume) = (stage.clone(), started.clone(), resume.clone());
                    async move {
                        stage.store(1, Ordering::SeqCst);
                        started.notify_one();
                        resume.notified().await;
                        stage.store(2, Ordering::SeqCst);
                        Value::Null
                    }
                })
                .await
                .unwrap();
            }
        });
        let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        client
            .send(Message::text(json!({"id":"a","request":{"method":method,"params":{}}}).to_string()))
            .await
            .unwrap();
        timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        client.close(None).await.unwrap();
        timeout(Duration::from_secs(2), peer)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stage.load(Ordering::SeqCst), 1);
        assert!(operations.0.available_permits() < Operations::LIMIT as usize);
        resume.notify_one();
        timeout(Duration::from_secs(2), operations.drain())
            .await
            .unwrap();
        assert_eq!(stage.load(Ordering::SeqCst), 2);
        drop(events);
    }

    #[tokio::test]
    async fn failed_action_worker_is_unknown_but_read_failure_is_not() {
        async fn fail(_: Value) -> Value {
            panic!("synthetic worker failure")
        }
        for method in ["editText", "deleteMessages", "pinMessage", "unpinMessage", "inventory"] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (events, rx) = tokio::sync::broadcast::channel(1);
            let peer = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let ws = tokio_tungstenite::accept_async(socket).await.unwrap();
                dispatch_frames(ws, rx, Operations::new(), fail).await.unwrap();
            });
            let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}")).await.unwrap();
            client.send(Message::text(json!({"id":"a","request":{"method":method,"params":{}}}).to_string())).await.unwrap();
            let frame = timeout(Duration::from_secs(2), client.next()).await.unwrap().unwrap().unwrap();
            let reply: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
            assert_eq!(reply["reply"]["error"]["code"], "internal");
            assert_eq!(reply["reply"]["error"]["outcomeUnknown"] == true, method != "inventory");
            client.close(None).await.unwrap();
            timeout(Duration::from_secs(2), peer).await.unwrap().unwrap();
            drop(events);
        }
    }
}
