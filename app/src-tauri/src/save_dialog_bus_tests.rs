//! Explicit opt-in, private dbus-run-session only; never part of the ordinary suite.
use super::*;
use std::{
    sync::{atomic::AtomicUsize, mpsc},
    thread::JoinHandle,
    time::{Duration, Instant},
};
use zbus::blocking::MessageIterator;

#[derive(Clone, Copy, Debug)]
enum Scenario {
    EarlyResponse(u32),
    ForgedSender,
    WrongHandle,
    LostReply,
    MultipleFiles,
    Traversal,
    Hold,
}

struct FakePortal {
    connection: Connection,
    calls: Arc<AtomicUsize>,
    worker: Option<JoinHandle<()>>,
    release: mpsc::Sender<()>,
}

impl FakePortal {
    fn new(scenario: Scenario) -> Self {
        let connection = Connection::session().unwrap();
        let bus = Proxy::new(
            &connection, "org.freedesktop.DBus", "/org/freedesktop/DBus", "org.freedesktop.DBus",
        ).unwrap();
        let owned: bool = bus.call("NameHasOwner", &(PORTAL,)).unwrap();
        assert!(!owned, "private test bus already has a portal; refusing to replace it");
        let mut messages = MessageIterator::from(&connection);
        connection.request_name(PORTAL).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let service = connection.clone();
        let (release, pending_release) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            while let Some(Ok(message)) = messages.next() {
                let header = message.header();
                if header.message_type() != zbus::message::Type::MethodCall {
                    continue;
                }
                if header.interface().map(|v| v.as_str()) != Some("org.freedesktop.portal.FileChooser")
                    || !matches!(header.member().map(|v| v.as_str()), Some("SaveFile" | "OpenFile"))
                {
                    let _ = service.reply_error(&message, "org.freedesktop.DBus.Error.UnknownMethod", &"unsupported test method");
                    continue;
                }
                count.fetch_add(1, Ordering::Relaxed);
                let sender = header.sender().unwrap().as_str().to_owned();
                let method = header.member().unwrap().as_str().to_owned();
                let (parent, _, mut options): (String, String, HashMap<String, OwnedValue>) =
                    message.body().deserialize().unwrap();
                assert!(parent.is_empty());
                let token = String::try_from(options.remove("handle_token").unwrap()).unwrap();
                assert!(!bool::try_from(options.remove("modal").unwrap()).unwrap());
                if method == "OpenFile" {
                    assert!(!bool::try_from(options.remove("multiple").unwrap()).unwrap());
                    assert!(!bool::try_from(options.remove("directory").unwrap()).unwrap());
                    assert!(!options.contains_key("current_name"));
                } else {
                    assert!(!String::try_from(options.remove("current_name").unwrap()).unwrap().is_empty());
                }
                assert!(options.is_empty(), "no user-controlled options or native paths");
                let path = request_path(&sender, &token).unwrap();
                let handle = OwnedObjectPath::try_from(path.as_str()).unwrap();
                match scenario {
                    Scenario::EarlyResponse(status) => {
                        // Deliberately no delay: the signal precedes the SaveFile reply.
                        emit_response(&service, &sender, &path, status);
                        service.reply(&message, &handle).unwrap();
                    }
                    Scenario::ForgedSender => {
                        let attacker = Connection::session().unwrap();
                        emit_response(&attacker, &sender, &path, 0);
                        // Same-connection bus round trip proves the forged signal was
                        // submitted before the real owner's terminal cancellation.
                        let bus = Proxy::new(
                            &attacker, "org.freedesktop.DBus", "/org/freedesktop/DBus", "org.freedesktop.DBus",
                        ).unwrap();
                        let _: String = bus.call("GetId", &()).unwrap();
                        emit_response(&service, &sender, &path, 1);
                        service.reply(&message, &handle).unwrap();
                    }
                    Scenario::WrongHandle => {
                        let wrong = OwnedObjectPath::try_from(format!("{path}_other")).unwrap();
                        service.reply(&message, &wrong).unwrap();
                    }
                    Scenario::MultipleFiles => {
                        emit_uris(&service, &sender, &path, &["file:///synthetic/a", "file:///synthetic/b"]);
                        service.reply(&message, &handle).unwrap();
                    }
                    Scenario::Traversal => {
                        emit_uris(&service, &sender, &path, &["file:///synthetic/%2e%2e/private"]);
                        service.reply(&message, &handle).unwrap();
                    }
                    Scenario::Hold => {
                        pending_release.recv_timeout(Duration::from_secs(5)).unwrap();
                        emit_response(&service, &sender, &path, 1);
                        service.reply(&message, &handle).unwrap();
                    }
                    Scenario::LostReply => {
                        // The request reached the service, then its connection died
                        // without a method reply or terminal Request.Response.
                        let _ = service.close();
                        break;
                    }
                }
            }
        });
        Self { connection, calls, worker: Some(worker), release }
    }
}

impl Drop for FakePortal {
    fn drop(&mut self) {
        // Closing the shared transport wakes the blocking iterator, including when
        // a timed assertion failed before SaveFile reached the fake service.
        let _ = self.release.send(());
        let _ = self.connection.clone().close();
        if let Some(worker) = self.worker.take() {
            let result = worker.join();
            if !std::thread::panicking() {
                result.unwrap();
            }
        }
    }
}

fn emit_response(connection: &Connection, sender: &str, path: &str, status: u32) {
    let results: HashMap<&str, Value<'_>> = if status == 0 {
        HashMap::from([("uris", Value::new(vec!["file:///synthetic/new-copy".to_owned()]))])
    } else {
        HashMap::new()
    };
    connection.emit_signal(Some(sender), path, REQUEST, "Response", &(status, results)).unwrap();
}

fn emit_uris(connection: &Connection, sender: &str, path: &str, uris: &[&str]) {
    let results = HashMap::from([("uris", Value::new(uris.iter().map(|uri| (*uri).to_owned()).collect::<Vec<_>>()))]);
    connection.emit_signal(Some(sender), path, REQUEST, "Response", &(0_u32, results)).unwrap();
}

fn begin(provider: &PortalSaveDialog, kind: DialogKind) -> mpsc::Receiver<Result<Option<PathBuf>, SaveDialogError>> {
    let (sender, receiver) = mpsc::channel();
    let callback: SaveDialogCallback = Box::new(move |result| { let _ = sender.send(result); });
    match kind {
        DialogKind::Save => provider.choose("synthetic-copy".into(), callback),
        DialogKind::Open => provider.choose_open(callback),
    }
    receiver
}

fn wait_settled(provider: &PortalSaveDialog) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while provider.busy.load(Ordering::Acquire) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(!provider.busy.load(Ordering::Acquire));
}

#[test]
#[ignore = "requires explicit private dbus-run-session and CH_NATIVE_PORTAL_TEST_BUS=1"]
fn native_portal_private_bus_transport() {
    assert_eq!(std::env::var("CH_NATIVE_PORTAL_TEST_BUS").as_deref(), Ok("1"),
        "run only in an explicitly isolated dbus-run-session");
    for kind in [DialogKind::Save, DialogKind::Open] {
        for scenario in [Scenario::EarlyResponse(0), Scenario::EarlyResponse(1), Scenario::EarlyResponse(2),
            Scenario::ForgedSender, Scenario::WrongHandle, Scenario::LostReply, Scenario::MultipleFiles, Scenario::Traversal] {
            if kind == DialogKind::Save && matches!(scenario, Scenario::Traversal) { continue; }
            let portal = FakePortal::new(scenario);
            let provider = PortalSaveDialog::default();
            let receiver = begin(&provider, kind);
            let result = receiver.recv_timeout(Duration::from_secs(5)).unwrap_or_else(|_| {
                panic!("provider did not settle within private-bus bound: {kind:?}/{scenario:?}")
            });
            assert_eq!(portal.calls.load(Ordering::Relaxed), 1,
                "provider returned {result:?} before expected dispatch: {kind:?}/{scenario:?}");
            match scenario {
                Scenario::EarlyResponse(0) => assert_eq!(result.unwrap(), Some(PathBuf::from("/synthetic/new-copy"))),
                Scenario::EarlyResponse(1) | Scenario::ForgedSender => assert!(matches!(result, Ok(None))),
                Scenario::EarlyResponse(2) | Scenario::MultipleFiles | Scenario::Traversal => assert!(matches!(result, Err(SaveDialogError::Unavailable))),
                Scenario::WrongHandle | Scenario::LostReply => assert!(matches!(result, Err(SaveDialogError::Uncertain))),
                Scenario::EarlyResponse(_) | Scenario::Hold => unreachable!(),
            }
            let uncertain = matches!(scenario, Scenario::WrongHandle | Scenario::LostReply);
            if !uncertain { wait_settled(&provider); }
            assert_eq!(provider.busy.load(Ordering::Acquire), uncertain, "{kind:?}/{scenario:?}");
            assert!(matches!(receiver.try_recv(), Err(mpsc::TryRecvError::Disconnected)));
            if uncertain {
                for blocked in [DialogKind::Save, DialogKind::Open] {
                    assert!(matches!(begin(&provider, blocked).recv_timeout(Duration::from_secs(1)).unwrap(), Err(SaveDialogError::Unavailable)));
                }
            }
            assert_eq!(portal.calls.load(Ordering::Relaxed), 1);
        }
        // A still-live request reserves both methods, not just its own trait object.
        let portal = FakePortal::new(Scenario::Hold);
        let provider = PortalSaveDialog::default();
        let receiver = begin(&provider, kind);
        let deadline = Instant::now() + Duration::from_secs(5);
        while portal.calls.load(Ordering::Relaxed) == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(portal.calls.load(Ordering::Relaxed), 1);
        let other = if kind == DialogKind::Save { DialogKind::Open } else { DialogKind::Save };
        assert!(matches!(begin(&provider, other).recv_timeout(Duration::from_secs(1)).unwrap(), Err(SaveDialogError::Unavailable)));
        assert_eq!(portal.calls.load(Ordering::Relaxed), 1);
        portal.release.send(()).unwrap();
        assert!(matches!(receiver.recv_timeout(Duration::from_secs(5)).unwrap(), Ok(None)));
        wait_settled(&provider);
    }
}
