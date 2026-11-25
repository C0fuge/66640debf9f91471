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
}

struct FakePortal {
    connection: Connection,
    calls: Arc<AtomicUsize>,
    worker: Option<JoinHandle<()>>,
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
        let worker = std::thread::spawn(move || {
            while let Some(Ok(message)) = messages.next() {
                let header = message.header();
                if header.message_type() != zbus::message::Type::MethodCall {
                    continue;
                }
                if header.interface().map(|v| v.as_str()) != Some("org.freedesktop.portal.FileChooser")
                    || header.member().map(|v| v.as_str()) != Some("SaveFile")
                {
                    let _ = service.reply_error(&message, "org.freedesktop.DBus.Error.UnknownMethod", &"unsupported test method");
                    continue;
                }
                count.fetch_add(1, Ordering::Relaxed);
                let sender = header.sender().unwrap().as_str().to_owned();
                let (parent, _, mut options): (String, String, HashMap<String, OwnedValue>) =
                    message.body().deserialize().unwrap();
                assert!(parent.is_empty());
                let token = String::try_from(options.remove("handle_token").unwrap()).unwrap();
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
                    Scenario::LostReply => {
                        // The request reached the service, then its connection died
                        // without a method reply or terminal Request.Response.
                        let _ = service.close();
                        break;
                    }
                }
            }
        });
        Self { connection, calls, worker: Some(worker) }
    }
}

impl Drop for FakePortal {
    fn drop(&mut self) {
        // Closing the shared transport wakes the blocking iterator, including when
        // a timed assertion failed before SaveFile reached the fake service.
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

#[test]
#[ignore = "requires explicit private dbus-run-session and CH_NATIVE_PORTAL_TEST_BUS=1"]
fn native_portal_private_bus_transport() {
    assert_eq!(std::env::var("CH_NATIVE_PORTAL_TEST_BUS").as_deref(), Ok("1"),
        "run only in an explicitly isolated dbus-run-session");
    for scenario in [
        Scenario::EarlyResponse(0), Scenario::EarlyResponse(1), Scenario::EarlyResponse(2),
        Scenario::ForgedSender, Scenario::WrongHandle, Scenario::LostReply,
    ] {
        let portal = FakePortal::new(scenario);
        let provider = PortalSaveDialog::default();
        let (sender, receiver) = mpsc::channel();
        provider.choose("synthetic-copy".into(), Box::new(move |result| {
            let _ = sender.send(result);
        }));
        let result = receiver.recv_timeout(Duration::from_secs(5)).unwrap_or_else(|_| {
            panic!("provider did not settle within private-bus bound: {scenario:?}")
        });
        assert_eq!(portal.calls.load(Ordering::Relaxed), 1,
            "provider returned {result:?} before expected SaveFile dispatch: {scenario:?}");
        match scenario {
            Scenario::EarlyResponse(0) => assert_eq!(result.unwrap(), Some(PathBuf::from("/synthetic/new-copy"))),
            Scenario::EarlyResponse(1) | Scenario::ForgedSender => assert!(matches!(result, Ok(None))),
            Scenario::EarlyResponse(2) => assert!(matches!(result, Err(SaveDialogError::Unavailable))),
            Scenario::WrongHandle | Scenario::LostReply => assert!(matches!(result, Err(SaveDialogError::Uncertain))),
            Scenario::EarlyResponse(_) => unreachable!(),
        }
        // Completion sends before releasing its slot. Wait a finite interval for
        // that final store in the settled cases; uncertain cases stay reserved.
        let uncertain = matches!(scenario, Scenario::WrongHandle | Scenario::LostReply);
        let deadline = Instant::now() + Duration::from_secs(1);
        while !uncertain && provider.busy.load(Ordering::Acquire) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(provider.busy.load(Ordering::Acquire), uncertain, "{scenario:?}");
        assert!(matches!(receiver.try_recv(), Err(mpsc::TryRecvError::Disconnected)));
        if uncertain {
            let (sender, receiver) = mpsc::channel();
            provider.choose("second-copy".into(), Box::new(move |result| { let _ = sender.send(result); }));
            assert!(matches!(receiver.recv_timeout(Duration::from_secs(1)).unwrap(), Err(SaveDialogError::Unavailable)));
        }
        assert_eq!(portal.calls.load(Ordering::Relaxed), 1, "{scenario:?}");
    }
}
