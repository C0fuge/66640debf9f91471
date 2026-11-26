//! Linux-only, single-flight xdg-desktop-portal chooser. No fallback programs.
//!
//! Request subscription/response semantics follow the first-party portal specifications:
//! https://flatpak.github.io/xdg-desktop-portal/docs/doc-org.freedesktop.portal.Request.html
//! https://flatpak.github.io/xdg-desktop-portal/docs/doc-org.freedesktop.portal.FileChooser.html
use ch_app::{OpenFileDialog, SaveDialog, SaveDialogCallback, SaveDialogError};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use zbus::{
    blocking::{Connection, Proxy},
    zvariant::{OwnedObjectPath, OwnedValue, Value},
};

const PORTAL: &str = "org.freedesktop.portal.Desktop";
const DESKTOP: &str = "/org/freedesktop/portal/desktop";
const REQUEST: &str = "org.freedesktop.portal.Request";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DialogKind { Save, Open }

#[derive(Default)]
pub(crate) struct PortalSaveDialog {
    busy: Arc<AtomicBool>,
}

// The callback is taken outside the lock. A spawn failure can still settle it exactly once.
struct Completion {
    busy: Arc<AtomicBool>,
    callback: Mutex<Option<SaveDialogCallback>>,
}

impl Completion {
    fn finish(&self, result: Result<Option<PathBuf>, SaveDialogError>, settled: bool) {
        let callback = self
            .callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(callback) = callback {
            callback(result);
            if settled {
                self.busy.store(false, Ordering::Release);
            }
        }
    }
}

impl SaveDialog for PortalSaveDialog {
    fn choose(&self, suggested_name: String, callback: SaveDialogCallback) {
        self.start(DialogKind::Save, Some(suggested_name), callback);
    }
}

impl OpenFileDialog for PortalSaveDialog {
    fn choose_open(&self, callback: SaveDialogCallback) {
        self.start(DialogKind::Open, None, callback);
    }
}

impl PortalSaveDialog {
    fn start(&self, kind: DialogKind, suggested_name: Option<String>, callback: SaveDialogCallback) {
        if self
            .busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            callback(Err(SaveDialogError::Unavailable));
            return;
        }
        let completion = Arc::new(Completion {
            busy: self.busy.clone(),
            callback: Mutex::new(Some(callback)),
        });
        let worker = completion.clone();
        if std::thread::Builder::new()
            .name("file-portal".into())
            .spawn(move || {
                let mut settled = true;
                let result = choose_portal(kind, suggested_name.as_deref(), &mut settled);
                worker.finish(
                    if settled { result } else { Err(SaveDialogError::Uncertain) },
                    settled,
                );
            })
            .is_err()
        {
            completion.finish(Err(SaveDialogError::Internal), true);
        }
    }
}

fn choose_portal(
    kind: DialogKind,
    suggested_name: Option<&str>,
    settled: &mut bool,
) -> Result<Option<PathBuf>, SaveDialogError> {
    let connection = Connection::session().map_err(|error| unavailable_at("session_connection", error))?;
    let bus = Proxy::new(
        &connection,
        "org.freedesktop.DBus",
        "/org/freedesktop/DBus",
        "org.freedesktop.DBus",
    )
    .map_err(|error| unavailable_at("bus_proxy", error))?;
    // A running service can own its name without an activation .service file.
    // Resolve it first: some bus implementations reject StartServiceByName for
    // such a name before consulting the registry. Activate only an absent owner.
    let owner: String = match bus.call("GetNameOwner", &(PORTAL,)) {
        Ok(owner) => owner,
        Err(zbus::Error::MethodError(name, _, _))
            if name.as_str() == "org.freedesktop.DBus.Error.NameHasNoOwner" =>
        {
            let _: u32 = bus.call("StartServiceByName", &(PORTAL, 0_u32))
                .map_err(|error| unavailable_at("activate_portal", error))?;
            bus.call("GetNameOwner", &(PORTAL,))
                .map_err(|error| unavailable_at("resolve_activated_owner", error))?
        }
        Err(error) => return Err(unavailable_at("resolve_owner", error)),
    };
    let sender = connection.unique_name().ok_or(SaveDialogError::Unavailable)?;
    let token = format!("ch_{}", zbus::Guid::generate());
    let path = request_path(sender.as_str(), &token).ok_or(SaveDialogError::Internal)?;
    let request = Proxy::new(&connection, owner.as_str(), path.as_str(), REQUEST)
        .map_err(|error| unavailable_at("request_proxy", error))?;
    let mut responses = request.receive_signal("Response").map_err(|error| unavailable_at("subscribe_response", error))?;
    let chooser = Proxy::new(
        &connection,
        owner.as_str(),
        DESKTOP,
        "org.freedesktop.portal.FileChooser",
    )
    .map_err(|error| unavailable_at("chooser_proxy", error))?;
    let name = safe_name(suggested_name.unwrap_or("attachment"));
    let mut options = HashMap::from([
        ("handle_token", Value::from(token.as_str())),
        ("modal", Value::from(false)),
    ]);
    let (method, title) = match kind {
        DialogKind::Save => {
            options.insert("current_name", Value::from(name.as_str()));
            ("SaveFile", "Save a new copy")
        }
        DialogKind::Open => {
            options.insert("multiple", Value::from(false));
            options.insert("directory", Value::from(false));
            ("OpenFile", "Choose a file")
        }
    };
    // From this point a transport error cannot prove the chooser disappeared. Keep
    // the provider reserved for this process in that case; never open a second one.
    *settled = false;
    let handle: OwnedObjectPath = chooser
        .call(method, &("", title, options))
        .map_err(|error| unavailable_at("file_dialog_reply", error))?;
    if handle.as_str() != path {
        // The spec permits old portals to return another path, but subscription would
        // race there. We cannot establish ownership of that other handle to Close it.
        return Err(SaveDialogError::Unavailable);
    }
    let message = responses.next().ok_or(SaveDialogError::Unavailable)?;
    let header = message.header();
    if header.sender().map(|v| v.as_str()) != Some(owner.as_str())
        || header.path().map(|v| v.as_str()) != Some(path.as_str())
        || header.interface().map(|v| v.as_str()) != Some(REQUEST)
        || header.member().map(|v| v.as_str()) != Some("Response")
    {
        return Err(SaveDialogError::Unavailable);
    }
    let (response, results): (u32, HashMap<String, OwnedValue>) =
        message.body().deserialize().map_err(|error| unavailable_at("response_body", error))?;
    decode_response(response, results, settled, kind)
}

fn decode_response(
    response: u32,
    mut results: HashMap<String, OwnedValue>,
    settled: &mut bool,
    kind: DialogKind,
) -> Result<Option<PathBuf>, SaveDialogError> {
    // A well-formed terminal response settles the native interaction, including an
    // unusable success payload. Unknown response codes do not prove settlement.
    match response {
        0 => {
            *settled = true;
            let uris = results.remove("uris").ok_or(SaveDialogError::Unavailable)?;
            let uris = Vec::<String>::try_from(uris).map_err(|error| unavailable_at("response_uris", error))?;
            match kind {
                DialogKind::Save => selected_path(&uris),
                DialogKind::Open => selected_open_path(&uris),
            }.map(Some)
        }
        1 => {
            *settled = true;
            Ok(None)
        }
        2 => {
            *settled = true;
            Err(SaveDialogError::Unavailable)
        }
        _ => Err(SaveDialogError::Unavailable),
    }
}

fn unavailable_at<E: std::fmt::Debug>(_stage: &'static str, _error: E) -> SaveDialogError {
    // Only the explicit synthetic private-bus test can print transport details.
    // This block does not exist in the production binary.
    #[cfg(test)]
    if std::env::var("CH_NATIVE_PORTAL_TEST_BUS").as_deref() == Ok("1") {
        eprintln!("private portal fixture stage {_stage}: {_error:?}");
    }
    SaveDialogError::Unavailable
}

fn request_path(sender: &str, token: &str) -> Option<String> {
    let sender = sender.strip_prefix(':')?;
    if sender.is_empty()
        || !sender.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'.' || c == b'_')
        || token.is_empty()
        || !token.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
    {
        return None;
    }
    Some(format!("{DESKTOP}/request/{}/{token}", sender.replace('.', "_")))
}

fn selected_open_path(uris: &[String]) -> Result<PathBuf, SaveDialogError> {
    let [uri] = uris else { return Err(SaveDialogError::Unavailable); };
    if uri.len() > 16_384 || uri.contains('\\')
        || uri.bytes().any(|byte| byte <= b' ' || byte == 0x7f)
    {
        return Err(SaveDialogError::Unavailable);
    }
    // URL normalization must not erase traversal or reinterpret an encoded separator.
    let bytes = uri.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() || !bytes[i + 1].is_ascii_hexdigit() || !bytes[i + 2].is_ascii_hexdigit() {
                return Err(SaveDialogError::Unavailable);
            }
            i += 3;
        } else { i += 1; }
    }
    let literal = uri.to_ascii_lowercase();
    if ["%00", "%2f", "%5c"].iter().any(|part| literal.contains(part))
        || literal.replace("%2e", ".").split('/').any(|part| matches!(part, "." | ".."))
    {
        return Err(SaveDialogError::Unavailable);
    }
    let path = selected_path(uris)?;
    let raw = uri.split_once(':').and_then(|(_, rest)| rest.strip_prefix("//"))
        .ok_or(SaveDialogError::Unavailable)?.as_bytes();
    let hex = |byte: u8| char::from(byte).to_digit(16).map(|n| n as u8)
        .ok_or(SaveDialogError::Unavailable);
    let mut expected = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'%' {
            let high = *raw.get(i + 1).ok_or(SaveDialogError::Unavailable)?;
            let low = *raw.get(i + 2).ok_or(SaveDialogError::Unavailable)?;
            expected.push(hex(high)? * 16 + hex(low)?);
            i += 3;
        } else { expected.push(raw[i]); i += 1; }
    }
    // On Linux these are the original filesystem bytes. The URL parser must not
    // trim controls, rewrite a drive-shaped component, or add/remove a separator.
    if path.as_os_str().as_encoded_bytes() != expected.as_slice() {
        return Err(SaveDialogError::Unavailable);
    }
    Ok(path)
}

fn selected_path(uris: &[String]) -> Result<PathBuf, SaveDialogError> {
    let [uri] = uris else {
        return Err(SaveDialogError::Unavailable);
    };
    if uri.len() > 16_384 {
        return Err(SaveDialogError::Unavailable);
    }
    // Require the portal's absolute, empty-authority file URI form before URL
    // normalization (which otherwise accepts file:relative or erases localhost).
    let rest = uri.split_once(':').map(|(_, rest)| rest).unwrap_or("");
    if !rest.starts_with("///") {
        return Err(SaveDialogError::Unavailable);
    }
    let parsed = url::Url::parse(uri).map_err(|_| SaveDialogError::Unavailable)?;
    if parsed.scheme() != "file"
        || parsed.host_str().is_some()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.port().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(SaveDialogError::Unavailable);
    }
    let path = parsed.to_file_path().map_err(|_| SaveDialogError::Unavailable)?;
    if !path.is_absolute() || path.as_os_str().as_encoded_bytes().contains(&0) {
        return Err(SaveDialogError::Unavailable);
    }
    Ok(path)
}

fn safe_name(name: &str) -> String {
    let mut result = String::new();
    for character in name.chars().filter(|c| {
        !c.is_control() && !matches!(c, '/' | '\\' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
    }) {
        if result.len() + character.len_utf8() > 240 {
            break;
        }
        result.push(character);
    }
    if result.trim().is_empty() || matches!(result.as_str(), "." | "..") {
        "attachment".into()
    } else {
        result
    }
}

#[cfg(test)]
#[path = "save_dialog_bus_tests.rs"]
mod bus_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn only_one_strict_local_file_uri_is_accepted() {
        assert_eq!(selected_path(&["file:///tmp/new%20copy".into()]).unwrap(), PathBuf::from("/tmp/new copy"));
        for uris in [
            vec![], vec!["file:///tmp/a".into(), "file:///tmp/b".into()],
            vec!["https://example.invalid/a".into()], vec!["file://server/tmp/a".into()],
            vec!["file://localhost/tmp/a".into()],
            vec!["file:relative".into()], vec!["file:/tmp/a".into()],
            vec!["file:///tmp/a?query".into()], vec!["file:///tmp/a#fragment".into()],
            vec!["file:///tmp/%00a".into()], vec!["not a URI".into()],
        ] {
            assert!(selected_path(&uris).is_err());
        }
    }

    #[test]
    fn response_codes_and_malformed_success_never_fabricate_a_path() {
        for response in [0, 1, 2, 99] {
            let mut settled = false;
            let result = decode_response(response, HashMap::new(), &mut settled, DialogKind::Save);
            assert_eq!(settled, response <= 2);
            if response == 1 { assert!(matches!(result, Ok(None))); }
            else { assert!(result.is_err()); }
        }
        let mut settled = false;
        let results = HashMap::from([("uris".into(), OwnedValue::from(42_u32))]);
        assert!(decode_response(0, results, &mut settled, DialogKind::Save).is_err());
        assert!(settled);
    }

    #[test]
    fn positive_response_round_trips_the_actual_dbus_variant_array() {
        let results = HashMap::from([("uris", Value::new(vec!["file:///synthetic/new-copy".to_owned()]))]);
        let message = zbus::Message::signal("/synthetic/request", REQUEST, "Response")
            .unwrap().build(&(0_u32, results)).unwrap();
        let (code, values): (u32, HashMap<String, OwnedValue>) = message.body().deserialize().unwrap();
        let mut settled = false;
        assert_eq!(decode_response(code, values, &mut settled, DialogKind::Save).unwrap(), Some(PathBuf::from("/synthetic/new-copy")));
        assert!(settled);
    }

    #[test]
    fn open_rejects_ambiguity_before_url_normalization() {
        assert_eq!(selected_open_path(&["file:///tmp/a%20file".into()]).unwrap(), PathBuf::from("/tmp/a file"));
        assert_eq!(selected_open_path(&["file:///tmp/%20a%20".into()]).unwrap(), PathBuf::from("/tmp/ a "));
        for uri in ["file:///tmp/../private", "file:///tmp/%2e%2E/private", "file:///tmp/./a",
            "file:///tmp/%2E/a", "file:///tmp/a%2fb", "file:///tmp/a%5Cb", "file:///tmp/a%00b",
            "file:///tmp/a%", "file:///tmp/a%GG", "file:///tmp/a\\b", "file://localhost/tmp/a",
            "file:///tmp/a\tb", "file:///tmp/a\rb", "file:///tmp/a\nb", "file:///tmp/a ",
            " file:///tmp/a", "file:///tmp/a\u{7f}", "file:///tmp/.\n./private",
            "file:///C:", "file:///tmp/aC%3A"] {
            assert!(selected_open_path(&[uri.into()]).is_err(), "{uri}");
        }
        // A drive-shaped directory is an ordinary Linux name if decoding keeps
        // every byte; reject reinterpretation, not an unchanged harmless name.
        assert_eq!(selected_open_path(&["file:///C|/a".into()]).unwrap(), PathBuf::from("/C|/a"));
        assert!(selected_open_path(&[]).is_err());
        assert!(selected_open_path(&["file:///tmp/a".into(), "file:///tmp/b".into()]).is_err());
        let results = HashMap::from([("uris", Value::new(vec!["file:///tmp/%2e%2e/private".to_owned()]))]);
        let message = zbus::Message::signal("/synthetic/request", REQUEST, "Response").unwrap().build(&(0_u32, results)).unwrap();
        let (code, values) = message.body().deserialize().unwrap();
        let mut settled = false;
        assert!(decode_response(code, values, &mut settled, DialogKind::Open).is_err());
        assert!(settled, "a malformed path cannot turn a terminal chooser response into an unresolved dialog");
    }

    #[test]
    fn predicted_handle_and_suggestion_are_bounded() {
        assert_eq!(request_path(":1.23", "ch_123").unwrap(), format!("{DESKTOP}/request/1_23/ch_123"));
        assert!(request_path("1.23", "ch_123").is_none());
        assert!(request_path(":1.23", "bad/token").is_none());
        assert_eq!(safe_name("../a\\b\0\u{202e}"), "..ab");
        assert_eq!(safe_name(".."), "attachment");
        assert!(safe_name(&"界".repeat(1000)).len() <= 240);
    }

    #[test]
    fn completion_is_once_and_ambiguous_interaction_retains_slot() {
        for settled in [false, true] {
            let busy = Arc::new(AtomicBool::new(true));
            let calls = Arc::new(AtomicUsize::new(0));
            let count = calls.clone();
            let completion = Completion {
                busy: busy.clone(),
                callback: Mutex::new(Some(Box::new(move |_| { count.fetch_add(1, Ordering::Relaxed); }))),
            };
            completion.finish(Err(SaveDialogError::Unavailable), settled);
            completion.finish(Ok(None), true);
            assert_eq!(calls.load(Ordering::Relaxed), 1);
            assert_eq!(busy.load(Ordering::Acquire), !settled);
        }
    }

    #[test]
    fn busy_provider_rejects_without_starting_a_worker() {
        let provider = PortalSaveDialog { busy: Arc::new(AtomicBool::new(true)) };
        let (sender, receiver) = std::sync::mpsc::channel();
        provider.choose("example".into(), Box::new(move |result| { sender.send(result.is_err()).unwrap(); }));
        assert!(receiver.try_recv().unwrap());
        assert!(provider.busy.load(Ordering::Acquire));
    }
}
