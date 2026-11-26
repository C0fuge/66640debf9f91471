//! The spec's tolerance and versioning rules (§8, §13, §18), checked on the types.

use ch_protocol::dto::*;
use ch_protocol::events::*;
use ch_protocol::*;
use serde_json::json;

#[test]
fn version_rule_is_major_only() {
    assert_eq!(ApiVersion::current().to_string(), API_VERSION);
    assert_eq!(ApiVersion::current().major, API_MAJOR);
    assert!(versions_compatible("1.0.0", "1.7.3"));
    assert!(versions_compatible("1.2.0-rc.1+build5", "1.0.0"));
    assert!(!versions_compatible("2.0.0", "1.9.9"));
    assert!(!versions_compatible("1.0", "1.0.0"));
    assert!(!versions_compatible("01.0.0", "1.0.0"));
    assert!(!versions_compatible("", "1.0.0"));
    assert_eq!(
        ApiVersion::parse("1.0.0.0"),
        Err(ParseVersionError::Invalid("extra component"))
    );
    let v = ApiVersion::parse("1.2.3-beta+7").unwrap();
    assert_eq!(
        (v.major, v.minor, v.patch, v.rest.as_str()),
        (1, 2, 3, "-beta+7")
    );
}

#[test]
fn timestamps_normalise_to_utc_millis() {
    for (input, want) in [
        ("2026-10-19T14:03:27.512Z", "2026-10-19T14:03:27.512Z"),
        ("2026-10-19T16:03:27.512+02:00", "2026-10-19T14:03:27.512Z"),
        ("2026-10-19T14:03:27Z", "2026-10-19T14:03:27.000Z"),
        ("2026-10-19T14:03:27.512987Z", "2026-10-19T14:03:27.512Z"),
        ("2026-10-19T14:03:27.5Z", "2026-10-19T14:03:27.500Z"),
    ] {
        let t: Timestamp = serde_json::from_value(json!(input)).unwrap();
        assert_eq!(serde_json::to_value(t).unwrap(), json!(want), "{input}");
        assert_eq!(t.to_string(), want);
    }
    assert!(
        serde_json::from_value::<Timestamp>(json!(1_700_000_000)).is_err(),
        "no unix integers"
    );
    assert!(serde_json::from_value::<Timestamp>(json!("2026-10-19 14:03:27")).is_err());
    let t = Timestamp::parse("2026-10-19T14:03:27.512Z").unwrap();
    assert_eq!(Timestamp::from_unix_millis(t.unix_millis()), Some(t));
    assert_eq!(t.unix_millis() % 1000, 512);
}

#[test]
fn unknown_enum_values_are_kept() {
    let m: Message = serde_json::from_value(json!({
        "id": "1", "chat_id": "c", "direction": "sideways", "sent_at": "2026-01-01T00:00:00Z",
        "media": {"kind": "hologram"},
        "entities": [{"type": "rainbow", "offset": 0, "length": 1}],
        "action": {"kind": "teleported"}
    }))
    .unwrap();
    assert_eq!(m.direction, Direction::Unknown("sideways".into()));
    assert!(!m.direction.is_known());
    assert_eq!(m.media.as_ref().unwrap().kind.as_str(), "hologram");
    assert_eq!(
        m.entities.as_ref().unwrap()[0].kind,
        EntityType::Unknown("rainbow".into())
    );
    // and they go back out unchanged
    let v = serde_json::to_value(&m).unwrap();
    assert_eq!(v["direction"], "sideways");
    assert_eq!(v["action"]["kind"], "teleported");
    assert_eq!(AccountState::from("connected"), AccountState::Connected);
    assert_eq!("qr".parse::<AuthStepKind>().unwrap(), AuthStepKind::Qr);
    assert_eq!(
        CapabilityKey::from("x-foo.bar"),
        CapabilityKey::Unknown("x-foo.bar".into())
    );
}

#[test]
fn unknown_fields_are_ignored_and_null_equals_absent() {
    let c: Conversation = serde_json::from_value(json!({
        "id": "1", "kind": "dm", "future_field": {"deep": [1, 2]}, "title": null, "muted": null, "draft": null
    }))
    .unwrap();
    assert_eq!(c.title, None);
    assert_eq!(c.muted, None);
    assert_eq!(c.draft, None);
    assert_eq!(c.archived, None, "merge fields stay unknown (absent = unchanged, §8.4)");
    assert_eq!(c.unread_count, None);
    assert_eq!(
        c.folder_ids, None,
        "absent array means not reported, not empty"
    );
    let v = serde_json::to_value(&c).unwrap();
    assert!(
        v.get("title").is_none(),
        "optional fields are omitted, not written as null"
    );
    assert!(v.get("future_field").is_none());
    // Empty accepts any object.
    let e: Empty = serde_json::from_value(json!({"whatever": 1})).unwrap();
    assert_eq!(serde_json::to_value(e).unwrap(), json!({}));
    // ext survives verbatim.
    let ct: Contact = serde_json::from_value(
        json!({"id": "u", "ext": {"telegram": {"peer_color": 3, "is_scam": false}}}),
    )
    .unwrap();
    assert_eq!(ct.ext.as_ref().unwrap()["telegram"]["peer_color"], 3);
}

#[test]
fn reaction_key_is_one_of_three() {
    assert_eq!(
        serde_json::to_value(ReactionKey::emoji("❤")).unwrap(),
        json!({"emoji": "❤"})
    );
    assert_eq!(
        serde_json::to_value(ReactionKey::custom("5")).unwrap(),
        json!({"custom_emoji_id": "5"})
    );
    assert_eq!(
        serde_json::to_value(ReactionKey::paid()).unwrap(),
        json!({"paid": true})
    );
    assert_eq!(
        serde_json::from_value::<ReactionKey>(json!({"paid": true})).unwrap(),
        ReactionKey::paid()
    );
    assert!(serde_json::from_value::<ReactionKey>(json!({})).is_err());
}

#[test]
fn error_object_rules() {
    let e = ErrorObject::flood_wait(37);
    assert_eq!(e.code, -32003);
    assert_eq!(e.code(), ErrorCode::FloodWait);
    assert_eq!(e.data.as_ref().unwrap().seconds, Some(37));
    // Unknown data.code → INTERNAL behaviour, string kept.
    let e: ErrorObject = serde_json::from_value(
        json!({"code": -32099, "message": "?", "data": {"code": "SOMETHING_NEW", "x": 1}}),
    )
    .unwrap();
    assert_eq!(e.code(), ErrorCode::Internal);
    assert_eq!(e.data.as_ref().unwrap().code.as_str(), "SOMETHING_NEW");
    assert_eq!(e.data.as_ref().unwrap().extra["x"], 1);
    assert_eq!(
        serde_json::to_value(&e).unwrap()["data"]["code"],
        "SOMETHING_NEW"
    );
    // Bare JSON-RPC error without data: numeric code decides, unknown → INTERNAL.
    let e: ErrorObject =
        serde_json::from_value(json!({"code": -32601, "message": "nope"})).unwrap();
    assert_eq!(e.code(), ErrorCode::MethodNotFound);
    let e: ErrorObject = serde_json::from_value(json!({"code": -1, "message": "nope"})).unwrap();
    assert_eq!(e.code(), ErrorCode::Internal);
    assert!(ErrorCode::Network.is_retryable());
    assert!(!ErrorCode::Forbidden.is_retryable());
    assert_eq!(
        ErrorObject::not_supported("send.ttl")
            .data
            .unwrap()
            .capability
            .as_deref(),
        Some("send.ttl")
    );
}

#[test]
fn envelope_discriminates_frames() {
    let req: Envelope =
        serde_json::from_value(json!({"jsonrpc": "2.0", "id": 7, "method": "ping"})).unwrap();
    assert!(
        matches!(&req, Envelope::Request(r) if r.params.is_none() && r.id == RequestId::Number(7))
    );
    let n: Envelope = serde_json::from_value(json!({"jsonrpc": "2.0", "method": "status", "params": {"account_id": "a", "state": "connecting"}})).unwrap();
    assert!(matches!(n, Envelope::Notification(_)));
    let ok: Envelope =
        serde_json::from_value(json!({"jsonrpc": "2.0", "id": "s-1", "result": {}})).unwrap();
    assert!(matches!(&ok, Envelope::Success(s) if s.id == RequestId::from("s-1")));
    let err: Envelope = serde_json::from_value(
        json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32700, "message": "bad json"}}),
    )
    .unwrap();
    assert!(
        matches!(&err, Envelope::Error(e) if e.id.is_none() && e.error.code() == ErrorCode::ParseError)
    );
    assert!(
        serde_json::from_value::<Envelope>(json!([{"jsonrpc": "2.0", "id": 1, "method": "ping"}]))
            .is_err(),
        "batches are rejected"
    );
    // Builders.
    let r = Request::new(3, "ping", Empty {});
    assert_eq!(
        serde_json::to_value(&r).unwrap(),
        json!({"jsonrpc": "2.0", "id": 3, "method": "ping", "params": {}})
    );
    let c = CancelParams::notification(42);
    assert_eq!(
        serde_json::to_value(&c).unwrap(),
        json!({"jsonrpc": "2.0", "method": "$/cancelRequest", "params": {"id": 42}})
    );
    let resp: Response<Empty> = Response::ok(3, Empty {});
    assert_eq!(
        serde_json::to_value(&resp).unwrap(),
        json!({"jsonrpc": "2.0", "id": 3, "result": {}})
    );
    let resp: Response<Empty> =
        Response::err(Some(3.into()), ErrorObject::new(ErrorCode::Timeout, "t"));
    assert_eq!(resp.into_result().unwrap_err().code(), ErrorCode::Timeout);
}

#[test]
fn events_map_names_to_types() {
    let e = Event::from_params("status", json!({"account_id": "a", "state": "connected"}))
        .unwrap()
        .unwrap();
    assert_eq!(e.name(), "status");
    assert_eq!(e.account_id(), "a");
    assert!(
        matches!(&e, Event::Status(s) if s.state == AccountState::Connected && !s.reconnecting)
    );
    assert!(
        Event::from_params("not.an.event", json!({})).is_none(),
        "unknown events are ignored, not errors"
    );
    assert!(
        Event::from_params("status", json!({"account_id": "a"}))
            .unwrap()
            .is_err(),
        "missing required field"
    );
    assert_eq!(EVENT_NAMES.len(), 18);
    for name in EVENT_NAMES {
        assert!(!method(name).is_some(), "{name} is an event, not a method");
    }
}

#[test]
fn registry_timeouts_follow_the_table() {
    assert_eq!(method("initialize").unwrap().timeout_seconds, 30);
    assert_eq!(method("ping").unwrap().timeout_seconds, 10);
    assert_eq!(method("media.download").unwrap().timeout_seconds, 600);
    assert!(method("media.download").unwrap().progress);
    assert_eq!(
        method("messages.fetch_recent").unwrap().timeout_seconds,
        120
    );
    assert_eq!(
        method("messages.send").unwrap().timeout_seconds,
        DEFAULT_TIMEOUT_SECONDS
    );
    assert_eq!(method("messages.send").unwrap().capability, None);
    assert_eq!(
        method("stickers.send").unwrap().capability,
        Some("stickers.send")
    );
    assert_eq!(
        method("secrets.set").unwrap().direction,
        CallDirection::PluginToCore
    );
    assert!(
        method("$/cancelRequest").is_none(),
        "cancel is a notification, not a method"
    );
    assert_eq!(METHODS.len(), 122);
}

#[test]
fn gated_params_are_detected_from_one_table() {
    let send = method("messages.send").unwrap();
    let has_none = |_: &str| false;
    let params = json!({"account_id": "a", "chat_id": "c", "text": "hi", "silent": null});
    assert_eq!(send.missing_capabilities(&params, has_none).count(), 0, "null = absent");
    let params = json!({"account_id": "a", "chat_id": "c", "text": "hi", "silent": true,
        "reply_to": {"message_id": "1", "quote_text": "q"}});
    let missing: Vec<&str> = send
        .missing_capabilities(&params, has_none)
        .map(|g| g.capability)
        .collect();
    assert_eq!(missing, ["quote_reply", "send.silent"]);
    let only_silent = |c: &str| c == "send.silent";
    let missing: Vec<&str> = send
        .missing_capabilities(&params, only_silent)
        .map(|g| g.capability)
        .collect();
    assert_eq!(missing, ["quote_reply"]);
    // Value-conditioned rows: kind = "voice" and revoke = false.
    let file = method("messages.send_file").unwrap();
    let voice = json!({"kind": "voice"});
    let photo = json!({"kind": "photo"});
    assert_eq!(
        file.missing_capabilities(&voice, has_none).map(|g| g.capability).collect::<Vec<_>>(),
        ["send.voice"]
    );
    assert_eq!(file.missing_capabilities(&photo, has_none).count(), 0);
    let del = method("messages.delete").unwrap();
    assert_eq!(del.missing_capabilities(&json!({"revoke": true}), has_none).count(), 0);
    assert_eq!(del.missing_capabilities(&json!({"revoke": false}), has_none).count(), 1);
    // Array segment: any element with a custom emoji needs reactions.custom.
    let react = method("reactions.set").unwrap();
    let custom = json!({"reactions": [{"emoji": "x"}, {"custom_emoji_id": "5"}]});
    let plain = json!({"reactions": [{"emoji": "x"}]});
    assert_eq!(react.missing_capabilities(&custom, has_none).count(), 1);
    assert_eq!(react.missing_capabilities(&plain, has_none).count(), 0);
    assert!(method("ping").unwrap().gated_params.is_empty());
}

#[test]
fn manifest_validation() {
    let mut m: Manifest = serde_json::from_value(json!({
        "id": "discord", "name": "Discord", "version": "0.3.1", "api_version": "1.4.0",
        "entry": ["./run"], "brand_color": "#5865F2", "multi_account": true, "auth_methods": ["token", "carrier_pigeon"]
    }))
    .unwrap();
    m.validate().unwrap();
    assert!(m.is_compatible());
    assert_eq!(
        m.auth_methods[1],
        AuthMethod::Unknown("carrier_pigeon".into())
    );
    m.id = "Discord".into();
    assert!(m.validate().is_err());
    m.id = "discord".into();
    m.brand_color = "5865F2".into();
    assert!(m.validate().is_err());
    m.brand_color = "#5865F2".into();
    m.api_version = "2.0.0".into();
    assert!(!m.is_compatible());
}
