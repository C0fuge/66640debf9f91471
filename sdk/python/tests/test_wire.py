"""Section 8 conventions on the typed DTOs."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from conftest import validate
from contact_hub_sdk import DecodeError, FloodWait, NotSupported, PluginError, from_wire, to_wire, types as T
from contact_hub_sdk.wire import format_timestamp, parse_timestamp

MIN = {"id": "1", "chat_id": "c", "direction": "in", "sent_at": "2026-10-19T14:03:27.512Z"}


def test_unknown_enum_values_are_preserved():
    m = from_wire(T.Message, {**MIN, "direction": "sideways", "media": {"kind": "hologram"}})
    assert m.direction == "sideways" and not isinstance(m.direction, T.Direction)
    assert to_wire(m)["media"] == {"kind": "hologram", "has_spoiler": False}
    assert from_wire(T.Message, MIN).direction is T.Direction.IN


def test_unknown_fields_ignored_and_null_is_absent():
    m = from_wire(T.Message, {**MIN, "future_field": 1, "text": None, "reactions": None})
    assert m.text is None and m.reactions is None
    assert "future_field" not in to_wire(m)


def test_absent_array_is_not_empty_array():
    assert "reactions" not in to_wire(from_wire(T.Message, MIN))
    assert to_wire(from_wire(T.Message, {**MIN, "reactions": []}))["reactions"] == []


def test_optionals_omitted_merge_fields_not_defaulted():
    # Spec 8.4: Conversation/Topic/Message are merge-updated; a partial update carries only
    # what it says, so absent counters and flags are never written as 0/False.
    out = to_wire(T.Conversation(id="c", kind=T.ConversationKind.DM))
    assert out == {"id": "c", "kind": "dm"}
    validate(out, "dto/conversation.json#/$defs/Conversation")
    assert to_wire(T.Conversation(id="c", kind=T.ConversationKind.DM, unread_count=0)) == {
        "id": "c", "kind": "dm", "unread_count": 0}
    assert "edited" not in to_wire(from_wire(T.Message, MIN))
    # Value-like nested objects keep their spec defaults (they are replaced wholesale).
    assert to_wire(T.Poll(id="p", question="q", options=[]))["closed"] is False
    assert to_wire(T.ReactionKey(emoji="x")) == {"emoji": "x"}


def test_timestamps():
    t = parse_timestamp("2026-10-19T16:03:27.512999+02:00")
    assert t == datetime(2026, 10, 19, 14, 3, 27, 512000, tzinfo=UTC)
    assert format_timestamp(t) == "2026-10-19T14:03:27.512Z"
    assert format_timestamp(parse_timestamp("2026-10-19t14:03:27z")) == "2026-10-19T14:03:27.000Z"
    assert format_timestamp(datetime(2026, 1, 1, 3, tzinfo=timezone(timedelta(hours=3)))) == "2026-01-01T00:00:00.000Z"
    with pytest.raises(ValueError):
        parse_timestamp("1760882607")


def test_decode_errors_name_the_field():
    with pytest.raises(DecodeError) as e:
        from_wire(T.Message, {**MIN, "entities": [{"type": "bold", "offset": "x", "length": 1}]})
    assert e.value.field == "entities[0].offset"
    with pytest.raises(DecodeError) as e:
        from_wire(T.Message, {"id": "1"})
    assert e.value.field == "chat_id"


def test_error_objects():
    assert FloodWait(36.2).to_wire() == {"code": -32003, "message": "Too many requests",
                                         "data": {"code": "FLOOD_WAIT", "seconds": 37}}
    assert NotSupported(capability="edit").to_wire()["data"] == {"code": "NOT_SUPPORTED", "capability": "edit"}
    e = PluginError.from_wire({"code": -32003, "message": "x", "data": {"code": "FLOOD_WAIT", "seconds": 5}})
    assert isinstance(e, FloodWait) and e.seconds == 5
    odd = PluginError.from_wire({"code": -32099, "message": "?", "data": {"code": "BRAND_NEW"}})
    assert odd.code == "BRAND_NEW" and odd.numeric == -32603


@pytest.mark.parametrize("payload", [{}, {"conversation": None}, {"conversation": {}},
                                      {"conversation": {"id": "target"}}])
def test_chat_get_requires_one_identified_conversation(payload):
    with pytest.raises(DecodeError):
        from_wire(T.ChatsGetResult, payload)
    with pytest.raises(AssertionError):
        validate(payload, "methods/chats.json#/$defs/chats.get.result")


def test_chat_get_optional_flags_remain_sparse():
    payload = {"future_result": True, "conversation": {"id": "c", "kind": "dm", "pinned": False,
                                                       "marked_unread": None, "future_field": 42}}
    validate(payload, "methods/chats.json#/$defs/chats.get.result")
    assert to_wire(from_wire(T.ChatsGetResult, payload)) == {"conversation": {"id": "c", "kind": "dm", "pinned": False}}
