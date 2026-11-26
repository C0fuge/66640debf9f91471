"""Capability objects, including empty objects, enable a method or gated parameter."""

import pytest

from contact_hub_sdk import NotSupported, Plugin
from echo_plugin import Echo


def test_capability_object_requires_method_handler():
    class MissingHandler(Echo):
        capabilities = {**Echo.capabilities, "messages.get": {}}

    with pytest.raises(TypeError, match=r"messages\.get"):
        MissingHandler()


@pytest.mark.parametrize("capability", [True, {}, {"option": 1}])
def test_capability_object_enables_explicit_false_parameter(capability):
    Plugin._check_param_caps("chats.set_flags", {"pinned": False}, {"chat.pin": capability})


@pytest.mark.parametrize("caps", [{}, {"chat.pin": False}, {"chat.pin": None}])
def test_disabled_capability_rejects_explicit_false_parameter(caps):
    with pytest.raises(NotSupported):
        Plugin._check_param_caps("chats.set_flags", {"pinned": False}, caps)
    Plugin._check_param_caps("chats.set_flags", {"pinned": None}, caps)


@pytest.mark.parametrize("value", [None, True, False, 1, [], {}, "ON_SUCCESS", "on_success ", "later", "on_success"])
def test_source_lifetime_promise_is_exact_without_changing_raw_upload_support(value):
    from contact_hub_sdk import file_source_released_on_success
    caps = {"send.file": {"source_release": value}}
    assert Plugin._enabled(caps, "send.file")
    assert file_source_released_on_success(caps) is (type(value) is str and value == "on_success")


@pytest.mark.parametrize("flag,enabled", [(True, True), (False, False), ({}, True)])
def test_upload_defaults_do_not_imply_source_settlement(flag, enabled):
    from contact_hub_sdk import file_source_released_on_success
    caps = {"send.file": flag}
    assert Plugin._enabled(caps, "send.file") is enabled
    assert not file_source_released_on_success(caps)
    assert not file_source_released_on_success({})


def test_typed_source_promise_keeps_unknown_strings_and_schema_rejects_malformed_values():
    from contact_hub_sdk import capabilities, from_wire, to_wire
    from conftest import validate
    for value in ("on_success", "future"):
        wire = {"source_release": value, "max_bytes": 7}
        assert to_wire(from_wire(capabilities.SendFileParams, wire)) == wire
        validate({"send.file": wire}, "capabilities.json#/$defs/Capabilities")
    for value in (None, 1, False, [], {}):
        with pytest.raises(AssertionError):
            validate({"send.file": {"source_release": value}}, "capabilities.json#/$defs/Capabilities")
