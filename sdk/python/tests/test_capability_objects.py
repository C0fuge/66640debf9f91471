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
