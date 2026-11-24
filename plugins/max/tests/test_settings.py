import pytest
from contact_hub_sdk import InvalidParams
from max_plugin.settings import Settings


def test_settings_and_secret_proxy_are_explicitly_validated():
    assert Settings.parse({}, {}).request_timeout == 30
    parsed = Settings.parse({"request_timeout": 5, "proxy": ""}, {"proxy": "socks5://user:secret@example.invalid:1080"})
    assert parsed.proxy == "socks5://user:secret@example.invalid:1080"
    for settings, secrets, field in [({"request_timeout": True}, {}, "request_timeout"),
                                     ({"request_timeout": 61}, {}, "request_timeout"),
                                     ({"proxy": "https://example.invalid"}, {}, "proxy"),
                                     ({"unexpected": "value"}, {}, "unexpected"),
                                     ({}, {"session": "do-not-discover"}, "session")]:
        with pytest.raises(InvalidParams) as error:
            Settings.parse(settings, secrets)
        assert error.value.data["field"] == field
        assert "secret" not in str(error.value) and "do-not-discover" not in str(error.value)
