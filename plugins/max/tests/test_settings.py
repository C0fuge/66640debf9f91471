import pytest
from contact_hub_sdk import InvalidParams
from max_plugin.settings import Settings


def test_settings_and_secret_proxy_are_explicitly_validated():
    assert Settings.parse({}, {}).request_timeout == 30
    password = "MAX_FIXTURE_PROXY_PASS_581bfbc1"
    unknown_secret = "MAX_FIXTURE_UNKNOWN_TOKEN_92af65b3"
    proxy = f"socks5://user:{password}@example.invalid:1080"
    parsed = Settings.parse({"request_timeout": 5, "proxy": ""}, {"proxy": proxy})
    assert parsed.proxy == proxy
    for settings, secrets, field in [({"request_timeout": True}, {}, "request_timeout"),
                                     ({"request_timeout": 61}, {}, "request_timeout"),
                                     ({"proxy": "https://example.invalid"}, {}, "proxy"),
                                     ({"proxy": f"socks5://user:{password}@example.invalid:invalid-port"}, {}, "proxy"),
                                     ({"unexpected": "value"}, {}, "unexpected"),
                                     ({}, {"session": unknown_secret}, "session")]:
        with pytest.raises(InvalidParams) as error:
            Settings.parse(settings, secrets)
        assert error.value.data["field"] == field
        message = str(error.value)
        assert password not in message and unknown_secret not in message and proxy not in message
