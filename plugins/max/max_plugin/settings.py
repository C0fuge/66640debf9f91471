from dataclasses import dataclass
from urllib.parse import urlsplit

from contact_hub_sdk import InvalidParams


@dataclass(frozen=True, slots=True)
class Settings:
    request_timeout: int
    proxy: str | None

    @classmethod
    def parse(cls, settings: dict, secrets: dict) -> "Settings":
        unknown = set(settings) - {"request_timeout", "proxy"}
        if unknown:
            raise InvalidParams("Unknown MAX setting", field=sorted(unknown)[0])
        unknown = set(secrets) - {"proxy"}
        if unknown:
            raise InvalidParams("Unknown MAX secret", field=sorted(unknown)[0])
        timeout = settings.get("request_timeout", 30)
        if type(timeout) is not int or not 5 <= timeout <= 60:
            raise InvalidParams("API timeout must be 5–60 seconds", field="request_timeout")
        proxy = secrets.get("proxy", settings.get("proxy"))
        if proxy == "":
            proxy = None
        if proxy is not None:
            if not isinstance(proxy, str):
                raise InvalidParams("Proxy must be a URL", field="proxy")
            try:
                url = urlsplit(proxy)
                valid = url.scheme in {"socks5", "socks4", "http"} and url.hostname and url.port
            except ValueError:
                valid = False
            if not valid:
                raise InvalidParams("Proxy must include a supported scheme, host and port", field="proxy")
        return cls(timeout, proxy)
