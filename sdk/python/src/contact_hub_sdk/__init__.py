"""Contact Hub plugin SDK (plugin API v1, docs/plugin-api.md).

Minimal plugin::

    from contact_hub_sdk import Plugin, method, types as T

    class Echo(Plugin):
        capabilities = {}

        async def on_account_start(self, account):
            account.spawn(account.status("connected"))

        @method("messages.fetch_recent")
        async def fetch(self, account, p: T.MessagesFetchRecentParams):
            return T.PagedMessagesResult(messages=[])

        @method("messages.send")
        async def send(self, account, p: T.MessagesSendParams):
            ...

    Echo().run()
"""

from . import capabilities, errors, types
from .errors import (
    AuthRequired,
    Cancelled,
    FloodWait,
    Forbidden,
    Internal,
    InvalidParams,
    NetworkError,
    NotFound,
    NotSupported,
    PluginError,
    RpcTimeout,
    Unavailable,
    UnknownAccount,
)
from .plugin import Account, AuthFlow, Plugin, method, progress
from .registry import API_VERSION, DEFAULT_TIMEOUT_SECONDS, EVENTS, METHODS, GatedParam, MethodInfo
from .transport import MAX_FRAME, Connection, RequestContext, current_request
from .wire import DecodeError, format_timestamp, from_wire, parse_timestamp, to_wire

__version__ = "1.0.0"

__all__ = [
    "API_VERSION",
    "DEFAULT_TIMEOUT_SECONDS",
    "EVENTS",
    "MAX_FRAME",
    "METHODS",
    "Account",
    "AuthFlow",
    "AuthRequired",
    "Cancelled",
    "Connection",
    "DecodeError",
    "FloodWait",
    "Forbidden",
    "GatedParam",
    "Internal",
    "InvalidParams",
    "MethodInfo",
    "NetworkError",
    "NotFound",
    "NotSupported",
    "Plugin",
    "PluginError",
    "RequestContext",
    "RpcTimeout",
    "Unavailable",
    "UnknownAccount",
    "capabilities",
    "current_request",
    "errors",
    "format_timestamp",
    "from_wire",
    "method",
    "parse_timestamp",
    "progress",
    "to_wire",
    "types",
]
