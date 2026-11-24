"""Account-private MTProto state and sanitized platform errors."""
import asyncio
import logging
import os
import re
import stat
from contextlib import contextmanager

from contact_hub_sdk import errors as rpc
from telethon import TelegramClient, errors as tg

from python_socks import ProxyError, ProxyType


def translated(exc):
    # RPC errors and library exception arguments may contain request payloads.
    if isinstance(exc, rpc.PluginError):
        return exc
    if isinstance(exc, (tg.FloodWaitError, tg.FloodPremiumWaitError, tg.SlowModeWaitError)):
        return rpc.FloodWait(exc.seconds)
    if isinstance(exc, (tg.UnauthorizedError, tg.AuthKeyNotFound, tg.AuthKeyDuplicatedError)):
        return rpc.AuthRequired()
    if isinstance(exc, (tg.ForbiddenError, tg.ChannelPrivateError, tg.ChatAdminRequiredError,
                        tg.MessageEditTimeExpiredError, tg.PinRestrictedError, tg.UserBannedInChannelError)):
        return rpc.Forbidden()
    if isinstance(exc, (tg.PeerIdInvalidError, tg.ChannelInvalidError, tg.MsgIdInvalidError, tg.MessageIdInvalidError)):
        return rpc.NotFound()
    if isinstance(exc, (TimeoutError, tg.TimedOutError)):
        return rpc.RpcTimeout()
    if isinstance(exc, (OSError, ProxyError, tg.ServerError)):
        return rpc.NetworkError()
    if isinstance(exc, tg.BadRequestError):
        return rpc.InvalidParams("Telegram rejected this request.")
    return rpc.Internal("Telegram operation failed.")


@contextmanager
def platform_errors():
    try:
        yield
    except rpc.PluginError:
        raise
    except Exception as exc:
        raise translated(exc) from None


def options(account):
    api_id = account.setting("api_id")
    api_hash = account.secrets.get("api_hash", "")
    if type(api_id) is not int or not 0 < api_id < 2**31:
        raise rpc.InvalidParams("Enter the numeric Telegram API ID.", field="api_id")
    if not re.fullmatch("[0-9a-fA-F]{32}", api_hash):
        raise rpc.InvalidParams("Enter the Telegram API hash.", field="api_hash")
    host = account.setting("proxy_host")
    credentials = [account.secrets.get(key) for key in ("proxy_username", "proxy_password")]
    proxy = None
    if host:
        if not isinstance(host, str) or any(c in host for c in "@/\\?#") or any(c.isspace() for c in host):
            raise rpc.InvalidParams("Enter a proxy hostname or IP address.", field="proxy_host")
        port = account.setting("proxy_port", 1080)
        if type(port) is not int or not 0 < port < 65536:
            raise rpc.InvalidParams(field="proxy_port")
        proxy = (ProxyType.SOCKS5, host, port, True, *credentials)
    elif any(credentials):
        raise rpc.InvalidParams("A proxy host is required for proxy credentials.", field="proxy_host")
    return api_id, api_hash, proxy


def session_path(directory):
    if not directory.is_absolute() or directory.is_symlink():
        raise rpc.InvalidParams(field="state_dir")
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    directory.chmod(0o700)
    path = directory / "telegram.session"
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise rpc.InvalidParams(field="state_dir")
        os.fchmod(fd, 0o600)
    finally:
        os.close(fd)
    return path


def make_client(path, api_id, api_hash, proxy):
    # Telethon debug records can include update data. Protocol status/errors carry
    # the user-facing diagnostics and never format platform requests.
    logger = logging.getLogger("contact_hub.telegram.mtproto")
    logger.handlers = [logging.NullHandler()]
    logger.propagate = False
    return TelegramClient(str(path), api_id, api_hash, proxy=proxy, base_logger=logger,
                          auto_reconnect=False, connection_retries=0, request_retries=0,
                          flood_sleep_threshold=0, sequential_updates=True, catch_up=True)


async def disconnect(client):
    # TelegramClient.disconnect() shields a detached task. Directly await the
    # teardown coroutine in the pinned release so the host deadline cancels the
    # actual teardown before another account.start opens the same session.
    try:
        async with asyncio.timeout(1.5):
            if isinstance(client, TelegramClient):
                await client._disconnect_coro()
            else:
                await client.disconnect()
    finally:
        if client.session is not None:
            client.session.close()
