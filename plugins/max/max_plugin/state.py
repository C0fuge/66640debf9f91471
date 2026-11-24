"""Small account-private store implementing PyMax 2.5.0 StoreProtocol.

Only files under the explicitly supplied state_dir are accessed. This is a new
format, not an importer for any legacy database. No environment/session discovery.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import secrets
import stat
from pathlib import Path
from typing import Any

from contact_hub_sdk import InvalidParams, PluginError


MAX_STATE_BYTES = 64 * 1024 * 1024


class PrivateState:
    def __init__(self, directory: Path, account_id: str):
        self.directory = directory
        self._lock_fd: int | None = None
        if not directory.is_absolute() or directory.is_symlink():
            raise InvalidParams("MAX state_dir must be an absolute private directory", field="state_dir")
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not directory.is_dir() or directory.stat().st_uid != os.getuid():
            raise InvalidParams("MAX state_dir must be owned by the current user", field="state_dir")
        directory.chmod(0o700)
        try:
            self._lock_fd = self._open("account.lock", os.O_RDWR | os.O_CREAT)
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            owner = hashlib.sha256(account_id.encode()).hexdigest()
            identity = self.read("identity.json")
            if identity is None:
                identity = {"version": 1, "owner": owner, "device_id": secrets.token_hex(8),
                            "mt_instance_id": secrets.token_hex(8), "mode": None, "user_agent": None}
                self.write("identity.json", identity)
            if identity.get("version") != 1 or identity.get("owner") != owner:
                raise InvalidParams("MAX state directory belongs to another account or format", field="state_dir")
            if not all(isinstance(identity.get(k), str) and identity[k] for k in ("device_id", "mt_instance_id")):
                raise InvalidParams("Invalid MAX device identity", field="state_dir")
            self.identity = identity
            self.ledger = self.read("sends.json") or {}
        except BaseException:
            self.release()
            raise

    def _open(self, name: str, flags: int) -> int:
        fd = os.open(self.directory / name, flags | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, 0o600)
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or info.st_size > MAX_STATE_BYTES:
            os.close(fd)
            raise InvalidParams("Unsafe MAX state file", field="state_dir")
        os.fchmod(fd, 0o600)
        return fd

    def read(self, name: str) -> dict[str, Any] | None:
        try:
            fd = self._open(name, os.O_RDONLY)
        except FileNotFoundError:
            return None
        with os.fdopen(fd, "r", encoding="utf-8") as stream:
            data = json.load(stream)
        if not isinstance(data, dict):
            raise InvalidParams("Invalid MAX private state", field="state_dir")
        return data

    def write(self, name: str, data: dict[str, Any]) -> None:
        # Existing symlinks are never followed (including when replacing a file).
        if (self.directory / name).is_symlink():
            raise InvalidParams("Unsafe MAX state file", field="state_dir")
        encoded = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > MAX_STATE_BYTES:
            raise PluginError("MAX private state exceeds the 64 MiB file limit")
        temp = f".{name}.{secrets.token_hex(8)}"
        fd = self._open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(self.directory / temp, self.directory / name)
            dir_fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        finally:
            (self.directory / temp).unlink(missing_ok=True)

    def save_identity(self) -> None:
        self.write("identity.json", self.identity)

    @property
    def has_session(self) -> bool:
        return self.read("session.json") is not None

    async def save_session(self, session_info: Any) -> None:
        data = session_info.model_dump(mode="json")
        if data["device_id"] != self.identity["device_id"]:
            raise PluginError("MAX session device identity mismatch")
        self.write("session.json", data)

    async def load_session(self) -> Any:
        data = self.read("session.json")
        if data is None:
            return None
        from pymax.session.models import SessionInfo
        result = SessionInfo.model_validate(data)
        if result.device_id != self.identity["device_id"]:
            raise PluginError("MAX session device identity mismatch")
        return result

    async def update_token(self, old_token: str, new_token: str, /) -> None:
        data = self.read("session.json")
        if data is None or data.get("token") != old_token:
            raise PluginError("MAX session changed while updating token")
        data["token"] = new_token
        self.write("session.json", data)

    async def load_session_by_device_id(self, device_id: str) -> Any:
        session = await self.load_session()
        return session if session and session.device_id == device_id else None

    async def load_session_by_phone(self, phone: str) -> Any:
        session = await self.load_session()
        return session if session and session.phone == phone else None

    async def delete_session(self, token: str, /) -> None:
        data = self.read("session.json")
        if data and data.get("token") == token:
            (self.directory / "session.json").unlink()

    async def close(self) -> None:
        # PyMax calls close between connection attempts; the account owns the lock.
        pass

    def release(self) -> None:
        if self._lock_fd is not None:
            os.close(self._lock_fd)
            self._lock_fd = None
