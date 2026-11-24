"""Private cache writes anchored to directory descriptors, published atomically."""
import os
import stat
import uuid
from contextlib import contextmanager
from pathlib import Path

from contact_hub_sdk import errors as rpc

MAX_BYTES = 2 * 1024**3


def open_directory(path, *, create=False):
    path = Path(path)
    if not path.is_absolute() or ".." in path.parts:
        raise rpc.InvalidParams("The media cache must be an absolute directory.", field="cache_dir")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for index, component in enumerate(path.parts[1:]):
            try:
                child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            except FileNotFoundError:
                if not create or index != len(path.parts) - 2:
                    raise
                os.mkdir(component, mode=0o700, dir_fd=fd)
                child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


class LimitedWriter:
    def __init__(self, stream, limit):
        self.stream, self.limit = stream, limit
        self.size = 0

    def write(self, data):
        if self.size + len(data) > self.limit:
            raise rpc.NotSupported("This attachment exceeds the download size limit.", capability="media.download")
        count = self.stream.write(data)
        self.size += count
        return count

    def flush(self):
        self.stream.flush()

    def tell(self):
        return self.stream.tell()


@contextmanager
def atomic_file(cache_dir, suffix=".bin", *, limit=MAX_BYTES):
    """Yield (bounded stream, publish); a failed/cancelled operation leaves no part file."""
    root = Path(cache_dir)
    directory = root / "telegram-media"
    root_fd = open_directory(root, create=True)
    try:
        try:
            os.mkdir("telegram-media", mode=0o700, dir_fd=root_fd)
        except FileExistsError:
            pass
        directory_fd = os.open("telegram-media", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd)
        os.fchmod(directory_fd, 0o700)
    finally:
        os.close(root_fd)
    token = uuid.uuid4().hex
    temporary, final = "." + token + ".part", token + suffix
    try:
        fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600, dir_fd=directory_fd)
        with os.fdopen(fd, "wb") as raw:
            stream = LimitedWriter(raw, limit)

            def publish():
                # The cache may be cleared or replaced while an RPC is active.
                # Refuse to return a pathname now pointing at a different inode.
                check_fd = open_directory(directory)
                try:
                    before, after = os.fstat(directory_fd), os.fstat(check_fd)
                    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
                        raise rpc.Internal("The media cache changed during download.")
                finally:
                    os.close(check_fd)
                raw.flush()
                os.fsync(raw.fileno())
                if not stat.S_ISREG(os.fstat(raw.fileno()).st_mode):
                    raise rpc.Internal("The media cache file is invalid.")
                os.replace(temporary, final, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
                os.fsync(directory_fd)
                return str(directory / final)

            yield stream, publish
    finally:
        try:
            os.unlink(temporary, dir_fd=directory_fd)
        except FileNotFoundError:
            pass
        os.close(directory_fd)
