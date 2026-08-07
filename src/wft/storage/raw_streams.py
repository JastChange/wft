import hashlib
import os
import secrets
from pathlib import Path
from typing import BinaryIO

from wft.storage.atomic import StorageFullError as StorageFullError
from wft.storage.atomic import fsync_directory, map_storage_error
from wft.tasks.models import StreamReference


class RawWriter:
    """Stream arbitrary bytes to a same-directory partial file before publication."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._partial = path.parent / f".{path.name}.{secrets.token_hex(8)}.partial"
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            descriptor = os.open(self._partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except OSError as exc:
            raise map_storage_error(exc) from exc
        self._stream: BinaryIO = os.fdopen(descriptor, "wb")
        self._digest = hashlib.sha256()
        self._size = 0
        self._closed = False

    def write(self, chunk: bytes) -> None:
        if self._closed:
            raise ValueError("raw writer is closed")
        try:
            self._stream.write(chunk)
        except OSError as exc:
            raise map_storage_error(exc) from exc
        self._digest.update(chunk)
        self._size += len(chunk)

    def finish(self) -> StreamReference:
        if self._closed:
            raise ValueError("raw writer is closed")
        try:
            self._stream.flush()
            os.fsync(self._stream.fileno())
            self._stream.close()
            self._closed = True
            os.replace(self._partial, self._path)
            fsync_directory(self._path.parent)
        except OSError as exc:
            self._closed = True
            self._stream.close()
            self._partial.unlink(missing_ok=True)
            raise map_storage_error(exc) from exc
        return StreamReference(
            path=self._path.name,
            size_bytes=self._size,
            sha256=self._digest.hexdigest(),
        )

    def abort(self) -> None:
        if not self._closed:
            self._stream.close()
            self._closed = True
        self._partial.unlink(missing_ok=True)


def storage_write_probe(data_dir: Path) -> None:
    probe = data_dir / f".storage-probe.{secrets.token_hex(8)}.partial"
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(probe, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except OSError as exc:
        raise map_storage_error(exc) from exc
    try:
        try:
            data = b"wft-storage-probe\n"
            offset = 0
            while offset < len(data):
                offset += os.write(descriptor, data[offset:])
            os.fsync(descriptor)
        except OSError as exc:
            raise map_storage_error(exc) from exc
    finally:
        os.close(descriptor)
        probe.unlink(missing_ok=True)
