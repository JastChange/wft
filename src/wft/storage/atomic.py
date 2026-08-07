import errno
import json
import os
import secrets
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import cast


class UnsupportedSchemaVersion(ValueError):
    """Raised when authoritative data cannot be read by this release."""


class StorageFullError(OSError):
    """Raised when an authoritative write cannot proceed because storage is full."""


def map_storage_error(exc: OSError) -> OSError:
    if exc.errno == errno.ENOSPC:
        return StorageFullError(errno.ENOSPC, "authoritative storage is full")
    return exc


def fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_atomic_json(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.parent / f".{path.name}.{secrets.token_hex(8)}.partial"
    try:
        descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(partial, path)
        fsync_directory(path.parent)
    except OSError as exc:
        raise map_storage_error(exc) from exc
    finally:
        partial.unlink(missing_ok=True)


def read_versioned_json(path: Path) -> dict[str, object]:
    with path.open(encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise UnsupportedSchemaVersion("versioned JSON root must be an object")
    version = value.get("schema_version")
    if not isinstance(version, str) or version.split(".", maxsplit=1)[0] != "1":
        raise UnsupportedSchemaVersion(f"unsupported schema version: {version!r}")
    return cast(dict[str, object], value)


def iter_json_files(directory: Path) -> Iterator[Path]:
    if not directory.exists():
        return
    for path in sorted(directory.iterdir()):
        if path.is_file() and not path.name.startswith(".") and path.suffix == ".json":
            yield path
