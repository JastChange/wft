import json
import os
import secrets
from pathlib import Path

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

_hasher = PasswordHasher()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _new_credentials() -> tuple[str, dict[str, str]]:
    password = secrets.token_urlsafe(24)
    return password, {
        "schema_version": "1.0",
        "username": "admin",
        "password_hash": _hasher.hash(password),
    }


def _write_password(path: Path, *, replace_existing: bool) -> str:
    path = path.expanduser().resolve()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    password, payload = _new_credentials()
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.partial")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        if replace_existing:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)
            temporary.unlink()
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)
    return password


def initialize_password(path: Path) -> str:
    """Create credentials without replacing an existing administrator."""
    return _write_password(path, replace_existing=False)


def reset_password(path: Path) -> str:
    """Atomically replace administrator credentials with a generated password."""
    return _write_password(path, replace_existing=True)


def verify_password(path: Path, password: str) -> bool:
    payload = json.loads(path.read_text(encoding="utf-8"))
    try:
        return _hasher.verify(payload["password_hash"], password)
    except VerifyMismatchError:
        return False
