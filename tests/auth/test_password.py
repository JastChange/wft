import json
from pathlib import Path

import pytest

from wft.auth.password import initialize_password, reset_password, verify_password


def test_initializes_hash_without_plaintext(tmp_path: Path) -> None:
    path = tmp_path / "auth" / "admin.json"

    generated = initialize_password(path)

    raw = path.read_text()
    saved = json.loads(raw)
    assert generated not in raw
    assert saved["schema_version"] == "1.0"
    assert saved["username"] == "admin"
    assert saved["password_hash"].startswith("$argon2")
    assert verify_password(path, generated) is True
    assert verify_password(path, "wrong-password") is False
    assert path.stat().st_mode & 0o777 == 0o600
    assert not list(path.parent.glob("*.partial"))


def test_initialize_refuses_to_replace_existing_credentials(tmp_path: Path) -> None:
    path = tmp_path / "admin.json"
    original = initialize_password(path)

    with pytest.raises(FileExistsError):
        initialize_password(path)

    assert verify_password(path, original) is True


def test_reset_replaces_password_atomically(tmp_path: Path) -> None:
    path = tmp_path / "admin.json"
    original = initialize_password(path)

    replacement = reset_password(path)

    assert replacement != original
    assert verify_password(path, original) is False
    assert verify_password(path, replacement) is True
    assert path.stat().st_mode & 0o777 == 0o600
