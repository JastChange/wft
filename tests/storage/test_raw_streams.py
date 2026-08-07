import errno
import hashlib
from pathlib import Path

import pytest

from wft.storage.raw_streams import RawWriter, StorageFullError, storage_write_probe


def test_raw_writer_preserves_non_utf8_bytes_and_builds_reference(tmp_path: Path) -> None:
    writer = RawWriter(tmp_path / "stdout.raw")

    writer.write(b"prefix\xff")
    writer.write(b"suffix")
    reference = writer.finish()

    expected = b"prefix\xffsuffix"
    assert (tmp_path / "stdout.raw").read_bytes() == expected
    assert reference.path == "stdout.raw"
    assert reference.size_bytes == len(expected)
    assert reference.sha256 == hashlib.sha256(expected).hexdigest()
    assert (tmp_path / "stdout.raw").stat().st_mode & 0o777 == 0o600
    assert not list(tmp_path.glob("*.partial"))


def test_abort_closes_and_removes_partial_file(tmp_path: Path) -> None:
    writer = RawWriter(tmp_path / "stdout.raw")
    writer.write(b"discard me")

    writer.abort()

    assert not (tmp_path / "stdout.raw").exists()
    assert not list(tmp_path.iterdir())
    with pytest.raises(ValueError, match="closed"):
        writer.write(b"too late")


def test_storage_probe_removes_probe_file(tmp_path: Path) -> None:
    storage_write_probe(tmp_path)

    assert list(tmp_path.iterdir()) == []


def test_storage_probe_maps_disk_full(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def raise_disk_full(_fd: int, _data: bytes) -> int:
        raise OSError(errno.ENOSPC, "disk full")

    monkeypatch.setattr("wft.storage.raw_streams.os.write", raise_disk_full)

    with pytest.raises(StorageFullError):
        storage_write_probe(tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_raw_writer_and_probe_map_disk_full_during_open(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def raise_disk_full(*_args: object) -> int:
        raise OSError(errno.ENOSPC, "disk full")

    monkeypatch.setattr("wft.storage.raw_streams.os.open", raise_disk_full)

    with pytest.raises(StorageFullError):
        RawWriter(tmp_path / "stdout.raw")
    with pytest.raises(StorageFullError):
        storage_write_probe(tmp_path)
