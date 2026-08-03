"""Content-addressed blob store under ``data/blobs/``.

Blobs are named by their SHA-256 hex, written via temp file -> fsync -> atomic
``os.replace`` so a reader never sees a partial file. Every public lookup
validates the reference is a 64-hex name, so a caller-supplied ``sha256`` can
never escape ``blob_dir`` via ``..`` or separators.
"""
from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path

_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


class BlobStore:
    def __init__(self, blob_dir: str | Path):
        self.blob_dir = Path(blob_dir)

    def _dest(self, sha256: str) -> Path:
        if not _HEX_RE.match(sha256):
            raise ValueError(f"invalid blob sha256: {sha256!r}")
        return self.blob_dir / sha256

    def write(self, data: bytes) -> str:
        sha = hashlib.sha256(data).hexdigest()
        dest = self.blob_dir / sha
        if dest.is_file() and dest.stat().st_size == len(data):
            return sha
        self.blob_dir.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.blob_dir, prefix=".blob.")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, dest)
            _fsync_dir(self.blob_dir)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        return sha

    def read(self, sha256: str) -> bytes:
        return self._dest(sha256).read_bytes()

    def contains(self, sha256: str) -> bool:
        return self._dest(sha256).is_file()

    def list(self) -> list[str]:
        """Return the stored content-addressed blob names (orphans included).

        Temp files, any non-hex leftovers and hex-named directories are
        excluded, so the result is exactly the set of completed blob FILES that
        ``find_orphan_blobs`` compares against the DB's references.
        """
        if not self.blob_dir.is_dir():
            return []
        return sorted(
            p.name for p in self.blob_dir.iterdir()
            if p.is_file() and _HEX_RE.match(p.name)
        )


def _fsync_dir(path: Path) -> None:
    """Best-effort directory fsync so the rename is durable (not all filesystems)."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)
