"""Content-addressed blob store under ``data/blobs/``.

Blobs are named by their SHA-256 hex, written via temp file -> fsync -> atomic
``os.replace`` so a reader never sees a partial file. The hex name cannot
contain ``..`` or separators, so path escape is impossible.
"""
from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path


class BlobStore:
    def __init__(self, blob_dir: str | Path):
        self.blob_dir = Path(blob_dir)

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
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        return sha

    def read(self, sha256: str) -> bytes:
        return (self.blob_dir / sha256).read_bytes()

    def contains(self, sha256: str) -> bool:
        return (self.blob_dir / sha256).is_file()
