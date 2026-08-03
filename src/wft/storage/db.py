"""SQLite connection factory with the Phase 2 durability settings.

WAL + ``synchronous=FULL`` (kill -9 gate), ``foreign_keys=ON`` and a
``busy_timeout`` so concurrent writers back off instead of failing fast.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from . import schema


class Database:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.path), isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def connect_migrated(self) -> sqlite3.Connection:
        conn = self.connect()
        schema.migrate(conn)
        return conn
