"""SQLite schema and migrations.

``PRAGMA user_version`` is the single migration-version source (per Phase 2
binding: no separate ``meta.schema_version`` row). ``synchronous=FULL`` is set
uniformly for the kill -9 durability gate; WAL, ``foreign_keys=ON`` and a
``busy_timeout`` are configured in :class:`wft.storage.db.Database`.
"""
from __future__ import annotations

import sqlite3

SCHEMA_VERSION = 1

_DDL_V1 = """
CREATE TABLE runs (
    run_id            TEXT PRIMARY KEY,
    run_spec_json     TEXT NOT NULL,
    status            TEXT NOT NULL,
    batch_status      TEXT,
    resume_count      INTEGER NOT NULL DEFAULT 0,
    heartbeat_at      TEXT NOT NULL,
    lease_owner       TEXT,
    lease_expires_at  TEXT,
    idempotency_key   TEXT,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    started_at        TEXT,
    finished_at       TEXT
);

CREATE UNIQUE INDEX idx_runs_idempotency
    ON runs(idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE node_tasks (
    run_id          TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    node_id         TEXT NOT NULL,
    status          TEXT NOT NULL,
    execution_uid   TEXT,
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    error_class     TEXT,
    started_at      TEXT,
    finished_at     TEXT,
    updated_at      TEXT,
    PRIMARY KEY (run_id, node_id)
);

CREATE TABLE executions (
    run_id           TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    node_id          TEXT NOT NULL,
    execution_uid    TEXT PRIMARY KEY,
    script_sha256    TEXT NOT NULL,
    status           TEXT NOT NULL,
    attempt_count    INTEGER NOT NULL,
    started_at       TEXT NOT NULL,
    finished_at      TEXT NOT NULL,
    duration_ms      INTEGER NOT NULL,
    exit_code        INTEGER,
    stdout_json      TEXT NOT NULL,
    stderr_json      TEXT NOT NULL,
    error_json       TEXT,
    result_json      TEXT NOT NULL,
    created_at       TEXT NOT NULL
);

CREATE INDEX idx_executions_run ON executions(run_id);

CREATE TABLE attempts (
    execution_uid   TEXT NOT NULL,
    attempt_id      TEXT NOT NULL,
    attempt_seq     INTEGER NOT NULL,
    status          TEXT NOT NULL,
    error_class     TEXT,
    error_category  TEXT,
    error_message   TEXT,
    retryable       INTEGER NOT NULL DEFAULT 0,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    PRIMARY KEY (execution_uid, attempt_id)
);

CREATE TABLE run_events (
    run_id         TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    event_id       TEXT PRIMARY KEY,
    event_type     TEXT NOT NULL,
    severity       TEXT NOT NULL,
    occurred_at    TEXT NOT NULL,
    message        TEXT NOT NULL,
    node_id        TEXT,
    execution_uid  TEXT,
    data_json      TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX idx_run_events_run ON run_events(run_id);

CREATE TABLE batch_summaries (
    run_id             TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
    summary_revision   INTEGER NOT NULL,
    summary_json       TEXT NOT NULL,
    final              INTEGER NOT NULL DEFAULT 0,
    created_at         TEXT NOT NULL
);

CREATE TABLE outbox (
    event_id      TEXT PRIMARY KEY,
    object_type   TEXT NOT NULL,
    object_id     TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    payload_json  TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    attempts      INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL
);

CREATE INDEX idx_outbox_status ON outbox(status);
"""


def migrate(conn: sqlite3.Connection) -> None:
    """Bring the schema up to ``SCHEMA_VERSION`` via ``PRAGMA user_version``."""
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version < 1:
        conn.executescript(_DDL_V1)
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
