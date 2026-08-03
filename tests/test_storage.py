"""Storage layer: atomic run/node lifecycle with the Phase 2 durability gate.

The vertical-slice binding is that a node result's business object, node
checkpoint, real outbox row and run event commit in ONE transaction
(``commit_execution_result``), never as a bare event-id list.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from wft.contracts.errors import WFTIdempotencyConflict
from wft.storage.blobs import BlobStore
from wft.storage.db import Database
from wft.storage.schema import SCHEMA_VERSION, migrate
from wft.storage.store import Store


@pytest.fixture()
def store(tmp_path: Path) -> Store:
    return Store(Database(tmp_path / "wft.db"), blob_dir=tmp_path / "blobs")


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _run_spec(run_id: str, *, idempotency_key: str | None = None, param: str = "a") -> dict:
    spec = {
        "run_id": run_id,
        "trigger": {
            "type": "manual",
            "actor": "operator",
            "requested_at": "2026-08-03T10:00:00+00:00",
        },
        "inventory_ref": "config/inventory.example.yaml",
        "selector": {"groups": [], "tags": []},
        "script": {"name": "disk-usage", "sha256": _sha("script"), "risk": "read_only"},
        "limits": {
            "global_concurrency": 50,
            "per_node_concurrency": 1,
            "connect_rate_per_sec": 20,
            "connect_timeout_sec": 10,
            "exec_timeout_sec": 30,
        },
        "config_snapshot_hash": _sha("config"),
    }
    if idempotency_key:
        spec["trigger"]["idempotency_key"] = idempotency_key
    spec["trigger"]["param"] = param  # extra field to distinguish variants
    return spec


def _stream(text: str, *, truncated: bool = False, encoding: str = "utf-8") -> dict:
    return {
        "inline": text,
        "bytes": len(text.encode()),
        "truncated": truncated,
        "sha256": _sha(text),
        "encoding": encoding,
    }


def _result(run_id: str, node_id: str, execution_uid: str, *, status: str = "SUCCEEDED") -> dict:
    payload = {
        "execution_uid": execution_uid,
        "node_id": node_id,
        "script_sha256": _sha("script"),
        "status": status,
        "attempt_count": 1,
        "started_at": "2026-08-03T10:00:01+00:00",
        "finished_at": "2026-08-03T10:00:02+00:00",
        "duration_ms": 1000,
        "exit_code": 0,
        "stdout": _stream("ok"),
        "stderr": _stream(""),
        "flags": [],
    }
    if status == "FAILED":
        payload["error"] = {
            "class": "exec_nonzero",
            "category": "PERMANENT",
            "message": "script exited 1",
            "retryable": False,
        }
    return {
        "meta": {
            "schema_name": "contract-03-execution-result",
            "schema_version": "1.1.0",
            "producer": "wft.execution",
            "created_at": "2026-08-03T10:00:02+00:00",
            "run_id": run_id,
        },
        "payload": payload,
    }


# --------------------------------------------------------------- migration


def test_migrate_is_idempotent(tmp_path: Path) -> None:
    conn = Database(tmp_path / "wft.db").connect()
    migrate(conn)
    migrate(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    conn.close()


# ----------------------------------------------------------- run creation


def test_create_run_and_get(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    run_id, created = store.create_run(_run_spec(rid))
    assert run_id == rid
    assert created is True
    row = store.get_run(rid)
    assert row["status"] == "QUEUED"
    assert row["idempotency_key"] is None


def test_idempotent_same_params_returns_existing(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    key = "idem-key-123456"
    first, created1 = store.create_run(_run_spec(rid, idempotency_key=key))
    second, created2 = store.create_run(_run_spec(rid, idempotency_key=key))
    assert first == second
    assert created1 is True
    assert created2 is False


def test_idempotent_conflict_on_different_params(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    key = "idem-key-123456"
    store.create_run(_run_spec(rid, idempotency_key=key, param="a"))
    with pytest.raises(WFTIdempotencyConflict):
        store.create_run(_run_spec(rid, idempotency_key=key, param="b"))


def test_find_by_idempotency_key(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    key = "idem-key-123456"
    store.create_run(_run_spec(rid, idempotency_key=key))
    assert store.find_run_by_idempotency_key(key)["run_id"] == rid
    assert store.find_run_by_idempotency_key("missing") is None


# ---------------------------------------------------------- run lifecycle


def test_start_run_and_lease(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    assert store.start_run(rid, lease_owner="worker-1") is True
    row = store.get_run(rid)
    assert row["status"] == "RUNNING"
    assert row["lease_owner"] == "worker-1"
    # A different owner cannot take the live lease.
    assert store.start_run(rid, lease_owner="worker-2") is True  # RUNNING re-entrant


def test_renew_lease_lost_returns_false(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.start_run(rid, lease_owner="worker-1")
    assert store.renew_lease(rid, lease_owner="worker-1") is True
    assert store.renew_lease(rid, lease_owner="intruder") is False


def test_acquire_resume_lock_requires_stale_heartbeat(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.start_run(rid, lease_owner="worker-1")
    # Heartbeat is fresh; resume must not succeed.
    assert store.acquire_resume_lock(rid, lease_owner="worker-2") is False


# ------------------------------------------------------- node result commit


def test_commit_execution_result_atomic(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.insert_node_tasks(rid, ["node-a"])
    execution_uid = "0190a2b3-c4d5-46e7-8890-1234567890ab"
    ack = store.commit_execution_result(
        rid,
        "node-a",
        result=_result(rid, "node-a", execution_uid),
        checkpoint_status="SUCCEEDED",
        outbox_event={
            "event_id": "0190a2b3-c4d5-46e7-8890-1234567890ac",
            "object_type": "execution_result",
            "object_id": execution_uid,
            "event_type": "execution_result.completed",
            "payload": {"execution_uid": execution_uid},
        },
        node_event={
            "payload": {
                "event_id": "0190a2b3-c4d5-46e7-8890-1234567890ad",
                "event_type": "node_finished",
                "severity": "info",
                "occurred_at": "2026-08-03T10:00:02+00:00",
                "message": "node-a finished SUCCEEDED",
                "node_id": "node-a",
                "execution_uid": execution_uid,
                "data": {},
            }
        },
    )
    assert ack["object_type"] == "execution_result"
    assert ack["object_id"] == execution_uid
    assert ack["outbox_event_ids"] == ["0190a2b3-c4d5-46e7-8890-1234567890ac"]

    conn = store.database.connect()
    assert conn.execute("SELECT 1 FROM executions WHERE execution_uid=?", (execution_uid,)).fetchone()
    outbox = conn.execute("SELECT status FROM outbox WHERE event_id=?", ("0190a2b3-c4d5-46e7-8890-1234567890ac",)).fetchone()
    assert outbox["status"] == "pending"
    assert conn.execute("SELECT 1 FROM run_events WHERE event_id=?", ("0190a2b3-c4d5-46e7-8890-1234567890ad",)).fetchone()
    task = conn.execute("SELECT status, execution_uid FROM node_tasks WHERE run_id=? AND node_id=?", (rid, "node-a")).fetchone()
    assert task["status"] == "SUCCEEDED"
    assert task["execution_uid"] == execution_uid
    conn.close()


def test_finalize_run(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    ack = store.finalize_run(
        rid,
        run_status="SUCCESS",
        batch_status="success",
        summary={
            "payload": {
                "summary_revision": 1,
                "run_status": "SUCCESS",
                "batch_status": "success",
                "final": True,
                "counts": {
                    "targeted": 1, "succeeded": 1, "failed": 0,
                    "unknown": 0, "cancelled": 0, "skipped": 0,
                },
                "error_counts": {},
                "started_at": "2026-08-03T10:00:01+00:00",
                "finished_at": "2026-08-03T10:00:02+00:00",
                "duration_ms": 1000,
                "exit_code": 0,
            }
        },
        outbox_event={
            "event_id": "0190a2b3-c4d5-46e7-8890-1234567890ac",
            "object_type": "batch_summary",
            "object_id": rid,
            "event_type": "batch_summary.final",
            "payload": {"run_id": rid},
        },
        final_event={
            "payload": {
                "event_id": "0190a2b3-c4d5-46e7-8890-1234567890ad",
                "event_type": "run_completed",
                "severity": "info",
                "occurred_at": "2026-08-03T10:00:02+00:00",
                "message": "run completed SUCCESS",
                "data": {},
            }
        },
    )
    assert ack["object_type"] == "batch_summary"
    assert ack["object_id"] == rid
    row = store.get_run(rid)
    assert row["status"] == "SUCCESS"
    assert row["batch_status"] == "success"
    conn = store.database.connect()
    assert conn.execute("SELECT 1 FROM batch_summaries WHERE run_id=?", (rid,)).fetchone()
    assert conn.execute("SELECT 1 FROM outbox WHERE event_id=?", ("0190a2b3-c4d5-46e7-8890-1234567890ac",)).fetchone()
    conn.close()


# ------------------------------------------------------------------- blobs


def test_blob_write_read_contains(tmp_path: Path) -> None:
    blob = BlobStore(tmp_path / "blobs")
    data = b"hello world" * 1000
    sha = blob.write(data)
    assert len(sha) == 64
    assert blob.contains(sha)
    assert blob.read(sha) == data


def test_blob_write_is_content_addressed(tmp_path: Path) -> None:
    blob = BlobStore(tmp_path / "blobs")
    assert blob.write(b"same bytes") == blob.write(b"same bytes")
    assert len(list((tmp_path / "blobs").iterdir())) == 1


def test_blob_write_atomic_no_partial_on_failure(tmp_path: Path) -> None:
    blob = BlobStore(tmp_path / "blobs")
    sha = blob.write(b"data")
    dest = tmp_path / "blobs" / sha
    # No temp files left behind.
    assert not list(tmp_path.rglob(".blob.*"))
    assert dest.is_file()
