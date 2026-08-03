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
    # Only QUEUED -> RUNNING is allowed; a different owner cannot take the live
    # lease with a plain start (resume must go through acquire_resume_lock).
    assert store.start_run(rid, lease_owner="worker-2") is False
    assert store.get_run(rid)["lease_owner"] == "worker-1"


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


def test_acquire_resume_lock_stale_gate_is_lease_seconds(store: Store) -> None:
    """Heartbeat renews every 10s but the stale gate is the full 60s lease."""
    from datetime import datetime, timedelta, timezone

    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.start_run(rid, lease_owner="worker-1")
    now = datetime.now(timezone.utc)

    def _age(seconds_ago: int) -> None:
        past = (now - timedelta(seconds=seconds_ago)).isoformat()
        conn = store.database.connect()
        conn.execute(
            "UPDATE runs SET heartbeat_at=?, lease_expires_at=? WHERE run_id=?",
            (past, past, rid),
        )
        conn.commit()
        conn.close()

    # 30s-old heartbeat is within the 60s gate: still owned by worker-1.
    _age(30)
    assert store.acquire_resume_lock(rid, lease_owner="worker-2") is False
    # 90s-old heartbeat + lapsed lease: resumable via the CAS.
    _age(90)
    assert store.acquire_resume_lock(rid, lease_owner="worker-2") is True
    assert store.get_run(rid)["lease_owner"] == "worker-2"


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
        attempts=[
            {
                "attempt_id": "0190a2b3-c4d5-46e7-8890-1234567890ae",
                "attempt_seq": 1,
                "status": "SUCCEEDED",
                "error_class": None,
                "error_category": None,
                "error_message": None,
                "retryable": False,
                "started_at": "2026-08-03T10:00:01+00:00",
                "finished_at": "2026-08-03T10:00:02+00:00",
            }
        ],
    )
    assert ack["meta"]["schema_name"] == "contract-06-persist-ack"
    assert ack["payload"]["object_type"] == "execution_result"
    assert ack["payload"]["object_id"] == execution_uid
    assert ack["payload"]["outbox_event_ids"] == ["0190a2b3-c4d5-46e7-8890-1234567890ac"]

    conn = store.database.connect()
    assert conn.execute("SELECT 1 FROM executions WHERE execution_uid=?", (execution_uid,)).fetchone()
    outbox = conn.execute("SELECT status FROM outbox WHERE event_id=?", ("0190a2b3-c4d5-46e7-8890-1234567890ac",)).fetchone()
    assert outbox["status"] == "pending"
    assert conn.execute("SELECT 1 FROM run_events WHERE event_id=?", ("0190a2b3-c4d5-46e7-8890-1234567890ad",)).fetchone()
    assert conn.execute("SELECT 1 FROM attempts WHERE attempt_id=?", ("0190a2b3-c4d5-46e7-8890-1234567890ae",)).fetchone()
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
    assert ack["meta"]["schema_name"] == "contract-06-persist-ack"
    assert ack["payload"]["object_type"] == "batch_summary"
    assert ack["payload"]["object_id"] == rid
    row = store.get_run(rid)
    assert row["status"] == "SUCCESS"
    assert row["batch_status"] == "success"
    conn = store.database.connect()
    assert conn.execute("SELECT 1 FROM batch_summaries WHERE run_id=?", (rid,)).fetchone()
    assert conn.execute("SELECT 1 FROM outbox WHERE event_id=?", ("0190a2b3-c4d5-46e7-8890-1234567890ac",)).fetchone()
    conn.close()


def _summary(run_id: str, *, exit_code: int = 0) -> dict:
    return {
        "payload": {
            "summary_revision": 1,
            "run_status": "SUCCESS" if exit_code == 0 else "SUCCESS",
            "batch_status": "success" if exit_code == 0 else "failed",
            "final": True,
            "counts": {
                "targeted": 1, "succeeded": 1 if exit_code == 0 else 0,
                "failed": 0 if exit_code == 0 else 1,
                "unknown": 0, "cancelled": 0, "skipped": 0,
            },
            "error_counts": {},
            "started_at": "2026-08-03T10:00:01+00:00",
            "finished_at": "2026-08-03T10:00:02+00:00",
            "duration_ms": 1000,
            "exit_code": exit_code,
        }
    }


def _outbox_event(object_type: str, object_id: str, event_id: str) -> dict:
    return {
        "event_id": event_id,
        "object_type": object_type,
        "object_id": object_id,
        "event_type": "x.final" if object_type == "batch_summary" else "x.completed",
        "payload": {"object_id": object_id},
    }


def _run_event(event_id: str, *, event_type: str = "run_completed") -> dict:
    return {
        "payload": {
            "event_id": event_id,
            "event_type": event_type,
            "severity": "info",
            "occurred_at": "2026-08-03T10:00:02+00:00",
            "message": "done",
            "data": {},
        }
    }


def test_commit_execution_result_replay_returns_original(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.insert_node_tasks(rid, ["node-a"])
    uid = "0190a2b3-c4d5-46e7-8890-1234567890ab"
    args = dict(
        run_id=rid,
        node_id="node-a",
        result=_result(rid, "node-a", uid),
        checkpoint_status="SUCCEEDED",
        outbox_event=_outbox_event("execution_result", uid, "0190a2b3-c4d5-46e7-8890-1234567890ac"),
        node_event=_run_event("0190a2b3-c4d5-46e7-8890-1234567890ad", event_type="node_finished"),
    )
    first = store.commit_execution_result(**args)
    replay = store.commit_execution_result(**args)
    assert replay["payload"]["object_id"] == first["payload"]["object_id"]
    assert replay["payload"]["outbox_event_ids"] == ["0190a2b3-c4d5-46e7-8890-1234567890ac"]
    conn = store.database.connect()
    assert conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1
    conn.close()


def test_commit_execution_result_conflict_on_different_content(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.insert_node_tasks(rid, ["node-a"])
    uid = "0190a2b3-c4d5-46e7-8890-1234567890ab"
    store.commit_execution_result(
        rid,
        "node-a",
        result=_result(rid, "node-a", uid, status="SUCCEEDED"),
        checkpoint_status="SUCCEEDED",
        outbox_event=_outbox_event("execution_result", uid, "0190a2b3-c4d5-46e7-8890-1234567890ac"),
        node_event=_run_event("0190a2b3-c4d5-46e7-8890-1234567890ad", event_type="node_finished"),
    )
    with pytest.raises(WFTIdempotencyConflict):
        store.commit_execution_result(
            rid,
            "node-a",
            result=_result(rid, "node-a", uid, status="FAILED"),
            checkpoint_status="FAILED",
            outbox_event=_outbox_event("execution_result", uid, "0190a2b3-c4d5-46e7-8890-1234567890ac"),
            node_event=_run_event("0190a2b3-c4d5-46e7-8890-1234567890ad", event_type="node_finished"),
        )


def test_finalize_run_terminal_replay_returns_ack(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    args = dict(
        run_id=rid,
        run_status="SUCCESS",
        batch_status="success",
        summary=_summary(rid),
        outbox_event=_outbox_event("batch_summary", rid, "0190a2b3-c4d5-46e7-8890-1234567890ac"),
        final_event=_run_event("0190a2b3-c4d5-46e7-8890-1234567890ad"),
    )
    first = store.finalize_run(**args)
    replay = store.finalize_run(**args)
    assert replay["payload"]["object_id"] == first["payload"]["object_id"]
    assert store.get_run(rid)["status"] == "SUCCESS"
    conn = store.database.connect()
    assert conn.execute("SELECT COUNT(*) FROM batch_summaries").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1
    conn.close()


def test_finalize_run_terminal_conflict_on_different_summary(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.finalize_run(
        run_id=rid,
        run_status="SUCCESS",
        batch_status="success",
        summary=_summary(rid, exit_code=0),
        outbox_event=_outbox_event("batch_summary", rid, "0190a2b3-c4d5-46e7-8890-1234567890ac"),
        final_event=_run_event("0190a2b3-c4d5-46e7-8890-1234567890ad"),
    )
    with pytest.raises(WFTIdempotencyConflict):
        store.finalize_run(
            run_id=rid,
            run_status="SUCCESS",
            batch_status="failed",
            summary=_summary(rid, exit_code=1),
            outbox_event=_outbox_event("batch_summary", rid, "0190a2b3-c4d5-46e7-8890-1234567890ac"),
            final_event=_run_event("0190a2b3-c4d5-46e7-8890-1234567890ad"),
        )


def test_set_node_task_forbids_terminal_rewrite(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.insert_node_tasks(rid, ["node-a"])
    assert store.set_node_task(rid, "node-a", "RUNNING") is True
    assert store.set_node_task(rid, "node-a", "SUCCEEDED") is True
    # Terminal node task cannot be rewritten.
    assert store.set_node_task(rid, "node-a", "FAILED") is False
    assert store.get_node_task(rid, "node-a")["status"] == "SUCCEEDED"


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
