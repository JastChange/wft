"""Group E: stale resume / UNKNOWN / cumulative attempts across a crash boundary.

Proves the Contract-02..05 resume path: ``execute_run(resume=True)`` claims a
stale lease and flips leftover RUNNING checkpoints to UNKNOWN first, dispatches
only PENDING/UNKNOWN nodes (terminal nodes never re-run), reuses the persisted
``execution_uid``, continues ``attempt_seq`` from ``max(attempt_seq)+1`` so
Contract-03 ``attempt_count`` is cumulative across the crash boundary (never a
reset to zero, never a duplicated attempt_id, never a 4th SSH attempt when the
cap of 3 is consumed), and finalizes a BatchSummary that counts every original
node (committed terminal + newly dispatched).

The ``_resume_disposition`` of a resumed node's persisted attempts decides the
path: a completed non-retryable/budget-exhausted attempt reconstructs a
terminal FAILED result (no further SSH); an interrupted attempt whose outcome
is unknown only re-runs when the cap allows; an interrupted seq-3 attempt with
no outcome stays UNKNOWN and leaves the Run RUNNING (exit 2) with the reason
audited -- never a fabricated Contract-03 result.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from wft.execution.errors import error_dict
from wft.execution.ssh import ExecutionOutcome
from wft.idgen import new_run_id, new_uuid7
from wft.orchestration.events import build_event, build_outbox_event, now_iso
from wft.scriptreg.registry import Script
from wft.storage.db import Database
from wft.storage.store import Store

import wft.orchestration.run as run_mod
from wft.execution.result import build_execution_result
from wft.orchestration.run import build_run_spec, create_run, execute_run


def _sha_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _scratch(tmp_path: Path) -> Path:
    p = tmp_path / "ok.sh"
    p.write_text("#!/bin/bash\necho ok\n", encoding="utf-8")
    return p


def _script(path: Path) -> Script:
    return Script(
        name="disk-usage",
        path=path,
        sha256=_sha_file(path),
        risk="read_only",
        shell="bash",
        timeout_sec=30,
        enabled=True,
        expected_exit_codes=(0,),
    )


def _node(node_id: str, key_path: Path) -> dict:
    return {
        "node_id": node_id,
        "host": "127.0.0.1",
        "port": 22,
        "username": "tester",
        "auth": {"method": "key", "credential_ref": f"file://{key_path}"},
        "groups": ["web"],
        "tags": [],
        "enabled": True,
    }


def _make_store(tmp_path: Path) -> Store:
    return Store(Database(tmp_path / "wft.db"))


def _make_run_spec(script: Script) -> dict:
    return build_run_spec(
        run_id=new_run_id(),
        trigger_type="manual",
        actor="tester",
        requested_at=now_iso(),
        inventory_ref="config/inventory.example.yaml",
        selector={"groups": ["web"], "tags": []},
        script=script,
        limits={
            "global_concurrency": 4,
            "per_node_concurrency": 1,
            "connect_rate_per_sec": 200,
            "connect_timeout_sec": 5,
            "exec_timeout_sec": 30,
        },
        config_snapshot_hash="b" * 64,
    )


def _make_stale(store: Store, run_id: str, seconds_ago: int = 90) -> None:
    """Age the heartbeat/lease so the run crosses the stale resume gate."""
    past = (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).isoformat()
    with store.database.connect() as conn:
        conn.execute(
            "UPDATE runs SET heartbeat_at=?, lease_expires_at=? WHERE run_id=?",
            (past, past, run_id),
        )
        conn.commit()


def _start_owned_run(store: Store, spec: dict, node_ids: list[str]) -> str:
    run_id, created = create_run(store, spec, node_ids)
    assert created
    assert store.start_run(run_id, lease_owner="owner-1")
    return run_id


def _commit_terminal(
    store: Store,
    run_id: str,
    node: dict,
    script: Script,
    uid: str,
    *,
    status: str,
    exit_code: int | None,
    error: dict | None,
) -> None:
    """Commit a finished node exactly as the orchestration flow would."""
    result, _degraded, secondary = build_execution_result(
        run_id=run_id,
        execution_uid=uid,
        node_id=node["node_id"],
        script=script,
        status=status,
        attempt_count=1,
        started_at=now_iso(),
        finished_at=now_iso(),
        duration_ms=1,
        exit_code=exit_code,
        stdout_bytes=b"",
        stderr_bytes=b"",
        error=error,
        blobs=store.blobs,
    )
    payload = result["payload"]
    store.commit_execution_result(
        run_id,
        node["node_id"],
        result=result,
        checkpoint_status=status,
        outbox_event=build_outbox_event(
            object_type="execution_result",
            object_id=uid,
            event_type="execution_result.completed",
            payload=payload,
        ),
        node_event=build_event(
            run_id,
            "node_finished",
            f"node {node['node_id']} {status.lower()}",
            node_id=node["node_id"],
            execution_uid=uid,
            data={
                "status": status,
                "secondary_errors": [dict(e) for e in secondary],
            },
        ),
        attempts=[],
        lease_owner="owner-1",
    )


def _record_attempts(
    store: Store, run_id: str, node_id: str, uid: str, *, finals: list[tuple[int, str]]
) -> None:
    """Durably persist final attempt rows for a crashed first execution."""
    for seq, error_class in finals:
        store.record_attempt(
            run_id,
            node_id,
            uid,
            attempt_id=new_uuid7(),
            attempt_seq=seq,
            status="FAILED",
            started_at=now_iso(),
            finished_at=now_iso(),
            error=error_dict(error_class, f"attempt {seq} failed"),
            lease_owner="owner-1",
        )


# ------------------------------------------------------------ dispatch rules


def test_resume_dispatches_only_pending_and_unknown(tmp_path: Path, monkeypatch) -> None:
    """Leftover RUNNING -> UNKNOWN re-runs; PENDING runs; terminal never re-runs."""
    script = _script(_scratch(tmp_path))
    nodes = [_node("node-a", tmp_path / "key"), _node("node-b", tmp_path / "key"),
             _node("node-c", tmp_path / "key")]
    store = _make_store(tmp_path)
    spec = _make_run_spec(script)
    run_id = _start_owned_run(store, spec, [n["node_id"] for n in nodes])

    # Crashed mid-run: node-a RUNNING (leftover), node-b PENDING (never
    # dispatched), node-c already committed SUCCEEDED (terminal).
    uid_a = new_uuid7()
    assert store.set_node_task(run_id, "node-a", "RUNNING", execution_uid=uid_a,
                               lease_owner="owner-1")
    _commit_terminal(store, run_id, nodes[2], script, new_uuid7(),
                     status="SUCCEEDED", exit_code=0, error=None)
    _make_stale(store, run_id)

    called: list[str] = []

    async def _ok(**kwargs):
        called.append(kwargs["node"]["node_id"])
        return ExecutionOutcome(exit_code=0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setattr(run_mod, "execute_script", _ok)
    outcome = asyncio.run(
        execute_run(
            store, run_id=run_id, run_spec=spec, nodes=nodes, script=script,
            known_hosts_path=None, resume=True,
        )
    )
    assert sorted(called) == ["node-a", "node-b"]  # terminal node-c not re-run
    assert outcome.run_status == "SUCCESS"
    assert outcome.batch_status == "success"
    assert outcome.counts["targeted"] == 3
    assert outcome.counts["succeeded"] == 3
    assert outcome.counts["failed"] == 0
    tasks = {t["node_id"]: t for t in store.get_node_tasks(run_id)}
    assert tasks["node-c"]["status"] == "SUCCEEDED"  # untouched
    assert tasks["node-a"]["status"] == "SUCCEEDED"
    assert tasks["node-a"]["execution_uid"] == uid_a  # uid preserved on re-dispatch
    assert store.get_run(run_id)["status"] == "SUCCESS"


def test_resume_preserves_pending_and_recovered_audit_chain(
    tmp_path: Path, monkeypatch
) -> None:
    """Recovery marks the leftover node UNKNOWN (with an event) while PENDING stays."""
    script = _script(_scratch(tmp_path))
    nodes = [_node("node-a", tmp_path / "key"), _node("node-b", tmp_path / "key")]
    store = _make_store(tmp_path)
    spec = _make_run_spec(script)
    run_id = _start_owned_run(store, spec, [n["node_id"] for n in nodes])
    uid_a = new_uuid7()
    assert store.set_node_task(run_id, "node-a", "RUNNING", execution_uid=uid_a,
                               lease_owner="owner-1")
    _make_stale(store, run_id)

    async def _ok(**kwargs):
        return ExecutionOutcome(exit_code=0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setattr(run_mod, "execute_script", _ok)
    asyncio.run(
        execute_run(
            store, run_id=run_id, run_spec=spec, nodes=nodes, script=script,
            known_hosts_path=None, resume=True,
        )
    )
    with store.database.connect() as conn:
        events = [
            dict(r) for r in conn.execute(
                "SELECT event_type, node_id, data_json FROM run_events "
                "WHERE run_id=? ORDER BY occurred_at", (run_id,)
            ).fetchall()
        ]
    # run-level resume audit + per-node checkpoint_updated for the recovered node.
    resumed = [e for e in events if e["event_type"] == "checkpoint_updated"]
    assert len(resumed) == 2
    node_unknown = [e for e in resumed if e["node_id"] == "node-a"]
    assert len(node_unknown) == 1
    assert json.loads(node_unknown[0]["data_json"])["status"] == "UNKNOWN"


# ------------------------------------------------ cumulative attempts / uid


def test_resume_reuses_uid_and_continues_attempts_from_max_plus_one(
    tmp_path: Path, monkeypatch
) -> None:
    """A crashed node with 2 persisted attempts resumes at attempt 3, same uid."""
    script = _script(_scratch(tmp_path))
    nodes = [_node("node-a", tmp_path / "key")]
    store = _make_store(tmp_path)
    spec = _make_run_spec(script)
    run_id = _start_owned_run(store, spec, ["node-a"])
    uid = new_uuid7()
    assert store.set_node_task(run_id, "node-a", "RUNNING", execution_uid=uid,
                               lease_owner="owner-1")
    _record_attempts(store, run_id, "node-a", uid,
                     finals=[(1, "conn_timeout"), (2, "conn_timeout")])
    _make_stale(store, run_id)

    calls: list[int] = []

    async def _ok(**kwargs):
        calls.append(kwargs["node"]["node_id"])
        return ExecutionOutcome(exit_code=0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setattr(run_mod, "execute_script", _ok)
    outcome = asyncio.run(
        execute_run(
            store, run_id=run_id, run_spec=spec, nodes=nodes, script=script,
            known_hosts_path=None, resume=True,
        )
    )
    assert len(calls) == 1  # only the 3rd (final) SSH attempt ran
    assert outcome.counts["succeeded"] == 1
    with store.database.connect() as conn:
        row = conn.execute(
            "SELECT attempt_count, execution_uid, status FROM executions "
            "WHERE run_id=?", (run_id,)
        ).fetchone()
        assert row["execution_uid"] == uid  # uid unchanged across the crash
        assert row["attempt_count"] == 3  # cumulative, never reset to zero
        assert row["status"] == "SUCCEEDED"
        seqs = [
            dict(a)["attempt_seq"]
            for a in conn.execute(
                "SELECT attempt_seq FROM attempts WHERE execution_uid=? "
                "ORDER BY attempt_seq", (uid,)
            ).fetchall()
        ]
        assert seqs == [1, 2, 3]
        ids = [
            dict(a)["attempt_id"]
            for a in conn.execute(
                "SELECT attempt_id FROM attempts WHERE execution_uid=?", (uid,)
            ).fetchall()
        ]
        assert len(ids) == len(set(ids))  # no duplicated attempt_id


def test_resume_attempt_cap_forbids_fourth_ssh_attempt(tmp_path: Path, monkeypatch) -> None:
    """3 persisted attempts exhaust the Contract-03 cap: resume reconstructs FAILED."""
    script = _script(_scratch(tmp_path))
    nodes = [_node("node-a", tmp_path / "key")]
    store = _make_store(tmp_path)
    spec = _make_run_spec(script)
    run_id = _start_owned_run(store, spec, ["node-a"])
    uid = new_uuid7()
    assert store.set_node_task(run_id, "node-a", "RUNNING", execution_uid=uid,
                               lease_owner="owner-1")
    _record_attempts(store, run_id, "node-a", uid,
                     finals=[(1, "conn_timeout"), (2, "conn_timeout"), (3, "conn_timeout")])
    _make_stale(store, run_id)

    calls: list[int] = []

    async def _never(**kwargs):  # must never be reached
        calls.append(1)
        return ExecutionOutcome(exit_code=0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setattr(run_mod, "execute_script", _never)
    outcome = asyncio.run(
        execute_run(
            store, run_id=run_id, run_spec=spec, nodes=nodes, script=script,
            known_hosts_path=None, resume=True,
        )
    )
    assert calls == []  # no 4th SSH attempt
    assert outcome.counts["failed"] == 1
    assert outcome.exit_code == 1
    with store.database.connect() as conn:
        row = conn.execute(
            "SELECT attempt_count, execution_uid, status, error_json "
            "FROM executions WHERE run_id=?", (run_id,)
        ).fetchone()
        assert row["execution_uid"] == uid
        assert row["attempt_count"] == 3  # the cumulative cap, never 4
        assert row["status"] == "FAILED"
        error = json.loads(row["error_json"])
        assert error["class"] == "conn_timeout"  # last persisted attempt's error
        assert (
            conn.execute("SELECT COUNT(*) FROM attempts WHERE execution_uid=?",
                         (uid,)).fetchone()[0] == 3
        )


def test_resume_non_retryable_attempt_reconstructs_failed_no_ssh(
    tmp_path: Path, monkeypatch
) -> None:
    """A completed PERMANENT failure consumes its single attempt: resume
    reconstructs FAILED without a second SSH attempt (error matrix respected)."""
    script = _script(_scratch(tmp_path))
    nodes = [_node("node-a", tmp_path / "key")]
    store = _make_store(tmp_path)
    spec = _make_run_spec(script)
    run_id = _start_owned_run(store, spec, ["node-a"])
    uid = new_uuid7()
    assert store.set_node_task(run_id, "node-a", "RUNNING", execution_uid=uid,
                               lease_owner="owner-1")
    _record_attempts(store, run_id, "node-a", uid, finals=[(1, "exec_nonzero")])
    _make_stale(store, run_id)

    calls: list[int] = []

    async def _never(**kwargs):  # must never be reached
        calls.append(1)
        return ExecutionOutcome(exit_code=0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setattr(run_mod, "execute_script", _never)
    outcome = asyncio.run(
        execute_run(
            store, run_id=run_id, run_spec=spec, nodes=nodes, script=script,
            known_hosts_path=None, resume=True,
        )
    )
    assert calls == []  # PERMANENT failure: no 2nd SSH attempt
    assert outcome.counts["failed"] == 1
    assert outcome.exit_code == 1
    with store.database.connect_migrated() as conn:
        row = conn.execute(
            "SELECT status, attempt_count, error_json FROM executions WHERE run_id=?",
            (run_id,),
        ).fetchone()
        assert row["status"] == "FAILED"
        assert row["attempt_count"] == 1
        assert json.loads(row["error_json"])["class"] == "exec_nonzero"


def test_resume_seq3_interrupted_node_stays_unknown_run_running_exit2(
    tmp_path: Path, monkeypatch
) -> None:
    """A seq-3 attempt left RUNNING by a crash is indeterminate: no 4th SSH, no
    fabricated Contract-03 result -- the node stays UNKNOWN, the Run stays
    RUNNING, exit 2, and the reason is audited."""
    script = _script(_scratch(tmp_path))
    nodes = [_node("node-a", tmp_path / "key")]
    store = _make_store(tmp_path)
    spec = _make_run_spec(script)
    run_id = _start_owned_run(store, spec, ["node-a"])
    uid = new_uuid7()
    assert store.set_node_task(run_id, "node-a", "RUNNING", execution_uid=uid,
                               lease_owner="owner-1")
    _record_attempts(store, run_id, "node-a", uid,
                     finals=[(1, "conn_timeout"), (2, "conn_timeout")])
    # The 3rd attempt started but never finished (outcome unknown, no error).
    store.record_attempt(
        run_id, "node-a", uid,
        attempt_id=new_uuid7(), attempt_seq=3, status="RUNNING",
        started_at=now_iso(), lease_owner="owner-1",
    )
    _make_stale(store, run_id)

    calls: list[int] = []

    async def _never(**kwargs):  # must never be reached
        calls.append(1)
        return ExecutionOutcome(exit_code=0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setattr(run_mod, "execute_script", _never)
    outcome = asyncio.run(
        execute_run(
            store, run_id=run_id, run_spec=spec, nodes=nodes, script=script,
            known_hosts_path=None, resume=True,
        )
    )
    assert calls == []  # no 4th SSH attempt
    assert outcome.exit_code == 2
    assert outcome.run_status == "RUNNING"
    assert outcome.batch_status is None
    assert outcome.summary == {}
    assert outcome.counts == {"targeted": 1, "succeeded": 0, "failed": 0,
                              "unknown": 1, "cancelled": 0, "skipped": 0}
    assert store.get_node_task(run_id, "node-a")["status"] == "UNKNOWN"
    assert store.get_run(run_id)["status"] == "RUNNING"
    with store.database.connect_migrated() as conn:
        # No Contract-03 result may be fabricated for an indeterminate attempt.
        assert (
            conn.execute("SELECT COUNT(*) FROM executions WHERE run_id=?",
                         (run_id,)).fetchone()[0] == 0
        )
        events = [
            dict(r) for r in conn.execute(
                "SELECT event_type, node_id, data_json FROM run_events "
                "WHERE run_id=? ORDER BY occurred_at", (run_id,)
            ).fetchall()
        ]
        audits = [
            e for e in events
            if e["event_type"] == "checkpoint_updated" and e["node_id"] == "node-a"
        ]
        assert len(audits) >= 1
        assert any(
            json.loads(e["data_json"]).get("reason")
            == "attempt_cap_consumed_no_outcome"
            for e in audits
        )


def test_resume_duration_and_retried_flag_across_crash_boundary(
    tmp_path: Path, monkeypatch
) -> None:
    """Resumed duration_ms spans the original first attempt (frozen 90s), and
    ``retried`` reflects the cumulative attempt_count, not this process's."""
    script = _script(_scratch(tmp_path))
    nodes = [_node("node-a", tmp_path / "key")]
    store = _make_store(tmp_path)
    spec = _make_run_spec(script)
    run_id = _start_owned_run(store, spec, ["node-a"])
    uid = new_uuid7()
    assert store.set_node_task(run_id, "node-a", "RUNNING", execution_uid=uid,
                               lease_owner="owner-1")
    # The first attempt failed retryably at T0; the resume re-runs 90s later.
    t90 = now_iso()
    t0 = (datetime.fromisoformat(t90) - timedelta(seconds=90)).isoformat()
    store.record_attempt(
        run_id, "node-a", uid,
        attempt_id=new_uuid7(), attempt_seq=1, status="FAILED",
        started_at=t0, finished_at=t0,
        error=error_dict("conn_timeout", "attempt 1 timed out"),
        lease_owner="owner-1",
    )
    _make_stale(store, run_id)

    # Freeze the clock: the resumed attempt runs at T0+90s, not wall-clock-now.
    monkeypatch.setattr(run_mod, "now_iso", lambda: t90)

    async def _ok(**kwargs):
        return ExecutionOutcome(exit_code=0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setattr(run_mod, "execute_script", _ok)
    outcome = asyncio.run(
        execute_run(
            store, run_id=run_id, run_spec=spec, nodes=nodes, script=script,
            known_hosts_path=None, resume=True,
        )
    )
    assert outcome.counts["succeeded"] == 1
    with store.database.connect_migrated() as conn:
        row = conn.execute(
            "SELECT result_json FROM executions WHERE run_id=?", (run_id,)
        ).fetchone()
        result = json.loads(row["result_json"])["payload"]
        assert result["status"] == "SUCCEEDED"
        assert result["attempt_count"] == 2
        assert result["started_at"] == t0  # spans the original first attempt
        assert abs(result["duration_ms"] - 90000) <= 5  # ~90s, not ~0s loop time
        assert "resumed" in result["flags"]
        assert "retried" in result["flags"]  # cumulative attempt_count > 1


def test_resume_zero_dispatch_finalizes_all_terminal(tmp_path: Path, monkeypatch) -> None:
    """Every node committed before the crash: resume only finalizes, no SSH."""
    script = _script(_scratch(tmp_path))
    nodes = [_node("node-a", tmp_path / "key"), _node("node-b", tmp_path / "key")]
    store = _make_store(tmp_path)
    spec = _make_run_spec(script)
    run_id = _start_owned_run(store, spec, [n["node_id"] for n in nodes])
    _commit_terminal(store, run_id, nodes[0], script, new_uuid7(),
                     status="SUCCEEDED", exit_code=0, error=None)
    _commit_terminal(store, run_id, nodes[1], script, new_uuid7(),
                     status="FAILED", exit_code=3,
                     error=error_dict("exec_nonzero", "script exited 3"))
    _make_stale(store, run_id)

    calls: list[int] = []

    async def _never(**kwargs):
        calls.append(1)
        return ExecutionOutcome(exit_code=0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setattr(run_mod, "execute_script", _never)
    outcome = asyncio.run(
        execute_run(
            store, run_id=run_id, run_spec=spec, nodes=nodes, script=script,
            known_hosts_path=None, resume=True,
        )
    )
    assert calls == []
    assert outcome.counts == {"targeted": 2, "succeeded": 1, "failed": 1,
                              "unknown": 0, "cancelled": 0, "skipped": 0}
    assert outcome.error_counts == {"exec_nonzero": 1}
    assert outcome.run_status == "SUCCESS"
    assert outcome.batch_status == "partial"
    assert outcome.exit_code == 1
    assert store.get_run(run_id)["status"] == "SUCCESS"
    with store.database.connect_migrated() as conn:
        row = conn.execute(
            "SELECT summary_json FROM batch_summaries WHERE run_id=?", (run_id,)
        ).fetchone()
        assert row is not None
        summary = json.loads(row["summary_json"])
        assert summary["counts"]["succeeded"] == 1
        assert summary["counts"]["failed"] == 1
