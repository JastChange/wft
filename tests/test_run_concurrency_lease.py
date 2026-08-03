"""Group D: multi-node concurrency, connect-rate throttle, lease heartbeat/loss.

Proves the Contract-02 limits are enforced with real overlapping execution
(not serial dispatch), connection starts are paced by the rate limiter, a
permit never leaks on error/cancel, the heartbeat renews on cadence and a lost
lease cancels in-flight nodes and leaves the Run RUNNING for a future resumer.
"""
from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import pytest

from wft.execution.errors import error_dict
from wft.execution.ssh import ExecutionOutcome
from wft.execution.throttle import RateLimiter, Throttle
from wft.idgen import new_run_id
from wft.scriptreg.registry import Script
from wft.storage.db import Database
from wft.storage.store import Store

import wft.orchestration.run as run_mod
from wft.orchestration.events import now_iso
from wft.orchestration.run import (
    _NodeOutcome,
    _aggregate,
    build_run_spec,
    create_run,
    execute_run,
)


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


_DEFAULT_LIMITS = {
    "global_concurrency": 50,
    "per_node_concurrency": 1,
    "connect_rate_per_sec": 20,
    "connect_timeout_sec": 5,
    "exec_timeout_sec": 30,
}


def _make_run_spec(script: Script, *, limits: dict | None = None) -> dict:
    return build_run_spec(
        run_id=new_run_id(),
        trigger_type="manual",
        actor="tester",
        requested_at=now_iso(),
        inventory_ref="config/inventory.example.yaml",
        selector={"groups": ["web"], "tags": []},
        script=script,
        limits=limits or _DEFAULT_LIMITS,
        config_snapshot_hash="b" * 64,
    )


# ------------------------------------------------------------ concurrency


def test_global_concurrency_caps_real_overlap(tmp_path: Path, monkeypatch) -> None:
    """Peak in-flight executions never exceeds the cap and does overlap."""
    script = _script(_scratch(tmp_path))
    nodes = [_node(f"node-{i}", tmp_path / "key") for i in range(8)]
    store = _make_store(tmp_path)
    spec = _make_run_spec(
        script,
        limits={
            "global_concurrency": 4,
            "per_node_concurrency": 1,
            "connect_rate_per_sec": 200,
            "connect_timeout_sec": 5,
            "exec_timeout_sec": 30,
        },
    )
    run_id, created = create_run(store, spec, [n["node_id"] for n in nodes])
    assert created

    state = {"active": 0, "peak": 0}

    async def _slow(**kwargs):
        state["active"] += 1
        state["peak"] = max(state["peak"], state["active"])
        await asyncio.sleep(0.1)
        state["active"] -= 1
        return ExecutionOutcome(exit_code=0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setattr(run_mod, "execute_script", _slow)
    outcome = asyncio.run(
        execute_run(
            store,
            run_id=run_id,
            run_spec=spec,
            nodes=nodes,
            script=script,
            known_hosts_path=None,
        )
    )
    assert outcome.run_status == "SUCCESS"
    assert outcome.batch_status == "success"
    assert outcome.exit_code == 0
    assert outcome.counts["succeeded"] == 8
    assert state["active"] == 0  # no permit leaked by the end
    assert 2 <= state["peak"] <= 4  # real overlap, never above the cap


def test_connect_rate_paces_connection_starts(tmp_path: Path, monkeypatch) -> None:
    """Connection starts are spaced by the rate limiter, not bursted."""
    script = _script(_scratch(tmp_path))
    nodes = [_node(f"node-{i}", tmp_path / "key") for i in range(3)]
    store = _make_store(tmp_path)
    spec = _make_run_spec(
        script,
        limits={
            "global_concurrency": 50,
            "per_node_concurrency": 1,
            "connect_rate_per_sec": 4,
            "connect_timeout_sec": 5,
            "exec_timeout_sec": 30,
        },
    )
    run_id, _ = create_run(store, spec, [n["node_id"] for n in nodes])
    starts: list[float] = []

    async def _instant(**kwargs):
        starts.append(asyncio.get_running_loop().time())
        return ExecutionOutcome(exit_code=0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setattr(run_mod, "execute_script", _instant)
    outcome = asyncio.run(
        execute_run(
            store,
            run_id=run_id,
            run_spec=spec,
            nodes=nodes,
            script=script,
            known_hosts_path=None,
        )
    )
    assert outcome.counts["succeeded"] == 3
    assert len(starts) == 3
    # 4 conns/sec => >=0.25s between connection starts; allow 50ms slack.
    assert starts[1] - starts[0] >= 0.2
    assert starts[2] - starts[1] >= 0.2


def test_error_releases_permit_and_others_proceed(tmp_path: Path, monkeypatch) -> None:
    """An error must release its concurrency permit (global_concurrency=1)."""
    script = _script(_scratch(tmp_path))
    nodes = [
        _node("node-0", tmp_path / "key"),
        _node("node-1", tmp_path / "key"),
        _node("node-2", tmp_path / "key"),
    ]
    store = _make_store(tmp_path)
    spec = _make_run_spec(
        script,
        limits={
            "global_concurrency": 1,
            "per_node_concurrency": 1,
            "connect_rate_per_sec": 200,
            "connect_timeout_sec": 5,
            "exec_timeout_sec": 30,
        },
    )
    run_id, _ = create_run(store, spec, [n["node_id"] for n in nodes])

    async def _fake(**kwargs):
        if kwargs["node"]["node_id"] == "node-0":
            return ExecutionOutcome(
                error=error_dict("exec_nonzero", "script exited 3")
            )
        return ExecutionOutcome(exit_code=0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setattr(run_mod, "execute_script", _fake)
    outcome = asyncio.run(
        execute_run(
            store,
            run_id=run_id,
            run_spec=spec,
            nodes=nodes,
            script=script,
            known_hosts_path=None,
        )
    )
    # With global_concurrency=1 the other nodes only run if node-0's error
    # released its permit; otherwise this would deadlock.
    assert outcome.counts["succeeded"] == 2
    assert outcome.counts["failed"] == 1
    assert outcome.batch_status == "partial"
    assert outcome.exit_code == 1


def test_throttle_permits_released_on_error_and_cancel() -> None:
    """Primitives: a semaphore permit returns after an exception, and a
    cancelled rate-limit waiter does not wedge the limiter's schedule."""

    async def _main():
        throttle = Throttle(global_concurrency=2, connect_rate_per_sec=100)

        async def _boom():
            async with throttle.global_semaphore:
                raise ValueError("boom")

        with pytest.raises(ValueError):
            await _boom()

        # Both permits must be back (draining both would block on a leak).
        async def _drain():
            async with throttle.global_semaphore:
                pass
            async with throttle.global_semaphore:
                pass

        await asyncio.wait_for(_drain(), timeout=1.0)

        limiter = RateLimiter(rate_per_sec=10)  # 0.1s slot spacing
        await limiter.wait()
        waiter = asyncio.create_task(limiter.wait())
        await asyncio.sleep(0.05)  # waiter is mid-sleep
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        # The next caller still gets a slot promptly, never hangs.
        await asyncio.wait_for(limiter.wait(), timeout=1.0)

    asyncio.run(_main())


# ------------------------------------------------------- heartbeat / lease


def test_heartbeat_renews_lease_on_cadence(tmp_path: Path, monkeypatch) -> None:
    script = _script(_scratch(tmp_path))
    nodes = [_node("node-a", tmp_path / "key")]
    store = _make_store(tmp_path)
    spec = _make_run_spec(script)
    run_id, _ = create_run(store, spec, ["node-a"])
    renews: list[float] = []
    original = store.renew_lease

    def _spy(run_id_: str, lease_owner: str):
        renews.append(asyncio.get_running_loop().time())
        return original(run_id_, lease_owner)

    store.renew_lease = _spy
    monkeypatch.setattr(run_mod, "HEARTBEAT_INTERVAL_SEC", 0.05)

    async def _slow(**kwargs):
        await asyncio.sleep(0.3)
        return ExecutionOutcome(exit_code=0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setattr(run_mod, "execute_script", _slow)
    outcome = asyncio.run(
        execute_run(
            store,
            run_id=run_id,
            run_spec=spec,
            nodes=nodes,
            script=script,
            known_hosts_path=None,
        )
    )
    assert outcome.run_status == "SUCCESS"
    assert len(renews) >= 2  # heartbeats fire during the run, not once
    # Spacing is at least the heartbeat interval (50ms slack for scheduling).
    assert all(b - a >= 0.03 for a, b in zip(renews, renews[1:]))


def test_lease_loss_cancels_inflight_and_leaves_run_for_resume(
    tmp_path: Path, monkeypatch
) -> None:
    """A lost lease stops dispatch, cancels in-flight nodes and never finalizes."""
    script = _script(_scratch(tmp_path))
    nodes = [_node("node-a", tmp_path / "key"), _node("node-b", tmp_path / "key")]
    store = _make_store(tmp_path)
    spec = _make_run_spec(script)
    run_id, _ = create_run(store, spec, [n["node_id"] for n in nodes])

    def _lost(run_id_: str, lease_owner: str):
        return False  # a resumer took the lease

    store.renew_lease = _lost
    monkeypatch.setattr(run_mod, "HEARTBEAT_INTERVAL_SEC", 0.05)

    async def _hung(**kwargs):
        await asyncio.sleep(60)  # in-flight until the heartbeat cancels us
        return ExecutionOutcome(exit_code=0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setattr(run_mod, "execute_script", _hung)
    outcome = asyncio.run(
        execute_run(
            store,
            run_id=run_id,
            run_spec=spec,
            nodes=nodes,
            script=script,
            known_hosts_path=None,
        )
    )
    assert outcome.lease_lost is True
    assert outcome.run_status == "INTERRUPTED"
    assert outcome.exit_code == 1
    assert outcome.summary == {}
    # The Run is left RUNNING for a resumer; no final summary, no commits.
    assert store.get_run(run_id)["status"] == "RUNNING"
    assert store.get_batch_summary(run_id) is None
    with store.database.connect_migrated() as conn:
        assert conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0] == 0
    for task in store.get_node_tasks(run_id):
        assert task["status"] not in ("SUCCEEDED", "FAILED")


# --------------------------------------------------------- aggregation


def test_aggregate_is_order_independent() -> None:
    """Per-node outcomes sum the same counts in any completion order."""
    nodes = [{"node_id": "n1"}, {"node_id": "n2"}, {"node_id": "n3"}]
    outcomes = [
        _NodeOutcome("n1", "SUCCEEDED", False, (), {"output_decode_failed": 1}),
        _NodeOutcome(
            "n2", "FAILED", True, (), {"exec_nonzero": 1, "blob_write_failed": 1}
        ),
        _NodeOutcome("n3", "SUCCEEDED", False, (), {}),
    ]
    expected_counts = {
        "targeted": 3,
        "succeeded": 2,
        "failed": 1,
        "unknown": 0,
        "cancelled": 0,
        "skipped": 0,
    }
    expected_errors = {
        "output_decode_failed": 1,
        "exec_nonzero": 1,
        "blob_write_failed": 1,
    }

    for counts, errors, degraded, any_ok, any_fail in (
        _aggregate(nodes, outcomes),
        _aggregate(nodes, list(reversed(outcomes))),
    ):
        assert counts == expected_counts
        assert errors == expected_errors
        assert degraded is True
        assert any_ok is True and any_fail is True
