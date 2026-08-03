"""Gate B vertical slice: RunSpec -> dispatch -> Contract-03 commit -> Contract-05.

The slice covers the approved single-node path: success and failure results
persisted to SQLite, retry bounds from the error matrix, idempotency reuse, and
the CLI exit codes 0/1/2 (命令契约 §7).
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import threading
from pathlib import Path

import asyncssh
import pytest

from wft.cli.main import build_parser
from wft.contracts.errors import WFTIdempotencyConflict
from wft.execution.errors import error_dict
from wft.execution.ssh import ExecutionOutcome
from wft.idgen import new_run_id
from wft.scriptreg.registry import Script
from wft.storage.db import Database
from wft.storage.store import Store

from ssh_test_server import _LocalSFTPServer, _run_command, TestSSHServer

import wft.orchestration.run as run_mod
from wft.orchestration.events import now_iso
from wft.orchestration.run import (
    build_batch_summary,
    build_run_spec,
    create_run,
    execute_run,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _sha_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _script(path: Path, *, expected=(0,), timeout_sec=30) -> Script:
    return Script(
        name="disk-usage",
        path=path,
        sha256=_sha_file(path),
        risk="read_only",
        shell="bash",
        timeout_sec=timeout_sec,
        enabled=True,
        expected_exit_codes=expected,
    )


def _node(host: str, port: int, *, key_path: Path, node_id="node-a") -> dict:
    return {
        "node_id": node_id,
        "host": host,
        "port": port,
        "username": "tester",
        "auth": {"method": "key", "credential_ref": f"file://{key_path}"},
        "groups": ["web"],
        "tags": [],
        "enabled": True,
    }


def _limits(script: Script) -> dict:
    return {
        "global_concurrency": 50,
        "per_node_concurrency": 1,
        "connect_rate_per_sec": 20,
        "connect_timeout_sec": 5,
        "exec_timeout_sec": script.timeout_sec,
    }


def _make_run_spec(script: Script, nodes: list[dict], *, idempotency_key=None) -> dict:
    return build_run_spec(
        run_id=new_run_id(),
        trigger_type="manual",
        actor="tester",
        requested_at=now_iso(),
        inventory_ref="config/inventory.example.yaml",
        selector={"groups": ["web"], "tags": []},
        script=script,
        limits=_limits(script),
        config_snapshot_hash="b" * 64,
        idempotency_key=idempotency_key,
    )


def _make_store(tmp_path: Path) -> Store:
    return Store(Database(tmp_path / "wft.db"))


def _make_host_keys(tmp_path: Path):
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    client_key = asyncssh.generate_private_key("ssh-ed25519")
    host_key_path = tmp_path / "hostkey"
    host_key_path.write_bytes(host_key.export_private_key())
    os.chmod(host_key_path, 0o600)
    client_key_path = tmp_path / "clientkey"
    client_key_path.write_bytes(client_key.export_private_key())
    os.chmod(client_key_path, 0o600)
    client_pub_path = tmp_path / "client.pub"
    client_pub_path.write_text(client_key.export_public_key().decode(), encoding="utf-8")
    return host_key, client_key, host_key_path, client_key_path, client_pub_path


def _known_hosts(known_hosts_path: Path, host: str, port: int, host_key) -> None:
    pub = host_key.export_public_key().decode().split()
    known_hosts_path.write_text(f"[{host}]:{port} {pub[0]} {pub[1]}\n", encoding="utf-8")


def _assert_summary_persisted(store: Store, run_id: str, run_status: str, batch_status: str) -> None:
    with store.database.connect_migrated() as conn:
        row = conn.execute(
            "SELECT summary_json FROM batch_summaries WHERE run_id=?", (run_id,)
        ).fetchone()
        assert row is not None
        summary = json.loads(row["summary_json"])
        assert summary["run_status"] == run_status
        assert summary["batch_status"] == batch_status


# ------------------------------------------------------------------ RunSpec


def test_build_run_spec_is_contract02_valid(tmp_path: Path) -> None:
    script_path = tmp_path / "ok.sh"
    script_path.write_text("#!/bin/bash\necho ok\n", encoding="utf-8")
    spec = _make_run_spec(_script(script_path), [])
    from wft.contracts.validate import validate_contract_all

    assert validate_contract_all("contract-02-runspec", spec) == []


def test_build_batch_summary_is_contract05_valid() -> None:
    summary = build_batch_summary(
        run_id=new_run_id(),
        run_status="SUCCESS",
        batch_status="success",
        counts={"targeted": 1, "succeeded": 1, "failed": 0, "unknown": 0,
                "cancelled": 0, "skipped": 0},
        error_counts={},
        started_at=now_iso(),
        finished_at=now_iso(),
        duration_ms=10,
        exit_code=0,
    )
    from wft.contracts.validate import validate_contract_all

    assert validate_contract_all("contract-05-batch-summary", summary) == []


# ------------------------------------------------------------------ create_run


def test_create_run_then_idempotent_reuse(tmp_path: Path) -> None:
    script_path = tmp_path / "ok.sh"
    script_path.write_text("#!/bin/bash\necho ok\n", encoding="utf-8")
    store = _make_store(tmp_path)
    script = _script(script_path)
    spec = _make_run_spec(script, [], idempotency_key="k_12345678")

    run_id, created = create_run(store, spec, ["node-a"])
    assert created is True
    assert store.get_node_tasks(run_id)[0]["status"] == "PENDING"

    same_spec = _make_run_spec(script, [], idempotency_key="k_12345678")
    again_id, created_again = create_run(store, same_spec, ["node-a"])
    assert again_id == run_id
    assert created_again is False


def test_create_run_same_key_different_params_conflicts(tmp_path: Path) -> None:
    script_path = tmp_path / "ok.sh"
    script_path.write_text("#!/bin/bash\necho ok\n", encoding="utf-8")
    store = _make_store(tmp_path)
    script = _script(script_path)
    create_run(store, _make_run_spec(script, [], idempotency_key="k_12345678"), ["node-a"])

    conflict = build_run_spec(
        run_id=new_run_id(),
        trigger_type="manual",
        actor="other",
        requested_at=now_iso(),
        inventory_ref="config/other.yaml",
        selector={"groups": [], "tags": []},
        script=script,
        limits=_limits(script),
        config_snapshot_hash="c" * 64,
        idempotency_key="k_12345678",
    )
    with pytest.raises(WFTIdempotencyConflict):
        create_run(store, conflict, ["node-a"])


# ------------------------------------------------------------ execute_run


def _run_and_execute(tmp_path: Path, script_body: str):
    """Spin a local SSH server and execute the script end to end."""
    script_path = tmp_path / "script.sh"
    script_path.write_text(script_body, encoding="utf-8")
    script = _script(script_path)

    async def _main():
        host_key, client_key, host_key_path, client_key_path, client_pub_path = _make_host_keys(tmp_path)
        from ssh_test_server import RunningServer

        async with RunningServer(host_key_path=host_key_path, authorized_keys=[client_pub_path]) as server:
            known_hosts_path = tmp_path / "known_hosts"
            _known_hosts(known_hosts_path, "127.0.0.1", server.port, host_key)
            node = _node("127.0.0.1", server.port, key_path=client_key_path)
            store = _make_store(tmp_path)
            spec = _make_run_spec(script, [node])
            run_id, _ = create_run(store, spec, [node["node_id"]])
            outcome = await execute_run(
                store,
                run_id=run_id,
                run_spec=spec,
                nodes=[node],
                script=script,
                known_hosts_path=known_hosts_path,
            )
            return store, run_id, outcome

    return asyncio.run(_main())


def test_success_execution_persists_contract03_and_summary(tmp_path: Path) -> None:
    store, run_id, outcome = _run_and_execute(
        tmp_path, "#!/bin/bash\necho hello\nexit 0\n"
    )
    assert outcome.exit_code == 0
    assert outcome.run_status == "SUCCESS"
    assert outcome.batch_status == "success"
    assert outcome.counts == {"targeted": 1, "succeeded": 1, "failed": 0,
                              "unknown": 0, "cancelled": 0, "skipped": 0}

    with store.database.connect_migrated() as conn:
        row = conn.execute(
            "SELECT status, exit_code, stdout_json, error_json FROM executions "
            "WHERE run_id=?", (run_id,)
        ).fetchone()
        assert row is not None
        assert row["status"] == "SUCCEEDED"
        assert row["exit_code"] == 0
        assert json.loads(row["stdout_json"])["inline"] == "hello\n"
        assert row["error_json"] is None
        task = conn.execute(
            "SELECT status, error_class FROM node_tasks WHERE run_id=? AND node_id=?",
            (run_id, "node-a"),
        ).fetchone()
        assert task["status"] == "SUCCEEDED"
        assert task["error_class"] is None
        run = conn.execute("SELECT status, batch_status FROM runs WHERE run_id=?", (run_id,)).fetchone()
        assert run["status"] == "SUCCESS"
        assert run["batch_status"] == "success"
    _assert_summary_persisted(store, run_id, "SUCCESS", "success")


def test_failure_execution_persisted(tmp_path: Path) -> None:
    store, run_id, outcome = _run_and_execute(
        tmp_path, "#!/bin/bash\necho out\necho boom >&2\nexit 3\n"
    )
    assert outcome.exit_code == 1
    assert outcome.run_status == "FAILED"
    assert outcome.batch_status == "failed"
    assert outcome.counts["failed"] == 1
    assert outcome.error_counts == {"exec_nonzero": 1}

    with store.database.connect_migrated() as conn:
        row = conn.execute(
            "SELECT status, exit_code, error_json FROM executions WHERE run_id=?",
            (run_id,),
        ).fetchone()
        assert row is not None
        assert row["status"] == "FAILED"
        assert row["exit_code"] == 3
        assert json.loads(row["error_json"])["class"] == "exec_nonzero"
        task = conn.execute(
            "SELECT status, error_class FROM node_tasks WHERE run_id=? AND node_id=?",
            (run_id, "node-a"),
        ).fetchone()
        assert task["status"] == "FAILED"
        assert task["error_class"] == "exec_nonzero"
    _assert_summary_persisted(store, run_id, "FAILED", "failed")


def test_transient_error_retries_then_succeeds(tmp_path: Path, monkeypatch) -> None:
    script_path = tmp_path / "ok.sh"
    script_path.write_text("#!/bin/bash\necho ok\n", encoding="utf-8")
    script = _script(script_path)
    store = _make_store(tmp_path)
    node = _node("127.0.0.1", 22, key_path=tmp_path / "key")
    spec = _make_run_spec(script, [node])
    run_id, _ = create_run(store, spec, [node["node_id"]])

    async def _no_sleep(_):  # keep the test fast
        return None

    calls = {"n": 0}

    async def _flaky(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return ExecutionOutcome(error=error_dict("conn_timeout", "first attempt timed out"))
        return ExecutionOutcome(exit_code=0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setattr(asyncio, "sleep", _no_sleep)
    monkeypatch.setattr(run_mod, "execute_script", _flaky)
    outcome = asyncio.run(
        execute_run(
            store,
            run_id=run_id,
            run_spec=spec,
            nodes=[node],
            script=script,
            known_hosts_path=None,
        )
    )
    assert calls["n"] == 2
    assert outcome.exit_code == 0
    assert outcome.run_status == "SUCCESS"
    assert outcome.counts["succeeded"] == 1

    with store.database.connect_migrated() as conn:
        attempts = conn.execute(
            "SELECT attempt_seq, status, error_class FROM attempts "
            "ORDER BY attempt_seq"
        ).fetchall()
        assert [dict(a)["attempt_seq"] for a in attempts] == [1, 2]
        assert dict(attempts[0])["error_class"] == "conn_timeout"
        row = conn.execute(
            "SELECT status FROM executions WHERE run_id=?", (run_id,)
        ).fetchone()
        assert row["status"] == "SUCCEEDED"
        flags = conn.execute(
            "SELECT result_json FROM executions WHERE run_id=?", (run_id,)
        ).fetchone()["result_json"]
        assert "retried" in json.loads(flags)["payload"]["flags"]


# ------------------------------------------------------------------- CLI


class ThreadedSSHServer:
    """Local asyncssh server on a background thread for subprocess CLI tests."""

    def __init__(self, tmp_path: Path):
        self._tmp = Path(tmp_path)
        self._host_key, self._client_key, self._host_key_path, self.client_key_path, self._client_pub = (
            _make_host_keys(self._tmp)
        )
        self.port = 0
        self._loop = None
        self._thread = None
        self._server = None

    def __enter__(self):
        self._loop = asyncio.new_event_loop()
        ready = threading.Event()

        def _run():
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self._start(ready))
            self._loop.run_forever()

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()
        assert ready.wait(10), "SSH test server failed to start"
        return self

    async def _start(self, ready):
        self._server = await asyncssh.create_server(
            lambda: TestSSHServer(),
            "127.0.0.1",
            0,
            server_host_keys=[self._host_key_path],
            authorized_client_keys=[self._client_pub],
            process_factory=_run_command,
            sftp_factory=_LocalSFTPServer,
            sftp_version=6,
            allow_scp=False,
        )
        self.port = self._server.sockets[0].getsockname()[1]
        ready.set()

    def __exit__(self, *exc_info):
        async def _close():
            self._server.close()
            await self._server.wait_closed()

        self._loop.call_soon_threadsafe(lambda: asyncio.ensure_future(_close(), loop=self._loop))
        import time

        time.sleep(0.2)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)


def _write_inventory(tmp_path: Path, host: str, port: int, key_path: Path) -> Path:
    p = tmp_path / "inventory.yaml"
    p.write_text(
        "nodes:\n"
        "  - node_id: node-a\n"
        f"    host: {host}\n"
        f"    port: {port}\n"
        "    username: tester\n"
        "    auth:\n"
        "      method: key\n"
        f"      credential_ref: file://{key_path}\n"
        "    groups: [web]\n"
        "    tags: []\n",
        encoding="utf-8",
    )
    return p


def _write_registry(tmp_path: Path, script_path: Path, *, name="ok") -> Path:
    p = tmp_path / "scripts.yaml"
    p.write_text(
        "scripts:\n"
        "  - name: ok\n"
        f"    path: {script_path.name}\n"
        f"    sha256: {_sha_file(script_path)}\n"
        "    risk: read_only\n"
        "    shell: bash\n"
        "    timeout_sec: 30\n"
        "    enabled: true\n"
        "    expected_exit_codes: [0]\n",
        encoding="utf-8",
    )
    return p


def run_cli(*argv: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "wft.cli.main", *argv],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )


def _cli_env(tmp_path: Path, server: ThreadedSSHServer, *, script_body="#!/bin/bash\necho hello\nexit 0\n") -> dict:
    script_path = tmp_path / "ok.sh"
    script_path.write_text(script_body, encoding="utf-8")
    config = tmp_path / "wft.yaml"
    config.write_text(f"data_dir: {tmp_path / 'data'}\n", encoding="utf-8")
    known_hosts = tmp_path / "known_hosts"
    _known_hosts(known_hosts, "127.0.0.1", server.port, server._host_key)
    return {
        "config": config,
        "inventory": _write_inventory(tmp_path, "127.0.0.1", server.port, server.client_key_path),
        "scripts": _write_registry(tmp_path, script_path),
        "known_hosts": known_hosts,
    }


def test_cli_run_success_exit_zero_and_persisted(tmp_path: Path) -> None:
    with ThreadedSSHServer(tmp_path) as server:
        env = _cli_env(tmp_path, server)
        proc = run_cli(
            "run",
            "--config", str(env["config"]),
            "--inventory", str(env["inventory"]),
            "--scripts", str(env["scripts"]),
            "--script", "ok",
            "--known-hosts", str(env["known_hosts"]),
            "--json",
        )
    assert proc.returncode == 0, proc.stderr
    summary = json.loads(proc.stdout)
    assert summary["meta"]["schema_name"] == "contract-05-batch-summary"
    assert summary["payload"]["run_status"] == "SUCCESS"
    assert summary["payload"]["counts"]["succeeded"] == 1

    # The result landed in the CLI's data_dir SQLite database.
    with Database(tmp_path / "data" / "wft.db").connect_migrated() as conn:
        assert conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM run_events").fetchone()[0] >= 3
        run_row = conn.execute("SELECT status FROM runs").fetchone()
        assert run_row["status"] == "SUCCESS"


def test_cli_run_failure_exit_one(tmp_path: Path) -> None:
    with ThreadedSSHServer(tmp_path) as server:
        env = _cli_env(
            tmp_path, server,
            script_body="#!/bin/bash\necho boom >&2\nexit 7\n",
        )
        proc = run_cli(
            "run",
            "--config", str(env["config"]),
            "--inventory", str(env["inventory"]),
            "--scripts", str(env["scripts"]),
            "--script", "ok",
            "--known-hosts", str(env["known_hosts"]),
            "--json",
        )
    assert proc.returncode == 1, proc.stderr
    summary = json.loads(proc.stdout)
    assert summary["payload"]["run_status"] == "FAILED"
    assert summary["payload"]["counts"]["failed"] == 1
    assert summary["payload"]["error_counts"] == {"exec_nonzero": 1}


def test_cli_run_idempotency_reuses_run(tmp_path: Path) -> None:
    with ThreadedSSHServer(tmp_path) as server:
        env = _cli_env(tmp_path, server)
        argv = [
            "run",
            "--config", str(env["config"]),
            "--inventory", str(env["inventory"]),
            "--scripts", str(env["scripts"]),
            "--script", "ok",
            "--known-hosts", str(env["known_hosts"]),
            "--idempotency-key", "k_cli_12345678",
            "--json",
        ]
        first = run_cli(*argv)
        second = run_cli(*argv)
    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    first_summary = json.loads(first.stdout)
    second_payload = json.loads(second.stdout)
    assert second_payload["meta"]["schema_name"] == "contract-01-envelope"
    assert second_payload["payload"]["reused"] is True
    assert second_payload["payload"]["run_id"] == first_summary["payload"]["run_id"]
    # Only one run exists in the database.
    with Database(tmp_path / "data" / "wft.db").connect_migrated() as conn:
        assert conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 1


def test_cli_run_idempotency_conflict_exit_two(tmp_path: Path) -> None:
    with ThreadedSSHServer(tmp_path) as server:
        env = _cli_env(tmp_path, server)
        argv = [
            "run",
            "--config", str(env["config"]),
            "--inventory", str(env["inventory"]),
            "--scripts", str(env["scripts"]),
            "--script", "ok",
            "--known-hosts", str(env["known_hosts"]),
            "--idempotency-key", "k_cli_conflict_1",
        ]
        first = run_cli(*argv)
        # Different selector -> different parameters -> idempotency conflict (exit 2).
        conflicted = run_cli(
            "run",
            "--config", str(env["config"]),
            "--inventory", str(env["inventory"]),
            "--scripts", str(env["scripts"]),
            "--script", "ok",
            "--known-hosts", str(env["known_hosts"]),
            "--group", "web",
            "--idempotency-key", "k_cli_conflict_1",
        )
    assert first.returncode == 0, first.stderr
    assert conflicted.returncode == 2, conflicted.stderr
    assert "idempotency" in conflicted.stderr.lower()


def test_cli_run_config_error_exit_two(tmp_path: Path) -> None:
    proc = run_cli("run", "--inventory", str(tmp_path / "missing.yaml"), "--script", "ok")
    assert proc.returncode == 2
    assert "error" in proc.stderr.lower()


def test_cli_run_help_mentions_options() -> None:
    parser = build_parser()
    args = parser.parse_args(["run", "--inventory", "x", "--script", "y"])
    assert callable(getattr(args, "handler", None))
    assert args.group == []
    assert args.tag == []
