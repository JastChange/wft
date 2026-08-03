"""asyncssh vertical slice: strict known_hosts -> SFTP -> verify -> run.

Spins up a local asyncssh server (key auth, exec, local-filesystem SFTP) and
drives the real ``execute_script`` chain against it.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
from pathlib import Path

import asyncssh
import pytest

from wft.execution.result import STREAM_HARD_CAP
from wft.execution.ssh import execute_script
from wft.scriptreg.registry import Script

from ssh_test_server import RunningServer


def _sha_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _script(path: Path, *, expected=(0,), shell="bash") -> Script:
    return Script(
        name="disk-usage",
        path=path,
        sha256=_sha_file(path),
        risk="read_only",
        shell=shell,
        timeout_sec=30,
        enabled=True,
        expected_exit_codes=expected,
    )


def _node(host: str, port: int, *, key_path: Path) -> dict:
    return {
        "node_id": "node-a",
        "host": host,
        "port": port,
        "username": "tester",
        "auth": {"method": "key", "credential_ref": f"file://{key_path}"},
        "groups": [],
        "tags": [],
        "enabled": True,
    }


def _known_hosts(known_hosts_path: Path, host: str, port: int, host_key) -> None:
    pub = host_key.export_public_key().decode().split()
    algo, blob = pub[0], pub[1]
    known_hosts_path.write_text(f"[{host}]:{port} {algo} {blob}\n", encoding="utf-8")


def _run_server_and(coro_factory, tmp_path: Path):
    async def _main():
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

        async with RunningServer(
            host_key_path=host_key_path, authorized_keys=[client_pub_path]
        ) as server:
            known_hosts_path = tmp_path / "known_hosts"
            _known_hosts(known_hosts_path, "127.0.0.1", server.port, host_key)
            return await coro_factory(
                host="127.0.0.1",
                port=server.port,
                key_path=client_key_path,
                known_hosts_path=known_hosts_path,
            )

    return asyncio.run(_main())


def test_success_execution(tmp_path: Path) -> None:
    script_path = tmp_path / "ok.sh"
    script_path.write_text("#!/bin/bash\necho hello\nexit 0\n", encoding="utf-8")
    script = _script(script_path)

    outcome = _run_server_and(
        lambda host, port, key_path, known_hosts_path: execute_script(
            node=_node(host, port, key_path=key_path),
            script=script,
            known_hosts_path=known_hosts_path,
            connect_timeout_sec=5,
            exec_timeout_sec=10,
        ),
        tmp_path,
    )
    assert outcome.error is None
    assert outcome.exit_code == 0
    assert outcome.stdout == b"hello\n"
    assert outcome.duration_ms >= 0
    # The temp script must be cleaned up from the real filesystem.
    assert not list(tmp_path.rglob("wft-*.sh"))


def test_large_output_keeps_tail_and_tracks_total(tmp_path: Path) -> None:
    # 2 MiB of output: the client must drain it all but keep only the 1 MiB
    # tail in memory, while still reporting the full byte count.
    size = 2 * STREAM_HARD_CAP
    script_path = tmp_path / "big.sh"
    script_path.write_text(
        "#!/bin/bash\n"
        f'python3 -c "import sys; sys.stdout.write(\'a\' * {size})"\n',
        encoding="utf-8",
    )
    script = _script(script_path)

    outcome = _run_server_and(
        lambda host, port, key_path, known_hosts_path: execute_script(
            node=_node(host, port, key_path=key_path),
            script=script,
            known_hosts_path=known_hosts_path,
            connect_timeout_sec=5,
            exec_timeout_sec=10,
        ),
        tmp_path,
    )
    assert outcome.error is None
    assert outcome.exit_code == 0
    assert outcome.stdout_total == size
    assert len(outcome.stdout) == STREAM_HARD_CAP
    assert outcome.stdout == b"a" * STREAM_HARD_CAP


def test_nonzero_exit_captured(tmp_path: Path) -> None:
    script_path = tmp_path / "boom.sh"
    script_path.write_text("#!/bin/bash\necho out\necho boom >&2\nexit 3\n", encoding="utf-8")
    script = _script(script_path, expected=(0,))

    outcome = _run_server_and(
        lambda host, port, key_path, known_hosts_path: execute_script(
            node=_node(host, port, key_path=key_path),
            script=script,
            known_hosts_path=known_hosts_path,
            connect_timeout_sec=5,
            exec_timeout_sec=10,
        ),
        tmp_path,
    )
    assert outcome.error is None
    assert outcome.exit_code == 3
    assert outcome.stdout == b"out\n"
    assert b"boom" in outcome.stderr


def test_host_key_unknown_fails_fast(tmp_path: Path) -> None:
    script_path = tmp_path / "ok.sh"
    script_path.write_text("#!/bin/bash\necho hi\n", encoding="utf-8")

    async def _run(host, port, key_path, known_hosts_path):
        # An empty known_hosts means the host has no entry.
        empty = tmp_path / "empty_known_hosts"
        empty.write_text("", encoding="utf-8")
        return await execute_script(
            node=_node(host, port, key_path=key_path),
            script=_script(script_path),
            known_hosts_path=empty,
            connect_timeout_sec=5,
            exec_timeout_sec=10,
        )

    outcome = _run_server_and(_run, tmp_path)
    assert outcome.error is not None
    assert outcome.error["class"] == "host_key_unknown"
    assert outcome.error["retryable"] is False


def test_exec_timeout_maps_to_exec_timeout(tmp_path: Path) -> None:
    script_path = tmp_path / "slow.sh"
    script_path.write_text("#!/bin/bash\nsleep 3\nexit 0\n", encoding="utf-8")

    async def _run(host, port, key_path, known_hosts_path):
        return await execute_script(
            node=_node(host, port, key_path=key_path),
            script=_script(script_path),
            known_hosts_path=known_hosts_path,
            connect_timeout_sec=5,
            exec_timeout_sec=1,
        )

    outcome = _run_server_and(_run, tmp_path)
    assert outcome.error is not None
    assert outcome.error["class"] == "exec_timeout"
    assert outcome.error["retryable"] is True
