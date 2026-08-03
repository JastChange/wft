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


def test_uploaded_script_is_0600(tmp_path: Path) -> None:
    # The script must be uploaded via exclusive 0600 create: a reader should
    # never observe it with weaker permissions or half-written. The mode is read
    # with Python's os.stat (portable across macOS/GNU stat flavors).
    script_path = tmp_path / "stat_self.sh"
    script_path.write_text(
        "#!/bin/bash\n"
        'python3 -c \'import os,sys; print("%03o" % (os.stat(sys.argv[1]).st_mode & 0o777))\' "$0"\n',
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
    assert outcome.stdout == b"600\n"


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


def test_host_key_mismatch_is_classified(tmp_path: Path) -> None:
    script = tmp_path / "ok.sh"
    script.write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")

    async def _run(host, port, key_path, known_hosts_path):
        wrong = asyncssh.generate_private_key("ssh-ed25519")
        mismatch = tmp_path / "mismatch_known_hosts"
        _known_hosts(mismatch, host, port, wrong)
        return await execute_script(
            node=_node(host, port, key_path=key_path), script=_script(script),
            known_hosts_path=mismatch, connect_timeout_sec=5, exec_timeout_sec=2,
        )

    outcome = _run_server_and(_run, tmp_path)
    assert outcome.error["class"] == "host_key_mismatch"
    assert outcome.error["category"] == "SECURITY"
    assert outcome.error["retryable"] is False


def test_auth_failed_is_classified(tmp_path: Path) -> None:
    script_path = tmp_path / "ok.sh"
    script_path.write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")

    async def _run(host, port, key_path, known_hosts_path):
        wrong = asyncssh.generate_private_key("ssh-ed25519")
        wrong_path = tmp_path / "wrong-key"
        wrong_path.write_bytes(wrong.export_private_key())
        os.chmod(wrong_path, 0o600)
        return await execute_script(
            node=_node(host, port, key_path=wrong_path), script=_script(script_path),
            known_hosts_path=known_hosts_path, connect_timeout_sec=5, exec_timeout_sec=2,
        )

    outcome = _run_server_and(_run, tmp_path)
    assert outcome.error["class"] == "auth_failed"
    assert outcome.error["category"] == "PERMANENT"
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


def test_large_utf8_output_not_misdetected_as_binary(tmp_path: Path) -> None:
    # 2.1 MB of legitimate Chinese UTF-8: the 1 MiB cut lands mid-character but
    # the full-stream validity must survive, so the tail is UTF-8, not binary.
    size = 700000  # '你' is 3 UTF-8 bytes -> 2.1 MiB total
    script_path = tmp_path / "chinese.sh"
    script_path.write_text(
        "#!/bin/bash\n"
        f'python3 -c "import sys; sys.stdout.write(\'你\' * {size})"\n',
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
    assert outcome.stdout_total == size * 3
    assert outcome.stdout_valid_utf8 is True
    assert STREAM_HARD_CAP - 3 <= len(outcome.stdout) <= STREAM_HARD_CAP
    decoded = outcome.stdout.decode("utf-8")  # must not raise
    assert decoded == "你" * len(decoded)

    # The full result build must not degrade: legit UTF-8 is never binary.
    from wft.execution.result import build_execution_result
    from wft.storage.blobs import BlobStore

    result, degraded, secondary = build_execution_result(
        run_id="01HX0" + "A" * 21,
        execution_uid="0190a2b3-c4d5-46e7-8890-1234567890ab",
        node_id="node-a",
        script=script,
        status="SUCCEEDED",
        attempt_count=1,
        started_at="2026-08-03T10:00:01+00:00",
        finished_at="2026-08-03T10:00:02+00:00",
        duration_ms=1000,
        exit_code=0,
        stdout_bytes=outcome.stdout,
        stderr_bytes=b"",
        stdout_total=outcome.stdout_total,
        stdout_valid_utf8=outcome.stdout_valid_utf8,
        stderr_valid_utf8=outcome.stderr_valid_utf8,
        error=None,
        blobs=BlobStore(tmp_path / "blobs"),
    )
    assert degraded is False
    assert secondary == ()
    assert result["payload"].get("error") is None
    assert result["payload"]["stdout"]["encoding"] == "utf-8"


def test_exec_timeout_reaps_process_and_cleanup(tmp_path: Path) -> None:
    # After an exec timeout the remote process must be terminated (not left
    # running detached from the result) and the uploaded temp script removed.
    for pattern in ("wft-*.sh", "wft-*.pid", "wft-*.done"):
        for stale in Path("/tmp").glob(pattern):
            stale.unlink()

    script_path = tmp_path / "slow.sh"
    script_path.write_text(
        "#!/bin/bash\n"
        'echo $$ > "${0%.sh}.pid"\n'
        "sleep 30\n"
        'touch "${0%.sh}.done"\n',
        encoding="utf-8",
    )

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

    # The script's own PID must no longer be alive.
    for pid_file in Path("/tmp").glob("wft-*.pid"):
        pid = int(pid_file.read_text().strip())
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
    assert not list(Path("/tmp").glob("wft-*.sh"))
    assert not list(Path("/tmp").glob("wft-*.done"))
