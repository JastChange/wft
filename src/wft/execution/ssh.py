"""asyncssh execution chain with strict host-key verification.

Per the approved plan the chain is: strict known_hosts -> SFTP upload to a
unique 0600 temp path -> remote sha256 re-verify -> run via the registry shell
under the exec timeout -> collect bytes -> always remove the temp file and
close the connection. Failures reduce to Contract-03 error objects via the
global error matrix so retry/aggregation stay consistent.
"""
from __future__ import annotations

import asyncio
import errno as _errno
import os
import secrets
import shlex
import socket
from dataclasses import dataclass
from pathlib import Path

import asyncssh

from wft.contracts.errors import WFTExecutionError
from wft.scriptreg.registry import Script

from .errors import error_dict

_AUTH_FAILED = "auth_failed"


@dataclass
class ExecutionOutcome:
    exit_code: int | None = None
    stdout: bytes = b""
    stderr: bytes = b""
    error: dict | None = None
    duration_ms: int = 0


async def execute_script(
    *,
    node: dict,
    script: Script,
    known_hosts_path: Path | None,
    connect_timeout_sec: int,
    exec_timeout_sec: int,
) -> ExecutionOutcome:
    """Run ``script`` on ``node`` and return an outcome or a mapped error."""
    host = node["host"]
    port = int(node["port"])
    username = node["username"]
    start = asyncio.get_running_loop().time()

    host_known = _host_is_trusted(known_hosts_path, host, port)
    if host_known is False:
        return ExecutionOutcome(
            error=error_dict(
                "host_key_unknown",
                f"host {host}:{port} has no entry in known_hosts "
                f"{known_hosts_path or '<none>'}; run 'wft hostkey onboard'",
            )
        )

    try:
        connect_kwargs = _auth_options(node)
    except WFTExecutionError as exc:
        return ExecutionOutcome(error=error_dict("secret_resolution_failed", str(exc)))

    connect_kwargs.update(
        {
            "host": host,
            "port": port,
            "username": username,
            "known_hosts": str(known_hosts_path) if known_hosts_path else b"",
            "connect_timeout": float(connect_timeout_sec),
        }
    )
    try:
        conn = await asyncssh.connect(**connect_kwargs)
    except (asyncssh.HostKeyNotVerifiable,) as exc:
        err_cls = "host_key_mismatch" if host_known else "host_key_unknown"
        return ExecutionOutcome(error=error_dict(err_cls, str(exc)))
    except (asyncio.TimeoutError, OSError, asyncssh.Error) as exc:
        return ExecutionOutcome(error=_map_connection_error(exc))

    remote_path = f"/tmp/wft-{secrets.token_hex(8)}.sh"
    try:
        outcome = await _run_remote(
            conn, script, remote_path, exec_timeout_sec=exec_timeout_sec
        )
    finally:
        try:
            await conn.run(f"rm -f {shlex.quote(remote_path)}", check=False)
        except (asyncssh.Error, OSError):
            pass
        conn.close()
    outcome.duration_ms = int((asyncio.get_running_loop().time() - start) * 1000)
    return outcome


async def _run_remote(
    conn: asyncssh.SSHClientConnection,
    script: Script,
    remote_path: str,
    *,
    exec_timeout_sec: int,
) -> ExecutionOutcome:
    try:
        async with conn.start_sftp_client() as sftp:
            await sftp.put(script.path, remotepath=remote_path)
            await sftp.chmod(remote_path, 0o600)
    except (asyncssh.Error, OSError) as exc:
        return ExecutionOutcome(error=error_dict("upload_failed", str(exc)))

    verify = await conn.run(f"sha256sum {shlex.quote(remote_path)}", check=False)
    remote_hash = (verify.stdout or "").split()[0] if verify.stdout else ""
    if remote_hash != script.sha256:
        return ExecutionOutcome(
            error=error_dict(
                "script_integrity_failed",
                f"remote sha256 {remote_hash!r} != local {script.sha256[:12]}...",
            )
        )

    try:
        proc = await conn.run(
            f"{script.shell} {shlex.quote(remote_path)}",
            check=False,
            timeout=exec_timeout_sec,
            encoding=None,
        )
    except asyncio.TimeoutError:
        return ExecutionOutcome(
            error=error_dict(
                "exec_timeout", f"script exceeded exec_timeout_sec={exec_timeout_sec}"
            )
        )
    except (asyncssh.Error, OSError) as exc:
        return ExecutionOutcome(error=_map_connection_error(exc))

    return ExecutionOutcome(
        exit_code=proc.exit_status,
        stdout=proc.stdout or b"",
        stderr=proc.stderr or b"",
    )


def _auth_options(node: dict) -> dict:
    """Translate the Contract-11 auth object into asyncssh connect kwargs."""
    auth = node["auth"]
    method = auth["method"]
    ref = auth["credential_ref"]
    if method == "agent":
        return {}
    if method == "key":
        return {"client_keys": [_resolve_ref(ref)]}
    if method == "password":
        return {"password": _resolve_ref(ref, secret=True)}
    raise WFTExecutionError(f"unsupported auth method {method!r}")


def _resolve_ref(ref: str, *, secret: bool = False) -> str:
    if ref.startswith("file://"):
        path = Path(ref[len("file://"):])
        if secret:
            try:
                return path.read_text(encoding="utf-8").strip()
            except OSError as exc:
                raise WFTExecutionError(f"cannot read secret file {path}: {exc}") from exc
        return str(path)
    if ref.startswith("env://"):
        name = ref[len("env://"):]
        if name not in os.environ:
            raise WFTExecutionError(f"environment variable {name!r} is not set")
        return os.environ[name]
    raise WFTExecutionError(f"unsupported credential_ref {ref!r}")


def _host_is_trusted(known_hosts_path: Path | None, host: str, port: int) -> bool | None:
    """Return True when the host has an entry, False when it is absent.

    ``None`` means the known_hosts file is missing or unreadable, in which case
    strict mode treats the host as unknown (the caller maps to host_key_unknown).
    """
    if known_hosts_path is None or not known_hosts_path.is_file():
        return None
    try:
        keys = asyncssh.match_known_hosts(
            str(known_hosts_path), host, host, port
        )[0]
    except (OSError, ValueError, IndexError):
        return None
    return bool(keys)


def _map_connection_error(exc: BaseException) -> dict:
    if isinstance(exc, asyncssh.PermissionDenied):
        return error_dict(_AUTH_FAILED, str(exc))
    if isinstance(exc, asyncssh.HostKeyNotVerifiable):
        return error_dict("host_key_unknown", str(exc))
    if isinstance(exc, asyncio.TimeoutError):
        return error_dict("conn_timeout", str(exc))
    if isinstance(exc, socket.gaierror):
        return error_dict("dns_failed", str(exc))
    if isinstance(exc, asyncssh.OSError) or isinstance(exc, OSError):
        cls = _errno_to_class(getattr(exc, "errno", None))
        return error_dict(cls, str(exc))
    if isinstance(exc, asyncssh.DisconnectError):
        return error_dict("conn_reset", str(exc))
    return error_dict("unknown", str(exc))


def _errno_to_class(errno: int | None) -> str:
    mapping = {
        _errno.ETIMEDOUT: "conn_timeout",
        _errno.ECONNREFUSED: "conn_refused",
        _errno.ECONNRESET: "conn_reset",
        _errno.ECONNABORTED: "conn_reset",
        _errno.ENETUNREACH: "network_unreachable",
        _errno.EHOSTUNREACH: "network_unreachable",
    }
    return mapping.get(errno, "unknown")
