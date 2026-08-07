import asyncio
import shlex
import traceback
from pathlib import Path
from typing import cast

import asyncssh

from wft.clock import format_timestamp, utc_now
from wft.storage.atomic import StorageFullError
from wft.storage.raw_streams import RawWriter
from wft.storage.task_store import TaskStore
from wft.tasks.models import (
    CleanupResult,
    FailureRecord,
    HostKeyRecord,
    InstallationConclusion,
    NodeExecutionResult,
    NodeSnapshot,
    NodeStatus,
    ScriptExecutionResult,
    ScriptStatus,
    TaskScriptDefinition,
    TaskScriptSnapshot,
    TaskType,
)
from wft.tasks.state import conclude_installation


async def _pump(reader: asyncssh.SSHReader[bytes], writer: RawWriter) -> None:
    while True:
        chunk = await reader.read(64 * 1024)
        if not chunk:
            return
        writer.write(chunk)


class AsyncSSHNodeExecutor:
    def __init__(self, store: TaskStore, *, connect_timeout: float = 10) -> None:
        self.store = store
        self.connect_timeout = connect_timeout

    async def _cleanup_remote(
        self, connection: asyncssh.SSHClientConnection, remote_dir: str
    ) -> None:
        result = await connection.run(
            f"rm -rf -- {shlex.quote(remote_dir)}", check=False, encoding=None
        )
        if result.exit_status != 0:
            raise RuntimeError("remote cleanup command failed")

    async def _signal_remote_process_group(
        self,
        connection: asyncssh.SSHClientConnection,
        process: asyncssh.SSHClientProcess[bytes],
        pid_path: str,
        signal: str,
    ) -> None:
        command = (
            f"test -s {shlex.quote(pid_path)} && kill -{signal} -- -$(cat {shlex.quote(pid_path)})"
        )
        result = await connection.run(command, check=False, encoding=None)
        if result.exit_status != 0:
            if signal == "TERM":
                process.terminate()
            else:
                process.kill()

    async def _remote_process_group_exists(
        self, connection: asyncssh.SSHClientConnection, pid_path: str
    ) -> bool:
        command = f"test -s {shlex.quote(pid_path)} && kill -0 -- -$(cat {shlex.quote(pid_path)})"
        result = await connection.run(command, check=False, encoding=None)
        return result.exit_status == 0

    async def _terminate_remote_process_group(
        self,
        connection: asyncssh.SSHClientConnection,
        process: asyncssh.SSHClientProcess[bytes],
        pid_path: str,
    ) -> None:
        await self._signal_remote_process_group(connection, process, pid_path, "TERM")
        for _ in range(20):
            if not await self._remote_process_group_exists(connection, pid_path):
                break
            await asyncio.sleep(0.1)
        if await self._remote_process_group_exists(connection, pid_path):
            await self._signal_remote_process_group(connection, process, pid_path, "KILL")
        try:
            await asyncio.wait_for(process.wait(), timeout=2)
        except TimeoutError:
            process.kill()
            await process.wait()

    def _writers(
        self, task_id: str, node_key: str, script_id: str
    ) -> tuple[RawWriter, RawWriter, RawWriter]:
        writers: list[RawWriter] = []
        try:
            for filename in ("stdout.raw", "stderr.raw", "execution.log.raw"):
                writers.append(self.store.raw_writer(task_id, node_key, script_id, filename))
        except BaseException:
            for writer in writers:
                writer.abort()
            raise
        return writers[0], writers[1], writers[2]

    async def _execute_script(
        self,
        connection: asyncssh.SSHClientConnection,
        sftp: asyncssh.SFTPClient,
        node: NodeSnapshot,
        definition: TaskScriptDefinition,
        remote_dir: str,
    ) -> ScriptExecutionResult:
        stdout_writer, stderr_writer, log_writer = self._writers(
            node.task_id, node.node_key, definition.id
        )
        started_at = format_timestamp(utc_now())
        log_writer.write(f"{started_at} phase=upload script={definition.id}\n".encode())
        remote_path = f"{remote_dir}/{definition.id}-{definition.sha256[:12]}"
        status = ScriptStatus.FAILED
        exit_code: int | None = None
        check_passed: bool | None = None
        failure: FailureRecord | None = None
        try:
            await sftp.put(Path(definition.source_path), remote_path)
            integrity = await connection.run(
                f"sha256sum {shlex.quote(remote_path)}", check=False, encoding=None
            )
            integrity_stdout = cast(bytes, integrity.stdout)
            actual_hash = integrity_stdout.split(maxsplit=1)[0].decode("ascii", errors="replace")
            if integrity.exit_status != 0 or actual_hash != definition.sha256:
                failure = FailureRecord(
                    code="INTEGRITY_CHECK_FAILED",
                    message="uploaded script hash does not match task snapshot",
                    phase="integrity",
                )
            else:
                interpreter = await connection.run(
                    f"command -v {shlex.quote(definition.interpreter)}",
                    check=False,
                    encoding=None,
                )
                if interpreter.exit_status != 0:
                    failure = FailureRecord(
                        code="INTERPRETER_NOT_FOUND",
                        message=f"interpreter not found: {definition.interpreter}",
                        phase="interpreter",
                    )
                else:
                    log_writer.write(f"{format_timestamp(utc_now())} phase=execute\n".encode())
                    pid_path = f"{remote_path}.pid"
                    shell_wrapper = 'echo "$$" > "$1"; shift; exec "$@"'
                    command = " ".join(
                        (
                            "exec setsid /bin/sh -c",
                            shlex.quote(shell_wrapper),
                            "wft-run",
                            shlex.quote(pid_path),
                            shlex.quote(definition.interpreter),
                            shlex.quote(remote_path),
                        )
                    )
                    process: asyncssh.SSHClientProcess[bytes] = await connection.create_process(
                        command, encoding=None
                    )
                    stdout_task = asyncio.create_task(_pump(process.stdout, stdout_writer))
                    stderr_task = asyncio.create_task(_pump(process.stderr, stderr_writer))
                    try:
                        completed = await asyncio.wait_for(
                            process.wait(), timeout=definition.timeout_seconds
                        )
                        exit_code = completed.exit_status
                        status = ScriptStatus.COMPLETED
                        check_passed = exit_code in definition.expected_exit_codes
                    except asyncio.CancelledError:
                        await asyncio.shield(
                            self._terminate_remote_process_group(connection, process, pid_path)
                        )
                        raise
                    except TimeoutError:
                        status = ScriptStatus.TIMEOUT
                        failure = FailureRecord(
                            code="TIMEOUT",
                            message=f"script exceeded {definition.timeout_seconds} seconds",
                            phase="execute",
                        )
                        await self._terminate_remote_process_group(connection, process, pid_path)
                    finally:
                        await asyncio.gather(stdout_task, stderr_task)
        except asyncio.CancelledError:
            stdout_writer.abort()
            stderr_writer.abort()
            log_writer.abort()
            raise
        except StorageFullError:
            stdout_writer.abort()
            stderr_writer.abort()
            log_writer.abort()
            raise
        except Exception as exc:
            log_writer.write(traceback.format_exc().encode("utf-8", errors="replace"))
            failure = FailureRecord(
                code="SCRIPT_EXECUTION_FAILED", message=str(exc), phase="execute"
            )
        finished_at = format_timestamp(utc_now())
        if failure is not None:
            log_writer.write(
                f"{finished_at} error={failure.code} message={failure.message}\n".encode(
                    "utf-8", errors="replace"
                )
            )
        log_writer.write(f"{finished_at} status={status.value} exit_code={exit_code}\n".encode())
        try:
            stdout = stdout_writer.finish()
            stderr = stderr_writer.finish()
            execution_log = log_writer.finish()
        except BaseException:
            stdout_writer.abort()
            stderr_writer.abort()
            log_writer.abort()
            raise
        result = ScriptExecutionResult(
            task_id=node.task_id,
            node_key=node.node_key,
            script_id=definition.id,
            script_sha256=definition.sha256,
            interpreter=definition.interpreter,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            exit_code=exit_code,
            expected_exit_codes=definition.expected_exit_codes,
            check_passed=check_passed,
            stdout=stdout,
            stderr=stderr,
            execution_log=execution_log,
            failure=failure,
        )
        self.store.commit_script_result(result)
        return result

    async def execute(
        self,
        node: NodeSnapshot,
        snapshot: TaskScriptSnapshot,
    ) -> NodeExecutionResult:
        started_at = format_timestamp(utc_now())
        remote_dir = f"/tmp/wft-{node.task_id}-{node.node_key}"
        host_key: HostKeyRecord | None = None
        script_results: list[ScriptExecutionResult] = []
        cleanup = CleanupResult(attempted=False, succeeded=False)
        node_failure: FailureRecord | None = None
        try:
            connection = await asyncssh.connect(
                node.host,
                node.port,
                username=node.username,
                client_keys=[node.private_key_path],
                known_hosts=None,
                agent_path=None,
                preferred_auth=("publickey",),
                login_timeout=self.connect_timeout,
                encoding=None,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            details = f"{exc}\n{traceback.format_exc()}"
            return NodeExecutionResult(
                task_id=node.task_id,
                node_key=node.node_key,
                status=NodeStatus.FAILED,
                installation_conclusion=(
                    InstallationConclusion.INCONCLUSIVE
                    if snapshot.task_type is TaskType.INSTALLATION_VALIDATION
                    else None
                ),
                started_at=started_at,
                finished_at=format_timestamp(utc_now()),
                cleanup=cleanup,
                failure=FailureRecord(code="CONNECTION_FAILED", message=details, phase="connect"),
            )
        async with connection:
            key = connection.get_server_host_key()
            if key is None:
                raise RuntimeError("SSH server did not provide a host key")
            host_key = HostKeyRecord(
                algorithm=key.get_algorithm(),
                fingerprint=key.get_fingerprint("sha256"),
                accepted_automatically=True,
            )
            self.store.update_node(
                NodeSnapshot.model_validate(
                    {**node.model_dump(mode="json"), "host_key": host_key.model_dump(mode="json")}
                )
            )
            try:
                sftp = await connection.start_sftp_client()
                async with sftp:
                    await sftp.mkdir(
                        remote_dir,
                        asyncssh.SFTPAttrs(permissions=0o700),
                    )
                    for definition in snapshot.scripts:
                        script_results.append(
                            await self._execute_script(
                                connection, sftp, node, definition, remote_dir
                            )
                        )
            except asyncio.CancelledError:
                raise
            except StorageFullError:
                raise
            except Exception as exc:
                node_failure = FailureRecord(
                    code="NODE_EXECUTION_FAILED",
                    message=f"{exc}\n{traceback.format_exc()}",
                    phase="execute",
                )
            finally:
                try:
                    await self._cleanup_remote(connection, remote_dir)
                    cleanup = CleanupResult(attempted=True, succeeded=True)
                except Exception as exc:
                    details = f"{exc}\n{traceback.format_exc()}"
                    cleanup = CleanupResult(attempted=True, succeeded=False, error=details)
                    if node_failure is None:
                        node_failure = FailureRecord(
                            code="CLEANUP_FAILED", message=details, phase="cleanup"
                        )
        status = (
            NodeStatus.COMPLETED
            if len(script_results) == len(snapshot.scripts)
            else NodeStatus.FAILED
        )
        conclusion = (
            conclude_installation(script_results)
            if snapshot.task_type is TaskType.INSTALLATION_VALIDATION
            else None
        )
        return NodeExecutionResult(
            task_id=node.task_id,
            node_key=node.node_key,
            status=status,
            installation_conclusion=conclusion,
            started_at=started_at,
            finished_at=format_timestamp(utc_now()),
            script_ids=tuple(result.script_id for result in script_results),
            cleanup=cleanup,
            failure=node_failure,
            host_key=host_key,
        )
