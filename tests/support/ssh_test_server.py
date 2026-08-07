"""Minimal asyncssh server for the vertical-slice integration tests.

Provides key-based auth, an exec subsystem that runs commands locally (so a
``sha256sum`` and ``bash <uploaded>.sh`` execute against the real filesystem),
and a local-filesystem SFTP subsystem rooted at ``/`` so uploaded scripts land
in real ``/tmp``.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import shlex
import signal as _signal
from contextlib import suppress

import asyncssh
from asyncssh.sftp import (
    FXF_APPEND,
    FXF_CREAT,
    FXF_EXCL,
    FXF_READ,
    FXF_TRUNC,
    FXF_WRITE,
    SFTPError,
    SFTPName,
    SFTPNoSuchFile,
    SFTPServer,
)


def _pflags_to_os(pflags: int) -> tuple[int, int]:
    """Translate SFTP protocol open flags (FXF_*) into os.open flags + mode."""
    flags = (
        os.O_RDWR
        if pflags & FXF_READ and pflags & FXF_WRITE
        else (os.O_WRONLY if pflags & FXF_WRITE else os.O_RDONLY)
    )
    if pflags & FXF_APPEND:
        flags |= os.O_APPEND
    if pflags & FXF_CREAT:
        flags |= os.O_CREAT
    if pflags & FXF_TRUNC:
        flags |= os.O_TRUNC
    if pflags & FXF_EXCL:
        flags |= os.O_EXCL
    return flags, 0o600


class _LocalSFTPServer(SFTPServer):
    def __init__(self, chan=None, root: str = "/") -> None:
        super().__init__(chan)
        self._root = os.path.realpath(root)

    def _map(self, path) -> str:
        text = os.fsdecode(path).lstrip("/")
        real = os.path.realpath(os.path.join(self._root, text))
        root = self._root.rstrip("/")
        if real != self._root and not real.startswith(root + os.sep):
            raise SFTPNoSuchFile(os.fsdecode(path))
        return real

    async def open(self, path, pflags, attrs):
        real = self._map(path)
        flags, mode = _pflags_to_os(pflags)
        try:
            return os.open(real, flags, mode)
        except FileNotFoundError:
            raise SFTPNoSuchFile(os.fsdecode(path)) from None

    async def read(self, file_obj, offset, size):
        try:
            os.lseek(file_obj, offset, os.SEEK_SET)
            return os.read(file_obj, size)
        except OSError as exc:
            raise SFTPError(exc.errno, str(exc)) from None

    async def write(self, file_obj, offset, data):
        try:
            os.lseek(file_obj, offset, os.SEEK_SET)
            return os.write(file_obj, data)
        except OSError as exc:
            raise SFTPError(exc.errno, str(exc)) from None

    async def close(self, file_obj) -> None:
        os.close(file_obj)

    async def stat(self, path):
        try:
            return os.stat(self._map(path))
        except FileNotFoundError:
            raise SFTPNoSuchFile(os.fsdecode(path)) from None

    async def lstat(self, path):
        try:
            return os.lstat(self._map(path))
        except FileNotFoundError:
            raise SFTPNoSuchFile(os.fsdecode(path)) from None

    async def setstat(self, path, attrs):
        real = self._map(path)
        try:
            if attrs.permissions is not None:
                os.chmod(real, attrs.permissions)
        except FileNotFoundError:
            raise SFTPNoSuchFile(os.fsdecode(path)) from None

    async def remove(self, path):
        try:
            os.remove(self._map(path))
        except FileNotFoundError:
            raise SFTPNoSuchFile(os.fsdecode(path)) from None

    async def realpath(self, path):
        return os.fsencode(self._map(path))

    async def scandir(self, path):
        try:
            entries = list(os.scandir(self._map(path)))
        except FileNotFoundError:
            raise SFTPNoSuchFile(os.fsdecode(path)) from None
        return [
            SFTPName(os.fsencode(entry.name), asyncssh.SFTPAttrs.from_local(entry.stat()))
            for entry in entries
        ]

    async def rename(self, oldpath, newpath):
        try:
            os.rename(self._map(oldpath), self._map(newpath))
        except FileNotFoundError:
            raise SFTPNoSuchFile(os.fsdecode(oldpath)) from None

    async def mkdir(self, path, attrs):
        try:
            os.mkdir(self._map(path))
        except FileNotFoundError:
            raise SFTPNoSuchFile(os.fsdecode(path)) from None

    async def rmdir(self, path):
        try:
            os.rmdir(self._map(path))
        except FileNotFoundError:
            raise SFTPNoSuchFile(os.fsdecode(path)) from None


_SIGNALS = {name: getattr(_signal, "SIG" + name) for name in ("TERM", "KILL", "INT", "HUP", "QUIT")}


async def _run_command(process: asyncssh.SSHServerProcess) -> None:
    try:
        command = process.channel.get_command() or ""
        # The fixture emulates a Linux target, but the test server itself runs
        # on the GitHub macOS runner.  Implement only the exact sha256sum form
        # used by the production integrity check so tests do not depend on a
        # host-specific GNU utility.
        try:
            argv = shlex.split(command)
        except ValueError:
            argv = []
        if len(argv) == 2 and argv[0] == "sha256sum":
            path = argv[1]
            if not os.path.isfile(path):
                process.stderr.write(b"sha256sum: missing file\n")
                process.exit(1)
                return
            digest = hashlib.sha256()
            with open(path, "rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
            process.stdout.write(f"{digest.hexdigest()}  {path}\n".encode())
            process.exit(0)
            return
        # Emulate a Linux target: ensure /sbin|/usr/sbin (where sha256sum
        # lives on macOS) are on PATH regardless of the host shell profile.
        # macOS lacks util-linux setsid. The subprocess below already creates
        # a new session, so remove only the production wrapper's setsid token.
        if command.startswith("exec setsid "):
            command = "exec " + command.removeprefix("exec setsid ")
        env = {**os.environ, "PATH": "/sbin:/usr/sbin:/bin:/usr/bin:/usr/local/bin"}
        proc = await asyncio.create_subprocess_shell(
            command,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            # A real sshd runs the remote command in its own session, so client
            # signal requests (terminate/kill) can target the whole process
            # tree via its process group.
            start_new_session=True,
        )

        def _kill_group(sig: int) -> None:
            with suppress(ProcessLookupError, PermissionError):
                os.killpg(proc.pid, sig)

        def _signal_received(name: str) -> None:
            _kill_group(_SIGNALS.get(name, _signal.SIGTERM))

        # Deliver client signals to the spawned tree and ensure it dies if the
        # channel drops, like an sshd sending SIGHUP on session end.
        process.signal_received = _signal_received  # type: ignore[method-assign]
        process.connection_lost = lambda exc: _kill_group(_signal.SIGKILL)  # type: ignore[method-assign]

        out, err = await proc.communicate()
        # The server runs with encoding=None so raw bytes are transmitted
        # unchanged, emulating a real sshd (binary script output must survive).
        process.stdout.write(out)
        process.stderr.write(err)
        returncode = proc.returncode
        process.exit((returncode & 0xFF) if returncode is not None else 0)
    except Exception:  # never let a broken command kill the server
        with suppress(Exception):
            process.exit(1)


class TestSSHServer(asyncssh.SSHServer):
    pass


class RunningServer:
    """Context manager holding a live TestSSHServer on an ephemeral port."""

    def __init__(self, *, host_key_path, authorized_keys, host: str = "127.0.0.1"):
        self._host = host
        self._host_key_path = host_key_path
        self._authorized_keys = authorized_keys
        self._server = None
        self.port: int = 0

    async def __aenter__(self):
        self._server = await asyncssh.create_server(
            lambda: TestSSHServer(),
            self._host,
            0,
            server_host_keys=[os.fspath(self._host_key_path)],
            authorized_client_keys=os.fspath(self._authorized_keys),
            process_factory=_run_command,
            sftp_factory=_LocalSFTPServer,
            sftp_version=6,
            allow_scp=False,
            encoding=None,
        )
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc_info) -> None:
        self._server.close()
        await self._server.wait_closed()
