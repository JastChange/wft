"""Minimal asyncssh server for the vertical-slice integration tests.

Provides key-based auth, an exec subsystem that runs commands locally (so a
``sha256sum`` and ``bash <uploaded>.sh`` execute against the real filesystem),
and a local-filesystem SFTP subsystem rooted at ``/`` so uploaded scripts land
in real ``/tmp``.
"""
from __future__ import annotations

import asyncio
import os

import asyncssh
from asyncssh.sftp import (
    FXF_APPEND,
    FXF_CREAT,
    FXF_EXCL,
    FXF_READ,
    FXF_TRUNC,
    FXF_WRITE,
    SFTPError,
    SFTPNoSuchFile,
    SFTPServer,
    SFTPName,
)


def _pflags_to_os(pflags: int) -> tuple[int, int]:
    """Translate SFTP protocol open flags (FXF_*) into os.open flags + mode."""
    flags = os.O_RDWR if pflags & FXF_READ and pflags & FXF_WRITE else (
        os.O_WRONLY if pflags & FXF_WRITE else os.O_RDONLY
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


async def _run_command(process: asyncssh.SSHServerProcess) -> None:
    try:
        command = process.channel.get_command() or ""
        # Emulate a Linux target: ensure /sbin|/usr/sbin (where sha256sum
        # lives on macOS) are on PATH regardless of the host shell profile.
        env = {**os.environ, "PATH": "/sbin:/usr/sbin:/bin:/usr/bin:/usr/local/bin"}
        proc = await asyncio.create_subprocess_shell(
            command,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await proc.communicate()
        # The server runs with encoding=None so raw bytes are transmitted
        # unchanged, emulating a real sshd (binary script output must survive).
        process.stdout.write(out)
        process.stderr.write(err)
        process.exit(proc.returncode)
    except Exception:  # never let a broken command kill the server
        try:
            process.exit(1)
        except Exception:
            pass


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
            server_host_keys=[self._host_key_path],
            authorized_client_keys=self._authorized_keys,
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
