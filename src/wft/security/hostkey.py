"""Host key onboarding: fingerprint discovery and atomic known_hosts update.

AC-002 requires that fingerprints are shown to the operator, confirmed via a
trusted channel, and only then written to ``known_hosts``. Unconfirmed
fingerprints must never be written.

``ssh-keyscan`` emits ``host algorithm <base64 key blob>`` lines. The key blob
is what gets written to ``known_hosts``; the standard ``SHA256:...`` fingerprint
(derived from the blob) is what an operator verifies through a trusted channel
and what ``--accept`` compares against.
"""
from __future__ import annotations

import base64
import hashlib
import os
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from wft.contracts.errors import WFTError

@dataclass
class DiscoveredKey:
    node_id: str
    host: str
    port: int
    algorithm: str
    key_blob: str

    @property
    def fingerprint(self) -> str:
        return fingerprint_of(self.key_blob)

    def display(self) -> str:
        return f"{self.node_id}: {self.algorithm} {self.fingerprint}"


def fingerprint_of(key_blob: str) -> str:
    """Standard OpenSSH host-key fingerprint: ``SHA256:`` + b64(sha256(blob))."""
    raw = base64.b64decode(key_blob + "=" * (-len(key_blob) % 4))
    digest = hashlib.sha256(raw).digest()
    return "SHA256:" + base64.b64encode(digest).decode().rstrip("=")


def ssh_keyscan(host: str, port: int, timeout_sec: float = 5.0) -> list[str]:
    """Run ``ssh-keyscan`` and return raw output lines."""
    cmd = ["ssh-keyscan", "-T", str(int(timeout_sec)), "-p", str(port), host]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec + 5)
    except (subprocess.TimeoutExpired, OSError) as exc:
        raise WFTError(f"ssh-keyscan failed for {host}:{port}: {exc}") from exc
    if result.returncode != 0:
        raise WFTError(f"ssh-keyscan exited {result.returncode} for {host}:{port}")
    return [line for line in result.stdout.splitlines() if line.strip()]


def parse_keyscan_lines(lines: list[str], *, node_id: str, host: str, port: int) -> list[DiscoveredKey]:
    """Parse ``host algorithm <base64 key blob>`` keyscan output lines."""
    keys: list[DiscoveredKey] = []
    for line in lines:
        parts = line.split()
        if len(parts) < 3:
            continue
        if parts[0].startswith("#") or parts[0] == "@cert-authority":
            continue
        algo = parts[1]
        blob = parts[2]
        keys.append(DiscoveredKey(node_id=node_id, host=host, port=port, algorithm=algo, key_blob=blob))
    return keys


def discover(host: str, port: int, node_id: str, *, timeout_sec: float = 5.0) -> list[DiscoveredKey]:
    return parse_keyscan_lines(ssh_keyscan(host, port, timeout_sec), node_id=node_id, host=host, port=port)


def read_known_hosts(path: Path) -> list[str]:
    if not path.is_file():
        return []
    return [line for line in path.read_text(encoding="utf-8", errors="replace").splitlines() if line.strip()]


def write_known_hosts_atomic(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".known_hosts.")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines))
            if lines:
                fh.write("\n")
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def known_hosts_entry(host: str, port: int, algorithm: str, key_blob: str) -> str:
    if port == 22:
        hostspec = host
    else:
        hostspec = f"[{host}]:{port}"
    return f"{hostspec} {algorithm} {key_blob}"


def commit_confirmed(
    path: Path,
    confirmed: list[DiscoveredKey],
) -> list[str]:
    """Append confirmed keys to known_hosts atomically; return newly-added entries."""
    if not confirmed:
        return []
    existing = read_known_hosts(path)
    new_entries = [e for e in (known_hosts_entry(k.host, k.port, k.algorithm, k.key_blob) for k in confirmed)
                   if e not in existing]
    merged = existing + new_entries
    write_known_hosts_atomic(path, merged)
    return new_entries
