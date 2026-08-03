"""Host key discovery and atomic known_hosts update (AC-002)."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from wft.security.hostkey import (
    commit_confirmed,
    known_hosts_entry,
    parse_keyscan_lines,
    read_known_hosts,
    write_known_hosts_atomic,
)


def test_parse_keyscan_lines() -> None:
    lines = [
        "10.0.0.11 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKh4p",
        "10.0.0.11 ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQ",
        "# 10.0.0.11 ssh-ed25519 (commented)",
        "@cert-authority 10.0.0.11 ssh-rsa AAAACERT",
    ]
    keys = parse_keyscan_lines(lines, node_id="node-a", host="10.0.0.11", port=22)
    assert len(keys) == 2
    assert keys[0].algorithm == "ssh-ed25519"
    assert keys[1].algorithm == "ssh-rsa"
    assert keys[0].node_id == "node-a"
    assert keys[0].host == "10.0.0.11"
    assert keys[0].port == 22


def test_parse_ignores_malformed_lines() -> None:
    keys = parse_keyscan_lines(["", "garbage"], node_id="node-a", host="h", port=22)
    assert keys == []


def test_known_hosts_entry_port_22_plain() -> None:
    assert known_hosts_entry("10.0.0.11", 22, "ssh-ed25519", "AAAA") == "10.0.0.11 ssh-ed25519 AAAA"


def test_known_hosts_entry_nonstandard_port_bracketed() -> None:
    assert known_hosts_entry("10.0.0.11", 2222, "ssh-ed25519", "AAAA") == "[10.0.0.11]:2222 ssh-ed25519 AAAA"


def test_write_atomic_creates_file(tmp_path: Path) -> None:
    target = tmp_path / "sub" / "known_hosts"
    write_known_hosts_atomic(target, ["a.example ssh-ed25519 AAA", "b.example ssh-ed25519 BBB"])
    assert target.read_text(encoding="utf-8") == "a.example ssh-ed25519 AAA\nb.example ssh-ed25519 BBB\n"
    assert (target.stat().st_mode & 0o777) == 0o600


def test_write_atomic_empty_lines(tmp_path: Path) -> None:
    target = tmp_path / "known_hosts"
    write_known_hosts_atomic(target, [])
    assert target.read_text(encoding="utf-8") == ""


def test_read_known_hosts_missing_file_is_empty(tmp_path: Path) -> None:
    assert read_known_hosts(tmp_path / "nope") == []


def test_commit_confirmed_appends_without_duplicates(tmp_path: Path) -> None:
    target = tmp_path / "known_hosts"
    write_known_hosts_atomic(target, ["10.0.0.11 ssh-ed25519 AAA"])
    k1 = parse_keyscan_lines(["10.0.0.11 ssh-ed25519 AAA"], node_id="n", host="10.0.0.11", port=22)
    k2 = parse_keyscan_lines(["10.0.0.12 ssh-ed25519 BBB"], node_id="n", host="10.0.0.12", port=22)
    written = commit_confirmed(target, k1 + k2)
    assert written == ["10.0.0.12 ssh-ed25519 BBB"]
    lines = read_known_hosts(target)
    assert lines.count("10.0.0.11 ssh-ed25519 AAA") == 1
    assert "10.0.0.12 ssh-ed25519 BBB" in lines


def test_commit_confirmed_with_empty_list_is_noop(tmp_path: Path) -> None:
    target = tmp_path / "known_hosts"
    assert commit_confirmed(target, []) == []
    assert not target.exists()


def test_known_hosts_file_mode_0600_after_rewrite(tmp_path: Path) -> None:
    target = tmp_path / "known_hosts"
    os.chmod(target, 0o644) if target.exists() else None
    write_known_hosts_atomic(target, ["x ssh-ed25519 A"])
    assert (target.stat().st_mode & 0o777) == 0o600


def test_discover_runs_keyscan_for_localhost(tmp_path: Path) -> None:
    """ssh-keyscan must exist on the test machine for this to be meaningful."""
    from wft.security import hostkey

    if hostkey.shutil.which("ssh-keyscan") is None:
        pytest.skip("ssh-keyscan not installed")
    keys = hostkey.discover("127.0.0.1", 22, "localhost", timeout_sec=3.0)
    assert isinstance(keys, list)
