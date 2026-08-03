"""Host key discovery and atomic known_hosts update (AC-002).

AC-002 separates the operator-verifiable ``SHA256:...`` fingerprint from the
Base64 key blob: the fingerprint is what is shown and confirmed, while the blob
is what gets written to known_hosts.
"""
from __future__ import annotations

import base64
import hashlib
import shutil
import subprocess
from pathlib import Path

import pytest

from wft.security.hostkey import (
    commit_confirmed,
    fingerprint_of,
    known_hosts_entry,
    parse_keyscan_lines,
    read_known_hosts,
    write_known_hosts_atomic,
)


def test_fingerprint_is_sha256_of_decoded_blob() -> None:
    raw = b"\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20" + b"\x5a" * 32
    blob = base64.b64encode(raw).decode()
    expected = "SHA256:" + base64.b64encode(hashlib.sha256(raw).digest()).decode().rstrip("=")
    assert fingerprint_of(blob) == expected
    assert fingerprint_of(blob).startswith("SHA256:")


def test_parse_keyscan_lines_extracts_blob_and_fingerprint() -> None:
    ed25519_blob = base64.b64encode(b"\x00\x00\x00\x0bssh-ed25519" + b"\x00\x00\x00\x20" + b"\x5a" * 32).decode()
    rsa_blob = base64.b64encode(b"\x00\x00\x00\x07ssh-rsa" + b"\x00\x00\x00\x08" + b"\x01\x02\x03").decode()
    lines = [
        f"10.0.0.11 ssh-ed25519 {ed25519_blob}",
        f"10.0.0.11 ssh-rsa {rsa_blob}",
        "# 10.0.0.11 ssh-ed25519 (commented)",
        "@cert-authority 10.0.0.11 ssh-rsa AAAACERT",
    ]
    keys = parse_keyscan_lines(lines, node_id="node-a", host="10.0.0.11", port=22)
    assert len(keys) == 2
    assert keys[0].algorithm == "ssh-ed25519"
    assert keys[0].key_blob == ed25519_blob
    assert keys[0].fingerprint == fingerprint_of(ed25519_blob)
    assert keys[1].algorithm == "ssh-rsa"
    assert keys[0].node_id == "node-a"
    assert keys[0].host == "10.0.0.11"
    assert keys[0].port == 22


def test_parse_ignores_malformed_lines() -> None:
    keys = parse_keyscan_lines(["", "garbage"], node_id="node-a", host="h", port=22)
    assert keys == []


def test_known_hosts_entry_writes_key_blob() -> None:
    assert known_hosts_entry("10.0.0.11", 22, "ssh-ed25519", "AAAABLOB") == "10.0.0.11 ssh-ed25519 AAAABLOB"


def test_known_hosts_entry_nonstandard_port_bracketed() -> None:
    assert known_hosts_entry("10.0.0.11", 2222, "ssh-ed25519", "AAAABLOB") == "[10.0.0.11]:2222 ssh-ed25519 AAAABLOB"


def test_write_atomic_creates_file(tmp_path: Path) -> None:
    target = tmp_path / "sub" / "known_hosts"
    write_known_hosts_atomic(target, ["a.example ssh-ed25519 AAAABLOB", "b.example ssh-ed25519 BBBBLOB"])
    assert target.read_text(encoding="utf-8") == "a.example ssh-ed25519 AAAABLOB\nb.example ssh-ed25519 BBBBLOB\n"
    assert (target.stat().st_mode & 0o777) == 0o600


def test_write_atomic_empty_lines(tmp_path: Path) -> None:
    target = tmp_path / "known_hosts"
    write_known_hosts_atomic(target, [])
    assert target.read_text(encoding="utf-8") == ""


def test_read_known_hosts_missing_file_is_empty(tmp_path: Path) -> None:
    assert read_known_hosts(tmp_path / "nope") == []


def test_commit_confirmed_appends_without_duplicates(tmp_path: Path) -> None:
    target = tmp_path / "known_hosts"
    write_known_hosts_atomic(target, ["10.0.0.11 ssh-ed25519 AAAABLOB"])
    k1 = parse_keyscan_lines(["10.0.0.11 ssh-ed25519 AAAABLOB"], node_id="n", host="10.0.0.11", port=22)
    k2 = parse_keyscan_lines(["10.0.0.12 ssh-ed25519 BBBBLOB"], node_id="n", host="10.0.0.12", port=22)
    written = commit_confirmed(target, k1 + k2)
    assert written == ["10.0.0.12 ssh-ed25519 BBBBLOB"]
    lines = read_known_hosts(target)
    assert lines.count("10.0.0.11 ssh-ed25519 AAAABLOB") == 1
    assert "10.0.0.12 ssh-ed25519 BBBBLOB" in lines


def test_commit_confirmed_with_empty_list_is_noop(tmp_path: Path) -> None:
    target = tmp_path / "known_hosts"
    assert commit_confirmed(target, []) == []
    assert not target.exists()


def test_known_hosts_file_mode_0600_after_rewrite(tmp_path: Path) -> None:
    target = tmp_path / "known_hosts"
    write_known_hosts_atomic(target, ["x ssh-ed25519 A"])
    assert (target.stat().st_mode & 0o777) == 0o600


def test_fingerprint_matches_ssh_keygen(tmp_path: Path) -> None:
    """Cross-check the Python fingerprint against the authoritative ssh-keygen."""
    if shutil.which("ssh-keygen") is None:
        pytest.skip("ssh-keygen not installed")
    key_path = tmp_path / "hostkey"
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key_path)],
                   check=True, capture_output=True)
    pub_line = (key_path.with_suffix(".pub")).read_text().strip()
    algo, blob = pub_line.split()[:2]
    ours = fingerprint_of(blob)
    out = subprocess.run(["ssh-keygen", "-lf", str(key_path.with_suffix(".pub"))],
                         check=True, capture_output=True, text=True).stdout
    # e.g. "256 SHA256:AbCd... no comment (ED25519)"
    theirs = out.split()[1]
    assert algo in ("ssh-ed25519", "ssh-rsa", "ecdsa-sha2-nistp256")
    assert ours == theirs


def test_discover_runs_keyscan_for_localhost() -> None:
    """ssh-keyscan must exist on the test machine for this to be meaningful."""
    from wft.security import hostkey

    if shutil.which("ssh-keyscan") is None:
        pytest.skip("ssh-keyscan not installed")
    keys = hostkey.discover("127.0.0.1", 22, "localhost", timeout_sec=3.0)
    assert isinstance(keys, list)
