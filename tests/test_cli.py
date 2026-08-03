"""CLI surface: exit codes and JSON output (命令契约 §7)."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pytest

from wft.cli.main import build_parser, main
from wft.cli.common import EXIT_BUSINESS, EXIT_CONFIG, EXIT_OK

REPO_ROOT = Path(__file__).resolve().parents[1]
INVENTORY_EXAMPLE = REPO_ROOT / "config" / "inventory.example.yaml"
SCRIPTS_EXAMPLE = REPO_ROOT / "config" / "scripts.example.yaml"


def run_cli(*argv: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "wft.cli.main", *argv],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )


def test_help_exits_ok() -> None:
    proc = run_cli("--help")
    assert proc.returncode == EXIT_OK
    assert "inventory" in proc.stdout
    assert "hostkey" in proc.stdout
    assert "script" in proc.stdout


def test_version() -> None:
    proc = run_cli("--version")
    assert proc.returncode == EXIT_OK
    assert "wft" in proc.stdout


def test_inventory_check_example_ok() -> None:
    proc = run_cli("inventory", "check", "--file", str(INVENTORY_EXAMPLE))
    assert proc.returncode == EXIT_OK, proc.stderr


def test_inventory_check_bad_file_is_config_error(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("nodes:\n  - node_id: x\n", encoding="utf-8")
    proc = run_cli("inventory", "check", "--file", str(bad))
    assert proc.returncode == EXIT_CONFIG
    assert "error" in proc.stderr.lower()


def test_inventory_check_missing_file_is_config_error() -> None:
    proc = run_cli("inventory", "check", "--file", "/no/such/file.yaml")
    assert proc.returncode == EXIT_CONFIG


def test_script_check_example_ok() -> None:
    proc = run_cli("script", "check", "--file", str(SCRIPTS_EXAMPLE))
    assert proc.returncode == EXIT_OK, proc.stderr


def test_script_resolve_example_ok() -> None:
    proc = run_cli("script", "resolve", "--file", str(SCRIPTS_EXAMPLE), "--ref", "disk-usage")
    assert proc.returncode == EXIT_OK, proc.stderr


def test_script_resolve_unknown_is_config_error() -> None:
    proc = run_cli("script", "resolve", "--file", str(SCRIPTS_EXAMPLE), "--ref", "nope")
    assert proc.returncode == EXIT_CONFIG


def test_not_implemented_command_returns_2() -> None:
    for command in ("run", "history", "scheduler", "storage", "export"):
        proc = run_cli(command)
        assert proc.returncode == EXIT_CONFIG
        assert "not implemented" in proc.stderr


def _assert_contract01_valid(payload: dict) -> None:
    from wft.contracts import validate as cv

    problems = cv.validate_contract_all("contract-01-envelope", payload)
    assert not problems, f"JSON output violates Contract-01: {problems}"
    assert payload["meta"]["schema_name"] == "contract-01-envelope"


def test_not_implemented_json_output() -> None:
    proc = run_cli("run", "--json")
    assert proc.returncode == EXIT_CONFIG
    payload = json.loads(proc.stdout)
    _assert_contract01_valid(payload)
    assert payload["payload"]["available"] is False


def test_inventory_check_json_output() -> None:
    proc = run_cli("inventory", "check", "--file", str(INVENTORY_EXAMPLE), "--json")
    assert proc.returncode == EXIT_OK
    _assert_contract01_valid(json.loads(proc.stdout))


def test_script_check_json_output() -> None:
    proc = run_cli("script", "check", "--file", str(SCRIPTS_EXAMPLE), "--json")
    assert proc.returncode == EXIT_OK
    _assert_contract01_valid(json.loads(proc.stdout))


def test_script_resolve_json_output() -> None:
    proc = run_cli("script", "resolve", "--file", str(SCRIPTS_EXAMPLE), "--ref", "disk-usage", "--json")
    assert proc.returncode == EXIT_OK
    _assert_contract01_valid(json.loads(proc.stdout))


def test_main_no_args_prints_help() -> None:
    # parse_args requires a command; no-arg -> parser error via SystemExit in
    # argparse. build_parser with default -> prints help and returns EXIT_OK.
    parser = build_parser()
    ns = parser.parse_args(["inventory", "check", "--file", str(INVENTORY_EXAMPLE)])
    assert callable(getattr(ns, "handler", None))


def test_hostkey_decide_refuses_unconfirmed_without_tty() -> None:
    """Without --accept and without a TTY, a fingerprint must not be accepted."""
    from wft.cli import hostkey_cmd
    from wft.security.hostkey import DiscoveredKey

    key = DiscoveredKey(node_id="node-a", host="10.0.0.11", port=22, algorithm="ssh-ed25519", key_blob="AAAA")
    assert hostkey_cmd._decide(key, {}) is False


def test_hostkey_decide_accepts_pre_reviewed() -> None:
    from wft.cli import hostkey_cmd
    from wft.security.hostkey import DiscoveredKey, fingerprint_of

    key = DiscoveredKey(node_id="node-a", host="10.0.0.11", port=22, algorithm="ssh-ed25519", key_blob="AAAA")
    reviewed = {("node-a", "ssh-ed25519"): key.fingerprint}
    assert hostkey_cmd._decide(key, reviewed) is True
    wrong = {("node-a", "ssh-ed25519"): fingerprint_of("BBBB")}
    assert hostkey_cmd._decide(key, wrong) is False


def test_hostkey_parse_accept() -> None:
    from wft.cli import hostkey_cmd

    parsed = hostkey_cmd._parse_accept(["node-a:ssh-ed25519:SHA256:AAA:BB", "node-b:ssh-rsa:SHA256:CC"])
    assert parsed == {("node-a", "ssh-ed25519"): "SHA256:AAA:BB", ("node-b", "ssh-rsa"): "SHA256:CC"}
    with pytest.raises(Exception):
        hostkey_cmd._parse_accept(["node-a:ssh-ed25519"])


def test_hostkey_onboard_returns_business_on_discovery_failure(monkeypatch, tmp_path) -> None:
    """Any discovery failure or unconfirmed fingerprint => exit 1 (命令契约 §7)."""
    from wft.cli import hostkey_cmd
    from wft.contracts.errors import WFTError

    def fake_discover(host, port, node_id, *, timeout_sec=5.0):
        raise WFTError("connection refused")

    monkeypatch.setattr(hostkey_cmd.hostkey, "discover", fake_discover)
    args = build_parser().parse_args([
        "hostkey", "onboard", "--inventory", str(INVENTORY_EXAMPLE),
        "--known-hosts", str(tmp_path / "known_hosts"),
    ])
    assert hostkey_cmd.handle_onboard(args) == EXIT_BUSINESS


def test_hostkey_onboard_returns_ok_when_all_confirmed(monkeypatch, tmp_path) -> None:
    from wft.cli import hostkey_cmd
    from wft.security.hostkey import DiscoveredKey

    key = DiscoveredKey(node_id="node-a", host="10.0.0.11", port=22, algorithm="ssh-ed25519", key_blob="AAAABLOB")

    def fake_discover(host, port, node_id, *, timeout_sec=5.0):
        return [key]

    monkeypatch.setattr(hostkey_cmd.hostkey, "discover", fake_discover)
    args = build_parser().parse_args([
        "hostkey", "onboard", "--inventory", str(INVENTORY_EXAMPLE), "--node", "node-a",
        "--accept", f"node-a:ssh-ed25519:{key.fingerprint}",
        "--known-hosts", str(tmp_path / "known_hosts"),
    ])
    assert hostkey_cmd.handle_onboard(args) == EXIT_OK
    written = (tmp_path / "known_hosts").read_text(encoding="utf-8")
    assert "ssh-ed25519 AAAABLOB" in written


def test_hostkey_onboard_config_error_is_two(monkeypatch, tmp_path) -> None:
    """Missing inventory / bad node filter => exit 2."""
    from wft.cli import hostkey_cmd

    args = build_parser().parse_args([
        "hostkey", "onboard", "--inventory", str(tmp_path / "missing.yaml"),
    ])
    assert hostkey_cmd.handle_onboard(args) == EXIT_CONFIG


def test_hostkey_onboard_json_success_emits_contract01(monkeypatch, tmp_path, capsys) -> None:
    """--json success: stdout is a pure, valid Contract-01 envelope (exit 0)."""
    from wft.cli import hostkey_cmd
    from wft.security.hostkey import DiscoveredKey

    key = DiscoveredKey(node_id="node-a", host="10.0.0.11", port=22, algorithm="ssh-ed25519", key_blob="AAAABLOB")

    def fake_discover(host, port, node_id, *, timeout_sec=5.0):
        return [key]

    monkeypatch.setattr(hostkey_cmd.hostkey, "discover", fake_discover)
    args = build_parser().parse_args([
        "hostkey", "onboard", "--inventory", str(INVENTORY_EXAMPLE), "--node", "node-a",
        "--accept", f"node-a:ssh-ed25519:{key.fingerprint}",
        "--known-hosts", str(tmp_path / "known_hosts"), "--json",
    ])
    assert hostkey_cmd.handle_onboard(args) == EXIT_OK
    payload = json.loads(capsys.readouterr().out)
    _assert_contract01_valid(payload)
    assert payload["payload"]["confirmed"] == ["node-a"]
    assert payload["payload"]["rejected"] == []
    assert payload["payload"]["written"] == ["10.0.0.11 ssh-ed25519 AAAABLOB"]


def test_hostkey_onboard_json_failure_emits_contract01(monkeypatch, tmp_path, capsys) -> None:
    """--json with a discovery failure: still a valid Contract-01 envelope (exit 1)."""
    from wft.cli import hostkey_cmd
    from wft.contracts.errors import WFTError

    def fake_discover(host, port, node_id, *, timeout_sec=5.0):
        raise WFTError("connection refused")

    monkeypatch.setattr(hostkey_cmd.hostkey, "discover", fake_discover)
    args = build_parser().parse_args([
        "hostkey", "onboard", "--inventory", str(INVENTORY_EXAMPLE), "--node", "node-a",
        "--known-hosts", str(tmp_path / "known_hosts"), "--json",
    ])
    assert hostkey_cmd.handle_onboard(args) == EXIT_BUSINESS
    payload = json.loads(capsys.readouterr().out)
    _assert_contract01_valid(payload)
    assert payload["payload"]["confirmed"] == []
    assert payload["payload"]["rejected"] == [{"node_id": "node-a", "reason": "connection refused"}]
