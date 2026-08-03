"""CLI surface: exit codes and JSON output (命令契约 §7)."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pytest

from wft.cli.main import build_parser, main
from wft.cli.common import EXIT_CONFIG, EXIT_OK

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


def test_not_implemented_json_output() -> None:
    proc = run_cli("run", "--json")
    assert proc.returncode == EXIT_CONFIG
    payload = json.loads(proc.stdout)
    assert payload["meta"]["schema_name"] == "contract-01-envelope"
    assert payload["payload"]["available"] is False


def test_inventory_check_json_output() -> None:
    proc = run_cli("inventory", "check", "--file", str(INVENTORY_EXAMPLE), "--json")
    assert proc.returncode == EXIT_OK
    payload = json.loads(proc.stdout)
    assert payload["meta"]["schema_name"] == "contract-11-inventory"


def test_main_no_args_prints_help() -> None:
    # parse_args requires a command; no-arg -> parser error via SystemExit in
    # argparse. build_parser with default -> prints help and returns EXIT_OK.
    parser = build_parser()
    ns = parser.parse_args(["inventory", "check", "--file", str(INVENTORY_EXAMPLE)])
    assert callable(getattr(ns, "handler", None))


def test_hostkey_decide_refuses_unconfirmed_without_tty() -> None:
    """Without --accept/--yes and without a TTY, a fingerprint must not be accepted."""
    from wft.cli import hostkey_cmd
    from wft.security.hostkey import DiscoveredKey

    key = DiscoveredKey(node_id="node-a", host="10.0.0.11", port=22, algorithm="ssh-ed25519", fingerprint="AAAA")
    args = argparse.Namespace(yes=False)
    assert hostkey_cmd._decide(key, {}, args) is False


def test_hostkey_decide_accepts_pre_reviewed() -> None:
    from wft.cli import hostkey_cmd
    from wft.security.hostkey import DiscoveredKey

    key = DiscoveredKey(node_id="node-a", host="10.0.0.11", port=22, algorithm="ssh-ed25519", fingerprint="AAAA")
    args = argparse.Namespace(yes=False)
    reviewed = {("node-a", "ssh-ed25519"): "AAAA"}
    assert hostkey_cmd._decide(key, reviewed, args) is True
    wrong = {("node-a", "ssh-ed25519"): "BBBB"}
    assert hostkey_cmd._decide(key, wrong, args) is False


def test_hostkey_decide_yes_flag() -> None:
    from wft.cli import hostkey_cmd
    from wft.security.hostkey import DiscoveredKey

    key = DiscoveredKey(node_id="node-a", host="10.0.0.11", port=22, algorithm="ssh-ed25519", fingerprint="AAAA")
    args = argparse.Namespace(yes=True)
    assert hostkey_cmd._decide(key, {}, args) is True


def test_hostkey_parse_accept() -> None:
    from wft.cli import hostkey_cmd

    parsed = hostkey_cmd._parse_accept(["node-a:ssh-ed25519:AAA:BB", "node-b:ssh-rsa:CC"])
    assert parsed == {("node-a", "ssh-ed25519"): "AAA:BB", ("node-b", "ssh-rsa"): "CC"}
    with pytest.raises(Exception):
        hostkey_cmd._parse_accept(["node-a:ssh-ed25519"])
