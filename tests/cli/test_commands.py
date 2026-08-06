import hashlib
import json
import subprocess
from pathlib import Path

from typer.testing import CliRunner

from wft.auth.password import verify_password
from wft.cli.app import app

runner = CliRunner()


def _write_config(tmp_path: Path, *, repository: Path | None = None) -> Path:
    key = tmp_path / "deploy-key"
    key.write_text("fake deploy key")
    key.chmod(0o600)
    inventory = tmp_path / "inventory.yaml"
    inventory.write_text(
        "nodes:\n"
        f"- name: node-b\n  host: 10.0.0.2\n  username: root\n  private_key_path: {key}\n"
        "  os: ubuntu-22.04\n  groups: [batch]\n  tags: [disk]\n"
        f"- name: node-a\n  host: 10.0.0.1\n  username: root\n  private_key_path: {key}\n"
        "  os: ubuntu-24.04\n  groups: [batch]\n  tags: [memory]\n"
    )
    config = tmp_path / "wft.yaml"
    config.write_text(
        f"data_dir: {tmp_path / 'data'}\n"
        f"inventory_path: {inventory}\n"
        "script_repository:\n"
        f"  url: {repository or tmp_path / 'missing-repository'}\n"
        "  branch: main\n"
        f"  deploy_key_path: {key}\n"
        "web:\n  bind_host: 127.0.0.1\n  port: 8080\n"
    )
    config.chmod(0o600)
    return config


def _write_script_repository(path: Path) -> None:
    path.mkdir()
    subprocess.run(["git", "init", "-b", "main"], cwd=path, check=True)
    script = path / "check.sh"
    script.write_text("#!/bin/sh\ntrue\n")
    digest = hashlib.sha256(script.read_bytes()).hexdigest()
    (path / "manifest.yaml").write_text(
        "schema_version: '1.0'\nscripts:\n"
        f"- id: check\n  path: check.sh\n  sha256: {digest}\n"
        "  interpreter: sh\n  timeout_seconds: 10\n"
        "  expected_exit_codes: [0]\n  read_only: true\n"
        "  supported_os: [ubuntu-22.04, ubuntu-24.04]\n"
        "plans:\n- id: baseline\n  scripts: [check]\n"
    )
    subprocess.run(["git", "add", "."], cwd=path, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=WFT Test",
            "-c",
            "user.email=wft@example.invalid",
            "commit",
            "-m",
            "fixture",
        ],
        cwd=path,
        check=True,
    )


def test_m1_help_exposes_only_available_commands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "admin" in result.stdout
    assert "config" in result.stdout
    assert "inventory" in result.stdout
    assert "scripts" in result.stdout
    assert "run" not in result.stdout


def test_admin_init_and_reset_print_each_generated_password_once(tmp_path: Path) -> None:
    auth_file = tmp_path / "auth" / "admin.json"

    initialized = runner.invoke(app, ["admin", "init", "--auth-file", str(auth_file)])
    assert initialized.exit_code == 0
    first = initialized.stdout.strip().splitlines()[-1]
    assert verify_password(auth_file, first)
    assert first not in auth_file.read_text()

    reset = runner.invoke(app, ["admin", "reset-password", "--auth-file", str(auth_file)])
    assert reset.exit_code == 0
    second = reset.stdout.strip().splitlines()[-1]
    assert second != first
    assert verify_password(auth_file, second)
    assert not verify_password(auth_file, first)


def test_config_check_json_omits_key_path(tmp_path: Path) -> None:
    config = _write_config(tmp_path)

    result = runner.invoke(app, ["config", "check", "--config", str(config), "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["default_concurrency"] == 10
    assert "deploy_key" not in result.stdout
    assert str(tmp_path / "deploy-key") not in result.stdout


def test_inventory_select_is_deterministic_and_omits_connection_details(tmp_path: Path) -> None:
    config = _write_config(tmp_path)

    result = runner.invoke(
        app,
        [
            "inventory",
            "select",
            "--config",
            str(config),
            "--node",
            "node-a",
            "--group",
            "batch",
            "--json",
        ],
    )

    assert result.exit_code == 0
    assert json.loads(result.stdout)["node_names"] == ["node-a", "node-b"]
    assert "10.0.0." not in result.stdout
    assert "deploy-key" not in result.stdout


def test_inventory_select_without_selector_is_configuration_error(tmp_path: Path) -> None:
    config = _write_config(tmp_path)

    result = runner.invoke(app, ["inventory", "select", "--config", str(config)])

    assert result.exit_code == 2


def test_scripts_sync_publishes_snapshot_and_reports_hashes(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    _write_script_repository(repository)
    config = _write_config(tmp_path, repository=repository)

    result = runner.invoke(app, ["scripts", "sync", "--config", str(config), "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert len(payload["commit_sha"]) == 40
    assert len(payload["manifest_sha256"]) == 64
    assert payload["used_cached_snapshot"] is False
    assert str(tmp_path / "deploy-key") not in result.stdout


def test_scripts_sync_requires_explicit_cache_acceptance(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    _write_script_repository(repository)
    config = _write_config(tmp_path, repository=repository)
    assert runner.invoke(app, ["scripts", "sync", "--config", str(config)]).exit_code == 0
    repository.rename(repository.with_suffix(".offline"))

    rejected = runner.invoke(app, ["scripts", "sync", "--config", str(config)], input="n\n")
    accepted = runner.invoke(
        app,
        ["scripts", "sync", "--config", str(config), "--allow-cached-scripts", "--json"],
    )

    assert rejected.exit_code == 2
    assert accepted.exit_code == 0
    assert json.loads(accepted.stdout)["used_cached_snapshot"] is True


def test_scripts_sync_accepts_cache_interactively(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    _write_script_repository(repository)
    config = _write_config(tmp_path, repository=repository)
    assert runner.invoke(app, ["scripts", "sync", "--config", str(config)]).exit_code == 0
    repository.rename(repository.with_suffix(".offline"))

    accepted = runner.invoke(
        app,
        ["scripts", "sync", "--config", str(config)],
        input="y\n",
    )

    assert accepted.exit_code == 0
    assert "cached=true" in accepted.stdout
