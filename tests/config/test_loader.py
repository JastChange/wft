from pathlib import Path

import pytest

from wft.config.loader import ConfigFileModeError, load_config


def test_loads_private_absolute_paths(tmp_path: Path) -> None:
    key = tmp_path / "node.key"
    key.write_text("fixture")
    key.chmod(0o600)
    cfg = tmp_path / "wft.yaml"
    cfg.write_text(
        "data_dir: ./data\n"
        "inventory_path: ./inventory.yaml\n"
        "script_repository:\n"
        "  url: ssh://git@example/scripts.git\n"
        "  branch: main\n"
        f"  deploy_key_path: {key}\n"
        "web:\n  bind_host: 10.0.0.5\n  port: 8080\n"
    )
    cfg.chmod(0o600)

    loaded = load_config(cfg)

    assert loaded.data_dir == (tmp_path / "data").resolve()
    assert loaded.inventory_path == (tmp_path / "inventory.yaml").resolve()
    assert loaded.script_repository.deploy_key_path == key.resolve()
    assert loaded.web.bind_host == "10.0.0.5"


def test_rejects_group_readable_config(tmp_path: Path) -> None:
    cfg = tmp_path / "wft.yaml"
    cfg.write_text("data_dir: ./data\ninventory_path: ./inventory.yaml\n")
    cfg.chmod(0o640)
    with pytest.raises(ConfigFileModeError):
        load_config(cfg)


def test_rejects_relative_deploy_key_path(tmp_path: Path) -> None:
    cfg = tmp_path / "wft.yaml"
    cfg.write_text(
        "data_dir: ./data\n"
        "inventory_path: ./inventory.yaml\n"
        "script_repository:\n"
        "  url: ssh://git@example/scripts.git\n"
        "  branch: main\n"
        "  deploy_key_path: ./deploy-key\n"
        "web:\n  bind_host: 127.0.0.1\n"
    )
    cfg.chmod(0o600)

    with pytest.raises(ConfigFileModeError, match="absolute"):
        load_config(cfg)


def test_rejects_non_private_deploy_key(tmp_path: Path) -> None:
    key = tmp_path / "deploy-key"
    key.write_text("fixture")
    key.chmod(0o644)
    cfg = tmp_path / "wft.yaml"
    cfg.write_text(
        "data_dir: ./data\n"
        "inventory_path: ./inventory.yaml\n"
        "script_repository:\n"
        "  url: ssh://git@example/scripts.git\n"
        "  branch: main\n"
        f"  deploy_key_path: {key}\n"
        "web:\n  bind_host: 127.0.0.1\n"
    )
    cfg.chmod(0o600)

    with pytest.raises(ConfigFileModeError, match="deploy key must be mode 0600"):
        load_config(cfg)


def test_rejects_concurrency_above_fifty(tmp_path: Path) -> None:
    key = tmp_path / "deploy-key"
    key.write_text("fixture")
    key.chmod(0o600)
    cfg = tmp_path / "wft.yaml"
    cfg.write_text(
        "data_dir: ./data\n"
        "inventory_path: ./inventory.yaml\n"
        "default_concurrency: 51\n"
        "script_repository:\n"
        "  url: ssh://git@example/scripts.git\n"
        "  branch: main\n"
        f"  deploy_key_path: {key}\n"
        "web:\n  bind_host: 127.0.0.1\n"
    )
    cfg.chmod(0o600)

    with pytest.raises(ValueError, match="less than or equal to 50"):
        load_config(cfg)
