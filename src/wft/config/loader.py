import stat
from pathlib import Path
from typing import Any

import yaml

from .models import AppConfig


class ConfigFileModeError(ValueError):
    """Raised when the private configuration is not mode 0600."""


def _resolve(value: str, base: Path) -> Path:
    path = Path(value).expanduser()
    return (base / path).resolve() if not path.is_absolute() else path.resolve()


def load_config(path: Path) -> AppConfig:
    path = path.expanduser().resolve()
    if stat.S_IMODE(path.stat().st_mode) != 0o600:
        raise ConfigFileModeError(f"config must be mode 0600: {path}")

    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, dict):
        raise TypeError("config root must be a mapping")
    raw: dict[str, Any] = loaded
    base = path.parent
    raw["data_dir"] = _resolve(raw["data_dir"], base)
    raw["inventory_path"] = _resolve(raw["inventory_path"], base)
    repository = raw["script_repository"]
    if not isinstance(repository, dict):
        raise TypeError("script_repository must be a mapping")
    deploy_key = Path(repository["deploy_key_path"]).expanduser()
    if not deploy_key.is_absolute():
        raise ConfigFileModeError("deploy key path must be absolute")
    deploy_key = deploy_key.resolve()
    if stat.S_IMODE(deploy_key.stat().st_mode) != 0o600:
        raise ConfigFileModeError(f"deploy key must be mode 0600: {deploy_key}")
    repository["deploy_key_path"] = deploy_key
    return AppConfig.model_validate(raw)
