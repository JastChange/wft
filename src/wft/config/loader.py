"""Global WFT configuration (wft.yaml).

Configuration values reference secrets only; secret bodies must never appear
in config files (NFR-S-01).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from wft.contracts.errors import WFTConfigError


@dataclass
class WFTConfig:
    data_dir: Path = Path("data")
    logs_dir: Path = Path("logs")
    vault_dir: Path | None = None
    known_hosts_path: Path | None = None
    audit_path: Path | None = None
    inventory_path: Path | None = None
    scripts_path: Path | None = None
    scheduler_config: Path | None = None
    log_level: str = "INFO"
    extra: dict = field(default_factory=dict)

    @property
    def resolved_known_hosts(self) -> Path:
        return self.known_hosts_path or self.data_dir / "known_hosts"

    @property
    def resolved_audit(self) -> Path:
        return self.audit_path or self.logs_dir / "audit.jsonl"


def load_config(path: str | Path | None) -> WFTConfig:
    if path is None:
        return WFTConfig()
    p = Path(path)
    if not p.is_file():
        raise WFTConfigError(f"config file not found: {p}")
    try:
        raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise WFTConfigError(f"config file {p} is not valid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise WFTConfigError(f"config file {p} must contain a mapping at top level")
    cfg = WFTConfig()
    cfg.data_dir = Path(raw.get("data_dir", "data"))
    cfg.logs_dir = Path(raw.get("logs_dir", "logs"))
    cfg.log_level = str(raw.get("log_level", "INFO"))
    for field_name in ("vault_dir", "known_hosts_path", "audit_path",
                       "inventory_path", "scripts_path", "scheduler_config"):
        value = raw.get(field_name)
        cfg.extra[field_name] = value
        if value is not None:
            setattr(cfg, field_name, Path(str(value)))
    for key, value in raw.items():
        if key not in {
            "data_dir", "logs_dir", "log_level", "vault_dir", "known_hosts_path",
            "audit_path", "inventory_path", "scripts_path", "scheduler_config",
        }:
            cfg.extra[key] = value
    return cfg
