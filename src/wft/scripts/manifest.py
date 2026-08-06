import hashlib
from pathlib import Path

import yaml
from pydantic import ValidationError

from .models import Manifest

SUPPORTED_INTERPRETERS = {"bash", "sh", "python3"}
SUPPORTED_OS = {"ubuntu-22.04", "ubuntu-24.04"}


class ManifestError(ValueError):
    """Raised when a script manifest cannot be trusted for execution."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise ManifestError(f"cannot read script: {path}") from error
    return digest.hexdigest()


def load_manifest(path: Path) -> Manifest:
    path = path.expanduser().resolve()
    root = path.parent
    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        manifest = Manifest.model_validate(loaded)
    except (OSError, yaml.YAMLError, ValidationError) as error:
        raise ManifestError(f"invalid manifest: {error}") from error

    if manifest.schema_version != "1.0":
        raise ManifestError(f"unsupported manifest schema: {manifest.schema_version}")

    script_ids = [script.id for script in manifest.scripts]
    if len(script_ids) != len(set(script_ids)):
        raise ManifestError("duplicate script id")

    for script in manifest.scripts:
        source = (root / script.path).resolve()
        if not source.is_relative_to(root):
            raise ManifestError(f"script path escapes repository: {script.id}")
        if script.interpreter not in SUPPORTED_INTERPRETERS:
            raise ManifestError(f"unsupported interpreter: {script.interpreter}")
        if script.read_only is not True:
            raise ManifestError(f"script is not declared read-only: {script.id}")
        if not script.supported_os or not set(script.supported_os) <= SUPPORTED_OS:
            raise ManifestError(f"unsupported target OS: {script.id}")
        if not script.expected_exit_codes or any(
            exit_code < 0 or exit_code > 255 for exit_code in script.expected_exit_codes
        ):
            raise ManifestError(f"invalid expected exit code: {script.id}")
        if _sha256(source) != script.sha256:
            raise ManifestError(f"sha256 mismatch: {script.id}")

    known_scripts = set(script_ids)
    plan_ids = [plan.id for plan in manifest.plans]
    if len(plan_ids) != len(set(plan_ids)):
        raise ManifestError("duplicate plan id")
    for plan in manifest.plans:
        missing = set(plan.scripts) - known_scripts
        if missing:
            raise ManifestError(f"plan {plan.id} references missing scripts: {sorted(missing)}")
    return manifest
