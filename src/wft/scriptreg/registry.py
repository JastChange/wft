"""Read-only script registry (Contract-12).

Full SHA-256 is the unique identity of a script version; ``display_ref``
(name@short-hash) is for humans only. Two versions of the same script name may
coexist as long as they differ in SHA-256 (AC-005). Any script whose ``risk``
is not ``read_only`` is rejected at registration (AC-008B).
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import yaml

from wft.contracts import registry as contract_registry
from wft.contracts.errors import WFTScriptRegistryError

SHA256_PATTERN_LEN = 64


@dataclass(frozen=True)
class Script:
    name: str
    path: Path
    sha256: str
    risk: str
    shell: str
    timeout_sec: int
    enabled: bool
    expected_exit_codes: tuple[int, ...] = (0,)

    @property
    def short_hash(self) -> str:
        return self.sha256[:12]

    @property
    def display_ref(self) -> str:
        return f"{self.name}@{self.short_hash}"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


class ScriptRegistry:
    def __init__(self, scripts: list[Script]):
        self.scripts = scripts
        self._by_sha = {s.sha256: s for s in scripts}

    @classmethod
    def from_file(cls, path: Path) -> "ScriptRegistry":
        return cls(load_registry_payload(path))

    def resolve(self, ref: str) -> Script:
        """Resolve a ref like ``name``, ``name@<12..64 hex>``, or ``@<sha256>``.

        A bare ``name`` is ambiguous when multiple versions coexist and is then
        rejected so callers cannot accidentally run the wrong version.
        """
        if "@" in ref:
            name, _, hashpart = ref.partition("@")
            candidates = [s for s in self.scripts if s.name == name and s.sha256.startswith(hashpart.lower())]
            if len(candidates) == 1:
                return candidates[0]
            if len(candidates) > 1:
                raise WFTScriptRegistryError(f"ambiguous script ref {ref!r}: matches multiple versions")
            raise WFTScriptRegistryError(f"unknown script version {ref!r}")
        matches = [s for s in self.scripts if s.name == ref]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise WFTScriptRegistryError(
                f"script {ref!r} has multiple versions; use name@<sha256 prefix> to disambiguate"
            )
        raise WFTScriptRegistryError(f"unknown script {ref!r}")

    def version_exists(self, sha256: str) -> bool:
        return sha256 in self._by_sha


def load_registry_payload(path: Path) -> list[Script]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise WFTScriptRegistryError(f"cannot read {path}: {exc}") from exc
    try:
        payload = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise WFTScriptRegistryError(f"{path} is not valid YAML: {exc}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("scripts"), list):
        raise WFTScriptRegistryError(f"{path} must contain a 'scripts' list")

    problems = _validate_payload(path, payload)
    if problems:
        raise WFTScriptRegistryError(f"{path}: " + "; ".join(problems[:10]))

    scripts: list[Script] = []
    base = path.resolve().parent
    boundary = _repo_root(base)
    for entry in payload["scripts"]:
        script_path = _resolve_script_path(base, boundary, entry["path"])
        declared = entry["sha256"].lower()
        actual = sha256_file(script_path)
        if actual != declared:
            raise WFTScriptRegistryError(
                f"{path}: script {entry['name']!r} SHA-256 mismatch: declared "
                f"{declared}, file {script_path} is {actual}"
            )
        # expected_exit_codes defaults to (0,) explicitly; the JSON Schema
        # default must not be relied on to materialise values into the model.
        expected = tuple(int(code) for code in entry.get("expected_exit_codes", [0]))
        scripts.append(
            Script(
                name=entry["name"],
                path=script_path,
                sha256=declared,
                risk=entry["risk"],
                shell=entry["shell"],
                timeout_sec=entry["timeout_sec"],
                enabled=bool(entry["enabled"]),
                expected_exit_codes=expected,
            )
        )
    return scripts


def _repo_root(start: Path) -> Path:
    """Nearest ancestor holding a contracts/ directory (the repo root)."""
    for parent in (start, *start.parents):
        if (parent / "contracts").is_dir():
            return parent
    return start


def _resolve_script_path(base: Path, boundary: Path, raw: str) -> Path:
    p = Path(raw)
    if not p.is_absolute():
        p = base / p
    p = p.resolve()
    # Security: keep script files inside the repository; no path escapes.
    try:
        p.relative_to(boundary)
    except ValueError as exc:
        raise WFTScriptRegistryError(
            f"script path {raw!r} escapes the repository root {boundary}"
        ) from exc
    return p


def _validate_payload(path: Path, payload: dict) -> list[str]:
    envelope = {
        "meta": {
            "schema_name": "contract-12-script-registry",
            "schema_version": "1.0.0",
            "producer": "wft.scriptreg",
            "created_at": "1970-01-01T00:00:00+00:00",
        },
        "payload": payload,
    }
    problems = contract_registry.validate_with_errors("contract-12-script-registry", envelope)

    scripts = payload["scripts"]
    by_name_sha: dict[tuple[str, str], int] = {}
    by_sha: dict[str, int] = {}
    for i, entry in enumerate(scripts):
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        sha = entry.get("sha256")
        risk = entry.get("risk")
        if name is not None and sha is not None:
            key = (name, str(sha).lower())
            if key in by_name_sha:
                problems.append(f"scripts[{i}] duplicate (name, sha256) pair {key}")
            by_name_sha[key] = i
            low = str(sha).lower()
            if low in by_sha:
                problems.append(f"scripts[{i}] sha256 {low} already used by scripts[{by_sha[low]}]")
            by_sha[low] = i
        if risk is not None and risk != "read_only":
            problems.append(
                f"scripts[{i}] risk={risk!r} is not allowed in MVP; only read_only "
                "scripts may be registered (AC-008B)"
            )
        codes = entry.get("expected_exit_codes")
        if codes is not None:
            if not isinstance(codes, list) or not codes:
                problems.append(f"scripts[{i}] expected_exit_codes must be a non-empty list")
            else:
                seen: set[int] = set()
                for code in codes:
                    if not isinstance(code, int) or isinstance(code, bool) or not 0 <= code <= 255:
                        problems.append(f"scripts[{i}] expected_exit_codes entry {code!r} must be an integer in 0..255")
                    elif code in seen:
                        problems.append(f"scripts[{i}] expected_exit_codes contains duplicate {code}")
                    else:
                        seen.add(code)
    return problems
