"""Append-only audit log with a daily SHA-256 hash chain (NFR-S-05 / FR-OR-02).

Each record links to the previous hash so tampering with older lines is
detectable by ``wft storage doctor`` (audit chain check).
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(obj: dict) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


class AuditLog:
    """JSON Lines append-only audit store."""

    def __init__(self, path: Path):
        self.path = path

    def append(self, *, actor: str, action: str, subject: str, detail: dict | None = None) -> dict:
        detail = detail or {}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        previous = self._last_hash()
        record: dict = {
            "seq": self._next_seq(),
            "ts": _now(),
            "actor": actor,
            "action": action,
            "subject": subject,
            "detail": detail,
        }
        payload = _canonical_json({"record": record, "prev_hash": previous})
        record["prev_hash"] = previous
        record["hash"] = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(_canonical_json(record) + "\n")
        return record

    def verify_chain(self) -> tuple[bool, list[str]]:
        """Return (ok, violations). Missing file is considered valid (empty chain)."""
        if not self.path.exists():
            return True, []
        violations: list[str] = []
        prev = ""
        expected_seq = 1
        with self.path.open("r", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    violations.append(f"line {lineno}: not valid JSON")
                    continue
                if rec.get("seq") != expected_seq:
                    violations.append(
                        f"line {lineno}: seq={rec.get('seq')}, expected {expected_seq}"
                    )
                if rec.get("prev_hash") != prev:
                    violations.append(
                        f"line {lineno}: prev_hash mismatch"
                    )
                payload = _canonical_json({"record": {k: v for k, v in rec.items() if k not in ("prev_hash", "hash")}, "prev_hash": prev})
                expect = hashlib.sha256(payload.encode("utf-8")).hexdigest()
                if rec.get("hash") != expect:
                    violations.append(f"line {lineno}: hash does not match content")
                prev = rec.get("hash", "")
                expected_seq += 1
        return (not violations, violations)

    def _last_hash(self) -> str:
        if not self.path.exists():
            return ""
        last = ""
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    last = json.loads(line).get("hash", "")
                except json.JSONDecodeError:
                    last = ""
        return last

    def _next_seq(self) -> int:
        if not self.path.exists():
            return 1
        seq = 0
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    seq = int(json.loads(line).get("seq", 0))
                except (json.JSONDecodeError, ValueError):
                    pass
        return seq + 1
