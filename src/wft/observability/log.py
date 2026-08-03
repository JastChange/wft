"""Structured JSON Lines logging (NFR-R-02).

Required fields: ts / level / run_id / stage / event / msg. ``run_id`` and
``stage`` are optional at the call site but recorded when supplied.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}


class StructuredLogger:
    def __init__(self, stream=None, level: str = "INFO"):
        self.stream = stream if stream is not None else sys.stderr
        self.level = LEVELS.get(level.upper(), 20)

    def _emit(self, level: str, msg: str, *, run_id=None, stage=None, event=None, **data):
        if LEVELS[level] < self.level:
            return
        record: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "run_id": run_id,
            "stage": stage,
            "event": event,
            "msg": msg,
        }
        record.update({k: v for k, v in data.items() if v is not None})
        self.stream.write(json.dumps(record, ensure_ascii=True) + "\n")
        self.stream.flush()

    def debug(self, msg, **kw):
        self._emit("DEBUG", msg, **kw)

    def info(self, msg, **kw):
        self._emit("INFO", msg, **kw)

    def warning(self, msg, **kw):
        self._emit("WARNING", msg, **kw)

    def error(self, msg, **kw):
        self._emit("ERROR", msg, **kw)

    def critical(self, msg, **kw):
        self._emit("CRITICAL", msg, **kw)


def json_lines_file(path: Path, level: str = "INFO") -> StructuredLogger:
    path.parent.mkdir(parents=True, exist_ok=True)
    return StructuredLogger(path.open("a", encoding="utf-8"), level=level)
