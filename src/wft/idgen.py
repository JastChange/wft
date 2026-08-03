"""Identifier generation: ULID run_id and UUIDv7 execution/attempt ids.

- ``run_id`` is a ULID (26-char Crockford base32), matching the Contract-01
  ``^[0-9A-HJKMNP-TV-Z]{26}$`` pattern (状态与命令契约_v0.1.md §5).
- ``execution_uid``/``attempt_id`` are UUIDv7 (RFC 9562): time-ordered, unique.
  A single locked generator issues them so many ids in the same millisecond and
  a backwards-moving system clock still yield strictly monotonic, unique ids.
"""
from __future__ import annotations

import os
import struct
import threading
import time
import uuid

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _encode_crockford_128(value: int) -> str:
    if value >> 128:
        raise ValueError("value exceeds 128 bits")
    chars = [_CROCKFORD[(value >> shift) & 0x1F] for shift in range(125, -1, -5)]
    return "".join(chars)


def new_run_id() -> str:
    """Return a new ULID run_id (48-bit ms timestamp + 80 random bits)."""
    ts_ms = int(time.time() * 1000)
    buf = struct.pack(">Q", ts_ms)[2:] + os.urandom(10)
    return _encode_crockford_128(int.from_bytes(buf, "big"))


def decode_run_id(run_id: str) -> int:
    """Decode a ULID string to its 128-bit integer value."""
    if len(run_id) != 26:
        raise ValueError(f"run_id must be 26 chars, got {len(run_id)}")
    value = 0
    for char in run_id:
        digit = _CROCKFORD.find(char)
        if digit < 0:
            raise ValueError(f"run_id char {char!r} is not Crockford base32")
        value = (value << 5) | digit
    return value


def run_id_timestamp_ms(run_id: str) -> int:
    """Return the millisecond timestamp embedded in a ULID run_id."""
    return decode_run_id(run_id) >> 80


class _UUID7Generator:
    """RFC 9562 UUIDv7 with a per-millisecond monotonicity counter.

    Layout: 48-bit unix_ts_ms | ver=7 | 12-bit counter | variant 10 | 62-bit
    random. The counter occupies ``rand_a`` so ids issued within the same
    millisecond are ordered and unique without relying on randomness. When the
    system clock moves backwards the last issued timestamp is held, keeping the
    sequence monotonic rather than repeating an older time. The lock makes the
    counter atomic across threads.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_ms = 0
        self._counter = 0

    def generate(self) -> uuid.UUID:
        with self._lock:
            ts_ms = int(time.time() * 1000)
            if ts_ms < self._last_ms:
                # Clock rollback: hold the last issued timestamp.
                ts_ms = self._last_ms
            if ts_ms == self._last_ms:
                self._counter += 1
                if self._counter >= (1 << 12):
                    # The counter filled the millisecond; advance the timestamp.
                    self._last_ms = ts_ms + 1
                    ts_ms = self._last_ms
                    self._counter = 0
            else:
                self._last_ms = ts_ms
                self._counter = 0
            rand_b = int.from_bytes(os.urandom(8), "big") & ((1 << 62) - 1)
            counter = self._counter
            b = bytearray(16)
            b[0:6] = struct.pack(">Q", ts_ms)[2:]
            b[6] = 0x70 | (counter >> 8)
            b[7] = counter & 0xFF
            b[8] = 0x80 | (rand_b >> 56)
            b[9:16] = (rand_b & ((1 << 56) - 1)).to_bytes(7, "big")
            return uuid.UUID(bytes=bytes(b))


_GEN = _UUID7Generator()


def new_uuid7() -> str:
    """Return a monotonic, unique UUIDv7 string (RFC 9562)."""
    return str(_GEN.generate())
