"""Identifier generation: ULID run_id and UUIDv7 execution/attempt ids.

- ``run_id`` is a ULID (26-char Crockford base32), matching the Contract-01
  ``^[0-9A-HJKMNP-TV-Z]{26}$`` pattern (状态与命令契约_v0.1.md §5).
- ``execution_uid``/``attempt_id`` are UUIDv7 (RFC 9562): time-ordered, unique.
  Python 3.14 ships ``uuid.uuid7``; older runtimes use a self-contained
  fallback implemented against the RFC layout.
"""
from __future__ import annotations

import os
import struct
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


def new_uuid7() -> str:
    """Return a UUIDv7 string (stdlib where available, RFC 9562 fallback else)."""
    try:
        return str(uuid.uuid7())
    except AttributeError:
        return str(_uuid7_fallback())


def _uuid7_fallback() -> uuid.UUID:
    """RFC 9562 UUIDv7 without stdlib support.

    Layout: 48-bit unix_ts_ms | ver=7 (4 bits) | rand_a (12 bits)
    | variant 10 (2 bits) | rand_b (62 bits).
    """
    ts_ms = int(time.time() * 1000)
    rand = int.from_bytes(os.urandom(10), "big")
    rand_a = rand & 0xFFF
    rand_b = (rand >> 12) & ((1 << 62) - 1)
    b = bytearray(16)
    b[0:6] = struct.pack(">Q", ts_ms)[2:]
    b[6] = 0x70 | (rand_a >> 8)
    b[7] = rand_a & 0xFF
    b[8] = 0x80 | (rand_b >> 56)
    b[9:16] = (rand_b & ((1 << 56) - 1)).to_bytes(7, "big")
    return uuid.UUID(bytes=bytes(b))
