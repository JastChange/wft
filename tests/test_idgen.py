"""ULID run_id and UUIDv7 execution/attempt ids (状态与命令契约_v0.1.md §5)."""
from __future__ import annotations

import re
import time

import pytest

from wft.idgen import (
    _encode_crockford_128,
    decode_run_id,
    new_run_id,
    new_uuid7,
    run_id_timestamp_ms,
)

_RUN_ID_RE = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


def test_run_id_is_26_char_crockford() -> None:
    for _ in range(100):
        assert _RUN_ID_RE.match(new_run_id())


def test_run_id_unique_across_many() -> None:
    ids = {new_run_id() for _ in range(1000)}
    assert len(ids) == 1000


def test_run_id_timestamp_roundtrip() -> None:
    before_ms = int(time.time() * 1000)
    rid = new_run_id()
    after_ms = int(time.time() * 1000)
    ts = run_id_timestamp_ms(rid)
    assert before_ms <= ts <= after_ms


def test_decode_run_id_roundtrip() -> None:
    rid = new_run_id()
    value = decode_run_id(rid)
    assert _encode_crockford_128(value) == rid


def test_decode_run_id_rejects_bad_length() -> None:
    with pytest.raises(ValueError):
        decode_run_id("SHORT")


def test_decode_run_id_rejects_non_crockford_char() -> None:
    with pytest.raises(ValueError):
        decode_run_id("0" * 25 + "I")  # 'I' is not in the Crockford alphabet


def test_uuid7_is_rfc9562() -> None:
    for _ in range(100):
        uid = new_uuid7()
        assert _UUID_RE.match(uid)


def test_uuid7_unique_across_many() -> None:
    ids = {new_uuid7() for _ in range(1000)}
    assert len(ids) == 1000


def test_uuid7_embedds_ms_timestamp() -> None:
    import uuid

    before_ms = int(time.time() * 1000)
    u = uuid.UUID(new_uuid7())
    after_ms = int(time.time() * 1000)
    embedded = int.from_bytes(u.bytes[0:6], "big")  # unix_ts_ms in bytes 0..5
    assert before_ms <= embedded <= after_ms
