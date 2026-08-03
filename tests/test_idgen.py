"""ULID run_id and UUIDv7 execution/attempt ids (状态与命令契约_v0.1.md §5)."""
from __future__ import annotations

import re
import threading
import time
import uuid

import pytest

from wft.idgen import (
    _UUID7Generator,
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
    before_ms = int(time.time() * 1000)
    u = uuid.UUID(new_uuid7())
    after_ms = int(time.time() * 1000)
    embedded = int.from_bytes(u.bytes[0:6], "big")  # unix_ts_ms in bytes 0..5
    assert before_ms <= embedded <= after_ms


def test_uuid7_generator_monotonic_and_unique_under_rollback(monkeypatch) -> None:
    """Clock rollback must not produce older, repeated ids: the sequence stays
    monotonic (non-decreasing integer value) and non-repeating throughout."""
    gen = _UUID7Generator()
    clock = {"ms": 1000}

    def _fake_time():
        return clock["ms"] / 1000.0

    monkeypatch.setattr("wft.idgen.time.time", _fake_time)

    vals: list[int] = []
    for _ in range(200):  # many ids in the same millisecond
        vals.append(gen.generate().int)
    clock["ms"] -= 500  # clock rolls backwards by half a second
    for _ in range(100):
        vals.append(gen.generate().int)
    clock["ms"] += 500  # returns to the original position
    for _ in range(50):
        vals.append(gen.generate().int)

    assert vals == sorted(vals)
    assert len(set(vals)) == len(vals)


def test_uuid7_generator_concurrent_unique_and_monotonic() -> None:
    gen = _UUID7Generator()
    results: list[list[uuid.UUID]] = []
    guard = threading.Lock()
    barrier = threading.Barrier(8)

    def _worker() -> None:
        barrier.wait()
        local = [gen.generate() for _ in range(500)]
        with guard:
            results.append(local)

    threads = [threading.Thread(target=_worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    all_vals = [u.int for batch in results for u in batch]
    assert len(all_vals) == 4000
    assert len(set(all_vals)) == len(all_vals)  # no duplicates across threads
    # Each consumer sees a monotonic batch; the locked generator never issues an
    # id older than one it already handed out.
    for batch in results:
        vals = [u.int for u in batch]
        assert vals == sorted(vals)
