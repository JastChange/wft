import secrets
import time
from uuid import UUID


def new_uuid7() -> str:
    """Return an RFC 9562 UUIDv7 using Unix milliseconds and random tail bits."""
    unix_ms = (time.time_ns() // 1_000_000) & ((1 << 48) - 1)
    random_a = secrets.randbits(12)
    random_b = secrets.randbits(62)
    value = (unix_ms << 80) | (0x7 << 76) | (random_a << 64) | (0b10 << 62) | random_b
    return str(UUID(int=value))
