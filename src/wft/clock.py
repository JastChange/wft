from datetime import UTC, datetime


def utc_now() -> datetime:
    return datetime.now(UTC)


def format_timestamp(value: datetime | None = None) -> str:
    current = value or utc_now()
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("timestamp must include timezone information")
    return current.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def validate_timestamp(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("timestamp must be RFC 3339") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include timezone information")
    return value
