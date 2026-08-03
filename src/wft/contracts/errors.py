"""Exception types used across WFT modules."""


class WFTError(Exception):
    """Base error for all WFT failures."""


class WFTContractError(WFTError):
    """A payload violated a contract's JSON Schema or semantic rules."""


class WFTConfigError(WFTError):
    """A configuration file is malformed, invalid, or self-inconsistent."""


class WFTInventoryError(WFTConfigError):
    """An inventory file failed schema or semantic validation."""


class WFTScriptRegistryError(WFTConfigError):
    """A script registry entry is invalid or its file hash does not match."""


class WFTUserError(WFTError):
    """Raised for user-facing input problems that should exit with code 2."""


class WFTIdempotencyConflict(WFTError):
    """An idempotency_key was reused with different parameters (exit 2)."""


class WFTStorageError(WFTError):
    """A storage-layer operation failed (database, blob, or outbox write)."""


class WFTExecutionError(WFTError):
    """A node execution could not be attempted (bad auth ref, bad config)."""
