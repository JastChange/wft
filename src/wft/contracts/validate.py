"""Contract validation: JSON Schema plus the semantic rules from 数据契约设计_v0.1.md §4.

Semantic rules that cannot be expressed in JSON Schema are checked here:
- ``meta.schema_name`` must match the contract key;
- ``meta.run_id`` (when present) must match ``payload.run_id``;
- error ``class``/``category``/``retryable`` must agree with the error matrix;
- Envelope payloads must be objects and must not smuggle extra fields.
"""
from __future__ import annotations

from . import registry
from .errors import WFTContractError

# Maps each error_class to (category, retryable) as per 错误矩阵_v0.1.md.
ERROR_MATRIX: dict[str, tuple[str, bool]] = {
    "dns_failed": ("TRANSIENT", True),
    "conn_timeout": ("TRANSIENT", True),
    "conn_refused": ("TRANSIENT", True),
    "conn_reset": ("TRANSIENT", True),
    "network_unreachable": ("TRANSIENT", True),
    "auth_failed": ("PERMANENT", False),
    "secret_resolution_failed": ("PERMANENT", False),
    "host_key_unknown": ("SECURITY", False),
    "host_key_mismatch": ("SECURITY", False),
    "bastion_failed": ("TRANSIENT", True),
    "upload_failed": ("TRANSIENT", True),
    "script_integrity_failed": ("SECURITY", False),
    "exec_timeout": ("RESOURCE", True),
    "exec_nonzero": ("PERMANENT", False),
    "output_decode_failed": ("DATA", False),
    "cancelled": ("PERMANENT", False),
    "llm_timeout": ("TRANSIENT", True),
    "llm_rate_limited": ("TRANSIENT", True),
    "llm_api_error": ("TRANSIENT", True),
    "llm_invalid_output": ("DATA", True),
    "llm_unavailable": ("RESOURCE", False),
    "db_busy": ("TRANSIENT", True),
    "db_write_failed": ("DATA", True),
    "blob_write_failed": ("RESOURCE", True),
    "outbox_write_failed": ("DATA", True),
    "export_failed": ("RESOURCE", True),
    "notification_failed": ("RESOURCE", True),
    "unknown": ("DATA", False),
}


def check_semantics(key: str, instance: object) -> list[str]:
    """Return a list of human-readable semantic violations (empty when valid)."""
    problems: list[str] = []
    if not isinstance(instance, dict):
        return ["instance must be an object"]

    if key in registry.ENVELOPE_CONTRACTS:
        meta = instance.get("meta")
        payload = instance.get("payload")
        if isinstance(meta, dict):
            name = meta.get("schema_name")
            if name != key:
                problems.append(f"meta.schema_name={name!r}, expected {key!r}")
            run_id = meta.get("run_id")
            if run_id is not None and isinstance(payload, dict):
                payload_run_id = payload.get("run_id")
                if payload_run_id is not None and payload_run_id != run_id:
                    problems.append(
                        f"meta.run_id={run_id!r} != payload.run_id={payload_run_id!r}"
                    )
        if isinstance(payload, dict) and "error" in payload:
            err = payload.get("error")
            if isinstance(err, dict):
                problems.extend(_check_error(err))
    elif key == "contract-03-execution-result":
        payload = instance.get("payload")
        if isinstance(payload, dict) and isinstance(payload.get("error"), dict):
            problems.extend(_check_error(payload["error"]))

    return problems


def _check_error(err: dict) -> list[str]:
    problems: list[str] = []
    cls = err.get("class")
    if not isinstance(cls, str):
        return problems
    expected = ERROR_MATRIX.get(cls)
    if expected is None:
        problems.append(f"error.class={cls!r} is not in the global error matrix")
        return problems
    expected_category, expected_retryable = expected
    if err.get("category") != expected_category:
        problems.append(
            f"error.class={cls!r}: category={err.get('category')!r}, "
            f"expected {expected_category!r}"
        )
    if err.get("retryable") != expected_retryable:
        problems.append(
            f"error.class={cls!r}: retryable={err.get('retryable')!r}, "
            f"expected {expected_retryable!r}"
        )
    return problems


def validate_contract(key: str, instance: object) -> None:
    """Validate ``instance`` against contract ``key`` (schema + semantics).

    Raises :class:`WFTContractError` on the first failure.
    """
    problems = registry.validate_with_errors(key, instance)
    problems += check_semantics(key, instance)
    if problems:
        raise WFTContractError(f"contract {key} invalid: {problems[0]}")


def validate_contract_all(key: str, instance: object) -> list[str]:
    """Return all schema + semantic violations (empty when valid)."""
    problems = registry.validate_with_errors(key, instance)
    problems += check_semantics(key, instance)
    return problems
