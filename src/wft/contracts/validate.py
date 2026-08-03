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


def _parse_version(v: object) -> tuple[int, int] | None:
    """Parse ``major.minor`` from a ``schema_version`` like ``1.1.0``."""
    if not isinstance(v, str):
        return None
    parts = v.split(".")
    try:
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0
    except ValueError:
        return None
    return major, minor


def _unsupported_major_problem(schema_version: object) -> str | None:
    """Gate every Contract-03 status on a supported schema version.

    Only major version 1 is supported; an unsupported or unparseable version is
    a semantic violation regardless of payload status, so the SUCCEEDED branch
    cannot bypass it by an early return.
    """
    parsed = _parse_version(schema_version)
    if parsed is None:
        return f"schema_version={schema_version!r} is not a supported 1.x version"
    major, _ = parsed
    if major != 1:
        return (
            f"schema_version={schema_version!r} has unsupported major version "
            f"{major}; only 1.x is supported"
        )
    return None


def _success_exit_code_problem(schema_version: object, exit_code: int) -> str | None:
    """Bind the SUCCEEDED exit-code rule to the declared schema version.

    Non-zero success codes only exist since Contract-03 1.1.0: a 1.0.x producer
    may only emit ``exit_code=0``. Higher compatible MINORs (e.g. 1.2.0) keep
    the 1.1.0 behaviour. The major gate already ran before this is called.
    """
    parsed = _parse_version(schema_version)
    if parsed is None:
        return None  # unreachable: the major gate already rejected it
    _, minor = parsed
    if exit_code != 0 and minor < 1:
        return (
            f"schema_version={schema_version!r} only permits SUCCEEDED exit_code=0; "
            "non-zero success codes require schema_version >= 1.1.0"
        )
    return None


def validate_execution_result(
    instance: object,
    *,
    expected_exit_codes: tuple[int, ...] = (0,),
) -> list[str]:
    """Validate a Contract-03 result, including the cross-contract exit-code rule.

    The declared ``meta.schema_version`` is gated for every status first:
    unsupported major versions are rejected outright. For ``SUCCEEDED`` results
    the exit-code rule is then bound to the version (1.0.x only permits
    ``exit_code=0``; non-zero success codes require ``schema_version >= 1.1.0``)
    and to the resolved script's ``expected_exit_codes`` (Contract-12).
    """
    problems = validate_contract_all("contract-03-execution-result", instance)
    if not isinstance(instance, dict):
        return problems
    payload = instance.get("payload")
    if not isinstance(payload, dict):
        return problems
    meta = instance.get("meta")
    schema_version = meta.get("schema_version") if isinstance(meta, dict) else None
    major_problem = _unsupported_major_problem(schema_version)
    if major_problem is not None:
        problems.append(major_problem)
        return problems
    if payload.get("status") != "SUCCEEDED":
        return problems
    exit_code = payload.get("exit_code")
    if not isinstance(exit_code, int):
        return problems  # the schema already rejects null/out-of-range on SUCCEEDED
    minor_problem = _success_exit_code_problem(schema_version, exit_code)
    if minor_problem is not None:
        problems.append(minor_problem)
        return problems
    if exit_code not in expected_exit_codes:
        problems.append(
            f"SUCCEEDED exit_code={exit_code!r} not in expected_exit_codes "
            f"{sorted(expected_exit_codes)} of the resolved script"
        )
    return problems
