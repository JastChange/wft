"""Contract-03 error object construction (错误矩阵_v0.1.md).

Every failure is reduced to an error object whose ``class``/``category``/
``retryable`` triple comes from the global error matrix so that retry bounds
and batch aggregation stay consistent across layers.
"""
from __future__ import annotations

from wft.contracts.validate import ERROR_MATRIX

MESSAGE_CAP = 2048


def error_dict(error_class: str, message: str) -> dict:
    category, retryable = ERROR_MATRIX[error_class]
    return {
        "class": error_class,
        "category": category,
        "message": (message or error_class)[:MESSAGE_CAP],
        "retryable": retryable,
    }
