"""Shared best-effort serialization helpers for observability payloads."""

from __future__ import annotations

from typing import Any

from pydantic_core import to_jsonable_python


def to_json_safe(value: Any) -> Any:
    """Convert a value into JSON-compatible data without raising on odd types.

    This helper is deliberately forgiving because observability payloads should
    not break user work. It may include ``repr(...)`` fallback text, so do not
    use it for cache keys or security-sensitive redaction.
    """
    try:
        return to_jsonable_python(value, serialize_unknown=True)
    except ValueError as exc:
        return {
            "repr": repr(value),
            "python_type": type(value).__name__,
            "serialization_error": str(exc),
        }
