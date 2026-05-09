"""Shared best-effort serialization helpers for internal observability payloads."""

from typing import Any

from pydantic_core import to_jsonable_python


def to_json_safe(value: Any) -> Any:
    """Best-effort conversion for observability payloads.

    This helper is deliberately forgiving so telemetry and event capture do not
    break user code. It may include ``repr(...)`` fallback text, so do not use it
    for cache keys.
    """
    try:
        return to_jsonable_python(value, serialize_unknown=True)
    except ValueError as exc:
        return {
            "repr": repr(value),
            "python_type": type(value).__name__,
            "serialization_error": str(exc),
        }
