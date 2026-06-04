"""Shared best-effort serialization helpers for observability payloads."""

from __future__ import annotations

from typing import Any

from pydantic_core import to_jsonable_python


def _safe_unknown_value(value: Any) -> dict[str, str]:
    """Return a non-content-bearing marker for an unknown JSON value."""
    return {
        "python_type": type(value).__name__,
        "serialization_error": "Value is not JSON serializable.",
    }


def to_json_safe(value: Any, *, include_repr: bool = True) -> Any:
    """Convert a value into JSON-compatible data without raising on odd types.

    This helper is deliberately forgiving because observability payloads should
    not break user work. By default it preserves the historical ``repr(...)``
    fallback text for existing internal callers, so do not use the default mode
    for cache keys or security-sensitive redaction. Pass ``include_repr=False``
    when a live event should expose only safe type/error metadata.
    """
    try:
        if include_repr:
            return to_jsonable_python(value, serialize_unknown=True)
        return to_jsonable_python(
            value,
            serialize_unknown=False,
            fallback=_safe_unknown_value,
        )
    except ValueError as exc:
        fallback = {
            "python_type": type(value).__name__,
            "serialization_error": str(exc),
        }
        if include_repr:
            fallback["repr"] = repr(value)
        return fallback
