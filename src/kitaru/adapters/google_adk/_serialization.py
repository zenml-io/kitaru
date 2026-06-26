"""Best-effort serialization helpers for Google ADK payloads."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pydantic_core import to_jsonable_python

_SECRET_KEY_FRAGMENTS = (
    "api_key",
    "apikey",
    "token",
    "secret",
    "password",
    "credential",
)


def safe_repr(value: Any) -> str:
    """Return ``repr(value)`` without letting fragile ADK objects break logging."""
    try:
        return repr(value)
    except Exception:
        return f"<unrepresentable {type(value).__module__}.{type(value).__name__}>"


def to_json_safe(value: Any, *, include_raw: bool = False) -> Any:
    """Return a JSON-safe representation of an ADK payload."""
    if not include_raw:
        value = redacted(value)
    try:
        return to_jsonable_python(value, serialize_unknown=True)
    except Exception:
        return {"repr": safe_repr(value), "python_type": type(value).__name__}


def redacted(value: Any) -> Any:
    """Return ``value`` with obvious secret-looking fields replaced."""
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if any(fragment in key_text.lower() for fragment in _SECRET_KEY_FRAGMENTS):
                result[key_text] = "<redacted>"
            else:
                result[key_text] = redacted(item)
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [redacted(item) for item in value]
    return value


def object_metadata(value: Any) -> dict[str, Any]:
    """Return a small non-sensitive identity block for an arbitrary object."""
    return {
        "python_type": type(value).__name__,
        "python_module": type(value).__module__,
    }
