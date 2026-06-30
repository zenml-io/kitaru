"""Best-effort serialization helpers for Google ADK payloads."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from contextlib import suppress
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


def to_cache_identity(value: Any) -> Any:
    """Return a JSON-safe cache identity without exposing secret values."""
    return to_json_safe(_cache_identity(value), include_raw=True)


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


def _cache_identity(value: Any) -> Any:
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if any(fragment in key_text.lower() for fragment in _SECRET_KEY_FRAGMENTS):
                result[key_text] = {"redacted_sha256": _secret_value_digest(item)}
            else:
                result[key_text] = _cache_identity(item)
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_cache_identity(item) for item in value]
    if isinstance(value, bytes | bytearray):
        return {"bytes_sha256": hashlib.sha256(bytes(value)).hexdigest()}
    if value is None or isinstance(value, str | int | float | bool):
        return value
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        with suppress(Exception):
            return _cache_identity(model_dump(mode="python"))
        with suppress(Exception):
            return _cache_identity(model_dump())
    return _json_or_opaque_identity(value)


def _json_or_opaque_identity(value: Any) -> Any:
    try:
        payload = to_jsonable_python(value, serialize_unknown=True)
    except Exception:
        return _opaque_identity(value)
    if isinstance(payload, str) and not isinstance(value, str):
        return _opaque_identity(value)
    return _cache_identity(payload)


def _secret_value_digest(value: Any) -> str:
    get_secret_value = getattr(value, "get_secret_value", None)
    if callable(get_secret_value):
        with suppress(Exception):
            value = get_secret_value()
    serialized = json.dumps(
        _cache_identity(value),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _opaque_identity(value: Any) -> dict[str, str]:
    return {
        "repr_sha256": hashlib.sha256(safe_repr(value).encode("utf-8")).hexdigest(),
        "python_type": type(value).__name__,
        "python_module": type(value).__module__,
    }


def object_metadata(value: Any) -> dict[str, Any]:
    """Return a small non-sensitive identity block for an arbitrary object."""
    return {
        "python_type": type(value).__name__,
        "python_module": type(value).__module__,
    }
