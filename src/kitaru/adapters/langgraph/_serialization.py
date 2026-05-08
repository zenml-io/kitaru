"""JSON-safe serialization and redaction helpers for the LangGraph adapter."""

import re
from collections.abc import Mapping
from typing import Any

from pydantic_core import to_jsonable_python

_SECRET_KEY_PARTS = (
    "api-key",
    "api_key",
    "apikey",
    "auth",
    "authorization",
    "auth_token",
    "bearer",
    "client_secret",
    "credential",
    "password",
    "secret",
    "token",
    "x-api-key",
)
_NORMALIZED_SECRET_KEY_PARTS = tuple(
    re.sub(r"[^a-z0-9]+", "", secret_part.lower()) for secret_part in _SECRET_KEY_PARTS
)
_NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")


def to_json_safe(value: Any) -> Any:
    """Best-effort conversion for observability payloads."""
    try:
        return to_jsonable_python(value, serialize_unknown=True)
    except ValueError as exc:
        return {
            "repr": repr(value),
            "python_type": type(value).__name__,
            "serialization_error": str(exc),
        }


def to_cache_identity(value: Any) -> Any:
    """Best-effort stable identity for synthetic checkpoint cache keys."""
    try:
        return to_jsonable_python(value, serialize_unknown=True)
    except ValueError:
        value_type = type(value)
        return {
            "python_type": f"{value_type.__module__}.{value_type.__qualname__}",
            "serialization_error": "cache_identity_serialization_failed",
        }


def redact_config(value: Any) -> Any:
    """Redact obvious secret-like keys from captured config/context data."""
    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        for key, nested in value.items():
            key_text = str(key)
            normalized = _NON_ALNUM_PATTERN.sub("", key_text.lower())
            if any(
                secret_part in normalized
                for secret_part in _NORMALIZED_SECRET_KEY_PARTS
            ):
                redacted[key_text] = "[REDACTED]"
            else:
                redacted[key_text] = redact_config(nested)
        return redacted
    if isinstance(value, list | tuple):
        return [redact_config(item) for item in value]
    try:
        return to_jsonable_python(value, serialize_unknown=False)
    except ValueError as exc:
        value_type = type(value)
        return {
            "python_type": f"{value_type.__module__}.{value_type.__qualname__}",
            "serialization_error": str(exc),
        }
