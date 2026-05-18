"""JSON-safe serialization and redaction helpers for the LangGraph adapter."""

import json
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
    except Exception as exc:
        return _serialization_fallback(value, exc)


def to_cache_identity(value: Any) -> Any:
    """Best-effort stable identity for synthetic checkpoint cache keys."""
    try:
        return to_jsonable_python(value, serialize_unknown=True)
    except Exception:
        return _cache_identity(value, seen=set())


def redact_config(value: Any) -> Any:
    """Redact obvious secret-like keys from captured config/context data."""
    return _redact_config(value, seen=set())


def _cache_identity(value: Any, *, seen: set[int]) -> Any:
    if isinstance(value, Mapping):
        value_id = id(value)
        if value_id in seen:
            return _cache_fallback(value, "cycle_detected")
        seen.add(value_id)
        try:
            try:
                items = list(value.items())
            except Exception as exc:
                return _cache_fallback(value, exc)
            return {
                _safe_key_text(key): _cache_identity(nested, seen=seen)
                for key, nested in items
            }
        finally:
            seen.discard(value_id)
    if isinstance(value, list | tuple):
        value_id = id(value)
        if value_id in seen:
            return _cache_fallback(value, "cycle_detected")
        seen.add(value_id)
        try:
            return [_cache_identity(item, seen=seen) for item in value]
        finally:
            seen.discard(value_id)
    if isinstance(value, set | frozenset):
        value_id = id(value)
        if value_id in seen:
            return _cache_fallback(value, "cycle_detected")
        seen.add(value_id)
        try:
            normalized = [_cache_identity(item, seen=seen) for item in value]
        finally:
            seen.discard(value_id)
        return sorted(normalized, key=_json_sort_key)
    try:
        return to_jsonable_python(value, serialize_unknown=True)
    except Exception as exc:
        return _cache_fallback(value, exc)


def _redact_config(value: Any, *, seen: set[int]) -> Any:
    if isinstance(value, Mapping):
        value_id = id(value)
        if value_id in seen:
            return _redaction_fallback(value, "cycle_detected")
        seen.add(value_id)
        try:
            try:
                items = list(value.items())
            except Exception as exc:
                return _redaction_fallback(value, exc)
            redacted: dict[str, Any] = {}
            for key, nested in items:
                key_text = _safe_key_text(key)
                if _is_secret_key(key_text):
                    redacted[key_text] = "[REDACTED]"
                    continue
                try:
                    redacted[key_text] = _redact_config(nested, seen=seen)
                except Exception as exc:
                    redacted[key_text] = _redaction_fallback(nested, exc)
            return redacted
        finally:
            seen.discard(value_id)
    if isinstance(value, list | tuple | set | frozenset):
        value_id = id(value)
        if value_id in seen:
            return _redaction_fallback(value, "cycle_detected")
        seen.add(value_id)
        try:
            try:
                items = list(value)
            except Exception as exc:
                return _redaction_fallback(value, exc)
            return [_redact_config(item, seen=seen) for item in items]
        finally:
            seen.discard(value_id)
    try:
        return to_jsonable_python(value, serialize_unknown=False)
    except Exception as exc:
        return _redaction_fallback(value, exc)


def _type_label(value: Any) -> str:
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"


def _safe_repr(value: Any) -> str:
    try:
        return repr(value)
    except Exception as exc:
        return f"<repr unavailable: {type(exc).__name__}: {exc}>"


def _serialization_fallback(
    value: Any, error_or_message: BaseException | str
) -> dict[str, str]:
    return {
        "repr": _safe_repr(value),
        "python_type": _type_label(value),
        "serialization_error": str(error_or_message),
    }


def _cache_fallback(
    value: Any, error_or_message: BaseException | str
) -> dict[str, str]:
    fallback = _serialization_fallback(value, error_or_message)
    fallback["serialization_error"] = "cache_identity_serialization_failed"
    return fallback


def _redaction_fallback(
    value: Any, error_or_message: BaseException | str
) -> dict[str, str]:
    return {
        "python_type": _type_label(value),
        "serialization_error": str(error_or_message),
    }


def _json_sort_key(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except Exception:
        return _safe_repr(value)


def _safe_key_text(key: Any) -> str:
    try:
        return str(key)
    except Exception:
        return f"<unprintable key {_type_label(key)}>"


def _is_secret_key(key_text: str) -> bool:
    normalized = _NON_ALNUM_PATTERN.sub("", key_text.lower())
    return any(
        secret_part in normalized for secret_part in _NORMALIZED_SECRET_KEY_PARTS
    )
