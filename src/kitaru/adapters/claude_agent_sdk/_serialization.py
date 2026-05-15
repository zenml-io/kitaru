"""JSON-safe serialization and redaction helpers for Claude capture."""

import dataclasses
import hashlib
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from pydantic_core import to_jsonable_python

from ._constants import ADAPTER_ID
from ._types import ClaudeRunRequest

_SECRET_FRAGMENTS = (
    "key",
    "token",
    "secret",
    "password",
    "authorization",
    "cookie",
    "anthropic",
    "openai",
)


def _to_jsonable_or_fallback(
    value: Any,
    *,
    on_error: Callable[[Any, ValueError], Any],
) -> Any:
    try:
        return to_jsonable_python(value, serialize_unknown=True)
    except ValueError as exc:
        return on_error(value, exc)


def to_json_safe(value: Any) -> Any:
    """Best-effort conversion for observability payloads."""
    return _to_jsonable_or_fallback(
        value,
        on_error=lambda original, exc: {
            "python_type": f"{type(original).__module__}.{type(original).__qualname__}",
            "serialization_error": str(exc),
        },
    )


def to_cache_identity(value: Any) -> Any:
    """Best-effort stable identity for synthetic checkpoint cache keys."""
    return _to_jsonable_or_fallback(
        value,
        on_error=lambda original, _exc: {
            "python_type": f"{type(original).__module__}.{type(original).__qualname__}",
            "name": getattr(original, "name", None),
            "serialization_error": "cache_identity_serialization_failed",
        },
    )


def redacted_options_manifest(
    options: Any,
    request: ClaudeRunRequest,
    *,
    redact: bool = True,
) -> dict[str, Any]:
    """Build a safe manifest of request and SDK option shape.

    The manifest stores configuration shape, not secrets or callable reprs. It is
    designed to answer "what was configured?" without storing credential values.
    """
    return {
        "adapter": ADAPTER_ID,
        "request": {
            "kind": request.kind,
            "prompt_sha256": hashlib.sha256(request.prompt.encode()).hexdigest(),
            "prompt_length": len(request.prompt),
            "resume_session_id": request.resume_session_id,
            "cwd": request.cwd,
            "max_turns": request.max_turns,
            "metadata_keys": sorted(request.metadata),
        },
        "options": _manifest_value(options, redact=redact),
    }


def _manifest_value(value: Any, *, redact: bool, key_hint: str | None = None) -> Any:
    if redact and key_hint is not None and _is_secret_key(key_hint):
        return "[REDACTED]"
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if callable(value):
        return _callable_manifest(value)
    if isinstance(value, Mapping):
        return {
            str(key): _manifest_value(nested, redact=redact, key_hint=str(key))
            for key, nested in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            "python_type": _python_type(value),
            "fields": {
                field.name: _manifest_value(
                    getattr(value, field.name), redact=redact, key_hint=field.name
                )
                for field in dataclasses.fields(value)
                if not field.name.startswith("_")
            },
        }
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        try:
            return _manifest_value(model_dump(mode="python"), redact=redact)
        except Exception as exc:
            return {
                "python_type": _python_type(value),
                "model_dump_error": type(exc).__name__,
            }
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        if _is_key_value_sequence(value):
            return [
                [
                    str(pair[0]),
                    _manifest_value(
                        pair[1],
                        redact=redact,
                        key_hint=str(pair[0]),
                    ),
                ]
                for pair in value
            ]
        return [_manifest_value(item, redact=redact) for item in value]
    server_name = getattr(value, "name", None)
    if server_name is not None:
        return {"python_type": _python_type(value), "name": server_name}
    return {"python_type": _python_type(value)}


def _is_key_value_sequence(value: Sequence[Any]) -> bool:
    if not value:
        return False
    return all(_is_two_item_sequence(item) for item in value)


def _is_two_item_sequence(value: Any) -> bool:
    return (
        isinstance(value, Sequence)
        and not isinstance(value, str | bytes | bytearray)
        and len(value) == 2
    )


def _is_secret_key(key: str) -> bool:
    lowered = key.lower()
    return any(fragment in lowered for fragment in _SECRET_FRAGMENTS)


def _callable_manifest(value: Any) -> dict[str, Any]:
    return {
        "module": getattr(value, "__module__", None),
        "qualname": getattr(value, "__qualname__", type(value).__qualname__),
        "configured": True,
    }


def _python_type(value: Any) -> str:
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"
