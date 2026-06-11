"""JSON-safe serialization and redaction helpers for Claude capture."""

import dataclasses
import hashlib
import json
from collections.abc import Callable, Mapping, Sequence, Set
from typing import Any

from pydantic_core import to_jsonable_python

from ._constants import ADAPTER_ID
from ._sandbox_tool import kitaru_sandbox_mcp_metadata
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
_PRIMITIVE_TYPES = str | int | float | bool


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
    """Best-effort stable identity for synthetic checkpoint cache keys.

    Live Claude SDK option objects can contain process-local Python objects. This
    helper records their visible configuration shape without falling back to
    object reprs, which often contain memory addresses and would make cache keys
    change across equivalent fresh objects.
    """
    try:
        return _cache_identity(value, seen=set())
    except Exception as exc:
        return {
            "python_type": _python_type(value),
            "cache_identity_error": type(exc).__name__,
        }


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


def _cache_identity(value: Any, *, seen: set[int]) -> Any:
    if value is None or isinstance(value, _PRIMITIVE_TYPES):
        return value
    metadata = kitaru_sandbox_mcp_metadata(value)
    if metadata is not None:
        return metadata
    if callable(value):
        return _callable_manifest(value)

    value_id = id(value)
    if value_id in seen:
        return {"python_type": _python_type(value), "cycle": True}
    seen.add(value_id)
    try:
        if isinstance(value, Mapping):
            return {
                str(key): _cache_identity(nested, seen=seen)
                for key, nested in sorted(value.items(), key=lambda item: str(item[0]))
            }
        if dataclasses.is_dataclass(value) and not isinstance(value, type):
            return {
                "python_type": _python_type(value),
                "fields": {
                    field.name: _cache_identity(getattr(value, field.name), seen=seen)
                    for field in dataclasses.fields(value)
                    if not field.name.startswith("_")
                },
            }
        model_dump = getattr(value, "model_dump", None)
        if callable(model_dump):
            try:
                return {
                    "python_type": _python_type(value),
                    "fields": _cache_identity(model_dump(mode="python"), seen=seen),
                }
            except Exception as exc:
                return {
                    "python_type": _python_type(value),
                    "model_dump_error": type(exc).__name__,
                }
        if isinstance(value, Set) and not isinstance(value, str | bytes | bytearray):
            normalized_items = [_cache_identity(item, seen=seen) for item in value]
            return sorted(normalized_items, key=_stable_json_sort_key)
        if isinstance(value, Sequence) and not isinstance(
            value, str | bytes | bytearray
        ):
            return [_cache_identity(item, seen=seen) for item in value]

        fields = _public_object_fields(value, seen=seen)
        if fields:
            return {"python_type": _python_type(value), "fields": fields}
        return {"python_type": _python_type(value)}
    finally:
        seen.remove(value_id)


def _public_object_fields(value: Any, *, seen: set[int]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    instance_dict = getattr(value, "__dict__", None)
    if isinstance(instance_dict, Mapping):
        for key, nested in sorted(instance_dict.items(), key=lambda item: str(item[0])):
            key_str = str(key)
            if not key_str.startswith("_"):
                fields[key_str] = _cache_identity(nested, seen=seen)

    for slot_name in _public_slot_names(value):
        if slot_name in fields:
            continue
        try:
            slot_value = getattr(value, slot_name)
        except AttributeError:
            continue
        fields[slot_name] = _cache_identity(slot_value, seen=seen)

    server_name = getattr(value, "name", None)
    if "name" not in fields and isinstance(server_name, _PRIMITIVE_TYPES):
        fields["name"] = server_name
    return fields


def _public_slot_names(value: Any) -> list[str]:
    slot_names: list[str] = []
    for cls in type(value).__mro__:
        slots = getattr(cls, "__slots__", ())
        candidates = [slots] if isinstance(slots, str) else list(slots)
        for candidate in candidates:
            if isinstance(candidate, str) and not candidate.startswith("_"):
                slot_names.append(candidate)
    return sorted(set(slot_names))


def _stable_json_sort_key(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _manifest_value(value: Any, *, redact: bool, key_hint: str | None = None) -> Any:
    if redact and key_hint is not None and _is_secret_key(key_hint):
        return "[REDACTED]"
    if value is None or isinstance(value, _PRIMITIVE_TYPES):
        return value
    metadata = kitaru_sandbox_mcp_metadata(value)
    if metadata is not None:
        return metadata
    if callable(value):
        return _callable_manifest(value)
    if isinstance(value, Mapping):
        redact_name_value = redact and _is_secret_name_value_mapping(value)
        return {
            str(key): (
                "[REDACTED]"
                if redact_name_value and str(key).lower() == "value"
                else _manifest_value(nested, redact=redact, key_hint=str(key))
            )
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


def _is_secret_name_value_mapping(value: Mapping[Any, Any]) -> bool:
    name_value = _case_insensitive_mapping_get(value, "name")
    has_value = _case_insensitive_mapping_get(value, "value") is not _MISSING
    return has_value and name_value is not _MISSING and _is_secret_key(str(name_value))


_MISSING = object()


def _case_insensitive_mapping_get(value: Mapping[Any, Any], key: str) -> Any:
    lowered_key = key.lower()
    for candidate_key, candidate_value in value.items():
        if str(candidate_key).lower() == lowered_key:
            return candidate_value
    return _MISSING


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
