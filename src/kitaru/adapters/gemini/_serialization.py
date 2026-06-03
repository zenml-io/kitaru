"""JSON-safe serialization and redaction helpers for Gemini capture."""

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from pydantic_core import to_jsonable_python

from ._constants import ADAPTER_ID
from ._types import GeminiInteractionRequest

_SECRET_FRAGMENTS = (
    "api_key",
    "authorization",
    "x-api-key",
    "cookie",
    "token",
    "secret",
    "credential",
    "password",
    "key",
)
_PRIMITIVE_TYPES = str | int | float | bool


def to_json_safe(value: Any) -> Any:
    """Best-effort conversion for observability payloads.

    Observability must never break a durable interaction, so any serialization
    failure degrades to a typed placeholder. Real agent steps (e.g. Antigravity)
    can embed SDK models whose Pydantic serializer was never built, which raises
    a ``TypeError`` ("MockValSer object cannot be converted to SchemaSerializer")
    rather than a ``ValueError`` -- so catch broadly here.
    """
    try:
        return to_jsonable_python(value, serialize_unknown=True)
    except Exception as exc:
        return {
            "python_type": f"{type(value).__module__}.{type(value).__qualname__}",
            "serialization_error": f"{type(exc).__name__}: {exc}",
        }


def redacted_request_manifest(
    request: GeminiInteractionRequest,
    *,
    client: Any | None,
    redact: bool = True,
) -> dict[str, Any]:
    """Build a safe manifest of request and client shape."""
    input_payload = request.input
    input_json = json.dumps(to_json_safe(input_payload), sort_keys=True, default=repr)
    has_function_result = request.has_function_result_payload
    return {
        "adapter": ADAPTER_ID,
        "request": {
            "kind": request.kind,
            "input_sha256": hashlib.sha256(input_json.encode()).hexdigest()
            if input_payload is not None
            else None,
            "input_length": len(input_json) if input_payload is not None else 0,
            "model": request.model,
            "agent": request.agent,
            "previous_interaction_id": request.previous_interaction_id,
            "interaction_id": request.interaction_id,
            "function_call_id": request.function_call_id,
            "function_name": request.function_name,
            "has_function_result": has_function_result,
            "function_result_is_json_null": has_function_result
            and request.function_result_payload is None,
            "has_environment": request.environment is not None,
            "tool_count": len(request.tools),
            "has_system_instruction": request.system_instruction is not None,
            "generation_config": _manifest_value(
                request.generation_config,
                redact=redact,
                key_hint="generation_config",
            ),
            "response_format": _manifest_value(
                request.response_format,
                redact=redact,
                key_hint="response_format",
            ),
            "background": request.background,
            "store": request.store,
            "timeout_s": request.timeout_s,
            "metadata_keys": sorted(request.metadata),
        },
        "client": _manifest_value(client, redact=redact),
    }


def _manifest_value(value: Any, *, redact: bool, key_hint: str | None = None) -> Any:
    if redact and key_hint is not None and _is_secret_key(key_hint):
        return "[REDACTED]"
    if value is None or isinstance(value, _PRIMITIVE_TYPES):
        return value
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
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        try:
            return _manifest_value(model_dump(mode="python"), redact=redact)
        except Exception as exc:
            return {
                "python_type": _python_type(value),
                "model_dump_error": type(exc).__name__,
            }
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
    lowered = key.lower().replace("_", "-")
    return any(fragment in lowered for fragment in _SECRET_FRAGMENTS)


def _callable_manifest(value: Callable[..., Any]) -> dict[str, str]:
    module = getattr(value, "__module__", "")
    qualname = getattr(
        value,
        "__qualname__",
        getattr(value, "__name__", type(value).__name__),
    )
    return {
        "callable": f"{module}.{qualname}",
    }


def _python_type(value: Any) -> str:
    return f"{type(value).__module__}.{type(value).__qualname__}"
