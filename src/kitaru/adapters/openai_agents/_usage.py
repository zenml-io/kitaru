"""Usage normalization for the OpenAI Agents SDK adapter."""

from collections.abc import Mapping
from typing import Any

from ._serialization import to_json_safe
from ._types import OpenAIUsageSummary


def _int_or_none(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _get_value(raw: Any, key: str) -> Any:
    if isinstance(raw, Mapping):
        return raw.get(key)
    return getattr(raw, key, None)


def _first_value(raw: Any, *keys: str) -> Any:
    for key in keys:
        value = _get_value(raw, key)
        if value is not None:
            return value
    return None


def _detail_value(raw: Any, detail_key: str, value_key: str) -> Any:
    details = _get_value(raw, detail_key)
    if details is None:
        return None
    return _get_value(details, value_key)


def _first_non_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _raw_mapping(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    json_safe = to_json_safe(raw)
    if isinstance(json_safe, Mapping):
        return {str(key): value for key, value in json_safe.items()}
    return {"repr": repr(raw), "python_type": type(raw).__name__}


def normalize_usage(raw: Any, *, model_name: str | None = None) -> OpenAIUsageSummary:
    """Normalize common OpenAI Agents usage shapes.

    The SDK may hand us a plain dictionary, a Pydantic model, or a small object
    with token attributes. This function pulls the common token names into
    stable top-level fields and preserves the raw payload for inspection.
    """
    input_tokens = _int_or_none(_first_value(raw, "input_tokens", "prompt_tokens"))
    output_tokens = _int_or_none(
        _first_value(raw, "output_tokens", "completion_tokens")
    )
    total_tokens = _int_or_none(_first_value(raw, "total_tokens"))
    if total_tokens is None and (input_tokens is not None or output_tokens is not None):
        total_tokens = (input_tokens or 0) + (output_tokens or 0)

    cached_input_tokens = _int_or_none(
        _first_non_none(
            _first_value(raw, "cached_input_tokens", "cached_prompt_tokens"),
            _detail_value(raw, "input_tokens_details", "cached_tokens"),
            _detail_value(raw, "prompt_tokens_details", "cached_tokens"),
        )
    )
    reasoning_tokens = _int_or_none(
        _first_non_none(
            _first_value(raw, "reasoning_tokens"),
            _detail_value(raw, "output_tokens_details", "reasoning_tokens"),
            _detail_value(raw, "completion_tokens_details", "reasoning_tokens"),
        )
    )

    return OpenAIUsageSummary(
        model_name=model_name,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total_tokens,
        cached_input_tokens=cached_input_tokens,
        reasoning_tokens=reasoning_tokens,
        raw=_raw_mapping(raw),
    )
