"""Usage normalization for Google ADK payloads."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from kitaru._llm_usage import token_usage_from_mapping

from ._types import ADKUsageSummary

_GOOGLE_USAGE_ALIASES = {
    "promptTokenCount": "input_tokens",
    "candidatesTokenCount": "output_tokens",
    "totalTokenCount": "total_tokens",
    "cachedContentTokenCount": "cached_input_tokens",
    "thoughtsTokenCount": "reasoning_tokens",
}
_USAGE_FIELD_NAMES = frozenset(
    {
        "usage",
        "usage_metadata",
        "usageMetadata",
        "token_usage",
        "tokenUsage",
    }
)


def _mapping_or_none(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump(mode="python")
        return dumped if isinstance(dumped, Mapping) else None
    return None


def _int_or_none(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_adk_usage(value: Any) -> ADKUsageSummary | None:
    """Normalize a Google/ADK usage payload into ``ADKUsageSummary``."""
    mapping = _mapping_or_none(value)
    if not mapping:
        return None

    canonical = token_usage_from_mapping(mapping)
    usage_data: dict[str, Any] = {
        "input_tokens": canonical.get("input_tokens"),
        "output_tokens": canonical.get("output_tokens"),
        "total_tokens": canonical.get("total_tokens"),
    }
    for source_key, target_key in _GOOGLE_USAGE_ALIASES.items():
        if source_key in mapping:
            usage_data[target_key] = _int_or_none(mapping.get(source_key))

    model_name = mapping.get("model_name") or mapping.get("model")
    provider_name = mapping.get("provider_name") or mapping.get("provider")
    if model_name is not None:
        usage_data["model_name"] = str(model_name)
    if provider_name is not None:
        usage_data["provider_name"] = str(provider_name)
    usage_data["raw"] = dict(mapping)

    if not any(
        usage_data.get(field) is not None
        for field in (
            "input_tokens",
            "output_tokens",
            "total_tokens",
            "cached_input_tokens",
            "reasoning_tokens",
        )
    ):
        return None
    return ADKUsageSummary(**usage_data)


def usage_from_event(value: Any) -> ADKUsageSummary | None:
    """Find a usage block on an ADK event-like object."""
    mapping = _mapping_or_none(value)
    if mapping is not None:
        direct = normalize_adk_usage(mapping)
        if direct is not None:
            return direct
        for field_name in _USAGE_FIELD_NAMES:
            if field_name in mapping and (
                usage := normalize_adk_usage(mapping[field_name])
            ):
                return usage

    for attr in _USAGE_FIELD_NAMES:
        if hasattr(value, attr) and (
            usage := normalize_adk_usage(getattr(value, attr))
        ):
            return usage
    return None
