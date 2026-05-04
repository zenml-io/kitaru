"""Usage normalization placeholders for the OpenAI Agents SDK adapter."""

from typing import Any

from ._types import OpenAIUsageSummary


def normalize_usage(raw: Any, *, model_name: str | None = None) -> OpenAIUsageSummary:
    """Return the initial JSON-safe usage summary skeleton."""
    if isinstance(raw, dict):
        return OpenAIUsageSummary(model_name=model_name, raw=raw)
    if raw is None:
        return OpenAIUsageSummary(model_name=model_name)
    return OpenAIUsageSummary(
        model_name=model_name,
        raw={"repr": repr(raw), "python_type": type(raw).__name__},
    )
