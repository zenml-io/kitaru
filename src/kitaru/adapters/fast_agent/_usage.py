"""Usage extraction for the fast-agent adapter."""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from kitaru._llm_usage import (
    build_usage_record,
    calculated_or_genai_cost_metadata,
    log_usage_record,
    token_usage_from_mapping,
)
from kitaru._serialization import to_json_safe

logger = logging.getLogger(__name__)


class FastAgentUsageSummary(BaseModel):
    """Kitaru-owned usage input for fast-agent cost calculators."""

    model_config = ConfigDict(extra="forbid")

    adapter_name: Literal["fast_agent"] = "fast_agent"
    provider: str | None = None
    model_name: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    cached_input_tokens: int | None = None
    reasoning_tokens: int | None = None
    raw_usage: dict[str, Any] | None = None


@dataclass(frozen=True)
class UsageAccumulatorCapture:
    """One usage accumulator and its turn position before a model call."""

    accumulator: Any
    start_index: int | None


@dataclass(frozen=True)
class UsageCaptureState:
    """Usage accumulator candidates captured before a model call."""

    captures: tuple[UsageAccumulatorCapture, ...]


_TOKEN_KEYS = frozenset(
    {
        "input_tokens",
        "prompt_tokens",
        "request_tokens",
        "tokens_input",
        "input_token_count",
        "prompt_token_count",
        "promptTokenCount",
        "output_tokens",
        "completion_tokens",
        "response_tokens",
        "tokens_output",
        "output_token_count",
        "candidates_token_count",
        "candidatesTokenCount",
        "total_tokens",
        "tokens_total",
        "total_token_count",
        "totalTokenCount",
    }
)
_USAGE_ATTRS = ("usage", "token_usage", "usage_metadata", "usage_details")


def begin_usage_capture(sources: Iterable[Any]) -> UsageCaptureState:
    """Capture current positions for all reachable usage accumulators."""
    captures = tuple(
        UsageAccumulatorCapture(
            accumulator=accumulator,
            start_index=_turn_count(accumulator),
        )
        for accumulator in _usage_accumulators(sources)
    )
    return UsageCaptureState(captures=captures)


def log_fast_agent_usage(
    *,
    call_name: str,
    provider: str | None,
    model_name: str | None,
    usage_state: UsageCaptureState | None,
    result: Any,
    cost_calculator: Callable[[FastAgentUsageSummary], float | None] | None,
) -> None:
    """Best-effort extract and persist of one canonical usage record.

    Missing or malformed usage is a normal preview-adapter outcome. The model
    checkpoint still exists, but no empty ``llm_usage_v1`` record is created and
    usage capture failures never change the original model result.
    """
    try:
        _log_fast_agent_usage(
            call_name=call_name,
            provider=provider,
            model_name=model_name,
            usage_state=usage_state,
            result=result,
            cost_calculator=cost_calculator,
        )
    except Exception:
        logger.debug("Failed to capture fast-agent usage metadata.", exc_info=True)


def _log_fast_agent_usage(
    *,
    call_name: str,
    provider: str | None,
    model_name: str | None,
    usage_state: UsageCaptureState | None,
    result: Any,
    cost_calculator: Callable[[FastAgentUsageSummary], float | None] | None,
) -> None:
    extracted = extract_usage(
        usage_state=usage_state,
        result=result,
        fallback_provider=provider,
        fallback_model=model_name,
    )
    if extracted is None or not _has_token_counts(extracted):
        return

    usage_payload = extracted.model_dump(mode="json", exclude_none=True)
    raw_usage = extracted.raw_usage or usage_payload
    warnings: list[str] = []
    cost_metadata = calculated_or_genai_cost_metadata(
        calculator=cost_calculator,
        calculator_usage=extracted,
        genai_provider=extracted.provider,
        genai_model=extracted.model_name,
        genai_usage=usage_payload,
        warnings=warnings,
        adapter_name="fast-agent",
        calculator_source_label="fast_agent.cost_calculator",
    )
    record = build_usage_record(
        adapter="fast_agent",
        surface="model_call",
        call_name=call_name,
        record_id=call_name,
        model=extracted.model_name,
        provider=extracted.provider,
        usage=usage_payload,
        input_tokens=extracted.input_tokens,
        output_tokens=extracted.output_tokens,
        total_tokens=extracted.total_tokens,
        cached_input_tokens=extracted.cached_input_tokens,
        reasoning_tokens=extracted.reasoning_tokens,
        raw_usage=raw_usage,
        estimated_cost_usd=cost_metadata.estimated_cost_usd,
        cost_source=cost_metadata.cost_source,
        cost_source_label=cost_metadata.cost_source_label,
        pricing_version=cost_metadata.pricing_version,
        warnings=warnings,
    )
    log_usage_record(record)


def extract_usage(
    *,
    usage_state: UsageCaptureState | None,
    result: Any,
    fallback_provider: str | None,
    fallback_model: str | None,
) -> FastAgentUsageSummary | None:
    """Return usage from the accumulator first, then known result shapes."""
    accumulator_usage = _usage_from_accumulator(
        usage_state,
        fallback_provider=fallback_provider,
        fallback_model=fallback_model,
    )
    if accumulator_usage is not None:
        return accumulator_usage

    usage_payload = _find_usage_payload(result)
    if usage_payload is None:
        return None
    return _summary_from_payload(
        usage_payload,
        provider=fallback_provider,
        model_name=fallback_model,
    )


def _usage_from_accumulator(
    usage_state: UsageCaptureState | None,
    *,
    fallback_provider: str | None,
    fallback_model: str | None,
) -> FastAgentUsageSummary | None:
    if usage_state is None:
        return None
    for capture in usage_state.captures:
        payload = _last_turn_usage(capture.accumulator, capture.start_index)
        if payload is None:
            continue
        latest_turn = _latest_turn_since(capture.accumulator, capture.start_index)
        if latest_turn is not None:
            payload = {**payload, **_turn_extra_usage(latest_turn)}
        summary = _summary_from_payload(
            payload,
            provider=_provider_from_turn(latest_turn) or fallback_provider,
            model_name=_model_from_turn(latest_turn) or fallback_model,
        )
        if summary is not None:
            return summary
    return None


def _summary_from_payload(
    payload: Mapping[str, Any],
    *,
    provider: str | None,
    model_name: str | None,
) -> FastAgentUsageSummary | None:
    normalized = token_usage_from_mapping(payload)
    if not any(
        normalized.get(key) is not None
        for key in ("input_tokens", "output_tokens", "total_tokens")
    ):
        return None
    raw_usage_candidate = payload.get("raw_usage")
    raw_payload: Mapping[str, Any] = (
        raw_usage_candidate if isinstance(raw_usage_candidate, Mapping) else payload
    )
    return FastAgentUsageSummary(
        provider=provider,
        model_name=model_name,
        input_tokens=normalized["input_tokens"],
        output_tokens=normalized["output_tokens"],
        total_tokens=normalized["total_tokens"],
        cached_input_tokens=normalized["cached_input_tokens"],
        reasoning_tokens=normalized["reasoning_tokens"],
        raw_usage=_jsonable_mapping(raw_payload),
    )


def _last_turn_usage(
    accumulator: Any,
    start_index: int | None,
) -> dict[str, Any] | None:
    last_turn_usage = _resolve_last_turn_usage()
    if last_turn_usage is None:
        return _fallback_last_turn_usage(accumulator, start_index)
    try:
        value = last_turn_usage(accumulator, start_index)
    except Exception:
        return _fallback_last_turn_usage(accumulator, start_index)
    mapping = _mapping_or_none(value)
    return mapping if mapping else _fallback_last_turn_usage(accumulator, start_index)


@lru_cache(maxsize=1)
def _resolve_last_turn_usage() -> Callable[[Any, int | None], Any] | None:
    try:
        module = importlib.import_module("fast_agent.llm.usage_tracking")
    except Exception:
        return None
    value = getattr(module, "last_turn_usage", None)
    return value if callable(value) else None


def _fallback_last_turn_usage(
    accumulator: Any,
    start_index: int | None,
) -> dict[str, Any] | None:
    turn = _latest_turn_since(accumulator, start_index)
    if turn is None:
        return None
    input_tokens = _first_value(
        turn,
        "display_input_tokens",
        "input_tokens",
        "prompt_tokens",
        "prompt_token_count",
    )
    output_tokens = _first_value(
        turn,
        "output_tokens",
        "completion_tokens",
        "candidates_token_count",
    )
    total_tokens = _first_value(turn, "total_tokens", "total_token_count")
    payload = {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
    }
    return {key: value for key, value in payload.items() if value is not None}


def _latest_turn_since(accumulator: Any, start_index: int | None) -> Any | None:
    turns = getattr(accumulator, "turns", None)
    if not isinstance(turns, list) or not turns:
        return None
    if start_index is not None and start_index >= len(turns):
        return None
    return turns[-1]


def _turn_extra_usage(turn: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    raw_usage = _first_value(turn, "raw_usage", "raw")
    if raw_usage is not None:
        payload["raw_usage"] = to_json_safe(raw_usage)
    reasoning_tokens = _first_value(turn, "reasoning_tokens", "thoughts_token_count")
    if reasoning_tokens is not None:
        payload["reasoning_tokens"] = reasoning_tokens
    cache_usage = _first_value(turn, "cache_usage")
    cached_input_tokens = _first_value(
        turn,
        "cached_input_tokens",
        "cached_prompt_tokens",
        "cache_read_tokens",
    )
    if cached_input_tokens is None and cache_usage is not None:
        cached_input_tokens = _first_value(
            cache_usage,
            "cache_read_tokens",
            "cache_hit_tokens",
        )
    if cached_input_tokens is not None:
        payload["cached_input_tokens"] = cached_input_tokens
    return payload


def _usage_accumulators(sources: Iterable[Any]) -> tuple[Any, ...]:
    accumulators: list[Any] = []
    seen: set[int] = set()
    for source in sources:
        accumulator = _usage_accumulator(source)
        if accumulator is None:
            continue
        identity = id(accumulator)
        if identity in seen:
            continue
        seen.add(identity)
        accumulators.append(accumulator)
    return tuple(accumulators)


def _usage_accumulator(source: Any) -> Any | None:
    if source is None:
        return None
    if isinstance(getattr(source, "turns", None), list):
        return source
    return getattr(source, "usage_accumulator", None)


def _turn_count(accumulator: Any | None) -> int | None:
    turns = getattr(accumulator, "turns", None)
    return len(turns) if isinstance(turns, list) else None


def _find_usage_payload(value: Any, *, depth: int = 0) -> dict[str, Any] | None:
    if value is None or depth > 4:
        return None
    mapping = _mapping_or_none(value)
    if mapping is not None:
        if _looks_like_usage(mapping):
            return mapping
        for key in _USAGE_ATTRS:
            if key in mapping:
                nested = _find_usage_payload(mapping[key], depth=depth + 1)
                if nested is not None:
                    return nested
        return None

    for attr in _USAGE_ATTRS:
        nested_value = getattr(value, attr, None)
        if nested_value is not None:
            nested = _find_usage_payload(nested_value, depth=depth + 1)
            if nested is not None:
                return nested
    return None


def _looks_like_usage(mapping: Mapping[str, Any]) -> bool:
    if any(key in mapping and mapping[key] is not None for key in _TOKEN_KEYS):
        return True
    nested_keys = ("details", "output_tokens_details", "completion_tokens_details")
    return any(isinstance(mapping.get(key), Mapping) for key in nested_keys)


def _has_token_counts(usage: FastAgentUsageSummary) -> bool:
    return any(
        value is not None
        for value in (usage.input_tokens, usage.output_tokens, usage.total_tokens)
    )


def _provider_from_turn(turn: Any | None) -> str | None:
    if turn is None:
        return None
    provider = _first_value(turn, "provider")
    if provider is None:
        return None
    value = getattr(provider, "value", provider)
    return str(value) if value is not None else None


def _model_from_turn(turn: Any | None) -> str | None:
    if turn is None:
        return None
    value = _first_value(turn, "model", "model_name")
    return str(value) if value is not None else None


def _first_value(raw: Any, *keys: str) -> Any:
    for key in keys:
        value = raw.get(key) if isinstance(raw, Mapping) else getattr(raw, key, None)
        if value is not None:
            return value
    return None


def _mapping_or_none(value: Any) -> dict[str, Any] | None:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        try:
            dumped = model_dump(mode="json")
        except TypeError:
            dumped = model_dump()
        if isinstance(dumped, Mapping):
            return {str(key): item for key, item in dumped.items()}
    return None


def _jsonable_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    jsonable = to_json_safe(value)
    if isinstance(jsonable, Mapping):
        return {str(key): item for key, item in jsonable.items()}
    return {"value": jsonable}
