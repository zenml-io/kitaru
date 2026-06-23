"""Canonical LLM usage and cost metadata helpers.

This module keeps the LLM usage contract in one place. Adapters build one
``llm_usage_v1`` record close to each provider call, and terminal execution
observation aggregates those records into execution-level metadata.
"""

from __future__ import annotations

import json
import logging
import math
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, cast

from pydantic_core import to_jsonable_python

LLM_USAGE_METADATA_KEY = "llm_usage_v1"
LLM_USAGE_SUMMARY_METADATA_KEY = "llm_usage_summary_v1"
LLM_USAGE_SCHEMA_VERSION = 1

LLM_FLAT_USAGE_RECORD_COUNT_KEY = "kitaru_llm_usage_record_count_v1"
LLM_FLAT_INCURRED_USAGE_RECORD_COUNT_KEY = "kitaru_llm_incurred_usage_record_count_v1"
LLM_FLAT_REUSED_USAGE_RECORD_COUNT_KEY = "kitaru_llm_reused_usage_record_count_v1"
LLM_FLAT_TOTAL_TOKENS_KEY = "kitaru_llm_total_tokens_v1"
LLM_FLAT_INCURRED_TOTAL_TOKENS_KEY = "kitaru_llm_incurred_total_tokens_v1"
LLM_FLAT_REUSED_TOTAL_TOKENS_KEY = "kitaru_llm_reused_total_tokens_v1"
LLM_FLAT_ACTUAL_COST_USD_KEY = "kitaru_llm_actual_cost_usd_v1"
LLM_FLAT_ESTIMATED_COST_USD_KEY = "kitaru_llm_estimated_cost_usd_v1"
LLM_FLAT_DISPLAY_COST_USD_KEY = "kitaru_llm_display_cost_usd_v1"
LLM_FLAT_RECORDS_WITHOUT_COST_COUNT_KEY = "kitaru_llm_records_without_cost_count_v1"
LLM_FLAT_INPUT_TOKENS_KEY = "kitaru_llm_input_tokens_v1"
LLM_FLAT_OUTPUT_TOKENS_KEY = "kitaru_llm_output_tokens_v1"
LLM_FLAT_INCURRED_INPUT_TOKENS_KEY = "kitaru_llm_incurred_input_tokens_v1"
LLM_FLAT_INCURRED_OUTPUT_TOKENS_KEY = "kitaru_llm_incurred_output_tokens_v1"
LLM_FLAT_REUSED_INPUT_TOKENS_KEY = "kitaru_llm_reused_input_tokens_v1"
LLM_FLAT_REUSED_OUTPUT_TOKENS_KEY = "kitaru_llm_reused_output_tokens_v1"

_FlatSummaryCoercer = Callable[[Any], int | float]
_FLAT_METADATA_FIELDS: tuple[tuple[str, str, _FlatSummaryCoercer], ...] = (
    (
        LLM_FLAT_USAGE_RECORD_COUNT_KEY,
        "usage_record_count",
        lambda value: _int_or_none(value) or 0,
    ),
    (
        LLM_FLAT_INCURRED_USAGE_RECORD_COUNT_KEY,
        "incurred_usage_record_count",
        lambda value: _int_or_none(value) or 0,
    ),
    (
        LLM_FLAT_REUSED_USAGE_RECORD_COUNT_KEY,
        "reused_usage_record_count",
        lambda value: _int_or_none(value) or 0,
    ),
    (
        LLM_FLAT_TOTAL_TOKENS_KEY,
        "total_tokens",
        lambda value: _int_or_none(value) or 0,
    ),
    (
        LLM_FLAT_INCURRED_TOTAL_TOKENS_KEY,
        "incurred_total_tokens",
        lambda value: _int_or_none(value) or 0,
    ),
    (
        LLM_FLAT_REUSED_TOTAL_TOKENS_KEY,
        "reused_total_tokens",
        lambda value: _int_or_none(value) or 0,
    ),
    (
        LLM_FLAT_ACTUAL_COST_USD_KEY,
        "actual_cost_usd",
        lambda value: _float_or_none(value) or 0.0,
    ),
    (
        LLM_FLAT_ESTIMATED_COST_USD_KEY,
        "estimated_cost_usd",
        lambda value: _float_or_none(value) or 0.0,
    ),
    (
        LLM_FLAT_DISPLAY_COST_USD_KEY,
        "display_cost_usd",
        lambda value: _float_or_none(value) or 0.0,
    ),
    (
        LLM_FLAT_RECORDS_WITHOUT_COST_COUNT_KEY,
        "records_without_cost_count",
        lambda value: _int_or_none(value) or 0,
    ),
    (
        LLM_FLAT_INPUT_TOKENS_KEY,
        "input_tokens",
        lambda value: _int_or_none(value) or 0,
    ),
    (
        LLM_FLAT_OUTPUT_TOKENS_KEY,
        "output_tokens",
        lambda value: _int_or_none(value) or 0,
    ),
    (
        LLM_FLAT_INCURRED_INPUT_TOKENS_KEY,
        "incurred_input_tokens",
        lambda value: _int_or_none(value) or 0,
    ),
    (
        LLM_FLAT_INCURRED_OUTPUT_TOKENS_KEY,
        "incurred_output_tokens",
        lambda value: _int_or_none(value) or 0,
    ),
    (
        LLM_FLAT_REUSED_INPUT_TOKENS_KEY,
        "reused_input_tokens",
        lambda value: _int_or_none(value) or 0,
    ),
    (
        LLM_FLAT_REUSED_OUTPUT_TOKENS_KEY,
        "reused_output_tokens",
        lambda value: _int_or_none(value) or 0,
    ),
)
LLM_FLAT_METADATA_KEYS = tuple(key for key, _, _ in _FLAT_METADATA_FIELDS)
_SUMMARY_FLOAT_FIELDS = frozenset(
    {
        "actual_cost_usd",
        "estimated_cost_usd",
        "display_cost_usd",
    }
)
_SUMMARY_LIST_FIELDS = ("adapters", "models", "warnings")

logger = logging.getLogger(__name__)

LLMAdapter = Literal[
    "kitaru.llm",
    "pydantic_ai",
    "openai_agents",
    "claude_agent_sdk",
    "langgraph",
    "gemini_interactions",
]
LLMSurface = Literal[
    "direct_llm",
    "model_call",
    "runner_call",
    "agent_invocation",
    "graph_call",
    "gemini_interaction",
]
LLMCostSource = Literal[
    "provider_reported",
    "calculator",
    "none",
    "calculator_error",
]
LLMStatus = Literal["completed", "failed", "interrupted"]
LLMBillingEffect = Literal["incurred", "reused_not_incurred", "unknown"]
LLMCacheStatus = Literal[
    "executed",
    "checkpoint_cache_hit",
    "replay_reused",
    "unknown",
]
_REUSED_CHECKPOINT_STATUSES = frozenset({"cached", "skipped"})
_REPLAY_REUSED_CHECKPOINT_STATUSES = frozenset(
    {"replay_reused", "reused", "reused_not_incurred"}
)


def cache_status_for_checkpoint_status(status: Any) -> LLMCacheStatus | None:
    """Return the LLM cache status implied by a raw checkpoint status."""
    raw_status = str(getattr(status, "value", status)).strip().lower()
    if raw_status in _REPLAY_REUSED_CHECKPOINT_STATUSES:
        return "replay_reused"
    if raw_status in _REUSED_CHECKPOINT_STATUSES:
        return "checkpoint_cache_hit"
    return None


def _int_or_none(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        float_value = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(float_value):
        return None
    return float_value


def coerce_cost_usd(value: Any) -> float | None:
    """Return a valid non-negative USD cost, or ``None`` when invalid."""
    cost = _float_or_none(value)
    if cost is None or cost < 0:
        return None
    return cost


@dataclass(frozen=True)
class CalculatedCostMetadata:
    """Canonical cost fields produced by an adapter cost calculator."""

    estimated_cost_usd: float | None
    cost_source: LLMCostSource
    cost_source_label: str | None


def calculated_cost_metadata(
    *,
    calculator: Callable[[Any], Any] | None,
    usage: Any | None,
    warnings: list[str],
    adapter_name: str,
    source_label: str,
) -> CalculatedCostMetadata:
    """Run an adapter cost calculator and return canonical record fields."""
    if calculator is None or usage is None:
        return CalculatedCostMetadata(
            estimated_cost_usd=None,
            cost_source="none",
            cost_source_label=None,
        )
    try:
        raw_cost = calculator(usage)
    except Exception as exc:
        warnings.append(
            f"{adapter_name} cost calculator failed; estimated cost was not "
            f"recorded: {type(exc).__name__}: {exc}"
        )
        return CalculatedCostMetadata(
            estimated_cost_usd=None,
            cost_source="calculator_error",
            cost_source_label=source_label,
        )
    cost = coerce_cost_usd(raw_cost)
    if cost is None:
        warnings.append(
            f"{adapter_name} cost calculator returned an invalid estimated cost; "
            "estimated cost was not recorded."
        )
        return CalculatedCostMetadata(
            estimated_cost_usd=None,
            cost_source="calculator_error",
            cost_source_label=source_label,
        )
    return CalculatedCostMetadata(
        estimated_cost_usd=cost,
        cost_source="calculator",
        cost_source_label=source_label,
    )


def estimate_calculated_cost_usd(
    *,
    calculator: Callable[[Any], Any] | None,
    usage: Any | None,
    warnings: list[str],
    adapter_name: str,
) -> float | None:
    """Run and validate an adapter cost calculator without failing the LLM call."""
    return calculated_cost_metadata(
        calculator=calculator,
        usage=usage,
        warnings=warnings,
        adapter_name=adapter_name,
        source_label="cost_calculator",
    ).estimated_cost_usd


def strip_usage_record_bookkeeping(record: Mapping[str, Any]) -> dict[str, Any]:
    """Return a public usage record without internal aggregation bookkeeping."""
    return {
        str(key): item for key, item in record.items() if not str(key).startswith("_")
    }


def _first_present(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _mapping_or_none(value: Any) -> dict[str, Any] | None:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    return None


def _jsonable_mapping(value: Any) -> dict[str, Any] | None:
    mapping = _mapping_or_none(value)
    if mapping is None:
        return None
    jsonable = to_jsonable_python(mapping, serialize_unknown=True)
    if isinstance(jsonable, Mapping):
        return {str(key): item for key, item in jsonable.items()}
    return {"value": jsonable}


def add_optional_token_count(total: int | None, value: Any) -> int | None:
    """Add one optional token count while preserving ``None`` for no data."""
    token_count = _int_or_none(value)
    if token_count is None:
        return total
    return (total or 0) + token_count


def token_usage_from_mapping(value: Any) -> dict[str, Any]:
    """Normalize common provider usage shapes into the canonical token block."""
    mapping = _mapping_or_none(value) or {}
    input_tokens = _int_or_none(
        _first_present(
            mapping,
            "input_tokens",
            "prompt_tokens",
            "request_tokens",
            "tokens_input",
            "input_token_count",
            "prompt_token_count",
            "promptTokenCount",
        )
    )
    output_tokens = _int_or_none(
        _first_present(
            mapping,
            "output_tokens",
            "completion_tokens",
            "response_tokens",
            "tokens_output",
            "output_token_count",
            "candidates_token_count",
            "candidatesTokenCount",
        )
    )
    total_tokens = _int_or_none(
        _first_present(
            mapping,
            "total_tokens",
            "tokens_total",
            "total_token_count",
            "totalTokenCount",
        )
    )
    if total_tokens is None and (input_tokens is not None or output_tokens is not None):
        total_tokens = (input_tokens or 0) + (output_tokens or 0)

    details = _mapping_or_none(mapping.get("details")) or {}
    output_details = _mapping_or_none(mapping.get("output_tokens_details")) or {}
    cached_input_value = _first_present(
        mapping,
        "cached_input_tokens",
        "cached_prompt_tokens",
        "cache_read_tokens",
        "cache_read_input_tokens",
        "cached_content_token_count",
        "cachedContentTokenCount",
    )
    reasoning_value = _first_present(
        mapping,
        "reasoning_tokens",
        "thoughts_token_count",
        "thoughtsTokenCount",
    )
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "cached_input_tokens": _int_or_none(
            cached_input_value
            if cached_input_value is not None
            else _first_present(details, "cached_tokens")
        ),
        "reasoning_tokens": _int_or_none(
            reasoning_value
            if reasoning_value is not None
            else _first_present(output_details, "reasoning_tokens")
        ),
        "raw": _jsonable_mapping(value),
    }


def build_usage_record(
    *,
    adapter: LLMAdapter,
    surface: LLMSurface,
    call_name: str | None = None,
    event_id: str | None = None,
    checkpoint_id: str | None = None,
    checkpoint_name: str | None = None,
    model: str | None = None,
    requested_model: str | None = None,
    resolved_model: str | None = None,
    provider: str | None = None,
    usage: Mapping[str, Any] | None = None,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    total_tokens: int | None = None,
    cached_input_tokens: int | None = None,
    reasoning_tokens: int | None = None,
    raw_usage: dict[str, Any] | None = None,
    actual_cost_usd: float | None = None,
    estimated_cost_usd: float | None = None,
    cost_source: LLMCostSource | None = None,
    cost_source_label: str | None = None,
    pricing_version: str | None = None,
    latency_ms: float | None = None,
    status: LLMStatus = "completed",
    billing_effect: LLMBillingEffect = "incurred",
    cache_status: LLMCacheStatus = "executed",
    warnings: Iterable[str] = (),
    record_id: str | None = None,
) -> dict[str, Any]:
    """Build a canonical ``llm_usage_v1`` record."""
    usage_block = token_usage_from_mapping(usage)
    usage_block["input_tokens"] = (
        input_tokens if input_tokens is not None else usage_block["input_tokens"]
    )
    usage_block["output_tokens"] = (
        output_tokens if output_tokens is not None else usage_block["output_tokens"]
    )
    usage_block["total_tokens"] = (
        total_tokens if total_tokens is not None else usage_block["total_tokens"]
    )
    usage_block["cached_input_tokens"] = (
        cached_input_tokens
        if cached_input_tokens is not None
        else usage_block["cached_input_tokens"]
    )
    usage_block["reasoning_tokens"] = (
        reasoning_tokens
        if reasoning_tokens is not None
        else usage_block["reasoning_tokens"]
    )
    if raw_usage is not None:
        usage_block["raw"] = _jsonable_mapping(raw_usage)

    actual_cost = coerce_cost_usd(actual_cost_usd)
    estimated_cost = coerce_cost_usd(estimated_cost_usd)
    if cost_source is None:
        if actual_cost is not None:
            cost_source = "provider_reported"
        elif estimated_cost is not None:
            cost_source = "calculator"
        else:
            cost_source = "none"

    stable_record_id = record_id or event_id or call_name or uuid.uuid4().hex
    return {
        "schema_version": LLM_USAGE_SCHEMA_VERSION,
        "record_id": str(stable_record_id),
        "adapter": adapter,
        "surface": surface,
        "call_name": call_name,
        "event_id": event_id,
        "checkpoint_id": checkpoint_id,
        "checkpoint_name": checkpoint_name,
        "model": model or resolved_model or requested_model,
        "requested_model": requested_model,
        "resolved_model": resolved_model,
        "provider": provider,
        "usage": usage_block,
        "cost": {
            "actual_cost_usd": actual_cost,
            "estimated_cost_usd": estimated_cost,
            "currency": "USD",
            "source": cost_source,
            "source_label": cost_source_label,
            "pricing_version": pricing_version,
        },
        "latency_ms": _float_or_none(latency_ms),
        "status": status,
        "billing_effect": billing_effect,
        "cache_status": cache_status,
        "warnings": [str(warning) for warning in warnings],
    }


def record_metadata_key(record: Mapping[str, Any]) -> str:
    """Return the nested metadata key used under ``llm_usage_v1``."""
    record_id = str(record.get("record_id") or uuid.uuid4().hex)
    event_id = record.get("event_id")
    if isinstance(event_id, str) and event_id:
        return f"{event_id}::{record_id}"
    return record_id


def usage_record_metadata(record: Mapping[str, Any]) -> dict[str, Any]:
    """Build the nested metadata payload for one canonical usage record."""
    return {LLM_USAGE_METADATA_KEY: {record_metadata_key(record): dict(record)}}


def usage_records_metadata(records: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Build one metadata payload containing multiple usage records."""
    nested: dict[str, dict[str, Any]] = {}
    for record in records:
        nested[record_metadata_key(record)] = dict(record)
    if not nested:
        return {}
    return {LLM_USAGE_METADATA_KEY: nested}


def log_usage_record_best_effort(record: Mapping[str, Any]) -> None:
    """Persist one usage record through ``kitaru.log`` when a run is active.

    Adapter code can also be called in plain unit tests or standalone scripts.
    In those cases there is no active Kitaru flow context, so usage metadata is
    simply not attached. The provider call result should still be returned.
    """
    from kitaru.errors import KitaruContextError, KitaruStateError
    from kitaru.logging import log

    try:
        log(**usage_record_metadata(record))
    except (KitaruContextError, KitaruStateError):
        logger.debug("Skipped LLM usage metadata write outside active Kitaru context.")
    except Exception:
        logger.debug("Failed to persist LLM usage metadata.", exc_info=True)


log_usage_record = log_usage_record_best_effort


_VALID_BILLING_EFFECTS = {"incurred", "reused_not_incurred", "unknown"}
_VALID_CACHE_STATUSES = {"executed", "checkpoint_cache_hit", "replay_reused", "unknown"}


def _coerce_record(value: Any) -> dict[str, Any] | None:
    record = _mapping_or_none(value)
    if record is None or record.get("schema_version") != LLM_USAGE_SCHEMA_VERSION:
        return None
    usage = _mapping_or_none(record.get("usage"))
    cost = _mapping_or_none(record.get("cost"))
    if usage is None or cost is None:
        return None

    normalized = dict(record)
    normalized["usage"] = usage
    normalized["cost"] = cost

    billing_effect = normalized.get("billing_effect")
    if billing_effect not in _VALID_BILLING_EFFECTS:
        normalized["billing_effect"] = "incurred"

    cache_status = normalized.get("cache_status")
    if cache_status not in _VALID_CACHE_STATUSES:
        normalized["cache_status"] = "executed"

    raw_warnings = normalized.get("warnings")
    if isinstance(raw_warnings, list | tuple):
        normalized["warnings"] = [str(warning) for warning in raw_warnings]
    else:
        normalized["warnings"] = []
    return normalized


def parse_usage_records(
    value: Any,
    *,
    source_attempt_id: str | None = None,
    default_checkpoint_name: str | None = None,
    reused: bool = False,
    reused_cache_status: LLMCacheStatus = "checkpoint_cache_hit",
) -> list[dict[str, Any]]:
    """Parse records from raw metadata value."""
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []

    candidates: Iterable[Any]
    if isinstance(value, Mapping):
        direct = _coerce_record(value)
        candidates = [direct] if direct is not None else value.values()
    elif isinstance(value, list | tuple):
        candidates = value
    else:
        return []

    records: list[dict[str, Any]] = []
    for candidate in candidates:
        record = _coerce_record(candidate)
        if record is None:
            continue
        normalized = dict(record)
        if (
            normalized.get("checkpoint_name") is None
            and default_checkpoint_name is not None
        ):
            normalized["checkpoint_name"] = default_checkpoint_name
        if reused:
            normalized["billing_effect"] = "reused_not_incurred"
            normalized["cache_status"] = reused_cache_status
        if source_attempt_id is not None:
            normalized["_source_attempt_id"] = source_attempt_id
        records.append(normalized)
    return records


def usage_records_from_metadata(
    metadata: Mapping[str, Any],
    *,
    source_attempt_id: str | None = None,
    default_checkpoint_name: str | None = None,
    reused: bool = False,
    reused_cache_status: LLMCacheStatus = "checkpoint_cache_hit",
) -> list[dict[str, Any]]:
    """Extract canonical usage records from one metadata mapping."""
    return parse_usage_records(
        metadata.get(LLM_USAGE_METADATA_KEY),
        source_attempt_id=source_attempt_id,
        default_checkpoint_name=default_checkpoint_name,
        reused=reused,
        reused_cache_status=reused_cache_status,
    )


def _token_or_none(record: Mapping[str, Any], key: str) -> int | None:
    usage = _mapping_or_none(record.get("usage")) or {}
    return _int_or_none(usage.get(key))


def _token(record: Mapping[str, Any], key: str) -> int:
    return _token_or_none(record, key) or 0


def _record_cost(record: Mapping[str, Any], key: str) -> float | None:
    cost = _mapping_or_none(record.get("cost")) or {}
    return coerce_cost_usd(cost.get(key))


def _round_money(value: float) -> float:
    return round(value, 10)


def empty_usage_summary() -> dict[str, Any]:
    """Return an empty v1 summary with every numeric field present."""
    return {
        "schema_version": LLM_USAGE_SCHEMA_VERSION,
        "usage_record_count": 0,
        "incurred_usage_record_count": 0,
        "reused_usage_record_count": 0,
        "total_tokens": 0,
        "incurred_total_tokens": 0,
        "reused_total_tokens": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "incurred_input_tokens": 0,
        "incurred_output_tokens": 0,
        "reused_input_tokens": 0,
        "reused_output_tokens": 0,
        "actual_cost_usd": 0.0,
        "estimated_cost_usd": 0.0,
        "display_cost_usd": 0.0,
        "records_without_cost_count": 0,
        "adapters": [],
        "models": [],
        "cost_policy": (
            "display_cost_usd uses actual_cost_usd when present for a record, "
            "otherwise estimated_cost_usd. It is an observability estimate, "
            "not an invoice."
        ),
        "warnings": [],
    }


def aggregate_usage_records(records: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Aggregate canonical records into an execution-level summary.

    Duplicate records are skipped only when they came from the same source
    attempt and have the same record identity. The same logical record ID on a
    later retry attempt is counted again because that second attempt could have
    made a second provider call.
    """
    summary = empty_usage_summary()
    adapters: set[str] = set()
    models: set[str] = set()
    warnings: list[str] = []
    seen: set[tuple[str | None, str | None, str | None]] = set()

    for raw_record in records:
        record = _coerce_record(raw_record)
        if record is None:
            continue
        dedup_key = (
            cast(str | None, raw_record.get("_source_attempt_id")),
            cast(str | None, record.get("record_id")),
            cast(str | None, record.get("event_id")),
        )
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        billing_effect = record.get("billing_effect")
        is_reused = billing_effect == "reused_not_incurred"
        is_incurred = billing_effect == "incurred"

        summary["usage_record_count"] += 1
        if is_incurred:
            summary["incurred_usage_record_count"] += 1
        if is_reused:
            summary["reused_usage_record_count"] += 1

        input_tokens = _token(record, "input_tokens")
        output_tokens = _token(record, "output_tokens")
        total_tokens = _token_or_none(record, "total_tokens")
        if total_tokens is None:
            total_tokens = input_tokens + output_tokens

        summary["input_tokens"] += input_tokens
        summary["output_tokens"] += output_tokens
        summary["total_tokens"] += total_tokens
        if is_incurred:
            summary["incurred_input_tokens"] += input_tokens
            summary["incurred_output_tokens"] += output_tokens
            summary["incurred_total_tokens"] += total_tokens
        if is_reused:
            summary["reused_input_tokens"] += input_tokens
            summary["reused_output_tokens"] += output_tokens
            summary["reused_total_tokens"] += total_tokens

        actual_cost = _record_cost(record, "actual_cost_usd")
        estimated_cost = _record_cost(record, "estimated_cost_usd")
        if not is_reused:
            if actual_cost is not None:
                summary["actual_cost_usd"] += actual_cost
            if estimated_cost is not None:
                summary["estimated_cost_usd"] += estimated_cost
            if actual_cost is not None:
                summary["display_cost_usd"] += actual_cost
            elif estimated_cost is not None:
                summary["display_cost_usd"] += estimated_cost
        if actual_cost is None and estimated_cost is None:
            summary["records_without_cost_count"] += 1

        adapter = record.get("adapter")
        if isinstance(adapter, str):
            adapters.add(adapter)
        model = record.get("model") or record.get("resolved_model")
        if isinstance(model, str):
            models.add(model)
        record_warnings = record.get("warnings")
        if isinstance(record_warnings, list):
            warnings.extend(str(warning) for warning in record_warnings)

    summary["actual_cost_usd"] = _round_money(float(summary["actual_cost_usd"]))
    summary["estimated_cost_usd"] = _round_money(float(summary["estimated_cost_usd"]))
    summary["display_cost_usd"] = _round_money(float(summary["display_cost_usd"]))
    summary["adapters"] = sorted(adapters)
    summary["models"] = sorted(models)
    summary["warnings"] = warnings
    return summary


def summary_to_flat_metadata(summary: Mapping[str, Any]) -> dict[str, int | float]:
    """Convert a rich summary into top-level numeric metadata for statistics."""
    return {
        metadata_key: coerce(summary.get(summary_key))
        for metadata_key, summary_key, coerce in _FLAT_METADATA_FIELDS
    }


def serialize_summary_for_metadata(summary: Mapping[str, Any]) -> str:
    """Serialize the rich summary as last-write-wins metadata text."""
    return json.dumps(summary, sort_keys=True, separators=(",", ":"))


def parse_usage_summary(value: Any) -> dict[str, Any] | None:
    """Parse ``llm_usage_summary_v1`` from raw execution metadata."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return None
    summary = _mapping_or_none(value)
    if summary is None or summary.get("schema_version") != LLM_USAGE_SCHEMA_VERSION:
        return None
    return summary


def _valid_summary_number(value: Any, *, is_float: bool) -> bool:
    if is_float:
        number = _float_or_none(value)
        return number is not None and number >= 0
    number = _int_or_none(value)
    return number is not None and number >= 0


def metadata_has_complete_usage_summary(metadata: Mapping[str, Any]) -> bool:
    """Return whether terminal LLM aggregation metadata is complete enough to trust."""
    summary = parse_usage_summary(metadata.get(LLM_USAGE_SUMMARY_METADATA_KEY))
    if summary is None:
        return False

    for metadata_key, summary_key, _coerce in _FLAT_METADATA_FIELDS:
        is_float = summary_key in _SUMMARY_FLOAT_FIELDS
        if not _valid_summary_number(summary.get(summary_key), is_float=is_float):
            return False
        if metadata_key not in metadata or not _valid_summary_number(
            metadata.get(metadata_key),
            is_float=is_float,
        ):
            return False

    if any(not isinstance(summary.get(key), list) for key in _SUMMARY_LIST_FIELDS):
        return False
    cost_policy = summary.get("cost_policy")
    return isinstance(cost_policy, str) and bool(cost_policy.strip())


def execution_metadata_from_records(
    records: Iterable[Mapping[str, Any]],
    *,
    include_empty_summary: bool = False,
) -> dict[str, Any]:
    """Build pipeline-run metadata from per-call records."""
    summary = aggregate_usage_records(records)
    if not include_empty_summary and summary["usage_record_count"] == 0:
        return {}

    metadata: dict[str, Any] = summary_to_flat_metadata(summary)
    metadata[LLM_USAGE_SUMMARY_METADATA_KEY] = serialize_summary_for_metadata(summary)
    return metadata
