"""Normalize Langfuse observations into provider-neutral trace graphs."""

import hashlib
import heapq
import json
import math
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from kitaru.imports._langfuse import (
    LangfuseImportError,
    LangfuseSourceRecord,
    strict_json_loads,
)
from kitaru.imports._models import (
    ImportedObservation,
    ImportedTrace,
    ObservationKind,
    ObservationStatus,
    SourceObservationType,
    TraceCost,
    TraceIntegrity,
    TraceSource,
    TraceUsage,
)


@dataclass(frozen=True)
class NormalizedLangfuseTrace:
    """One normalized trace paired with its selected source records."""

    trace: ImportedTrace
    records: tuple[LangfuseSourceRecord, ...]


_TYPE_TO_KIND = {
    SourceObservationType.AGENT: ObservationKind.AGENT_CALL,
    SourceObservationType.CHAIN: ObservationKind.CHAIN,
    SourceObservationType.GENERATION: ObservationKind.LLM_CALL,
    SourceObservationType.RETRIEVER: ObservationKind.RETRIEVAL_CALL,
    SourceObservationType.SPAN: ObservationKind.SPAN,
    SourceObservationType.TOOL: ObservationKind.TOOL_CALL,
}


def normalize_langfuse_observations(
    rows: Iterable[Mapping[str, Any]], *, project_id: str
) -> list[ImportedTrace]:
    """Group and normalize Langfuse observation rows into trace graphs."""
    grouped: dict[str, list[tuple[int, Mapping[str, Any]]]] = defaultdict(list)
    for row_number, row in enumerate(rows, start=1):
        trace_id = _required_string(row, "traceId", "trace_id", row_number=row_number)
        grouped[trace_id].append((row_number, row))

    return _normalize_grouped_traces(grouped, project_id=project_id)


def normalize_selected_langfuse_observations(
    rows: Iterable[Mapping[str, Any]],
    *,
    project_id: str,
    trace_ids: set[str],
) -> tuple[list[ImportedTrace], int]:
    """Normalize exact traces while counting all trace identities in an export."""
    grouped: dict[str, list[tuple[int, Mapping[str, Any]]]] = defaultdict(list)
    all_trace_ids: set[str] = set()
    for row_number, row in enumerate(rows, start=1):
        trace_id = _required_string(row, "traceId", "trace_id", row_number=row_number)
        all_trace_ids.add(trace_id)
        if trace_id in trace_ids:
            grouped[trace_id].append((row_number, row))
    return (
        _normalize_grouped_traces(grouped, project_id=project_id),
        len(all_trace_ids),
    )


def normalize_langfuse_records(
    records: Iterable[LangfuseSourceRecord], *, project_id: str
) -> list[NormalizedLangfuseTrace]:
    """Normalize record-aware rows while retaining exact selected evidence."""
    grouped: dict[str, list[LangfuseSourceRecord]] = defaultdict(list)
    for record in records:
        trace_id = _required_string(
            record.row,
            "traceId",
            "trace_id",
            row_number=record.line_number,
        )
        grouped[trace_id].append(record)
    return _normalize_grouped_records(grouped, project_id=project_id)


def normalize_selected_langfuse_records(
    records: Iterable[LangfuseSourceRecord],
    *,
    project_id: str,
    trace_ids: set[str],
) -> tuple[list[NormalizedLangfuseTrace], int]:
    """Normalize exact record-aware traces and count all source identities."""
    grouped: dict[str, list[LangfuseSourceRecord]] = defaultdict(list)
    all_trace_ids: set[str] = set()
    for record in records:
        trace_id = _required_string(
            record.row,
            "traceId",
            "trace_id",
            row_number=record.line_number,
        )
        all_trace_ids.add(trace_id)
        if trace_id in trace_ids:
            grouped[trace_id].append(record)
    return (
        _normalize_grouped_records(grouped, project_id=project_id),
        len(all_trace_ids),
    )


def _normalize_grouped_records(
    grouped: Mapping[str, list[LangfuseSourceRecord]],
    *,
    project_id: str,
) -> list[NormalizedLangfuseTrace]:
    normalized: list[NormalizedLangfuseTrace] = []
    for trace_id, records in grouped.items():
        trace = _normalize_trace_safely(
            trace_id,
            [(record.line_number, record.row) for record in records],
            project_id=project_id,
        )
        normalized.append(
            NormalizedLangfuseTrace(
                trace=trace,
                records=tuple(sorted(records, key=lambda record: record.source_order)),
            )
        )
    return sorted(
        normalized,
        key=lambda item: (
            item.trace.started_at or datetime.min.astimezone(),
            item.trace.source.trace_id,
        ),
    )


def _normalize_grouped_traces(
    grouped: Mapping[str, list[tuple[int, Mapping[str, Any]]]],
    *,
    project_id: str,
) -> list[ImportedTrace]:
    traces = [
        _normalize_trace_safely(trace_id, trace_rows, project_id=project_id)
        for trace_id, trace_rows in grouped.items()
    ]
    return sorted(
        traces,
        key=lambda trace: (
            trace.started_at or datetime.min.astimezone(),
            trace.source.trace_id,
        ),
    )


def _normalize_trace_safely(
    trace_id: str,
    rows: list[tuple[int, Mapping[str, Any]]],
    *,
    project_id: str,
) -> ImportedTrace:
    try:
        return _normalize_trace(trace_id, rows, project_id=project_id)
    except LangfuseImportError:
        raise
    # Payload trees below json.loads's depth limit can still overflow
    # Python-level normalization or Pydantic serialization.
    except (RecursionError, ValueError) as exc:
        raise LangfuseImportError(
            f"Trace {trace_id!r} could not be normalized: {exc}"
        ) from exc


def _normalize_trace(
    trace_id: str,
    rows: list[tuple[int, Mapping[str, Any]]],
    *,
    project_id: str,
) -> ImportedTrace:
    observations_by_id: dict[str, ImportedObservation] = {}
    for row_number, row in rows:
        observation = _normalize_observation(row, row_number=row_number)
        existing = observations_by_id.get(observation.id)
        if existing is not None and existing != observation:
            raise LangfuseImportError(
                f"Conflicting rows use observation id {observation.id!r} "
                f"in trace {trace_id!r}."
            )
        observations_by_id[observation.id] = observation

    ordered, has_cycle = _topological_order(observations_by_id)
    observation_ids = set(observations_by_id)
    missing_parent_ids = sorted(
        {
            observation.parent_id
            for observation in ordered
            if observation.parent_id is not None
            and observation.parent_id not in observation_ids
        }
    )
    component_count = _component_count(observations_by_id)
    integrity = _classify_integrity(
        has_cycle=has_cycle,
        missing_parent_ids=missing_parent_ids,
        component_count=component_count,
    )
    root = next(
        (
            observation
            for observation in ordered
            if observation.parent_id is None
            or observation.parent_id not in observation_ids
        ),
        ordered[0],
    )
    started_at = min(observation.started_at for observation in ordered)
    ended_times = [
        observation.ended_at
        for observation in ordered
        if observation.ended_at is not None
    ]
    ended_at = max(ended_times) if len(ended_times) == len(ordered) else None
    source = TraceSource(provider="langfuse", project_id=project_id, trace_id=trace_id)
    trace_values: dict[str, Any] = {
        "source": source,
        "observations": ordered,
        "integrity": integrity,
        "name": root.name,
        "started_at": started_at,
        "ended_at": ended_at,
        "input_present": root.input_present,
        "output_present": root.output_present,
        "input": root.input,
        "output": root.output,
        "metadata": root.metadata,
        "missing_parent_ids": missing_parent_ids,
        "component_count": component_count,
    }
    digest_payload = {key: _json_value(value) for key, value in trace_values.items()}
    content_digest = hashlib.sha256(
        json.dumps(
            digest_payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()
    return ImportedTrace(**trace_values, content_digest=content_digest)


def _normalize_observation(
    row: Mapping[str, Any], *, row_number: int
) -> ImportedObservation:
    source_type_value = _required_string(row, "type", row_number=row_number).upper()
    try:
        source_type = SourceObservationType(source_type_value)
    except ValueError as exc:
        raise LangfuseImportError(
            f"Unsupported Langfuse observation type {source_type_value!r} "
            f"in row {row_number}."
        ) from exc

    try:
        return ImportedObservation(
            id=_required_string(row, "id", row_number=row_number),
            trace_id=_required_string(
                row, "traceId", "trace_id", row_number=row_number
            ),
            parent_id=_optional_string(
                _first(row, "parentObservationId", "parent_observation_id")
            ),
            name=_optional_string(row.get("name")) or source_type.value.lower(),
            source_type=source_type,
            kind=_TYPE_TO_KIND[source_type],
            started_at=_timestamp(
                _first(row, "startTime", "start_time"),
                field="start time",
                row_number=row_number,
            ),
            ended_at=_optional_timestamp(
                _first(row, "endTime", "end_time"),
                field="end time",
                row_number=row_number,
            ),
            status=_status(row),
            status_message=_optional_string(
                _first(row, "statusMessage", "status_message")
            ),
            input_present="input" in row,
            output_present="output" in row,
            input=_parse_json_string(row.get("input")),
            output=_parse_json_string(row.get("output")),
            metadata=_metadata(row.get("metadata")),
            model=_optional_string(
                _first(
                    row,
                    "providedModelName",
                    "provided_model_name",
                    "modelId",
                    "model_id",
                    "model",
                    "internalModelId",
                    "internal_model_id",
                )
            ),
            usage=_usage(row),
            cost=_cost(row),
            latency_ms=_optional_number(_first(row, "latencyMs", "latency_ms")),
        )
    except LangfuseImportError:
        raise
    except (TypeError, ValidationError, ValueError) as exc:
        raise LangfuseImportError(
            f"Invalid Langfuse observation in row {row_number}: {exc}"
        ) from exc


def _topological_order(
    observations: Mapping[str, ImportedObservation],
) -> tuple[list[ImportedObservation], bool]:
    children: dict[str, list[str]] = defaultdict(list)
    indegree = {observation_id: 0 for observation_id in observations}
    for observation in observations.values():
        if observation.parent_id in observations:
            parent_id = observation.parent_id
            assert parent_id is not None
            children[parent_id].append(observation.id)
            indegree[observation.id] += 1

    ready = [
        (observations[observation_id].started_at, observation_id)
        for observation_id, degree in indegree.items()
        if degree == 0
    ]
    heapq.heapify(ready)
    ordered_ids: list[str] = []
    while ready:
        _, observation_id = heapq.heappop(ready)
        ordered_ids.append(observation_id)
        for child_id in children[observation_id]:
            indegree[child_id] -= 1
            if indegree[child_id] == 0:
                child = observations[child_id]
                heapq.heappush(ready, (child.started_at, child.id))

    has_cycle = len(ordered_ids) != len(observations)
    if has_cycle:
        remaining = sorted(
            set(observations).difference(ordered_ids),
            key=lambda observation_id: (
                observations[observation_id].started_at,
                observation_id,
            ),
        )
        ordered_ids.extend(remaining)
    return [observations[observation_id] for observation_id in ordered_ids], has_cycle


def _component_count(observations: Mapping[str, ImportedObservation]) -> int:
    neighbors: dict[str, set[str]] = defaultdict(set)
    for observation in observations.values():
        if observation.parent_id in observations:
            parent_id = observation.parent_id
            assert parent_id is not None
            neighbors[observation.id].add(parent_id)
            neighbors[parent_id].add(observation.id)

    unseen = set(observations)
    components = 0
    while unseen:
        components += 1
        pending = [unseen.pop()]
        while pending:
            current = pending.pop()
            connected = neighbors[current].intersection(unseen)
            unseen.difference_update(connected)
            pending.extend(connected)
    return components


def _classify_integrity(
    *, has_cycle: bool, missing_parent_ids: list[str], component_count: int
) -> TraceIntegrity:
    if has_cycle:
        return TraceIntegrity.INVALID
    if component_count > 1 or len(missing_parent_ids) > 1:
        return TraceIntegrity.FRAGMENTED
    if missing_parent_ids:
        return TraceIntegrity.ROOT_OMITTED
    return TraceIntegrity.COMPLETE


def _usage(row: Mapping[str, Any]) -> TraceUsage | None:
    raw = _first(row, "usage", "usageDetails", "usage_details")
    details = dict(raw) if isinstance(raw, Mapping) else {}
    input_units = _optional_number(
        _first(details, "input", "inputTokens", "input_tokens")
    )
    output_units = _optional_number(
        _first(details, "output", "outputTokens", "output_tokens")
    )
    total_units = _optional_number(
        _first(details, "total", "totalTokens", "total_tokens")
    )
    unit = _optional_string(_first(details, "unit"))
    if all(value is None for value in (input_units, output_units, total_units, unit)):
        return None
    return TraceUsage(
        input=input_units,
        output=output_units,
        total=total_units,
        unit=unit,
        details=details,
    )


def _cost(row: Mapping[str, Any]) -> TraceCost | None:
    raw = _first(row, "costDetails", "cost_details")
    details = dict(raw) if isinstance(raw, Mapping) else {}
    input_cost = _optional_number(
        _first(row, "calculatedInputCost", "calculated_input_cost", "inputCost")
    )
    output_cost = _optional_number(
        _first(row, "calculatedOutputCost", "calculated_output_cost", "outputCost")
    )
    total_cost = _optional_number(
        _first(
            row,
            "calculatedTotalCost",
            "calculated_total_cost",
            "totalCost",
            "total_cost",
        )
    )
    if input_cost is None:
        input_cost = _optional_number(_first(details, "input", "inputCost"))
    if output_cost is None:
        output_cost = _optional_number(_first(details, "output", "outputCost"))
    if total_cost is None:
        total_cost = _optional_number(_first(details, "total", "totalCost"))
    if all(value is None for value in (input_cost, output_cost, total_cost)):
        return None
    return TraceCost(
        input=input_cost,
        output=output_cost,
        total=total_cost,
        currency=_optional_string(_first(row, "currency", "costCurrency")),
        details=details,
    )


def _status(row: Mapping[str, Any]) -> ObservationStatus:
    level = _optional_string(_first(row, "level", "status"))
    if level is not None and level.upper() in {"ERROR", "FAILED", "FAILURE"}:
        return ObservationStatus.ERROR
    if _first(row, "endTime", "end_time") is not None:
        return ObservationStatus.SUCCESS
    return ObservationStatus.UNKNOWN


def _metadata(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _parse_json_string(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return strict_json_loads(value)
    # Values that are not valid JSON (including NaN/Infinity extensions and
    # payloads too deep to decode) stay as opaque strings.
    except (RecursionError, ValueError):
        return value


def _timestamp(value: Any, *, field: str, row_number: int) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise LangfuseImportError(f"Missing {field} in row {row_number}.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LangfuseImportError(
            f"Invalid {field} {value!r} in row {row_number}."
        ) from exc
    # Langfuse's downloaded JSONL export currently emits UTC database
    # timestamps without an offset. Normalize that provider-specific form here
    # while keeping the canonical ImportedObservation contract timezone-aware.
    if parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _optional_timestamp(value: Any, *, field: str, row_number: int) -> datetime | None:
    if value is None:
        return None
    return _timestamp(value, field=field, row_number=row_number)


def _required_string(row: Mapping[str, Any], *keys: str, row_number: int) -> str:
    value = _optional_string(_first(row, *keys))
    if value is None:
        names = "/".join(keys)
        raise LangfuseImportError(f"Missing {names} in row {row_number}.")
    return value


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _optional_number(value: Any) -> float | int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        try:
            parsed = float(value)
        except ValueError:
            return None
        value = int(parsed) if parsed.is_integer() else parsed
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, (int, float)):
        return value
    return None


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _json_value(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, datetime):
        return value.isoformat()
    return value
