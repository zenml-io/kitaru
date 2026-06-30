"""Map Langfuse observation rows into neutral imported replay cases."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, cast

from kitaru._replay_verify_imported_models import (
    ImportedCaseSourceRef,
    ImportedReplayCase,
    RecordedCall,
    RecordedCallKind,
    ReplayTraceContract,
    RunnerContract,
    TenantContext,
    retrieval_context_from_mapping,
)
from kitaru._replay_verify_imported_validation import dedupe, is_write_like_tool_name

RECORDED_CONTROL_UNAVAILABLE_REASON = (
    "recorded_response_control_unavailable_for_imported_langfuse_observations"
)


def cases_from_langfuse_observations(
    observation_rows: Iterable[Mapping[str, Any]],
    *,
    base_url: str | None = None,
    source_ref: str | None = None,
    partial_trace_ids: set[str] | None = None,
) -> list[ImportedReplayCase]:
    """Convert already-fetched Langfuse observation rows into neutral cases.

    The function does not contact Langfuse. Callers pass rows from a fixture,
    export, or API client. Rows are grouped by trace id, de-duplicated by
    observation id, sorted by observation start time, and then translated into
    neutral ``ImportedReplayCase`` records.
    """
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    missing_trace_rows: list[dict[str, Any]] = []
    for row in observation_rows:
        plain_row = dict(row)
        trace_id = _trace_id(plain_row)
        if trace_id is None:
            missing_trace_rows.append(plain_row)
            continue
        grouped[trace_id].append(plain_row)

    cases: list[ImportedReplayCase] = []
    for trace_id, rows in grouped.items():
        cases.append(
            _case_from_trace_rows(
                trace_id=trace_id,
                rows=rows,
                base_url=base_url,
                source_ref=source_ref,
                partial_trace_ids=partial_trace_ids or set(),
            )
        )
    for index, row in enumerate(missing_trace_rows, start=1):
        cases.append(
            _case_from_missing_trace_row(
                row=row,
                index=index,
                source_ref=source_ref,
            )
        )
    return cases


def _case_from_trace_rows(
    *,
    trace_id: str,
    rows: list[dict[str, Any]],
    base_url: str | None,
    source_ref: str | None,
    partial_trace_ids: set[str],
) -> ImportedReplayCase:
    out_of_order = _was_out_of_order(rows)
    sorted_rows, duplicate_ids = _deduplicate_rows(rows)
    # _deduplicate_rows returns a fresh list, so sorting in place is safe.
    sorted_rows.sort(key=_observation_sort_key)
    root = _root_observation(sorted_rows)
    root_metadata = _metadata_from_row(root)
    partial_ingestion = trace_id in partial_trace_ids or _metadata_marks_partial(rows)
    source_import_reasons = _source_import_reasons(
        duplicate_ids=duplicate_ids,
        out_of_order=out_of_order,
        partial_ingestion=partial_ingestion,
    )
    recorded_calls, ignored_observation_count = _recorded_calls(sorted_rows, root)
    available_tools = _available_tool_names(root_metadata)
    metadata_application_tool_names = _application_tool_names(root_metadata)
    recorded_application_tool_names = [
        call.name for call in recorded_calls if call.kind in {"tool", "retrieval"}
    ]
    application_tool_names = [
        *metadata_application_tool_names,
        *recorded_application_tool_names,
    ]
    observed_at = _observation_time(root)
    raw_source_payload = {
        "source_system": "langfuse",
        "source_import_reasons": source_import_reasons,
        "source_import_summary": {
            "raw_observation_count": len(rows),
            "deduplicated_observation_count": len(sorted_rows),
            "duplicate_observation_ids": duplicate_ids,
            "out_of_order_observations": out_of_order,
            "partial_ingestion": partial_ingestion,
            "ignored_observation_count": ignored_observation_count,
            "observation_ids": _observation_ids(sorted_rows),
        },
    }
    return ImportedReplayCase(
        case_id=_case_id(root_metadata, root, trace_id),
        source_ref=ImportedCaseSourceRef(
            source_system="langfuse",
            source_id=trace_id,
            observation_ids=[str(row_id) for row_id in _observation_ids(sorted_rows)],
            url=_trace_url(base_url, trace_id),
            observed_at=observed_at,
            raw_source_ref=source_ref,
        ),
        root_input=_root_input(_maybe_parse(root.get("input"))),
        observed_output=_maybe_parse(root.get("output")),
        recorded_calls=recorded_calls,
        trace_contract=ReplayTraceContract(
            trace_contract_version=_optional_str(
                root_metadata.get("trace_contract_version")
            ),
            app_name=_optional_str(root_metadata.get("app_name")),
            app_version=_optional_str(root_metadata.get("app_version")),
            model=_optional_str(_first_present(root_metadata, "model", "model_name")),
            prompt_version=_optional_str(root_metadata.get("prompt_version")),
            prompt_hash=_optional_str(root_metadata.get("prompt_hash")),
            available_tools=available_tools,
            application_tool_names=dedupe(application_tool_names),
            side_effect_policy=_optional_str(
                _first_present(
                    root_metadata,
                    "side_effect_policy",
                    "expected_side_effect_status",
                )
            ),
            tool_registry_version=_optional_str(
                root_metadata.get("tool_registry_version")
            ),
            recorded_response_control=_recorded_response_control(root_metadata),
            raw_config=dict(root_metadata),
        ),
        runner_contract=RunnerContract(
            entrypoint=_optional_str(
                _first_present(
                    root_metadata,
                    "local_runner_entrypoint",
                    "runner_entrypoint",
                )
            )
        ),
        tenant_context=_tenant_context(root_metadata),
        retrieval_context=retrieval_context_from_mapping(
            _maybe_parse(
                _first_present(
                    root_metadata,
                    "retrieval_metadata_json",
                    "retrieval_metadata",
                    "retrieval_context",
                )
            )
        ),
        cohort=_optional_str(root_metadata.get("cohort")),
        labels=_labels(root_metadata),
        raw_source_payload=raw_source_payload,
    )


def _case_from_missing_trace_row(
    *,
    row: dict[str, Any],
    index: int,
    source_ref: str | None,
) -> ImportedReplayCase:
    metadata = _metadata_from_row(row)
    observation_id = _observation_id(row)
    return ImportedReplayCase(
        case_id=_case_id(metadata, row, f"missing-trace-{index}"),
        source_ref=ImportedCaseSourceRef(
            source_system="langfuse",
            source_id="",
            observation_ids=[observation_id] if observation_id else [],
            observed_at=_observation_time(row),
            raw_source_ref=source_ref,
        ),
        root_input=_root_input(_maybe_parse(row.get("input"))),
        observed_output=_maybe_parse(row.get("output")),
        recorded_calls=[],
        trace_contract=ReplayTraceContract(
            trace_contract_version=_optional_str(
                metadata.get("trace_contract_version")
            ),
            app_name=_optional_str(metadata.get("app_name")),
            app_version=_optional_str(metadata.get("app_version")),
            model=_optional_str(_first_present(metadata, "model", "model_name")),
            prompt_version=_optional_str(metadata.get("prompt_version")),
            prompt_hash=_optional_str(metadata.get("prompt_hash")),
            available_tools=_available_tool_names(metadata),
            side_effect_policy=_optional_str(
                _first_present(
                    metadata,
                    "side_effect_policy",
                    "expected_side_effect_status",
                )
            ),
            raw_config=dict(metadata),
        ),
        runner_contract=RunnerContract(
            entrypoint=_optional_str(
                _first_present(
                    metadata,
                    "local_runner_entrypoint",
                    "runner_entrypoint",
                )
            )
        ),
        tenant_context=_tenant_context(metadata),
        cohort=_optional_str(metadata.get("cohort")),
        labels=_labels(metadata),
        raw_source_payload={
            "source_system": "langfuse",
            "source_import_reasons": ["missing_trace_id"],
            "source_import_summary": {
                "raw_observation_count": 1,
                "deduplicated_observation_count": 1,
                "ignored_observation_count": 0,
                "observation_ids": [observation_id] if observation_id else [],
            },
        },
    )


def _recorded_calls(
    sorted_rows: Sequence[dict[str, Any]],
    root: dict[str, Any],
) -> tuple[list[RecordedCall], int]:
    """Return recorded calls plus the count of ignored 'other'-kind rows."""
    calls: list[RecordedCall] = []
    ignored_observation_count = 0
    root_id = _observation_id(root)
    for row in sorted_rows:
        row_id = _observation_id(row)
        if row is root or (root_id and row_id == root_id):
            continue
        kind = _call_kind(row)
        if kind == "other":
            ignored_observation_count += 1
            continue
        metadata = _metadata_from_row(row)
        calls.append(
            RecordedCall(
                kind=cast(RecordedCallKind, kind),
                name=_observation_name(row),
                input_payload=_maybe_parse(row.get("input")),
                output_payload=_maybe_parse(row.get("output")),
                metadata=metadata,
                observation_id=_optional_str(row_id),
                started_at=_optional_str(_observation_time(row)),
                model=_optional_str(
                    _first_present(
                        row,
                        "providedModelName",
                        "provided_model_name",
                        "model",
                        "internalModelId",
                        "internal_model_id",
                    )
                ),
                usage=_first_present(
                    row,
                    "usageDetails",
                    "usage_details",
                    "inputUsage",
                    "input_usage",
                    "outputUsage",
                    "output_usage",
                    "totalUsage",
                    "total_usage",
                ),
                cost=_optional_float(
                    _first_present(
                        row,
                        "totalCost",
                        "total_cost",
                        "totalPrice",
                        "total_price",
                    )
                ),
                latency=_optional_float(row.get("latency")),
            )
        )
    return calls, ignored_observation_count


def _call_kind(row: Mapping[str, Any]) -> str:
    row_type = str(row.get("type") or row.get("observationType") or "").lower()
    name = _observation_name(row).lower()
    if "generation" in row_type:
        return "llm"
    if "retriever" in row_type or "retrieval" in row_type:
        return "retrieval"
    if "evaluator" in row_type:
        return "evaluator"
    if "tool" in row_type:
        return "tool"
    if name.startswith("retrieve") or name.startswith("search_"):
        return "retrieval"
    if is_write_like_tool_name(name) or name.endswith("_tool"):
        return "tool"
    return "other"


def _source_import_reasons(
    *,
    duplicate_ids: list[str],
    out_of_order: bool,
    partial_ingestion: bool,
) -> list[str]:
    reasons: list[str] = []
    if duplicate_ids:
        reasons.append("duplicate_observations_deduplicated:" + ",".join(duplicate_ids))
    if out_of_order:
        reasons.append("out_of_order_observations_sorted_before_import")
    if partial_ingestion:
        reasons.append("partial_langfuse_ingestion_after_polling_window")
    return reasons


def _metadata_marks_partial(rows: Sequence[dict[str, Any]]) -> bool:
    for row in rows:
        metadata = _metadata_from_row(row)
        raw_artifacts = metadata.get("raw_artifacts")
        if isinstance(raw_artifacts, Mapping) and raw_artifacts.get(
            "dropped_observation_to_simulate_partial_ingestion"
        ):
            return True
        if metadata.get("partial_langfuse_ingestion_after_polling_window"):
            return True
        if metadata.get("ingestion_status") == "partial":
            return True
    return False


def _deduplicate_rows(
    rows: Sequence[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    duplicate_ids: list[str] = []
    for row in rows:
        row_id = _observation_id(row)
        if row_id is not None:
            if row_id in seen:
                duplicate_ids.append(row_id)
                continue
            seen.add(row_id)
        deduped.append(row)
    return deduped, dedupe(duplicate_ids)


def _was_out_of_order(rows: Sequence[dict[str, Any]]) -> bool:
    times = [_observation_sort_key(row) for row in rows]
    return times != sorted(times)


def _root_observation(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    named_roots = [
        row
        for row in rows
        if _observation_name(row) in {"trust-boundary-case", "root", "agent"}
    ]
    if named_roots:
        return named_roots[0]
    parentless = [
        row
        for row in rows
        if _first_present(
            row,
            "parentObservationId",
            "parent_observation_id",
            "parentId",
            "parent_id",
        )
        in (None, "")
    ]
    return parentless[0] if parentless else rows[0]


def _metadata_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    metadata = _maybe_parse(row.get("metadata"))
    if isinstance(metadata, Mapping):
        return dict(metadata)
    attributes = _maybe_parse(row.get("attributes"))
    return dict(attributes) if isinstance(attributes, Mapping) else {}


def _available_tool_names(metadata: Mapping[str, Any]) -> list[str] | None:
    value = _first_present(
        metadata,
        "available_tool_names",
        "available_tools",
        "available_tools_json",
    )
    parsed = _maybe_parse(value)
    if parsed is None:
        return None
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    return None


def _application_tool_names(metadata: Mapping[str, Any]) -> list[str]:
    value = _first_present(
        metadata,
        "application_tool_names",
        "application_tool_names_json",
        "observed_tool_names",
        "observed_tool_names_json",
    )
    parsed = _maybe_parse(value)
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    return []


def _recorded_response_control(metadata: Mapping[str, Any]) -> dict[str, Any]:
    value = _maybe_parse(metadata.get("recorded_response_control"))
    if isinstance(value, Mapping):
        return dict(value)
    return {"available": False, "reason": RECORDED_CONTROL_UNAVAILABLE_REASON}


def _tenant_context(metadata: Mapping[str, Any]) -> TenantContext | None:
    tenant_fields = {
        "tenant_id": _optional_str(metadata.get("tenant_id")),
        "workspace_id": _optional_str(metadata.get("workspace_id")),
        "user_id": _optional_str(metadata.get("user_id")),
        "role": _optional_str(metadata.get("role")),
        "permission_scope": _optional_str(metadata.get("permission_scope")),
    }
    if not any(tenant_fields.values()):
        return None
    return TenantContext(**tenant_fields)


def _labels(metadata: Mapping[str, Any]) -> dict[str, str]:
    labels = _maybe_parse(metadata.get("labels"))
    if isinstance(labels, Mapping):
        return {str(key): str(value) for key, value in labels.items()}
    selected = {
        "case_kind": metadata.get("case_kind"),
        "group": metadata.get("group"),
        "broken_theme": metadata.get("broken_theme"),
    }
    return {key: str(value) for key, value in selected.items() if value is not None}


def _case_id(
    metadata: Mapping[str, Any],
    root: Mapping[str, Any],
    trace_id: str,
) -> str:
    metadata_case_id = _optional_str(metadata.get("case_id"))
    if metadata_case_id:
        return metadata_case_id
    trace_name = _optional_str(_first_present(root, "traceName", "trace_name"))
    if trace_name:
        return trace_name
    return trace_id


def _root_input(parsed_input: Any) -> Any:
    if isinstance(parsed_input, Mapping) and "root_input" in parsed_input:
        return parsed_input["root_input"]
    return parsed_input


def _trace_id(row: Mapping[str, Any]) -> str | None:
    return _optional_str(_first_present(row, "traceId", "trace_id"))


def _observation_name(row: Mapping[str, Any]) -> str:
    return str(row.get("name") or row.get("observationName") or "unknown")


def _observation_time(row: Mapping[str, Any]) -> str | None:
    return _optional_str(
        _first_present(
            row,
            "startTime",
            "start_time",
            "timestamp",
            "createdAt",
            "created_at",
        )
    )


def _observation_sort_key(row: Mapping[str, Any]) -> str:
    return _observation_time(row) or ""


def _observation_id(row: Mapping[str, Any]) -> str | None:
    return _optional_str(
        row.get("id") or row.get("observationId") or row.get("observation_id")
    )


def _observation_ids(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    return [row_id for row in rows if (row_id := _observation_id(row)) is not None]


def _trace_url(base_url: str | None, trace_id: str) -> str | None:
    if not base_url:
        return None
    return f"{base_url.rstrip('/')}/project/traces/{trace_id}"


def _first_present(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _maybe_parse(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return value


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_float(value: Any) -> float | None:
    if value is None or value == "" or isinstance(value, Mapping):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
