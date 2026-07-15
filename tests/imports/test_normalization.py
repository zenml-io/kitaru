"""Tests for normalizing Langfuse observations into trace graphs."""

from datetime import UTC
from pathlib import Path

import pytest

from kitaru.imports import (
    LangfuseImportError,
    ObservationKind,
    ObservationStatus,
    TraceIntegrity,
    normalize_langfuse_observations,
    read_langfuse_jsonl,
)

FIXTURE = Path(__file__).parent / "fixtures" / "langfuse_observations.jsonl"


def _fixture_traces():
    return normalize_langfuse_observations(
        read_langfuse_jsonl(FIXTURE), project_id="langfuse-project"
    )


def test_normalizes_all_observation_types_and_payloads() -> None:
    complete = _fixture_traces()[0]

    assert [observation.kind for observation in complete.observations] == [
        ObservationKind.AGENT_CALL,
        ObservationKind.CHAIN,
        ObservationKind.LLM_CALL,
        ObservationKind.TOOL_CALL,
        ObservationKind.RETRIEVAL_CALL,
        ObservationKind.SPAN,
    ]
    assert complete.input == {"question": "synthetic fixture"}
    assert complete.observations[-1].input == "not-json"
    assert complete.observations[2].usage.total == 16
    assert complete.observations[2].cost.total == pytest.approx(0.00016)
    assert complete.observations[2].latency_ms == 1000
    assert complete.observations[3].status is ObservationStatus.ERROR


def test_classifies_complete_root_omitted_and_fragmented_graphs() -> None:
    traces = {trace.source.trace_id: trace for trace in _fixture_traces()}

    assert traces["trace-complete"].integrity is TraceIntegrity.COMPLETE
    assert traces["trace-root-omitted"].integrity is TraceIntegrity.ROOT_OMITTED
    assert traces["trace-root-omitted"].missing_parent_ids == ["external-agent"]
    assert traces["trace-fragmented"].integrity is TraceIntegrity.FRAGMENTED
    assert traces["trace-fragmented"].component_count == 2


def test_accepts_snake_case_fields() -> None:
    trace = normalize_langfuse_observations(
        [
            {
                "id": "span-1",
                "trace_id": "trace-1",
                "parent_observation_id": None,
                "type": "SPAN",
                "name": "span",
                "start_time": "2026-07-15T10:00:00Z",
                "end_time": "2026-07-15T10:00:01Z",
            }
        ],
        project_id="project-1",
    )[0]

    assert trace.source.trace_id == "trace-1"
    assert trace.observations[0].kind is ObservationKind.SPAN


def test_interprets_langfuse_export_timestamps_as_utc() -> None:
    trace = normalize_langfuse_observations(
        [
            {
                "id": "span-1",
                "traceId": "trace-1",
                "type": "SPAN",
                "name": "span",
                "startTime": "2026-07-15 10:00:00.123000",
            }
        ],
        project_id="project-1",
    )[0]

    assert trace.observations[0].started_at.tzinfo is UTC


def test_deduplicates_identical_observations() -> None:
    row = {
        "id": "span-1",
        "traceId": "trace-1",
        "type": "SPAN",
        "name": "span",
        "startTime": "2026-07-15T10:00:00Z",
    }

    trace = normalize_langfuse_observations([row, dict(row)], project_id="project-1")[0]

    assert len(trace.observations) == 1


def test_rejects_conflicting_duplicate_observations() -> None:
    row = {
        "id": "span-1",
        "traceId": "trace-1",
        "type": "SPAN",
        "name": "span",
        "startTime": "2026-07-15T10:00:00Z",
    }

    with pytest.raises(LangfuseImportError, match="Conflicting rows"):
        normalize_langfuse_observations(
            [row, {**row, "name": "changed"}], project_id="project-1"
        )


def test_cycle_is_preserved_but_marked_invalid() -> None:
    rows = [
        {
            "id": "a",
            "traceId": "trace-cycle",
            "parentObservationId": "b",
            "type": "SPAN",
            "name": "a",
            "startTime": "2026-07-15T10:00:00Z",
        },
        {
            "id": "b",
            "traceId": "trace-cycle",
            "parentObservationId": "a",
            "type": "SPAN",
            "name": "b",
            "startTime": "2026-07-15T10:00:01Z",
        },
    ]

    trace = normalize_langfuse_observations(rows, project_id="project-1")[0]

    assert trace.integrity is TraceIntegrity.INVALID
    assert [observation.id for observation in trace.observations] == ["a", "b"]


def test_digest_is_stable_across_input_order() -> None:
    rows = list(read_langfuse_jsonl(FIXTURE))[:6]

    forward = normalize_langfuse_observations(rows, project_id="project-1")[0]
    reverse = normalize_langfuse_observations(reversed(rows), project_id="project-1")[0]

    assert forward.content_digest == reverse.content_digest


def test_incomplete_observation_keeps_trace_end_time_unknown() -> None:
    trace = normalize_langfuse_observations(
        [
            {
                "id": "ended",
                "traceId": "trace-1",
                "type": "SPAN",
                "name": "ended",
                "startTime": "2026-07-15T10:00:00Z",
                "endTime": "2026-07-15T10:01:00Z",
            },
            {
                "id": "unfinished",
                "traceId": "trace-1",
                "type": "SPAN",
                "name": "unfinished",
                "startTime": "2026-07-15T10:02:00Z",
            },
        ],
        project_id="project-1",
    )[0]

    assert trace.ended_at is None
    assert trace.observations[1].status is ObservationStatus.UNKNOWN


def test_validation_error_uses_physical_export_row_number() -> None:
    rows = [
        {
            "id": "trace-a-first",
            "traceId": "trace-a",
            "type": "SPAN",
            "startTime": "2026-07-15T10:00:00Z",
        },
        {
            "id": "trace-b-first",
            "traceId": "trace-b",
            "type": "SPAN",
            "startTime": "2026-07-15T10:00:01Z",
        },
        {
            "id": "trace-a-invalid",
            "traceId": "trace-a",
            "type": "SPAN",
        },
    ]

    with pytest.raises(LangfuseImportError, match="row 3"):
        normalize_langfuse_observations(rows, project_id="project-1")
