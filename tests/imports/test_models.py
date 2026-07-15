"""Tests for canonical imported-trace data models."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from kitaru.imports import (
    ImportedObservation,
    ImportedTrace,
    ObservationKind,
    SourceObservationType,
    TraceIntegrity,
    TraceSource,
)


def _observation(**overrides: object) -> ImportedObservation:
    values: dict[str, object] = {
        "id": "observation-1",
        "trace_id": "trace-1",
        "name": "agent",
        "source_type": SourceObservationType.AGENT,
        "kind": ObservationKind.AGENT_CALL,
        "started_at": datetime(2026, 7, 15, tzinfo=UTC),
    }
    values.update(overrides)
    return ImportedObservation.model_validate(values)


def test_trace_source_identity_is_provider_scoped() -> None:
    source = TraceSource(provider="langfuse", project_id="project-1", trace_id="t")

    assert source.identity == ("langfuse", "project-1", "t")


def test_observation_rejects_naive_timestamp() -> None:
    with pytest.raises(ValidationError, match="must include a timezone"):
        _observation(started_at=datetime(2026, 7, 15))


def test_trace_rejects_duplicate_observation_ids() -> None:
    with pytest.raises(ValidationError, match="must be unique"):
        ImportedTrace(
            source=TraceSource(
                provider="langfuse", project_id="project-1", trace_id="trace-1"
            ),
            observations=[_observation(), _observation()],
            integrity=TraceIntegrity.COMPLETE,
            content_digest="0" * 64,
        )


def test_trace_rejects_observation_from_another_trace() -> None:
    with pytest.raises(ValidationError, match="belong to the source trace"):
        ImportedTrace(
            source=TraceSource(
                provider="langfuse", project_id="project-1", trace_id="trace-1"
            ),
            observations=[_observation(trace_id="trace-2")],
            integrity=TraceIntegrity.COMPLETE,
            content_digest="0" * 64,
        )
