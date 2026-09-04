#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Tests for the reusable insight generation pipeline."""

import json
import uuid
from datetime import UTC, datetime

import pytest

from kitaru.api_models.v1.session import (
    SessionDetailResponse,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru.insights import (
    INSIGHT_METADATA_KEY,
    InsightGenerationContext,
    SourceImportContext,
)
from kitaru.insights.generation import (
    AnalystPlan,
    EditorialCardCopy,
    EditorialPlan,
    InsightModelGenerator,
    ModelGenerationConfig,
    ModelStageResponse,
)
from kitaru.insights.models import GenerationMode, ProviderReceipt
from kitaru.insights.observability import GenerationEvent
from kitaru.insights.pipeline import (
    InsightGenerationConfig,
    InsightResultSizeError,
    generate_insights,
)

NOW = datetime(2026, 9, 4, tzinfo=UTC)
OWNER_ID = uuid.UUID("01990000-0000-7000-8000-000000000001")
AGENT_ID = uuid.UUID("01990000-0000-7000-8000-000000000002")
IMPORT_TASK_ID = uuid.UUID("01990000-0000-7000-8000-000000000003")


def _session(number: int, *, status: SessionStatus) -> SessionWithNodesResponse:
    session_id = uuid.UUID(f"01990000-0000-7000-8000-{100 + number:012d}")
    return SessionWithNodesResponse(
        session=SessionDetailResponse(
            id=session_id,
            owner_id=OWNER_ID,
            created=NOW,
            updated=NOW,
            agent_id=AGENT_ID,
            number=number,
            origin=SessionOrigin.IMPORTED,
            status=status,
            inputs={"message": "THAT IS WRONG!!!" if number == 1 else "please retry"},
            outputs=None,
            started_at=NOW,
            ended_at=NOW,
            metadata={},
            cost=None,
            tokens=None,
            llm_call_count=0,
            tool_call_count=0,
        ),
        nodes=[],
    )


def _context() -> InsightGenerationContext:
    return InsightGenerationContext(
        agent_id=AGENT_ID,
        agent_name="returns-agent",
        source_import=SourceImportContext(
            task_id=IMPORT_TASK_ID,
            provider="langfuse",
        ),
    )


def _node(
    *, session_id: uuid.UUID, node_id: uuid.UUID, index: int
) -> SessionNodeResponse:
    return SessionNodeResponse(
        id=node_id,
        session_id=session_id,
        index=index,
        parent_index=None,
        secondary_parent_indexes=[],
        secondary_parent_ids=[],
        node_type=NodeType.TOOL_CALL,
        name="lookup_order",
        status=NodeStatus.COMPLETED,
        inputs={},
        outputs={},
        tool_name="lookup_order",
        metadata={},
    )


class RecordingObserver:
    """Retain metadata-only pipeline lifecycle events."""

    def __init__(self) -> None:
        self.events: list[GenerationEvent] = []

    async def record(self, event: GenerationEvent) -> None:
        self.events.append(event)


def _assert_pipeline_events(observer: RecordingObserver) -> None:
    assert [event.name for event in observer.events].count("profiling") == 1
    assert [event.name for event in observer.events].count("validation") == 1
    assert len({event.run_id for event in observer.events}) == 1


class FailingEditor(InsightModelGenerator):
    """Select one known candidate, then fail editorial generation."""

    def __init__(self) -> None:
        self.selected: str | None = None

    async def analyze(self, *, projection, config, timeout_seconds):
        from kitaru.insights.generation import ModelStageResponse
        from kitaru.insights.models import ProviderReceipt

        selected = projection.candidates[-1].id
        self.selected = selected
        return ModelStageResponse(
            value=AnalystPlan(
                selected_candidate_ids=[selected],
                recommended_candidate_id=selected,
                rationale="Specific and actionable.",
            ),
            receipt=ProviderReceipt(stage="analyst", latency_ms=1, outcome="succeeded"),
        )

    async def edit(self, *, projection, config, timeout_seconds):
        raise RuntimeError("provider detail that must not escape")


class FailingAnalyst(InsightModelGenerator):
    """Fail before a candidate selection and reject any editorial call."""

    def __init__(self) -> None:
        self.editor_called = False

    async def analyze(self, *, projection, config, timeout_seconds):
        raise RuntimeError("provider detail that must not escape")

    async def edit(self, *, projection, config, timeout_seconds):
        self.editor_called = True
        raise AssertionError("editor must not run after analyst failure")


class SuccessfulGenerator(InsightModelGenerator):
    """Return valid analyst and editorial values for one candidate."""

    async def analyze(self, *, projection, config, timeout_seconds):
        selected = projection.candidates[0].id
        return ModelStageResponse(
            value=AnalystPlan(
                selected_candidate_ids=[selected],
                recommended_candidate_id=selected,
                rationale="Specific and actionable.",
            ),
            receipt=ProviderReceipt(stage="analyst", latency_ms=1, outcome="succeeded"),
        )

    async def edit(self, *, projection, config, timeout_seconds):
        selected = projection.candidates[0].id
        return ModelStageResponse(
            value=EditorialPlan(
                intro_eyebrow="Worth looking at first",
                intro_title="One pattern deserves attention",
                intro_description="Start with a focused investigation.",
                recommendation_title="Recommended next step",
                recommendation_description="Compare a focused cohort.",
                insights=[
                    EditorialCardCopy(
                        id=selected,
                        eyebrow="Agent behavior",
                        description="This pattern is worth a closer look.",
                    )
                ],
            ),
            receipt=ProviderReceipt(stage="editor", latency_ms=1, outcome="succeeded"),
        )


async def test_empty_input_returns_without_model_generation() -> None:
    observer = RecordingObserver()
    result = await generate_insights([], context=_context(), observer=observer)

    assert result.insights == []
    assert result.empty_reason == "no_eligible_candidates"
    assert result.mode is GenerationMode.DETERMINISTIC
    _assert_pipeline_events(observer)
    assert observer.events[-1].metadata["outcome"] == "empty"


async def test_deterministic_result_is_canonical_and_byte_stable() -> None:
    sessions = [
        _session(1, status=SessionStatus.FAILED),
        _session(2, status=SessionStatus.COMPLETED),
    ]

    observer = RecordingObserver()
    first = await generate_insights(sessions, context=_context(), observer=observer)
    second = await generate_insights(list(reversed(sessions)), context=_context())

    assert first.model_dump_json() == second.model_dump_json()
    assert 1 <= len(first.insights) <= 6
    assert first.mode is GenerationMode.DETERMINISTIC
    for insight in first.insights:
        metadata = first.card_metadata(insight)
        assert set(insight.metadata) == {INSIGHT_METADATA_KEY}
        assert metadata.context == _context()
        assert str(IMPORT_TASK_ID) in metadata.investigation_prompt
        assert (
            str(metadata.contributing_session_ids[0]) in metadata.investigation_prompt
        )
        chart_json = json.dumps(
            insight.data.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        )
        assert chart_json in metadata.investigation_prompt
    _assert_pipeline_events(observer)
    assert observer.events[-1].metadata["outcome"] == "deterministic"


async def test_editor_failure_preserves_analyst_selection() -> None:
    sessions = [
        _session(1, status=SessionStatus.FAILED),
        _session(2, status=SessionStatus.COMPLETED),
    ]
    generator = FailingEditor()

    observer = RecordingObserver()
    result = await generate_insights(
        sessions,
        context=_context(),
        config=InsightGenerationConfig(model=ModelGenerationConfig(model="test-model")),
        generator=generator,
        observer=observer,
    )

    assert result.mode is GenerationMode.DETERMINISTIC_FALLBACK
    assert len(result.insights) == 1
    assert result.insights[0].name == generator.selected
    assert result.diagnostics.fallback_reason == "editor_failed"
    assert "provider detail" not in result.model_dump_json()
    _assert_pipeline_events(observer)
    assert observer.events[-1].metadata["outcome"] == "deterministic_fallback"


async def test_analyst_failure_uses_stable_deterministic_selection() -> None:
    sessions = [
        _session(1, status=SessionStatus.FAILED),
        _session(2, status=SessionStatus.COMPLETED),
    ]
    generator = FailingAnalyst()

    result = await generate_insights(
        sessions,
        context=_context(),
        config=InsightGenerationConfig(model=ModelGenerationConfig(model="test-model")),
        generator=generator,
    )
    deterministic = await generate_insights(sessions, context=_context())

    assert result.mode is GenerationMode.DETERMINISTIC_FALLBACK
    assert [item.name for item in result.insights] == [
        item.name for item in deterministic.insights
    ]
    assert result.diagnostics.fallback_reason == "analyst_failed"
    assert generator.editor_called is False


async def test_model_result_uses_one_pipeline_run_id_and_final_event() -> None:
    observer = RecordingObserver()
    result = await generate_insights(
        [
            _session(1, status=SessionStatus.FAILED),
            _session(2, status=SessionStatus.COMPLETED),
        ],
        context=_context(),
        config=InsightGenerationConfig(model=ModelGenerationConfig(model="test-model")),
        generator=SuccessfulGenerator(),
        observer=observer,
    )

    assert result.mode is GenerationMode.MODEL_BACKED
    _assert_pipeline_events(observer)
    assert observer.events[-1].metadata["outcome"] == "model_backed"


async def test_model_config_requires_a_model_implementation() -> None:
    with pytest.raises(ValueError, match="model generator"):
        await generate_insights(
            [_session(1, status=SessionStatus.FAILED)],
            context=_context(),
            config=InsightGenerationConfig(
                model=ModelGenerationConfig(model="test-model")
            ),
        )


async def test_rejects_result_that_exceeds_serialized_bound() -> None:
    result = await generate_insights(
        [
            _session(1, status=SessionStatus.FAILED),
            _session(2, status=SessionStatus.COMPLETED),
        ],
        context=_context(),
        config=InsightGenerationConfig(max_result_bytes=1_000),
    )

    assert result.insights == []
    assert result.empty_reason == "serialized_result_too_large"
    assert any(
        item.dimension == "serialized_result_bytes"
        for item in result.coverage.truncations
    )


async def test_raises_when_even_empty_result_exceeds_serialized_bound() -> None:
    context = _context().model_copy(
        update={
            "agent_name": "a" * 255,
            "source_import": SourceImportContext(
                task_id=IMPORT_TASK_ID,
                provider="p" * 255,
            ),
        }
    )
    with pytest.raises(InsightResultSizeError, match="minimum valid"):
        await generate_insights(
            [],
            context=context,
            config=InsightGenerationConfig(max_result_bytes=1_000),
        )


async def test_reports_bounded_card_contribution_references() -> None:
    result = await generate_insights(
        [_session(number, status=SessionStatus.COMPLETED) for number in range(1, 13)],
        context=_context(),
        config=InsightGenerationConfig(max_contributing_sessions_per_insight=3),
    )

    assert any(
        item.dimension == "card_contributing_sessions"
        for item in result.coverage.truncations
    )
    for insight in result.insights:
        assert len(result.card_metadata(insight).contributing_session_ids) <= 3


async def test_rejects_sessions_from_another_agent() -> None:
    context = _context().model_copy(update={"agent_id": uuid.uuid4()})

    with pytest.raises(ValueError, match="context agent"):
        await generate_insights(
            [_session(1, status=SessionStatus.FAILED)], context=context
        )


@pytest.mark.parametrize("duplicate", ["session", "node_id", "node_index"])
async def test_rejects_duplicate_normalized_identities(duplicate: str) -> None:
    first = _session(1, status=SessionStatus.FAILED)
    second = _session(2, status=SessionStatus.COMPLETED)
    node_id = uuid.uuid4()
    first.nodes = [
        _node(session_id=first.session.id, node_id=node_id, index=0),
        _node(
            session_id=first.session.id,
            node_id=node_id if duplicate == "node_id" else uuid.uuid4(),
            index=0 if duplicate == "node_index" else 1,
        ),
    ]
    sessions = [first, first] if duplicate == "session" else [first, second]

    with pytest.raises(ValueError, match="unique"):
        await generate_insights(sessions, context=_context())


async def test_rejects_node_from_another_session() -> None:
    session = _session(1, status=SessionStatus.FAILED)
    session.nodes = [_node(session_id=uuid.uuid4(), node_id=uuid.uuid4(), index=0)]

    with pytest.raises(ValueError, match="enclosing session"):
        await generate_insights([session], context=_context())
