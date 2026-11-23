#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Recommendation relevance and tool-telemetry coverage regressions."""

from datetime import UTC, datetime, timedelta
from unittest.mock import create_autospec
from uuid import UUID

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
from kitaru_post_import_insights.generation import (
    InsightModelGenerator,
    ModelGenerationConfig,
    generate_deterministic_plan,
)
from kitaru_post_import_insights.models import (
    InsightGenerationContext,
    SourceImportContext,
)
from kitaru_post_import_insights.pipeline import (
    InsightGenerationConfig,
    generate_insights,
)
from kitaru_post_import_insights.profiling import ProfilingConfig, profile_sessions

NOW = datetime(2026, 10, 31, tzinfo=UTC)
AGENT_ID = UUID(int=1)
IMPORT_ID = UUID(int=2)


def _session(number: int, *, requests: int = 0) -> SessionWithNodesResponse:
    session_id = UUID(int=100 + number)
    return SessionWithNodesResponse(
        session=SessionDetailResponse(
            id=session_id,
            owner_id=UUID(int=3),
            agent_id=AGENT_ID,
            created=NOW,
            updated=NOW,
            number=number,
            import_id=IMPORT_ID,
            origin=SessionOrigin.IMPORTED,
            status=SessionStatus.COMPLETED,
            inputs=None,
            outputs=None,
            started_at=NOW,
            ended_at=NOW + timedelta(seconds=1),
            metadata={},
            cost=None,
            tokens=None,
            llm_call_count=1,
            tool_call_count=0,
        ),
        nodes=[
            SessionNodeResponse(
                id=UUID(int=1000 + number),
                session_id=session_id,
                external_id="model",
                links=[],
                node_type=NodeType.LLM_CALL,
                name="model",
                status=NodeStatus.COMPLETED,
                model="test-model",
                inputs=None,
                outputs={
                    "role": "assistant",
                    "parts": [
                        {"type": "tool_call", "id": str(index)}
                        for index in range(requests)
                    ],
                },
                metadata={},
            )
        ],
    )


@pytest.mark.parametrize("requests", [0, 2])
@pytest.mark.parametrize("model_enabled", [False, True])
async def test_uniform_successful_sessions_produce_no_filler_or_model_calls(
    requests: int, model_enabled: bool
) -> None:
    generator = create_autospec(InsightModelGenerator, instance=True)
    generator.analyze.side_effect = AssertionError("unexpected analyst call")
    generator.edit.side_effect = AssertionError("unexpected editor call")
    result = await generate_insights(
        [_session(number, requests=requests) for number in range(1, 4)],
        context=InsightGenerationContext(
            agent_id=AGENT_ID,
            source_import=SourceImportContext(import_id=IMPORT_ID),
        ),
        config=InsightGenerationConfig(
            model=ModelGenerationConfig(model="test-model") if model_enabled else None
        ),
        generator=generator if model_enabled else None,
    )

    assert result.insights == []
    assert result.empty_reason == "no_eligible_candidates"
    assert result.coverage.sessions_analyzed == 3
    generator.analyze.assert_not_called()
    generator.edit.assert_not_called()


def test_uniform_activity_does_not_hide_a_substantive_failure() -> None:
    sessions = [_session(number) for number in range(1, 4)]
    sessions[0].session.status = SessionStatus.FAILED
    profiling = profile_sessions(sessions)

    assert [item.id for item in profiling.candidates] == ["session-outcomes"]
    assert (
        generate_deterministic_plan(profiling).selection.recommended_candidate_id
        == "session-outcomes"
    )


@pytest.mark.parametrize(
    "outputs",
    [
        None,
        "unstructured output",
        {},
        [],
        {"role": "assistant", "content": "ok"},
        {"role": "assistant", "parts": [{}]},
        {"role": "assistant", "parts": [{"type": "tool_use", "id": "lookup"}]},
    ],
)
def test_missing_or_unsupported_output_is_not_a_zero_count(outputs: object) -> None:
    unknown = _session(1)
    unknown.nodes[0].outputs = outputs
    result = profile_sessions(
        [unknown, _session(2, requests=2), _session(3, requests=2)]
    )

    assert result.candidates == []
    assert any(
        "Tool activity coverage is incomplete" in x for x in result.coverage.caveats
    )


def test_incomplete_node_output_invalidates_a_partial_positive_count() -> None:
    unknown = _session(1, requests=1)
    unknown.nodes.append(
        unknown.nodes[0].model_copy(
            update={"id": UUID(int=2001), "external_id": "missing", "outputs": None}
        )
    )
    result = profile_sessions(
        [unknown, _session(2, requests=2), _session(3, requests=2)]
    )

    assert "tool-call-distribution" not in {item.id for item in result.candidates}
    assert any(
        "Tool activity coverage is incomplete" in x for x in result.coverage.caveats
    )


def test_truncated_requests_remain_unknown_when_no_card_survives() -> None:
    result = profile_sessions(
        [_session(1, requests=100), _session(2), _session(3)],
        config=ProfilingConfig(max_payload_items=30),
    )

    assert result.candidates == []
    assert any(
        "Tool activity coverage is incomplete" in x for x in result.coverage.caveats
    )


def test_observed_zero_stays_in_a_varying_request_distribution() -> None:
    result = profile_sessions([_session(1), _session(2, requests=4)])
    candidate = next(x for x in result.candidates if x.id == "tool-call-distribution")

    assert candidate.data.unit == "requests"
    assert {fact.name: fact.value for fact in candidate.facts} == {
        "observations": 2,
        "minimum": 0,
        "maximum": 4,
    }
    assert "may not have executed" in (candidate.caveat or "")
