#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Card wording regressions for sessions that share no import."""

from datetime import UTC, datetime, timedelta
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
from kitaru_post_import_insights.profiling import profile_sessions

NOW = datetime(2026, 11, 2, tzinfo=UTC)


def _recorded_session(number: int, status: SessionStatus) -> SessionWithNodesResponse:
    session_id = UUID(int=100 + number)
    return SessionWithNodesResponse(
        session=SessionDetailResponse(
            id=session_id,
            owner_id=UUID(int=3),
            agent_id=UUID(int=1),
            created=NOW,
            updated=NOW,
            number=number,
            origin=SessionOrigin.RECORDED,
            status=status,
            inputs={"messages": [{"role": "user", "content": "That is WRONG"}]},
            outputs=None,
            started_at=NOW,
            ended_at=NOW + timedelta(seconds=number),
            metadata={},
            cost=None,
            tokens=None,
            llm_call_count=number,
            tool_call_count=0,
        ),
        nodes=[
            SessionNodeResponse(
                id=UUID(int=1000 * number + index),
                session_id=session_id,
                external_id=f"model-{index}",
                links=[],
                node_type=NodeType.LLM_CALL,
                name="model",
                status=NodeStatus.COMPLETED,
                model="test-model",
                inputs=None,
                outputs="ok",
                metadata={},
            )
            for index in range(number)
        ],
    )


@pytest.mark.parametrize("all_failed", [False, True])
def test_card_text_does_not_assume_the_sessions_share_an_import(
    all_failed: bool,
) -> None:
    sessions = [
        _recorded_session(
            number,
            SessionStatus.FAILED
            if all_failed or number % 2
            else SessionStatus.COMPLETED,
        )
        for number in range(1, 5)
    ]

    result = profile_sessions(sessions, source_session_count=6)

    assert {
        "correction-language",
        "model-call-distribution",
        "session-outcomes",
    } <= {candidate.id for candidate in result.candidates}
    assert "import" not in result.model_dump_json().lower()
