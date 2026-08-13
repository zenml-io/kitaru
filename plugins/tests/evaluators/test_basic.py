#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Tests for the starting-point evaluator plugins."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from kitaru.api_models.v1.session import SessionResponse
from kitaru.api_models.v1.session_node import NodeType, SessionNodeResponse
from kitaru.task.evaluator import SessionView
from kitaru_evaluator.basic import cost, latency, tool_call_patterns


def _view(**session_fields: Any) -> SessionView:
    return SessionView(
        session=SessionResponse.model_construct(**session_fields),
        nodes=[],
    )


def test_latency_reports_elapsed_seconds() -> None:
    """Use session timestamps for the latency signal."""
    started_at = datetime(2026, 8, 4, tzinfo=UTC)
    result = latency(
        _view(started_at=started_at, ended_at=started_at + timedelta(seconds=2.5))
    )

    assert result.name == "latency_seconds"
    assert result.score == 2.5


def test_cost_reports_session_rollup() -> None:
    """Use the session cost rollup for the cost signal."""
    result = cost(_view(cost=Decimal("0.0125")))

    assert result.name == "cost"
    assert result.score == 0.0125


def test_cost_reports_unavailable_when_llm_cost_is_missing() -> None:
    """Do not present an unrecorded replay cost as a real zero."""
    view = _view(cost=Decimal("0"))
    view.nodes = [
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            name="model_request",
            cost=None,
        )
    ]

    result = cost(view)

    assert result.score is None
    assert result.value == "unavailable"


def test_tool_call_patterns_counts_repeated_tools() -> None:
    """Label sessions that call the same tool more than once."""
    view = _view()
    view.nodes = [
        SessionNodeResponse.model_construct(
            node_type=NodeType.TOOL_CALL, tool_name="search", name="search"
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.TOOL_CALL, tool_name="search", name="search"
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.TOOL_CALL, tool_name="fetch", name="fetch"
        ),
    ]

    result = tool_call_patterns(view)

    assert result.name == "tool_call_pattern"
    assert result.score == 1.0
    assert result.value == "repeated-tools"
