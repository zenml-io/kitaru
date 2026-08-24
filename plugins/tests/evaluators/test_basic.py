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

import uuid
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


def test_cost_uses_root_span_when_llm_cost_is_missing() -> None:
    """Use an explicit aggregate root cost when call costs are unavailable."""
    root_id = uuid.uuid4()
    view = _view(cost=Decimal("0.02"))
    view.nodes = [
        SessionNodeResponse.model_construct(
            id=root_id,
            node_type=NodeType.SPAN,
            parent_id=None,
            name="agent run",
            cost=Decimal("0.0125"),
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            parent_id=root_id,
            name="model request",
            cost=None,
        ),
    ]

    result = cost(view)

    assert result.score == 0.0125


def test_cost_prefers_call_costs_over_aggregate_root() -> None:
    """Do not count a root aggregate in addition to complete call costs."""
    root_id = uuid.uuid4()
    view = _view(cost=Decimal("0.065"))
    view.nodes = [
        SessionNodeResponse.model_construct(
            id=root_id,
            node_type=NodeType.SPAN,
            parent_id=None,
            name="agent run",
            cost=Decimal("0.04"),
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            parent_id=root_id,
            name="first model request",
            cost=Decimal("0.01"),
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            parent_id=root_id,
            name="second model request",
            cost=Decimal("0.015"),
        ),
    ]

    result = cost(view)

    assert result.score == 0.025


def test_cost_includes_priced_tool_calls_with_complete_llm_costs() -> None:
    """Include direct tool costs without double-counting the root aggregate."""
    root_id = uuid.uuid4()
    view = _view(cost=Decimal("0.08"))
    view.nodes = [
        SessionNodeResponse.model_construct(
            id=root_id,
            node_type=NodeType.SPAN,
            parent_id=None,
            name="agent run",
            cost=Decimal("0.05"),
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            parent_id=root_id,
            name="model request",
            cost=Decimal("0.01"),
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.TOOL_CALL,
            parent_id=root_id,
            name="paid search",
            cost=Decimal("0.02"),
        ),
    ]

    result = cost(view)

    assert result.score == 0.03


def test_cost_reports_tool_only_session_cost() -> None:
    """Report direct costs when a session has no LLM calls."""
    view = _view(cost=Decimal("0.02"))
    view.nodes = [
        SessionNodeResponse.model_construct(
            node_type=NodeType.TOOL_CALL,
            name="paid search",
            cost=Decimal("0.02"),
        )
    ]

    result = cost(view)

    assert result.score == 0.02


def test_cost_excludes_multiple_root_span_aggregates() -> None:
    """Sum direct calls instead of duplicate aggregate costs from each trace."""
    first_root_id = uuid.uuid4()
    second_root_id = uuid.uuid4()
    view = _view(cost=Decimal("0.06"))
    view.nodes = [
        SessionNodeResponse.model_construct(
            id=first_root_id,
            node_type=NodeType.SPAN,
            name="first trace",
            cost=Decimal("0.01"),
        ),
        SessionNodeResponse.model_construct(
            id=second_root_id,
            node_type=NodeType.SPAN,
            name="second trace",
            cost=Decimal("0.02"),
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            parent_id=first_root_id,
            name="first model request",
            cost=Decimal("0.01"),
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            parent_id=second_root_id,
            name="second model request",
            cost=Decimal("0.02"),
        ),
    ]

    result = cost(view)

    assert result.score == 0.03


def test_cost_uses_multiple_root_span_aggregates_when_llm_costs_are_missing() -> None:
    """Use each trace aggregate when no direct LLM cost was recorded."""
    first_root_id = uuid.uuid4()
    second_root_id = uuid.uuid4()
    view = _view(cost=Decimal("0.03"))
    view.nodes = [
        SessionNodeResponse.model_construct(
            id=first_root_id,
            node_type=NodeType.SPAN,
            name="first trace",
            cost=Decimal("0.01"),
        ),
        SessionNodeResponse.model_construct(
            id=second_root_id,
            node_type=NodeType.SPAN,
            name="second trace",
            cost=Decimal("0.02"),
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            parent_id=first_root_id,
            name="first model request",
            cost=None,
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            parent_id=second_root_id,
            name="second model request",
            cost=None,
        ),
    ]

    result = cost(view)

    assert result.score == 0.03


def test_cost_uses_root_span_when_call_rollup_is_partial() -> None:
    """Prefer a complete root aggregate to a partial call rollup."""
    root_id = uuid.uuid4()
    view = _view(cost=Decimal("0.04"))
    view.nodes = [
        SessionNodeResponse.model_construct(
            id=root_id,
            node_type=NodeType.SPAN,
            parent_id=None,
            name="agent run",
            cost=Decimal("0.03"),
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            parent_id=root_id,
            name="recorded model request",
            cost=Decimal("0.01"),
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            parent_id=root_id,
            name="missing model request",
            cost=None,
        ),
    ]

    result = cost(view)

    assert result.score == 0.03


def test_cost_uses_session_aggregate_when_all_llm_costs_are_missing() -> None:
    """Use a positive session aggregate when no call-level cost was recorded."""
    view = _view(cost=Decimal("0.0125"))
    view.nodes = [
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            name="model request",
            cost=None,
        )
    ]

    result = cost(view)

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


def test_cost_reports_unavailable_for_partial_call_rollup() -> None:
    """Do not present a partial session rollup as an aggregate cost."""
    view = _view(cost=Decimal("0.01"))
    view.nodes = [
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            name="recorded model request",
            cost=Decimal("0.01"),
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            name="missing model request",
            cost=None,
        ),
    ]

    result = cost(view)

    assert result.score is None
    assert result.value == "unavailable"


def test_cost_reports_unavailable_for_zero_root_rollup() -> None:
    """Do not present an ambiguous zero root rollup as a real total."""
    view = _view(cost=Decimal("0"))
    view.nodes = [
        SessionNodeResponse.model_construct(
            node_type=NodeType.SPAN,
            parent_id=None,
            name="agent run",
            cost=Decimal("0"),
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            name="model request",
            cost=None,
        ),
    ]

    result = cost(view)

    assert result.score is None
    assert result.value == "unavailable"


def test_cost_reports_unavailable_for_partial_root_rollup() -> None:
    """Do not present a subset of root costs as the session total."""
    view = _view(cost=Decimal("0.01"))
    view.nodes = [
        SessionNodeResponse.model_construct(
            node_type=NodeType.SPAN,
            parent_id=None,
            name="recorded agent run",
            cost=Decimal("0.01"),
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.SPAN,
            parent_id=None,
            name="missing agent run",
            cost=None,
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            name="model request",
            cost=None,
        ),
    ]

    result = cost(view)

    assert result.score is None
    assert result.value == "unavailable"


def test_cost_reports_unavailable_for_nested_span_rollup() -> None:
    """Do not present a nested span's partial rollup as a session total."""
    view = _view(cost=Decimal("0.01"))
    view.nodes = [
        SessionNodeResponse.model_construct(
            node_type=NodeType.SPAN,
            parent_id=uuid.uuid4(),
            name="nested operation",
            cost=Decimal("0.01"),
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            name="model request",
            cost=None,
        ),
    ]

    result = cost(view)

    assert result.score is None
    assert result.value == "unavailable"


def test_cost_reports_unavailable_for_unpriced_sibling_root() -> None:
    """Do not let a priced root span hide an unpriced sibling tree."""
    view = _view(cost=Decimal("0.01"))
    view.nodes = [
        SessionNodeResponse.model_construct(
            node_type=NodeType.SPAN,
            parent_id=None,
            name="recorded agent run",
            cost=Decimal("0.01"),
        ),
        SessionNodeResponse.model_construct(
            node_type=NodeType.LLM_CALL,
            parent_id=None,
            name="unpriced root call",
            cost=None,
        ),
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
