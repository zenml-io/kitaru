#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Transition failure matrix analysis, handlers, and MCP Apps wiring."""

import uuid
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from mcp.server.lowlevel.helper_types import ReadResourceContents
from mcp.types import CallToolResult, TextContent
from mcp_fakes import build_server_context

from kitaru.api_models.v1.annotation import AnnotationResponse, AnnotationSelector
from kitaru.api_models.v1.base import JsonValue, Page
from kitaru.api_models.v1.evaluation import EvaluationResponse
from kitaru.api_models.v1.session import SessionResponse
from kitaru.api_models.v1.session_node import SessionNodeResponse
from kitaru.mcp.apps import FAILURE_MATRIX_URI
from kitaru.mcp.models.failure_matrix import GroupRecords
from kitaru.mcp.server import create_server
from kitaru.mcp.settings import MCPSettings
from kitaru.mcp.tools.transitions import (
    START,
    analyze_group,
    build_cells,
    build_labeler,
    locate_failure,
)

T0 = datetime(2026, 9, 1, tzinfo=UTC)


def _session(status: str = "completed") -> SessionResponse:
    return SessionResponse.model_construct(
        id=uuid.uuid4(), number=1, name=None, status=status, agent_id=uuid.uuid4()
    )


def _node(
    session: SessionResponse,
    external_id: str,
    kind: str,
    name: str,
    *,
    parent: str | None = None,
    status: str = "completed",
    error: str | None = None,
    at: int = 0,
) -> SessionNodeResponse:
    return SessionNodeResponse(
        id=uuid.uuid4(),
        session_id=session.id,
        external_id=external_id,
        parent_external_id=parent,
        links=[],
        node_type=kind,
        name=name,
        tool_name=name if kind == "tool_call" else None,
        status=status,
        error=error,
        started_at=T0 + timedelta(seconds=at),
        metadata={},
    )


def _annotation(node: SessionNodeResponse, value: JsonValue) -> AnnotationResponse:
    return AnnotationResponse.model_construct(
        session_id=node.session_id,
        selector=AnnotationSelector(node_id=node.id),
        value=value,
        question_key=None,
    )


def _evaluation(session: SessionResponse, name: str) -> EvaluationResponse:
    return EvaluationResponse.model_construct(
        session_id=session.id, name=name, passed=False
    )


def _langgraph_failure(session: SessionResponse) -> list[SessionNodeResponse]:
    """Root span and graph node both inherit the failed status of the tool."""
    return [
        _node(session, "root", "span", "invoke", status="failed", at=0),
        _node(session, "look", "tool_call", "lookup_order", parent="root", at=1),
        _node(session, "agent", "span", "agent", parent="root", status="failed", at=2),
        _node(
            session,
            "refund",
            "tool_call",
            "issue_refund",
            parent="agent",
            status="failed",
            error="TimeoutError: payments API",
            at=3,
        ),
    ]


def _records(
    nodes: dict[uuid.UUID, list[SessionNodeResponse]],
    sessions: Sequence[SessionResponse],
    annotations: Sequence[AnnotationResponse] = (),
    evaluations: Sequence[EvaluationResponse] = (),
) -> GroupRecords:
    return GroupRecords(
        sessions=tuple(sessions),
        nodes={key: tuple(value) for key, value in nodes.items()},
        annotations=tuple(annotations),
        evaluations=tuple(evaluations),
        truncated=False,
    )


def test_error_is_placed_at_deepest_failed_node_not_inherited_span() -> None:
    session = _session("failed")
    nodes = _langgraph_failure(session)

    point = locate_failure(nodes, {})
    [outcome] = analyze_group(
        _records({session.id: nodes}, [session]), build_labeler("tool", None), None
    )

    assert point is not None and point.node.name == "issue_refund"
    assert outcome.failure_transition == ("lookup_order", "issue_refund")
    assert (
        outcome.point is not None and outcome.point.note == "TimeoutError: payments API"
    )


def test_span_states_skip_ancestors_of_the_failing_node() -> None:
    session = _session("failed")
    nodes = _langgraph_failure(session)

    [outcome] = analyze_group(
        _records({session.id: nodes}, [session]), build_labeler("span", None), None
    )

    # `agent` is the failing tool's parent and `invoke` is the root, so no
    # completed span came before the failure.
    assert outcome.failure_transition == (START, "issue_refund")


def test_reviewer_mark_wins_over_recorded_error_and_carries_note() -> None:
    session = _session("failed")
    nodes = _langgraph_failure(session)
    mark = _annotation(nodes[1], {"first_failure": True, "note": "Wrong order ID"})

    [outcome] = analyze_group(
        _records({session.id: nodes}, [session], [mark]),
        build_labeler("tool", None),
        None,
    )

    assert outcome.point is not None
    assert (outcome.point.source, outcome.point.note) == (
        "annotation",
        "Wrong order ID",
    )
    assert outcome.failure_transition == (START, "lookup_order")


def test_evaluation_failure_without_location_is_unlocated_not_guessed() -> None:
    session = _session("completed")
    nodes = [_node(session, "a", "tool_call", "lookup_order")]

    outcomes = analyze_group(
        _records(
            {session.id: nodes},
            [session],
            evaluations=[_evaluation(session, "grounded")],
        ),
        build_labeler("tool", None),
        None,
    )
    _rows, _cols, cells = build_cells(outcomes, None, ["error", "annotation"])

    assert outcomes[0].failed and outcomes[0].point is None
    assert all(cell.count == 0 for cell in cells)


def test_failing_llm_call_has_failures_but_no_rate() -> None:
    passing, failing = _session(), _session("failed")
    nodes = {
        passing.id: [_node(passing, "a", "tool_call", "search")],
        failing.id: [
            _node(failing, "a", "tool_call", "search", at=0),
            _node(failing, "b", "llm_call", "chat", status="failed", error="429", at=1),
        ],
    }

    outcomes = analyze_group(
        _records(nodes, [passing, failing]), build_labeler("tool", None), None
    )
    _rows, _cols, cells = build_cells(outcomes, None, ["error"])
    by_pair = {(c.from_state, c.to_state): c for c in cells}

    assert by_pair[("search", "llm")].count == 1
    assert by_pair[("search", "llm")].attempts is None
    assert by_pair[(START, "search")].attempts == 2


def test_state_map_merges_states_and_compare_counts_both_groups() -> None:
    before, after = _session("failed"), _session("completed")
    nodes = {
        before.id: [
            _node(before, "a", "tool_call", "sql_generate", at=0),
            _node(before, "b", "tool_call", "sql_execute", status="failed", at=1),
        ],
        after.id: [
            _node(after, "a", "tool_call", "sql_generate", at=0),
            _node(after, "b", "tool_call", "sql_execute", at=1),
        ],
    }
    labeler = build_labeler("tool", {"sql_*": "SQL"})

    base = analyze_group(_records(nodes, [before]), labeler, None)
    compare = analyze_group(_records(nodes, [after]), labeler, None)
    rows, cols, cells = build_cells(base, compare, ["error"])
    cell = next(c for c in cells if (c.from_state, c.to_state) == ("SQL", "SQL"))

    assert rows == [START, "SQL"] and cols == ["SQL"]
    assert (cell.count, cell.compare_count) == (1, 0)
    assert (cell.attempts, cell.compare_attempts) == (1, 1)


class _FakeSessions:
    def __init__(
        self,
        sessions: list[SessionResponse],
        nodes: dict[uuid.UUID, list[SessionNodeResponse]],
    ) -> None:
        self.sessions, self.nodes, self.list_calls = sessions, nodes, 0

    async def list(self, _params: object) -> Page[SessionResponse]:
        self.list_calls += 1
        return Page(items=self.sessions, next_cursor=None)

    async def iter_nodes(
        self, session_id: uuid.UUID, _params: object
    ) -> AsyncIterator[SessionNodeResponse]:
        for node in self.nodes[session_id]:
            yield node


class _FakeIterable:
    def __init__(self, items: Sequence[object]) -> None:
        self.items = items

    async def iter(self, _params: object) -> AsyncIterator[object]:
        for item in self.items:
            yield item


class _FakeClient:
    def __init__(self, records: GroupRecords) -> None:
        self.sessions = _FakeSessions(
            list(records.sessions), {k: list(v) for k, v in records.nodes.items()}
        )
        self.annotations = _FakeIterable(records.annotations)
        self.evaluations = _FakeIterable(records.evaluations)


def _failing_group() -> GroupRecords:
    sessions = [_session("failed") for _ in range(3)] + [_session()]
    nodes = {s.id: _langgraph_failure(s) for s in sessions[:3]}
    nodes[sessions[3].id] = [_node(sessions[3], "look", "tool_call", "lookup_order")]
    return _records(nodes, sessions)


async def test_matrix_tool_returns_summary_text_and_structured_matrix() -> None:
    client = _FakeClient(_failing_group())
    server, context = build_server_context(client)

    result = await server.call_tool(
        "kitaru_failure_matrix", {"request": {"label": "returns"}}, context
    )

    assert isinstance(result, CallToolResult) and not result.is_error
    assert isinstance(result.content[0], TextContent)
    assert "lookup_order -> issue_refund: 3" in result.content[0].text
    data = cast(dict[str, Any], result.structured_content)["data"]
    assert data["base"]["failed_count"] == 3
    cell = next(c for c in data["cells"] if c["to_state"] == "issue_refund")
    assert (cell["count"], cell["error_count"], cell["attempts"]) == (3, 3, 3)


async def test_cell_tool_reuses_cached_group_and_lists_sessions() -> None:
    client = _FakeClient(_failing_group())
    server, context = build_server_context(client)
    request = {"request": {"from_state": "lookup_order", "to_state": "issue_refund"}}

    await server.call_tool("kitaru_failure_matrix", {"request": {}}, context)
    result = await server.call_tool("kitaru_failure_matrix_cell", request, context)

    data = cast(dict[str, Any], cast(CallToolResult, result).structured_content)["data"]
    assert client.sessions.list_calls == 1
    assert data["total"] == 3
    assert data["patterns"] == [{"note": "TimeoutError: payments API", "count": 3}]
    assert data["sessions"][0]["path"] == ["lookup_order", "issue_refund"]


async def test_tools_link_the_view_and_hide_the_cell_tool_from_the_model() -> None:
    server = create_server(MCPSettings())

    tools = {tool.name: tool for tool in await server.list_tools()}
    [content] = list(await server.read_resource(FAILURE_MATRIX_URI))
    assert isinstance(content, ReadResourceContents)

    assert tools["kitaru_failure_matrix"].meta == {
        "ui": {"resourceUri": FAILURE_MATRIX_URI}
    }
    assert tools["kitaru_failure_matrix_cell"].meta == {
        "ui": {"resourceUri": FAILURE_MATRIX_URI, "visibility": ["app"]}
    }
    assert content.mime_type == "text/html;profile=mcp-app"
    assert "{{KITARU_LOGO}}" not in str(content.content)
