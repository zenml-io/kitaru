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
"""`kitaru_session_read` MCP tool behavior."""

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

from mcp.types import CallToolResult, TextContent
from mcp_fakes import build_server_context

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

NOW = datetime(2026, 10, 31, tzinfo=UTC)


def _session(reply: str) -> SessionWithNodesResponse:
    session_id = uuid.uuid4()
    detail = SessionDetailResponse(
        id=session_id,
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        created=NOW,
        updated=NOW,
        number=1,
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.FAILED,
        name="Returns ticket",
        metadata={},
        llm_call_count=0,
        tool_call_count=1,
        inputs="Refund order #48213",
        outputs=reply,
    )
    node = SessionNodeResponse(
        id=uuid.uuid4(),
        session_id=session_id,
        external_id="lookup",
        links=[],
        node_type=NodeType.TOOL_CALL,
        name="lookup_order",
        status=NodeStatus.FAILED,
        error="Order '#48213' not found.",
        inputs={"order_id": "#48213"},
        outputs=None,
        attributes=None,
        metadata={},
    )
    return SessionWithNodesResponse(session=detail, nodes=[node])


def _build(*sessions: SessionWithNodesResponse) -> tuple[Any, Any]:
    stored = {session.session.id: session for session in sessions}

    async def get_with_nodes(session_id: uuid.UUID) -> SessionWithNodesResponse:
        return stored[session_id]

    client = SimpleNamespace(sessions=SimpleNamespace(get_with_nodes=get_with_nodes))
    return build_server_context(client)


async def test_session_read_sends_transcript_text_and_structured_steps() -> None:
    """The model reads the transcript text; the structured result keeps node IDs."""
    short, long = _session("Escalated."), _session("x" * 400)
    server, context = _build(short, long)

    result = await server.call_tool(
        "kitaru_session_read",
        {"request": {"session_ids": [str(short.session.id), str(long.session.id)]}},
        context,
    )

    assert isinstance(result, CallToolResult) and not result.is_error
    assert isinstance(result.content[0], TextContent)
    text = result.content[0].text
    assert "[0] tool_call lookup_order · failed" in text
    assert "    error: Order '#48213' not found." in text
    assert text.endswith("pass `node` and `full` to see one node whole.")
    data = cast(dict[str, Any], result.structured_content)["data"]
    assert [item["session_id"] for item in data["transcripts"]] == [
        str(short.session.id),
        str(long.session.id),
    ]
    assert data["transcripts"][0]["steps"][0]["node_id"] == str(short.nodes[0].id)


async def test_session_read_rejects_node_with_several_sessions() -> None:
    """Zooming into one node needs exactly one session."""
    first, second = _session("a"), _session("b")
    server, context = _build(first, second)

    result = await server.call_tool(
        "kitaru_session_read",
        {
            "request": {
                "session_ids": [str(first.session.id), str(second.session.id)],
                "node": 0,
            }
        },
        context,
    )

    assert isinstance(result, CallToolResult) and result.is_error
    error = cast(dict[str, Any], result.structured_content)["error"]
    assert error["code"] == "invalid_arguments"
