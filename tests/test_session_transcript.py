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
"""Compact session transcript rendering."""

import uuid
from datetime import UTC, datetime
from typing import Any

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
from kitaru.session_transcript import build_transcript, format_transcript

NOW = datetime(2026, 10, 31, tzinfo=UTC)
REQUEST = (
    "I ordered the Merino Runners (order #48213) three weeks ago and they "
    "arrived with a hole in the left shoe. I'd like a refund."
)
SYSTEM = "You resolve one customer return ticket. Look up the order before replying."


def _node(node_type: NodeType, name: str, **fields: Any) -> SessionNodeResponse:
    return SessionNodeResponse.model_validate(
        {
            "id": uuid.uuid4(),
            "session_id": uuid.UUID(int=1),
            "external_id": name,
            "links": [],
            "node_type": node_type,
            "name": name,
            "status": NodeStatus.COMPLETED,
            "inputs": None,
            "outputs": None,
            "attributes": None,
            "metadata": {},
            **fields,
        }
    )


def _model_call(output: Any, **fields: Any) -> SessionNodeResponse:
    return _node(
        NodeType.LLM_CALL,
        "chat",
        model="gpt-5-nano",
        inputs={"system": SYSTEM, "messages": [{"role": "user", "content": REQUEST}]},
        input_text_selector="/messages/0/content",
        system_prompt_selector="/system",
        outputs=output,
        **fields,
    )


def _session(*nodes: SessionNodeResponse, **fields: Any) -> SessionWithNodesResponse:
    detail = SessionDetailResponse.model_validate(
        {
            "id": uuid.UUID(int=1),
            "owner_id": uuid.UUID(int=2),
            "agent_id": uuid.UUID(int=3),
            "created": NOW,
            "updated": NOW,
            "number": 1,
            "origin": SessionOrigin.IMPORTED,
            "imported_from": "langfuse",
            "status": SessionStatus.COMPLETED,
            "name": "Returns ticket",
            "metadata": {},
            "llm_call_count": 2,
            "tool_call_count": 1,
            "inputs": None,
            "outputs": {"customer_reply": "Refunded."},
            "output_text_selector": "/customer_reply",
            **fields,
        }
    )
    return SessionWithNodesResponse(session=detail, nodes=list(nodes))


def _refund_session() -> SessionWithNodesResponse:
    return _session(
        _node(NodeType.SPAN, "agent run"),
        _model_call(
            [
                {
                    "role": "assistant",
                    "parts": [
                        {"type": "thinking", "content": "I should look it up."},
                        {"type": "tool_call", "name": "lookup_order", "arguments": {}},
                    ],
                }
            ],
            reasoning_selectors=["/0/parts/0/content"],
        ),
        _node(
            NodeType.TOOL_CALL,
            "lookup_order",
            tool_name="lookup_order",
            inputs={"order_id": "#48213"},
            status=NodeStatus.FAILED,
            error="Order '#48213' not found. " + "x" * 400,
        ),
        _model_call({"reply": "Sorry Dana."}, output_text_selector="/reply"),
    )


def test_transcript_shows_new_text_once_and_keeps_positions() -> None:
    """Repeated inputs point back at the request, and steps keep node positions."""
    transcript = build_transcript(_refund_session())

    assert transcript.request == REQUEST
    assert transcript.system_prompt == SYSTEM
    assert [step.index for step in transcript.steps] == [1, 2, 3]
    first_call, tool, second_call = transcript.steps
    assert first_call.input == "(same as request)"
    assert second_call.input == "(same as request)"
    assert first_call.output == "calls lookup_order"
    assert first_call.reasoning is None
    assert tool.input == '{"order_id":"#48213"}'
    assert second_call.output == "Sorry Dana."
    assert transcript.final_output == "Refunded."


def test_transcript_never_cuts_errors_and_marks_every_cut() -> None:
    """Errors stay whole, long fields end with a size marker, and full keeps all."""
    session = _refund_session()
    compact = build_transcript(session, max_chars=40)
    full = build_transcript(session, max_chars=None)

    assert compact.steps[1].error == session.nodes[2].error
    assert compact.request == f"{REQUEST[:40]}… [+{len(REQUEST) - 40} chars]"
    assert compact.truncated
    assert full.request == REQUEST
    assert full.steps[0].input == REQUEST
    assert not full.truncated


def test_transcript_names_unselected_model_input_by_size() -> None:
    """A model input without a text selector is never dumped into the transcript."""
    history = [{"role": "user", "content": "x" * 500}]
    transcript = build_transcript(
        _session(_node(NodeType.LLM_CALL, "chat", inputs=history))
    )

    assert transcript.steps[0].input == "[raw payload, 530 chars]"
    assert transcript.truncated


def test_transcript_reports_reasoning_and_prompt_changes_on_request() -> None:
    """Reasoning is opt-in, and a changed system prompt is shown where it changes."""
    changed = _model_call("done")
    changed.inputs["system"] = "A different prompt."
    transcript = build_transcript(
        _session(*_refund_session().nodes, changed), include_reasoning=True
    )

    assert transcript.steps[0].reasoning == "I should look it up."
    assert transcript.steps[-1].system_prompt == "A different prompt."
    assert transcript.steps[0].system_prompt is None


def test_transcript_node_index_shows_one_node_including_plain_spans() -> None:
    """Zooming in shows the chosen node only, with no header text."""
    session = _refund_session()

    transcript = build_transcript(session, node_index=0)

    assert [step.index for step in transcript.steps] == [0]
    assert transcript.request is None
    with pytest.raises(IndexError, match="outside this session's 4 nodes"):
        build_transcript(session, node_index=4)


def test_format_transcript_renders_header_and_labeled_fields() -> None:
    """Text output puts the header first and labels tool fields as args/result."""
    text = format_transcript(build_transcript(_refund_session()))

    assert text.splitlines()[:2] == [
        "session 00000000-0000-0000-0000-000000000001 · Returns ticket · completed "
        "· imported from langfuse",
        "2 LLM calls · 1 tool calls · 4 nodes",
    ]
    assert "[2] tool_call lookup_order · failed" in text
    assert '    args: {"order_id":"#48213"}' in text
    assert text.endswith("final output: Refunded.")


def test_transcript_redacts_credential_keys_before_flattening_payloads() -> None:
    """A secret under a credential key never reaches the transcript text."""
    tool = _node(
        NodeType.TOOL_CALL,
        "charge_card",
        inputs={"order_id": "48213", "api_key": "sk-test-not-real"},
    )

    transcript = build_transcript(_session(tool))

    assert transcript.steps[0].input == '{"order_id":"48213","api_key":"***"}'
