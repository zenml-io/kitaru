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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Tests for rebuilding chat requests from recorded nodes."""

import uuid
from collections import Counter
from typing import Any

import pytest

from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru.task.importer import ImportedSession
from kitaru_mlflow_importer.importer import MlflowTraceImporter
from kitaru_model_swap.normalize import (
    ReplayRequest,
    Skipped,
    ToolCall,
    build_replay_request,
    parse_recorded_output,
)

from .helpers import REPO_ROOT, to_response

MLFLOW_FIXTURE = (
    REPO_ROOT / "plugins/tests/importers/fixtures/mlflow/3.16.1/traces.json"
)
RETURNS_TOOLS = {
    "check_shipping",
    "create_replacement",
    "escalate_to_human",
    "final_result",
    "get_return_policy",
    "issue_refund",
    "lookup_order",
}


def llm_nodes(sessions: list[SessionWithNodesResponse]) -> list[SessionNodeResponse]:
    return [
        node
        for session in sessions
        for node in session.nodes
        if node.node_type is NodeType.LLM_CALL
    ]


def rebuild(node: SessionNodeResponse, **overrides: Any) -> ReplayRequest | Skipped:
    options: dict[str, Any] = {
        "tool_schemas": {},
        "tool_descriptions": {},
        "use_recorded_tools": True,
        "infer_missing_schemas": False,
    }
    options.update(overrides)
    return build_replay_request(node, **options)


def node(**fields: Any) -> SessionNodeResponse:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "session_id": uuid.uuid4(),
        "external_id": "node",
        "links": [],
        "node_type": NodeType.LLM_CALL,
        "name": "chat",
        "status": NodeStatus.COMPLETED,
        "inputs": None,
        "outputs": None,
        "attributes": None,
        "metadata": {},
    }
    values.update(fields)
    return SessionNodeResponse.model_validate(values)


def test_rebuilds_every_quickstart_call_with_recorded_tools(
    quickstart_sessions: list[SessionWithNodesResponse],
) -> None:
    nodes = llm_nodes(quickstart_sessions)
    requests = [rebuild(item) for item in nodes]

    assert len(quickstart_sessions) == 10
    assert len(nodes) == 33
    assert all(isinstance(item, ReplayRequest) for item in requests)
    replayable = [item for item in requests if isinstance(item, ReplayRequest)]
    assert {item.source_format for item in replayable} == {"otel_parts"}
    assert {item.schema_source for item in replayable} == {"recorded"}
    for item in replayable:
        assert {tool["function"]["name"] for tool in item.tools} == RETURNS_TOOLS
        assert item.messages[0]["role"] == "system"


def test_otel_parts_become_openai_messages(
    quickstart_sessions: list[SessionWithNodesResponse],
) -> None:
    # A later call in the first session carries tool calls and tool results.
    request = next(
        item
        for item in map(rebuild, llm_nodes(quickstart_sessions))
        if isinstance(item, ReplayRequest)
        and any(m["role"] == "tool" for m in item.messages)
    )
    assistant = next(m for m in request.messages if m.get("tool_calls"))
    call = assistant["tool_calls"][0]
    tool_message = next(m for m in request.messages if m["role"] == "tool")

    assert call["type"] == "function"
    assert isinstance(call["function"]["arguments"], str)
    assert tool_message["tool_call_id"] == call["id"]
    assert isinstance(tool_message["content"], str)
    assert request.dropped_reasoning_parts > 0
    assert all("thinking" not in str(m.get("content")) for m in request.messages[2:])


def test_quickstart_calls_are_skipped_without_any_schema(
    quickstart_sessions: list[SessionWithNodesResponse],
) -> None:
    results = [
        rebuild(item, use_recorded_tools=False)
        for item in llm_nodes(quickstart_sessions)
    ]

    assert Counter(item.reason for item in results if isinstance(item, Skipped)) == {
        "missing_tool_schema": 33
    }


def test_supplied_schemas_make_quickstart_calls_replayable(
    quickstart_sessions: list[SessionWithNodesResponse],
) -> None:
    schemas = {name: {"type": "object", "properties": {}} for name in RETURNS_TOOLS}
    results = [
        rebuild(item, use_recorded_tools=False, tool_schemas=schemas)
        for item in llm_nodes(quickstart_sessions)
    ]

    replayable = [item for item in results if isinstance(item, ReplayRequest)]
    assert len(replayable) == 33
    assert {item.schema_source for item in replayable} == {"supplied"}


def test_inferred_schemas_are_marked(
    quickstart_sessions: list[SessionWithNodesResponse],
) -> None:
    results = [
        rebuild(item, use_recorded_tools=False, infer_missing_schemas=True)
        for item in llm_nodes(quickstart_sessions)
    ]
    replayable = [item for item in results if isinstance(item, ReplayRequest)]

    assert len(replayable) == 33
    assert {item.schema_source for item in replayable} == {"inferred"}
    schema = next(
        tool["function"]["parameters"]
        for item in replayable
        for tool in item.tools
        if tool["function"]["name"] == "issue_refund"
        and tool["function"]["parameters"]["properties"]
    )
    assert set(schema["properties"]) == {"order_id", "amount"}


def test_tool_definitions_metadata_takes_precedence() -> None:
    definition = {
        "type": "function",
        "function": {"name": "search", "parameters": {"type": "object"}},
    }
    request = rebuild(
        node(
            inputs={
                "messages": [{"role": "user", "content": "hi"}],
                "tools": [{"type": "function", "name": "other", "parameters": {}}],
            },
            outputs={"role": "assistant", "content": "hello"},
            metadata={"kitaru.tool_definitions": [definition]},
        )
    )

    assert isinstance(request, ReplayRequest)
    assert request.tools == [definition]


def test_mlflow_fixture_rebuilds_openai_chat_calls() -> None:
    sessions = [
        to_response(item, index)
        for index, item in enumerate(
            (
                parsed
                for parsed in MlflowTraceImporter().parse(
                    MLFLOW_FIXTURE.read_bytes(), {}
                )
                if isinstance(parsed, ImportedSession)
            ),
            1,
        )
    ]
    results = [rebuild(item) for item in llm_nodes(sessions)]
    replayable = [item for item in results if isinstance(item, ReplayRequest)]

    assert {item.source_format for item in replayable} == {"openai_chat"}
    assert any(
        item.recorded.tool_calls == (ToolCall("get_weather", {"city": "Delft"}),)
        for item in replayable
    )
    # The fixture's Anthropic call is not guessed into OpenAI form.
    assert [item.reason for item in results if isinstance(item, Skipped)] == [
        "unsupported_input_format"
    ]


@pytest.mark.parametrize(
    ("fields", "reason"),
    [
        ({"status": NodeStatus.FAILED, "error": "boom"}, "failed_recorded_call"),
        ({"inputs": None}, "missing_inputs"),
        ({"inputs": "just text"}, "unsupported_input_format"),
        ({"inputs": {"prompt": "x"}}, "unsupported_input_format"),
        (
            {
                "inputs": [
                    {
                        "role": "user",
                        "content": [{"type": "image_url", "image_url": {}}],
                    }
                ]
            },
            "non_text_content",
        ),
        (
            {"inputs": [{"role": "user", "content": "hi"}], "outputs": 42},
            "unrecognized_recorded_output",
        ),
    ],
)
def test_skips_what_it_cannot_rebuild(fields: dict[str, Any], reason: str) -> None:
    defaults = {
        "inputs": [{"role": "user", "content": "hi"}],
        "outputs": "hello",
    }
    result = rebuild(node(**(defaults | fields)))

    assert isinstance(result, Skipped)
    assert result.reason == reason


@pytest.mark.parametrize(
    ("outputs", "expected"),
    [
        ("plain", ((), "plain")),
        (
            {"choices": [{"message": {"role": "assistant", "content": "hi"}}]},
            ((), "hi"),
        ),
        (
            [
                {
                    "role": "assistant",
                    "parts": [
                        {"type": "thinking", "content": "hmm"},
                        {
                            "type": "tool_call",
                            "name": "lookup_order",
                            "arguments": '{"order_id": "1"}',
                        },
                    ],
                }
            ],
            ((ToolCall("lookup_order", {"order_id": "1"}),), None),
        ),
    ],
)
def test_reads_recorded_output_shapes(outputs: Any, expected: tuple[Any, ...]) -> None:
    outcome = parse_recorded_output(outputs)

    assert (outcome.tool_calls, outcome.text) == expected
