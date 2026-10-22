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
"""Rebuilt requests keep the recorded constraints or skip the call."""

import json
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.session_node import SessionNodeResponse
from kitaru.task.importer import ImportedSession, flatten_nodes
from kitaru_langfuse_importer.importer import LangfuseJSONLImporter
from kitaru_model_swap.endpoint import build_body, build_responses_body
from kitaru_model_swap.normalize import ReplayRequest, Skipped, build_replay_request
from kitaru_model_swap.params import Candidate

TOOL: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "final_result",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {"result": {"type": "string"}},
            "required": ["result"],
            "additionalProperties": False,
        },
    },
}
MESSAGES = [{"role": "user", "content": "Classify this ticket"}]
CHAT = Candidate.model_validate(
    {"name": "reference", "model": "same-model", "base_url": "http://localhost/v1"}
)
RESPONSES = Candidate.model_validate(
    {
        "name": "reference",
        "model": "same-model",
        "base_url": "http://localhost/v1",
        "provider_settings": {"api": "responses"},
    }
)
SCHEMA_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "classification",
        "strict": True,
        "schema": TOOL["function"]["parameters"],
    },
}


def rebuild(
    inputs: Any, outputs: Any, *, model_params: dict[str, Any] | None = None
) -> ReplayRequest | Skipped:
    node = SessionNodeResponse.model_validate(
        {
            "id": uuid4(),
            "session_id": uuid4(),
            "external_id": "probe",
            "links": [],
            "node_type": "llm_call",
            "name": "chat",
            "status": "completed",
            "inputs": inputs,
            "outputs": outputs,
            "model_params": model_params,
            "metadata": {},
        }
    )
    return build_replay_request(
        node,
        tool_schemas={},
        tool_descriptions={},
        use_recorded_tools=True,
        infer_missing_schemas=False,
    )


def replayable(inputs: Any, outputs: Any = "done") -> ReplayRequest:
    result = rebuild(inputs, outputs)
    assert isinstance(result, ReplayRequest), result
    return result


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("tool_choice", {"type": "function", "function": {"name": "final_result"}}),
        ("tool_choice", "required"),
        ("parallel_tool_calls", False),
        ("response_format", SCHEMA_FORMAT),
    ],
)
def test_chat_request_keeps_recorded_controls(field: str, value: Any) -> None:
    request = replayable({"messages": MESSAGES, "tools": [TOOL], field: value})
    body = build_body(CHAT, request.messages, request.tools, request.controls)
    assert body[field] == value
    assert body["tools"][0]["function"]["strict"] is True


def test_responses_request_keeps_recorded_controls() -> None:
    request = replayable(
        {
            "messages": MESSAGES,
            "tools": [TOOL],
            "tool_choice": {"type": "function", "function": {"name": "final_result"}},
            "parallel_tool_calls": False,
            "response_format": SCHEMA_FORMAT,
        }
    )
    body = build_responses_body(
        RESPONSES, request.messages, request.tools, request.controls
    )
    assert body["tool_choice"] == {"type": "function", "name": "final_result"}
    assert body["parallel_tool_calls"] is False
    assert body["text"]["format"] == {
        "type": "json_schema",
        "name": "classification",
        "strict": True,
        "schema": TOOL["function"]["parameters"],
    }
    assert body["tools"][0]["strict"] is True


def test_responses_tools_are_not_strict_unless_recorded() -> None:
    loose = {"type": "function", "function": {"name": "f", "parameters": {}}}
    request = replayable({"messages": MESSAGES, "tools": [loose]})
    body = build_responses_body(RESPONSES, request.messages, request.tools)
    assert body["tools"][0]["strict"] is False


def test_controls_recorded_as_model_parameters_are_kept() -> None:
    result = rebuild(
        {"messages": MESSAGES, "tools": [TOOL]},
        "done",
        model_params={"tool_choice": "required"},
    )
    assert isinstance(result, ReplayRequest)
    assert result.controls == {"tool_choice": "required"}


def test_calls_in_one_import_keep_their_own_controls() -> None:
    forced = replayable(
        {"messages": MESSAGES, "tools": [TOOL], "tool_choice": "required"}
    )
    free = replayable({"messages": MESSAGES, "tools": [TOOL], "tool_choice": "auto"})
    assert build_body(CHAT, forced.messages, forced.tools, forced.controls)[
        "tool_choice"
    ] == ("required")
    assert (
        build_body(CHAT, free.messages, free.tools, free.controls)["tool_choice"]
        == "auto"
    )


@pytest.mark.parametrize(
    ("inputs", "detail"),
    [
        (
            {
                "messages": MESSAGES,
                "tools": [TOOL],
                "tool_choice": {"type": "function", "function": {"name": "other"}},
            },
            "not offered",
        ),
        (
            {"messages": MESSAGES, "tools": [TOOL], "tool_choice": {"type": "any"}},
            "unsupported form",
        ),
        ({"messages": MESSAGES, "tool_choice": "required"}, "requires a tool"),
        (
            {"messages": MESSAGES, "response_format": {"type": "regex"}},
            "response_format",
        ),
        ({"messages": MESSAGES, "parallel_tool_calls": "no"}, "parallel_tool_calls"),
        ({"messages": MESSAGES, "functions": [{"name": "f"}]}, "functions"),
    ],
)
def test_controls_that_cannot_be_sent_skip_the_call(
    inputs: dict[str, Any], detail: str
) -> None:
    result = rebuild(inputs, "done")
    assert isinstance(result, Skipped)
    assert result.reason == "unsupported_request_control"
    assert detail in result.detail


def test_tool_choice_without_tools_is_not_sent() -> None:
    request = replayable({"messages": MESSAGES, "tool_choice": "auto"})
    assert "tool_choice" not in build_body(CHAT, request.messages, request.tools)
    assert request.controls == {}


def test_importer_preserves_strict_schema() -> None:
    record = {
        "id": "generation",
        "traceId": "trace-1",
        "type": "GENERATION",
        "name": "chat",
        "sessionId": "session-1",
        "projectId": "project-1",
        "startTime": "2026-07-24T10:00:00Z",
        "endTime": "2026-07-24T10:00:01Z",
        "input": {"messages": MESSAGES, "tools": [TOOL]},
        "output": {"role": "assistant", "content": "done"},
    }
    parsed = list(LangfuseJSONLImporter().parse(json.dumps(record).encode(), {}))
    session = next(x for x in parsed if isinstance(x, ImportedSession))
    imported = flatten_nodes(session.nodes)[0]
    assert imported.inputs["tools"] == [TOOL]
    definition = imported.metadata["kitaru.tool_definitions"][0]["function"]
    assert definition["strict"] is True


def test_strict_flag_from_the_request_survives_older_metadata() -> None:
    node = SessionNodeResponse.model_validate(
        {
            "id": uuid4(),
            "session_id": uuid4(),
            "external_id": "probe",
            "links": [],
            "node_type": "llm_call",
            "name": "chat",
            "status": "completed",
            "inputs": {"messages": MESSAGES, "tools": [TOOL]},
            "outputs": "done",
            # Imports made before strictness was kept have no `strict` here.
            "metadata": {
                "kitaru.tool_definitions": [
                    {
                        "type": "function",
                        "function": {
                            "name": "final_result",
                            "parameters": TOOL["function"]["parameters"],
                        },
                    }
                ]
            },
        }
    )
    result = build_replay_request(
        node,
        tool_schemas={},
        tool_descriptions={},
        use_recorded_tools=True,
        infer_missing_schemas=False,
    )
    assert isinstance(result, ReplayRequest)
    assert result.tools[0]["function"]["strict"] is True


def test_function_without_parameters_is_offered_with_an_empty_schema() -> None:
    no_arguments = {
        "type": "function",
        "function": {"name": "get_current_time", "description": "No arguments"},
    }
    request = replayable({"messages": MESSAGES, "tools": [TOOL, no_arguments]})
    assert [tool["function"]["name"] for tool in request.tools] == [
        "final_result",
        "get_current_time",
    ]
    assert request.tools[1]["function"]["parameters"] == {
        "type": "object",
        "properties": {},
    }


def test_importer_keeps_function_without_parameters() -> None:
    no_arguments = {"type": "function", "function": {"name": "get_current_time"}}
    record = {
        "id": "generation",
        "traceId": "trace-1",
        "type": "GENERATION",
        "name": "chat",
        "sessionId": "session-1",
        "projectId": "project-1",
        "startTime": "2026-07-24T10:00:00Z",
        "endTime": "2026-07-24T10:00:01Z",
        "input": {"messages": MESSAGES, "tools": [TOOL, no_arguments]},
        "output": {"role": "assistant", "content": "done"},
    }
    parsed = list(LangfuseJSONLImporter().parse(json.dumps(record).encode(), {}))
    session = next(x for x in parsed if isinstance(x, ImportedSession))
    definitions = flatten_nodes(session.nodes)[0].metadata["kitaru.tool_definitions"]
    assert [d["function"]["name"] for d in definitions] == [
        "final_result",
        "get_current_time",
    ]


def test_unsupported_offered_tool_skips_the_call() -> None:
    custom = {
        "type": "custom",
        "custom": {"name": "run_code", "format": {"type": "text"}},
    }
    result = rebuild({"messages": MESSAGES, "tools": [TOOL, custom]}, "done")
    assert isinstance(result, Skipped)
    assert result.reason == "unsupported_tool"


@pytest.mark.parametrize("output", ["42", "true", '"hello"', '{"result":"refund"}'])
def test_plain_text_that_is_valid_json_stays_text(output: str) -> None:
    request = replayable(MESSAGES, output)
    assert request.recorded.text == output
    assert request.recorded.tool_calls == ()


def test_recorded_response_envelope_in_a_string_is_still_read() -> None:
    envelope = json.dumps(
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "c1",
                    "type": "function",
                    "function": {"name": "final_result", "arguments": "{}"},
                }
            ],
        }
    )
    request = replayable({"messages": MESSAGES, "tools": [TOOL]}, envelope)
    assert [call.name for call in request.recorded.tool_calls] == ["final_result"]


@pytest.mark.parametrize(
    "extra_body",
    [
        {"max_completion_tokens": 10000},
        {"max_output_tokens": 10000},
        {"max_tokens": 10000},
        {"model": "other"},
        {"tool_choice": "none"},
        {"n": 3},
        {"text": {"format": {"type": "text"}}},
    ],
)
def test_extra_body_cannot_override_analyzer_fields(extra_body: dict[str, Any]) -> None:
    with pytest.raises(ValidationError, match="extra_body must not set"):
        Candidate.model_validate(
            {
                "name": "c",
                "model": "m",
                "base_url": "http://localhost/v1",
                "provider_settings": {"extra_body": extra_body},
            }
        )


def test_extra_body_text_options_merge_with_the_recorded_format() -> None:
    candidate = RESPONSES.model_copy(
        update={
            "provider_settings": RESPONSES.provider_settings.model_copy(
                update={"extra_body": {"text": {"verbosity": "low"}}}
            )
        }
    )
    request = replayable(
        {"messages": MESSAGES, "response_format": {"type": "json_object"}}
    )
    body = build_responses_body(
        candidate, request.messages, request.tools, request.controls
    )
    assert body["text"] == {"verbosity": "low", "format": {"type": "json_object"}}
