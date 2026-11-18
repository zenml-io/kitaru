#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Arize Phoenix importer tool definition tests."""

import json
from typing import Any

from kitaru.task.importer import ImportedNode, ImportedSession
from kitaru_phoenix_importer.importer import TOOL_DEFINITIONS_METADATA_KEY

from .test_phoenix import flatten, jsonl, parse, span

WEATHER_SCHEMA = {
    "type": "object",
    "properties": {"city": {"type": "string"}},
    "required": ["city"],
}


def _llm_node(attributes: dict[str, Any]) -> ImportedNode:
    """Import one LLM span under an agent span and return its node."""
    [session] = parse(
        jsonl(
            span("root", span_kind="AGENT"),
            span("llm", parent_id="root", span_kind="LLM", attributes=attributes),
        )
    )
    assert isinstance(session, ImportedSession)
    return next(
        node for node in flatten(session.nodes) if node.external_id == "trace-1:llm"
    )


def test_openinference_llm_tools_become_tool_definitions() -> None:
    """Normalize indexed OpenInference tool JSON schemas in index order."""
    node = _llm_node(
        {
            "llm.model_name": "gpt-4o-mini",
            "llm.input_messages.0.message.role": "user",
            "llm.input_messages.0.message.content": "Weather in Paris?",
            "llm.tools.1.tool.json_schema": json.dumps(
                {
                    "name": "get_time",
                    "description": "Current time in a city.",
                    "input_schema": WEATHER_SCHEMA,
                }
            ),
            "llm.tools.0.tool.json_schema": json.dumps(
                {
                    "type": "function",
                    "function": {
                        "name": "get_weather",
                        "description": "Current weather in a city.",
                        "parameters": WEATHER_SCHEMA,
                        "strict": True,
                    },
                }
            ),
        }
    )

    assert node.metadata[TOOL_DEFINITIONS_METADATA_KEY] == [
        {
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "Current weather in a city.",
                "parameters": WEATHER_SCHEMA,
                "strict": True,
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_time",
                "description": "Current time in a city.",
                "parameters": WEATHER_SCHEMA,
            },
        },
    ]


def test_genai_tool_definitions_become_tool_definitions() -> None:
    """Normalize flat OTel GenAI tool definitions to the nested form."""
    node = _llm_node(
        {
            "gen_ai.request.model": "claude-haiku-4-5",
            "gen_ai.tool.definitions": json.dumps(
                [
                    {
                        "type": "function",
                        "name": "get_weather",
                        "parameters": WEATHER_SCHEMA,
                    }
                ]
            ),
        }
    )

    assert node.metadata[TOOL_DEFINITIONS_METADATA_KEY] == [
        {
            "type": "function",
            "function": {"name": "get_weather", "parameters": WEATHER_SCHEMA},
        }
    ]


def test_llm_span_without_tools_has_no_tool_definitions() -> None:
    """Leave the tool definitions key absent when the span has no schemas."""
    node = _llm_node({"llm.model_name": "gpt-4o-mini"})

    assert TOOL_DEFINITIONS_METADATA_KEY not in node.metadata


def test_model_request_parameters_are_not_model_params() -> None:
    """Keep Pydantic AI's tool listing out of the sampling parameters."""
    node = _llm_node(
        {
            "llm.model_name": "gpt-4o-mini",
            "model_request_parameters": {"output_mode": "tool"},
        }
    )

    assert node.model_params is None


def test_model_request_parameters_tools_become_tool_definitions() -> None:
    """Fall back to Pydantic AI's function and output tools."""
    result_schema = {"type": "object", "properties": {"reply": {"type": "string"}}}
    node = _llm_node(
        {
            "llm.model_name": "gpt-4o-mini",
            "model_request_parameters": json.dumps(
                {
                    "function_tools": [
                        {
                            "name": "get_weather",
                            "description": "Current weather in a city.",
                            "parameters_json_schema": WEATHER_SCHEMA,
                            "kind": "function",
                        }
                    ],
                    "output_tools": [
                        {
                            "name": "final_result",
                            "parameters_json_schema": result_schema,
                            "kind": "output",
                        }
                    ],
                    "output_mode": "tool",
                }
            ),
        }
    )

    assert node.model_params is None
    assert node.metadata[TOOL_DEFINITIONS_METADATA_KEY] == [
        {
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "Current weather in a city.",
                "parameters": WEATHER_SCHEMA,
            },
        },
        {
            "type": "function",
            "function": {"name": "final_result", "parameters": result_schema},
        },
    ]
    # Model-swap reads Pydantic AI's flags from the raw span attributes.
    raw = json.loads(node.attributes["phoenix.attributes"]["model_request_parameters"])
    assert raw["output_mode"] == "tool"


def test_anthropic_custom_tools_become_tool_definitions() -> None:
    """Normalize Anthropic `custom` tools that keep their schema in input_schema."""
    node = _llm_node(
        {
            "gen_ai.request.model": "claude-haiku-4-5",
            "gen_ai.tool.definitions": json.dumps(
                [
                    {
                        "type": "custom",
                        "name": "get_weather",
                        "input_schema": WEATHER_SCHEMA,
                    }
                ]
            ),
        }
    )

    assert node.metadata[TOOL_DEFINITIONS_METADATA_KEY] == [
        {
            "type": "function",
            "function": {"name": "get_weather", "parameters": WEATHER_SCHEMA},
        }
    ]


def test_model_request_parameters_win_over_genai_tool_definitions() -> None:
    """Prefer Pydantic AI's list, which also carries output tools."""
    node = _llm_node(
        {
            "llm.model_name": "gpt-4o-mini",
            "gen_ai.tool.definitions": json.dumps(
                [
                    {
                        "type": "function",
                        "name": "get_weather",
                        "parameters": WEATHER_SCHEMA,
                    }
                ]
            ),
            "model_request_parameters": {
                "function_tools": [
                    {"name": "get_weather", "parameters_json_schema": WEATHER_SCHEMA}
                ],
                "output_tools": [
                    {"name": "final_result", "parameters_json_schema": WEATHER_SCHEMA}
                ],
            },
        }
    )

    names = [
        d["function"]["name"] for d in node.metadata[TOOL_DEFINITIONS_METADATA_KEY]
    ]
    assert names == ["get_weather", "final_result"]
