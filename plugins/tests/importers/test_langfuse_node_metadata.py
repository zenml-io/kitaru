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
"""Langfuse importer tests for model-call tool definitions and request parameters."""

import json
from typing import Any

import kitaru_langfuse_importer.node_metadata as node_metadata
from kitaru.api_models.v1.session_node import NodeType
from kitaru.task.importer import (
    ImportedNode,
    ImportedSession,
)

from .test_langfuse import flatten, jsonl, observation, sessions

# Copied from the first generation in
# examples/python/pydantic_ai_ticket_resolver/traces/langfuse-traces.jsonl.
PYDANTIC_AI_INPUT_TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "name": "lookup_order",
        "description": "Look up an order by exact order number or customer email.",
        "parameters": {
            "additionalProperties": False,
            "properties": {
                "order_id": {
                    "default": None,
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                },
                "email": {
                    "default": None,
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                },
            },
            "type": "object",
        },
    },
    {
        "type": "function",
        "name": "get_return_policy",
        "description": "Get the return window, defect rules, final-sale rule, and "
        "approval limit.",
        "parameters": {
            "additionalProperties": False,
            "properties": {"category": {"type": "string"}},
            "required": ["category"],
            "type": "object",
        },
    },
    {
        "type": "function",
        "name": "check_shipping",
        "description": "Check carrier status for a shipped or missing order.",
        "parameters": {
            "additionalProperties": False,
            "properties": {"tracking_no": {"type": "string"}},
            "required": ["tracking_no"],
            "type": "object",
        },
    },
    {
        "type": "function",
        "name": "issue_refund",
        "description": "Record a mock refund; no payment processor is contacted.",
        "parameters": {
            "additionalProperties": False,
            "properties": {
                "order_id": {"type": "string"},
                "amount": {
                    "anyOf": [
                        {"type": "number"},
                        {
                            "pattern": "^(?!^[-+.]*$)[+-]?0*\\d*\\.?\\d*$",
                            "type": "string",
                        },
                    ]
                },
            },
            "required": ["order_id", "amount"],
            "type": "object",
        },
    },
    {
        "type": "function",
        "name": "create_replacement",
        "description": "Record a mock replacement; no fulfillment order is created.",
        "parameters": {
            "additionalProperties": False,
            "properties": {"order_id": {"type": "string"}},
            "required": ["order_id"],
            "type": "object",
        },
    },
    {
        "type": "function",
        "name": "escalate_to_human",
        "description": "Record a mock escalation with a concise internal reason.",
        "parameters": {
            "additionalProperties": False,
            "properties": {"reason": {"type": "string"}},
            "required": ["reason"],
            "type": "object",
        },
    },
    {
        "type": "function",
        "name": "final_result",
        "description": "Structured resolution and customer reply returned by the "
        "agent.",
        "parameters": {
            "properties": {
                "action": {"$ref": "#/$defs/ResolutionAction"},
                "amount": {
                    "default": None,
                    "anyOf": [
                        {"type": "number"},
                        {
                            "pattern": "^(?!^[-+.]*$)[+-]?0*\\d*\\.?\\d*$",
                            "type": "string",
                        },
                        {"type": "null"},
                    ],
                },
                "reason": {"type": "string"},
                "customer_reply": {"type": "string"},
            },
            "required": ["action", "reason", "customer_reply"],
            "type": "object",
            "additionalProperties": False,
            "$defs": {
                "ResolutionAction": {
                    "description": "Terminal outcome selected for a support ticket.",
                    "enum": ["refund", "replacement", "escalate", "reject"],
                    "type": "string",
                }
            },
        },
    },
]


def _generation_node(session: ImportedSession) -> ImportedNode:
    """Return the only model-call node in one imported session."""
    [node] = [
        node for node in flatten(session.nodes) if node.node_type is NodeType.LLM_CALL
    ]
    return node


def test_keeps_pydantic_ai_input_tools_as_tool_definitions() -> None:
    """Normalize flat pydantic-ai OTel tools, including the output tool."""
    session = sessions(
        jsonl(
            observation("root", "3a7be91cef57eac0fa037593368e8dca"),
            observation(
                "631dcb13e8533ce2",
                "3a7be91cef57eac0fa037593368e8dca",
                parent_id="root",
                observation_type="GENERATION",
                name="chat gpt-5-nano",
                model="gpt-5-nano-2025-08-07",
                modelParameters={},
                input_={
                    "messages": [
                        {
                            "role": "user",
                            "parts": [
                                {"type": "text", "content": "Ticket: ticket-001"}
                            ],
                        }
                    ],
                    "tools": PYDANTIC_AI_INPUT_TOOLS,
                },
                metadata={
                    "attributes": {
                        "gen_ai.operation.name": "chat",
                        "gen_ai.request.model": "gpt-5-nano",
                        "model_request_parameters": json.dumps(
                            {"output_mode": "tool", "allow_text_output": False}
                        ),
                    }
                },
            ),
        )
    )[0]
    definitions = _generation_node(session).metadata[
        node_metadata.TOOL_DEFINITIONS_METADATA_KEY
    ]

    assert len(definitions) == 7
    assert [definition["function"]["name"] for definition in definitions] == [
        "lookup_order",
        "get_return_policy",
        "check_shipping",
        "issue_refund",
        "create_replacement",
        "escalate_to_human",
        "final_result",
    ]
    assert definitions[1] == {
        "type": "function",
        "function": {
            "name": "get_return_policy",
            "description": (
                "Get the return window, defect rules, final-sale rule, and "
                "approval limit."
            ),
            "parameters": PYDANTIC_AI_INPUT_TOOLS[1]["parameters"],
        },
    }
    assert all(set(definition) == {"type", "function"} for definition in definitions)
    assert "tools" in _generation_node(session).inputs


def test_falls_back_to_model_request_parameters_tools() -> None:
    """Use pydantic-ai function and output tools when the input has none."""
    lookup_schema = {
        "type": "object",
        "properties": {"order_id": {"type": "string"}},
        "required": ["order_id"],
    }
    result_schema = {"type": "object", "properties": {"reply": {"type": "string"}}}
    request_parameters = {
        "function_tools": [
            {
                "name": "lookup_order",
                "description": "Look up an order.",
                "parameters_json_schema": lookup_schema,
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
        "allow_text_output": False,
    }
    session = sessions(
        jsonl(
            observation("root", "trace-1"),
            observation(
                "generation",
                "trace-1",
                parent_id="root",
                observation_type="GENERATION",
                input_={"messages": [{"role": "user", "content": "hello"}]},
                metadata={
                    "attributes.model_request_parameters": json.dumps(
                        request_parameters
                    )
                },
            ),
        )
    )[0]

    assert _generation_node(session).metadata[
        node_metadata.TOOL_DEFINITIONS_METADATA_KEY
    ] == [
        {
            "type": "function",
            "function": {
                "name": "lookup_order",
                "description": "Look up an order.",
                "parameters": lookup_schema,
            },
        },
        {
            "type": "function",
            "function": {"name": "final_result", "parameters": result_schema},
        },
    ]
    # The flags decide the recorded `tool_choice`; the tool lists are already
    # normalized above.
    assert _generation_node(session).metadata[
        node_metadata.REQUEST_PARAMETERS_METADATA_KEY
    ] == {"output_mode": "tool", "allow_text_output": False}


def test_falls_back_to_genai_tool_definitions() -> None:
    """Normalize OTel GenAI tool definitions from metadata attributes."""
    schema = {"type": "object", "properties": {"city": {"type": "string"}}}
    session = sessions(
        jsonl(
            observation("root", "trace-1"),
            observation(
                "generation",
                "trace-1",
                parent_id="root",
                observation_type="GENERATION",
                metadata={
                    "attributes": {
                        "gen_ai.tool.definitions": json.dumps(
                            [
                                {
                                    "type": "function",
                                    "name": "get_weather",
                                    "parameters": schema,
                                },
                                {"type": "function", "name": "no_schema"},
                            ]
                        )
                    }
                },
            ),
        )
    )[0]

    assert _generation_node(session).metadata[
        node_metadata.TOOL_DEFINITIONS_METADATA_KEY
    ] == [
        {"type": "function", "function": {"name": "get_weather", "parameters": schema}},
        # A function without parameters takes no arguments.
        {
            "type": "function",
            "function": {
                "name": "no_schema",
                "parameters": {"type": "object", "properties": {}},
            },
        },
    ]


def test_generation_without_tools_has_no_tool_definitions() -> None:
    """Leave the tool definitions key absent when the source has no schemas."""
    session = sessions(
        jsonl(
            observation("root", "trace-1"),
            observation(
                "generation",
                "trace-1",
                parent_id="root",
                observation_type="GENERATION",
                input_={"messages": [{"role": "user", "content": "hello"}]},
                metadata={
                    "attributes": {
                        "model_request_parameters": json.dumps(
                            {"function_tools": [], "output_tools": []}
                        )
                    }
                },
            ),
        )
    )[0]

    assert (
        node_metadata.TOOL_DEFINITIONS_METADATA_KEY
        not in _generation_node(session).metadata
    )


def test_keeps_anthropic_custom_tools_as_tool_definitions() -> None:
    """Normalize Anthropic `custom` tools that keep their schema in input_schema."""
    schema = {"type": "object", "properties": {"city": {"type": "string"}}}
    session = sessions(
        jsonl(
            observation("root", "trace-1"),
            observation(
                "generation",
                "trace-1",
                parent_id="root",
                observation_type="GENERATION",
                input_={
                    "messages": [{"role": "user", "content": "hello"}],
                    "tools": [
                        {
                            "type": "custom",
                            "name": "get_weather",
                            "description": "Current weather in a city.",
                            "input_schema": schema,
                        }
                    ],
                },
            ),
        )
    )[0]

    assert _generation_node(session).metadata[
        node_metadata.TOOL_DEFINITIONS_METADATA_KEY
    ] == [
        {
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "Current weather in a city.",
                "parameters": schema,
            },
        }
    ]
