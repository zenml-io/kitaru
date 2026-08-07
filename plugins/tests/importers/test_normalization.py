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
"""Tests for normalization shared by the official importers."""

from kitaru_plugins.importers.normalization import (
    detect_framework,
    get_input_text,
    get_output_text,
    get_system_prompt,
    get_tool_payload_text,
    get_reasoning,
)
def test_populates_prompt_fields_from_provider_message_shapes() -> None:
    """Surface role-tagged text while retaining nested provider payloads."""
    value = {
        "messages": [
            {
                "id": ["langchain", "schema", "messages", "SystemMessage"],
                "kwargs": {"content": "Follow policy."},
            },
            {
                "data": {"type": "human", "content": "Where is my order?"},
                "type": "human",
            },
        ]
    }
    assert get_system_prompt(value) == "Follow policy."
    assert get_input_text(value) == "Where is my order?"


def test_populates_nested_system_instruction_and_user_text() -> None:
    """Surface provider instructions nested inside request configuration."""
    value = {
        "config": {"system_instruction": "Answer from the handbook."},
        "contents": [{"role": "user", "parts": [{"text": "Refunds?"}]}],
    }

    assert get_system_prompt(value) == "Answer from the handbook."
    assert get_input_text(value) == "Refunds?"


def test_populates_output_and_retains_visible_reasoning() -> None:
    """Surface assistant output and retain visible reasoning separately."""
    value = {
        "messages": [{"role": "assistant", "content": "Order 42 shipped."}],
        "reasoning": "The tracking event says shipped.",
        "encrypted_content": "ciphertext",
    }
    assert get_output_text(value) == "Order 42 shipped."
    assert get_reasoning(value) == "The tracking event says shipped."


def test_serializes_structured_tool_payloads() -> None:
    """Surface structured tool payloads without losing their detail."""
    assert get_tool_payload_text({"city": "Delft"}) == '{"city":"Delft"}'
    assert get_tool_payload_text({"rain": True, "temperature": 18}) == (
        '{"rain":true,"temperature":18}'
    )


def test_detects_one_framework_from_metadata() -> None:
    """Map instrumentation markers to a canonical framework name."""
    assert detect_framework({"otel.scope.name": "pydantic_ai"}) == "pydantic-ai"
    assert detect_framework({"framework": "langgraph"}) == "langgraph"
    assert detect_framework(["langgraph", "pydantic-ai"]) is None
