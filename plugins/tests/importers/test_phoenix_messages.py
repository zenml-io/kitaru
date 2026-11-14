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
"""Phoenix message normalization and readable text selectors."""

import json
from typing import Any

import pytest

from kitaru.json_pointer import resolve_json_pointer
from kitaru.task.importer import ImportedNode, ImportedSession
from kitaru_phoenix_importer import parse


def _parse_node(attributes: dict[str, Any], *, span_kind: str = "LLM") -> ImportedNode:
    payload = {
        "context": {"trace_id": "trace-1", "span_id": "span-1"},
        "name": "model",
        "span_kind": span_kind,
        "attributes": attributes,
    }
    [session] = list(parse(json.dumps(payload).encode(), {"project": "messages"}))
    assert isinstance(session, ImportedSession)
    [node] = session.nodes
    return node


def _assert_text(document: Any, selector: str | None, text: str) -> None:
    assert selector is not None
    assert resolve_json_pointer(document, selector) == (True, text)


def test_normalizes_messages_and_selects_text_after_compacting_indexes() -> None:
    attributes = {
        "llm.input_messages.12.message.role": "user",
        "llm.input_messages.12.message.content": "Current question",
        "llm.input_messages.2.message.role": "system",
        "llm.input_messages.2.message.content": "First system prompt",
        "llm.input_messages.3.message.role": "system",
        "llm.input_messages.3.message.content": "Second system prompt",
        "llm.input_messages.4.message.role": "user",
        "llm.input_messages.4.message.content": "Earlier question",
        "llm.output_messages.8.message.role": "assistant",
        "llm.output_messages.8.message.content": "Other candidate",
        "llm.output_messages.1.message.role": "tool",
        "llm.output_messages.1.message.content": "Tool result",
        "llm.output_messages.5.message.role": "assistant",
        "llm.output_messages.5.message.content": "Primary answer",
    }

    node = _parse_node(attributes)

    assert node.inputs == [
        {"role": "system", "content": "First system prompt"},
        {"role": "system", "content": "Second system prompt"},
        {"role": "user", "content": "Earlier question"},
        {"role": "user", "content": "Current question"},
    ]
    assert node.outputs == [
        {"role": "tool", "content": "Tool result"},
        {"role": "assistant", "content": "Primary answer"},
        {"role": "assistant", "content": "Other candidate"},
    ]
    assert node.system_prompt_selector == "/0/content"
    assert node.input_text_selector == "/3/content"
    assert node.output_text_selector == "/1/content"
    _assert_text(node.inputs, node.system_prompt_selector, "First system prompt")
    _assert_text(node.inputs, node.input_text_selector, "Current question")
    _assert_text(node.outputs, node.output_text_selector, "Primary answer")
    assert node.attributes["phoenix.attributes"] == attributes


@pytest.mark.parametrize("text", ['{"answer":42}', '["one","two"]', '"quoted"'])
def test_keeps_json_looking_message_text_as_strings(text: str) -> None:
    node = _parse_node(
        {
            "llm.input_messages.0.message.role": "user",
            "llm.input_messages.0.message.content": text,
            "llm.output_messages.0.message.role": "assistant",
            "llm.output_messages.0.message.content": text,
        }
    )

    assert node.inputs == [{"role": "user", "content": text}]
    assert node.outputs == [{"role": "assistant", "content": text}]
    _assert_text(node.inputs, node.input_text_selector, text)
    _assert_text(node.outputs, node.output_text_selector, text)


@pytest.mark.parametrize("text", [None, "Checking the weather"])
def test_reconstructs_tool_calls_without_using_arguments_as_reply(
    text: str | None,
) -> None:
    attributes = {
        "llm.output_messages.0.message.role": "assistant",
        "llm.output_messages.0.message.tool_calls.9.tool_call.id": "call-2",
        "llm.output_messages.0.message.tool_calls.9.tool_call.function.name": "weather",
        "llm.output_messages.0.message.tool_calls.9.tool_call.function.arguments": (
            '{"city":"London"}'
        ),
        "llm.output_messages.0.message.tool_calls.3.tool_call.id": "call-1",
        "llm.output_messages.0.message.tool_calls.3.tool_call.function.name": "weather",
        "llm.output_messages.0.message.tool_calls.3.tool_call.function.arguments": (
            '{"city":"Paris"}'
        ),
    }
    if text is not None:
        attributes["llm.output_messages.0.message.content"] = text

    node = _parse_node(attributes)

    expected = {
        "role": "assistant",
        "tool_calls": [
            {
                "id": "call-1",
                "function": {"name": "weather", "arguments": '{"city":"Paris"}'},
            },
            {
                "id": "call-2",
                "function": {"name": "weather", "arguments": '{"city":"London"}'},
            },
        ],
    }
    if text is not None:
        expected["content"] = text
        _assert_text(node.outputs, node.output_text_selector, text)
    else:
        assert node.output_text_selector is None
    assert node.outputs == [expected]
    assert node.attributes["phoenix.attributes"] == attributes


def test_selects_visible_text_and_reasoning_from_openinference_content_parts() -> None:
    attributes = {
        "llm.output_messages.4.message.role": "assistant",
        "llm.output_messages.4.message.contents.9.message_content.type": "text",
        "llm.output_messages.4.message.contents.9.message_content.text": (
            "The answer is 42"
        ),
        "llm.output_messages.4.message.contents.2.message_content.type": "reasoning",
        "llm.output_messages.4.message.contents.2.message_content.text": (
            "Let me calculate"
        ),
        "llm.output_messages.4.message.contents.6.message_content.type": "reasoning",
        "llm.output_messages.4.message.contents.6.message_content.encrypted_content": (
            "opaque"
        ),
    }

    node = _parse_node(attributes)

    assert node.outputs == [
        {
            "role": "assistant",
            "contents": [
                {"type": "reasoning", "text": "Let me calculate"},
                {"type": "reasoning", "encrypted_content": "opaque"},
                {"type": "text", "text": "The answer is 42"},
            ],
        }
    ]
    assert node.output_text_selector == "/0/contents/2/text"
    assert node.reasoning_selectors == ["/0/contents/0/text"]
    _assert_text(node.outputs, node.output_text_selector, "The answer is 42")
    _assert_text(node.outputs, node.reasoning_selectors[0], "Let me calculate")


@pytest.mark.parametrize(
    "index",
    ["-1", "²", "\u0661", "9" * 5000],
    ids=["negative", "superscript", "arabic", "long"],
)
def test_ignores_malformed_nested_indexes_without_losing_valid_content(
    index: str,
) -> None:
    node = _parse_node(
        {
            "llm.output_messages.0.message.role": "assistant",
            f"llm.output_messages.0.message.tool_calls.{index}.tool_call.id": "invalid",
            "llm.output_messages.0.message.tool_calls.5.tool_call.id": "call-1",
            "llm.output_messages.0.message.tool_calls.5.tool_call.function.name": (
                "weather"
            ),
            f"llm.output_messages.0.message.contents.{index}.message_content.text": (
                "invalid"
            ),
            "llm.output_messages.0.message.contents.8.message_content.type": "text",
            "llm.output_messages.0.message.contents.8.message_content.text": (
                "Valid answer"
            ),
        }
    )

    assert node.outputs == [
        {
            "role": "assistant",
            "tool_calls": [{"id": "call-1", "function": {"name": "weather"}}],
            "contents": [{"type": "text", "text": "Valid answer"}],
        }
    ]
    _assert_text(node.outputs, node.output_text_selector, "Valid answer")


@pytest.mark.parametrize("encoded", [False, True], ids=["structured", "json-encoded"])
def test_selects_genai_parts_without_changing_the_provider_payload(
    encoded: bool,
) -> None:
    inputs = [
        {"role": "system", "parts": [{"type": "text", "content": "Be concise"}]},
        {"role": "user", "parts": [{"type": "text", "content": "Question"}]},
    ]
    outputs = [
        {
            "role": "assistant",
            "parts": [
                {"type": "reasoning", "content": "Visible reasoning"},
                {"type": "text", "content": "Answer"},
            ],
        }
    ]
    node = _parse_node(
        {
            "gen_ai.input.messages": json.dumps(inputs) if encoded else inputs,
            "gen_ai.output.messages": json.dumps(outputs) if encoded else outputs,
            "input.value": "Opaque input stub",
            "output.value": "Opaque output stub",
        }
    )

    assert node.inputs == inputs
    assert node.outputs == outputs
    assert node.system_prompt_selector == "/0/parts/0/content"
    assert node.input_text_selector == "/1/parts/0/content"
    assert node.output_text_selector == "/0/parts/1/content"
    assert node.reasoning_selectors == ["/0/parts/0/content"]
    _assert_text(node.inputs, node.system_prompt_selector, "Be concise")
    _assert_text(node.inputs, node.input_text_selector, "Question")
    _assert_text(node.outputs, node.output_text_selector, "Answer")
    _assert_text(node.outputs, node.reasoning_selectors[0], "Visible reasoning")


def test_selects_reasoning_from_the_same_assistant_candidate_as_the_reply() -> None:
    node = _parse_node(
        {
            "gen_ai.output.messages": [
                {
                    "role": "assistant",
                    "parts": [
                        {"type": "reasoning", "content": "Primary reasoning"},
                        {"type": "text", "content": "Primary answer"},
                    ],
                },
                {
                    "role": "assistant",
                    "parts": [
                        {"type": "reasoning", "content": "Other reasoning"},
                        {"type": "text", "content": "Other answer"},
                    ],
                },
            ]
        }
    )

    _assert_text(node.outputs, node.output_text_selector, "Primary answer")
    assert node.reasoning_selectors == ["/0/parts/0/content"]
    _assert_text(node.outputs, node.reasoning_selectors[0], "Primary reasoning")


@pytest.mark.parametrize(
    "content",
    [None, "", "   ", {"text": "An object"}, [{"type": "image", "url": "image.png"}]],
)
def test_does_not_select_nontext_or_substitute_an_earlier_user_turn(
    content: Any,
) -> None:
    node = _parse_node(
        {
            "gen_ai.input.messages": [
                {"role": "system", "content": content},
                {"role": "user", "content": "Earlier question"},
                {"role": "user", "content": content},
            ],
            "gen_ai.output.messages": [{"role": "assistant", "content": content}],
            "llm.token_count.completion_details.reasoning": 10,
        }
    )

    assert node.input_text_selector is None
    assert node.output_text_selector is None
    assert node.system_prompt_selector is None
    assert node.reasoning_selectors == []


def test_selects_plain_string_payloads_at_the_document_root() -> None:
    node = _parse_node({"input.value": "Question", "output.value": "Answer"})

    assert node.input_text_selector == ""
    assert node.output_text_selector == ""
    assert node.system_prompt_selector is None
    _assert_text(node.inputs, node.input_text_selector, "Question")
    _assert_text(node.outputs, node.output_text_selector, "Answer")


def test_does_not_add_llm_selectors_to_other_span_kinds() -> None:
    node = _parse_node(
        {"input.value": "Question", "output.value": "Answer"}, span_kind="CHAIN"
    )

    assert node.inputs == "Question"
    assert node.outputs == "Answer"
    assert node.input_text_selector is None
    assert node.output_text_selector is None
    assert node.system_prompt_selector is None
    assert node.reasoning_selectors == []
