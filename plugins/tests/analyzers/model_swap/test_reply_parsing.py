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
"""Malformed replies are counted failures and null decisions are asked."""

from typing import Any

import httpx
import pytest

from kitaru_model_swap import endpoint as endpoint_module
from kitaru_model_swap.decision_plan import plan_decisions
from kitaru_model_swap.endpoint import send
from kitaru_model_swap.normalize import RecordedOutcome, ReplayRequest, ToolCall
from kitaru_model_swap.params import Candidate


def candidate(**settings: Any) -> Candidate:
    return Candidate(
        name="c",
        base_url="https://mock/v1",
        model="c",
        provider_settings={"max_retries": 0} | settings,
    )


async def reply_to(content: dict[str, Any], **settings: Any) -> Any:
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=content))
    async with httpx.AsyncClient(transport=transport) as client:
        return await send(
            client, candidate(**settings), api_key=None, messages=[], tools=[]
        )


def chat(message: dict[str, Any]) -> dict[str, Any]:
    return {"choices": [{"message": message, "finish_reason": "stop"}]}


@pytest.mark.parametrize(
    ("message", "settings"),
    [
        ({"content": "ok", "tool_calls": 7}, {}),
        (
            {"content": '{"name":"refund","arguments":"not json"}'},
            {"tool_call_format": "json_content"},
        ),
        (
            {
                "content": '<tool_call>{"name":"refund","arguments":"not json"}'
                "</tool_call>"
            },
            {"tool_call_format": "hermes"},
        ),
        (
            {
                "tool_calls": [
                    {"function": {"name": "refund", "arguments": "[1, 2]"}},
                ]
            },
            {},
        ),
    ],
)
async def test_malformed_replies_are_parse_failures(
    message: dict[str, Any], settings: dict[str, Any]
) -> None:
    result = await reply_to(chat(message), **settings)

    assert result.failure == "tool_call_parse_error"
    assert result.tool_calls == ()


async def test_responses_arguments_must_be_an_object() -> None:
    result = await reply_to(
        {
            "status": "completed",
            "output": [{"type": "function_call", "name": "refund", "arguments": "7"}],
        },
        api="responses",
    )

    assert result.failure == "tool_call_parse_error"


async def test_unexpected_reply_shape_does_not_abort_the_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken(*_: object) -> None:
        raise TypeError("'int' object is not iterable")

    monkeypatch.setattr(endpoint_module, "_read_tool_calls", broken)
    result = await reply_to(chat({"content": "ok"}))

    assert result.failure == "tool_call_parse_error"
    assert "malformed reply" in (result.failure_detail or "")


def plan(value: Any, *, required: bool) -> Any:
    request = ReplayRequest(
        [{"role": "user", "content": "Should I refund?"}],
        [
            {
                "type": "function",
                "function": {
                    "name": "refund",
                    "parameters": {
                        "type": "object",
                        "properties": {"approved": {"type": ["boolean", "null"]}},
                        "required": ["approved"] if required else [],
                    },
                },
            }
        ],
        "recorded",
        "openai_chat",
        RecordedOutcome((ToolCall("refund", {"approved": value}),), None),
    )
    return plan_decisions(request)


def test_required_null_decision_is_asked_with_null_as_an_option() -> None:
    result = plan(None, required=True)

    assert result.shape == "decision"
    question = result.questions[1]
    assert list(question.options) == ["true", "false", "null"]
    assert result.expected[question.key] == "null"


def test_required_nullable_decision_offers_null_when_recorded_true() -> None:
    result = plan(True, required=True)

    assert list(result.questions[1].options) == ["true", "false", "null"]
    assert result.expected[result.questions[1].key] == "true"


def test_optional_null_argument_is_left_out_of_grading() -> None:
    result = plan(None, required=False)

    assert result.shape == "decision"
    assert result.expected == {"tool": "refund"}
    assert result.arguments == {}
    assert result.questions == plan(True, required=False).questions
