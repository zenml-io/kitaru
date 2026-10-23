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
"""Tests for comparing candidate replies with recorded answers."""

from typing import Any

import pytest

from kitaru_model_swap.compare import compare, field_matches, normalize_value
from kitaru_model_swap.endpoint import CandidateReply, Usage
from kitaru_model_swap.normalize import RecordedOutcome, ToolCall


def reply(*calls: ToolCall, text: str | None = None) -> CandidateReply:
    return CandidateReply(
        tool_calls=calls, text=text, usage=Usage(), latency_seconds=0, attempts=1
    )


def recorded(*calls: ToolCall, text: str | None = None) -> RecordedOutcome:
    return RecordedOutcome(tool_calls=calls, text=text)


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ({"a": 1, "b": 2}, {"b": 2, "a": 1}),
        ({"amount": 98}, {"amount": "98.00"}),
        ({"amount": 98.0}, {"amount": 98}),
        ({"order_id": None, "email": "x"}, {"email": " x "}),
        ([1, {"k": "v"}], ["1", {"k": "v"}]),
    ],
)
def test_normalization_treats_equal_arguments_as_equal(left: Any, right: Any) -> None:
    assert normalize_value(left) == normalize_value(right)


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ({"amount": 98}, {"amount": 99}),
        ({"a": [1, 2]}, {"a": [2, 1]}),
        ({"reason": "Defect"}, {"reason": "defect"}),
        (True, "true"),
    ],
)
def test_normalization_keeps_real_differences(left: Any, right: Any) -> None:
    assert normalize_value(left) != normalize_value(right)


@pytest.mark.parametrize(
    ("before", "after", "expected"),
    [
        (
            recorded(ToolCall("refund", {"amount": "98.00"})),
            reply(ToolCall("refund", {"amount": 98})),
            "same_tool_same_args",
        ),
        (
            recorded(ToolCall("refund", {"amount": 98})),
            reply(ToolCall("refund", {"amount": 50})),
            "same_tool_different_args",
        ),
        (
            recorded(ToolCall("refund", {})),
            reply(ToolCall("escalate", {})),
            "different_tool",
        ),
        (recorded(ToolCall("refund", {})), reply(text="ok"), "text_instead_of_tool"),
        (recorded(text="Hello  World"), reply(text="hello world"), "same_text"),
        (recorded(text="yes"), reply(text="no"), "different_text"),
        (recorded(text="yes"), reply(ToolCall("x", {})), "tool_instead_of_text"),
        (
            recorded(ToolCall("a", {"x": 1}), ToolCall("b", {})),
            reply(ToolCall("b", {}), ToolCall("a", {"x": 1})),
            "same_tool_same_args",
        ),
    ],
)
def test_compare_classifies_replies(
    before: RecordedOutcome, after: CandidateReply, expected: str
) -> None:
    assert compare(before, after) == expected


def test_field_matches_reports_each_argument() -> None:
    matches = field_matches(
        recorded(
            ToolCall("final_result", {"action": "refund", "amount": 98, "reason": "a"})
        ),
        reply(
            ToolCall(
                "final_result", {"action": "refund", "amount": "98.0", "reason": "b"}
            )
        ),
    )

    assert matches == {
        "final_result": {"action": True, "amount": True, "reason": False}
    }


def test_field_matches_skips_ambiguous_repeated_tools() -> None:
    assert (
        field_matches(
            recorded(ToolCall("a", {"x": 1}), ToolCall("a", {"x": 2})),
            reply(ToolCall("a", {"x": 1})),
        )
        == {}
    )
