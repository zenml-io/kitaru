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
"""Compare a candidate's reply with the recorded answer."""

import json
import re
from collections import Counter
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from kitaru_model_swap.endpoint import CandidateReply
from kitaru_model_swap.normalize import RecordedOutcome, ToolCall

Agreement = Literal[
    "same_tool_same_args",
    "same_tool_different_args",
    "different_tool",
    "text_instead_of_tool",
    "same_text",
    "different_text",
    "tool_instead_of_text",
]

AGREEING: frozenset[Agreement] = frozenset({"same_tool_same_args", "same_text"})
AGREEMENT_LABELS: dict[Agreement, str] = {
    "same_tool_same_args": "Same tool and arguments",
    "same_tool_different_args": "Same tool, different arguments",
    "different_tool": "Different tool",
    "text_instead_of_tool": "Text instead of a tool call",
    "same_text": "Same answer",
    "different_text": "Different answer",
    "tool_instead_of_text": "Tool call instead of an answer",
}

_NUMBER = re.compile(r"^-?\d+(\.\d+)?$")
_SPACE = re.compile(r"\s+")


def normalize_value(value: Any) -> Any:
    """Normalize parsed JSON so equal arguments compare equal.

    Object keys are order-free, null-valued keys equal missing keys, strings
    are stripped, and numbers and numeric strings compare by value, so
    `98`, `98.0` and `"98.00"` are the same amount.
    """
    if isinstance(value, dict):
        return {
            key: normalize_value(item)
            for key, item in sorted(value.items())
            if item is not None
        }
    if isinstance(value, list):
        return [normalize_value(item) for item in value]
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int | float):
        return _number(value)
    if isinstance(value, str):
        stripped = value.strip()
        return _number(stripped) if _NUMBER.match(stripped) else stripped
    return value


def _number(value: float | int | str) -> str:
    """Render a number canonically."""
    try:
        return format(Decimal(str(value)).normalize(), "f")
    except InvalidOperation:
        return str(value)


def canonical_call(call: ToolCall) -> str:
    """Render a tool call as a canonical string."""
    return json.dumps(
        [call.name, normalize_value(call.arguments)], sort_keys=True, default=str
    )


def normalize_text(text: str | None) -> str:
    """Normalize free text for an exact comparison."""
    return _SPACE.sub(" ", (text or "").strip()).casefold()


def compare(recorded: RecordedOutcome, reply: CandidateReply) -> Agreement:
    """Classify how a successful reply relates to the recorded answer."""
    if recorded.tool_calls:
        if not reply.tool_calls:
            return "text_instead_of_tool"
        if Counter(call.name for call in recorded.tool_calls) != Counter(
            call.name for call in reply.tool_calls
        ):
            return "different_tool"
        if Counter(map(canonical_call, recorded.tool_calls)) == Counter(
            map(canonical_call, reply.tool_calls)
        ):
            return "same_tool_same_args"
        return "same_tool_different_args"
    if reply.tool_calls:
        return "tool_instead_of_text"
    if normalize_text(recorded.text) == normalize_text(reply.text):
        return "same_text"
    return "different_text"


def field_matches(
    recorded: RecordedOutcome, reply: CandidateReply
) -> dict[str, dict[str, bool]]:
    """Report, per tool and argument, whether the values match.

    Only tools called exactly once on both sides are compared, so the pairing
    is unambiguous.
    """
    recorded_by_name = _single_calls(recorded.tool_calls)
    reply_by_name = _single_calls(reply.tool_calls)
    matches: dict[str, dict[str, bool]] = {}
    for name, recorded_call in recorded_by_name.items():
        reply_call = reply_by_name.get(name)
        if reply_call is None:
            continue
        left = normalize_value(recorded_call.arguments)
        right = normalize_value(reply_call.arguments)
        if not isinstance(left, dict) or not isinstance(right, dict):
            continue
        matches[name] = {key: left.get(key) == right.get(key) for key in left}
    return matches


def _single_calls(calls: tuple[ToolCall, ...]) -> dict[str, ToolCall]:
    """Map tool names called exactly once to that call."""
    counts = Counter(call.name for call in calls)
    return {call.name: call for call in calls if counts[call.name] == 1}
