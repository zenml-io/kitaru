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
"""Find the parts of a recorded call a decision model could make.

A decision model picks from options; it cannot write text or invent values.
So a call splits into decisions (which tool to call, or to answer in text,
and which value each enum or boolean argument takes) and the rest, which
needs a generative model. Each call is classified as:

* `decision`: every part is a decision, so the call could move off an LLM;
* `partial`: the choice of tool or of answering in text is a decision, but
  free-form arguments or the text itself need a generative model;
* `generative`: nothing to decide, or a shape the plan does not cover.
"""

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from kitaru_model_swap.decisions import Question
from kitaru_model_swap.normalize import ReplayRequest

DecisionShape = Literal["decision", "partial", "generative"]

TOOL_QUESTION = "tool"
TEXT_ANSWER = "answer_in_text"
# Decision services accept at most 255 options per choice question.
_MAX_OPTIONS = 255
_KEY = re.compile(r"[^A-Za-z0-9_.-]")


@dataclass(frozen=True)
class DecisionPlan:
    """The questions a decision model would answer for one call."""

    shape: DecisionShape
    questions: tuple[Question, ...] = ()
    expected: Mapping[str, str] | None = None
    reason: str | None = None


def plan_decisions(request: ReplayRequest) -> DecisionPlan:
    """Split one rebuilt call into decision questions and the recorded answers."""
    tools = {tool["function"]["name"]: tool["function"] for tool in request.tools}
    if not tools:
        return DecisionPlan("generative", reason="no tools to choose from")
    calls = request.recorded.tool_calls
    if len(calls) > 1:
        return DecisionPlan("generative", reason="several tool calls at once")
    if len(tools) + 1 > _MAX_OPTIONS:
        return DecisionPlan("generative", reason="too many tools to choose from")
    options: dict[str, str | None] = {
        name: function.get("description") for name, function in tools.items()
    }
    options[TEXT_ANSWER] = "Answer the user directly in text instead of a tool."
    tool_question = Question(
        TOOL_QUESTION,
        "Which tool should the assistant call next, or should it answer the user "
        "in text?",
        options,
    )
    if not calls:
        return DecisionPlan(
            "partial",
            (tool_question,),
            {TOOL_QUESTION: TEXT_ANSWER},
            reason="the text answer needs a generative model",
        )
    call = calls[0]
    if call.name not in tools or not isinstance(call.arguments, dict):
        return DecisionPlan("generative", reason="the recorded call is not offered")
    parameters = tools[call.name].get("parameters") or {}
    properties = parameters.get("properties") or {}
    questions = [tool_question]
    expected = {TOOL_QUESTION: call.name}
    free: list[str] = []
    for argument, value in call.arguments.items():
        if value is None:
            continue
        values = _decidable_values(properties.get(argument), parameters)
        label = _label(value)
        if values is None or label not in values:
            free.append(argument)
            continue
        key = f"arg.{_KEY.sub('_', argument)}"
        questions.append(
            Question(
                key,
                f"If the assistant calls {call.name}, which value should its "
                f"argument {argument!r} have?",
                dict.fromkeys(values),
            )
        )
        expected[key] = label
    if free:
        return DecisionPlan(
            "partial",
            tuple(questions),
            expected,
            reason=f"free-form arguments: {', '.join(sorted(free))}",
        )
    return DecisionPlan("decision", tuple(questions), expected)


def _decidable_values(
    schema: Any, root: Mapping[str, Any], depth: int = 0
) -> list[str] | None:
    """Return the labels an argument can take, or None when it is free-form."""
    if not isinstance(schema, dict) or depth > 4:
        return None
    reference = schema.get("$ref")
    if isinstance(reference, str) and reference.startswith("#/$defs/"):
        definitions = root.get("$defs") or {}
        return _decidable_values(
            definitions.get(reference.removeprefix("#/$defs/")), root, depth + 1
        )
    enum = schema.get("enum")
    if isinstance(enum, list) and 1 < len(enum) <= _MAX_OPTIONS:
        labels = [_label(value) for value in enum if value is not None]
        return labels if len(set(labels)) == len(labels) else None
    if schema.get("type") == "boolean":
        return ["true", "false"]
    # Optional enums are written as anyOf with null.
    for option in schema.get("anyOf") or ():
        values = _decidable_values(option, root, depth + 1)
        if values is not None:
            return values
    return None


def _label(value: Any) -> str:
    """Render a value as an option label."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True)


def build_state(request: ReplayRequest) -> dict[str, Any]:
    """Describe the call to a decision model: the conversation and the tools."""
    return {
        "conversation": request.messages,
        "tools": [
            {
                "name": tool["function"]["name"],
                "description": tool["function"].get("description"),
            }
            for tool in request.tools
        ],
    }
