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
# Decision services accept at most 255 options per choice question, and Clef
# at most 64 questions per request.
_MAX_OPTIONS = 255
_MAX_QUESTIONS = 60
_KEY = re.compile(r"[^A-Za-z0-9_]")
_NULL = "null"


@dataclass(frozen=True)
class DecisionPlan:
    """The questions a decision model would answer for one call."""

    shape: DecisionShape
    questions: tuple[Question, ...] = ()
    expected: Mapping[str, str] | None = None
    reason: str | None = None
    arguments: Mapping[str, str] | None = None
    """The argument each `arg_` question asks about, by question key."""


def plan_decisions(request: ReplayRequest) -> DecisionPlan:
    """Split one rebuilt call into decision questions and the recorded answers."""
    # Inferred schemas only echo the recorded arguments and list only the
    # tools this call used, so they cannot say what the model chose from.
    if request.schema_source == "inferred":
        return DecisionPlan("generative", reason="tool schemas were inferred")
    tools = {tool["function"]["name"]: tool["function"] for tool in request.tools}
    if not tools:
        return DecisionPlan("generative", reason="no tools to choose from")
    calls = request.recorded.tool_calls
    if len(calls) > 1:
        return DecisionPlan("generative", reason="several tool calls at once")
    if len(tools) + 1 > _MAX_OPTIONS:
        return DecisionPlan("generative", reason="too many tools to choose from")
    if TEXT_ANSWER in tools:
        return DecisionPlan("generative", reason=f"a tool is named {TEXT_ANSWER}")
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
    questions = [tool_question]
    fields: dict[tuple[str, str], tuple[str, list[str]]] = {}
    # Questions must depend only on the offered schemas. Asking about only
    # the recorded tool's arguments would reveal the tool-choice answer.
    for name, function in tools.items():
        parameters = function.get("parameters") or {}
        required = parameters.get("required") or ()
        for argument, schema in (parameters.get("properties") or {}).items():
            values = _decidable_values(
                schema, parameters, with_null=argument in required
            )
            if values is None or len(questions) >= _MAX_QUESTIONS:
                continue
            key = f"arg_{len(questions) - 1}_{_KEY.sub('_', argument)[:40]}"
            questions.append(
                Question(
                    key,
                    f"If the assistant calls {name}, which value should its "
                    f"argument {argument!r} have?",
                    dict.fromkeys(values),
                )
            )
            fields[name, argument] = (key, values)
    if not calls:
        return DecisionPlan(
            "partial",
            tuple(questions),
            {TOOL_QUESTION: TEXT_ANSWER},
            reason="the text answer needs a generative model",
        )
    call = calls[0]
    if call.name not in tools or not isinstance(call.arguments, dict):
        return DecisionPlan("generative", reason="the recorded call is not offered")
    parameters = tools[call.name].get("parameters") or {}
    expected = {TOOL_QUESTION: call.name}
    arguments: dict[str, str] = {}
    free: list[str] = []
    required = parameters.get("required") or ()
    for argument, value in call.arguments.items():
        # A null optional argument is the same as leaving it out, but a null
        # required one is a choice the model made, so it is asked about.
        if value is None and argument not in required:
            continue
        field = fields.get((call.name, argument))
        label = _label(value)
        if field is None or label not in field[1]:
            free.append(argument)
            continue
        key = field[0]
        expected[key] = label
        arguments[key] = argument[:80]
    if free:
        return DecisionPlan(
            "partial",
            tuple(questions),
            expected,
            reason=f"free-form arguments: {', '.join(sorted(free))}"[:200],
            arguments=arguments,
        )
    return DecisionPlan("decision", tuple(questions), expected, arguments=arguments)


def _decidable_values(
    schema: Any, root: Mapping[str, Any], *, with_null: bool
) -> list[str] | None:
    """Return the labels an argument can take, or None when it is free-form.

    Null is an option only `with_null`, for a required argument; for an
    optional one it means the same as leaving the argument out.
    """
    values = _schema_values(schema, root)
    if values is None:
        return None
    unique = [v for v in dict.fromkeys(values) if with_null or v != _NULL]
    # A single option is not a decision, and too many cannot be offered.
    return unique if 1 < len(unique) <= _MAX_OPTIONS else None


def _schema_values(
    schema: Any, root: Mapping[str, Any], depth: int = 0
) -> list[str] | None:
    """Collect every label a schema allows, or None when any value is allowed."""
    if not isinstance(schema, dict) or depth > 4:
        return None
    reference = schema.get("$ref")
    if isinstance(reference, str):
        for prefix, section in (
            ("#/$defs/", "$defs"),
            ("#/definitions/", "definitions"),
        ):
            if reference.startswith(prefix):
                definitions = root.get(section) or {}
                return _schema_values(
                    definitions.get(reference.removeprefix(prefix)), root, depth + 1
                )
        return None
    values: list[str] = []
    enum = schema.get("enum")
    if isinstance(enum, list):
        values += [_label(value) for value in enum]
    kinds = schema.get("type")
    if kinds == "boolean" or (isinstance(kinds, list) and "boolean" in kinds):
        values += ["true", "false"]
    # Null is one of the options a nullable decision can take.
    if kinds == "null" or (isinstance(kinds, list) and "null" in kinds):
        values.append(_NULL)
    branches = [*(schema.get("anyOf") or ()), *(schema.get("oneOf") or ())]
    for option in branches:
        if isinstance(option, dict) and option.get("type") == "null":
            values.append(_NULL)
            continue
        branch = _schema_values(option, root, depth + 1)
        if branch is None:
            # A free-form branch makes the whole argument free-form.
            return None
        values += branch
    parts = schema.get("allOf") or ()
    for part in parts:
        branch = _schema_values(part, root, depth + 1)
        if branch is not None:
            values += branch
    if not values and enum is None and not branches and not parts:
        return None
    return values


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
