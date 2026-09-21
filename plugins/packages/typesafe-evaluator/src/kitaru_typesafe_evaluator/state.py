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
"""Build the state jev judges from a recorded session."""

import json
import re
from datetime import date
from typing import Any

from kitaru.api_models.v1.session_node import NodeType, SessionNodeResponse
from kitaru.json_pointer import resolve_json_pointer
from kitaru.task.evaluator import SessionView
from kitaru_typesafe_evaluator.params import (
    ChoiceQuestion,
    JudgeParams,
    NoulQuestion,
    ScoreQuestion,
)

_OUTCOME_FIELDS = ("request", "tool_calls", "final_answer")
# Questions name these fields in backticks, so renaming one silently degrades
# every question a user has already written.
VIEW_FIELDS: dict[str, tuple[str, ...]] = {
    "outcome": _OUTCOME_FIELDS,
    "full": (*_OUTCOME_FIELDS, "system_prompt", "model_messages"),
}
_BACKTICKED = re.compile(r"`([^`]+)`")
_FIRST_SEGMENT = re.compile(r"[A-Za-z_]+")


def _resolve_text(document: Any, pointer: str | None) -> Any:
    """Return the value a JSON pointer selects, or None when it selects nothing."""
    if pointer is None:
        return None
    found, value = resolve_json_pointer(document, pointer)
    return value if found else None


def _sort_nodes(nodes: list[SessionNodeResponse]) -> list[SessionNodeResponse]:
    """Order nodes by start time, nodes without one kept last in recorded order."""
    # Pair each timed node with its original index so the sort key never needs
    # to narrow `started_at` back from `datetime | None`, and ties break by
    # recorded order instead of relying on Python's stable-sort side effect.
    timed = sorted(
        (
            (node.started_at, index, node)
            for index, node in enumerate(nodes)
            if node.started_at is not None
        ),
        key=lambda item: item[:2],
    )
    return [node for _, _, node in timed] + [
        node for node in nodes if node.started_at is None
    ]


def _find_request(session: SessionView, nodes: list[SessionNodeResponse]) -> Any:
    """Find what the user asked for."""
    selected = _resolve_text(
        session.session.inputs, session.session.input_text_selector
    )
    if selected is not None:
        return selected
    # Imported sessions wrap their inputs together with their outputs, so the
    # first node's own input text is the cleanest statement of the request.
    for node in nodes:
        selected = _resolve_text(node.inputs, node.input_text_selector)
        if selected is not None:
            return selected
    return session.session.inputs


def _find_system_prompt(nodes: list[SessionNodeResponse]) -> Any:
    for node in nodes:
        selected = _resolve_text(node.inputs, node.system_prompt_selector)
        if selected is not None:
            return selected
    return None


def _to_json_safe(value: Any) -> str:
    """Render a value json cannot encode, keeping dates in ISO form."""
    return value.isoformat() if isinstance(value, date) else str(value)


def build_state(session: SessionView, params: JudgeParams) -> dict[str, Any]:
    """Build the JSON state for the chosen view, narrowed by include."""
    nodes = _sort_nodes(session.nodes)
    llm_calls = [n for n in nodes if n.node_type is NodeType.LLM_CALL]
    fields: dict[str, Any] = {
        "request": _find_request(session, nodes),
        "tool_calls": [
            {
                "tool": n.tool_name or n.name,
                "arguments": n.inputs,
                "result": n.outputs,
                "error": n.error,
            }
            for n in nodes
            if n.node_type is NodeType.TOOL_CALL
        ],
        "final_answer": session.session.outputs,
        "system_prompt": _find_system_prompt(llm_calls),
        "model_messages": [
            {"model": n.model, "input": n.inputs, "output": n.outputs}
            for n in llm_calls
        ],
    }
    keep = params.include or VIEW_FIELDS[params.state]
    state = {name: fields[name] for name in VIEW_FIELDS[params.state] if name in keep}
    # Recorded payloads can hold datetimes and decimals the SDK cannot serialize.
    return json.loads(json.dumps(state, default=_to_json_safe))


def _get_question_texts(
    question: NoulQuestion | ChoiceQuestion | ScoreQuestion,
) -> list[str]:
    texts = [question.instructions]
    if isinstance(question, ChoiceQuestion):
        texts += [*question.criteria, *(d for d in question.criteria.values() if d)]
    if isinstance(question, ScoreQuestion):
        texts += question.criteria
    return texts


def check_params_against_view(params: JudgeParams) -> None:
    """Reject include names and question field references the state will not contain."""
    view_fields = VIEW_FIELDS[params.state]
    unknown = [name for name in params.include or [] if name not in view_fields]
    if unknown:
        raise ValueError(
            f"include names {unknown} are not fields of the '{params.state}' "
            f"view {list(view_fields)}"
        )
    sent = set(params.include or view_fields)
    known = set(VIEW_FIELDS["full"])
    for question_id, question in params.questions.items():
        for text in _get_question_texts(question):
            for reference in _BACKTICKED.findall(text):
                match = _FIRST_SEGMENT.match(reference)
                field = match.group(0) if match else None
                if field in known and field not in sent:
                    hint = (
                        "use the 'full' view"
                        if field not in view_fields
                        else "add it to include"
                    )
                    raise ValueError(
                        f"Question '{question_id}' refers to `{field}`, which is "
                        f"not sent to jev; {hint}."
                    )
