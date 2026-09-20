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
"""Tests for the state jev receives."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from typesafe_helpers import build_view_from_imported

from kitaru.api_models.v1.session import SessionDetailResponse
from kitaru.api_models.v1.session_node import NodeType, SessionNodeResponse
from kitaru.task.evaluator import SessionView
from kitaru.task.importer import ImportedSession
from kitaru_langfuse_importer.importer import parse
from kitaru_typesafe_evaluator.params import JudgeParams
from kitaru_typesafe_evaluator.state import build_state, check_params_against_view

T0 = datetime(2026, 9, 1, tzinfo=UTC)
NOUL = {"type": "noul", "instructions": "Is `final_answer` polite?"}


def _node(node_type: NodeType, offset: int, **fields: Any) -> SessionNodeResponse:
    defaults: dict[str, Any] = {
        "name": "n",
        "inputs": None,
        "outputs": None,
        "error": None,
        "tool_name": None,
        "model": None,
        "input_text_selector": None,
        "system_prompt_selector": None,
        "started_at": T0 + timedelta(seconds=offset),
    }
    return SessionNodeResponse.model_construct(
        node_type=node_type, **(defaults | fields)
    )


def _view(nodes: list[SessionNodeResponse], **session: Any) -> SessionView:
    defaults = {"inputs": None, "outputs": None, "input_text_selector": None}
    return SessionView(
        session=SessionDetailResponse.model_construct(**(defaults | session)),
        nodes=nodes,
    )


def _params(**fields: Any) -> JudgeParams:
    return JudgeParams.model_validate({"questions": {"polite": NOUL}} | fields)


LLM_INPUTS = {
    "messages": [
        {"role": "system", "content": "Be kind."},
        {"role": "user", "content": "Refund please"},
    ]
}


def _imported_style_view() -> SessionView:
    """Shape of a Langfuse-imported session: wrapper inputs, no session selectors."""
    return _view(
        [
            _node(
                NodeType.TOOL_CALL,
                2,
                tool_name="issue_refund",
                inputs={"order": "1"},
                outputs=None,
                error="boom",
            ),
            _node(
                NodeType.LLM_CALL,
                0,
                model="gpt-5-nano",
                inputs=LLM_INPUTS,
                outputs=[{"text": "ok"}],
                input_text_selector="/messages/1/content",
                system_prompt_selector="/messages/0/content",
            ),
            _node(
                NodeType.TOOL_CALL,
                1,
                tool_name="lookup_order",
                inputs={"order": "1"},
                outputs={"found": True},
            ),
        ],
        inputs={
            "schema_version": 1,
            "turns": [{"inputs": {"body": "x"}, "outputs": {"action": "refund"}}],
        },
        outputs={"action": "refund"},
    )


def test_outcome_view_orders_tool_calls_and_finds_the_request() -> None:
    state = build_state(_imported_style_view(), _params())
    assert state == {
        "request": "Refund please",
        "tool_calls": [
            {
                "tool": "lookup_order",
                "arguments": {"order": "1"},
                "result": {"found": True},
                "error": None,
            },
            {
                "tool": "issue_refund",
                "arguments": {"order": "1"},
                "result": None,
                "error": "boom",
            },
        ],
        "final_answer": {"action": "refund"},
    }


def test_native_style_view_prefers_the_session_selector() -> None:
    """A natively recorded session points at its own request text."""
    view = _view(
        [],
        inputs={"prompt": "Where is my order?"},
        outputs="On its way.",
        input_text_selector="/prompt",
    )
    assert build_state(view, _params()) == {
        "request": "Where is my order?",
        "tool_calls": [],
        "final_answer": "On its way.",
    }


def test_request_falls_back_to_session_inputs() -> None:
    view = _view([], inputs={"prompt": "hi"}, outputs=None)
    assert build_state(view, _params())["request"] == {"prompt": "hi"}


def test_failed_session_sends_a_null_final_answer() -> None:
    assert build_state(_view([]), _params())["final_answer"] is None


def test_full_view_adds_system_prompt_and_messages() -> None:
    state = build_state(_imported_style_view(), _params(state="full"))
    assert list(state) == [
        "request",
        "tool_calls",
        "final_answer",
        "system_prompt",
        "model_messages",
    ]
    assert state["system_prompt"] == "Be kind."
    assert state["model_messages"] == [
        {"model": "gpt-5-nano", "input": LLM_INPUTS, "output": [{"text": "ok"}]}
    ]


def test_include_keeps_only_named_fields() -> None:
    state = build_state(
        _imported_style_view(),
        _params(state="full", include=["system_prompt", "final_answer"]),
    )
    assert state == {"system_prompt": "Be kind.", "final_answer": {"action": "refund"}}


def test_state_is_json_safe() -> None:
    """Datetimes and decimals in payloads must not break the request body."""
    view = _view([], inputs={"at": T0}, outputs=None)
    assert build_state(view, _params())["request"] == {"at": T0.isoformat()}


def test_include_must_name_fields_of_the_view() -> None:
    with pytest.raises(ValueError, match="system_prompt"):
        check_params_against_view(_params(include=["system_prompt"]))


def test_question_must_not_reference_a_dropped_field() -> None:
    params = JudgeParams.model_validate(
        {
            "include": ["final_answer"],
            "questions": {
                "grounded": {
                    "type": "noul",
                    "instructions": (
                        "Is `final_answer` backed by `tool_calls[0].result`?"
                    ),
                }
            },
        }
    )
    with pytest.raises(ValueError, match=r"grounded.*tool_calls"):
        check_params_against_view(params)


def test_question_must_not_reference_a_field_outside_the_view() -> None:
    params = JudgeParams.model_validate(
        {
            "questions": {
                "obeys": {
                    "type": "noul",
                    "instructions": "Does it follow `system_prompt`?",
                }
            }
        }
    )
    with pytest.raises(ValueError, match=r"obeys.*system_prompt.*full"):
        check_params_against_view(params)


TRACES = (
    Path(__file__).parents[3]
    / "examples/python/pydantic_ai_ticket_resolver/traces/langfuse-traces.jsonl"
)


def test_outcome_view_of_a_real_imported_session_reaches_the_refund() -> None:
    """`build_state` must work on a session shaped by a real importer, not a fixture."""
    imported = next(
        session
        for session in parse(TRACES.read_bytes(), {"source_instance": "state-test"})
        if isinstance(session, ImportedSession)
    )
    state = build_state(build_view_from_imported(imported), _params())

    assert isinstance(state["request"], str) and state["request"]
    assert "Merino Runners" in state["request"]
    assert "turns" not in state["request"]

    assert [call["tool"] for call in state["tool_calls"]] == [
        "lookup_order",
        "get_return_policy",
        "issue_refund",
    ]
    assert all(call["result"] is not None for call in state["tool_calls"])

    assert state["final_answer"]["action"] == "refund"


def test_backticked_text_that_is_not_a_field_is_ignored() -> None:
    params = JudgeParams.model_validate(
        {
            "questions": {
                "q": {
                    "type": "choice",
                    "instructions": "Did it call `issue_refund`?",
                    "criteria": {"yes": "`final_answer` says so", "no": None},
                }
            }
        }
    )
    check_params_against_view(params)
