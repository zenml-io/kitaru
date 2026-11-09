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
"""Tests for finding and trying calls a decision model could make."""

import json
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from kitaru_model_swap.analyzer import run_model_swap
from kitaru_model_swap.calls import Collection
from kitaru_model_swap.decision_plan import (
    TEXT_ANSWER,
    TOOL_QUESTION,
    plan_decisions,
)
from kitaru_model_swap.decisions import Question, build_questions_request, read_answers
from kitaru_model_swap.normalize import RecordedOutcome, ReplayRequest, ToolCall
from kitaru_model_swap.params import DecisionCandidate, ModelSwapParams

from .helpers import StubClient, completion
from .test_analyzer import CHEAP, report

ACTION_SCHEMA = {
    "$defs": {"Action": {"enum": ["refund", "reject"], "type": "string"}},
    "properties": {
        "action": {"$ref": "#/$defs/Action"},
        "urgent": {"type": "boolean"},
        "note": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "priority": {"anyOf": [{"enum": ["low", "high"]}, {"type": "null"}]},
    },
    "type": "object",
}


def request(
    *calls: ToolCall, text: str | None = None, tools: bool = True
) -> ReplayRequest:
    definitions = (
        [
            {
                "type": "function",
                "function": {
                    "name": "decide",
                    "description": "Settle the ticket.",
                    "parameters": ACTION_SCHEMA,
                },
            },
            {"type": "function", "function": {"name": "lookup", "parameters": {}}},
        ]
        if tools
        else []
    )
    return ReplayRequest(
        messages=[{"role": "user", "content": "help"}],
        tools=definitions,
        schema_source="recorded",
        source_format="openai_chat",
        recorded=RecordedOutcome(tool_calls=calls, text=text),
    )


def test_enum_and_boolean_arguments_make_a_decision_call() -> None:
    plan = plan_decisions(
        request(
            ToolCall("decide", {"action": "refund", "urgent": True, "priority": "low"})
        )
    )

    assert plan.shape == "decision"
    assert plan.expected == {
        TOOL_QUESTION: "decide",
        "arg_0_action": "refund",
        "arg_1_urgent": "true",
        "arg_2_priority": "low",
    }
    assert plan.arguments == {
        "arg_0_action": "action",
        "arg_1_urgent": "urgent",
        "arg_2_priority": "priority",
    }
    options = {q.key: list(q.options or {}) for q in plan.questions}
    assert options["arg_0_action"] == ["refund", "reject"]
    assert TEXT_ANSWER in options[TOOL_QUESTION]


def test_free_form_arguments_make_a_partial_call() -> None:
    plan = plan_decisions(
        request(ToolCall("decide", {"action": "refund", "note": "x"}))
    )

    assert plan.shape == "partial"
    assert plan.reason == "free-form arguments: note"
    assert "arg_0_action" in (plan.expected or {})


def test_a_value_outside_the_enum_is_free_form() -> None:
    plan = plan_decisions(request(ToolCall("decide", {"action": "maybe"})))

    assert plan.shape == "partial"


def test_a_text_answer_is_a_partial_decision() -> None:
    plan = plan_decisions(request(text="Sure, done."))

    assert plan.shape == "partial"
    assert plan.expected == {TOOL_QUESTION: TEXT_ANSWER}


@pytest.mark.parametrize(
    ("built", "reason"),
    [
        (request(text="hi", tools=False), "no tools to choose from"),
        (
            request(ToolCall("lookup", {}), ToolCall("decide", {"action": "refund"})),
            "several tool calls at once",
        ),
    ],
)
def test_generative_calls_name_why(built: ReplayRequest, reason: str) -> None:
    plan = plan_decisions(built)

    assert plan.shape == "generative"
    assert plan.reason == reason
    assert plan.questions == ()


def test_quickstart_calls_are_classified(quickstart_sessions: list[Any]) -> None:
    params = ModelSwapParams.model_validate({"candidates": [CHEAP]})
    collection = Collection(max_per_group=100)
    for session in quickstart_sessions:
        collection.add_session(session, params)
    plans = [plan_decisions(call.request) for call in collection.calls]

    assert {plan.shape for plan in plans} <= {"partial", "generative"}
    final = next(
        p for p in plans if (p.expected or {}).get(TOOL_QUESTION) == "final_result"
    )
    # Pydantic AI writes the action enum through $defs.
    assert final.expected is not None and final.expected["arg_0_action"] in {
        "refund",
        "replacement",
        "escalate",
        "reject",
    }


CHOICE = Question("tool", "Which tool?", {"decide": "Settle.", "lookup": None})


def test_choice_questions_in_both_request_shapes() -> None:
    jev = DecisionCandidate.model_validate({"name": "j", "kind": "jev"})
    openai = DecisionCandidate.model_validate({"name": "o", "kind": "openai_decisions"})

    _, system_one = build_questions_request(jev, {"conversation": []}, [CHOICE])
    url, decisions = build_questions_request(openai, {"conversation": []}, [CHOICE])

    assert system_one["questions"]["tool"] == {
        "type": "choice",
        "instructions": "Which tool?",
        "criteria": {"decide": "Settle.", "lookup": None},
    }
    assert url.endswith("/decisions")
    assert decisions["questions"][0]["choices"] == [
        {"value": "decide", "description": "Settle."},
        {"value": "lookup", "description": "lookup"},
    ]
    assert json.loads(decisions["input"]) == {"conversation": []}


@pytest.mark.parametrize(
    ("payload", "values", "failure"),
    [
        (
            {"answers": {"tool": {"choice": "decide", "confidence": 0.9}}},
            {"tool": "decide"},
            None,
        ),
        (
            {"answers": [{"type": "choice", "name": "tool", "choice": "lookup"}]},
            {"tool": "lookup"},
            None,
        ),
        ({"answers": {"tool": {"choice": "elsewhere"}}}, {}, "unreadable_answer"),
        ({"answers": {}}, {}, "unreadable_answer"),
    ],
)
def test_reads_choice_answers(
    payload: Any, values: dict[str, str], failure: str | None
) -> None:
    answers = read_answers(payload, [CHOICE])

    assert answers.values == values
    assert answers.failure == failure


def test_a_candidate_or_decision_candidate_is_required() -> None:
    with pytest.raises(ValidationError, match="decision_candidates"):
        ModelSwapParams.model_validate({"candidates": []})


def test_names_are_unique_across_both_kinds_of_candidates() -> None:
    with pytest.raises(ValidationError, match="unique"):
        ModelSwapParams.model_validate(
            {
                "candidates": [CHEAP],
                "decision_candidates": [{"name": CHEAP["name"], "kind": "jev"}],
            }
        )


def always_decides(request: httpx.Request) -> httpx.Response:
    """A System One server that always picks final_result and refund."""
    body = json.loads(request.content)
    answers: dict[str, Any] = {}
    for key, question in body["questions"].items():
        options = list(question["criteria"])
        pick = (
            "final_result"
            if key == "tool" and "final_result" in options
            else options[0]
        )
        if key.endswith("_action"):
            pick = "refund"
        answers[key] = {"choice": pick, "confidence": 0.9}
    return httpx.Response(
        200, json={"answers": answers, "usage": {"input_tokens": 500}}
    )


JEV = {
    "name": "jev",
    "kind": "jev",
    "price": {"input_per_mtok": "0.042", "output_per_mtok": "0"},
}


async def test_decision_candidates_end_to_end(
    stub_client: StubClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TYPESAFE_API_KEY", "ts-test")

    insights = await run_model_swap(
        list(stub_client._by_id),
        ModelSwapParams.model_validate(
            {"decision_candidates": [JEV], "max_spend_usd": "1"}
        ),
        transport=httpx.MockTransport(always_decides),
    )
    result = report({insight.name: insight for insight in insights})
    shapes = result["decision_shapes"]
    jev = result["decision_candidates"][0]

    assert shapes == shapes | {
        "calls": 33,
        "decision": 0,
        "partial": 25,
        "generative": 8,
    }
    assert jev["calls_asked"] == 25
    assert jev["failures"] == {}
    # It picks final_result every time, so it matches exactly the final steps.
    assert jev["tool_choice_rate"] == pytest.approx(10 / 25, abs=0.001)
    assert jev["argument_rates"]["action"] is not None
    assert jev["cost_per_call_usd"] == "0.00002100"
    assert result["candidates"] == []
    assert "model-swap-decision-shapes" in {insight.name for insight in insights}


async def test_decision_candidates_in_a_dry_run(
    stub_client: StubClient,
) -> None:
    insights = await run_model_swap(
        list(stub_client._by_id),
        ModelSwapParams.model_validate({"decision_candidates": [JEV], "dry_run": True}),
        transport=httpx.MockTransport(always_decides),
    )
    jev = report({insight.name: insight for insight in insights})[
        "decision_candidates"
    ][0]

    assert jev["estimate"]["calls"] == 25
    assert jev["estimate"]["spend_usd"] is not None


def plan_for(schema: dict[str, Any], arguments: dict[str, Any]) -> Any:
    built = ReplayRequest(
        messages=[{"role": "user", "content": "help"}],
        tools=[{"type": "function", "function": {"name": "act", "parameters": schema}}],
        schema_source="recorded",
        source_format="openai_chat",
        recorded=RecordedOutcome(tool_calls=(ToolCall("act", arguments),), text=None),
    )
    return plan_decisions(built)


@pytest.mark.parametrize(
    "schema",
    [
        {"properties": {"x": {"type": ["boolean", "null"]}}},
        {"properties": {"x": {"anyOf": [{"type": "boolean"}, {"enum": ["maybe"]}]}}},
        {
            "properties": {"x": {"allOf": [{"$ref": "#/definitions/Flag"}]}},
            "definitions": {"Flag": {"type": "boolean"}},
        },
    ],
)
def test_more_schema_shapes_are_decisions(schema: dict[str, Any]) -> None:
    assert plan_for(schema, {"x": True}).shape == "decision"


def test_a_single_option_is_not_a_decision() -> None:
    plan = plan_for({"properties": {"x": {"enum": [None, "only"]}}}, {"x": "only"})

    assert plan.shape == "partial"


def test_keys_stay_unique_after_unsafe_characters_are_replaced() -> None:
    plan = plan_for(
        {"properties": {"a b": {"type": "boolean"}, "a_b": {"type": "boolean"}}},
        {"a b": True, "a_b": False},
    )

    keys = [q.key for q in plan.questions]
    assert len(keys) == len(set(keys)) == 3
    assert all("." not in key for key in keys)


def test_questions_per_request_are_capped() -> None:
    properties = {f"f{i}": {"type": "boolean"} for i in range(80)}

    plan = plan_for({"properties": properties}, dict.fromkeys(properties, True))

    assert len(plan.questions) == 60
    assert plan.shape == "partial"


def test_inferred_schemas_are_not_trusted() -> None:
    built = request(ToolCall("decide", {"action": "refund"}))
    inferred = ReplayRequest(
        messages=built.messages,
        tools=built.tools,
        schema_source="inferred",
        source_format=built.source_format,
        recorded=built.recorded,
    )

    assert plan_decisions(inferred).shape == "generative"


def test_a_tool_named_like_the_text_answer_is_generative() -> None:
    built = ReplayRequest(
        messages=[{"role": "user", "content": "hi"}],
        tools=[
            {"type": "function", "function": {"name": TEXT_ANSWER, "parameters": {}}}
        ],
        schema_source="recorded",
        source_format="openai_chat",
        recorded=RecordedOutcome(tool_calls=(), text="hello"),
    )

    assert plan_decisions(built).shape == "generative"


async def test_decision_only_runs_summarize_and_chart_the_estimate(
    stub_client: StubClient,
) -> None:
    insights = await run_model_swap(
        list(stub_client._by_id),
        ModelSwapParams.model_validate({"decision_candidates": [JEV], "dry_run": True}),
        transport=httpx.MockTransport(always_decides),
    )
    by_name = {insight.name: insight for insight in insights}
    summary = by_name["model-swap-summary"].data

    assert getattr(summary, "content", "").count("| Candidate |") == 0
    assert "would be asked about 25 calls" in getattr(summary, "content", "")
    cost = by_name["model-swap-cost"].data
    assert [v.label for v in getattr(cost, "values", [])] == ["jev"]


async def test_decision_requests_run_before_candidates(
    stub_client: StubClient,
    quickstart_sessions: list[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TYPESAFE_API_KEY", "ts-test")
    order: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        order.append(
            "decision" if request.url.path.endswith("/v1/systemone") else "llm"
        )
        if order[-1] == "decision":
            return always_decides(request)
        return completion(tool_calls=[("escalate_to_human", {"reason": "x"})])

    await run_model_swap(
        list(stub_client._by_id),
        ModelSwapParams.model_validate(
            {"candidates": [CHEAP], "decision_candidates": [JEV], "max_calls": 5}
        ),
        transport=httpx.MockTransport(handle),
    )

    assert order[0] == "decision"
    assert order.index("llm") > max(
        i for i, kind in enumerate(order) if kind == "decision"
    )
