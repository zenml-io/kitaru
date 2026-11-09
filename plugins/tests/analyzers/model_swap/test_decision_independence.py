# Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Decision requests must not disclose their reference answers."""

import json
import re
import time
from dataclasses import replace
from decimal import Decimal
from typing import Any
from uuid import uuid4

import httpx
import pytest

from kitaru_model_swap.calls import RecordedCall
from kitaru_model_swap.decision_plan import build_state, plan_decisions
from kitaru_model_swap.decision_report import build_decision_candidate_report
from kitaru_model_swap.decisions import build_questions_request
from kitaru_model_swap.endpoint import Usage
from kitaru_model_swap.normalize import RecordedOutcome, ReplayRequest, ToolCall
from kitaru_model_swap.params import DecisionCandidate, ModelSwapParams
from kitaru_model_swap.runner import Runner


def _get_request(outcome: RecordedOutcome) -> ReplayRequest:
    return ReplayRequest(
        messages=[{"role": "user", "content": "Resolve this defective ticket."}],
        tools=[
            {
                "type": "function",
                "function": {
                    "name": name,
                    "parameters": {
                        "type": "object",
                        "properties": {"urgent": {"type": "boolean"}},
                    },
                },
            }
            for name in ("refund", "escalate")
        ],
        schema_source="recorded",
        source_format="openai_chat",
        recorded=outcome,
    )


@pytest.mark.parametrize("kind", ["jev", "clef", "openai_decisions"])
def test_requests_are_independent_of_recorded_tool_and_arguments(kind: str) -> None:
    candidate = DecisionCandidate.model_validate(
        {
            "name": "decision",
            "kind": kind,
            "base_url": "https://mock",
            "api_key_env": "DECISION_TEST_KEY",
        }
    )
    outcomes = [
        RecordedOutcome((ToolCall("refund", {"urgent": True}),), None),
        RecordedOutcome((ToolCall("escalate", {"urgent": False}),), None),
        RecordedOutcome((ToolCall("refund", {}),), None),
        RecordedOutcome((), "A text reply."),
    ]
    payloads = []
    for outcome in outcomes:
        request = _get_request(outcome)
        plan = plan_decisions(request)
        payloads.append(
            build_questions_request(candidate, build_state(request), plan.questions)
        )

    assert all(payload == payloads[0] for payload in payloads)


async def test_reading_question_clues_does_not_reveal_the_recorded_tool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DECISION_TEST_KEY", "test-key")
    candidate = DecisionCandidate.model_validate(
        {
            "name": "clue-reader",
            "kind": "jev",
            "base_url": "https://mock",
            "api_key_env": "DECISION_TEST_KEY",
        }
    )
    calls = [
        RecordedCall(
            uuid4(),
            uuid4(),
            "resolve",
            "recorded",
            _get_request(RecordedOutcome((ToolCall(name, {"urgent": True}),), None)),
            Usage(input_tokens=10, output_tokens=10),
            Decimal(".01"),
            10,
        )
        for name in ("refund", "escalate")
    ]
    plans = [(call, plan_decisions(call.request)) for call in calls]
    payloads: list[dict[str, Any]] = []

    def handle(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        payloads.append(payload)
        questions = payload["questions"]
        clue = next(q["instructions"] for k, q in questions.items() if k != "tool")
        match = re.search(r"If the assistant calls (\w+),", clue)
        assert match is not None
        answers = {"tool": {"choice": match[1]}}
        answers.update({key: {"choice": "true"} for key in questions if key != "tool"})
        return httpx.Response(200, json={"answers": answers})

    params = ModelSwapParams(decision_candidates=[candidate])
    runner = Runner(params, {}, transport=httpx.MockTransport(handle))
    results = await runner.run_decisions(plans, deadline=time.monotonic() + 10)
    report = build_decision_candidate_report(
        candidate, plans, results, None, dry_run=False
    )

    assert report["tool_choice_rate"] == 0.5
    assert payloads[0] == payloads[1]


def test_question_limit_does_not_depend_on_recorded_arguments() -> None:
    request = _get_request(RecordedOutcome((ToolCall("refund", {"f79": True}),), None))
    request.tools[0]["function"]["parameters"]["properties"] = {
        f"f{index}": {"type": "boolean"} for index in range(80)
    }
    alternative = replace(
        request,
        recorded=RecordedOutcome((ToolCall("escalate", {"urgent": True}),), None),
    )

    first = plan_decisions(request)
    second = plan_decisions(alternative)

    assert first.questions == second.questions
    assert len(first.questions) == 60
    assert first.shape == second.shape == "partial"
