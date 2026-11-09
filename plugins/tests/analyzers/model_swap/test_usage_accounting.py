# Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Unknown usage must retain reservations across every endpoint type."""

import time
from dataclasses import replace
from decimal import Decimal
from typing import Any

import httpx
import pytest

from kitaru_model_swap.decision_plan import build_state, plan_decisions
from kitaru_model_swap.decisions import Question, ask, decide, read_decision
from kitaru_model_swap.endpoint import read_usage
from kitaru_model_swap.normalize import RecordedOutcome, ReplayRequest, ToolCall
from kitaru_model_swap.params import DecisionCandidate, DecisionJudge, ModelSwapParams
from kitaru_model_swap.pricing import ResolvedPrice, resolve_price
from kitaru_model_swap.runner import Runner, decision_tokens

from .test_spend_cap import CAP, call, candidate


@pytest.mark.parametrize(
    "usage",
    [
        {},
        {"total_tokens": 100},
        {"prompt_tokens": 1},
        {"input_tokens": 1},
        {"prompt_tokens": 1, "completion_tokens": "100"},
        {"prompt_tokens": -1, "completion_tokens": 100},
        {"input_tokens": True, "output_tokens": 100},
        {"input_tokens": 1, "output_tokens": 0.5},
    ],
)
async def test_unusable_usage_retains_the_spend_reservation(usage: Any) -> None:
    target = candidate()
    params = ModelSwapParams(candidates=[target], max_spend_usd=CAP, concurrency=1)
    sent = 0

    def handle(_: httpx.Request) -> httpx.Response:
        nonlocal sent
        sent += 1
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": "answer"}}],
                "usage": usage,
            },
        )

    runner = Runner(
        params,
        {target.name: resolve_price(target)},
        transport=httpx.MockTransport(handle),
    )
    results = await runner.run([call() for _ in range(5)])

    assert sent == 2
    assert runner.budget.spent == CAP
    assert runner.unmetered_attempts == 2
    assert [result.cost for result in results[:2]] == [None, None]
    assert [result.not_run for result in results[2:]] == ["max_spend"] * 3


@pytest.mark.parametrize("failure", ["read_timeout", "read_error", "invalid_json"])
async def test_unmetered_decision_requests_retain_the_spend_reservation(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    monkeypatch.setenv("REVIEW_DECISION_KEY", "local-test-key")
    model = DecisionCandidate.model_validate(
        {
            "name": "decision",
            "kind": "jev",
            "base_url": "https://mock",
            "api_key_env": "REVIEW_DECISION_KEY",
            "price": {"input_per_mtok": "1", "output_per_mtok": "0"},
        }
    )
    recorded = replace(
        call(),
        request=ReplayRequest(
            [{"role": "user", "content": "Choose a tool."}],
            [{"type": "function", "function": {"name": "choose", "parameters": {}}}],
            "recorded",
            "openai_chat",
            RecordedOutcome((ToolCall("choose", {}),), None),
        ),
    )
    plan = plan_decisions(recorded.request)
    estimate = decision_tokens(model, build_state(recorded.request), plan.questions)
    cap = Decimal(estimate * 2) / Decimal(1_000_000)
    params = ModelSwapParams(
        decision_candidates=[model], max_spend_usd=cap, concurrency=1
    )
    sent = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal sent
        sent += 1
        if failure == "read_timeout":
            raise httpx.ReadTimeout("response lost", request=request)
        if failure == "read_error":
            raise httpx.ReadError("connection lost", request=request)
        return httpx.Response(200, content=b"not JSON")

    assert model.price is not None
    runner = Runner(
        params,
        {model.name: ResolvedPrice(model.price, "params")},
        transport=httpx.MockTransport(handle),
    )
    results = await runner.run_decisions(
        [(recorded, plan) for _ in range(5)], deadline=time.monotonic() + 10
    )

    assert sent == 2
    assert runner.budget.spent == cap
    assert runner.unmetered_attempts == 2
    assert [result.cost for result in results[:2]] == [None, None]
    assert [result.not_run for result in results[2:]] == ["max_spend"] * 3


async def test_a_decision_judge_preserves_unknown_usage() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("response lost", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        result = await decide(
            client,
            DecisionJudge.model_validate({"kind": "jev"}),
            api_key="local-test-key",
            first="a",
            second="b",
            estimated_tokens=100,
        )

    assert result.usage.reported is False
    assert result.unmetered_attempts == 1


@pytest.mark.parametrize(
    "error_type", [httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout]
)
async def test_unsent_decision_requests_are_not_billed(error_type) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        raise error_type("not sent", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        result = await ask(
            client,
            DecisionCandidate.model_validate({"name": "decision", "kind": "jev"}),
            api_key="local-test-key",
            state="a",
            questions=[Question("equivalent", "Are they equal?")],
            estimated_tokens=100,
        )

    assert result.usage.reported is True
    assert result.usage.input_tokens == 0
    assert result.unmetered_attempts == 0


def test_invalid_decision_usage_cannot_lower_the_estimate() -> None:
    result = read_decision(
        {"answers": {"equivalent": 0.9}, "usage": {"input_tokens": -10}},
        estimated_tokens=100,
    )

    assert result.usage.input_tokens == 100


def test_nonfinite_usage_is_unknown() -> None:
    usage = read_usage({"input_tokens": 1, "output_tokens": float("inf")})

    assert usage.reported is False
