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
"""The spend cap holds for judges, unknown usage, timeouts and retries."""

import asyncio
import json
import time
from decimal import Decimal
from typing import Any
from uuid import uuid4

import httpx
import pytest

from kitaru_model_swap import analyzer as analyzer_module
from kitaru_model_swap import endpoint as endpoint_module
from kitaru_model_swap.calls import RecordedCall
from kitaru_model_swap.endpoint import Usage
from kitaru_model_swap.normalize import RecordedOutcome, ReplayRequest
from kitaru_model_swap.params import Candidate, ModelSwapParams
from kitaru_model_swap.pricing import resolve_price
from kitaru_model_swap.runner import Runner

CAP = Decimal("0.0002")


def call() -> RecordedCall:
    return RecordedCall(
        uuid4(),
        uuid4(),
        "answer",
        "recorded",
        ReplayRequest(
            [{"role": "user", "content": "hi"}],
            [],
            "none",
            "openai_chat",
            RecordedOutcome((), "recorded"),
        ),
        Usage(input_tokens=1, output_tokens=1),
        Decimal(".01"),
        1,
    )


def candidate(name: str = "candidate", **settings: Any) -> Candidate:
    """A candidate whose 100-token limit costs $0.0001 at $1 per million."""
    return Candidate(
        name=name,
        base_url="https://mock/v1",
        model=name,
        price={"input_per_mtok": "0", "output_per_mtok": "1"},
        provider_settings={"max_tokens": 100, "max_retries": 0} | settings,
    )


def response(
    text: str = "different", output: int = 100, *, usage: bool = True
) -> httpx.Response:
    payload: dict[str, Any] = {
        "choices": [{"message": {"content": text}, "finish_reason": "stop"}]
    }
    if usage:
        payload["usage"] = {"prompt_tokens": 1, "completion_tokens": output}
    return httpx.Response(200, json=payload)


async def test_judge_reserves_its_own_output_limit() -> None:
    target = candidate()
    judge = candidate("judge", max_tokens=10000)
    params = ModelSwapParams(
        candidates=[target], judge=judge, max_spend_usd=CAP, concurrency=1
    )
    bodies: list[dict[str, Any]] = []

    def handle(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        bodies.append(body)
        if body["model"] == "judge":
            return response('{"equivalent": true}', 10000)
        return response()

    runner = Runner(
        params,
        {target.name: resolve_price(target)},
        judge_price=resolve_price(judge),
        transport=httpx.MockTransport(handle),
    )
    [result] = await runner.run([call()])

    # The judge's 10,000-token limit can cost $0.01, far over the cap, so it
    # is not asked rather than sent with a 50-token reservation.
    assert [body["model"] for body in bodies] == ["candidate"]
    assert result.judge_failure == "max_spend"
    assert runner.budget.spent <= CAP


async def test_replies_without_usage_are_charged_their_reservation() -> None:
    target = candidate()
    params = ModelSwapParams(candidates=[target], max_spend_usd=CAP, concurrency=1)
    sent = 0

    def handle(_: httpx.Request) -> httpx.Response:
        nonlocal sent
        sent += 1
        return response(usage=False)

    runner = Runner(
        params,
        {target.name: resolve_price(target)},
        transport=httpx.MockTransport(handle),
    )
    results = await runner.run([call() for _ in range(5)])

    assert sent == 2
    assert runner.budget.spent == CAP
    assert runner.unmetered_attempts == 2
    assert [r.not_run for r in results] == [None, None] + ["max_spend"] * 3
    # Unknown usage is not a measured zero.
    assert [r.cost for r in results[:2]] == [None, None]


async def test_timed_out_requests_are_charged_their_reservation() -> None:
    target = candidate()
    params = ModelSwapParams(candidates=[target], max_spend_usd=CAP, concurrency=1)
    sent = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal sent
        sent += 1
        raise httpx.ReadTimeout("stalled after generating", request=request)

    runner = Runner(
        params,
        {target.name: resolve_price(target)},
        transport=httpx.MockTransport(handle),
    )
    results = await runner.run([call() for _ in range(5)])

    assert sent == 2
    assert runner.budget.spent == CAP
    assert [r.reply.failure if r.reply else r.not_run for r in results] == [
        "timeout",
        "timeout",
        "max_spend",
        "max_spend",
        "max_spend",
    ]


async def test_requests_that_never_connected_are_not_charged() -> None:
    target = candidate()
    params = ModelSwapParams(candidates=[target], max_spend_usd=CAP, concurrency=1)

    def handle(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    runner = Runner(
        params,
        {target.name: resolve_price(target)},
        transport=httpx.MockTransport(handle),
    )
    results = await runner.run([call() for _ in range(3)])

    assert runner.budget.spent == 0
    assert all(r.reply and r.reply.failure == "http_error" for r in results)


async def test_retries_reserve_room_for_each_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Room for exactly two attempts: the first and one retry.
    target = candidate(max_retries=3)
    params = ModelSwapParams(candidates=[target], max_spend_usd=CAP, concurrency=1)
    sent = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal sent
        sent += 1
        raise httpx.ReadTimeout("stalled", request=request)

    runner = Runner(
        params,
        {target.name: resolve_price(target)},
        transport=httpx.MockTransport(handle),
    )
    real_sleep = asyncio.sleep

    async def no_backoff(_: float) -> None:
        await real_sleep(0)

    monkeypatch.setattr(endpoint_module.asyncio, "sleep", no_backoff)
    [result] = await runner.run([call()])

    assert sent == 2
    assert result.reply is not None
    assert "spend cap" in (result.reply.failure_detail or "")
    assert runner.budget.spent == CAP


async def test_measured_usage_is_still_charged_as_measured() -> None:
    target = candidate()
    params = ModelSwapParams(candidates=[target], max_spend_usd=CAP, concurrency=1)
    runner = Runner(
        params,
        {target.name: resolve_price(target)},
        transport=httpx.MockTransport(lambda _: response(output=10)),
    )
    [result] = await runner.run([call()])

    assert result.cost == Decimal("0.00001")
    assert runner.budget.spent == Decimal("0.00001")
    assert runner.unmetered_attempts == 0


async def test_dashboard_lookup_stays_within_the_time_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SlowInfo:
        base_url = "http://localhost:8000"
        sessions = None

        def __init__(self) -> None:
            self.info = self

        async def __aenter__(self) -> "SlowInfo":
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def get(self) -> None:
            await asyncio.sleep(0.5)

    monkeypatch.setattr(analyzer_module, "KitaruAPIClient", SlowInfo)
    started = time.monotonic()
    await analyzer_module.run_model_swap(
        [],
        ModelSwapParams(
            candidates=[candidate()], time_budget_seconds=0.02, dry_run=True
        ),
    )
    assert time.monotonic() - started < 0.3
