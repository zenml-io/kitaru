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
"""Best for names only candidates measured on enough comparable calls."""

from dataclasses import replace
from typing import Any

import httpx

from kitaru_model_swap.params import ModelSwapParams
from kitaru_model_swap.pricing import resolve_price
from kitaru_model_swap.runner import CallResult, Runner

from .test_mix_weighting import call, candidate, reply, report


def goals(built: dict[str, Any]) -> dict[str, str]:
    return {row["goal"]: row["candidate"] for row in built["best_for"]}


def test_one_answer_earns_no_ranking() -> None:
    calls = [call(i) for i in range(100)]
    results = [reply(c, "reference", "1") for c in calls] + [
        reply(c, "cheap", "0.1")
        if i == 0
        else CallResult(call=c, candidate="cheap", not_run="max_spend")
        for i, c in enumerate(calls)
    ]
    built = report(calls, results)
    cheap = built["candidates"][1]

    assert cheap["coverage"] == {"answered": 1, "attempted": 100}
    assert cheap["measured"] is False
    assert goals(built) == {"Needs more measurement": "cheap"}
    assert "cheap 1 of 100" in built["best_for"][0]["why"]


async def test_budget_starved_candidate_stays_unconfirmed() -> None:
    # Through the real scheduler: the cheap candidate's large reservation fits
    # the cap once, then blocks its next 99 calls.
    cheap = candidate(
        "cheap",
        price={"input_per_mtok": "0", "output_per_mtok": "10"},
        provider_settings={"max_tokens": 100000},
    )
    reference = candidate(
        "reference",
        reference=True,
        price={"input_per_mtok": "0", "output_per_mtok": "100"},
        provider_settings={"max_tokens": 100},
    )
    params = ModelSwapParams(
        candidates=[cheap, reference], max_spend_usd=1, concurrency=1
    )

    def respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "tool_calls": [
                                {
                                    "id": "c",
                                    "type": "function",
                                    "function": {"name": "lookup", "arguments": "{}"},
                                }
                            ]
                        },
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 100, "completion_tokens": 10},
            },
        )

    runner = Runner(
        params,
        {c.name: resolve_price(c) for c in params.candidates},
        transport=httpx.MockTransport(respond),
    )
    calls = [call(i) for i in range(100)]
    built = report(calls, await runner.run(calls))
    by_name = {c["name"]: c for c in built["candidates"]}

    assert by_name["cheap"]["answered"] == 1
    assert by_name["reference"]["answered"] == 100
    assert goals(built) == {"Needs more measurement": "cheap"}


def test_comparability_uses_the_calls_both_answered() -> None:
    # Each side answered 85 of 100 calls (enough to be ranked), but not the
    # same ones. The candidate's 91% overall looks within 10 points of the
    # reference's 100%, yet on the 70 calls both answered it matches 89%.
    calls = [call(i) for i in range(100)]
    results = [
        reply(c, "reference", "1")
        if i < 85
        else CallResult(call=c, candidate="reference", not_run="max_spend")
        for i, c in enumerate(calls)
    ]
    for i, c in enumerate(calls):
        if i < 15:
            results.append(CallResult(call=c, candidate="cheap", not_run="max_spend"))
        elif i < 23:
            results.append(replace(reply(c, "cheap"), agreement="different_tool"))
        else:
            results.append(reply(c, "cheap"))
    built = report(calls, results)
    cheap = built["candidates"][1]

    assert cheap["tool_choice_rate"] == 0.9059
    assert cheap["paired"] == {"calls": 70, "rate": 0.8857, "reference_rate": 1.0}
    assert goals(built)["Closest to today"] == "cheap"
    assert "Cheapest" not in goals(built)
    assert "Fastest" not in goals(built)


def test_rankings_show_how_many_calls_were_answered() -> None:
    calls = [call(i) for i in range(30)]
    results = [reply(c, "reference", "1") for c in calls] + [
        reply(c, "cheap", "0.1") for c in calls
    ]
    rows = {row["goal"]: row["why"] for row in report(calls, results)["best_for"]}

    assert "(answered 30 of 30 calls)" in rows["Closest to today"]
    assert "(answered 30 of 30 calls)" in rows["Cheapest"]
