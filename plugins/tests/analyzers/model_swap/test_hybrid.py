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
"""Tests for best mixes, model origins, repeats, and the cost-per-task note."""

import json
from typing import Any

import httpx
import pytest

from kitaru.api_models.v1.session_node import SessionWithNodesResponse
from kitaru_model_swap.params import Candidate, ModelSwapParams

from .helpers import MockOpenAIServer, StubClient, completion
from .test_analyzer import CHEAP, REFERENCE, escalator, mirror, report, run

CHINA_LAB = {"lab": "Example Lab", "lab_country": "CN", "host": "Example Cloud"}


def finals_only(sessions: list[SessionWithNodesResponse]) -> Any:
    """Answer final steps like the recording and escalate everything else."""
    replay = mirror(sessions)

    def reply(body: dict[str, Any]) -> httpx.Response:
        recorded = replay(body)
        calls = recorded.json()["choices"][0]["message"].get("tool_calls") or []
        if any(call["function"]["name"] == "final_result" for call in calls):
            return recorded
        return completion(tool_calls=[("escalate_to_human", {"reason": "x"})])

    return reply


@pytest.fixture
def mixed_server(
    quickstart_sessions: list[SessionWithNodesResponse],
) -> MockOpenAIServer:
    return MockOpenAIServer(
        {
            "mirror": mirror(quickstart_sessions),
            "finals": finals_only(quickstart_sessions),
            "escalator": escalator,
        }
    )


FINALS = CHEAP | {"name": "finals-only", "model": "finals"}


async def test_best_mix_routes_each_kind_of_call(
    stub_client: StubClient, mixed_server: MockOpenAIServer
) -> None:
    insights = await run(stub_client, mixed_server, candidates=[REFERENCE, FINALS])
    result = report(insights)
    mix = result["best_mix"]
    by_group = {item["group"]: item for item in mix["groups"]}

    assert by_group["final_result"]["candidate"] == "finals-only"
    # The reference is today's model, so other calls stay on today's model.
    assert by_group["lookup_order"]["candidate"] is None
    assert sum(item["share"] for item in mix["groups"]) == pytest.approx(1, abs=0.01)
    assert mix["cost"]["per_call_usd"] is not None
    goals = {choice["goal"]: choice for choice in result["best_for"]}
    assert "finals-only" in goals["Best mix"]["candidate"]


async def test_no_mix_when_nothing_can_move(
    stub_client: StubClient, mixed_server: MockOpenAIServer
) -> None:
    insights = await run(
        stub_client,
        mixed_server,
        candidates=[REFERENCE, CHEAP | {"name": "escalates", "model": "escalator"}],
    )
    result = report(insights)

    # Escalating only matches recorded escalations, which is too little to
    # move any kind of call but escalations themselves.
    mix = result["best_mix"]
    moved = [
        item["group"] for item in (mix or {"groups": []})["groups"] if item["candidate"]
    ]
    assert moved in ([], ["escalate_to_human"])


def test_excluded_origins_match_case_insensitively() -> None:
    params = ModelSwapParams.model_validate(
        {"candidates": [CHEAP], "excluded_origins": {"lab_country": ["cn"]}}
    )

    assert params.is_excluded(Candidate.model_validate(CHEAP | {"origin": CHINA_LAB}))
    assert not params.is_excluded(Candidate.model_validate(CHEAP))
    assert not params.is_excluded(
        Candidate.model_validate(CHEAP | {"origin": {"lab_country": "US"}})
    )


async def test_excluded_candidates_are_compared_but_never_recommended(
    stub_client: StubClient, mixed_server: MockOpenAIServer
) -> None:
    excluded = FINALS | {"origin": CHINA_LAB, "open_weights": True}

    insights = await run(
        stub_client,
        mixed_server,
        candidates=[REFERENCE, excluded],
        excluded_origins={"lab_country": ["CN"]},
    )
    result = report(insights)
    finals = next(c for c in result["candidates"] if c["name"] == "finals-only")

    assert finals["excluded"] is True
    assert finals["origin"]["lab_country"] == "CN"
    assert all(choice["candidate"] != "finals-only" for choice in result["best_for"])
    mix = result["best_mix"]
    assert mix is None or all(
        item["candidate"] != "finals-only" for item in mix["groups"]
    )
    html = str(insights["model-swap-summary"].metadata["kitaru.report_section/v1"])
    assert "(excluded)" in html
    assert "served by Example Cloud" in html


async def test_repeats_measure_consistency(
    stub_client: StubClient, quickstart_sessions: list[SessionWithNodesResponse]
) -> None:
    replay = mirror(quickstart_sessions)
    seen: dict[str, int] = {}

    def flaky(body: dict[str, Any]) -> httpx.Response:
        # The first attempt at each call answers like the recording; the
        # second does not.
        key = json.dumps(body["messages"], sort_keys=True)
        seen[key] = seen.get(key, 0) + 1
        return replay(body) if seen[key] == 1 else escalator(body)

    server = MockOpenAIServer({"flaky": flaky})
    insights = await run(
        stub_client,
        server,
        candidates=[CHEAP | {"model": "flaky"}],
        max_calls=6,
        repeats=2,
        concurrency=1,
    )
    candidate = report(insights)["candidates"][0]

    assert len(server.requests) == 12
    assert candidate["calls_sent"] == 12
    assert candidate["answered"] == 6
    assert candidate["consistency"] == {
        "repeats": 2,
        "calls": 6,
        "any_match_rate": 1.0,
        "all_match_rate": 0.0,
    }


async def test_repeats_multiply_the_estimate(
    stub_client: StubClient, mixed_server: MockOpenAIServer
) -> None:
    once = report(
        await run(stub_client, mixed_server, candidates=[REFERENCE], dry_run=True)
    )
    twice = report(
        await run(
            stub_client, mixed_server, candidates=[REFERENCE], dry_run=True, repeats=2
        )
    )

    assert (
        twice["candidates"][0]["estimate"]["calls"]
        == 2 * once["candidates"][0]["estimate"]["calls"]
    )


async def test_reports_say_costs_assume_the_same_steps(
    stub_client: StubClient, mixed_server: MockOpenAIServer
) -> None:
    result = report(
        await run(stub_client, mixed_server, candidates=[REFERENCE], max_calls=3)
    )

    assert any("same steps as the recording" in note for note in result["notes"])


async def test_tokens_per_call_lead_the_comparison(
    stub_client: StubClient, mixed_server: MockOpenAIServer
) -> None:
    insights = await run(stub_client, mixed_server, candidates=[REFERENCE], max_calls=3)
    html = str(insights["model-swap-summary"].metadata["kitaru.report_section/v1"])

    assert "Tokens per call" in html
    assert json.dumps(report(insights)["candidates"][0]["tokens_per_call"])
