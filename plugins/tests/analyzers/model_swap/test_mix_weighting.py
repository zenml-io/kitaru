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
"""The best mix weights steps by traffic and checks each step's reliability."""

from collections import Counter
from dataclasses import replace
from decimal import Decimal
from typing import Any
from uuid import UUID

from kitaru_model_swap.calls import Collection, RecordedCall
from kitaru_model_swap.endpoint import CandidateReply, Usage
from kitaru_model_swap.html import render_section
from kitaru_model_swap.normalize import RecordedOutcome, ReplayRequest, ToolCall
from kitaru_model_swap.params import Candidate, ModelSwapParams
from kitaru_model_swap.report import _is_learning_session, build_report
from kitaru_model_swap.runner import CallResult


def candidate(name: str, **fields: Any) -> Candidate:
    return Candidate(
        name=name, model=name, base_url="https://example.test/v1", **fields
    )


def call(i: int, node: str = "chat", cost: str = "1") -> RecordedCall:
    return RecordedCall(
        session_id=UUID(int=i + 1),
        node_id=UUID(int=i + 1),
        node_name=node,
        model="old",
        usage=Usage(input_tokens=100),
        cost=Decimal(cost),
        estimated_input_tokens=100,
        request=ReplayRequest(
            messages=[{"role": "user", "content": str(i)}],
            tools=[],
            schema_source="none",
            source_format="openai_chat",
            recorded=RecordedOutcome(tool_calls=(ToolCall("lookup", {}),), text=None),
        ),
    )


def reply(c: RecordedCall, name: str, cost: str = "0.1") -> CallResult:
    return CallResult(
        call=c,
        candidate=name,
        cost=Decimal(cost),
        agreement="same_tool_same_args",
        reply=CandidateReply(
            tool_calls=c.request.recorded.tool_calls,
            text=None,
            usage=Usage(input_tokens=100),
            latency_seconds=1,
            attempts=1,
        ),
    )


def report(
    calls: list[RecordedCall],
    results: list[CallResult],
    weights: Counter[tuple[str, str | None]] | None = None,
    **params: Any,
) -> dict[str, Any]:
    weights = weights or Counter((c.node_name, c.model) for c in calls)
    collection = Collection(
        max_per_group=len(calls),
        calls=calls,
        sessions=len(calls),
        llm_calls_seen=weights.total(),
        replayable=Counter({"none": weights.total()}),
        group_seen=weights,
    )
    return build_report(
        ModelSwapParams(
            candidates=[candidate("reference", reference=True), candidate("cheap")],
            sessions_per_month=1000,
            **params,
        ),
        collection,
        calls,
        results,
        {},
        spent=Decimal(0),
        stopped_early=("max_spend" if any(r.not_run for r in results) else None),
    )


def test_mix_on_one_candidate_costs_what_that_candidate_costs() -> None:
    # Balanced sampling takes 40 calls of each step, though real traffic is
    # 9,900 common calls to 100 rare ones.
    calls = [call(i, "common" if i < 40 else "rare") for i in range(80)]
    results = [reply(c, "reference", "1") for c in calls] + [
        reply(c, "cheap", "0.01" if c.node_name == "common" else "0.9") for c in calls
    ]
    weighted = report(
        calls, results, Counter({("common", "old"): 9900, ("rare", "old"): 100})
    )
    mix = weighted["best_mix"]
    cheap = weighted["candidates"][1]

    assert {item["candidate"] for item in mix["groups"]} == {"cheap"}
    assert mix["cost"] == cheap["cost"]
    assert mix["cost"]["per_call_usd"] == "0.01890000"
    assert [item["share"] for item in mix["groups"]] == [0.99, 0.01]
    assert mix["recorded_cost"]["per_call_usd"] == "1.00000000"


def test_route_with_frequent_failures_is_not_moved() -> None:
    # 80 of the rare step's 100 calls fail, which the 8% overall failure
    # rate hides.
    calls = [call(i, "common" if i < 900 else "rare") for i in range(1000)]
    results = [reply(c, "reference", "1") for c in calls]
    for i, c in enumerate(calls):
        result = reply(c, "cheap", "0.1")
        if i >= 920:
            assert result.reply is not None
            result = replace(
                result,
                agreement=None,
                cost=Decimal(0),
                reply=replace(
                    result.reply,
                    tool_calls=(),
                    failure="http_error",
                    failure_detail="Unsupported rare route schema",
                ),
            )
        results.append(result)
    built = report(calls, results, max_calls=1000)
    rare = next(g for g in built["best_mix"]["groups"] if g["group"].startswith("rare"))
    cheap_rare = built["candidates"][1]["by_route"]["rare: first turn"]

    assert built["candidates"][1]["failure_rate"] == 0.08
    assert (cheap_rare["attempted"], cheap_rare["calls"], cheap_rare["failed"]) == (
        100,
        20,
        80,
    )
    assert cheap_rare["failure_rate"] == 0.8
    assert rare["candidate"] is None
    assert rare["held_out"] is None
    assert rare["unreliable"] == ["cheap"]
    assert "candidates failed too often here: cheap" in render_section(
        built, dashboard_url=None
    )


def test_held_out_failures_drop_a_pick() -> None:
    calls = [call(i) for i in range(400)]
    results = [reply(c, "reference", "1") for c in calls]
    held_out = [c for c in calls if not _is_learning_session(c)]
    # 60 failures, all on held-out sessions: about 30% there, 15% overall.
    failing = {c.node_id for c in held_out[:60]}
    for c in calls:
        result = reply(c, "cheap", "0.1")
        if c.node_id in failing:
            assert result.reply is not None
            result = replace(
                result,
                agreement=None,
                reply=replace(result.reply, tool_calls=(), failure="timeout"),
            )
        results.append(result)
    group = report(calls, results, max_calls=400)["best_mix"]["groups"][0]

    assert group["candidate"] is None
    assert group["held_out"] == "failed"


def test_reference_baseline_records_where_each_step_cost_comes_from() -> None:
    # The reference never replayed the rare step, so today's cost there comes
    # from the recording, which is not like for like.
    calls = [
        call(i, "common" if i < 40 else "rare", "1" if i < 40 else "0.0001")
        for i in range(80)
    ]
    results = [
        reply(c, "reference", "1")
        if c.node_name == "common"
        else CallResult(call=c, candidate="reference", not_run="max_spend")
        for c in calls
    ] + [reply(c, "cheap", "0.1") for c in calls]
    mix = report(calls, results)["best_mix"]

    assert [g["today_source"] for g in mix["groups"]] == [
        "reference replay",
        "recording",
    ]
    assert mix["today_source"].startswith("reference replay on 1 of 2 steps")
    assert "not like for like" in mix["today_source"]
