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
"""Regression tests for safety and robustness fixes."""

import asyncio
import json
import time
import uuid
from collections import Counter
from dataclasses import replace
from decimal import Decimal
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeResponse,
)
from kitaru.cli.insight_report import REPORT_SECTION_KEY as CORE_SECTION_KEY
from kitaru_model_swap import analyzer as analyzer_module
from kitaru_model_swap.calls import (
    Collection,
    RecordedCall,
    reservation_usage,
    sample_calls,
)
from kitaru_model_swap.endpoint import Usage, read_usage
from kitaru_model_swap.normalize import (
    RecordedOutcome,
    ReplayRequest,
    Skipped,
    build_replay_request,
)
from kitaru_model_swap.params import Candidate, ModelSwapParams
from kitaru_model_swap.pricing import call_cost, resolve_price
from kitaru_model_swap.report import (
    REPORT_SECTION_KEY,
    SUMMARY_INSIGHT_NAME,
    build_report,
    weighted_per_call,
)
from kitaru_model_swap.runner import Budget, Runner

from .helpers import MockOpenAIServer, StubClient, completion
from .test_analyzer import CHEAP, REFERENCE, escalator, report, run


def candidate(**fields: Any) -> Candidate:
    return Candidate.model_validate(
        {"name": "c", "base_url": "https://api.example/v1", "model": "m"} | fields
    )


@pytest.mark.parametrize(
    "fields",
    [
        {"base_url": "https://user:secret@api.example/v1"},
        {"base_url": "ftp://api.example/v1"},
        {"api_key_env": "KITARU_API_TOKEN"},
        {"api_key_env": "OPENAI_API_KEY", "base_url": "http://api.example/v1"},
    ],
)
def test_unsafe_endpoints_are_rejected(fields: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        candidate(**fields)


def test_keys_may_go_to_a_local_endpoint_over_http() -> None:
    assert candidate(api_key_env="LOCAL_KEY", base_url="http://localhost:8000/v1")


def test_a_pasted_key_is_not_echoed_in_the_error() -> None:
    with pytest.raises(ValidationError) as error:
        candidate(api_key_env="sk-proj-abcdefSECRET123")

    assert "SECRET123" not in str(error.value)


def node(inputs: Any, outputs: Any) -> SessionNodeResponse:
    return SessionNodeResponse.model_validate(
        {
            "id": uuid.uuid4(),
            "session_id": uuid.uuid4(),
            "external_id": "n",
            "links": [],
            "node_type": NodeType.LLM_CALL,
            "name": "chat",
            "status": NodeStatus.COMPLETED,
            "inputs": inputs,
            "outputs": outputs,
            "attributes": None,
            "metadata": {},
        }
    )


def rebuild(inputs: Any, outputs: Any = "ok") -> ReplayRequest | Skipped:
    return build_replay_request(
        node(inputs, outputs),
        tool_schemas={},
        tool_descriptions={},
        use_recorded_tools=True,
        infer_missing_schemas=False,
    )


@pytest.mark.parametrize(
    ("inputs", "outputs"),
    [
        ([{"role": "user", "parts": None}], "ok"),
        ([{"role": "user", "content": "hi"}], [{"role": "assistant", "parts": ["x"]}]),
        (
            [{"role": "assistant", "content": None, "tool_calls": ["x"]}],
            "ok",
        ),
        ([{"role": "user", "content": "hi"}], {"choices": ["x"]}),
        (
            [
                {
                    "role": "assistant",
                    "parts": [{"type": "tool_call", "name": "lookup", "arguments": {}}],
                }
            ],
            "ok",
        ),
        ([{"role": "tool", "parts": [{"type": "text", "content": "result"}]}], "ok"),
    ],
)
def test_malformed_recordings_are_skipped_not_raised(inputs: Any, outputs: Any) -> None:
    assert isinstance(rebuild(inputs, outputs), Skipped)


def test_cached_tokens_above_input_do_not_break_bundled_pricing() -> None:
    resolved = resolve_price(candidate(model="gpt-5-nano", price_provider="openai"))
    assert resolved is not None

    cost = call_cost(resolved, Usage(input_tokens=10, cached_input_tokens=20))

    assert cost >= 0


def recorded_call(name: str, cost: str) -> RecordedCall:
    request = ReplayRequest(
        messages=[{"role": "user", "content": "hi"}],
        tools=[],
        schema_source="none",
        source_format="openai_chat",
        recorded=RecordedOutcome(tool_calls=(), text="ok"),
    )
    return RecordedCall(
        session_id=uuid.uuid4(),
        node_id=uuid.uuid4(),
        node_name=name,
        model="m",
        request=request,
        usage=Usage(),
        cost=Decimal(cost),
        estimated_input_tokens=1,
    )


def test_missing_system_message_does_not_imply_missing_instructions() -> None:
    policy = "Verify the customer's account before making changes."
    request = rebuild([{"role": "user", "content": policy}])
    assert isinstance(request, ReplayRequest)
    call = replace(recorded_call("chat", "0"), request=request)
    collection = Collection(
        max_per_group=1,
        calls=[call],
        llm_calls_seen=1,
        sessions=1,
        replayable=Counter({"none": 1}),
        group_seen=Counter({("chat", "m"): 1}),
    )
    params = ModelSwapParams(candidates=[candidate()], dry_run=True)

    result = build_report(
        params, collection, [call], [], {}, spent=Decimal(0), stopped_early=None
    )

    assert request.messages == [{"role": "user", "content": policy}]
    assert result["coverage"]["calls_without_system_prompt"] == 1
    assert (
        "Sampled calls without a system or developer message: 1. "
        "The first user message may carry the instructions."
    ) in result["notes"]


def test_per_call_cost_is_weighted_by_how_often_calls_occur() -> None:
    planner, classifier = recorded_call("planner", "1"), recorded_call("classify", "0")

    per_call = weighted_per_call(
        [(planner, Decimal(1)), (classifier, Decimal(0))],
        Counter({("planner", "m"): 900, ("classify", "m"): 100}),
    )

    assert per_call == Decimal("0.9")


def test_new_groups_beyond_the_sample_size_are_counted_not_kept(
    quickstart_sessions: list[Any],
) -> None:
    collection = Collection(max_per_group=1)
    params = ModelSwapParams.model_validate({"candidates": [REFERENCE]})
    for session in quickstart_sessions:
        collection.add_session(session, params)

    assert len(collection.calls) == 1
    assert collection.group_seen.total() == 33


def test_sample_spreads_across_the_import_and_ignores_fetch_order(
    quickstart_sessions: list[Any],
) -> None:
    sessions = []
    for copy in range(6):
        for item in quickstart_sessions:
            session_id = uuid.uuid5(item.session.id, str(copy))
            nodes = [
                node.model_copy(
                    update={
                        "id": uuid.uuid5(node.id, str(copy)),
                        "session_id": session_id,
                    }
                )
                for node in item.nodes
            ]
            session = item.session.model_copy(update={"id": session_id})
            sessions.append(
                item.model_copy(update={"session": session, "nodes": nodes})
            )
    params = ModelSwapParams.model_validate(
        {"candidates": [REFERENCE], "max_calls": 20}
    )

    def sample(ordered: list[Any]) -> list[RecordedCall]:
        collection = Collection(max_per_group=params.max_calls)
        for session in ordered:
            collection.add_session(session, params)
        return sample_calls(collection.calls, params.max_calls)

    sampled = sample(sessions)
    first_ten = {session.session.id for session in sessions[:10]}
    sampled_sessions = {call.session_id for call in sampled}

    assert len(sampled_sessions) > 10
    assert len(sampled_sessions - first_ten) > len(sampled_sessions) // 2
    assert {c.node_id for c in sample(sessions[::-1])} == {c.node_id for c in sampled}


async def test_failed_calls_do_not_lower_the_cost_per_call(
    stub_client: StubClient,
) -> None:
    replies = iter([httpx.Response(400), completion(text="ok")] * 20)
    server = MockOpenAIServer({"escalator": lambda _: next(replies)})

    insights = await run(stub_client, server, candidates=[CHEAP], max_calls=6)
    cheap = report(insights)["candidates"][0]

    # Every answered call costs the same, so the per-call cost must not be
    # pulled down by the failed calls priced at zero.
    assert cheap["failures"]
    # 1000 input and 200 output tokens at $0.01 and $0.02 per million.
    assert Decimal(cheap["cost"]["per_call_usd"]) == Decimal("0.000014")


class SlowTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(5)
        return completion(text="late")


async def test_time_budget_cuts_off_calls_in_flight(
    quickstart_sessions: list[Any],
) -> None:
    params = ModelSwapParams.model_validate({"candidates": [CHEAP], "concurrency": 2})
    collection = Collection(max_per_group=2)
    for session in quickstart_sessions:
        collection.add_session(session, params)
    runner = Runner(params, {"cheap-open-model": None}, transport=SlowTransport())

    started = time.monotonic()
    results = await runner.run(collection.calls, deadline=started + 0.2)

    assert time.monotonic() - started < 2
    assert {result.not_run for result in results} == {"time_budget"}
    assert runner.stopped_early == "time_budget"


async def test_judge_verdicts_survive_a_text_tool_call_format(
    stub_client: StubClient, quickstart_sessions: list[Any]
) -> None:
    def changed(body: dict[str, Any]) -> httpx.Response:
        return completion(tool_calls=[("final_result", {"changed": True})])

    server = MockOpenAIServer(
        {
            "changed": changed,
            "judge": lambda _: completion(text='{"equivalent": false}'),
        }
    )
    judge = {
        "name": "judge",
        "base_url": "https://mock/v1",
        "model": "judge",
        "provider_settings": {"tool_call_format": "json_content"},
    }
    insights = await run(
        stub_client,
        server,
        candidates=[CHEAP | {"model": "changed"}],
        max_calls=10,
        judge=judge,
    )

    judged = [d["judge_equivalent"] for d in report(insights)["disagreements"]]
    assert False in judged


ESCALATING = MockOpenAIServer({"escalator": escalator})


async def test_dry_run_cost_card_is_not_labeled_monthly(
    stub_client: StubClient,
) -> None:
    insights = await run(
        stub_client,
        ESCALATING,
        candidates=[REFERENCE | {"model": "escalator"}],
        dry_run=True,
        sessions_per_month=1000,
    )
    cost = insights["model-swap-cost"].data

    assert getattr(cost, "unit", None) == "USD for the sampled calls"


async def test_a_candidate_named_recorded_does_not_break_the_cost_card(
    stub_client: StubClient,
) -> None:
    insights = await run(
        stub_client,
        ESCALATING,
        candidates=[REFERENCE | {"name": "Recorded", "model": "escalator"}],
        max_calls=3,
    )

    assert "model-swap-cost" in insights


def test_section_key_matches_the_report_command() -> None:
    assert REPORT_SECTION_KEY == CORE_SECTION_KEY
    assert SUMMARY_INSIGHT_NAME == "model-swap-summary"


async def test_budget_waits_for_calls_in_flight_instead_of_dropping() -> None:
    budget = Budget(Decimal("1"))
    assert await budget.reserve(Decimal("0.8"))

    waiter = asyncio.create_task(budget.reserve(Decimal("0.5")))
    await asyncio.sleep(0)
    assert not waiter.done()
    await budget.settle(Decimal("0.8"), Decimal("0.1"))

    assert await waiter
    assert budget.spent == Decimal("0.1")


async def test_a_reservation_that_can_never_fit_is_refused() -> None:
    assert not await Budget(Decimal("0.1")).reserve(Decimal("0.5"))


def test_reservation_never_exceeds_the_output_limit() -> None:
    call = recorded_call("planner", "0")

    assert reservation_usage(call, max_tokens=None).output_tokens == 2048
    assert reservation_usage(call, max_tokens=64000).output_tokens == 2048
    assert reservation_usage(call, max_tokens=100).output_tokens == 100


async def test_calls_cut_off_by_the_time_budget_are_charged(
    quickstart_sessions: list[Any],
) -> None:
    params = ModelSwapParams.model_validate({"candidates": [CHEAP]})
    collection = Collection(max_per_group=1)
    for session in quickstart_sessions:
        collection.add_session(session, params)
    price = resolve_price(Candidate.model_validate(CHEAP))
    runner = Runner(params, {"cheap-open-model": price}, transport=SlowTransport())

    await runner.run(collection.calls, deadline=time.monotonic() + 0.1)

    assert runner.budget.spent > 0


def test_supplied_schemas_skip_calls_without_any_sign_of_tools() -> None:
    result = build_replay_request(
        node([{"role": "user", "content": "summarize"}], "a summary"),
        tool_schemas={"lookup": {"type": "object"}},
        tool_descriptions={},
        use_recorded_tools=True,
        infer_missing_schemas=False,
    )

    assert isinstance(result, ReplayRequest)
    assert result.tools == []


def test_system_instructions_kept_in_attributes_are_replayed() -> None:
    recorded = node([{"role": "user", "content": "hi"}], "hello")
    with_attributes = recorded.model_copy(
        update={
            "attributes": {
                "logfire.attributes": {
                    "gen_ai.system_instructions": json.dumps(
                        [{"type": "text", "content": "Be brief."}]
                    )
                }
            }
        }
    )

    result = build_replay_request(
        with_attributes,
        tool_schemas={},
        tool_descriptions={},
        use_recorded_tools=True,
        infer_missing_schemas=False,
    )

    assert isinstance(result, ReplayRequest)
    assert result.messages[0] == {"role": "system", "content": "Be brief."}


def test_missing_tool_call_ids_have_their_own_skip_reason() -> None:
    result = rebuild(
        [
            {
                "role": "assistant",
                "parts": [{"type": "tool_call", "name": "lookup", "arguments": {}}],
            }
        ]
    )

    assert isinstance(result, Skipped)
    assert result.reason == "missing_tool_call_id"


def test_negative_token_counts_do_not_lower_spend() -> None:
    usage = read_usage({"prompt_tokens": -100, "completion_tokens": "x"})

    assert usage.input_tokens == 0
    assert usage.output_tokens == 0


async def test_a_candidate_failing_a_kind_of_call_is_not_named_cheapest(
    quickstart_sessions: list[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    # Calls that follow a tool result become their own kind of call.
    sessions = [
        session.model_copy(
            update={
                "nodes": [
                    node.model_copy(update={"name": "planner"})
                    if node.node_type is NodeType.LLM_CALL
                    and "tool_call_response" in json.dumps(node.inputs)
                    else node
                    for node in session.nodes
                ]
            }
        )
        for session in quickstart_sessions
    ]
    stub_client = StubClient(sessions)
    monkeypatch.setattr(analyzer_module, "KitaruAPIClient", lambda: stub_client)

    def fails_on_tools(body: dict[str, Any]) -> httpx.Response:
        if any(m["role"] == "tool" for m in body["messages"]):
            return httpx.Response(400)
        return completion(tool_calls=[("escalate_to_human", {"reason": "x"})])

    server = MockOpenAIServer({"escalator": escalator, "partial": fails_on_tools})
    free = {"input_per_mtok": "0", "output_per_mtok": "0"}
    insights = await run(
        stub_client,
        server,
        candidates=[
            REFERENCE | {"model": "escalator"},
            CHEAP | {"name": "partial", "model": "partial", "price": free},
        ],
        max_calls=10,
    )
    result = report(insights)
    partial = result["candidates"][1]

    assert partial["cost_complete"] is False
    assert all(
        c["candidate"] != "partial"
        for c in result["best_for"]
        if c["goal"] == "Cheapest"
    )


async def test_fetching_stops_when_the_time_budget_runs_out(
    stub_client: StubClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = iter(range(0, 100_000, 500))
    monkeypatch.setattr(analyzer_module.time, "monotonic", lambda: next(clock))

    insights = await run(
        stub_client,
        ESCALATING,
        candidates=[REFERENCE | {"model": "escalator"}],
        time_budget_seconds=600,
    )
    result = report(insights)

    assert result["coverage"]["sessions_not_fetched"] > 0
    assert result["stopped_early"] == "time_budget"


def test_a_capped_run_reserves_the_full_output_limit() -> None:
    call = recorded_call("planner", "0")

    assert reservation_usage(call, 64000, strict=True).output_tokens == 64000
    assert reservation_usage(call, None, strict=True).output_tokens == 2048


async def test_a_cap_that_cannot_fit_one_call_is_refused_up_front(
    stub_client: StubClient,
) -> None:
    expensive = CHEAP | {
        "price": {"input_per_mtok": "1", "output_per_mtok": "100"},
        "provider_settings": {"max_tokens": 64000},
    }

    with pytest.raises(ValueError, match="max_tokens"):
        await run(stub_client, ESCALATING, candidates=[expensive], max_spend_usd="1")


async def test_waiting_past_the_deadline_is_not_charged(
    quickstart_sessions: list[Any],
) -> None:
    params = ModelSwapParams.model_validate(
        {"candidates": [CHEAP], "max_spend_usd": "1", "concurrency": 2}
    )
    collection = Collection(max_per_group=2)
    for session in quickstart_sessions:
        collection.add_session(session, params)
    price = resolve_price(Candidate.model_validate(CHEAP))
    runner = Runner(params, {"cheap-open-model": price}, transport=SlowTransport())
    # The first call holds most of the budget, so the second waits for it.
    await runner.budget.reserve(Decimal("0.99999"))

    results = await runner.run(collection.calls[:2], deadline=time.monotonic() + 0.2)

    assert {r.not_run for r in results} == {"time_budget"}


class FailingTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        raise RuntimeError("transport bug")


async def test_an_unexpected_error_releases_its_reservation(
    quickstart_sessions: list[Any],
) -> None:
    params = ModelSwapParams.model_validate(
        {"candidates": [CHEAP], "max_spend_usd": "1"}
    )
    collection = Collection(max_per_group=1)
    for session in quickstart_sessions:
        collection.add_session(session, params)
    price = resolve_price(Candidate.model_validate(CHEAP))
    runner = Runner(params, {"cheap-open-model": price}, transport=FailingTransport())

    with pytest.raises(RuntimeError):
        await runner.run(collection.calls)

    assert await runner.budget.reserve(Decimal("1"))


def test_instructions_join_a_recorded_system_prompt() -> None:
    recorded = node(
        [
            {"role": "system", "content": "You are helpful."},
            {"role": "user", "content": "hi"},
        ],
        "hello",
    ).model_copy(
        update={
            "attributes": {"phoenix.attributes": {"gen_ai.system_instructions": "42"}}
        }
    )

    result = build_replay_request(
        recorded,
        tool_schemas={},
        tool_descriptions={},
        use_recorded_tools=True,
        infer_missing_schemas=False,
    )

    assert isinstance(result, ReplayRequest)
    assert result.messages[:2] == [
        {"role": "system", "content": "42"},
        {"role": "system", "content": "You are helpful."},
    ]
