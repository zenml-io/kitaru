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
import time
import uuid
from collections import Counter
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
from kitaru_model_swap.calls import Collection, RecordedCall
from kitaru_model_swap.endpoint import Usage
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
    weighted_per_call,
)
from kitaru_model_swap.runner import Runner

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
