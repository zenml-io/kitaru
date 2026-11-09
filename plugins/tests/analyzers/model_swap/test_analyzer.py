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
"""End-to-end tests of the analyzer on the quickstart traces."""

import asyncio
import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import pytest

from kitaru.api_models.v1.insight import (
    InsightInput,
    InsightResponse,
    TextInsightData,
)
from kitaru.api_models.v1.session_node import SessionWithNodesResponse
from kitaru.cli.insight_report import render_insight_report
from kitaru_model_swap import REPORT_METADATA_KEY, SUMMARY_INSIGHT_NAME
from kitaru_model_swap import analyzer as analyzer_module
from kitaru_model_swap.analyzer import analyze_model_swap, run_model_swap
from kitaru_model_swap.calls import Collection
from kitaru_model_swap.params import ModelSwapParams
from kitaru_model_swap.report import REPORT_SECTION_KEY

from .helpers import MockOpenAIServer, StubClient, completion

REFERENCE = {
    "name": "gpt-5-nano (reference)",
    "base_url": "http://mock/v1",
    "model": "mirror",
    "reference": True,
    "price": {"input_per_mtok": "0.05", "output_per_mtok": "0.4"},
}
CHEAP = {
    "name": "cheap-open-model",
    "base_url": "http://mock/v1",
    "model": "escalator",
    "price": {"input_per_mtok": "0.01", "output_per_mtok": "0.02"},
}


def mirror(sessions: list[SessionWithNodesResponse]) -> Any:
    """Reply with exactly what was recorded for the same messages."""
    params = ModelSwapParams.model_validate({"candidates": [REFERENCE]})
    collection = Collection(max_per_group=100)
    for session in sessions:
        collection.add_session(session, params)
    recorded = {
        json.dumps(call.request.messages, sort_keys=True): call.request.recorded
        for call in collection.calls
    }

    def reply(body: dict[str, Any]) -> httpx.Response:
        outcome = recorded[json.dumps(body["messages"], sort_keys=True)]
        return completion(
            tool_calls=[(c.name, c.arguments) for c in outcome.tool_calls],
            text=outcome.text,
        )

    return reply


def escalator(body: dict[str, Any]) -> httpx.Response:
    return completion(tool_calls=[("escalate_to_human", {"reason": "unsure"})])


@pytest.fixture
def server(quickstart_sessions: list[SessionWithNodesResponse]) -> MockOpenAIServer:
    return MockOpenAIServer(
        {"mirror": mirror(quickstart_sessions), "escalator": escalator}
    )


def ids(client: StubClient) -> list[Any]:
    return list(client._by_id)


async def run(
    client: StubClient, server: MockOpenAIServer, **params: Any
) -> dict[str, InsightInput]:
    insights = await run_model_swap(
        ids(client),
        ModelSwapParams.model_validate(params),
        transport=server.transport(),
    )
    return {insight.name: insight for insight in insights}


def report(insights: dict[str, InsightInput]) -> dict[str, Any]:
    value = insights[SUMMARY_INSIGHT_NAME].metadata[REPORT_METADATA_KEY]
    assert isinstance(value, dict)
    return value


async def test_compares_candidates_on_all_quickstart_calls(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    insights = await run(stub_client, server, candidates=[REFERENCE, CHEAP])
    result = report(insights)
    reference, cheap = result["candidates"]

    assert result["coverage"]["llm_calls_seen"] == 33
    assert result["coverage"]["replayable"] == 33
    assert result["coverage"]["sampled"] == 33
    assert result["coverage"]["calls_with_dropped_reasoning"] > 0
    assert reference["agreement_rate"] == 1.0
    assert reference["failures"] == {}
    assert cheap["calls_sent"] == 33
    assert cheap["agreement_rate"] < 0.5
    assert "different_tool" in cheap["agreement"]
    assert cheap["by_tool"]["final_result"]["calls"] == 10
    assert Decimal(cheap["cost"]["per_call_usd"]) < Decimal(
        reference["cost"]["per_call_usd"]
    )
    assert result["disagreements"]
    first = result["disagreements"][0]
    assert first["candidate"] == "cheap-open-model"
    assert {"session_id", "node_id", "recorded", "candidate_answer"} <= set(first)
    assert {
        "model-swap-summary",
        "model-swap-coverage",
        "model-swap-cost",
        "model-swap-failures",
        "model-swap-agreement-gpt-5-nano-reference",
        "model-swap-agreement-cheap-open-model",
    } <= set(insights)
    assert len(server.requests) == 66
    # Requests carry the recorded tool definitions in OpenAI form.
    assert len(server.requests[0]["tools"]) == 7


async def test_dry_run_sends_nothing(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    insights = await run(
        stub_client, server, candidates=[REFERENCE, CHEAP], dry_run=True
    )
    result = report(insights)

    assert server.requests == []
    assert result["dry_run"] is True
    assert result["candidates"][0]["estimate"]["calls"] == 33
    assert Decimal(result["candidates"][0]["estimate"]["spend_usd"]) > 0
    assert "model-swap-agreement-cheap-open-model" not in insights


async def test_spend_cap_stops_cleanly(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    insights = await run(
        stub_client,
        server,
        candidates=[REFERENCE],
        max_spend_usd="0.003",
        concurrency=1,
    )
    result = report(insights)
    reference = result["candidates"][0]

    assert result["stopped_early"] == "max_spend"
    assert 0 < reference["calls_sent"] < 33
    assert reference["calls_not_run"] == {"max_spend": 33 - reference["calls_sent"]}
    assert Decimal(result["spent_usd"]) <= Decimal("0.003")


async def test_spend_cap_requires_known_prices(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    unpriced = REFERENCE | {"price": None, "model": "qwen3:8b"}

    with pytest.raises(ValueError, match="price"):
        await run(stub_client, server, candidates=[unpriced], max_spend_usd="1")
    assert server.requests == []


async def test_time_budget_stops_cleanly(
    stub_client: StubClient, quickstart_sessions: list[SessionWithNodesResponse]
) -> None:
    replay = mirror(quickstart_sessions)

    class SlowServer(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(0.05)
            return replay(json.loads(request.content))

    insights = await run_model_swap(
        list(stub_client._by_id),
        ModelSwapParams.model_validate(
            {"candidates": [REFERENCE], "concurrency": 1, "time_budget_seconds": 0.4}
        ),
        transport=SlowServer(),
    )
    result = report({insight.name: insight for insight in insights})

    assert result["stopped_early"] == "time_budget"
    assert result["candidates"][0]["calls_not_run"]["time_budget"] > 0
    assert 0 < result["candidates"][0]["calls_sent"] < 33


async def test_without_schemas_the_report_says_what_was_skipped(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    insights = await run(
        stub_client, server, candidates=[REFERENCE], use_recorded_tools=False
    )
    result = report(insights)

    assert server.requests == []
    assert result["coverage"]["replayable"] == 0
    assert result["coverage"]["skipped"] == {"missing_tool_schema": 33}
    assert any("tool_schemas" in note for note in result["notes"])


async def test_inferred_schemas_stay_out_of_the_headline(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    insights = await run(
        stub_client,
        server,
        candidates=[REFERENCE],
        use_recorded_tools=False,
        infer_missing_schemas=True,
    )
    reference = report(insights)["candidates"][0]

    assert reference["answered"] == 0
    assert reference["agreement_rate"] is None
    assert sum(reference["inferred_schema_agreement"].values()) == 33


async def test_supplied_schemas_restore_coverage(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    schemas = {
        tool["function"]["name"]: tool["function"]["parameters"]
        for tool in await _recorded_tools(stub_client)
    }
    insights = await run(
        stub_client,
        server,
        candidates=[REFERENCE],
        use_recorded_tools=False,
        tool_schemas=schemas,
    )
    result = report(insights)

    assert result["coverage"]["replayable"] == 33
    assert result["coverage"]["schema_sources"] == {"supplied": 33}


async def _recorded_tools(client: StubClient) -> list[dict[str, Any]]:
    session = await client.get_with_nodes(ids(client)[0])
    params = ModelSwapParams.model_validate({"candidates": [REFERENCE]})
    collection = Collection(max_per_group=100)
    collection.add_session(session, params)
    return collection.calls[0].request.tools


async def test_failures_are_not_counted_as_disagreement(
    stub_client: StubClient,
) -> None:
    server = MockOpenAIServer({"broken": lambda _: httpx.Response(400)})
    broken = CHEAP | {"name": "broken", "model": "broken"}

    insights = await run(stub_client, server, candidates=[broken], max_calls=5)
    candidate = report(insights)["candidates"][0]

    assert candidate["failures"] == {"http_error": 5}
    assert candidate["failure_rate"] == 1.0
    assert candidate["agreement"] == {}
    assert candidate["summary"].startswith("Fix the provider setup")
    assert report(insights)["best_for"] == []


async def test_judge_verdicts_are_reported(
    stub_client: StubClient, quickstart_sessions: list[SessionWithNodesResponse]
) -> None:
    replay = mirror(quickstart_sessions)

    def changed_arguments(body: dict[str, Any]) -> httpx.Response:
        message = replay(body).json()["choices"][0]["message"]
        return completion(
            tool_calls=[
                (call["function"]["name"], {"changed": True})
                for call in message.get("tool_calls") or []
            ],
            text=message["content"],
        )

    server = MockOpenAIServer(
        {
            "changed": changed_arguments,
            "judge": lambda _: completion(text='{"equivalent": true}'),
        }
    )
    insights = await run(
        stub_client,
        server,
        candidates=[CHEAP | {"model": "changed"}],
        max_calls=3,
        judge={"name": "judge", "base_url": "http://mock/v1", "model": "judge"},
    )
    candidate = report(insights)["candidates"][0]

    assert candidate["agreement"] == {"same_tool_different_args": 3}
    assert candidate["judged_equivalent"] == 3
    # Three rulings, each asked again with the answers swapped.
    assert [r["model"] for r in server.requests].count("judge") == 6
    assert report(insights)["judge_check"] == {
        "checked": 3,
        "changed": 0,
        "unsure": 0,
        "unanswered": 0,
    }
    assert all(
        item["judge_equivalent"] is True for item in report(insights)["disagreements"]
    )


async def test_entrypoint_validates_params(stub_client: StubClient) -> None:
    with pytest.raises(ValueError, match="candidates"):
        await analyze_model_swap(ids(stub_client))


async def test_core_report_command_renders_the_analyzer_output(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    insights = await run(stub_client, server, candidates=[REFERENCE, CHEAP])
    now = datetime.now(UTC)
    analyzer_version_id = uuid.uuid4()
    stored = [
        InsightResponse.model_validate(
            {
                **insight.model_dump(mode="json"),
                "id": uuid.uuid4(),
                "owner_id": uuid.uuid4(),
                "agent_id": uuid.uuid4(),
                "import_id": uuid.uuid4(),
                "analyzer_version_id": analyzer_version_id,
                "created": now,
                "updated": now,
            }
        )
        for insight in insights.values()
    ]

    document = render_insight_report(stored, import_id=uuid.uuid4())

    assert "Which model is right for this agent" in document
    assert "cheap-open-model" in document
    assert "https://ui.example/sessions/" in document
    # The section covers the analyzer's own cards, so none is drawn twice.
    assert "Other insights" not in document
    assert "Recorded calls covered" not in document


async def test_links_are_left_out_without_a_dashboard(
    monkeypatch: pytest.MonkeyPatch,
    quickstart_sessions: list[SessionWithNodesResponse],
    server: MockOpenAIServer,
) -> None:
    client = StubClient(quickstart_sessions, dashboard_url=None)
    monkeypatch.setattr(analyzer_module, "KitaruAPIClient", lambda: client)

    insights = await run(client, server, candidates=[CHEAP], max_calls=3)
    section = insights[SUMMARY_INSIGHT_NAME].metadata[REPORT_SECTION_KEY]

    assert isinstance(section, dict)
    assert "href=" not in str(section["html"])


async def test_summary_warns_when_the_current_model_is_noisy(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    noisy_reference = REFERENCE | {"model": "escalator"}
    candidate = CHEAP | {"model": "mirror"}

    insights = await run(stub_client, server, candidates=[noisy_reference, candidate])
    reference, other = report(insights)["candidates"]

    assert reference["tool_choice_rate"] < 0.5
    assert other["tool_choice_rate"] == 1.0
    assert other["summary"].startswith("Picks the same tool")
    assert "matches itself only" in other["summary"]


async def test_section_escapes_model_output(stub_client: StubClient) -> None:
    def hostile(body: dict[str, Any]) -> httpx.Response:
        return completion(
            tool_calls=[("escalate_to_human", {"reason": "<script>alert(1)</script>"})]
        )

    server = MockOpenAIServer({"hostile": hostile})
    insights = await run(
        stub_client, server, candidates=[CHEAP | {"model": "hostile"}], max_calls=3
    )
    section = insights[SUMMARY_INSIGHT_NAME].metadata[REPORT_SECTION_KEY]

    assert isinstance(section, dict)
    assert "<script>" not in str(section["html"])
    assert "&lt;script&gt;" in str(section["html"])


async def test_best_for_names_a_candidate_per_goal(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    open_copy = CHEAP | {"name": "open-copy", "model": "mirror", "open_weights": True}
    worse = CHEAP | {
        "name": "worse",
        "price": {"input_per_mtok": "0", "output_per_mtok": "0"},
    }

    insights = await run(stub_client, server, candidates=[REFERENCE, open_copy, worse])
    result = report(insights)
    best = {choice["goal"]: choice["candidate"] for choice in result["best_for"]}

    # Today's model run again sets the bar; it is never its own closest match.
    assert best["Closest to today"] == "open-copy"
    # The free candidate behaves too differently to count as the cheapest
    # comparable choice.
    assert best["Cheapest"] == "open-copy"
    assert best["Best open model"] == "open-copy"
    assert best["Worth a full replay"] == "open-copy + worse"
    assert result["candidates"][1]["open_weights"] is True
    summary = insights[SUMMARY_INSIGHT_NAME].data
    assert isinstance(summary, TextInsightData)
    assert "Best open model" in summary.content


async def test_no_equivalent_names_finalists_instead_of_a_cheapest(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    # Today's model matches itself every time; the only candidate rarely does,
    # so nothing is comparable and the report must not crown a cheap choice.
    insights = await run(stub_client, server, candidates=[REFERENCE, CHEAP])
    best = {c["goal"]: c for c in report(insights)["best_for"]}

    assert best["Closest to today"]["candidate"] == "cheap-open-model"
    assert best["Cheapest"]["candidate"] == ""
    assert best["Fastest"]["candidate"] == ""
    assert (
        "No candidate is within 10 points of today's model" in (best["Cheapest"]["why"])
    )
    assert best["Worth a full replay"]["candidate"] == "cheap-open-model"
    assert "cannot name an equivalent" in best["Worth a full replay"]["why"]


async def test_finalists_drop_clear_failures_but_keep_the_cheapest(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    copy = CHEAP | {"name": "copy", "model": "mirror"}
    pricey_miss = CHEAP | {
        "name": "pricey-miss",
        "price": {"input_per_mtok": "5", "output_per_mtok": "20"},
    }
    cheap_miss = CHEAP | {
        "name": "cheap-miss",
        "price": {"input_per_mtok": "0", "output_per_mtok": "0"},
    }

    insights = await run(
        stub_client, server, candidates=[REFERENCE, copy, pricey_miss, cheap_miss]
    )
    best = {c["goal"]: c for c in report(insights)["best_for"]}
    finalists = best["Worth a full replay"]

    # Both misses rarely match today's model; only the cheapest one survives,
    # because a cheap model can keep outcomes by a different route.
    assert finalists["candidate"] == "copy + cheap-miss"
    assert "clear failures" in finalists["why"]
    assert "pricey-miss" in finalists["why"]


async def test_noisy_reference_names_finalists_only(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    # Today's model run again escalates everywhere, so it rarely matches its
    # own recording and agreement cannot vouch for any candidate.
    insights = await run(
        stub_client,
        server,
        candidates=[REFERENCE | {"model": "escalator"}, CHEAP | {"model": "mirror"}],
    )
    best = {c["goal"]: c for c in report(insights)["best_for"]}

    assert best["Cheapest"]["candidate"] == ""
    assert "matches its own recording only" in best["Cheapest"]["why"]
    assert best["Worth a full replay"]["candidate"] == "cheap-open-model"
    assert "matches its own recording only" in best["Worth a full replay"]["why"]


async def test_dry_run_has_no_choices(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    insights = await run(stub_client, server, candidates=[REFERENCE], dry_run=True)

    assert report(insights)["best_for"] == []
