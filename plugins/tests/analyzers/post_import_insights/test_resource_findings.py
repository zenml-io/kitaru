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
"""Tests for the cost, time, token, and tool-path insight candidates."""

import random
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from functools import partial

import pytest

from kitaru.api_models.v1.insight import BinnedInsightData, CategoricalInsightData
from kitaru.api_models.v1.session import (
    SessionDetailResponse,
    SessionOrigin,
    SessionStatus,
    TokenUsage,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru_post_import_insights import resource_profile
from kitaru_post_import_insights.copy_validation import validate_card_copy
from kitaru_post_import_insights.models import (
    INSIGHT_METADATA_KEY,
    InsightGenerationContext,
    SourceImportContext,
)
from kitaru_post_import_insights.pipeline import generate_insights_from_profile
from kitaru_post_import_insights.profiling import (
    CandidateFinding,
    ProfilingResult,
    profile_sessions,
)
from kitaru_post_import_insights.profiling_state import (
    Observation,
    ObservationStore,
)
from kitaru_post_import_insights.resource_inputs import SessionView
from kitaru_post_import_insights.tool_paths import get_tool_path

NOW = datetime(2026, 10, 1, tzinfo=UTC)
OWNER_ID = uuid.UUID("01990000-0000-7000-8000-000000000001")
AGENT_ID = uuid.UUID("01990000-0000-7000-8000-000000000002")
IMPORT_ID = uuid.UUID("01990000-0000-7000-8000-000000000003")
RESOURCE_IDS = {
    "cost-concentration",
    "component-time-share",
    "context-growth",
    "context-outliers",
    "prompt-cache",
    "tool-paths",
}


def _id(number: int) -> uuid.UUID:
    return uuid.UUID(f"01990000-0000-7000-8000-{number:012d}")


def _node_id(session: int, index: int) -> uuid.UUID:
    return _id(1_000_000 + session * 1_000 + index)


def _llm(
    session: int,
    index: int,
    *,
    input_tokens: int | None = None,
    cached: int | None = None,
    cost: str | None = None,
    model: str = "model-a",
    system: str | None = None,
    start: float | None = None,
    seconds: float = 1,
) -> SessionNodeResponse:
    return SessionNodeResponse(
        id=_node_id(session, index),
        session_id=_id(session),
        external_id=f"llm-{index}",
        links=[],
        node_type=NodeType.LLM_CALL,
        name="chat",
        status=NodeStatus.COMPLETED,
        started_at=None if start is None else NOW + timedelta(seconds=start),
        ended_at=None if start is None else NOW + timedelta(seconds=start + seconds),
        system_prompt_selector=None if system is None else "/messages/0/content",
        inputs=None if system is None else {"messages": [{"content": system}]},
        model=model,
        tokens=TokenUsage(input_tokens=input_tokens, cached_input_tokens=cached),
        cost=None if cost is None else Decimal(cost),
        metadata={},
    )


def _tool(
    session: int,
    index: int,
    name: str,
    *,
    start: float | None = None,
    seconds: float = 1,
) -> SessionNodeResponse:
    return SessionNodeResponse(
        id=_node_id(session, index),
        session_id=_id(session),
        external_id=f"tool-{index}",
        links=[],
        node_type=NodeType.TOOL_CALL,
        name=name,
        status=NodeStatus.COMPLETED,
        started_at=None if start is None else NOW + timedelta(seconds=start),
        ended_at=None if start is None else NOW + timedelta(seconds=start + seconds),
        tool_name=name,
        metadata={},
    )


def _session(
    number: int,
    nodes: list[SessionNodeResponse],
    *,
    cost: str | None = None,
    duration: float | None = None,
) -> SessionWithNodesResponse:
    return SessionWithNodesResponse(
        session=SessionDetailResponse(
            id=_id(number),
            owner_id=OWNER_ID,
            created=NOW,
            updated=NOW,
            agent_id=AGENT_ID,
            number=number,
            import_id=IMPORT_ID,
            origin=SessionOrigin.IMPORTED,
            status=SessionStatus.COMPLETED,
            inputs=None,
            outputs=None,
            started_at=NOW,
            ended_at=None if duration is None else NOW + timedelta(seconds=duration),
            metadata={},
            cost=None if cost is None else Decimal(cost),
            tokens=None,
            llm_call_count=sum(node.node_type is NodeType.LLM_CALL for node in nodes),
            tool_call_count=sum(node.node_type is NodeType.TOOL_CALL for node in nodes),
        ),
        nodes=nodes,
    )


def _find(result: ProfilingResult, candidate_id: str) -> CandidateFinding | None:
    return next((item for item in result.candidates if item.id == candidate_id), None)


def _get(result: ProfilingResult, candidate_id: str) -> CandidateFinding:
    candidate = _find(result, candidate_id)
    assert candidate is not None
    return candidate


def _facts(candidate: CandidateFinding) -> dict[str, object]:
    return {fact.name: fact.value for fact in candidate.facts}


def _priced_sessions(costs: list[str]) -> list[SessionWithNodesResponse]:
    return [
        _session(
            number,
            [
                _llm(number, 0, cost="0.001"),
                _llm(number, 1, cost=cost),
            ],
            cost=cost,
        )
        for number, cost in enumerate(costs, start=1)
    ]


def test_cost_concentration_reports_the_costliest_tenth() -> None:
    sessions = _priced_sessions(["10.00", "10.00"] + ["0.50"] * 18)
    sessions.append(_session(21, [_llm(21, 0)]))
    candidate = _get(profile_sessions(sessions), "cost-concentration")
    assert candidate.title == (
        "The costliest 10% of sessions (2 of 20) account for 68.97% of recorded cost"
    )
    assert _facts(candidate) == {
        "sessions_with_cost": 20,
        "costliest_sessions": 2,
        "costliest_cost_share_percent": 68.97,
        "sessions_without_cost": 1,
    }
    assert candidate.contributing_session_ids == [_id(1), _id(2)]
    assert [(item.session_id, item.node_id) for item in candidate.evidence] == [
        (_id(1), _node_id(1, 1)),
        (_id(2), _node_id(2, 1)),
    ]
    assert isinstance(candidate.data, CategoricalInsightData)
    assert [(item.label, item.value) for item in candidate.data.values] == [
        ("Costliest 2 sessions", 68.97),
        ("Next 8 sessions", 13.79),
        ("Remaining 10 sessions", 17.24),
    ]
    assert "1 analyzed session without a recorded cost" in (candidate.caveat or "")
    assert candidate.coverage.sessions_analyzed == 20


@pytest.mark.parametrize(
    "costs",
    [["10.00", "10.00"] + ["0.50"] * 17, ["1.00"] * 20, ["0"] * 20],
    ids=["too-few-sessions", "even-spread", "zero-cost"],
)
def test_cost_concentration_needs_enough_concentrated_sessions(
    costs: list[str],
) -> None:
    assert (
        _find(profile_sessions(_priced_sessions(costs)), "cost-concentration") is None
    )


def _timed_session(number: int) -> SessionWithNodesResponse:
    return _session(
        number,
        [
            # Two parallel calls cover 0-8 seconds once, not 6 + 6 seconds.
            _tool(number, 0, "search", start=0, seconds=6),
            _tool(number, 1, "search", start=2, seconds=6),
            _llm(number, 2, start=8, seconds=1),
            _tool(number, 3, "notify"),
        ],
        duration=10,
    )


def test_time_share_counts_overlapping_calls_once() -> None:
    sessions = [_timed_session(number) for number in range(1, 6)]
    sessions.append(_session(6, [_tool(6, 0, "search", start=0)]))
    candidate = _get(profile_sessions(sessions), "component-time-share")
    assert candidate.title == (
        "search (tool) covers 80% of recorded session time in 5 of 5 sessions"
    )
    assert isinstance(candidate.data, CategoricalInsightData)
    assert [(item.label, item.value) for item in candidate.data.values] == [
        ("search (tool)", 40.0),
        ("model-a (model)", 5.0),
    ]
    facts = _facts(candidate)
    assert facts["leading_component_overall_share_percent"] == 80
    assert facts["nodes_without_timestamps"] == 5
    assert [item.node_id for item in candidate.evidence] == [
        _node_id(number, 0) for number in range(1, 6)
    ]
    caveat = candidate.caveat or ""
    assert "5 tool or model nodes without usable start and end times" in caveat
    assert "1 session without a valid duration or any timed node" in caveat


def test_time_share_needs_enough_sessions_and_a_dominant_component() -> None:
    assert (
        _find(
            profile_sessions([_timed_session(number) for number in range(1, 5)]),
            "component-time-share",
        )
        is None
    )
    spread = [
        _session(
            number,
            [_tool(number, index, f"tool_{index}", start=index) for index in range(5)],
            duration=10,
        )
        for number in range(1, 6)
    ]
    assert _find(profile_sessions(spread), "component-time-share") is None


def _growth_session(number: int, inputs: list[int]) -> SessionWithNodesResponse:
    return _session(
        number,
        [_llm(number, index, input_tokens=value) for index, value in enumerate(inputs)],
    )


def test_context_growth_reports_sessions_whose_input_triples() -> None:
    sessions = [
        _growth_session(1, [1_000, 2_000, 5_000]),
        _growth_session(2, [1_000, 6_000]),
        _growth_session(3, [2_000, 3_000, 8_000]),
        _growth_session(4, [3_000, 3_100]),
        _growth_session(5, [4_000]),
    ]
    candidate = _get(profile_sessions(sessions), "context-growth")
    assert candidate.title == (
        "In 3 sessions, model input grew at least 3 times from the first call to the "
        "last (median 1,000 to 6,000 tokens)"
    )
    assert _facts(candidate) == {
        "growing_sessions": 3,
        "sessions_with_repeated_model_calls": 4,
        "growing_share_percent": 75,
        "median_first_call_tokens": 1_000,
        "median_last_call_tokens": 6_000,
        "maximum": 6.0,
    }
    assert [(item.session_id, item.node_id) for item in candidate.evidence] == [
        (_id(2), _node_id(2, 1)),
        (_id(1), _node_id(1, 2)),
        (_id(3), _node_id(3, 2)),
    ]
    assert isinstance(candidate.data, BinnedInsightData)
    assert [item.count for item in candidate.data.bins] == [1, 0, 0, 1, 2, 0]
    assert "1 session with model calls had fewer than two" in (candidate.caveat or "")
    validate_card_copy(
        "In 3 sessions input grew at least 3 times, from a median of 1,000 to "
        "6,000 tokens.",
        candidate,
    )
    with pytest.raises(ValueError, match="numeric claim"):
        validate_card_copy("Input reached 9,000 tokens.", candidate)


def test_context_growth_ignores_small_and_rare_growth() -> None:
    small = [_growth_session(number, [100, 900]) for number in range(1, 5)]
    assert _find(profile_sessions(small), "context-growth") is None
    rare = [_growth_session(number, [1_000, 6_000]) for number in range(1, 3)]
    rare += [_growth_session(number, [1_000, 1_100]) for number in range(3, 30)]
    assert _find(profile_sessions(rare), "context-growth") is None


def test_input_counts_cache_reads_when_the_source_reports_them_separately() -> None:
    # Cache reads above input tokens, as in Anthropic-style usage, prove that
    # this model's calls in the session count cache reads separately.
    separate = _session(
        1,
        [
            _llm(1, 0, input_tokens=1_000, cached=0),
            _llm(1, 1, input_tokens=5_168, cached=5_504),
        ],
    )
    inclusive = _session(
        2,
        [
            _llm(2, 0, input_tokens=1_000, cached=0),
            _llm(2, 1, input_tokens=5_168, cached=900),
        ],
    )
    sessions = [separate, inclusive, _growth_session(3, [1_000, 4_000])]
    candidate = _get(profile_sessions(sessions), "context-growth")
    assert candidate.coverage.affected_sessions == 3
    assert candidate.evidence[0].session_id == _id(1)
    assert _facts(candidate)["maximum"] == 10.67


def _sized_sessions(
    sizes: list[int], *, start: int = 1, cached: int | None = None
) -> list[SessionWithNodesResponse]:
    return [
        _session(number, [_llm(number, 0, input_tokens=size, cached=cached)])
        for number, size in enumerate(sizes, start=start)
    ]


def test_context_outliers_point_at_oversized_calls() -> None:
    sessions = _sized_sessions([2_000] * 20 + [50_000, 12_000])
    candidate = _get(profile_sessions(sessions), "context-outliers")
    assert candidate.title == (
        "2 model calls had at least 10,000 input tokens, against a median of 2,000"
    )
    assert [(item.session_id, item.node_id) for item in candidate.evidence] == [
        (_id(21), _node_id(21, 0)),
        (_id(22), _node_id(22, 0)),
    ]
    assert isinstance(candidate.data, BinnedInsightData)
    assert [
        (item.lower_bound, item.upper_bound, item.count) for item in candidate.data.bins
    ] == [(None, 2_000, 0), (2_000, 10_000, 20), (10_000, None, 2)]
    assert _facts(candidate)["largest_input_tokens"] == 50_000


@pytest.mark.parametrize(
    "sizes",
    [[2_000] * 18 + [50_000], [2_000] * 20 + [9_999]],
    ids=["too-few-calls", "below-threshold"],
)
def test_context_outliers_need_enough_calls_and_an_oversized_one(
    sizes: list[int],
) -> None:
    assert _find(profile_sessions(_sized_sessions(sizes)), "context-outliers") is None


def _cache_sessions(
    count: int, calls: list[tuple[int, int | None]], model: str = "model-a"
) -> list[SessionWithNodesResponse]:
    return [
        _session(
            number,
            [
                _llm(number, index, input_tokens=value, cached=cached, model=model)
                for index, (value, cached) in enumerate(calls)
            ],
        )
        for number in range(1, count + 1)
    ]


def test_prompt_cache_share_follows_the_established_convention() -> None:
    # The second call proves separate counting, so both calls add cache reads
    # to their input: 5,604 cached of 12,072 input tokens in each session.
    sessions = _cache_sessions(5, [(300, 100), (5_168, 5_504), (1_000, 0)])
    candidate = _get(profile_sessions(sessions), "prompt-cache")
    assert candidate.title == (
        "Cache reads covered 46.42% of model input tokens across 15 calls"
    )
    assert _facts(candidate)["cache_read_share_percent"] == 46.42
    assert "the share is a range" not in (candidate.caveat or "")
    assert [item.node_id for item in candidate.evidence] == [
        _node_id(number, 2) for number in range(1, 6)
    ]


def test_prompt_cache_share_is_a_range_when_the_convention_is_unknown() -> None:
    candidate = _get(
        profile_sessions(_cache_sessions(10, [(1_000, 400)])), "prompt-cache"
    )
    assert candidate.title == (
        "Cache reads covered 28.57% to 40% of model input tokens across 10 calls"
    )
    facts = _facts(candidate)
    assert facts["cache_read_share_low_percent"] == 28.57
    assert facts["cache_read_share_high_percent"] == 40
    assert "the share is a range" in (candidate.caveat or "")
    assert candidate.evidence == []
    assert len(candidate.contributing_session_ids) == 10


@pytest.mark.parametrize(
    "calls",
    [[(1_000, None)] * 20, [(1_000, 900)] * 20, [(1_000, 0)] * 9],
    ids=["no-cache-fields", "mostly-cached", "too-few-calls"],
)
def test_prompt_cache_needs_reported_and_low_cache_reads(
    calls: list[tuple[int, int | None]],
) -> None:
    assert _find(profile_sessions(_cache_sessions(1, calls)), "prompt-cache") is None


def _prompt_session(
    number: int, prompts: list[tuple[str, str]]
) -> SessionWithNodesResponse:
    return _session(
        number,
        [
            _llm(
                number, index, input_tokens=1_000, cached=900, model=model, system=text
            )
            for index, (model, text) in enumerate(prompts)
        ],
    )


def test_prompt_cache_detects_system_prompts_that_change_between_calls() -> None:
    policy = "Follow the refund policy. " * 10
    changing = [
        _prompt_session(
            number,
            [("model-a", f"{policy}Time: {minute}") for minute in range(3)],
        )
        for number in range(1, 4)
    ]
    # Calls to another model in between do not count as a change.
    alternating = _prompt_session(
        4, [("model-a", policy), ("model-b", "Route requests."), ("model-a", policy)]
    )
    result = profile_sessions([*changing, alternating])
    candidate = _get(result, "prompt-cache")
    assert candidate.title == (
        "In 3 of 4 sessions the system prompt changed between model calls; cache "
        "reads covered 47.37% to 90% of input tokens"
    )
    facts = _facts(candidate)
    assert facts["system_prompt_changes"] == 6
    assert facts["median_shared_prefix_characters"] == len(policy) + 6
    assert {item.signal for item in candidate.evidence} == {"system-prompt-change"}
    assert candidate.contributing_session_ids == [_id(1), _id(2), _id(3)]
    assert "Follow the refund policy" not in result.model_dump_json()


def _path_session(number: int, names: list[str]) -> SessionWithNodesResponse:
    return _session(
        number, [_tool(number, index, name) for index, name in enumerate(names)]
    )


def test_tool_paths_chart_common_paths_and_one_off_sessions() -> None:
    sessions = [
        _path_session(number, ["search", "search", "answer"]) for number in range(1, 9)
    ]
    sessions += [_path_session(9, ["lookup", "search"])]
    sessions += [_path_session(10, ["lookup", "search"])]
    sessions += [_path_session(11, ["escalate"]), _path_session(12, ["refund"])]
    candidate = _get(profile_sessions(sessions), "tool-paths")
    assert candidate.title == (
        "All 4 tool paths cover 100% of sessions; 2 sessions took a path no other "
        "session took"
    )
    assert isinstance(candidate.data, CategoricalInsightData)
    assert [(item.label, item.value) for item in candidate.data.values] == [
        ("search -> answer", 8),
        ("lookup -> search", 2),
        ("escalate", 1),
        ("refund", 1),
    ]
    assert [item.session_id for item in candidate.evidence] == [_id(11), _id(12)]
    assert candidate.contributing_session_ids == [_id(11), _id(12)]


def test_tool_paths_need_several_sessions_and_paths() -> None:
    assert (
        _find(
            profile_sessions(
                [_path_session(number, [f"tool_{number}"]) for number in range(1, 10)]
            ),
            "tool-paths",
        )
        is None
    )
    same = [_path_session(number, ["search"]) for number in range(1, 20)]
    assert _find(profile_sessions(same), "tool-paths") is None


def test_long_tool_paths_keep_bounded_distinct_labels() -> None:
    first = [f"step_{index}" for index in range(40)]
    second = [*first[:-1], "other_end"]
    labels = [
        get_tool_path(SessionView.from_session(session, session.nodes))
        for session in (_path_session(1, first), _path_session(2, second))
    ]
    assert all(label is not None and len(label) <= 120 for label in labels)
    assert labels[0] != labels[1]
    assert labels[0] is not None
    assert labels[0].startswith("step_0 -> ... -> step_39 (40 steps, path ")


def _mixed_sessions() -> list[SessionWithNodesResponse]:
    sessions = _priced_sessions(["10.00", "10.00"] + ["0.50"] * 18)
    sessions += [_timed_session(number) for number in range(21, 26)]
    sessions += [_growth_session(number, [1_000, 6_000]) for number in range(26, 30)]
    sessions += [
        _path_session(number, ["search", "answer"]) for number in range(30, 40)
    ]
    sessions += [_path_session(40, ["escalate"])]
    sessions += _sized_sessions([2_000] * 20 + [50_000], start=41, cached=0)
    return sessions


async def test_full_resource_prompts_fit_the_length_bound() -> None:
    # A 118-character tool name stays one component after its suffix is added.
    slow_tool = "search_" + "x" * 111
    sessions = [
        _session(
            number,
            [
                _tool(number, 0, slow_tool, start=0, seconds=6),
                _tool(number, 1, slow_tool, start=2, seconds=6),
                _tool(number, 2, f"tool_{number}" if number % 2 else "answer", start=8),
                _llm(number, 3, input_tokens=1_000, cached=0, start=9, seconds=0.5),
                _llm(
                    number,
                    4,
                    input_tokens=60_000 if number % 10 == 0 else 6_000,
                    cached=0,
                    start=9.5,
                    seconds=0.5,
                ),
            ],
            cost="10.00" if number % 10 == 0 else "0.50",
            duration=10,
        )
        for number in range(1, 2_501)
    ]
    profile = profile_sessions(sessions)
    candidates = [item for item in profile.candidates if item.id in RESOURCE_IDS]
    assert {item.id for item in candidates} == RESOURCE_IDS
    assert _get(profile, "component-time-share").title.startswith(f"{slow_tool} (tool)")
    context = InsightGenerationContext(
        agent_id=AGENT_ID,
        agent_name="returns-agent",
        source_import=SourceImportContext(import_id=IMPORT_ID, provider="langfuse"),
        server_url="https://kitaru.example.test",
    )
    for candidate in candidates:
        result = await generate_insights_from_profile(
            profile.model_copy(update={"candidates": [candidate]}), context=context
        )
        assert result.insights, candidate.id
        metadata = result.card_metadata(result.insights[0])
        assert len(metadata.contributing_session_ids) == 200, candidate.id
        assert len(metadata.investigation_prompt) < 16_000, candidate.id


def test_resource_findings_ignore_arrival_order_and_spilling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessions = _mixed_sessions()
    expected = profile_sessions(sessions)
    assert {item.id for item in expected.candidates} >= RESOURCE_IDS
    shuffled = list(sessions)
    random.Random(7).shuffle(shuffled)
    assert profile_sessions(shuffled) == expected
    monkeypatch.setattr(
        resource_profile, "ObservationStore", partial(ObservationStore, memory_limit=3)
    )
    assert profile_sessions(reversed(sessions)) == expected


def test_observation_store_queries_match_after_spilling() -> None:
    rows = [
        Observation(
            value=value % 7,
            session=f"s{value % 5}",
            node=f"n{value}",
            key=f"k{value % 4}",
        )
        for value in range(40)
    ] + [Observation(value=3, session="s9", node="n99", key="unique")]

    def read(store: ObservationStore) -> tuple[object, ...]:
        for row in rows:
            store.add("rows", row)
        return (
            store.count("rows", minimum=3),
            store.count_sessions("rows", minimum=5),
            store.get_median("rows"),
            list(store.iter_highest("rows", key="k1", minimum=2)),
            store.get_unique_key_sessions("rows", 5),
        )

    in_memory, spilled = ObservationStore(), ObservationStore(memory_limit=4)
    try:
        results = read(in_memory)
        assert results == read(spilled)
        assert results[4] == (1, ["s9"])
        with pytest.raises(ValueError, match="supported range"):
            in_memory.add("rows", Observation(value=-1, session="s"))
    finally:
        in_memory.close()
        spilled.close()


async def test_resource_cards_carry_investigation_labels() -> None:
    profile = profile_sessions(_mixed_sessions())
    profile = profile.model_copy(
        update={
            "candidates": [
                item for item in profile.candidates if item.id in RESOURCE_IDS
            ]
        }
    )
    result = await generate_insights_from_profile(
        profile,
        context=InsightGenerationContext(
            agent_id=AGENT_ID,
            source_import=SourceImportContext(import_id=IMPORT_ID),
        ),
    )
    labels = {
        insight.name: insight.metadata[INSIGHT_METADATA_KEY]["cta_label"]
        for insight in result.insights
    }
    assert labels == {
        "cost-concentration": "Investigate cost concentration",
        "component-time-share": "Investigate where time goes",
        "context-growth": "Investigate context growth",
        "prompt-cache": "Investigate prompt cache use",
        "tool-paths": "Investigate tool paths",
    }
