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
"""Recorded and replayed token counts when cache tokens sit beside the input."""

from decimal import Decimal

import pytest

from kitaru.api_models.v1.session import TokenUsage
from kitaru.api_models.v1.session_node import NodeType, SessionWithNodesResponse
from kitaru_model_swap.calls import (
    Collection,
    RecordedCall,
    estimate_usage,
    reservation_usage,
)
from kitaru_model_swap.endpoint import read_usage
from kitaru_model_swap.params import Candidate, ModelSwapParams
from kitaru_model_swap.pricing import call_cost, resolve_price

PARAMS = ModelSwapParams(
    candidates=[Candidate(name="c", model="m", base_url="http://localhost:1/v1")]
)


def collect(
    session: SessionWithNodesResponse, tokens: list[TokenUsage]
) -> list[RecordedCall]:
    """Record `tokens` on the session's first model calls and collect those calls."""
    remaining = iter(tokens)
    nodes = [
        node.model_copy(update={"tokens": next(remaining, None)})
        if node.node_type is NodeType.LLM_CALL
        else node
        for node in session.nodes
    ]
    collection = Collection(max_per_group=100)
    collection.add_session(session.model_copy(update={"nodes": nodes}), PARAMS)
    by_node = {call.node_id: call for call in collection.calls}
    return [by_node[n.id] for n in nodes if n.id in by_node and n.tokens is not None]


def get_recorded_input(calls: list[RecordedCall]) -> list[int]:
    return [call.usage.input_tokens for call in calls]


def test_cache_reads_beside_the_input_count_toward_the_replay_size(
    quickstart_sessions: list[SessionWithNodesResponse],
) -> None:
    """A replay sends the whole prompt, so its estimate includes cache reads."""
    [call, _] = collect(
        quickstart_sessions[0],
        [
            TokenUsage(input_tokens=12, cached_input_tokens=24_000, output_tokens=5),
            TokenUsage(input_tokens=9_000, cached_input_tokens=3_000, output_tokens=5),
        ],
    )

    assert call.usage.input_tokens == call.estimated_input_tokens == 24_012


def test_one_separate_call_settles_the_other_calls_of_its_model(
    quickstart_sessions: list[SessionWithNodesResponse],
) -> None:
    """A source records every call of a model the same way."""
    calls = collect(
        quickstart_sessions[0],
        [
            TokenUsage(input_tokens=12, cached_input_tokens=24_000),
            TokenUsage(input_tokens=9_000, cached_input_tokens=3_000),
        ],
    )

    assert get_recorded_input(calls) == [24_012, 12_000]


def test_cache_reads_inside_the_input_are_not_counted_twice(
    quickstart_sessions: list[SessionWithNodesResponse],
) -> None:
    calls = collect(
        quickstart_sessions[0],
        [
            TokenUsage(input_tokens=9_000, cached_input_tokens=3_000),
            TokenUsage(input_tokens=9_500, cached_input_tokens=9_000),
        ],
    )

    assert get_recorded_input(calls) == [9_000, 9_500]


def test_cache_writes_are_settled_apart_from_cache_reads(
    quickstart_sessions: list[SessionWithNodesResponse],
) -> None:
    """Some sources count writes inside the input and reads beside it."""
    calls = collect(
        quickstart_sessions[0],
        [
            TokenUsage(
                input_tokens=50, cached_input_tokens=10, cache_write_input_tokens=20_000
            )
        ],
    )

    assert get_recorded_input(calls) == [20_050]


def test_dry_run_and_reservation_use_the_whole_prompt(
    quickstart_sessions: list[SessionWithNodesResponse],
) -> None:
    [call] = collect(
        quickstart_sessions[0],
        [TokenUsage(input_tokens=12, cached_input_tokens=24_000)],
    )

    assert estimate_usage(call, 4096).input_tokens == 24_012
    assert reservation_usage(call, 4096, strict=True).input_tokens == 24_012


@pytest.mark.parametrize(
    "usage",
    [
        {
            "prompt_tokens": 12,
            "completion_tokens": 5,
            "prompt_tokens_details": {"cached_tokens": 24_000},
        },
        {
            "input_tokens": 12,
            "output_tokens": 5,
            "input_tokens_details": {"cached_tokens": 24_000},
        },
    ],
)
def test_candidate_cached_tokens_beside_the_prompt_are_billed(
    usage: dict[str, object],
) -> None:
    """An endpoint counting cached tokens beside the prompt is not billed as 12."""
    read = read_usage(usage)
    candidate = Candidate(
        name="c",
        model="m",
        base_url="http://localhost:1/v1",
        price={
            "input_per_mtok": "1",
            "output_per_mtok": "0",
            "cached_input_per_mtok": "0.1",
        },
    )
    price = resolve_price(candidate)
    assert price is not None

    assert (read.input_tokens, read.cached_input_tokens) == (24_012, 24_000)
    assert call_cost(price, read) == (Decimal(12) + Decimal(2_400)) / 1_000_000


def test_candidate_cached_tokens_inside_the_prompt_are_unchanged() -> None:
    read = read_usage(
        {
            "prompt_tokens": 9_000,
            "completion_tokens": 5,
            "prompt_tokens_details": {"cached_tokens": 3_000},
        }
    )

    assert (read.input_tokens, read.cached_input_tokens) == (9_000, 3_000)
