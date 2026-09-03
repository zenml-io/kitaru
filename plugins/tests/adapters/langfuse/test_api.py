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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Focused contract tests for the Langfuse fetch entrypoint."""

import asyncio
from datetime import UTC, datetime

import pytest
from langfuse.api.core import ApiError

from kitaru.task.importer import ImportedNode, ImportedSession
from kitaru_langfuse_importer.api import fetch
from kitaru_langfuse_importer.importer import parse

from ..fetch_helpers import collect_payloads
from .fixtures import (
    FakeLangfuseClient,
    build_complete_trace,
    build_observation_v2,
    build_observations_page,
    build_trace_page,
    seed_default_observations,
)


def _flatten(nodes: list[ImportedNode]) -> list[ImportedNode]:
    """Flatten imported nodes depth-first for assertions."""
    return [node for root in nodes for node in (root, *_flatten(root.children))]


async def test_fetch_with_trace_ids_fetches_exactly_those_in_order(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Fetch exactly the requested trace ids, in the given order, as one payload."""
    fake_langfuse.trace_builders = [build_complete_trace, build_complete_trace]
    seed_default_observations(fake_langfuse, ["trace-2", "trace-1"])

    payloads = await collect_payloads(fetch({"trace_ids": ["trace-2", "trace-1"]}))

    assert fake_langfuse.requested == ["trace-2", "trace-1"]
    assert fake_langfuse.list_calls == []
    assert [call["trace_id"] for call in fake_langfuse.observation_calls] == [
        "trace-2",
        "trace-1",
    ]
    assert all(
        call["from_start_time"] is None and call["to_start_time"] is None
        for call in fake_langfuse.observation_calls
    )
    assert len(payloads) == 1
    parsed = list(parse(payloads[0], {}))
    sessions = [item for item in parsed if isinstance(item, ImportedSession)]
    assert len(sessions) == len(parsed) == 2
    assert [session.metadata["langfuse.trace_ids"] for session in sessions] == [
        ["trace-2"],
        ["trace-1"],
    ]


async def test_fetch_with_trace_ids_ignores_the_time_window(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Ignore since and until when trace_ids is present."""
    fake_langfuse.trace_builders = [build_complete_trace]
    seed_default_observations(fake_langfuse, ["trace-1"])

    payloads = await collect_payloads(
        fetch({"trace_ids": ["trace-1"], "since": "2020-01-01T00:00:00+00:00"})
    )

    assert fake_langfuse.requested == ["trace-1"]
    assert fake_langfuse.list_calls == []
    assert len(payloads) == 1


async def test_fetch_time_window_lists_across_two_pages_and_fetches_each_trace(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """List every page of the time window and read each listed trace's observations."""
    fake_langfuse.trace_list_pages = [
        build_trace_page(["trace-1", "trace-2"], page=1, total_pages=2),
        build_trace_page(["trace-3"], page=2, total_pages=2),
    ]
    seed_default_observations(fake_langfuse, ["trace-1", "trace-2", "trace-3"])
    since = "2026-07-01T00:00:00+00:00"
    until = "2026-07-24T00:00:00+00:00"

    payloads = await collect_payloads(fetch({"since": since, "until": until}))

    assert len(payloads) == 1
    assert [call["page"] for call in fake_langfuse.list_calls] == [1, 2]
    assert all(
        call["from_timestamp"] == datetime.fromisoformat(since)
        and call["to_timestamp"] == datetime.fromisoformat(until)
        and call["order_by"] == "timestamp.asc"
        for call in fake_langfuse.list_calls
    )
    assert [call["trace_id"] for call in fake_langfuse.observation_calls] == [
        "trace-1",
        "trace-2",
        "trace-3",
    ]
    assert all(
        call["from_start_time"] == datetime.fromisoformat(since)
        and call["to_start_time"] == datetime.fromisoformat(until)
        for call in fake_langfuse.observation_calls
    )
    sessions = list(parse(payloads[0], {}))
    assert len(sessions) == 3


async def test_fetch_paginates_observations_within_one_trace(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Follow the observations listing cursor across pages of one trace."""
    fake_langfuse.trace_list_pages = [
        build_trace_page(["trace-1"], page=1, total_pages=1)
    ]
    fake_langfuse.observation_pages = {
        "trace-1": [
            build_observations_page(
                [build_observation_v2("obs-root", "trace-1")], cursor="cursor-1"
            ),
            build_observations_page(
                [
                    build_observation_v2(
                        "obs-llm",
                        "trace-1",
                        parent_id="obs-root",
                        observation_type="GENERATION",
                        provided_model_name="gpt-5-nano",
                    )
                ]
            ),
        ]
    }
    since = "2026-07-01T00:00:00+00:00"
    until = "2026-07-24T00:00:00+00:00"

    payloads = await collect_payloads(fetch({"since": since, "until": until}))

    calls = fake_langfuse.observation_calls
    assert [call["trace_id"] for call in calls] == ["trace-1", "trace-1"]
    assert calls[0]["cursor"] is None
    assert calls[1]["cursor"] == "cursor-1"

    assert len(payloads) == 1
    sessions = [
        item for item in parse(payloads[0], {}) if isinstance(item, ImportedSession)
    ]
    assert len(sessions) == 1
    assert len(_flatten(sessions[0].nodes)) == 2


async def test_fetch_time_window_groups_shared_session_into_one_payload(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Group traces that share a Langfuse session id when parsed from one payload."""
    fake_langfuse.trace_list_pages = [
        build_trace_page(
            ["trace-1", "trace-2", "trace-3"],
            page=1,
            total_pages=1,
            session_ids={"trace-1": "session-A", "trace-2": "session-A"},
        )
    ]
    seed_default_observations(fake_langfuse, ["trace-1", "trace-2", "trace-3"])
    since = "2026-07-01T00:00:00+00:00"
    until = "2026-07-24T00:00:00+00:00"

    payloads = await collect_payloads(fetch({"since": since, "until": until}))

    assert len(payloads) == 1
    assert all(call["order_by"] == "timestamp.asc" for call in fake_langfuse.list_calls)

    parsed = list(parse(payloads[0], {}))
    sessions = [item for item in parsed if isinstance(item, ImportedSession)]
    assert len(sessions) == len(parsed) == 2

    shared = next(
        session
        for session in sessions
        if session.metadata["langfuse.session_id"] == "session-A"
    )
    assert shared.metadata["langfuse.trace_ids"] == ["trace-1", "trace-2"]
    nodes = _flatten(shared.nodes)
    assert {node.trace_id for node in nodes} == {"trace-1", "trace-2"}
    assert len(nodes) == 4


async def test_fetch_time_window_defaults_until_to_now(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Default until to the current time when it is not given."""
    fake_langfuse.trace_list_pages = [build_trace_page([], page=1, total_pages=0)]

    before = datetime.now(UTC)
    await collect_payloads(fetch({"since": "2026-07-01T00:00:00+00:00"}))
    after = datetime.now(UTC)

    until = fake_langfuse.list_calls[0]["to_timestamp"]
    assert before <= until <= after


async def test_fetch_yields_nothing_for_an_empty_listing(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Yield no payloads when the time window listing is empty."""
    fake_langfuse.trace_list_pages = [build_trace_page([], page=1, total_pages=0)]

    payloads = await collect_payloads(fetch({"since": "2026-07-01T00:00:00+00:00"}))

    assert payloads == []
    assert fake_langfuse.requested == []
    assert fake_langfuse.observation_calls == []


async def test_fetch_bounds_concurrency_and_preserves_order(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Fetch at most the configured concurrency of traces at once, oldest first."""
    trace_ids = ["trace-1", "trace-2", "trace-3", "trace-4"]
    fake_langfuse.trace_builders = [build_complete_trace] * len(trace_ids)
    # Delays scramble completion order relative to submission order, so the
    # merged result proves gather_bounded restores it rather than happening
    # to already match it.
    fake_langfuse.fetch_delays = [0.03, 0.01, 0.02, 0.0]
    seed_default_observations(fake_langfuse, trace_ids)

    payloads = await collect_payloads(fetch({"trace_ids": trace_ids, "concurrency": 2}))

    assert fake_langfuse.peak_in_flight == 2
    assert len(payloads) == 1
    sessions = [
        item for item in parse(payloads[0], {}) if isinstance(item, ImportedSession)
    ]
    assert [session.metadata["langfuse.trace_ids"][0] for session in sessions] == (
        trace_ids
    )

    # The default query still works at the default concurrency.
    fake_langfuse.trace_builders = [build_complete_trace, build_complete_trace]
    seed_default_observations(fake_langfuse, ["trace-5", "trace-6"])
    payloads = await collect_payloads(fetch({"trace_ids": ["trace-5", "trace-6"]}))
    assert len(payloads) == 1


async def test_fetch_rejects_an_invalid_query(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Raise ValueError before the first yield for an invalid query."""
    query = {"trace_ids": ["trace-1"], "bogus": 1}
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        await collect_payloads(fetch(query))

    assert fake_langfuse.requested == []
    assert fake_langfuse.list_calls == []
    assert fake_langfuse.observation_calls == []


async def test_fetch_retries_after_a_rate_limited_trace_and_succeeds(
    fake_langfuse: FakeLangfuseClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sleep for the reported retry-after and succeed on the next attempt."""
    sleeps: list[float] = []

    async def _sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", _sleep)

    fake_langfuse.trace_builders = [
        ApiError(status_code=429, headers={"retry-after": "3"}, body={}),
        build_complete_trace,
    ]
    seed_default_observations(fake_langfuse, ["trace-1"])

    payloads = await collect_payloads(fetch({"trace_ids": ["trace-1"]}))

    assert sleeps == [3.0]
    assert len(payloads) == 1


async def test_fetch_propagates_a_non_rate_limit_api_error(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Propagate an ApiError that is not a rate limit without retrying."""
    fake_langfuse.trace_builders = [
        ApiError(status_code=500, headers={}, body={"message": "boom"})
    ]

    with pytest.raises(ApiError):
        await collect_payloads(fetch({"trace_ids": ["trace-1"]}))

    assert fake_langfuse.observation_calls == []
