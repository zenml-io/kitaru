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
from functools import partial

import pytest
from langfuse.api.core import ApiError

import kitaru_langfuse_importer.api as api_module
from kitaru.task.importer import ImportedNode, ImportedSession
from kitaru_langfuse_importer.api import fetch
from kitaru_langfuse_importer.importer import importer, parse

from ..fetch_helpers import collect_payloads
from .fixtures import (
    FakeLangfuseClient,
    build_complete_trace,
    build_observation_v2,
    build_trace_page,
    seed_default_observations,
)


def _flatten(nodes: list[ImportedNode]) -> list[ImportedNode]:
    """Flatten imported nodes depth-first for assertions."""
    return [node for root in nodes for node in (root, *_flatten(root.children))]


_TIMESTAMPS = {
    "trace-1": "2026-07-24T10:00:00Z",
    "trace-2": "2026-07-24T10:05:00Z",
    "trace-3": "2026-07-24T10:10:00Z",
}


def _at(clock: str) -> datetime:
    """Return the fixture day's datetime at a UTC clock time."""
    return datetime.fromisoformat(f"2026-07-24T{clock}+00:00")


async def test_fetch_with_trace_ids_fetches_exactly_those_in_order(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Fetch exactly the requested trace ids, in the given order, one payload each.

    The trace fetch carries the observations inline, so no observations
    listing happens.
    """
    fake_langfuse.trace_builders = [build_complete_trace, build_complete_trace]

    payloads = await collect_payloads(fetch({"trace_ids": ["trace-2", "trace-1"]}))

    assert fake_langfuse.requested == ["trace-2", "trace-1"]
    assert fake_langfuse.list_calls == []
    assert fake_langfuse.observation_calls == []
    assert len(payloads) == 2
    sessions = [
        item
        for payload in payloads
        for item in parse(payload, {})
        if isinstance(item, ImportedSession)
    ]
    assert [session.metadata["langfuse.trace_ids"] for session in sessions] == [
        ["trace-2"],
        ["trace-1"],
    ]
    assert all(len(_flatten(session.nodes)) == 2 for session in sessions)


async def test_fetch_with_trace_ids_holds_shared_sessions_until_the_end(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Yield standalone traces as fetched and a shared session once all are in."""
    fake_langfuse.trace_builders = [
        partial(build_complete_trace, session_id="session-A"),
        build_complete_trace,
        partial(build_complete_trace, session_id="session-A"),
    ]

    payloads = await collect_payloads(
        fetch({"trace_ids": ["trace-1", "trace-2", "trace-3"], "concurrency": 1})
    )

    assert len(payloads) == 2
    [solo] = [
        item for item in parse(payloads[0], {}) if isinstance(item, ImportedSession)
    ]
    assert solo.metadata["langfuse.trace_ids"] == ["trace-2"]
    [shared] = [
        item for item in parse(payloads[1], {}) if isinstance(item, ImportedSession)
    ]
    assert shared.metadata["langfuse.session_id"] == "session-A"
    assert shared.metadata["langfuse.trace_ids"] == ["trace-1", "trace-3"]
    assert len(_flatten(shared.nodes)) == 4


async def test_importer_fetch_matches_api_fetch(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Yield the same payload from the importer instance as from the API fetch."""
    query = {"trace_ids": ["trace-1"]}
    fake_langfuse.trace_builders = [build_complete_trace]
    seed_default_observations(fake_langfuse, ["trace-1"])
    expected = await collect_payloads(fetch(query))

    fake_langfuse.trace_builders = [build_complete_trace]
    seed_default_observations(fake_langfuse, ["trace-1"])
    actual = await collect_payloads(importer.fetch(query))

    assert actual == expected


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


async def test_fetch_time_window_lists_across_two_pages_and_batches_observations(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """List every page of the time window and read observations in one range request.

    Three traces fit one batch, so a single observations request spanning
    the window covers all of them and one payload carries three sessions.
    """
    fake_langfuse.trace_list_pages = [
        build_trace_page(
            ["trace-1", "trace-2"], page=1, total_pages=2, timestamps=_TIMESTAMPS
        ),
        build_trace_page(["trace-3"], page=2, total_pages=2, timestamps=_TIMESTAMPS),
    ]
    seed_default_observations(
        fake_langfuse, ["trace-1", "trace-2", "trace-3"], start_times=_TIMESTAMPS
    )
    since = "2026-07-01T00:00:00+00:00"
    until = "2026-07-24T11:00:00+00:00"

    payloads = await collect_payloads(fetch({"since": since, "until": until}))

    assert [call["page"] for call in fake_langfuse.list_calls] == [1, 2]
    assert all(
        call["from_timestamp"] == datetime.fromisoformat(since)
        and call["to_timestamp"] == datetime.fromisoformat(until)
        for call in fake_langfuse.list_calls
    )
    [call] = fake_langfuse.observation_calls
    assert "trace_id" not in call
    assert call["from_start_time"] == datetime.fromisoformat(since)
    # The range reaches one second past the latest trace end.
    assert call["to_start_time"] == datetime.fromisoformat("2026-07-24T10:10:02+00:00")
    assert call["limit"] == 500

    assert len(payloads) == 1
    sessions = [
        item for item in parse(payloads[0], {}) if isinstance(item, ImportedSession)
    ]
    assert [session.metadata["langfuse.trace_ids"] for session in sessions] == [
        ["trace-1"],
        ["trace-2"],
        ["trace-3"],
    ]
    assert all(len(_flatten(session.nodes)) == 2 for session in sessions)


async def test_fetch_time_window_yields_one_payload_per_batch(
    fake_langfuse: FakeLangfuseClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Split the listing into batches and yield each batch's complete sessions."""
    monkeypatch.setattr(api_module, "_TRACES_PER_BATCH", 1)
    fake_langfuse.trace_list_pages = [
        build_trace_page(
            ["trace-1", "trace-2"],
            page=1,
            total_pages=1,
            session_ids={"trace-1": "session-A", "trace-2": "session-B"},
            timestamps=_TIMESTAMPS,
        )
    ]
    seed_default_observations(
        fake_langfuse, ["trace-1", "trace-2"], start_times=_TIMESTAMPS
    )
    since = "2026-07-01T00:00:00+00:00"

    payloads = await collect_payloads(
        fetch({"since": since, "until": "2026-07-24T11:00:00+00:00"})
    )

    # Batch ranges are contiguous: each starts where the previous ended.
    ranges = [
        (call["from_start_time"], call["to_start_time"])
        for call in fake_langfuse.observation_calls
    ]
    assert ranges == [
        (datetime.fromisoformat(since), _at("10:05:00")),
        (_at("10:05:00"), _at("10:05:02")),
    ]
    assert len(payloads) == 2
    first = [
        item for item in parse(payloads[0], {}) if isinstance(item, ImportedSession)
    ]
    second = [
        item for item in parse(payloads[1], {}) if isinstance(item, ImportedSession)
    ]
    assert [session.metadata["langfuse.session_id"] for session in first] == [
        "session-A"
    ]
    assert [session.metadata["langfuse.session_id"] for session in second] == [
        "session-B"
    ]


async def test_fetch_paginates_observations_within_one_batch(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Follow the observations listing cursor across pages of one range."""
    fake_langfuse.trace_list_pages = [
        build_trace_page(["trace-1"], page=1, total_pages=1)
    ]
    seed_default_observations(fake_langfuse, ["trace-1"])
    fake_langfuse.observation_page_size = 1

    payloads = await collect_payloads(
        fetch({"since": "2026-07-01T00:00:00+00:00", "until": "2026-07-24T11:00:00Z"})
    )

    calls = fake_langfuse.observation_calls
    assert [call.get("cursor") for call in calls] == [None, "offset-1"]
    assert len({call["from_start_time"] for call in calls}) == 1

    assert len(payloads) == 1
    sessions = [
        item for item in parse(payloads[0], {}) if isinstance(item, ImportedSession)
    ]
    assert len(sessions) == 1
    assert len(_flatten(sessions[0].nodes)) == 2


async def test_fetch_time_window_holds_a_session_until_all_its_traces_are_complete(
    fake_langfuse: FakeLangfuseClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Release a session only after the batch covering its last trace end.

    Session A spans trace-1 and trace-3 with trace-2 in between, so the
    first batch yields nothing, the second yields trace-2 alone, and the
    third yields session A with both of its traces in one payload.
    """
    monkeypatch.setattr(api_module, "_TRACES_PER_BATCH", 1)
    fake_langfuse.trace_list_pages = [
        build_trace_page(
            ["trace-1", "trace-2", "trace-3"],
            page=1,
            total_pages=1,
            session_ids={"trace-1": "session-A", "trace-3": "session-A"},
            timestamps=_TIMESTAMPS,
        )
    ]
    seed_default_observations(
        fake_langfuse, ["trace-1", "trace-2", "trace-3"], start_times=_TIMESTAMPS
    )

    payloads = await collect_payloads(
        fetch({"since": "2026-07-01T00:00:00+00:00", "until": "2026-07-24T11:00:00Z"})
    )

    assert len(fake_langfuse.observation_calls) == 3
    assert len(payloads) == 2
    solo = [
        item for item in parse(payloads[0], {}) if isinstance(item, ImportedSession)
    ]
    assert [session.metadata["langfuse.trace_ids"] for session in solo] == [["trace-2"]]
    [shared] = [
        item for item in parse(payloads[1], {}) if isinstance(item, ImportedSession)
    ]
    assert shared.metadata["langfuse.session_id"] == "session-A"
    assert shared.metadata["langfuse.trace_ids"] == ["trace-1", "trace-3"]
    nodes = _flatten(shared.nodes)
    assert {node.trace_id for node in nodes} == {"trace-1", "trace-3"}
    assert len(nodes) == 4


async def test_fetch_time_window_holds_a_long_trace_until_its_end_is_covered(
    fake_langfuse: FakeLangfuseClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep a trace pending while later batches may still hold its children."""
    monkeypatch.setattr(api_module, "_TRACES_PER_BATCH", 1)
    fake_langfuse.trace_list_pages = [
        build_trace_page(
            ["trace-1", "trace-2"],
            page=1,
            total_pages=1,
            timestamps=_TIMESTAMPS,
            latencies={"trace-1": 400.0},
        )
    ]
    seed_default_observations(
        fake_langfuse, ["trace-1", "trace-2"], start_times=_TIMESTAMPS
    )
    # A child of trace-1 that starts after trace-2 lands in the second batch.
    fake_langfuse.observations.append(
        build_observation_v2(
            "late-child",
            "trace-1",
            parent_id="obs-root",
            start_time="2026-07-24T10:06:00Z",
            end_time="2026-07-24T10:06:40Z",
        )
    )

    payloads = await collect_payloads(
        fetch({"since": "2026-07-01T00:00:00+00:00", "until": "2026-07-24T11:00:00Z"})
    )

    # trace-2 completes in the second batch, trace-1 only once that batch's
    # range reaches past its end at 10:06:40.
    assert len(payloads) == 1
    sessions = [
        item for item in parse(payloads[0], {}) if isinstance(item, ImportedSession)
    ]
    assert [session.metadata["langfuse.trace_ids"] for session in sessions] == [
        ["trace-1"],
        ["trace-2"],
    ]
    assert {node.name for node in _flatten(sessions[0].nodes)} == {
        "obs-root",
        "obs-llm",
        "late-child",
    }


async def test_fetch_time_window_drops_observations_of_unlisted_traces(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Ignore observations in the range whose trace the listing did not select."""
    fake_langfuse.trace_list_pages = [
        build_trace_page(["trace-1"], page=1, total_pages=1)
    ]
    seed_default_observations(fake_langfuse, ["trace-1", "trace-older"])

    [payload] = await collect_payloads(
        fetch({"since": "2026-07-01T00:00:00+00:00", "until": "2026-07-24T11:00:00Z"})
    )

    sessions = [
        item for item in parse(payload, {}) if isinstance(item, ImportedSession)
    ]
    assert [session.metadata["langfuse.trace_ids"] for session in sessions] == [
        ["trace-1"]
    ]


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
    # merged result proves stream_bounded restores it rather than happening
    # to already match it.
    fake_langfuse.fetch_delays = [0.03, 0.01, 0.02, 0.0]

    payloads = await collect_payloads(fetch({"trace_ids": trace_ids, "concurrency": 2}))

    assert fake_langfuse.peak_in_flight == 2
    assert len(payloads) == 4
    sessions = [
        item
        for payload in payloads
        for item in parse(payload, {})
        if isinstance(item, ImportedSession)
    ]
    assert [session.metadata["langfuse.trace_ids"][0] for session in sessions] == (
        trace_ids
    )

    # The default query still works at the default concurrency.
    fake_langfuse.trace_builders = [build_complete_trace, build_complete_trace]
    payloads = await collect_payloads(fetch({"trace_ids": ["trace-5", "trace-6"]}))
    assert len(payloads) == 2


async def test_fetch_time_window_bounds_concurrency_across_batches(
    fake_langfuse: FakeLangfuseClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep at most concurrency range requests in flight and yield in batch order."""
    monkeypatch.setattr(api_module, "_TRACES_PER_BATCH", 1)
    trace_ids = ["trace-1", "trace-2", "trace-3", "trace-4"]
    timestamps = {
        trace_id: f"2026-07-24T10:{index:02d}:00Z"
        for index, trace_id in enumerate(trace_ids)
    }
    fake_langfuse.trace_list_pages = [
        build_trace_page(trace_ids, page=1, total_pages=1, timestamps=timestamps)
    ]
    seed_default_observations(fake_langfuse, trace_ids, start_times=timestamps)
    fake_langfuse.observation_delays = [0.03, 0.0, 0.02, 0.0]

    payloads = await collect_payloads(
        fetch(
            {
                "since": "2026-07-01T00:00:00+00:00",
                "until": "2026-07-24T11:00:00Z",
                "concurrency": 2,
            }
        )
    )

    assert fake_langfuse.observation_peak_in_flight == 2
    assert len(payloads) == 4
    sessions = [
        item
        for payload in payloads
        for item in parse(payload, {})
        if isinstance(item, ImportedSession)
    ]
    assert [session.metadata["langfuse.trace_ids"][0] for session in sessions] == (
        trace_ids
    )


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


async def test_window_keeps_children_starting_after_until(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Fetch observations up to the trace end even when they start after until."""
    fake_langfuse.trace_list_pages = [
        build_trace_page(
            ["trace-1"], page=1, total_pages=1, latencies={"trace-1": 120.0}
        )
    ]
    fake_langfuse.observations = [
        build_observation_v2("root", "trace-1"),
        build_observation_v2(
            "late-child",
            "trace-1",
            parent_id="root",
            start_time="2026-07-24T10:01:00Z",
            end_time="2026-07-24T10:02:00Z",
        ),
    ]
    [payload] = await collect_payloads(
        fetch({"since": "2026-07-24T10:00:00Z", "until": "2026-07-24T10:00:30Z"})
    )
    [call] = fake_langfuse.observation_calls
    assert call["to_start_time"] == _at("10:02:01")
    [session] = list(parse(payload, {}))
    assert isinstance(session, ImportedSession)
    assert {node.name for node in _flatten(session.nodes)} == {"root", "late-child"}


@pytest.mark.parametrize("source", [None, " explicit "])
async def test_file_and_api_source_identity_match(
    fake_langfuse: FakeLangfuseClient,
    source: str | None,
) -> None:
    """Query selection stays separate from parser identity for API and file inputs."""
    trace_id = "trace-1"
    fake_langfuse.trace_builders = [build_complete_trace]
    seed_default_observations(fake_langfuse, [trace_id])
    [payload] = await collect_payloads(fetch({"trace_ids": [trace_id]}))
    params = {"source_instance": source}
    [api_session] = list(parse(payload, params))
    [file_session] = list(
        parse(
            build_complete_trace(trace_id).model_dump_json(by_alias=True).encode(),
            params,
        )
    )
    assert isinstance(api_session, ImportedSession)
    assert isinstance(file_session, ImportedSession)
    expected = source.strip() if source else "project-1"
    assert (
        api_session.external_id == file_session.external_id == f"{expected}:{trace_id}"
    )
