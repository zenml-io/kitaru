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
"""Focused contract tests for the LangSmith API fetch entrypoint."""

import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from langsmith.schemas import Run
from langsmith.utils import LangSmithRateLimitError

import kitaru_langsmith_importer.api as api_module
from kitaru.api_models.v1.session import SessionStatus
from kitaru.task import importer as importer_module
from kitaru.task.importer import ImportedNode, ImportedSession
from kitaru_langsmith_importer.adapter import _PARSER_PARAMS
from kitaru_langsmith_importer.api import fetch, serialize_runs
from kitaru_langsmith_importer.importer import importer, parse

from ..fetch_helpers import collect_payloads
from .fixtures import (
    PROJECT_ID,
    FakeLangSmith,
    RunsBuilder,
    build_child_run,
    build_complete_runs,
    build_root_run,
    build_run,
    seed_range_runs,
)


def _at(clock: str) -> datetime:
    """Return the fixture day's datetime at a UTC clock time."""
    return datetime.fromisoformat(f"2026-07-24T{clock}+00:00")


def _range_end(filter_str: str) -> datetime:
    """Return the exclusive upper bound encoded in a range listing filter."""
    match = re.fullmatch(r'lt\(start_time, "(?P<until>[^"]+)"\)', filter_str)
    assert match is not None, f"unexpected range filter {filter_str!r}"
    return datetime.fromisoformat(match["until"])


def _flatten(nodes: list[ImportedNode]) -> list[ImportedNode]:
    """Flatten imported nodes depth-first for assertions."""
    return [node for root in nodes for node in (root, *_flatten(root.children))]


def _threaded_trace_runs(
    thread_ids: dict[str, str | None], start_times: dict[str, datetime]
) -> RunsBuilder:
    """Build a runs builder whose root run carries the thread id of its trace.

    Fetches run concurrently, so the fake cannot rely on trace ids arriving
    in a fixed order. Looking the thread id and start time up by trace id,
    instead of by call position, keeps the builder correct regardless of
    dispatch order.
    """

    def build(trace_id: str) -> list[Run]:
        thread_id = thread_ids.get(trace_id)
        root_kwargs: dict[str, Any] = (
            {"extra": {"metadata": {"thread_id": thread_id}}} if thread_id else {}
        )
        return [
            build_run(
                trace_id, trace_id, start_time=start_times[trace_id], **root_kwargs
            ),
            build_run(
                str(uuid.uuid5(uuid.NAMESPACE_OID, f"{trace_id}-llm")),
                trace_id,
                parent_run_id=trace_id,
                name="llm-call",
                run_type="llm",
                start_time=start_times[trace_id] + timedelta(seconds=1),
            ),
        ]

    return build


async def test_fetch_by_trace_ids_fetches_exactly_those_in_order(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """Fetch the given trace ids in order, yielding one standalone payload each."""
    trace_id_1 = str(uuid.uuid4())
    trace_id_2 = str(uuid.uuid4())
    fake_langsmith_api.runs_builders = [build_complete_runs, build_complete_runs]

    payloads = await collect_payloads(fetch({"trace_ids": [trace_id_1, trace_id_2]}))

    # Fetches run concurrently across threads, so dispatch order is not
    # guaranteed, but each requested trace is still fetched exactly once.
    assert sorted(fake_langsmith_api.requested) == sorted([trace_id_1, trace_id_2])
    assert fake_langsmith_api.root_listing_calls == []
    assert len(payloads) == 2
    # join_on=trace_id forces one session per trace, matching how the
    # live-recording adapter parses its own fetched payloads.
    sessions = [
        item
        for payload in payloads
        for item in parse(payload, _PARSER_PARAMS)
        if isinstance(item, ImportedSession)
    ]
    assert [session.external_id for session in sessions] == [
        f"{PROJECT_ID}:{trace_id_1}",
        f"{PROJECT_ID}:{trace_id_2}",
    ]
    assert all(session.status == SessionStatus.COMPLETED for session in sessions)


async def test_fetch_by_trace_ids_holds_shared_threads_until_the_end(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """Yield a standalone trace as fetched and a shared thread once all are in."""
    trace_id_1 = str(uuid.uuid4())
    trace_id_2 = str(uuid.uuid4())
    trace_id_3 = str(uuid.uuid4())
    thread_id = "thread-shared"
    builder = _threaded_trace_runs(
        {trace_id_1: thread_id, trace_id_2: None, trace_id_3: thread_id},
        {
            trace_id_1: datetime(2026, 7, 24, 10, tzinfo=UTC),
            trace_id_2: datetime(2026, 7, 24, 11, tzinfo=UTC),
            trace_id_3: datetime(2026, 7, 24, 12, tzinfo=UTC),
        },
    )
    fake_langsmith_api.runs_builders = [builder, builder, builder]

    payloads = await collect_payloads(
        fetch({"trace_ids": [trace_id_1, trace_id_2, trace_id_3], "concurrency": 1})
    )

    assert len(payloads) == 2
    solo_sessions = [
        session
        for session in parse(payloads[0], {})
        if isinstance(session, ImportedSession)
    ]
    assert [session.external_id for session in solo_sessions] == [
        f"{PROJECT_ID}:{trace_id_2}"
    ]

    shared_sessions = [
        session
        for session in parse(payloads[1], {})
        if isinstance(session, ImportedSession)
    ]
    assert len(shared_sessions) == 1
    shared_session = shared_sessions[0]
    assert shared_session.external_id == f"{PROJECT_ID}:{thread_id}"
    assert {node.trace_id for node in shared_session.nodes} == {
        trace_id_1,
        trace_id_3,
    }


async def test_importer_fetch_matches_api_fetch(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """Yield the same payload from the importer instance as from the API fetch."""
    trace_id = str(uuid.uuid4())
    query = {"trace_ids": [trace_id]}
    fake_langsmith_api.runs_builders = [build_complete_runs]
    expected = await collect_payloads(fetch(query))

    fake_langsmith_api.runs_builders = [build_complete_runs]
    actual = await collect_payloads(importer.fetch(query))

    assert actual == expected


async def test_fetch_by_trace_ids_bounds_concurrency_and_preserves_order(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """Fetch at most the configured concurrency of traces at once, oldest first."""
    trace_ids = [str(uuid.uuid4()) for _ in range(4)]
    fake_langsmith_api.runs_builders = [build_complete_runs] * len(trace_ids)
    # Delays scramble completion order relative to submission order, so the
    # merged result proves stream_bounded restores it rather than happening
    # to already match it.
    fake_langsmith_api.fetch_delays = [0.03, 0.01, 0.02, 0.0]

    payloads = await collect_payloads(fetch({"trace_ids": trace_ids, "concurrency": 2}))

    assert fake_langsmith_api.peak_in_flight == 2
    assert len(payloads) == len(trace_ids)
    sessions = [
        item
        for payload in payloads
        for item in parse(payload, _PARSER_PARAMS)
        if isinstance(item, ImportedSession)
    ]
    assert [session.external_id for session in sessions] == [
        f"{PROJECT_ID}:{trace_id}" for trace_id in trace_ids
    ]

    # The default query still works at the default concurrency.
    other_trace_ids = [str(uuid.uuid4()), str(uuid.uuid4())]
    fake_langsmith_api.runs_builders = [build_complete_runs, build_complete_runs]
    payloads = await collect_payloads(fetch({"trace_ids": other_trace_ids}))
    assert len(payloads) == 2


async def test_fetch_time_window_lists_roots_and_batches_runs_into_one_payload(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """List root runs oldest first and read their runs in one range request.

    Three roots fit one batch, so a single range request covers all of them
    and one payload carries three sessions.
    """
    trace_ids = [str(uuid.uuid4()) for _ in range(3)]
    roots = [
        build_root_run(trace_id, start_time=_at(f"10:0{index}:00"))
        for index, trace_id in enumerate(trace_ids)
    ]
    # The listing is out of order and the same trace can surface twice, the
    # fetch must still sort oldest first and dedupe before batching.
    fake_langsmith_api.root_run_listings = [[roots[1], roots[0], roots[0], roots[2]]]
    seed_range_runs(fake_langsmith_api, roots)
    since = datetime(2026, 7, 1, tzinfo=UTC)
    until = datetime(2026, 8, 1, tzinfo=UTC)

    payloads = await collect_payloads(
        fetch(
            {
                "since": since.isoformat(),
                "until": until.isoformat(),
                "project_name": "my-project",
            }
        )
    )

    assert len(fake_langsmith_api.root_listing_calls) == 1
    root_call = fake_langsmith_api.root_listing_calls[0]
    assert root_call["project_name"] == "my-project"
    assert root_call["is_root"] is True
    assert root_call["start_time"] == since
    assert root_call["filter"] == f'lt(end_time, "{until.isoformat()}")'

    assert len(fake_langsmith_api.range_calls) == 1
    range_call = fake_langsmith_api.range_calls[0]
    assert range_call["project_name"] == "my-project"
    assert range_call["start_time"] == since
    latest_end = max(root.end_time for root in roots if root.end_time is not None)
    # The range reaches one second past the latest root end.
    assert _range_end(range_call["filter"]) == latest_end + timedelta(seconds=1)

    assert len(payloads) == 1
    sessions = [
        session
        for session in parse(payloads[0], {})
        if isinstance(session, ImportedSession)
    ]
    assert [session.external_id for session in sessions] == [
        f"{PROJECT_ID}:{trace_id}" for trace_id in trace_ids
    ]


async def test_fetch_time_window_yields_one_payload_per_batch(
    fake_langsmith_api: FakeLangSmith, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Split the listing into batches and yield each batch's complete sessions."""
    monkeypatch.setattr(api_module, "_TRACES_PER_BATCH", 1)
    trace_id_1 = str(uuid.uuid4())
    trace_id_2 = str(uuid.uuid4())
    root_1 = build_root_run(trace_id_1, start_time=_at("10:00:00"))
    root_2 = build_root_run(trace_id_2, start_time=_at("10:05:00"))
    fake_langsmith_api.root_run_listings = [[root_1, root_2]]
    seed_range_runs(fake_langsmith_api, [root_1, root_2])
    since = datetime(2026, 7, 1, tzinfo=UTC)

    # Range requests run in separate threads, so a concurrency of 1 is what
    # keeps their dispatch order, and therefore the recorded call order,
    # deterministic.
    payloads = await collect_payloads(
        fetch(
            {
                "since": since.isoformat(),
                "until": "2026-07-24T11:00:00Z",
                "concurrency": 1,
            }
        )
    )

    # Batch ranges are contiguous: each starts where the previous ended.
    ranges = [
        (call["start_time"], _range_end(call["filter"]))
        for call in fake_langsmith_api.range_calls
    ]
    assert ranges == [
        (since, _at("10:05:00")),
        (_at("10:05:00"), _at("10:05:03")),
    ]
    assert len(payloads) == 2
    first = [
        session
        for session in parse(payloads[0], {})
        if isinstance(session, ImportedSession)
    ]
    second = [
        session
        for session in parse(payloads[1], {})
        if isinstance(session, ImportedSession)
    ]
    assert [session.external_id for session in first] == [f"{PROJECT_ID}:{trace_id_1}"]
    assert [session.external_id for session in second] == [f"{PROJECT_ID}:{trace_id_2}"]


async def test_fetch_time_window_holds_a_session_until_all_its_traces_are_complete(
    fake_langsmith_api: FakeLangSmith, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Release a group only after the batch covering its last trace end.

    Thread A spans trace-1 and trace-3 with trace-2 in between, so the
    first batch yields nothing, the second yields trace-2 alone, and the
    third yields thread A with both of its traces in one payload.
    """
    monkeypatch.setattr(api_module, "_TRACES_PER_BATCH", 1)
    thread_id = "thread-A"
    trace_id_1 = str(uuid.uuid4())
    trace_id_2 = str(uuid.uuid4())
    trace_id_3 = str(uuid.uuid4())
    root_1 = build_root_run(trace_id_1, start_time=_at("10:00:00"), thread_id=thread_id)
    root_2 = build_root_run(trace_id_2, start_time=_at("10:05:00"))
    root_3 = build_root_run(trace_id_3, start_time=_at("10:10:00"), thread_id=thread_id)
    fake_langsmith_api.root_run_listings = [[root_1, root_2, root_3]]
    seed_range_runs(fake_langsmith_api, [root_1, root_2, root_3])

    payloads = await collect_payloads(
        fetch({"since": "2026-07-01T00:00:00Z", "until": "2026-07-24T11:00:00Z"})
    )

    assert len(fake_langsmith_api.range_calls) == 3
    assert len(payloads) == 2
    solo = [
        session
        for session in parse(payloads[0], {})
        if isinstance(session, ImportedSession)
    ]
    assert [session.external_id for session in solo] == [f"{PROJECT_ID}:{trace_id_2}"]

    shared = [
        session
        for session in parse(payloads[1], {})
        if isinstance(session, ImportedSession)
    ]
    assert len(shared) == 1
    shared_session = shared[0]
    assert shared_session.external_id == f"{PROJECT_ID}:{thread_id}"
    assert {node.trace_id for node in shared_session.nodes} == {
        trace_id_1,
        trace_id_3,
    }


async def test_fetch_time_window_holds_a_long_trace_until_its_end_is_covered(
    fake_langsmith_api: FakeLangSmith, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep a trace pending while a later batch may still hold its children."""
    monkeypatch.setattr(api_module, "_TRACES_PER_BATCH", 1)
    trace_id_1 = str(uuid.uuid4())
    trace_id_2 = str(uuid.uuid4())
    root_1 = build_root_run(
        trace_id_1, start_time=_at("10:00:00"), end_time=_at("10:06:40")
    )
    root_2 = build_root_run(trace_id_2, start_time=_at("10:05:00"))
    fake_langsmith_api.root_run_listings = [[root_1, root_2]]
    seed_range_runs(fake_langsmith_api, [root_1, root_2])
    # A child of trace-1 that starts after trace-2 lands in the second batch.
    fake_langsmith_api.range_runs.append(
        build_child_run(
            trace_id_1,
            start_time=_at("10:05:30"),
            end_time=_at("10:06:10"),
            name="late-child",
        )
    )

    payloads = await collect_payloads(
        fetch({"since": "2026-07-01T00:00:00Z", "until": "2026-07-24T11:00:00Z"})
    )

    # trace-2 completes in the second batch, and trace-1 only once that
    # batch's range reaches past its end at 10:06:40, so both release
    # together in the same payload.
    assert len(payloads) == 1
    sessions = [
        session
        for session in parse(payloads[0], {})
        if isinstance(session, ImportedSession)
    ]
    assert [session.external_id for session in sessions] == [
        f"{PROJECT_ID}:{trace_id_1}",
        f"{PROJECT_ID}:{trace_id_2}",
    ]
    assert {node.name for node in _flatten(sessions[0].nodes)} == {
        "kitaru-run",
        "llm-call",
        "late-child",
    }


async def test_fetch_time_window_drops_runs_of_unlisted_traces(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """Ignore runs in the range whose trace the root listing did not select."""
    trace_id = str(uuid.uuid4())
    unlisted_trace_id = str(uuid.uuid4())
    root = build_root_run(trace_id, start_time=_at("10:00:00"))
    fake_langsmith_api.root_run_listings = [[root]]
    seed_range_runs(fake_langsmith_api, [root])
    fake_langsmith_api.range_runs.append(
        build_root_run(unlisted_trace_id, start_time=_at("10:00:30"))
    )

    [payload] = await collect_payloads(
        fetch({"since": "2026-07-01T00:00:00Z", "until": "2026-07-24T11:00:00Z"})
    )

    sessions = [
        session
        for session in parse(payload, {})
        if isinstance(session, ImportedSession)
    ]
    assert [session.external_id for session in sessions] == [f"{PROJECT_ID}:{trace_id}"]


async def test_fetch_time_window_falls_back_to_the_default_project(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """Fall back to the SDK's tracer project when project_name is absent."""
    fake_langsmith_api.root_run_listings = [[]]

    await collect_payloads(fetch({"since": "2026-07-01T00:00:00Z"}))

    call = fake_langsmith_api.root_listing_calls[0]
    assert call["project_name"] == fake_langsmith_api.default_project_name
    assert fake_langsmith_api.range_calls == []


async def test_fetch_until_defaults_to_now(fake_langsmith_api: FakeLangSmith) -> None:
    """Default until to the current time when absent from the query."""
    fake_langsmith_api.root_run_listings = [[]]
    before = datetime.now(UTC)

    await collect_payloads(fetch({"since": "2026-07-01T00:00:00Z"}))

    after = datetime.now(UTC)
    call = fake_langsmith_api.root_listing_calls[0]
    match = re.fullmatch(r'lt\(end_time, "(?P<until>[^"]+)"\)', call["filter"])
    assert match is not None
    until = datetime.fromisoformat(match["until"])
    assert before <= until <= after


async def test_fetch_yields_nothing_for_an_empty_listing(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """Yield no payloads when the time window listing is empty."""
    fake_langsmith_api.root_run_listings = [[]]

    payloads = await collect_payloads(fetch({"since": "2026-07-01T00:00:00Z"}))

    assert payloads == []
    assert fake_langsmith_api.requested == []
    assert fake_langsmith_api.range_calls == []


async def test_fetch_rejects_unknown_query_keys(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """Reject a query carrying a key outside the fetch contract."""
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        await anext(fetch({"bogus": True, "since": "2026-07-01T00:00:00Z"}))


async def test_fetch_time_window_bounds_concurrency_across_batches(
    fake_langsmith_api: FakeLangSmith, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep at most concurrency range requests in flight and yield in batch order."""
    monkeypatch.setattr(api_module, "_TRACES_PER_BATCH", 1)
    trace_ids = [str(uuid.uuid4()) for _ in range(4)]
    roots = [
        build_root_run(trace_id, start_time=_at(f"10:0{index}:00"))
        for index, trace_id in enumerate(trace_ids)
    ]
    fake_langsmith_api.root_run_listings = [roots]
    seed_range_runs(fake_langsmith_api, roots)
    fake_langsmith_api.range_delays = [0.03, 0.0, 0.02, 0.0]

    payloads = await collect_payloads(
        fetch(
            {
                "since": "2026-07-01T00:00:00Z",
                "until": "2026-07-24T11:00:00Z",
                "concurrency": 2,
            }
        )
    )

    assert fake_langsmith_api.range_peak_in_flight == 2
    assert len(payloads) == len(trace_ids)
    sessions = [
        session
        for payload in payloads
        for session in parse(payload, {})
        if isinstance(session, ImportedSession)
    ]
    assert [session.external_id for session in sessions] == [
        f"{PROJECT_ID}:{trace_id}" for trace_id in trace_ids
    ]


async def test_fetch_waits_out_a_rate_limit_and_succeeds(
    fake_langsmith_api: FakeLangSmith, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sleep for the fixed delay once, then fetch the unthrottled payload."""
    sleeps: list[float] = []

    async def _sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(importer_module.asyncio, "sleep", _sleep)
    trace_id = str(uuid.uuid4())
    fake_langsmith_api.runs_builders = [build_complete_runs]
    fake_langsmith_api.raise_once = LangSmithRateLimitError("rate limit exceeded")

    payloads = await collect_payloads(fetch({"trace_ids": [trace_id]}))

    assert sleeps == [60.0]
    assert payloads == [serialize_runs(build_complete_runs(trace_id))]


async def test_fetch_propagates_a_non_rate_limit_error(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """Propagate a non-rate-limit error unchanged, without retrying."""
    fake_langsmith_api.raise_once = RuntimeError("server error")

    with pytest.raises(RuntimeError, match="server error"):
        await collect_payloads(fetch({"trace_ids": [str(uuid.uuid4())]}))


@pytest.mark.parametrize("source", [None, " explicit "])
async def test_file_and_api_source_identity_match(
    fake_langsmith_api: FakeLangSmith,
    source: str | None,
) -> None:
    """Query selection stays separate from parser identity for API and file inputs."""
    trace_id = "22222222-2222-4222-8222-222222222222"
    fake_langsmith_api.runs_builders = [build_complete_runs]
    [payload] = await collect_payloads(
        fetch({"project_name": "query-project", "trace_ids": [trace_id]})
    )
    params = {"source_instance": source, "join_on": "trace_id"}
    [api_session] = list(parse(payload, params))
    [file_session] = list(parse(serialize_runs(build_complete_runs(trace_id)), params))
    assert isinstance(api_session, ImportedSession)
    assert isinstance(file_session, ImportedSession)
    expected = source.strip() if source else "11111111-1111-4111-8111-111111111111"
    assert (
        api_session.external_id == file_session.external_id == f"{expected}:{trace_id}"
    )
