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

from kitaru.api_models.v1.session import SessionStatus
from kitaru.task import importer as importer_module
from kitaru.task.importer import ImportedSession
from kitaru_langsmith_importer.adapter import _PARSER_PARAMS
from kitaru_langsmith_importer.api import fetch, serialize_runs
from kitaru_langsmith_importer.importer import parse

from ..fetch_helpers import collect_payloads
from .fixtures import (
    PROJECT_ID,
    FakeLangSmith,
    RunsBuilder,
    build_complete_runs,
    build_run,
)


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


async def test_fetch_by_trace_ids_fetches_exactly_those_in_one_payload(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """Fetch the given trace ids in order and yield one combined payload."""
    trace_id_1 = str(uuid.uuid4())
    trace_id_2 = str(uuid.uuid4())
    fake_langsmith_api.runs_builders = [build_complete_runs, build_complete_runs]

    payloads = await collect_payloads(fetch({"trace_ids": [trace_id_1, trace_id_2]}))

    # Fetches run concurrently across threads, so dispatch order is not
    # guaranteed, but each requested trace is still fetched exactly once.
    assert sorted(fake_langsmith_api.requested) == sorted([trace_id_1, trace_id_2])
    assert fake_langsmith_api.root_listing_calls == []
    assert len(payloads) == 1
    # join_on=trace_id forces one session per trace, matching how the
    # live-recording adapter parses its own fetched payloads.
    sessions = [
        item
        for item in parse(payloads[0], _PARSER_PARAMS)
        if isinstance(item, ImportedSession)
    ]
    assert [session.external_id for session in sessions] == [
        f"{PROJECT_ID}:{trace_id_1}",
        f"{PROJECT_ID}:{trace_id_2}",
    ]
    assert all(session.status == SessionStatus.COMPLETED for session in sessions)


async def test_fetch_bounds_concurrency_and_preserves_order(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """Fetch at most the configured concurrency of traces at once, oldest first."""
    trace_ids = [str(uuid.uuid4()) for _ in range(4)]
    fake_langsmith_api.runs_builders = [build_complete_runs] * len(trace_ids)
    # Delays scramble completion order relative to submission order, so the
    # merged result proves gather_bounded restores it rather than happening
    # to already match it.
    fake_langsmith_api.fetch_delays = [0.03, 0.01, 0.02, 0.0]

    payloads = await collect_payloads(fetch({"trace_ids": trace_ids, "concurrency": 2}))

    assert fake_langsmith_api.peak_in_flight == 2
    assert len(payloads) == 1
    sessions = [
        item
        for item in parse(payloads[0], _PARSER_PARAMS)
        if isinstance(item, ImportedSession)
    ]
    assert [session.external_id for session in sessions] == [
        f"{PROJECT_ID}:{trace_id}" for trace_id in trace_ids
    ]

    # The default query still works at the default concurrency.
    other_trace_ids = [str(uuid.uuid4()), str(uuid.uuid4())]
    fake_langsmith_api.runs_builders = [build_complete_runs, build_complete_runs]
    payloads = await collect_payloads(fetch({"trace_ids": other_trace_ids}))
    assert len(payloads) == 1


async def test_fetch_time_window_lists_root_runs_oldest_first_and_dedupes(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """List root runs oldest first and fetch each distinct trace once."""
    trace_id_1 = str(uuid.uuid4())
    trace_id_2 = str(uuid.uuid4())
    root_run_1 = build_run(
        trace_id_1, trace_id_1, start_time=datetime(2026, 7, 24, 11, tzinfo=UTC)
    )
    root_run_2 = build_run(
        trace_id_2, trace_id_2, start_time=datetime(2026, 7, 24, 10, tzinfo=UTC)
    )
    # The listing is out of order and the same trace can surface twice, the
    # fetch must still sort oldest first and request each trace once.
    fake_langsmith_api.root_run_listings = [[root_run_1, root_run_1, root_run_2]]
    fake_langsmith_api.runs_builders = [build_complete_runs, build_complete_runs]

    payloads = await collect_payloads(
        fetch(
            {
                "since": "2026-07-01T00:00:00Z",
                "until": "2026-08-01T00:00:00Z",
                "project_name": "my-project",
            }
        )
    )

    assert len(payloads) == 1
    # Fetches run concurrently across threads, so dispatch order is not
    # guaranteed, but each distinct trace is still fetched exactly once.
    assert sorted(fake_langsmith_api.requested) == sorted([trace_id_1, trace_id_2])
    # root_run_2 started earlier than root_run_1, so it is merged first.
    sessions = [
        item for item in parse(payloads[0], {}) if isinstance(item, ImportedSession)
    ]
    assert [session.external_id for session in sessions] == [
        f"{PROJECT_ID}:{trace_id_2}",
        f"{PROJECT_ID}:{trace_id_1}",
    ]
    assert len(fake_langsmith_api.root_listing_calls) == 1
    call = fake_langsmith_api.root_listing_calls[0]
    assert call["project_name"] == "my-project"
    assert call["is_root"] is True
    assert call["start_time"] == datetime(2026, 7, 1, tzinfo=UTC)
    assert call["filter"] == 'lt(end_time, "2026-08-01T00:00:00+00:00")'


async def test_fetch_time_window_groups_shared_thread_traces_into_one_session(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """Fetch a thread's traces into one payload that parses into one session."""
    trace_id_1 = str(uuid.uuid4())
    trace_id_2 = str(uuid.uuid4())
    trace_id_3 = str(uuid.uuid4())
    thread_id = "thread-shared"
    # trace_3 starts earliest, then trace_1, then trace_2, listed out of order.
    root_run_3 = build_run(
        trace_id_3, trace_id_3, start_time=datetime(2026, 7, 24, 9, tzinfo=UTC)
    )
    root_run_1 = build_run(
        trace_id_1, trace_id_1, start_time=datetime(2026, 7, 24, 10, tzinfo=UTC)
    )
    root_run_2 = build_run(
        trace_id_2, trace_id_2, start_time=datetime(2026, 7, 24, 11, tzinfo=UTC)
    )
    fake_langsmith_api.root_run_listings = [[root_run_2, root_run_3, root_run_1]]
    # The fetched root runs carry the same start times as their listing rows,
    # so the parser's turn ordering, which sorts by started_at, resolves
    # unambiguously instead of tying on identical default timestamps.
    builder = _threaded_trace_runs(
        {trace_id_3: None, trace_id_1: thread_id, trace_id_2: thread_id},
        {
            trace_id_3: datetime(2026, 7, 24, 9, tzinfo=UTC),
            trace_id_1: datetime(2026, 7, 24, 10, tzinfo=UTC),
            trace_id_2: datetime(2026, 7, 24, 11, tzinfo=UTC),
        },
    )
    fake_langsmith_api.runs_builders = [builder, builder, builder]

    payloads = await collect_payloads(
        fetch({"since": "2026-07-01T00:00:00Z", "until": "2026-08-01T00:00:00Z"})
    )

    assert len(payloads) == 1
    # Fetches run concurrently across threads, so dispatch order is not
    # guaranteed, but each distinct trace is still fetched exactly once.
    assert sorted(fake_langsmith_api.requested) == sorted(
        [trace_id_3, trace_id_1, trace_id_2]
    )

    sessions = [
        item for item in parse(payloads[0], {}) if isinstance(item, ImportedSession)
    ]
    assert len(sessions) == 2
    shared_session = next(
        session
        for session in sessions
        if session.external_id == f"{PROJECT_ID}:{thread_id}"
    )
    assert [turn["source_trace_id"] for turn in shared_session.inputs["turns"]] == [
        trace_id_1,
        trace_id_2,
    ]
    assert {node.trace_id for node in shared_session.nodes} == {trace_id_1, trace_id_2}

    solo_session = next(
        session
        for session in sessions
        if session.external_id == f"{PROJECT_ID}:{trace_id_3}"
    )
    assert solo_session.inputs["turns"][0]["source_trace_id"] == trace_id_3


async def test_fetch_time_window_falls_back_to_the_default_project(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """Fall back to the SDK's tracer project when project_name is absent."""
    fake_langsmith_api.root_run_listings = [[]]

    await collect_payloads(fetch({"since": "2026-07-01T00:00:00Z"}))

    call = fake_langsmith_api.root_listing_calls[0]
    assert call["project_name"] == fake_langsmith_api.default_project_name


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


async def test_fetch_rejects_unknown_query_keys(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """Reject a query carrying a key outside the fetch contract."""
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        await anext(fetch({"bogus": True, "since": "2026-07-01T00:00:00Z"}))


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
