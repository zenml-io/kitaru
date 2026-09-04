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
"""Focused contract tests for the Logfire fetch entrypoint."""

from datetime import UTC, datetime

import httpx
import pytest

from kitaru.task import importer as importer_module
from kitaru.task.importer import ImportedSession
from kitaru_logfire_importer.api import fetch
from kitaru_logfire_importer.importer import parse

from ..fetch_helpers import collect_payloads
from .fixtures import (
    FakeLogfire,
    build_complete_rows,
    build_conversation_rows,
    build_list_row,
    ndjson,
)

TRACE_ID_1 = "a" * 32
TRACE_ID_2 = "b" * 32
TRACE_ID_3 = "c" * 32


async def test_fetch_by_trace_ids_fetches_exactly_those_traces_into_one_payload(
    fake_logfire: FakeLogfire,
) -> None:
    """Fetch exactly the requested trace ids, concatenated in order."""
    fake_logfire.fetch_builders = [build_complete_rows, build_complete_rows]

    payloads = await collect_payloads(
        fetch({"trace_ids": [TRACE_ID_1, TRACE_ID_2], "since": "2026-07-24T09:00:00Z"})
    )

    assert fake_logfire.events == ["fetch", "fetch"]
    assert fake_logfire.requested == [TRACE_ID_1, TRACE_ID_2]
    assert fake_logfire.fetch_min_timestamps == ["2026-07-24T09:00:00+00:00"] * 2
    assert len(payloads) == 1
    # Both traces share the default conversation id, so the parser groups
    # them into one session instead of dropping the second as a duplicate.
    sessions = list(parse(payloads[0], {}))
    assert len(sessions) == 1
    assert isinstance(sessions[0], ImportedSession)
    assert sessions[0].metadata["logfire.trace_ids"] == [TRACE_ID_1, TRACE_ID_2]


async def test_fetch_by_trace_ids_without_since_uses_the_earliest_bound(
    fake_logfire: FakeLogfire,
) -> None:
    """Fall back to the earliest possible timestamp without a since bound."""
    fake_logfire.fetch_builders = [build_complete_rows]

    await collect_payloads(fetch({"trace_ids": [TRACE_ID_1]}))

    assert fake_logfire.fetch_min_timestamps == [
        datetime.min.replace(tzinfo=UTC).isoformat()
    ]


async def test_fetch_bounds_concurrency_and_preserves_order(
    fake_logfire: FakeLogfire,
) -> None:
    """Fetch at most the configured concurrency of traces at once, oldest first."""
    trace_ids = [str(digit) * 32 for digit in range(1, 5)]
    fake_logfire.fetch_builders = [build_complete_rows] * len(trace_ids)
    # Delays scramble completion order relative to submission order, so the
    # merged result proves gather_bounded restores it rather than happening
    # to already match it.
    fake_logfire.fetch_delays = [0.03, 0.01, 0.02, 0.0]

    payloads = await collect_payloads(
        fetch(
            {
                "trace_ids": trace_ids,
                "since": "2026-07-24T09:00:00Z",
                "concurrency": 2,
            }
        )
    )

    assert fake_logfire.peak_in_flight == 2
    assert len(payloads) == 1
    sessions = [
        session
        for session in parse(payloads[0], {})
        if isinstance(session, ImportedSession)
    ]
    # All four traces share the default conversation id, so the parser
    # groups them into one session, merged in fetch order.
    assert len(sessions) == 1
    assert sessions[0].metadata["logfire.trace_ids"] == trace_ids

    # The default query still works at the default concurrency.
    fake_logfire.fetch_builders = [build_complete_rows, build_complete_rows]
    payloads = await collect_payloads(
        fetch({"trace_ids": [TRACE_ID_1, TRACE_ID_2], "since": "2026-07-24T09:00:00Z"})
    )
    assert len(payloads) == 1


async def test_fetch_by_time_window_lists_trace_ids_and_fetches_each(
    fake_logfire: FakeLogfire,
) -> None:
    """List trace ids within the window, then fetch each into one payload."""
    fake_logfire.list_builders = [
        lambda: [
            build_list_row(TRACE_ID_1, "2026-07-24T09:00:00Z"),
            build_list_row(TRACE_ID_2, "2026-07-24T09:05:00Z"),
        ]
    ]
    fake_logfire.fetch_builders = [build_complete_rows, build_complete_rows]

    payloads = await collect_payloads(
        fetch({"since": "2026-07-24T09:00:00Z", "until": "2026-07-24T10:00:00Z"})
    )

    assert fake_logfire.events == ["list", "fetch", "fetch"]
    assert fake_logfire.list_min_timestamps == ["2026-07-24T09:00:00+00:00"]
    assert fake_logfire.list_max_timestamps == ["2026-07-24T10:00:00+00:00"]
    assert fake_logfire.requested == [TRACE_ID_1, TRACE_ID_2]
    assert fake_logfire.fetch_min_timestamps == ["2026-07-24T09:00:00+00:00"] * 2
    assert len(payloads) == 1


async def test_fetch_by_time_window_groups_a_shared_session_into_one_session(
    fake_logfire: FakeLogfire,
) -> None:
    """Group traces sharing a session id even when listed apart in the window."""
    fake_logfire.list_builders = [
        lambda: [
            build_list_row(TRACE_ID_1, "2026-07-24T09:00:00Z"),
            build_list_row(TRACE_ID_2, "2026-07-24T09:05:00Z"),
            build_list_row(TRACE_ID_3, "2026-07-24T09:10:00Z"),
        ]
    ]
    fake_logfire.fetch_builders = [
        lambda trace_id: build_conversation_rows(trace_id, "conversation-a"),
        lambda trace_id: build_conversation_rows(trace_id, "conversation-b"),
        lambda trace_id: build_conversation_rows(trace_id, "conversation-a"),
    ]

    payloads = await collect_payloads(
        fetch({"since": "2026-07-24T09:00:00Z", "until": "2026-07-24T10:00:00Z"})
    )

    assert len(payloads) == 1
    sessions = [
        session
        for session in parse(payloads[0], {})
        if isinstance(session, ImportedSession)
    ]
    assert len(sessions) == 2
    by_trace_ids = {
        tuple(session.metadata["logfire.trace_ids"]): session for session in sessions
    }
    shared = by_trace_ids[(TRACE_ID_1, TRACE_ID_3)]
    assert len(shared.nodes) == 2
    assert {node.trace_id for node in shared.nodes} == {TRACE_ID_1, TRACE_ID_3}
    assert by_trace_ids[(TRACE_ID_2,)].nodes[0].trace_id == TRACE_ID_2


async def test_fetch_by_time_window_defaults_until_to_now(
    fake_logfire: FakeLogfire,
) -> None:
    """Default until to the current time when it is not given."""
    fake_logfire.list_builders = [lambda: []]
    before = datetime.now(UTC)

    await collect_payloads(fetch({"since": "2026-07-24T09:00:00Z"}))

    after = datetime.now(UTC)
    until = datetime.fromisoformat(fake_logfire.list_max_timestamps[0])
    assert before <= until <= after


async def test_fetch_by_time_window_yields_nothing_for_an_empty_listing(
    fake_logfire: FakeLogfire,
) -> None:
    """Yield nothing when the time window has no root traces."""
    fake_logfire.list_builders = [lambda: []]

    payloads = await collect_payloads(
        fetch({"since": "2026-07-24T09:00:00Z", "until": "2026-07-24T10:00:00Z"})
    )

    assert payloads == []
    assert fake_logfire.requested == []


async def test_fetch_waits_out_a_rate_limit_and_succeeds(
    fake_logfire: FakeLogfire, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sleep for the reported delay once, then fetch the unthrottled payload."""
    sleeps: list[float] = []

    async def _sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(importer_module.asyncio, "sleep", _sleep)
    fake_logfire.fetch_builders = [build_complete_rows]
    fake_logfire.raise_once = httpx.HTTPStatusError(
        "rate limited",
        request=httpx.Request("POST", "https://logfire-api.test/v2/query"),
        response=httpx.Response(
            429,
            headers={"Retry-After": "5"},
            request=httpx.Request("POST", "https://logfire-api.test/v2/query"),
        ),
    )

    payloads = await collect_payloads(
        fetch({"trace_ids": [TRACE_ID_1], "since": "2026-07-24T09:00:00Z"})
    )

    assert sleeps == [5.0]
    assert payloads == [ndjson(build_complete_rows(TRACE_ID_1))]


async def test_fetch_propagates_a_non_rate_limit_error(
    fake_logfire: FakeLogfire,
) -> None:
    """Propagate a non-429 error unchanged, without retrying."""
    fake_logfire.raise_once = httpx.HTTPStatusError(
        "server error",
        request=httpx.Request("POST", "https://logfire-api.test/v2/query"),
        response=httpx.Response(
            500, request=httpx.Request("POST", "https://logfire-api.test/v2/query")
        ),
    )

    with pytest.raises(httpx.HTTPStatusError):
        await collect_payloads(
            fetch({"trace_ids": [TRACE_ID_1], "since": "2026-07-24T09:00:00Z"})
        )


async def test_fetch_rejects_invalid_queries() -> None:
    """Reject a query that fails a fetch contract rule before the first yield."""
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        async for _ in fetch({"trace_ids": [TRACE_ID_1], "extra": 1}):
            pass
