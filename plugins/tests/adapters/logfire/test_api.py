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
from typing import Any

import pytest

from kitaru.task.importer import ImportedSession
from kitaru_logfire_importer.api import fetch
from kitaru_logfire_importer.importer import parse

from .fixtures import FakeLogfire, build_complete_rows, build_list_row

TRACE_ID_1 = "a" * 32
TRACE_ID_2 = "b" * 32


async def _collect(query: dict[str, Any]) -> list[bytes]:
    """Drain a fetch call into a list of payloads."""
    return [payload async for payload in fetch(query)]


async def test_fetch_by_trace_ids_fetches_exactly_those_traces_in_order(
    fake_logfire: FakeLogfire,
) -> None:
    """Fetch exactly the requested trace ids, in the order given."""
    fake_logfire.fetch_builders = [build_complete_rows, build_complete_rows]

    payloads = await _collect(
        {"trace_ids": [TRACE_ID_1, TRACE_ID_2], "since": "2026-07-24T09:00:00Z"}
    )

    assert fake_logfire.events == ["fetch", "fetch"]
    assert fake_logfire.requested == [TRACE_ID_1, TRACE_ID_2]
    assert fake_logfire.fetch_min_timestamps == ["2026-07-24T09:00:00+00:00"] * 2
    assert len(payloads) == 2
    for payload in payloads:
        sessions = list(parse(payload, {}))
        assert len(sessions) == 1
        assert isinstance(sessions[0], ImportedSession)


async def test_fetch_by_trace_ids_without_since_uses_the_earliest_bound(
    fake_logfire: FakeLogfire,
) -> None:
    """Fall back to the earliest possible timestamp without a since bound."""
    fake_logfire.fetch_builders = [build_complete_rows]

    await _collect({"trace_ids": [TRACE_ID_1]})

    assert fake_logfire.fetch_min_timestamps == [
        datetime.min.replace(tzinfo=UTC).isoformat()
    ]


async def test_fetch_by_time_window_lists_trace_ids_and_fetches_each(
    fake_logfire: FakeLogfire,
) -> None:
    """List trace ids within the window, then fetch each one."""
    fake_logfire.list_builders = [
        lambda: [
            build_list_row(TRACE_ID_1, "2026-07-24T09:00:00Z"),
            build_list_row(TRACE_ID_2, "2026-07-24T09:05:00Z"),
        ]
    ]
    fake_logfire.fetch_builders = [build_complete_rows, build_complete_rows]

    payloads = await _collect(
        {"since": "2026-07-24T09:00:00Z", "until": "2026-07-24T10:00:00Z"}
    )

    assert fake_logfire.events == ["list", "fetch", "fetch"]
    assert fake_logfire.list_min_timestamps == ["2026-07-24T09:00:00+00:00"]
    assert fake_logfire.list_max_timestamps == ["2026-07-24T10:00:00+00:00"]
    assert fake_logfire.requested == [TRACE_ID_1, TRACE_ID_2]
    assert fake_logfire.fetch_min_timestamps == ["2026-07-24T09:00:00+00:00"] * 2
    assert len(payloads) == 2


async def test_fetch_by_time_window_defaults_until_to_now(
    fake_logfire: FakeLogfire,
) -> None:
    """Default until to the current time when it is not given."""
    fake_logfire.list_builders = [lambda: []]
    before = datetime.now(UTC)

    await _collect({"since": "2026-07-24T09:00:00Z"})

    after = datetime.now(UTC)
    until = datetime.fromisoformat(fake_logfire.list_max_timestamps[0])
    assert before <= until <= after


async def test_fetch_by_time_window_yields_nothing_for_an_empty_listing(
    fake_logfire: FakeLogfire,
) -> None:
    """Yield nothing when the time window has no root traces."""
    fake_logfire.list_builders = [lambda: []]

    payloads = await _collect(
        {"since": "2026-07-24T09:00:00Z", "until": "2026-07-24T10:00:00Z"}
    )

    assert payloads == []
    assert fake_logfire.requested == []


@pytest.mark.parametrize(
    ("query", "match"),
    [
        (
            {"trace_ids": [TRACE_ID_1], "extra": 1},
            "Extra inputs are not permitted",
        ),
        ({}, "since is required when trace_ids is absent"),
        (
            {"trace_ids": "not-a-list", "since": "2026-07-24T09:00:00Z"},
            "Input should be a valid list",
        ),
        ({"since": "not-a-date"}, "Input should be a valid datetime"),
        ({"since": "2026-07-24T09:00:00"}, "Input should have timezone info"),
        (
            {"since": "2026-07-24T10:00:00Z", "until": "2026-07-24T09:00:00Z"},
            "until must not be before since",
        ),
    ],
)
async def test_fetch_rejects_invalid_queries(query: dict[str, Any], match: str) -> None:
    """Reject a query that fails a fetch contract rule before the first yield."""
    with pytest.raises(ValueError, match=match):
        async for _ in fetch(query):
            pass
