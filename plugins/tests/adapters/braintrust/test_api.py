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
"""Focused contract tests for the Braintrust API fetch entrypoint."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import pytest

from kitaru.task.importer import ImportedSession
from kitaru_braintrust_importer.api import fetch
from kitaru_braintrust_importer.importer import parse

from .fixtures import FakeBraintrust, build_complete_rows


async def _drain(payloads: AsyncIterator[bytes]) -> list[bytes]:
    """Collect all payloads yielded by a fetch call."""
    return [payload async for payload in payloads]


async def test_fetch_trace_ids_fetches_exactly_those_in_order(
    fake_braintrust: FakeBraintrust,
) -> None:
    """Fetch exactly the given trace ids and ignore the time window."""
    fake_braintrust.rows_builders = [build_complete_rows, build_complete_rows]

    payloads = await _drain(
        fetch({"project_id": "project-1", "trace_ids": ["root-a", "root-b"]})
    )

    assert fake_braintrust.requested == ["root-a", "root-b"]
    assert fake_braintrust.list_queries == []
    assert len(payloads) == 2
    sessions = list(parse(payloads[0], {}))
    assert len(sessions) == 1
    session = sessions[0]
    assert isinstance(session, ImportedSession)
    assert session.external_id == "project-1:root-a"


async def test_time_window_lists_root_span_ids_and_fetches_each_trace(
    fake_braintrust: FakeBraintrust,
) -> None:
    """List root span ids in the window, then fetch each trace in order."""
    fake_braintrust.list_pages = [(["root-a", "root-b"], None)]
    fake_braintrust.rows_builders = [build_complete_rows, build_complete_rows]

    payloads = await _drain(
        fetch(
            {
                "project_id": "project-1",
                "since": "2026-01-01T00:00:00+00:00",
                "until": "2026-01-02T00:00:00+00:00",
            }
        )
    )

    assert len(fake_braintrust.list_queries) == 1
    assert fake_braintrust.list_queries[0]["since"] == "2026-01-01T00:00:00+00:00"
    assert fake_braintrust.list_queries[0]["until"] == "2026-01-02T00:00:00+00:00"
    assert fake_braintrust.requested == ["root-a", "root-b"]
    assert len(payloads) == 2
    sessions = list(parse(payloads[1], {}))
    assert len(sessions) == 1
    assert isinstance(sessions[0], ImportedSession)
    assert sessions[0].external_id == "project-1:root-b"


async def test_time_window_paginates_through_multiple_list_pages(
    fake_braintrust: FakeBraintrust,
) -> None:
    """Follow the BTQL cursor across pages and fetch ids in listing order."""
    fake_braintrust.list_pages = [
        (["root-a"], "cursor-1"),
        (["root-b"], None),
    ]
    fake_braintrust.rows_builders = [build_complete_rows, build_complete_rows]

    payloads = await _drain(
        fetch({"project_id": "project-1", "since": "2026-01-01T00:00:00+00:00"})
    )

    assert len(fake_braintrust.list_queries) == 2
    assert fake_braintrust.list_cursors_received == [None, "cursor-1"]
    assert fake_braintrust.requested == ["root-a", "root-b"]
    assert len(payloads) == 2


async def test_until_defaults_to_now(fake_braintrust: FakeBraintrust) -> None:
    """Default until to the current time when it is omitted."""
    fake_braintrust.list_pages = [([], None)]
    before = datetime.now(UTC)

    await _drain(
        fetch({"project_id": "project-1", "since": "2020-01-01T00:00:00+00:00"})
    )

    after = datetime.now(UTC)
    until = datetime.fromisoformat(fake_braintrust.list_queries[0]["until"])
    assert before <= until <= after


async def test_fetch_yields_nothing_for_an_empty_listing(
    fake_braintrust: FakeBraintrust,
) -> None:
    """Yield no payloads when the time window listing has no root spans."""
    fake_braintrust.list_pages = [([], None)]

    payloads = await _drain(
        fetch({"project_id": "project-1", "since": "2026-01-01T00:00:00+00:00"})
    )

    assert payloads == []
    assert fake_braintrust.requested == []


@pytest.mark.parametrize(
    ("query", "match"),
    [
        (
            {"project_id": "project-1", "unexpected": 1},
            "Extra inputs are not permitted",
        ),
        ({"trace_ids": ["root-a"]}, "Field required"),
        (
            {"project_id": "project-1"},
            "since is required when trace_ids is absent",
        ),
        (
            {"project_id": "project-1", "trace_ids": "root-a"},
            "Input should be a valid list",
        ),
        (
            {"project_id": "project-1", "trace_ids": [1, 2]},
            "Input should be a valid string",
        ),
        (
            {"project_id": "project-1", "since": "not-a-date"},
            "Input should be a valid datetime",
        ),
        (
            {"project_id": "project-1", "since": "2026-01-01T00:00:00"},
            "Input should have timezone info",
        ),
        (
            {
                "project_id": "project-1",
                "since": "2026-01-01T00:00:00+00:00",
                "until": "not-a-date",
            },
            "Input should be a valid datetime",
        ),
        (
            {
                "project_id": "project-1",
                "since": "2026-01-02T00:00:00+00:00",
                "until": "2026-01-01T00:00:00+00:00",
            },
            "until must not be before since",
        ),
    ],
)
async def test_fetch_rejects_invalid_queries(query: dict[str, Any], match: str) -> None:
    """Reject an invalid query before yielding any payload."""
    with pytest.raises(ValueError, match=match):
        await _drain(fetch(query))
