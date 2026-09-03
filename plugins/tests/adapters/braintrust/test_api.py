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

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from kitaru.task import importer as importer_module
from kitaru.task.importer import ImportedSession
from kitaru_braintrust_importer.api import fetch, serialize_spans
from kitaru_braintrust_importer.importer import parse

from ..fetch_helpers import collect_payloads
from .fixtures import FakeBraintrust, build_complete_rows, build_session_rows


async def test_fetch_trace_ids_fetches_exactly_those_in_order(
    fake_braintrust: FakeBraintrust,
) -> None:
    """Fetch exactly the given trace ids and ignore the time window."""
    fake_braintrust.rows_builders = [build_complete_rows, build_complete_rows]

    payloads = await collect_payloads(
        fetch({"project_id": "project-1", "trace_ids": ["root-a", "root-b"]})
    )

    assert fake_braintrust.requested == ["root-a", "root-b"]
    assert fake_braintrust.list_queries == []
    assert len(payloads) == 1
    sessions = list(parse(payloads[0], {}))
    assert len(sessions) == 2
    assert all(isinstance(session, ImportedSession) for session in sessions)
    assert [session.external_id for session in sessions] == [
        "project-1:root-a",
        "project-1:root-b",
    ]


async def test_time_window_lists_root_span_ids_and_fetches_each_trace(
    fake_braintrust: FakeBraintrust,
) -> None:
    """List root span ids in the window, then fetch each trace in order."""
    fake_braintrust.list_pages = [(["root-a", "root-b"], None)]
    fake_braintrust.rows_builders = [build_complete_rows, build_complete_rows]

    payloads = await collect_payloads(
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
    assert len(payloads) == 1
    sessions = list(parse(payloads[0], {}))
    assert len(sessions) == 2
    assert isinstance(sessions[1], ImportedSession)
    assert sessions[1].external_id == "project-1:root-b"


async def test_time_window_paginates_through_multiple_list_pages(
    fake_braintrust: FakeBraintrust,
) -> None:
    """Follow the BTQL cursor across pages and fetch ids in listing order."""
    fake_braintrust.list_pages = [
        (["root-a"], "cursor-1"),
        (["root-b"], None),
    ]
    fake_braintrust.rows_builders = [build_complete_rows, build_complete_rows]

    payloads = await collect_payloads(
        fetch({"project_id": "project-1", "since": "2026-01-01T00:00:00+00:00"})
    )

    assert len(fake_braintrust.list_queries) == 2
    assert fake_braintrust.list_cursors_received == [None, "cursor-1"]
    assert fake_braintrust.requested == ["root-a", "root-b"]
    assert len(payloads) == 1


async def test_traces_sharing_a_session_are_fetched_in_one_payload(
    fake_braintrust: FakeBraintrust,
) -> None:
    """Merge same-session traces into one Kitaru session, not a dropped duplicate."""
    fake_braintrust.list_pages = [(["root-a", "root-b", "root-c"], None)]
    fake_braintrust.rows_builders = [
        build_session_rows("sess-1"),
        build_complete_rows,
        build_session_rows("sess-1"),
    ]

    payloads = await collect_payloads(
        fetch({"project_id": "project-1", "since": "2026-01-01T00:00:00+00:00"})
    )

    assert len(payloads) == 1
    sessions = [
        item for item in parse(payloads[0], {}) if isinstance(item, ImportedSession)
    ]
    assert len(sessions) == 2
    # sess-1 groups root-a, the first trace read from the payload, so it is
    # emitted before root-b even though "root-b" sorts before "sess-1".
    assert [session.external_id for session in sessions] == [
        "project-1:sess-1",
        "project-1:root-b",
    ]
    by_id = {session.external_id: session for session in sessions}
    shared_session = by_id["project-1:sess-1"]
    assert shared_session.metadata["braintrust.trace_ids"] == ["root-a", "root-c"]
    assert len(shared_session.nodes) == 2
    assert {node.trace_id for node in shared_session.nodes} == {"root-a", "root-c"}


async def test_until_defaults_to_now(fake_braintrust: FakeBraintrust) -> None:
    """Default until to the current time when it is omitted."""
    fake_braintrust.list_pages = [([], None)]
    before = datetime.now(UTC)

    await collect_payloads(
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

    payloads = await collect_payloads(
        fetch({"project_id": "project-1", "since": "2026-01-01T00:00:00+00:00"})
    )

    assert payloads == []
    assert fake_braintrust.requested == []


async def test_fetch_bounds_concurrency_and_preserves_order(
    fake_braintrust: FakeBraintrust,
) -> None:
    """Fetch at most the configured concurrency of traces at once, oldest first."""
    trace_ids = ["root-a", "root-b", "root-c", "root-d"]
    fake_braintrust.rows_builders = [build_complete_rows] * len(trace_ids)
    # Delays scramble completion order relative to submission order, so the
    # merged result proves gather_bounded restores it rather than happening
    # to already match it.
    fake_braintrust.fetch_delays = [0.03, 0.01, 0.02, 0.0]

    payloads = await collect_payloads(
        fetch({"project_id": "project-1", "trace_ids": trace_ids, "concurrency": 2})
    )

    assert fake_braintrust.peak_in_flight == 2
    assert len(payloads) == 1
    sessions = [
        item for item in parse(payloads[0], {}) if isinstance(item, ImportedSession)
    ]
    assert [session.external_id for session in sessions] == [
        f"project-1:{trace_id}" for trace_id in trace_ids
    ]

    # The default query still works at the default concurrency.
    fake_braintrust.rows_builders = [build_complete_rows, build_complete_rows]
    payloads = await collect_payloads(
        fetch({"project_id": "project-1", "trace_ids": ["root-e", "root-f"]})
    )
    assert len(payloads) == 1


async def test_fetch_waits_out_a_rate_limit_and_succeeds(
    fake_braintrust: FakeBraintrust, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sleep for the reported delay once, then fetch the unthrottled payload."""
    sleeps: list[float] = []

    async def _sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(importer_module.asyncio, "sleep", _sleep)
    fake_braintrust.rows_builders = [build_complete_rows]
    fake_braintrust.raise_once = httpx.HTTPStatusError(
        "rate limited",
        request=httpx.Request("POST", "https://api.braintrust.dev/btql"),
        response=httpx.Response(
            429,
            headers={"Retry-After": "5"},
            request=httpx.Request("POST", "https://api.braintrust.dev/btql"),
        ),
    )

    payloads = await collect_payloads(
        fetch({"project_id": "project-1", "trace_ids": ["root-a"]})
    )

    assert sleeps == [5.0]
    assert payloads == [serialize_spans(build_complete_rows("root-a"))]


async def test_fetch_propagates_a_non_rate_limit_error(
    fake_braintrust: FakeBraintrust,
) -> None:
    """Propagate a non-429 error unchanged, without retrying."""
    fake_braintrust.raise_once = httpx.HTTPStatusError(
        "server error",
        request=httpx.Request("POST", "https://api.braintrust.dev/btql"),
        response=httpx.Response(
            500, request=httpx.Request("POST", "https://api.braintrust.dev/btql")
        ),
    )

    with pytest.raises(httpx.HTTPStatusError):
        await collect_payloads(
            fetch({"project_id": "project-1", "trace_ids": ["root-a"]})
        )


@pytest.mark.parametrize(
    ("query", "match"),
    [
        (
            {"project_id": "project-1", "unexpected": 1},
            "Extra inputs are not permitted",
        ),
        ({"trace_ids": ["root-a"]}, "Field required"),
    ],
)
async def test_fetch_rejects_invalid_queries(query: dict[str, Any], match: str) -> None:
    """Reject an invalid query before yielding any payload."""
    with pytest.raises(ValueError, match=match):
        await collect_payloads(fetch(query))
