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

import json
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

import kitaru_braintrust_importer.api as api_module
from kitaru.task import importer as importer_module
from kitaru.task.importer import ImportedSession, flatten_nodes
from kitaru_braintrust_importer.api import fetch, serialize_spans
from kitaru_braintrust_importer.importer import importer, parse

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


async def test_importer_fetch_matches_api_fetch(
    fake_braintrust: FakeBraintrust,
) -> None:
    """Yield the same payload from the importer instance as from the API fetch."""
    query = {"project_id": "project-1", "trace_ids": ["root-a"]}
    fake_braintrust.rows_builders = [build_complete_rows]
    expected = await collect_payloads(fetch(query))

    fake_braintrust.rows_builders = [build_complete_rows]
    actual = await collect_payloads(importer.fetch(query))

    assert actual == expected


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


async def test_window_uses_supported_btql_and_restores_creation_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Follow query-clause cursors and sort roots across pages by creation time."""
    requests: list[str] = []

    async def post(
        client: httpx.AsyncClient, api_url: str, body: dict[str, Any]
    ) -> httpx.Response:
        assert set(body) == {"query"}
        query = body["query"]
        requests.append(query)
        assert "sort: _pagination_key asc" in query
        if query.startswith("select: root_span_id"):
            assert "filter: is_root AND" in query
            if "cursor:" not in query:
                data = {
                    "data": [{"root_span_id": "newer", "created": "2026-01-02"}],
                    "cursor": "next-page",
                }
            else:
                assert query.endswith(" | cursor: 'next-page'")
                data = {"data": [{"root_span_id": "older", "created": "2026-01-01"}]}
        else:
            root_id = "older" if "root_span_id = 'older'" in query else "newer"
            data = {"data": build_complete_rows(root_id)}
        return httpx.Response(200, json=data)

    monkeypatch.setattr(api_module, "_post_btql", post)
    payloads = await collect_payloads(
        fetch({"project_id": "project-1", "since": "2026-01-01T00:00:00Z"})
    )

    sessions = list(parse(payloads[0], {}))
    assert [session.external_id for session in sessions] == [
        "project-1:older",
        "project-1:newer",
    ]
    assert len(requests) == 4


async def test_exact_trace_fetch_includes_spans_after_first_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A child on a later BTQL page remains attached to its imported root."""
    rows = build_complete_rows("root-a")
    requests: list[str] = []

    async def post(
        client: httpx.AsyncClient, api_url: str, body: dict[str, Any]
    ) -> httpx.Response:
        assert set(body) == {"query"}
        query = body["query"]
        requests.append(query)
        assert "sort: _pagination_key asc" in query
        if "cursor:" not in query:
            return httpx.Response(200, json={"data": rows[:1], "cursor": "child"})
        assert query.endswith(" | cursor: 'child'")
        return httpx.Response(200, json={"data": rows[1:]})

    monkeypatch.setattr(api_module, "_post_btql", post)
    payloads = await collect_payloads(
        fetch({"project_id": "project-1", "trace_ids": ["root-a"]})
    )

    assert json.loads(payloads[0])["events"] == rows
    sessions = list(parse(payloads[0], {}))
    assert len(sessions) == 1
    assert isinstance(sessions[0], ImportedSession)
    nodes = flatten_nodes(sessions[0].nodes)
    assert len(nodes) == 2
    assert nodes[1].parent_external_id == nodes[0].external_id
    assert len(requests) == 2


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


@pytest.mark.parametrize("source", [None, " explicit "])
async def test_file_and_api_source_identity_match(
    fake_braintrust: FakeBraintrust,
    source: str | None,
) -> None:
    """Query selection stays separate from parser identity for API and file inputs."""
    trace_id = "root-a"
    fake_braintrust.rows_builders = [build_complete_rows]
    fake_braintrust.project_id = "query-project"
    [payload] = await collect_payloads(
        fetch({"project_id": "query-project", "trace_ids": [trace_id]})
    )
    params = {"source_instance": source}
    [api_session] = list(parse(payload, params))
    [file_session] = list(
        parse(json.dumps(build_complete_rows(trace_id)).encode(), params)
    )
    assert isinstance(api_session, ImportedSession)
    assert isinstance(file_session, ImportedSession)
    expected = source.strip() if source else "project-1"
    assert (
        api_session.external_id == file_session.external_id == f"{expected}:{trace_id}"
    )
