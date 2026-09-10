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


def _external_ids(payloads: list[bytes]) -> list[str]:
    """Return the external ids of every session parsed across payloads, in order."""
    return [
        item.external_id
        for payload in payloads
        for item in parse(payload, {})
        if isinstance(item, ImportedSession)
    ]


async def test_fetch_trace_ids_fetches_exactly_those_in_order(
    fake_braintrust: FakeBraintrust,
) -> None:
    """Fetch exactly the given trace ids, in one batch query, ignoring the window."""
    fake_braintrust.rows_builders = [build_complete_rows, build_complete_rows]

    payloads = await collect_payloads(
        fetch({"project_id": "project-1", "trace_ids": ["root-a", "root-b"]})
    )

    assert fake_braintrust.batch_queries == [["root-a", "root-b"]]
    assert fake_braintrust.requested == ["root-a", "root-b"]
    assert fake_braintrust.list_queries == []
    assert len(payloads) == 1
    assert _external_ids(payloads) == ["project-1:root-a", "project-1:root-b"]


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


async def test_trace_ids_chunks_separate_standalone_and_held_session_traces(
    fake_braintrust: FakeBraintrust, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Batch trace_ids into chunks, yield standalone traces per chunk, hold sessions."""
    monkeypatch.setattr(api_module, "_TRACES_PER_BATCH", 2)
    fake_braintrust.rows_builders_by_root_span_id = {
        "root-a": build_session_rows("sess-1"),
        "root-b": build_complete_rows,
        "root-c": build_session_rows("sess-1"),
        "root-d": build_complete_rows,
    }

    payloads = await collect_payloads(
        fetch(
            {
                "project_id": "project-1",
                "trace_ids": ["root-a", "root-b", "root-c", "root-d"],
            }
        )
    )

    # One query per chunk of two requested ids.
    assert fake_braintrust.batch_queries == [
        ["root-a", "root-b"],
        ["root-c", "root-d"],
    ]
    # The standalone trace of each chunk is yielded right after that chunk's
    # query, before the held session is known to be complete.
    assert len(payloads) == 3
    assert _external_ids(payloads[:2]) == ["project-1:root-b", "project-1:root-d"]

    # The session spanning both chunks is only yielded once every chunk is in.
    shared_sessions = [
        item for item in parse(payloads[2], {}) if isinstance(item, ImportedSession)
    ]
    assert len(shared_sessions) == 1
    shared_session = shared_sessions[0]
    assert shared_session.external_id == "project-1:sess-1"
    assert shared_session.metadata["braintrust.trace_ids"] == ["root-a", "root-c"]
    assert {node.trace_id for node in shared_session.nodes} == {"root-a", "root-c"}


async def test_trace_ids_chunk_concurrency_is_bounded_and_order_preserved(
    fake_braintrust: FakeBraintrust, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A trace_ids fetch bounds concurrency across chunks and keeps request order."""
    monkeypatch.setattr(api_module, "_TRACES_PER_BATCH", 1)
    trace_ids = ["root-a", "root-b", "root-c", "root-d"]
    fake_braintrust.rows_builders = [build_complete_rows] * len(trace_ids)
    # Delays scramble completion order relative to submission order, so the
    # merged result proves stream_bounded restores it rather than happening
    # to already match it.
    fake_braintrust.fetch_delays = [0.03, 0.01, 0.02, 0.0]

    payloads = await collect_payloads(
        fetch({"project_id": "project-1", "trace_ids": trace_ids, "concurrency": 2})
    )

    assert fake_braintrust.peak_in_flight == 2
    assert len(fake_braintrust.batch_queries) == 4
    assert len(payloads) == 4
    assert _external_ids(payloads) == [
        f"project-1:{trace_id}" for trace_id in trace_ids
    ]


async def test_window_batches_single_trace_sessions_at_the_batch_size(
    fake_braintrust: FakeBraintrust, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A window with many single-trace sessions issues one query per batch."""
    monkeypatch.setattr(api_module, "_TRACES_PER_BATCH", 2)
    root_span_ids = ["root-a", "root-b", "root-c", "root-d", "root-e"]
    fake_braintrust.list_pages = [(list(root_span_ids), None)]
    fake_braintrust.rows_builders_by_root_span_id = {
        root_span_id: build_complete_rows for root_span_id in root_span_ids
    }

    payloads = await collect_payloads(
        fetch({"project_id": "project-1", "since": "2026-01-01T00:00:00+00:00"})
    )

    assert fake_braintrust.batch_queries == [
        ["root-a", "root-b"],
        ["root-c", "root-d"],
        ["root-e"],
    ]
    assert len(payloads) == 3
    assert _external_ids(payloads) == [
        f"project-1:{root_span_id}" for root_span_id in root_span_ids
    ]


async def test_window_session_spanning_two_traces_is_never_split_across_batches(
    fake_braintrust: FakeBraintrust, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Merge same-session traces into one query even at a tight batch boundary."""
    monkeypatch.setattr(api_module, "_TRACES_PER_BATCH", 1)
    fake_braintrust.list_pages = [
        ([("root-a", "sess-1"), "root-b", ("root-c", "sess-1")], None)
    ]
    fake_braintrust.rows_builders_by_root_span_id = {
        "root-a": build_session_rows("sess-1"),
        "root-b": build_complete_rows,
        "root-c": build_session_rows("sess-1"),
    }

    payloads = await collect_payloads(
        fetch({"project_id": "project-1", "since": "2026-01-01T00:00:00+00:00"})
    )

    # sess-1's two traces are packed into one batch despite the batch size
    # of 1, since a group is never split. root-b starts its own batch.
    assert fake_braintrust.batch_queries == [["root-a", "root-c"], ["root-b"]]
    assert len(payloads) == 2
    shared_sessions = [
        item for item in parse(payloads[0], {}) if isinstance(item, ImportedSession)
    ]
    assert len(shared_sessions) == 1
    shared_session = shared_sessions[0]
    assert shared_session.external_id == "project-1:sess-1"
    assert shared_session.metadata["braintrust.trace_ids"] == ["root-a", "root-c"]
    assert len(shared_session.nodes) == 2
    assert {node.trace_id for node in shared_session.nodes} == {"root-a", "root-c"}

    other_sessions = [
        item for item in parse(payloads[1], {}) if isinstance(item, ImportedSession)
    ]
    assert len(other_sessions) == 1
    assert other_sessions[0].external_id == "project-1:root-b"


async def test_time_window_paginates_through_multiple_list_pages(
    fake_braintrust: FakeBraintrust,
) -> None:
    """Follow the BTQL cursor across list pages and fetch ids in listing order."""
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
    # Both roots pack into a single batch, so both traces land in one payload.
    assert len(payloads) == 1


async def test_window_batch_query_paginates_through_multiple_pages(
    fake_braintrust: FakeBraintrust,
) -> None:
    """Follow the cursor across pages of one batch query and merge the rows."""
    fake_braintrust.list_pages = [(["root-a", "root-b"], None)]
    fake_braintrust.rows_builders_by_root_span_id = {
        "root-a": build_complete_rows,
        "root-b": build_complete_rows,
    }
    fake_braintrust.batch_page_size = 1

    payloads = await collect_payloads(
        fetch({"project_id": "project-1", "since": "2026-01-01T00:00:00+00:00"})
    )

    # One query per batch, but the four rows across both traces are only
    # answered one row at a time, so the cursor is followed three times.
    assert len(fake_braintrust.batch_queries) == 1
    assert fake_braintrust.batch_cursors_received == [None, "1", "2", "3"]
    assert len(payloads) == 1
    assert _external_ids(payloads) == ["project-1:root-a", "project-1:root-b"]


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
            assert (
                "filter: (root_span_id = 'older') OR (root_span_id = 'newer')" in query
            )
            data = {
                "data": [*build_complete_rows("older"), *build_complete_rows("newer")]
            }
        return httpx.Response(200, json=data)

    monkeypatch.setattr(api_module, "_post_btql", post)
    payloads = await collect_payloads(
        fetch({"project_id": "project-1", "since": "2026-01-01T00:00:00Z"})
    )

    assert _external_ids(payloads) == ["project-1:older", "project-1:newer"]
    assert len(requests) == 3


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
    assert nodes[1].parent_index == nodes[0].index
    assert len(requests) == 2


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
    assert fake_braintrust.batch_queries == []


async def test_fetch_bounds_concurrency_across_batches_and_preserves_order(
    fake_braintrust: FakeBraintrust, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A window fetch bounds in-flight batch queries, results stay oldest first."""
    monkeypatch.setattr(api_module, "_TRACES_PER_BATCH", 1)
    root_span_ids = ["root-a", "root-b", "root-c", "root-d"]
    fake_braintrust.list_pages = [(list(root_span_ids), None)]
    fake_braintrust.rows_builders = [build_complete_rows] * len(root_span_ids)
    # The listing query consumes the first delay. The remaining delays
    # scramble completion order relative to submission order, so the merged
    # result proves stream_bounded restores it rather than happening to
    # already match it.
    fake_braintrust.fetch_delays = [0.0, 0.03, 0.01, 0.02, 0.0]

    payloads = await collect_payloads(
        fetch(
            {
                "project_id": "project-1",
                "since": "2026-01-01T00:00:00+00:00",
                "concurrency": 2,
            }
        )
    )

    assert fake_braintrust.peak_in_flight == 2
    assert len(fake_braintrust.batch_queries) == len(root_span_ids)
    assert len(payloads) == len(root_span_ids)
    assert _external_ids(payloads) == [
        f"project-1:{root_span_id}" for root_span_id in root_span_ids
    ]


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
