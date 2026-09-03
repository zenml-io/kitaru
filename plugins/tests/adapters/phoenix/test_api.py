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
"""Focused contract tests for the Phoenix API fetch entrypoint."""

import json
from datetime import UTC, datetime

import pytest

import kitaru_phoenix_importer.api as api_module
from kitaru.api_models.v1.session import SessionStatus
from kitaru.task.importer import ImportedSession
from kitaru_phoenix_importer.api import fetch
from kitaru_phoenix_importer.importer import parse

from ..fetch_helpers import collect_payloads
from .fixtures import PROJECT, FakePhoenix, build_complete_spans, build_span


async def test_trace_ids_fetches_exactly_those_traces_in_order(
    fake_phoenix: FakePhoenix,
) -> None:
    """Fetch the requested traces, skip the time window, and preserve order."""
    fake_phoenix.span_builders = [build_complete_spans, build_complete_spans]

    payloads = await collect_payloads(fetch({"trace_ids": ["trace-b", "trace-a"]}))

    assert fake_phoenix.requested == ["trace-b", "trace-a"]
    assert not fake_phoenix.list_windows
    sessions = [
        session
        for payload in payloads
        for session in parse(payload, {})
        if isinstance(session, ImportedSession)
    ]
    assert [session.external_id for session in sessions] == ["trace-b", "trace-a"]
    assert sessions[0].status == SessionStatus.COMPLETED
    assert [node.name for node in sessions[0].nodes] == ["kitaru-run"]


async def test_time_window_lists_spans_across_two_pages(
    fake_phoenix: FakePhoenix, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Page through the listing and fetch each distinct root trace once."""
    monkeypatch.setattr(api_module, "_SPAN_LIMIT", 2)
    fake_phoenix.list_pages = [
        [
            build_span(
                "root-1",
                "trace-1",
                name="kitaru-run",
                start_time="2026-08-27T10:00:00+00:00",
            ),
            build_span(
                "child-1",
                "trace-1",
                parent_id="root-1",
                start_time="2026-08-27T10:00:00+00:00",
            ),
        ],
        [
            build_span(
                "root-2",
                "trace-2",
                name="kitaru-run",
                start_time="2026-08-27T10:00:01+00:00",
            ),
        ],
    ]
    fake_phoenix.span_builders = [build_complete_spans, build_complete_spans]

    payloads = await collect_payloads(
        fetch(
            {"since": "2026-08-27T09:00:00+00:00", "until": "2026-08-27T11:00:00+00:00"}
        )
    )

    assert len(payloads) == 2
    assert fake_phoenix.requested == ["trace-1", "trace-2"]
    assert fake_phoenix.project_identifiers[:2] == [PROJECT, PROJECT]
    since = datetime(2026, 8, 27, 9, 0, 0, tzinfo=UTC)
    assert fake_phoenix.list_windows[0] == (
        since,
        datetime(2026, 8, 27, 11, 0, 0, tzinfo=UTC),
    )
    assert fake_phoenix.list_windows[1][0] == datetime(
        2026, 8, 27, 10, 0, 0, tzinfo=UTC
    )


async def test_time_window_project_defaults_to_env_project(
    fake_phoenix: FakePhoenix,
) -> None:
    """Fall back to the environment project when the query omits one."""
    fake_phoenix.list_pages = [[]]

    payloads = await collect_payloads(fetch({"since": "2026-08-27T09:00:00+00:00"}))

    assert payloads == []
    assert fake_phoenix.project_identifiers == [PROJECT]


async def test_until_defaults_to_now(fake_phoenix: FakePhoenix) -> None:
    """Default the upper bound to the current time when omitted."""
    fake_phoenix.list_pages = [[]]

    before = datetime.now(UTC)
    await collect_payloads(fetch({"since": "2026-08-27T09:00:00+00:00"}))
    after = datetime.now(UTC)

    [(_, until)] = fake_phoenix.list_windows
    assert until is not None
    assert before <= until <= after


async def test_empty_listing_yields_nothing(fake_phoenix: FakePhoenix) -> None:
    """Yield nothing when the time window has no spans."""
    fake_phoenix.list_pages = [[]]

    payloads = await collect_payloads(fetch({"since": "2026-08-27T09:00:00+00:00"}))

    assert payloads == []
    assert fake_phoenix.requested == []


async def test_validation_errors(fake_phoenix: FakePhoenix) -> None:
    """Raise ValueError before the first yield for an invalid query."""
    query = {"since": "2026-08-27T09:00:00+00:00", "bogus": "x"}
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        await collect_payloads(fetch(query))

    assert fake_phoenix.requested == []
    assert fake_phoenix.list_windows == []


async def test_fetch_payload_round_trips_through_the_real_parser(
    fake_phoenix: FakePhoenix,
) -> None:
    """Run a fetched payload through the real parser end to end."""
    fake_phoenix.span_builders = [build_complete_spans]

    [payload] = await collect_payloads(fetch({"trace_ids": ["trace-1"]}))

    spans = json.loads(payload)
    assert [span["name"] for span in spans] == ["kitaru-run", "llm-call"]
    sessions = list(parse(payload, {}))
    assert len(sessions) == 1
    [session] = sessions
    assert isinstance(session, ImportedSession)
    assert session.external_id == "trace-1"
