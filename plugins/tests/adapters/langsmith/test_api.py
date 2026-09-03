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
from datetime import UTC, datetime

import pytest

from kitaru.api_models.v1.session import SessionStatus
from kitaru.task.importer import ImportedSession
from kitaru_langsmith_importer.adapter import _PARSER_PARAMS
from kitaru_langsmith_importer.api import fetch
from kitaru_langsmith_importer.importer import parse

from ..fetch_helpers import collect_payloads
from .fixtures import PROJECT_ID, FakeLangSmith, build_complete_runs, build_run


async def test_fetch_by_trace_ids_fetches_exactly_those_in_order(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """Fetch the given trace ids in order and yield a parseable payload each."""
    trace_id_1 = str(uuid.uuid4())
    trace_id_2 = str(uuid.uuid4())
    fake_langsmith_api.runs_builders = [build_complete_runs, build_complete_runs]

    payloads = await collect_payloads(fetch({"trace_ids": [trace_id_1, trace_id_2]}))

    assert fake_langsmith_api.requested == [trace_id_1, trace_id_2]
    assert fake_langsmith_api.root_listing_calls == []
    assert len(payloads) == 2
    sessions = list(parse(payloads[0], _PARSER_PARAMS))
    assert len(sessions) == 1
    session = sessions[0]
    assert isinstance(session, ImportedSession)
    assert session.external_id == f"{PROJECT_ID}:{trace_id_1}"
    assert session.status == SessionStatus.COMPLETED


async def test_fetch_time_window_lists_root_runs_and_fetches_each_distinct_trace_once(
    fake_langsmith_api: FakeLangSmith,
) -> None:
    """List root runs in the window and fetch each distinct trace once."""
    trace_id_1 = str(uuid.uuid4())
    trace_id_2 = str(uuid.uuid4())
    root_run_1 = build_run(trace_id_1, trace_id_1)
    root_run_2 = build_run(trace_id_2, trace_id_2)
    # The same trace can surface twice in a root run listing, the fetch
    # must still request it once.
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

    assert len(payloads) == 2
    assert fake_langsmith_api.requested == [trace_id_1, trace_id_2]
    assert len(fake_langsmith_api.root_listing_calls) == 1
    call = fake_langsmith_api.root_listing_calls[0]
    assert call["project_name"] == "my-project"
    assert call["is_root"] is True
    assert call["start_time"] == datetime(2026, 7, 1, tzinfo=UTC)
    assert call["filter"] == 'lt(end_time, "2026-08-01T00:00:00+00:00")'


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
