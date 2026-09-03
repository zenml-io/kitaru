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
"""LangSmith API read layer."""

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

from langsmith import Client
from langsmith.schemas import Run
from langsmith.utils import get_tracer_project

from kitaru.task.importer import FetchQuery

__all__ = ["fetch", "fetch_runs", "serialize_runs", "wait_for_runs"]

_POLL_INTERVAL = 2.0


def _list_runs(client: Client, trace_id: str) -> list[Run]:
    """Fetch all runs of one trace via the LangSmith API.

    Args:
        client: LangSmith client.
        trace_id: LangSmith trace id.

    Returns:
        Trace runs.
    """
    return list(client.list_runs(trace_id=trace_id))


def _trace_has_ended(runs: list[Run], trace_id: str) -> bool:
    """Return whether the root run is present and every run has an end time."""
    has_root = any(str(run.id) == trace_id for run in runs)
    return has_root and all(run.end_time is not None for run in runs)


async def wait_for_runs(client: Client, trace_id: str) -> list[Run]:
    """Poll the LangSmith API until the trace is complete.

    Args:
        client: LangSmith client.
        trace_id: LangSmith trace id.

    Returns:
        Fetched trace runs.
    """
    # The trace is complete when the root run is present, every run has
    # an end time, and the run count is stable across two consecutive
    # polls.
    previous_count: int | None = None
    while True:
        runs = await asyncio.to_thread(_list_runs, client, trace_id)
        if len(runs) == previous_count and _trace_has_ended(runs, trace_id):
            return runs
        previous_count = len(runs)
        await asyncio.sleep(_POLL_INTERVAL)


async def fetch_runs(client: Client, trace_id: str) -> list[Run]:
    """Fetch the runs of a trace once from the LangSmith API.

    Args:
        client: LangSmith client.
        trace_id: LangSmith trace id.

    Returns:
        Fetched trace runs.
    """
    return await asyncio.to_thread(_list_runs, client, trace_id)


def serialize_runs(runs: list[Run]) -> bytes:
    """Serialize fetched runs into the payload the parser accepts.

    Args:
        runs: Fetched trace runs.

    Returns:
        Trace payload bytes.
    """
    # Serialize with the snake_case run field names the importer parser
    # expects.
    return "\n".join(json.dumps(run.model_dump(mode="json")) for run in runs).encode(
        "utf-8"
    )


class LangSmithFetchQuery(FetchQuery):
    """LangSmith fetch query."""

    project_name: str | None = None


def _list_root_trace_ids(
    client: Client,
    project_name: str | None,
    since: datetime,
    until: datetime,
) -> list[str]:
    """List distinct trace ids of root runs started in a time window.

    Args:
        client: LangSmith client.
        project_name: LangSmith project name, the SDK's tracer project when None.
        since: Lower bound of trace start time.
        until: Upper bound of trace end time.

    Returns:
        Distinct trace ids in listing order.
    """
    runs = client.list_runs(
        project_name=project_name or get_tracer_project(),
        is_root=True,
        start_time=since,
        filter=f'lt(end_time, "{until.isoformat()}")',
    )
    trace_ids: list[str] = []
    seen: set[str] = set()
    for run in runs:
        trace_id = str(run.trace_id)
        if trace_id not in seen:
            seen.add(trace_id)
            trace_ids.append(trace_id)
    return trace_ids


async def fetch(query: dict[str, Any]) -> AsyncIterator[bytes]:
    """Fetch LangSmith traces as parser payloads.

    Args:
        query: Fetch query with trace_ids, since, until, and project_name.

    Raises:
        ValueError: The query is invalid.

    Yields:
        One parser payload per fetched trace.
    """
    parsed = LangSmithFetchQuery.model_validate(query)
    client = Client()

    if parsed.trace_ids is not None:
        trace_ids = parsed.trace_ids
    else:
        since, until = parsed.get_window()
        trace_ids = await asyncio.to_thread(
            _list_root_trace_ids,
            client,
            parsed.project_name,
            since,
            until,
        )

    for trace_id in trace_ids:
        runs = await fetch_runs(client, trace_id)
        yield serialize_runs(runs)
