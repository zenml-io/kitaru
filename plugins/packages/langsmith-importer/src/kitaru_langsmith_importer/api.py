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

from langsmith import Client
from langsmith.schemas import Run

__all__ = ["fetch_runs", "serialize_runs", "wait_for_runs"]

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
