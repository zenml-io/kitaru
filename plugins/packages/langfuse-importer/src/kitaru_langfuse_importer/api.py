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
"""Langfuse API read layer."""

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

from langfuse import get_client
from langfuse.api import NotFoundError, ObservationsView, TraceWithFullDetails
from langfuse.api.core import RequestOptions

from kitaru.task.importer import FetchQuery, gather_bounded

__all__ = [
    "fetch",
    "fetch_trace",
    "serialize_trace",
    "serialize_traces",
    "wait_for_trace",
]

_POLL_INTERVAL = 2.0
# The SDK's default request timeout is 5 seconds, too short for listing a
# time window or fetching a large trace with all observations.
_REQUEST_OPTIONS: RequestOptions = {"timeout_in_seconds": 60}


def _roots_have_ended(observations: list[ObservationsView]) -> bool:
    """Return whether every root observation has an end time."""
    ids = {observation.id for observation in observations}
    roots = [
        observation
        for observation in observations
        if observation.parent_observation_id is None
        or observation.parent_observation_id not in ids
    ]
    return bool(roots) and all(
        observation.end_time is not None for observation in roots
    )


async def wait_for_trace(trace_id: str) -> TraceWithFullDetails:
    """Poll the Langfuse API until the trace is complete.

    Args:
        trace_id: Langfuse trace id.

    Returns:
        Fetched trace.
    """
    api = get_client().async_api
    # The trace is complete when it is fetchable, every root observation
    # has an end time, and the observation count is stable across two
    # consecutive polls.
    previous_count: int | None = None
    while True:
        try:
            trace = await api.trace.get(trace_id)
        except NotFoundError:
            previous_count = None
        else:
            if len(trace.observations) == previous_count and _roots_have_ended(
                trace.observations
            ):
                return trace
            previous_count = len(trace.observations)
        await asyncio.sleep(_POLL_INTERVAL)


async def fetch_trace(trace_id: str) -> TraceWithFullDetails:
    """Fetch a trace once from the Langfuse API.

    Args:
        trace_id: Langfuse trace id.

    Returns:
        Fetched trace.
    """
    return await get_client().async_api.trace.get(
        trace_id, request_options=_REQUEST_OPTIONS
    )


def serialize_trace(trace: TraceWithFullDetails) -> bytes:
    """Serialize a fetched trace into the payload the parser accepts.

    Args:
        trace: Fetched trace.

    Returns:
        Trace payload bytes.
    """
    # Serialize with the camelCase wire field names the importer parser
    # expects.
    return trace.model_dump_json(by_alias=True).encode("utf-8")


def serialize_traces(traces: list[TraceWithFullDetails]) -> bytes:
    """Serialize fetched traces into the multi-trace payload the parser accepts.

    Args:
        traces: Fetched traces, in the order they should be ingested.

    Returns:
        Trace list payload bytes.
    """
    return json.dumps(
        [trace.model_dump(mode="json", by_alias=True) for trace in traces]
    ).encode("utf-8")


async def _list_trace_ids(since: datetime, until: datetime) -> AsyncIterator[str]:
    """List trace ids in a time window, paging through every result page.

    Args:
        since: Lower bound of trace start time.
        until: Upper bound of trace start time.

    Yields:
        Trace ids, oldest first.
    """
    api = get_client().async_api
    page = 1
    while True:
        traces = await api.trace.list(
            from_timestamp=since,
            to_timestamp=until,
            page=page,
            order_by="timestamp.asc",
            request_options=_REQUEST_OPTIONS,
        )
        for trace in traces.data:
            yield trace.id
        if page >= traces.meta.total_pages:
            return
        page += 1


async def fetch(query: dict[str, Any]) -> AsyncIterator[bytes]:
    """Fetch one parser payload containing every trace matching a query.

    The parser groups traces into sessions by Langfuse session id, so every
    matching trace must reach it in a single payload for that grouping to
    work. A time-window query lists traces oldest first, then fetches them
    concurrently, up to the query's concurrency, and merges them back into
    that order.

    Args:
        query: Fetch query with `trace_ids`, `since`, and `until` keys.

    Raises:
        ValueError: The query is invalid.

    Yields:
        One trace list payload, or nothing when no trace matches.
    """
    parsed = FetchQuery.model_validate(query)

    if parsed.trace_ids is not None:
        trace_ids = parsed.trace_ids
    else:
        since, until = parsed.get_window()
        trace_ids = [trace_id async for trace_id in _list_trace_ids(since, until)]
    traces = await gather_bounded(
        (fetch_trace(trace_id) for trace_id in trace_ids), parsed.concurrency
    )
    if traces:
        yield serialize_traces(traces)
