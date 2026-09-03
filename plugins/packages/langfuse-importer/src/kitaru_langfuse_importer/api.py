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
from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from functools import partial
from typing import Any

from langfuse import get_client
from langfuse.api import (
    NotFoundError,
    ObservationsView,
    ObservationV2,
    TraceWithDetails,
    TraceWithFullDetails,
)
from langfuse.api.core import ApiError, RequestOptions

from kitaru.task.importer import FetchQuery, gather_bounded, retry_rate_limited

__all__ = [
    "fetch",
    "fetch_trace",
    "serialize_trace",
    "wait_for_trace",
]

_POLL_INTERVAL = 2.0
# The SDK's default request timeout is 5 seconds, too short for listing a
# time window or fetching a large trace with all observations.
_REQUEST_OPTIONS: RequestOptions = {"timeout_in_seconds": 60}
_DEFAULT_RETRY_AFTER = 60.0
# Field groups needed for the parser's observation record shape: "core"
# carries id, traceId, startTime, endTime, and parentObservationId; "basic"
# carries name, level, statusMessage, environment, and version.
_OBSERVATION_FIELDS = "core,basic,io,metadata,model,usage,prompt"
_OBSERVATION_LIMIT = 100
# Metadata keys the parser reads through nested lookups, exempted from the
# endpoint's default 200-character truncation of metadata values.
_EXPAND_METADATA = "attributes,resourceAttributes"


def _get_retry_after(exc: Exception) -> float | None:
    """Return the Langfuse rate-limit wait in seconds, or None for other errors.

    Args:
        exc: Exception raised by a Langfuse API call.

    Returns:
        Seconds to wait before retrying, or None when the error is not a
        rate limit.
    """
    if not isinstance(exc, ApiError) or exc.status_code != 429:
        return None
    body = exc.body if isinstance(exc.body, dict) else {}
    details = body.get("details")
    retry_after = (
        details.get("retryAfterSeconds") if isinstance(details, dict) else None
    )
    if retry_after is None:
        headers = exc.headers or {}
        retry_after = headers.get("retry-after")
    if retry_after is None:
        return _DEFAULT_RETRY_AFTER
    try:
        return float(retry_after)
    except (TypeError, ValueError):
        return _DEFAULT_RETRY_AFTER


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


def _serialize_observation(observation: ObservationV2) -> dict[str, Any]:
    """Convert one bulk-listed observation into the parser's record shape.

    Args:
        observation: Observation from the v2 observations listing.

    Returns:
        Observation payload dict.
    """
    payload = observation.model_dump(mode="json", by_alias=True)
    # The v2 listing exposes the raw model string as providedModelName. The
    # parser looks for a plain "model" key first, so carry it across under
    # that name too instead of falling through to modelId, which names a
    # matched catalog entry rather than the model string itself.
    payload["model"] = observation.provided_model_name
    return payload


async def _list_traces(
    since: datetime, until: datetime
) -> AsyncIterator[TraceWithDetails]:
    """List trace rows in a time window, paging through every result page.

    Args:
        since: Lower bound of trace start time.
        until: Upper bound of trace start time.

    Yields:
        Trace rows, oldest first.
    """
    api = get_client().async_api
    page = 1
    while True:
        traces = await retry_rate_limited(
            partial(
                api.trace.list,
                from_timestamp=since,
                to_timestamp=until,
                page=page,
                order_by="timestamp.asc",
                request_options=_REQUEST_OPTIONS,
            ),
            _get_retry_after,
        )
        for trace in traces.data:
            yield trace
        if page >= traces.meta.total_pages:
            return
        page += 1


async def _list_observations(
    trace_id: str, since: datetime | None, until: datetime | None
) -> list[dict[str, Any]]:
    """List every observation of one trace through the bulk v2 endpoint.

    Args:
        trace_id: Langfuse trace id.
        since: Lower bound of observation start time, or None for no bound.
        until: Upper bound of observation start time, or None for no bound.

    Returns:
        Observation payload dicts, in listing order.
    """
    api = get_client().async_api
    observations: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        response = await retry_rate_limited(
            partial(
                api.observations.get_many,
                trace_id=trace_id,
                from_start_time=since,
                to_start_time=until,
                fields=_OBSERVATION_FIELDS,
                expand_metadata=_EXPAND_METADATA,
                limit=_OBSERVATION_LIMIT,
                cursor=cursor,
                request_options=_REQUEST_OPTIONS,
            ),
            _get_retry_after,
        )
        observations.extend(
            _serialize_observation(observation) for observation in response.data
        )
        cursor = response.meta.cursor
        if cursor is None:
            return observations


async def _assemble_trace_payload(
    trace: TraceWithDetails | TraceWithFullDetails,
    since: datetime | None,
    until: datetime | None,
) -> dict[str, Any]:
    """Assemble one trace row and its observations into a parser payload record.

    Args:
        trace: Trace row from listing or a single trace fetch.
        since: Lower bound of observation start time, or None for no bound.
        until: Upper bound of observation start time, or None for no bound.

    Returns:
        Trace payload dict with a populated observations list.
    """
    payload = trace.model_dump(mode="json", by_alias=True)
    payload["observations"] = await _list_observations(trace.id, since, until)
    return payload


async def fetch(query: dict[str, Any]) -> AsyncIterator[bytes]:
    """Fetch one parser payload containing every trace matching a query.

    The parser groups traces into sessions by Langfuse session id, so every
    matching trace must reach it in a single payload for that grouping to
    work. A time-window query lists traces oldest first, then reads each
    one's observations through the high-volume bulk observations endpoint,
    concurrently up to the query's concurrency, and merges them back into
    that order.

    Args:
        query: Fetch query with `trace_ids`, `since`, and `until` keys.

    Raises:
        ValueError: The query is invalid.

    Yields:
        One trace list payload, or nothing when no trace matches.
    """
    parsed = FetchQuery.model_validate(query)

    since: datetime | None
    until: datetime | None
    trace_rows: Sequence[TraceWithDetails | TraceWithFullDetails]
    if parsed.trace_ids is not None:
        since = until = None
        trace_rows = await gather_bounded(
            (
                retry_rate_limited(partial(fetch_trace, trace_id), _get_retry_after)
                for trace_id in parsed.trace_ids
            ),
            parsed.concurrency,
        )
    else:
        since, until = parsed.get_window()
        trace_rows = [trace async for trace in _list_traces(since, until)]

    payloads = await gather_bounded(
        (_assemble_trace_payload(trace, since, until) for trace in trace_rows),
        parsed.concurrency,
    )
    if payloads:
        yield json.dumps(payloads).encode("utf-8")
