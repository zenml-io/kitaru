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
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import aclosing
from datetime import datetime, timedelta
from functools import partial
from typing import Any, NamedTuple

from langfuse import get_client
from langfuse.api import (
    NotFoundError,
    ObservationsView,
    ObservationV2,
    TraceWithDetails,
    TraceWithFullDetails,
)
from langfuse.api.core import ApiError, RequestOptions
from pydantic import ConfigDict

from kitaru.api_models.v1.imports import ImportQuery
from kitaru.task.importer import retry_rate_limited, stream_bounded

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
_OBSERVATION_LIMIT = 500
# Listed traces whose start times span one observations request.
_TRACES_PER_BATCH = 25
# Added past the latest trace end so the last batch's exclusive upper bound
# still covers observations starting at that end.
_BATCH_END_MARGIN = timedelta(seconds=1)
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


async def _list_observations(**filters: Any) -> list[dict[str, Any]]:
    """List every observation matching the filters through the bulk v2 endpoint.

    Args:
        filters: Query filters passed to the endpoint, such as a trace id or
            a start time range.

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
                fields=_OBSERVATION_FIELDS,
                expand_metadata=_EXPAND_METADATA,
                limit=_OBSERVATION_LIMIT,
                cursor=cursor,
                request_options=_REQUEST_OPTIONS,
                **filters,
            ),
            _get_retry_after,
        )
        observations.extend(
            _serialize_observation(observation) for observation in response.data
        )
        cursor = response.meta.cursor
        if cursor is None:
            return observations


def _get_session_group_key(trace: TraceWithDetails) -> str:
    """Return the session grouping key the parser's default join would use.

    Args:
        trace: Listed trace row.

    Returns:
        The trace's session id when it is a non-empty string, its own id
        otherwise.
    """
    if isinstance(trace.session_id, str) and trace.session_id:
        return trace.session_id
    return trace.id


def _get_trace_end(trace: TraceWithDetails) -> datetime:
    """Return the trace end time from its timestamp and latency.

    Args:
        trace: Listed trace row.

    Returns:
        Trace end time, the timestamp itself when latency is unknown.
    """
    return trace.timestamp + timedelta(seconds=trace.latency or 0.0)


class _Batch(NamedTuple):
    """Observation start time range, inclusive start and exclusive end."""

    start: datetime
    end: datetime


def _split_batches(traces: list[TraceWithDetails], since: datetime) -> list[_Batch]:
    """Cut the listed traces into contiguous observation start time ranges.

    Args:
        traces: Listed trace rows, oldest first.
        since: Lower bound of the listing window.

    Returns:
        Batches in listing order. Each one starts where the previous ended,
        the first at since, and the last reaches past every trace end.
    """
    starts = [
        traces[index].timestamp for index in range(0, len(traces), _TRACES_PER_BATCH)
    ]
    final_end = max(_get_trace_end(trace) for trace in traces) + _BATCH_END_MARGIN
    batches: list[_Batch] = []
    start = since
    for end in [*starts[1:], final_end]:
        end = max(start, end)
        batches.append(_Batch(start, end))
        start = end
    return batches


async def _fetch_batch(batch: _Batch) -> tuple[_Batch, list[dict[str, Any]]]:
    """Fetch every observation starting within one batch's time range.

    Args:
        batch: Observation start time range.

    Returns:
        The batch and its observation payload dicts, none for an empty range.
    """
    if batch.end <= batch.start:
        return batch, []
    observations = await _list_observations(
        from_start_time=batch.start, to_start_time=batch.end
    )
    return batch, observations


class _WindowAssembler:
    """Collect batch observations and release sessions whose traces have all ended."""

    def __init__(self, traces: list[TraceWithDetails]) -> None:
        self._traces = {trace.id: trace for trace in traces}
        self._observations: dict[str, list[dict[str, Any]]] = {
            trace.id: [] for trace in traces
        }
        self._groups: dict[str, list[str]] = {}
        for trace in traces:
            self._groups.setdefault(_get_session_group_key(trace), []).append(trace.id)
        self._pending = list(self._groups)

    def add(self, observations: list[dict[str, Any]]) -> None:
        """Bucket observations by trace, dropping those of unlisted traces.

        Args:
            observations: Observation payload dicts from one batch.
        """
        # A batch range also catches observations of traces that started
        # before the window, which the listing did not select.
        for observation in observations:
            bucket = self._observations.get(observation.get("traceId"))
            if bucket is not None:
                bucket.append(observation)

    def release(self, fetched_until: datetime) -> list[dict[str, Any]]:
        """Remove and return the traces of every session complete up to a time.

        Args:
            fetched_until: Exclusive upper bound of observation start times
                fetched so far.

        Returns:
            Trace payload records of the released sessions, in listing order.
        """
        released: list[dict[str, Any]] = []
        pending: list[str] = []
        for key in self._pending:
            trace_ids = self._groups[key]
            # Every observation starts no later than its trace ends, so a
            # trace is complete once the fetched range passes its end.
            if all(
                _get_trace_end(self._traces[trace_id]) < fetched_until
                for trace_id in trace_ids
            ):
                for trace_id in trace_ids:
                    payload = self._traces[trace_id].model_dump(
                        mode="json", by_alias=True
                    )
                    payload["observations"] = self._observations.pop(trace_id)
                    released.append(payload)
            else:
                pending.append(key)
        self._pending = pending
        return released


class LangfuseImportQuery(ImportQuery):
    """Langfuse import query."""

    model_config = ConfigDict(extra="forbid")


async def fetch(query: dict[str, Any]) -> AsyncGenerator[bytes, None]:
    """Fetch parser payloads matching a query, one per batch of complete sessions.

    A time-window query lists traces oldest first, then reads observations
    in contiguous start time ranges, each spanning the starts of a run of
    listed traces, so one paginated request covers many traces. A trace is
    complete once the fetched ranges pass its end, known from the listed
    timestamp and latency, and a session is released as soon as every trace
    sharing its Langfuse session id is complete, so the parser sees all of
    a session in one payload. Observations of traces the listing did not
    select are dropped. A `trace_ids` query fetches each trace with its
    observations inline, yields a trace without a session id as soon as it
    is in, and yields traces sharing a session id together once every
    requested trace is in. Requests run concurrently up to the query's
    concurrency.

    Args:
        query: Fetch query with `trace_ids`, `since`, and `until` keys.

    Raises:
        ValueError: The query is invalid.

    Yields:
        One trace list payload per batch that completes at least one
        session, in listing order, or one payload per requested standalone
        trace and per session, or nothing when no trace matches.
    """
    parsed = LangfuseImportQuery.model_validate(query)

    if parsed.trace_ids is not None:
        trace_awaitables = (
            retry_rate_limited(partial(fetch_trace, trace_id), _get_retry_after)
            for trace_id in parsed.trace_ids
        )
        # A trace's session is only known once its row is back, so traces
        # with a session id wait until every requested trace is in.
        held: dict[str, list[dict[str, Any]]] = {}
        async with aclosing(
            stream_bounded(trace_awaitables, parsed.concurrency)
        ) as traces:
            async for trace in traces:
                payload = trace.model_dump(mode="json", by_alias=True)
                if isinstance(trace.session_id, str) and trace.session_id:
                    held.setdefault(trace.session_id, []).append(payload)
                else:
                    yield json.dumps([payload]).encode("utf-8")
        for payloads in held.values():
            yield json.dumps(payloads).encode("utf-8")
        return

    since, until = parsed.get_window()
    traces = [trace async for trace in _list_traces(since, until)]
    if not traces:
        return

    assembler = _WindowAssembler(traces)
    batch_awaitables = (_fetch_batch(batch) for batch in _split_batches(traces, since))
    async with aclosing(
        stream_bounded(batch_awaitables, parsed.concurrency)
    ) as results:
        async for batch, observations in results:
            assembler.add(observations)
            records = assembler.release(batch.end)
            if records:
                yield json.dumps(records).encode("utf-8")
