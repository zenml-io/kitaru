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
from collections.abc import AsyncGenerator
from contextlib import aclosing
from datetime import datetime, timedelta
from typing import Any, NamedTuple

from langsmith import Client
from langsmith.schemas import Run
from langsmith.utils import LangSmithRateLimitError, get_tracer_project
from pydantic import ConfigDict

from kitaru.api_models.v1.imports import ImportQuery
from kitaru.task.importer import retry_rate_limited, stream_bounded

from .importer import get_default_join_value

__all__ = ["fetch", "fetch_runs", "serialize_runs", "wait_for_runs"]

_POLL_INTERVAL = 2.0
_RATE_LIMIT_RETRY_AFTER = 60.0
# Listed root runs whose start times span one range request.
_TRACES_PER_BATCH = 25
# Added past the latest root run end so the last batch's exclusive upper
# bound still covers runs starting at that end.
_BATCH_END_MARGIN = timedelta(seconds=1)


def _get_retry_after(exc: Exception) -> float | None:
    """Return the LangSmith rate limit wait in seconds, or None otherwise.

    Args:
        exc: Exception raised while calling the LangSmith API.

    Returns:
        A fixed wait in seconds when exc is a LangSmith rate limit error,
        since the SDK does not expose the response or its headers on that
        exception. None otherwise.
    """
    if not isinstance(exc, LangSmithRateLimitError):
        return None
    return _RATE_LIMIT_RETRY_AFTER


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
    return await retry_rate_limited(
        lambda: asyncio.to_thread(_list_runs, client, trace_id), _get_retry_after
    )


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


class LangSmithImportQuery(ImportQuery):
    """LangSmith import query."""

    model_config = ConfigDict(extra="forbid")

    project_name: str | None = None


def _get_root_end(run: Run) -> datetime:
    """Return a root run's end time, its start time when it has none."""
    return run.end_time or run.start_time


def _list_root_runs(
    client: Client,
    project_name: str | None,
    since: datetime,
    until: datetime,
) -> tuple[str | None, list[Run]]:
    """List root runs started in a time window, oldest first.

    Args:
        client: LangSmith client.
        project_name: LangSmith project name, the SDK's tracer project when None.
        since: Lower bound of trace start time.
        until: Upper bound of trace end time.

    Returns:
        The resolved project name and the distinct root runs, ordered by
        ascending start time.
    """
    resolved_project_name = project_name or get_tracer_project()
    # Client.list_runs has no ordering parameter, so sort the root runs
    # here before deduplicating them.
    runs = sorted(
        client.list_runs(
            project_name=resolved_project_name,
            is_root=True,
            start_time=since,
            filter=f'lt(end_time, "{until.isoformat()}")',
        ),
        key=lambda run: (run.start_time, str(run.trace_id)),
    )
    roots: list[Run] = []
    seen: set[str] = set()
    for run in runs:
        trace_id = str(run.trace_id)
        if trace_id in seen:
            continue
        seen.add(trace_id)
        roots.append(run)
    return resolved_project_name, roots


class _Batch(NamedTuple):
    """Run start time range, inclusive start and exclusive end."""

    start: datetime
    end: datetime


def _split_batches(roots: list[Run], since: datetime) -> list[_Batch]:
    """Cut the listed root runs into contiguous start time ranges.

    Args:
        roots: Listed root runs, oldest first.
        since: Lower bound of the listing window.

    Returns:
        Batches in listing order. Each one starts where the previous ended,
        the first at since, and the last reaches past every root end.
    """
    starts = [
        roots[index].start_time for index in range(0, len(roots), _TRACES_PER_BATCH)
    ]
    final_end = max(_get_root_end(run) for run in roots) + _BATCH_END_MARGIN
    batches: list[_Batch] = []
    start = since
    for end in [*starts[1:], final_end]:
        end = max(start, end)
        batches.append(_Batch(start, end))
        start = end
    return batches


def _list_batch_runs(
    client: Client, project_name: str | None, batch: _Batch
) -> list[Run]:
    """Fetch every run starting within one batch's time range via the LangSmith API.

    Args:
        client: LangSmith client.
        project_name: Resolved LangSmith project name.
        batch: Run start time range.

    Returns:
        Fetched runs.
    """
    return list(
        client.list_runs(
            project_name=project_name,
            start_time=batch.start,
            filter=f'lt(start_time, "{batch.end.isoformat()}")',
        )
    )


async def _fetch_batch(
    client: Client, project_name: str | None, batch: _Batch
) -> tuple[_Batch, list[Run]]:
    """Fetch every run starting within one batch's time range.

    Args:
        client: LangSmith client.
        project_name: Resolved LangSmith project name.
        batch: Run start time range.

    Returns:
        The batch and its runs, none for an empty range.
    """
    if batch.end <= batch.start:
        return batch, []
    runs = await retry_rate_limited(
        lambda: asyncio.to_thread(_list_batch_runs, client, project_name, batch),
        _get_retry_after,
    )
    return batch, runs


class _WindowAssembler:
    """Collect batch runs and release traces whose groups have all ended."""

    def __init__(self, roots: list[Run]) -> None:
        self._ends = {str(run.trace_id): _get_root_end(run) for run in roots}
        self._runs: dict[str, list[Run]] = {str(run.trace_id): [] for run in roots}
        self._groups: dict[str, list[str]] = {}
        for run in roots:
            trace_id = str(run.trace_id)
            key = get_default_join_value(run.model_dump(mode="json")) or trace_id
            self._groups.setdefault(key, []).append(trace_id)
        self._pending = list(self._groups)

    def add(self, runs: list[Run]) -> None:
        """Bucket runs by trace, dropping those of unlisted traces.

        Args:
            runs: Runs from one batch.
        """
        # A batch range also catches runs of traces that started before the
        # window, which the root listing did not select.
        for run in runs:
            bucket = self._runs.get(str(run.trace_id))
            if bucket is not None:
                bucket.append(run)

    def release(self, fetched_until: datetime) -> list[Run]:
        """Remove and return the runs of every group complete up to a time.

        Args:
            fetched_until: Exclusive upper bound of run start times fetched
                so far.

        Returns:
            Released runs, in listing order per trace.
        """
        released: list[Run] = []
        pending: list[str] = []
        for key in self._pending:
            trace_ids = self._groups[key]
            # Every run starts no later than its trace ends, so a trace is
            # complete once the fetched range passes its end.
            if all(self._ends[trace_id] < fetched_until for trace_id in trace_ids):
                for trace_id in trace_ids:
                    released.extend(self._runs.pop(trace_id))
            else:
                pending.append(key)
        self._pending = pending
        return released


async def _fetch_trace(client: Client, trace_id: str) -> tuple[str, list[Run]]:
    """Fetch one trace's runs, paired with its trace id.

    Args:
        client: LangSmith client.
        trace_id: LangSmith trace id.

    Returns:
        The trace id and its fetched runs.
    """
    return trace_id, await fetch_runs(client, trace_id)


async def fetch(query: dict[str, Any]) -> AsyncGenerator[bytes, None]:
    """Fetch parser payloads matching a query.

    A time-window query lists root runs oldest first, then reads runs in
    contiguous start time ranges, each spanning the starts of a run of
    listed roots, so one paginated request covers many traces. A trace is
    complete once the fetched ranges pass its end, known from its root
    run's start and end time, and a thread or session group is released as
    soon as every trace sharing its key is complete, so the parser sees all
    of a group in one payload. Runs of traces the root listing did not
    select are dropped. A trace_ids query fetches each trace's runs, yields
    a trace without a resolved thread or session key as soon as it is in,
    and yields traces sharing a key together once every requested trace is
    in. Requests run concurrently up to the query's concurrency. A request
    that hits the LangSmith rate limit waits out a fixed delay and retries
    instead of failing the fetch.

    Args:
        query: Fetch query with trace_ids, since, until, and project_name.

    Raises:
        ValueError: The query is invalid.

    Yields:
        One payload per batch that completes at least one group, in
        listing order, in the time window case, or one payload per
        standalone requested trace and per shared-key group in the
        trace_ids case. Nothing when no trace matches the query.
    """
    parsed = LangSmithImportQuery.model_validate(query)
    client = Client()

    if parsed.trace_ids is not None:
        trace_awaitables = (
            _fetch_trace(client, trace_id) for trace_id in parsed.trace_ids
        )
        # A trace's group key is only known once its root run is back, so
        # traces sharing a key wait until every requested trace is in.
        held: dict[str, list[Run]] = {}
        async with aclosing(
            stream_bounded(trace_awaitables, parsed.concurrency)
        ) as traces:
            async for trace_id, runs in traces:
                root = next((run for run in runs if str(run.id) == trace_id), None)
                key = (
                    get_default_join_value(root.model_dump(mode="json"))
                    if root
                    else None
                )
                if key:
                    held.setdefault(key, []).extend(runs)
                else:
                    yield serialize_runs(runs)
        for group_runs in held.values():
            yield serialize_runs(group_runs)
        return

    since, until = parsed.get_window()
    resolved_project_name, roots = await retry_rate_limited(
        lambda: asyncio.to_thread(
            _list_root_runs, client, parsed.project_name, since, until
        ),
        _get_retry_after,
    )
    if not roots:
        return

    assembler = _WindowAssembler(roots)
    batch_awaitables = (
        _fetch_batch(client, resolved_project_name, batch)
        for batch in _split_batches(roots, since)
    )
    # Close the stream explicitly so an early stop cancels in-flight fetches.
    async with aclosing(
        stream_bounded(batch_awaitables, parsed.concurrency)
    ) as results:
        async for batch, runs in results:
            assembler.add(runs)
            released = assembler.release(batch.end)
            if released:
                yield serialize_runs(released)
