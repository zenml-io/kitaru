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
"""Arize Phoenix API read layer."""

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import httpx
from phoenix.client import AsyncClient
from phoenix.client.utils.config import get_env_project_name

from kitaru.task.importer import FetchQuery

__all__ = ["fetch", "fetch_spans", "serialize_spans", "wait_for_spans"]

_POLL_INTERVAL = 2.0
_SPAN_LIMIT = 1000


def _has_root(spans: list[Any]) -> bool:
    """Return whether a root span is among the fetched spans."""
    ids = {span["context"]["span_id"] for span in spans}
    return any(
        span.get("parent_id") is None or span["parent_id"] not in ids for span in spans
    )


async def wait_for_spans(trace_id: str) -> list[Any]:
    """Poll the Phoenix span API until the trace is complete.

    Args:
        trace_id: Phoenix trace id.

    Returns:
        Fetched spans.
    """
    project = get_env_project_name()
    client = AsyncClient()
    # The trace is complete when it has spans, a root span is present,
    # and the span count is stable across two consecutive polls.
    previous_count: int | None = None
    while True:
        try:
            spans = await client.spans.get_spans(
                project_identifier=project,
                trace_ids=[trace_id],
                limit=_SPAN_LIMIT,
            )
        except httpx.HTTPStatusError as exc:
            # The project only exists once its first spans land, so a
            # missing project is a trace without spans.
            if exc.response.status_code != httpx.codes.NOT_FOUND:
                raise
            previous_count = None
        else:
            if len(spans) == previous_count and _has_root(spans):
                return spans
            previous_count = len(spans)
        await asyncio.sleep(_POLL_INTERVAL)


async def fetch_spans(
    trace_id: str,
    project: str | None = None,
    client: AsyncClient | None = None,
) -> list[Any]:
    """Fetch the spans of a trace once from the Phoenix span API.

    Args:
        trace_id: Phoenix trace id.
        project: Phoenix project identifier, the environment project when None.
        client: Phoenix client, a new one when None.

    Returns:
        Fetched spans.
    """
    return await (client or AsyncClient()).spans.get_spans(
        project_identifier=project or get_env_project_name(),
        trace_ids=[trace_id],
        limit=_SPAN_LIMIT,
    )


def serialize_spans(spans: list[Any]) -> bytes:
    """Serialize fetched spans into the payload the parser accepts.

    Args:
        spans: Fetched spans.

    Returns:
        Trace payload bytes.
    """
    return json.dumps(spans).encode("utf-8")


class PhoenixFetchQuery(FetchQuery):
    """Phoenix fetch query."""

    project: str | None = None


def _span_start_time(span: Any) -> datetime | None:
    """Parse one fetched span's start time, or None when absent or unparsable."""
    value = span.get("start_time")
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


async def _list_root_trace_ids(
    client: AsyncClient, project: str, since: datetime, until: datetime
) -> list[str]:
    """List root-span trace ids in a time window, oldest first.

    Args:
        client: Phoenix client.
        project: Phoenix project identifier.
        since: Lower bound of span start time.
        until: Upper bound of span start time.

    Returns:
        Trace ids ordered by ascending root span start time.
    """
    # get_spans has no ordering parameter, so sort the root spans here
    # before collecting trace ids.
    starts: dict[str, datetime] = {}
    window_start = since
    while True:
        spans = await client.spans.get_spans(
            project_identifier=project,
            start_time=window_start,
            end_time=until,
            limit=_SPAN_LIMIT,
        )
        if not spans:
            break
        for span in spans:
            if span.get("parent_id") is not None:
                continue
            trace_id = span["context"]["trace_id"]
            starts.setdefault(
                trace_id, _span_start_time(span) or datetime.min.replace(tzinfo=UTC)
            )
        if len(spans) < _SPAN_LIMIT:
            break
        next_start = max(
            (start for span in spans if (start := _span_start_time(span))),
            default=None,
        )
        if next_start is None or next_start <= window_start:
            break
        window_start = next_start
    return sorted(starts, key=lambda trace_id: (starts[trace_id], trace_id))


async def fetch(query: dict[str, Any]) -> AsyncIterator[bytes]:
    """Fetch one parser payload holding every span matching a query.

    Every fetched trace's spans land in a single payload, oldest trace
    first, so the parser groups them into Kitaru sessions itself instead
    of seeing one trace at a time.

    Args:
        query: Fetch query with `project`, `trace_ids`, `since`, and `until`
            keys.

    Raises:
        ValueError: The query is invalid.

    Yields:
        One payload with every fetched trace's spans, oldest first, or
        nothing when no trace matches.
    """
    parsed = PhoenixFetchQuery.model_validate(query)
    client = AsyncClient()

    if parsed.trace_ids is not None:
        trace_ids = parsed.trace_ids
    else:
        since, until = parsed.get_window()
        project = parsed.project or get_env_project_name()
        trace_ids = await _list_root_trace_ids(client, project, since, until)

    spans: list[Any] = []
    for trace_id in trace_ids:
        spans.extend(await fetch_spans(trace_id, parsed.project, client))
    if spans:
        yield serialize_spans(spans)
