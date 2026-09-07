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
"""Braintrust API read layer."""

import asyncio
import functools
import json
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
from pydantic import ConfigDict

from kitaru.api_models.v1.imports import ImportQuery
from kitaru.env import get_required_env
from kitaru.task.importer import gather_bounded, retry_rate_limited

__all__ = ["fetch", "fetch_spans", "serialize_spans", "wait_for_spans"]

_POLL_INTERVAL = 2.0
_DEFAULT_API_URL = "https://api.braintrust.dev"
_LIST_PAGE_SIZE = 1000


def _parse_retry_after(value: str | None) -> float:
    """Parse a Retry-After header value into a wait in seconds.

    Args:
        value: Retry-After header value, or None when absent.

    Returns:
        Seconds to wait, 60 when value is absent or not a delta-seconds
        integer or an HTTP date.
    """
    if value is None:
        return 60.0
    try:
        return float(int(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return 60.0
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max((when - datetime.now(UTC)).total_seconds(), 0.0)


def _get_retry_after(exc: Exception) -> float | None:
    """Return the Braintrust rate limit wait in seconds, or None otherwise.

    Args:
        exc: Exception raised while calling the Braintrust API.

    Returns:
        Seconds to wait before retrying, or None when exc is not a
        Braintrust rate limit error.
    """
    if not isinstance(exc, httpx.HTTPStatusError) or exc.response.status_code != 429:
        return None
    return _parse_retry_after(exc.response.headers.get("Retry-After"))


async def _post_btql(
    client: httpx.AsyncClient, api_url: str, body: dict[str, Any]
) -> httpx.Response:
    """POST one BTQL request and raise for a non-2xx response.

    Args:
        client: HTTP client.
        api_url: Braintrust API base URL.
        body: BTQL request body.

    Returns:
        The response.
    """
    response = await client.post(
        f"{api_url}/btql",
        headers={"Authorization": f"Bearer {get_required_env('BRAINTRUST_API_KEY')}"},
        json=body,
    )
    response.raise_for_status()
    return response


async def _query_rows(client: httpx.AsyncClient, query: str) -> list[dict[str, Any]]:
    """Fetch every page of a BTQL query ordered by its pagination key.

    Args:
        client: HTTP client.
        query: BTQL query with a cursor-compatible sort.

    Returns:
        All matching rows.
    """
    api_url = os.environ.get("BRAINTRUST_API_URL") or _DEFAULT_API_URL
    rows: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        # BTQL accepts cursors as query clauses, not top-level request fields.
        page_query = query if cursor is None else f"{query} | cursor: '{cursor}'"
        response = await retry_rate_limited(
            functools.partial(_post_btql, client, api_url, {"query": page_query}),
            _get_retry_after,
        )
        payload = response.json()
        rows.extend(payload["data"])
        cursor = payload.get("cursor")
        if not cursor:
            return rows


async def _query_spans(
    client: httpx.AsyncClient, project_id: str, root_span_id: str
) -> list[dict[str, Any]]:
    """Fetch all span rows of one trace via BTQL.

    Args:
        client: HTTP client.
        project_id: Braintrust project id.
        root_span_id: Braintrust root span id.

    Returns:
        Span rows.
    """
    query = (
        f"select: * | from: project_logs('{project_id}') spans"
        f" | filter: root_span_id = '{root_span_id}'"
        f" | sort: _pagination_key asc | limit: {_LIST_PAGE_SIZE}"
    )
    return await _query_rows(client, query)


async def _list_root_span_ids(
    client: httpx.AsyncClient, project_id: str, since: datetime, until: datetime
) -> AsyncIterator[str]:
    """List root span ids of a project in ascending creation-time order.

    Args:
        client: HTTP client.
        project_id: Braintrust project id.
        since: Lower bound of root span start time.
        until: Upper bound of root span start time.

    Yields:
        Root span ids, ordered after collecting every BTQL page.
    """
    since_ts, until_ts = since.timestamp(), until.timestamp()
    query = (
        f"select: root_span_id, created | from: project_logs('{project_id}') spans"
        f" | filter: is_root AND"
        f" ((created >= '{since.isoformat()}' AND created <= '{until.isoformat()}')"
        f" OR (metrics.start >= {since_ts} AND metrics.start <= {until_ts}))"
        f" | sort: _pagination_key asc"
        f" | limit: {_LIST_PAGE_SIZE}"
    )
    rows = await _query_rows(client, query)
    # Sorting by created on the server suppresses its pagination cursor.
    for row in sorted(rows, key=lambda row: row.get("created") or ""):
        if row.get("root_span_id"):
            yield str(row["root_span_id"])


def _roots_have_ended(rows: list[dict[str, Any]]) -> bool:
    """Return whether every root span row has an end metric."""
    roots = [row for row in rows if not row.get("span_parents")]
    return bool(roots) and all(
        isinstance(metrics := row.get("metrics"), dict)
        and metrics.get("end") is not None
        for row in roots
    )


async def wait_for_spans(project_id: str, root_span_id: str) -> list[dict[str, Any]]:
    """Poll the Braintrust API until the trace is complete.

    Args:
        project_id: Braintrust project id.
        root_span_id: Braintrust root span id.

    Returns:
        Fetched span rows.
    """
    # The trace is complete when it has rows, every root span row has an
    # end metric, and the row count is stable across two consecutive
    # polls.
    previous_count: int | None = None
    async with httpx.AsyncClient() as client:
        while True:
            rows = await _query_spans(client, project_id, root_span_id)
            if len(rows) == previous_count and _roots_have_ended(rows):
                return rows
            previous_count = len(rows)
            await asyncio.sleep(_POLL_INTERVAL)


async def fetch_spans(
    project_id: str,
    root_span_id: str,
    client: httpx.AsyncClient | None = None,
) -> list[dict[str, Any]]:
    """Fetch the span rows of a trace once from the Braintrust API.

    Args:
        project_id: Braintrust project id.
        root_span_id: Braintrust root span id.
        client: HTTP client, a new one when None.

    Returns:
        Fetched span rows.
    """
    if client is not None:
        return await _query_spans(client, project_id, root_span_id)
    async with httpx.AsyncClient() as new_client:
        return await _query_spans(new_client, project_id, root_span_id)


def serialize_spans(rows: list[dict[str, Any]]) -> bytes:
    """Serialize fetched span rows into the payload the parser accepts.

    Args:
        rows: Fetched span rows.

    Returns:
        Trace payload bytes.
    """
    # Serialize with the events envelope the importer parser expects.
    return json.dumps({"events": rows}).encode("utf-8")


class BraintrustImportQuery(ImportQuery):
    """Braintrust import query."""

    model_config = ConfigDict(extra="forbid")

    project_id: str


async def fetch(query: dict[str, Any]) -> AsyncIterator[bytes]:
    """Fetch one parser payload holding every span row matching a query.

    Every fetched trace's rows land in a single payload, oldest trace
    first, so the parser groups them into Kitaru sessions itself instead
    of seeing one trace at a time. Traces are fetched concurrently, up to
    the query's concurrency, and merged back into that order. A request
    that hits the Braintrust rate limit waits out the reported delay and
    retries instead of failing the fetch.

    Args:
        query: Fetch query.

    Raises:
        ValueError: The query is invalid.

    Yields:
        The trace payload bytes, or nothing when no trace matches.
    """
    parsed = BraintrustImportQuery.model_validate(query)
    async with httpx.AsyncClient() as client:
        if parsed.trace_ids is not None:
            trace_ids = parsed.trace_ids
        else:
            since, until = parsed.get_window()
            trace_ids = [
                trace_id
                async for trace_id in _list_root_span_ids(
                    client, parsed.project_id, since, until
                )
            ]
        row_batches = await gather_bounded(
            (
                fetch_spans(parsed.project_id, trace_id, client)
                for trace_id in trace_ids
            ),
            parsed.concurrency,
        )
    rows = [row for batch in row_batches for row in batch]
    if rows:
        yield serialize_spans(rows)
