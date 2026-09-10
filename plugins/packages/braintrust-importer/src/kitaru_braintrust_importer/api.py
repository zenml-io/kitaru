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
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import aclosing
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
from pydantic import ConfigDict

from kitaru.api_models.v1.imports import ImportQuery
from kitaru.env import get_required_env
from kitaru.task.importer import retry_rate_limited, stream_bounded

from .importer import get_session_id

__all__ = ["fetch", "fetch_spans", "serialize_spans", "wait_for_spans"]

_POLL_INTERVAL = 2.0
_DEFAULT_API_URL = "https://api.braintrust.dev"
_LIST_PAGE_SIZE = 1000
# Root span ids packed into one spans query, in time-window and trace_ids mode.
_TRACES_PER_BATCH = 25


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


async def _query_spans_batch(
    client: httpx.AsyncClient, project_id: str, root_span_ids: list[str]
) -> list[dict[str, Any]]:
    """Fetch all span rows of many traces via one BTQL query.

    Args:
        client: HTTP client.
        project_id: Braintrust project id.
        root_span_ids: Root span ids to match.

    Returns:
        Span rows across every matched trace.
    """
    terms = " OR ".join(
        f"(root_span_id = '{root_span_id}')" for root_span_id in root_span_ids
    )
    query = (
        f"select: * | from: project_logs('{project_id}') spans"
        f" | filter: {terms}"
        f" | sort: _pagination_key asc | limit: {_LIST_PAGE_SIZE}"
    )
    return await _query_rows(client, query)


async def _list_root_spans(
    client: httpx.AsyncClient, project_id: str, since: datetime, until: datetime
) -> AsyncIterator[dict[str, Any]]:
    """List root span rows of a project in ascending creation-time order.

    Args:
        client: HTTP client.
        project_id: Braintrust project id.
        since: Lower bound of root span start time.
        until: Upper bound of root span start time.

    Yields:
        Root span rows with root_span_id and metadata, ordered after
        collecting every BTQL page.
    """
    since_ts, until_ts = since.timestamp(), until.timestamp()
    query = (
        f"select: root_span_id, created, metadata"
        f" | from: project_logs('{project_id}') spans"
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
            yield row


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


def _group_roots_by_session(
    roots: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Group listed root span rows by the parser's default session key.

    Args:
        roots: Root span rows, in listing order.

    Returns:
        Root row groups keyed by session id, in first-appearance order.
    """
    groups: dict[str, list[dict[str, Any]]] = {}
    for root in roots:
        key = get_session_id(root) or str(root["root_span_id"])
        groups.setdefault(key, []).append(root)
    return groups


def _bucket_rows(
    rows: list[dict[str, Any]], root_span_ids: list[str]
) -> dict[str, list[dict[str, Any]]]:
    """Bucket a batch query's rows by root span id, in query order.

    Args:
        rows: Span rows from one batch query.
        root_span_ids: Root span ids the query matched, in request order.

    Returns:
        Row lists keyed by root span id, in root_span_ids order. An id
        with no matching rows keys an empty list.
    """
    buckets: dict[str, list[dict[str, Any]]] = {
        root_span_id: [] for root_span_id in root_span_ids
    }
    for row in rows:
        bucket = buckets.get(row.get("root_span_id"))
        if bucket is not None:
            bucket.append(row)
    return buckets


def _find_root_row(
    rows: list[dict[str, Any]], root_span_id: str
) -> dict[str, Any] | None:
    """Return a trace's root span row among its fetched rows.

    Args:
        rows: Fetched span rows of one trace.
        root_span_id: Root span id identifying the trace.

    Returns:
        The row whose span_id is root_span_id, None when absent.
    """
    for row in rows:
        if row.get("span_id") == root_span_id:
            return row
    return None


def _split_batches(groups: dict[str, list[dict[str, Any]]]) -> list[list[str]]:
    """Pack session groups into batches of at least _TRACES_PER_BATCH root span ids.

    Args:
        groups: Root row groups keyed by session id, in first-appearance order.

    Returns:
        Root span id batches, in group order. A group's ids are never
        split across batches.
    """
    batches: list[list[str]] = []
    current: list[str] = []
    for group_roots in groups.values():
        current.extend(str(root["root_span_id"]) for root in group_roots)
        if len(current) >= _TRACES_PER_BATCH:
            batches.append(current)
            current = []
    if current:
        batches.append(current)
    return batches


async def _fetch_batch(
    client: httpx.AsyncClient, project_id: str, root_span_ids: list[str]
) -> bytes:
    """Fetch and serialize the span rows of one batch of session groups.

    Args:
        client: HTTP client.
        project_id: Braintrust project id.
        root_span_ids: Root span ids in the batch, in listing order.

    Returns:
        Trace payload bytes for the batch.
    """
    rows = await _query_spans_batch(client, project_id, root_span_ids)
    buckets = _bucket_rows(rows, root_span_ids)
    ordered = [row for root_span_id in root_span_ids for row in buckets[root_span_id]]
    return serialize_spans(ordered)


async def _fetch_chunk(
    client: httpx.AsyncClient, project_id: str, root_span_ids: list[str]
) -> dict[str, list[dict[str, Any]]]:
    """Fetch and bucket the span rows of one trace_ids chunk by root span id.

    Args:
        client: HTTP client.
        project_id: Braintrust project id.
        root_span_ids: Root span ids in the chunk, in the requested order.

    Returns:
        Row lists keyed by root span id, in the requested order.
    """
    rows = await _query_spans_batch(client, project_id, root_span_ids)
    return _bucket_rows(rows, root_span_ids)


async def fetch(query: dict[str, Any]) -> AsyncGenerator[bytes, None]:
    """Fetch parser payloads matching a query, one per batch of many traces.

    A time-window query lists root spans, groups them by the same session
    key the parser resolves by default, and packs whole groups into
    batches of at least _TRACES_PER_BATCH root span ids, so a session
    group is never split across batches. Each batch is one BTQL query,
    and its rows are bucketed by root span id and yielded as a single
    payload, so one payload can carry several sessions. The parser still
    assigns each fetched trace to a Kitaru session by its session id, so
    a session split across payloads would import its second trace as a
    duplicate external id and get it rejected, which the batching avoids.
    A trace_ids query batches the requested ids into chunks of the same
    size, one query per chunk. A chunk row whose root carries a session
    key is held until every chunk has been fetched, since its session
    membership is only known once fetched, then yielded per session. A
    chunk row without a session key is yielded with the rest of its
    chunk right away. Batches and chunks run concurrently, up to the
    query's concurrency, and results stay in listing or request order. A
    request that hits the Braintrust rate limit waits out the reported
    delay and retries instead of failing the fetch.

    Args:
        query: Fetch query.

    Raises:
        ValueError: The query is invalid.

    Yields:
        Trace payload bytes, one per batch in time-window mode, or one
        per chunk of standalone traces plus one per held session in
        trace_ids mode, or nothing when no trace matches.
    """
    parsed = BraintrustImportQuery.model_validate(query)
    async with httpx.AsyncClient() as client:
        if parsed.trace_ids is not None:
            chunks = [
                parsed.trace_ids[index : index + _TRACES_PER_BATCH]
                for index in range(0, len(parsed.trace_ids), _TRACES_PER_BATCH)
            ]
            chunk_awaitables = (
                _fetch_chunk(client, parsed.project_id, chunk) for chunk in chunks
            )
            held: dict[str, list[dict[str, Any]]] = {}
            # Close the stream explicitly so an early stop cancels in-flight fetches.
            async with aclosing(
                stream_bounded(chunk_awaitables, parsed.concurrency)
            ) as results:
                async for buckets in results:
                    standalone: list[dict[str, Any]] = []
                    for root_span_id, rows in buckets.items():
                        root = _find_root_row(rows, root_span_id)
                        session_key = get_session_id(root) if root is not None else None
                        if session_key:
                            held.setdefault(session_key, []).extend(rows)
                        else:
                            standalone.extend(rows)
                    if standalone:
                        yield serialize_spans(standalone)
            for rows in held.values():
                yield serialize_spans(rows)
            return

        since, until = parsed.get_window()
        roots = [
            root
            async for root in _list_root_spans(client, parsed.project_id, since, until)
        ]
        groups = _group_roots_by_session(roots)
        batch_awaitables = (
            _fetch_batch(client, parsed.project_id, root_span_ids)
            for root_span_ids in _split_batches(groups)
        )
        # Close the stream explicitly so an early stop cancels in-flight fetches.
        async with aclosing(
            stream_bounded(batch_awaitables, parsed.concurrency)
        ) as payloads:
            async for payload in payloads:
                yield payload
