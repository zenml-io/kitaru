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
"""Logfire API read layer."""

import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import aclosing
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
from logfire._internal.config import get_base_url_from_token
from logfire.query_client import AsyncLogfireQueryClient
from pydantic import ConfigDict

from kitaru.api_models.v1.imports import ImportQuery
from kitaru.env import get_required_env
from kitaru.task.importer import gather_bounded, retry_rate_limited, stream_bounded

from .importer import get_default_join_value

__all__ = ["fetch", "fetch_trace", "wait_for_trace"]

_POLL_INTERVAL = 2.0


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
    """Return the Logfire rate limit wait in seconds, or None otherwise.

    Args:
        exc: Exception raised while calling the Logfire Query API.

    Returns:
        Seconds to wait before retrying, or None when exc is not a
        Logfire rate limit error.
    """
    if not isinstance(exc, httpx.HTTPStatusError) or exc.response.status_code != 429:
        return None
    return _parse_retry_after(exc.response.headers.get("Retry-After"))


def _get_query_client() -> AsyncLogfireQueryClient:
    """Build a Query API client from the read token in the environment.

    Returns:
        Query API client.
    """
    return AsyncLogfireQueryClient(get_required_env("LOGFIRE_READ_TOKEN"))


def _roots_have_ended(rows: list[dict[str, Any]]) -> bool:
    """Return whether every root record row has an end timestamp."""
    ids = {row.get("span_id") for row in rows}
    roots = [
        row
        for row in rows
        if row.get("parent_span_id") is None or row.get("parent_span_id") not in ids
    ]
    return bool(roots) and all(row.get("end_timestamp") is not None for row in roots)


async def wait_for_trace(trace_id: str, min_timestamp: datetime) -> None:
    """Poll the Logfire Query API until the trace is complete.

    Args:
        trace_id: Logfire trace id.
        min_timestamp: Minimum timestamp for the records query.
    """
    sql = (
        "SELECT span_id, parent_span_id, end_timestamp FROM records "
        f"WHERE trace_id = '{trace_id}'"
    )
    # The trace is complete when it has rows, every root record row has
    # an end timestamp, and the row count is stable across two
    # consecutive polls.
    previous_count: int | None = None
    async with _get_query_client() as client:
        while True:
            results = await client.query_json_rows(sql, min_timestamp=min_timestamp)
            rows = results["rows"]
            if len(rows) == previous_count and _roots_have_ended(rows):
                return
            previous_count = len(rows)
            await asyncio.sleep(_POLL_INTERVAL)


async def _post_query(
    client: httpx.AsyncClient, read_token: str, body: dict[str, Any]
) -> bytes:
    """Post one Query API request and return the raw NDJSON response body.

    Args:
        client: HTTP client.
        read_token: Logfire read token.
        body: Query API request body.

    Returns:
        NDJSON response body.
    """

    async def _post() -> httpx.Response:
        response = await client.post(
            "/v2/query",
            headers={
                "accept": "application/x-ndjson",
                "authorization": f"Bearer {read_token}",
            },
            json=body,
        )
        response.raise_for_status()
        return response

    response = await retry_rate_limited(_post, _get_retry_after)
    # HTTP success can contain a stream error after zero or more data rows.
    _rows_from_ndjson(response.content)
    return response.content


async def fetch_trace(
    trace_id: str,
    min_timestamp: datetime,
    client: httpx.AsyncClient | None = None,
) -> bytes:
    """Fetch a trace once from the Logfire Query API as NDJSON.

    Args:
        trace_id: Logfire trace id.
        min_timestamp: Minimum timestamp for the records query.
        client: HTTP client, a new one when None.

    Returns:
        Trace payload bytes.
    """
    read_token = get_required_env("LOGFIRE_READ_TOKEN")
    body = {
        "sql": f"SELECT * FROM records WHERE trace_id = '{trace_id}'",
        "min_timestamp": min_timestamp.isoformat(),
    }
    if client is not None:
        return await _post_query(client, read_token, body)
    # Request the NDJSON stream the importer parser expects directly
    # because the query client only returns decoded results.
    async with httpx.AsyncClient(
        base_url=get_base_url_from_token(read_token)
    ) as new_client:
        return await _post_query(new_client, read_token, body)


def _rows_from_ndjson(content: bytes) -> list[dict[str, Any]]:
    """Decode Query API NDJSON messages into result rows.

    Args:
        content: Query API NDJSON response body.

    Returns:
        Rows from the data messages, in encounter order.

    Raises:
        RuntimeError: The provider reports a query execution error.
    """
    rows: list[dict[str, Any]] = []
    for line in content.splitlines():
        if not line.strip():
            continue
        message = json.loads(line)
        if message.get("type") == "error":
            detail = message.get("message") or "Unknown query error"
            raise RuntimeError(f"Logfire query failed: {detail}")
        if message.get("type") == "data":
            rows.extend(message.get("rows", []))
    return rows


async def _list_root_rows(
    client: httpx.AsyncClient, since: datetime, until: datetime
) -> list[dict[str, Any]]:
    """List distinct root trace rows started within a time window.

    Args:
        client: HTTP client.
        since: Lower bound of trace start time.
        until: Upper bound of trace start time.

    Returns:
        Root rows carrying trace_id, start_timestamp, and attributes,
        ordered by start timestamp.
    """
    read_token = get_required_env("LOGFIRE_READ_TOKEN")
    content = await _post_query(
        client,
        read_token,
        {
            "sql": (
                "SELECT DISTINCT trace_id, start_timestamp, attributes FROM records "
                "WHERE parent_span_id IS NULL "
                f"AND start_timestamp >= '{since.isoformat()}' "
                f"AND start_timestamp <= '{until.isoformat()}' "
                "ORDER BY start_timestamp"
            ),
            "min_timestamp": since.isoformat(),
            "max_timestamp": until.isoformat(),
        },
    )
    rows = _rows_from_ndjson(content)

    roots: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        trace_id = str(row["trace_id"])
        if trace_id not in seen:
            seen.add(trace_id)
            roots.append(row)
    return roots


def _get_session_group_key(row: dict[str, Any]) -> str:
    """Return the session grouping key the parser's default join would use.

    Args:
        row: Listed root trace row.

    Returns:
        The value at the parser's default join paths, the row's own trace
        id when none of them resolve.
    """
    return get_default_join_value(row) or str(row["trace_id"])


async def _fetch_group_payload(
    trace_ids: list[str],
    min_timestamp: datetime,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> bytes:
    """Fetch one session group's traces and join them into a parser payload.

    Args:
        trace_ids: Trace ids sharing one session grouping key, in listing
            order.
        min_timestamp: Minimum timestamp for each trace's records query.
        client: HTTP client.
        semaphore: Bound on in-flight trace fetches, shared across every
            group so the total stays within the query concurrency.

    Returns:
        NDJSON payload joining the group's trace payloads in listing order.
    """

    async def _fetch_one(trace_id: str) -> bytes:
        async with semaphore:
            return await fetch_trace(trace_id, min_timestamp, client)

    trace_payloads = await asyncio.gather(
        *(_fetch_one(trace_id) for trace_id in trace_ids)
    )
    # Join every trace of one session into a single payload so the parser
    # groups them into one Kitaru session instead of splitting them across
    # separate parse calls, where only the first trace of a session would
    # survive deduplication.
    return b"\n".join(trace_payloads)


class LogfireImportQuery(ImportQuery):
    """Logfire import query."""

    model_config = ConfigDict(extra="forbid")


async def fetch(query: dict[str, Any]) -> AsyncGenerator[bytes, None]:
    """Fetch one parser payload per session group matching a query.

    A time-window query lists root trace rows oldest first, groups them by
    the parser's default session join value, falling back to the trace id,
    and yields one payload per group in listing order so the runtime can
    ingest completed sessions before the rest of the window has been
    fetched. Every trace of one group still has to reach the parser in the
    same payload, because the parser folds them into a single session, and
    a second payload carrying the same external id conflicts with the
    first and is skipped. A `trace_ids` query has no cheap listing step to
    group ahead of the fetch, so it fetches every requested trace and
    yields them as one payload instead.

    Traces are fetched concurrently, up to the query's concurrency, and
    merged back in listing order. A request that hits the Logfire rate
    limit waits out the reported delay and retries instead of failing
    the fetch.

    Args:
        query: Fetch query. `trace_ids` fetches exactly those traces in
            order and ignores the time window. Otherwise `since` is
            required and `until` defaults to now.

    Raises:
        ValueError: The query is invalid.

    Yields:
        One NDJSON payload per session group, oldest first, or one payload
        for all requested trace ids. Nothing when there is nothing to
        fetch.
    """
    parsed = LogfireImportQuery.model_validate(query)
    read_token = get_required_env("LOGFIRE_READ_TOKEN")
    async with httpx.AsyncClient(
        base_url=get_base_url_from_token(read_token)
    ) as client:
        if parsed.trace_ids is not None:
            # The adapter approximates min_timestamp with the trace's own
            # start time. Arbitrary trace ids carry no such reference
            # point, so fall back to the earliest possible timestamp
            # instead.
            min_timestamp = parsed.since or datetime.min.replace(tzinfo=UTC)
            trace_payloads = await gather_bounded(
                (
                    fetch_trace(trace_id, min_timestamp, client)
                    for trace_id in parsed.trace_ids
                ),
                parsed.concurrency,
            )
            if trace_payloads:
                yield b"\n".join(trace_payloads)
            return

        since, until = parsed.get_window()
        groups: dict[str, list[str]] = {}
        for row in await _list_root_rows(client, since, until):
            groups.setdefault(_get_session_group_key(row), []).append(
                str(row["trace_id"])
            )
        if not groups:
            return

        semaphore = asyncio.Semaphore(parsed.concurrency)
        group_awaitables = (
            _fetch_group_payload(trace_ids, since, client, semaphore)
            for trace_ids in groups.values()
        )
        async with aclosing(
            stream_bounded(group_awaitables, parsed.concurrency)
        ) as payloads:
            async for payload in payloads:
                yield payload
