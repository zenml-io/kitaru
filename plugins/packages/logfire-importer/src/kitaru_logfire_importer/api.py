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
from kitaru.task.importer import retry_rate_limited, stream_bounded

from .importer import get_default_join_value

__all__ = ["fetch", "fetch_trace", "wait_for_trace"]

_POLL_INTERVAL = 2.0
# Trace ids fetched by one records query.
_TRACES_PER_BATCH = 25
# Rows one records query may return. The query API applies its own cap
# when the body names none, which would silently truncate a batch.
_ROW_LIMIT = 10_000


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
        "limit": _ROW_LIMIT,
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


def _split_batches(groups: dict[str, list[str]]) -> list[list[str]]:
    """Pack whole session groups into batches of at least _TRACES_PER_BATCH ids.

    Args:
        groups: Trace ids per session grouping key, in first-appearance
            order.

    Returns:
        Batches of trace ids in listing order. A group is never split
        across batches.
    """
    batches: list[list[str]] = []
    current: list[str] = []
    for trace_ids in groups.values():
        current.extend(trace_ids)
        if len(current) >= _TRACES_PER_BATCH:
            batches.append(current)
            current = []
    if current:
        batches.append(current)
    return batches


def _serialize_rows(rows: list[dict[str, Any]]) -> bytes:
    """Serialize one trace's rows into the row format fetch_trace produces.

    Args:
        rows: Records rows for one trace.

    Returns:
        NDJSON data-message payload carrying the rows.
    """
    return json.dumps({"type": "data", "rows": rows}).encode("utf-8")


def _get_root_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Return the root row among one trace's rows, None when absent.

    Args:
        rows: Records rows for one trace.

    Returns:
        The row without a parent span, None when no such row is present.
    """
    for row in rows:
        if row.get("parent_span_id") is None:
            return row
    return None


async def _fetch_batch(
    trace_ids: list[str],
    min_timestamp: datetime,
    client: httpx.AsyncClient,
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Fetch every row of a batch of traces in one records query.

    Args:
        trace_ids: Trace ids to fetch, in listing order.
        min_timestamp: Minimum timestamp for the records query.
        client: HTTP client.

    Returns:
        Each requested trace id paired with its rows, in listing order. A
        trace with no returned rows is omitted.
    """
    read_token = get_required_env("LOGFIRE_READ_TOKEN")
    ids = ", ".join(f"'{trace_id}'" for trace_id in trace_ids)
    content = await _post_query(
        client,
        read_token,
        {
            "sql": f"SELECT * FROM records WHERE trace_id IN ({ids})",
            "min_timestamp": min_timestamp.isoformat(),
            "limit": _ROW_LIMIT,
        },
    )
    rows = _rows_from_ndjson(content)
    # A full page means the cap cut rows off, so the batch is refetched one
    # trace at a time where each trace gets the whole limit to itself.
    if len(rows) >= _ROW_LIMIT and len(trace_ids) > 1:
        traces: list[tuple[str, list[dict[str, Any]]]] = []
        for trace_id in trace_ids:
            traces.extend(await _fetch_batch([trace_id], min_timestamp, client))
        return traces
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("trace_id")), []).append(row)
    return [
        (trace_id, grouped[trace_id]) for trace_id in trace_ids if trace_id in grouped
    ]


class LogfireImportQuery(ImportQuery):
    """Logfire import query."""

    model_config = ConfigDict(extra="forbid")


async def fetch(query: dict[str, Any]) -> AsyncGenerator[bytes, None]:
    """Fetch parser payloads matching a query, one per batch of complete sessions.

    A time-window query lists root trace rows oldest first, groups them by
    the parser's default session join value, falling back to the trace id,
    and packs whole groups into batches holding at least _TRACES_PER_BATCH
    trace ids, never splitting a group across batches. Each batch runs one
    records query for every trace id it holds and yields its rows as a
    single payload, oldest batch first, so the runtime can ingest complete
    sessions before the rest of the window has been fetched.

    A `trace_ids` query has no listing step to group ahead of the fetch,
    so it chunks the requested ids into batches of the same size and runs
    one query per chunk. A trace whose root row carries a session key is
    held until every chunk has been fetched, then yielded together with
    every other trace sharing that key. A trace without a session key is
    yielded as soon as its chunk completes.

    Batches and chunks are fetched concurrently, up to the query's
    concurrency, and yielded in submission order. A request that hits the
    Logfire rate limit waits out the reported delay and retries instead of
    failing the fetch.

    Args:
        query: Fetch query. `trace_ids` fetches exactly those traces in
            order and ignores the time window. Otherwise `since` is
            required and `until` defaults to now.

    Raises:
        ValueError: The query is invalid.

    Yields:
        One NDJSON payload per batch of complete sessions in a time
        window, or one payload per chunk of standalone trace ids and one
        payload per session spanning multiple requested trace ids. Nothing
        when there is nothing to fetch.
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
            chunks = [
                parsed.trace_ids[index : index + _TRACES_PER_BATCH]
                for index in range(0, len(parsed.trace_ids), _TRACES_PER_BATCH)
            ]
            chunk_awaitables = (
                _fetch_batch(chunk, min_timestamp, client) for chunk in chunks
            )
            # A trace's session is only known once its root row is back, so
            # traces with a session key wait until every chunk is in.
            held: dict[str, list[bytes]] = {}
            async with aclosing(
                stream_bounded(chunk_awaitables, parsed.concurrency)
            ) as results:
                async for traces in results:
                    immediate: list[bytes] = []
                    for _trace_id, rows in traces:
                        root = _get_root_row(rows)
                        session_key = get_default_join_value(root) if root else None
                        if session_key:
                            held.setdefault(session_key, []).append(
                                _serialize_rows(rows)
                            )
                        else:
                            immediate.append(_serialize_rows(rows))
                    if immediate:
                        yield b"\n".join(immediate)
            for segments in held.values():
                yield b"\n".join(segments)
            return

        since, until = parsed.get_window()
        groups: dict[str, list[str]] = {}
        for row in await _list_root_rows(client, since, until):
            groups.setdefault(_get_session_group_key(row), []).append(
                str(row["trace_id"])
            )
        if not groups:
            return

        batches = _split_batches(groups)
        batch_awaitables = (_fetch_batch(batch, since, client) for batch in batches)
        async with aclosing(
            stream_bounded(batch_awaitables, parsed.concurrency)
        ) as results:
            async for traces in results:
                if traces:
                    yield b"\n".join(_serialize_rows(rows) for _, rows in traces)
