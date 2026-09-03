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
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import httpx
from logfire._internal.config import get_base_url_from_token
from logfire.query_client import AsyncLogfireQueryClient

from kitaru.env import get_required_env
from kitaru.task.importer import FetchQuery

__all__ = ["fetch", "fetch_trace", "wait_for_trace"]

_POLL_INTERVAL = 2.0


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
    response = await client.post(
        "/v2/query",
        headers={
            "accept": "application/x-ndjson",
            "authorization": f"Bearer {read_token}",
        },
        json=body,
    )
    response.raise_for_status()
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
    """
    rows: list[dict[str, Any]] = []
    for line in content.splitlines():
        if not line.strip():
            continue
        message = json.loads(line)
        if message.get("type") == "data":
            rows.extend(message.get("rows", []))
    return rows


async def _list_root_trace_ids(
    client: httpx.AsyncClient, since: datetime, until: datetime
) -> list[str]:
    """List distinct root trace ids started within a time window.

    Args:
        client: HTTP client.
        since: Lower bound of trace start time.
        until: Upper bound of trace start time.

    Returns:
        Trace ids ordered by start timestamp.
    """
    read_token = get_required_env("LOGFIRE_READ_TOKEN")
    content = await _post_query(
        client,
        read_token,
        {
            "sql": (
                "SELECT DISTINCT trace_id, start_timestamp FROM records "
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

    trace_ids: list[str] = []
    seen: set[str] = set()
    for row in rows:
        trace_id = str(row["trace_id"])
        if trace_id not in seen:
            seen.add(trace_id)
            trace_ids.append(trace_id)
    return trace_ids


async def fetch(query: dict[str, Any]) -> AsyncIterator[bytes]:
    """Fetch every trace matched by the query into one parser payload.

    Args:
        query: Fetch query. `trace_ids` fetches exactly those traces in
            order and ignores the time window. Otherwise `since` is
            required and `until` defaults to now.

    Raises:
        ValueError: The query is invalid.

    Yields:
        One NDJSON payload concatenating every fetched trace, oldest
        first. Nothing when there is nothing to fetch.
    """
    parsed = FetchQuery.model_validate(query)
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
            trace_ids = parsed.trace_ids
        else:
            since, until = parsed.get_window()
            min_timestamp = since
            trace_ids = await _list_root_trace_ids(client, since, until)

        trace_payloads = [
            await fetch_trace(trace_id, min_timestamp, client) for trace_id in trace_ids
        ]
        # Concatenate into one payload so the parser groups traces sharing
        # a session id into one Kitaru session instead of splitting them
        # across separate parse calls, where only the first trace of a
        # session would survive deduplication.
        if trace_payloads:
            yield b"\n".join(trace_payloads)
