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
from datetime import datetime
from typing import Any

import httpx
from logfire._internal.config import get_base_url_from_token
from logfire.query_client import AsyncLogfireQueryClient

from kitaru.env import get_required_env

__all__ = ["fetch_trace", "wait_for_trace"]

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


async def fetch_trace(trace_id: str, min_timestamp: datetime) -> bytes:
    """Fetch a trace once from the Logfire Query API as NDJSON.

    Args:
        trace_id: Logfire trace id.
        min_timestamp: Minimum timestamp for the records query.

    Returns:
        Trace payload bytes.
    """
    read_token = get_required_env("LOGFIRE_READ_TOKEN")
    # Request the NDJSON stream the importer parser expects directly
    # because the query client only returns decoded results.
    async with httpx.AsyncClient(
        base_url=get_base_url_from_token(read_token)
    ) as client:
        response = await client.post(
            "/v2/query",
            headers={
                "accept": "application/x-ndjson",
                "authorization": f"Bearer {read_token}",
            },
            json={
                "sql": f"SELECT * FROM records WHERE trace_id = '{trace_id}'",
                "min_timestamp": min_timestamp.isoformat(),
            },
        )
        response.raise_for_status()
        return response.content
