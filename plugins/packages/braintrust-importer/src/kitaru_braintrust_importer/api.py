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
import json
import os
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

import httpx

from kitaru.env import get_required_env
from kitaru.task.importer import FetchQuery

__all__ = ["fetch", "fetch_spans", "serialize_spans", "wait_for_spans"]

_POLL_INTERVAL = 2.0
_DEFAULT_API_URL = "https://api.braintrust.dev"
_LIST_PAGE_SIZE = 1000


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
    api_url = os.environ.get("BRAINTRUST_API_URL") or _DEFAULT_API_URL
    query = (
        f"select: * | from: project_logs('{project_id}') spans"
        f" | filter: root_span_id = '{root_span_id}'"
    )
    response = await client.post(
        f"{api_url}/btql",
        headers={"Authorization": f"Bearer {get_required_env('BRAINTRUST_API_KEY')}"},
        json={"query": query},
    )
    response.raise_for_status()
    return response.json()["data"]


async def _list_root_span_ids(
    project_id: str, since: datetime, until: datetime
) -> list[str]:
    """List root span ids of a project in ascending start-time order.

    Args:
        project_id: Braintrust project id.
        since: Lower bound of root span start time.
        until: Upper bound of root span start time.

    Returns:
        Root span ids.
    """
    api_url = os.environ.get("BRAINTRUST_API_URL") or _DEFAULT_API_URL
    since_ts, until_ts = since.timestamp(), until.timestamp()
    query = (
        f"select: root_span_id | from: project_logs('{project_id}') spans"
        f" | filter: NOT EXISTS(span_parents) AND"
        f" ((created >= '{since.isoformat()}' AND created <= '{until.isoformat()}')"
        f" OR (metrics.start >= {since_ts} AND metrics.start <= {until_ts}))"
        f" | sort: created asc"
        f" | limit: {_LIST_PAGE_SIZE}"
    )
    root_span_ids: list[str] = []
    cursor: str | None = None
    async with httpx.AsyncClient() as client:
        while True:
            body: dict[str, Any] = {"query": query}
            if cursor is not None:
                body["cursor"] = cursor
            response = await client.post(
                f"{api_url}/btql",
                headers={
                    "Authorization": f"Bearer {get_required_env('BRAINTRUST_API_KEY')}"
                },
                json=body,
            )
            response.raise_for_status()
            payload = response.json()
            rows = payload["data"]
            root_span_ids.extend(
                str(row["root_span_id"]) for row in rows if row.get("root_span_id")
            )
            cursor = payload.get("cursor")
            if not cursor:
                return root_span_ids


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


async def fetch_spans(project_id: str, root_span_id: str) -> list[dict[str, Any]]:
    """Fetch the span rows of a trace once from the Braintrust API.

    Args:
        project_id: Braintrust project id.
        root_span_id: Braintrust root span id.

    Returns:
        Fetched span rows.
    """
    async with httpx.AsyncClient() as client:
        return await _query_spans(client, project_id, root_span_id)


def serialize_spans(rows: list[dict[str, Any]]) -> bytes:
    """Serialize fetched span rows into the payload the parser accepts.

    Args:
        rows: Fetched span rows.

    Returns:
        Trace payload bytes.
    """
    # Serialize with the events envelope the importer parser expects.
    return json.dumps({"events": rows}).encode("utf-8")


class BraintrustFetchQuery(FetchQuery):
    """Braintrust fetch query."""

    project_id: str


async def fetch(query: dict[str, Any]) -> AsyncIterator[bytes]:
    """Fetch parser payloads for traces matching a query.

    Args:
        query: Fetch query.

    Raises:
        ValueError: The query is invalid.

    Yields:
        Trace payload bytes.
    """
    parsed = BraintrustFetchQuery.model_validate(query)
    if parsed.trace_ids is not None:
        trace_ids = parsed.trace_ids
    else:
        since, until = parsed.get_window()
        trace_ids = await _list_root_span_ids(parsed.project_id, since, until)
    for trace_id in trace_ids:
        rows = await fetch_spans(parsed.project_id, trace_id)
        yield serialize_spans(rows)
