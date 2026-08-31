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
"""Logfire importer-backed adapter."""

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, ClassVar

import httpx
from logfire import force_flush, span
from logfire._internal.config import get_base_url_from_token
from logfire.query_client import AsyncLogfireQueryClient
from opentelemetry.trace import format_trace_id

from kitaru.env import get_required_env
from kitaru.importer_adapter import ImporterBackedAdapter
from kitaru_logfire_importer.importer import parse

__all__ = ["LogfireAdapter"]

_POLL_INTERVAL = 2.0
_ROOT_SPAN_NAME = "kitaru-run"


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


class LogfireAdapter(ImporterBackedAdapter):
    """Adapter importing Logfire traces of wrapped runs."""

    provider = "logfire"
    parser = staticmethod(parse)
    parser_params: ClassVar[dict[str, Any]] = {"join_on": "trace_id"}

    def __init__(self, completeness_timeout: float = 120.0) -> None:
        """Initialize the adapter.

        Args:
            completeness_timeout: Seconds to wait for the provider trace to
                complete.
        """
        super().__init__(completeness_timeout)
        self._started_at: dict[str, datetime] = {}

    @contextmanager
    def trace(self) -> Iterator[str]:
        """Activate a Logfire trace and yield its trace id.

        Raises:
            RuntimeError: The Logfire span carries no span context.

        Yields:
            Logfire trace id.
        """
        with span(_ROOT_SPAN_NAME) as root_span:
            context = root_span.context
            if context is None:
                raise RuntimeError("Logfire span carries no span context")
            trace_id = format_trace_id(context.trace_id)
            # Capture the trace start because the Query API requires a
            # minimum timestamp.
            self._started_at[trace_id] = datetime.now(UTC)
            yield trace_id

    async def wait_until_complete(self, external_id: str) -> None:
        """Poll the Logfire Query API until the trace is complete.

        Args:
            external_id: Logfire trace id.
        """
        # Flush in a worker thread because the SDK call blocks on network
        # delivery.
        await asyncio.to_thread(force_flush)
        started_at = self._started_at[external_id]
        sql = (
            "SELECT span_id, parent_span_id, end_timestamp FROM records "
            f"WHERE trace_id = '{external_id}'"
        )
        # The trace is complete when it has rows, every root record row has
        # an end timestamp, and the row count is stable across two
        # consecutive polls.
        previous_count: int | None = None
        async with _get_query_client() as client:
            while True:
                results = await client.query_json_rows(sql, min_timestamp=started_at)
                rows = results["rows"]
                if len(rows) == previous_count and _roots_have_ended(rows):
                    return
                previous_count = len(rows)
                await asyncio.sleep(_POLL_INTERVAL)

    async def fetch(self, external_id: str) -> bytes:
        """Fetch the finished trace as Logfire Query API NDJSON.

        Args:
            external_id: Logfire trace id.

        Returns:
            Trace payload bytes.
        """
        started_at = self._started_at.pop(external_id)
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
                    "sql": f"SELECT * FROM records WHERE trace_id = '{external_id}'",
                    "min_timestamp": started_at.isoformat(),
                },
            )
            response.raise_for_status()
            return response.content
