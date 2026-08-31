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
"""Braintrust importer-backed adapter."""

import asyncio
import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import httpx
from braintrust import SpanImpl, current_logger, flush, start_span

from kitaru.env import get_required_env
from kitaru.importer_adapter import ImporterBackedAdapter
from kitaru_braintrust_importer.importer import parse

__all__ = ["BraintrustAdapter"]

_POLL_INTERVAL = 2.0
_ROOT_SPAN_NAME = "kitaru-run"
_DEFAULT_API_URL = "https://api.braintrust.dev"


def _get_project_id() -> str:
    """Return the project id of the current Braintrust logger.

    Raises:
        RuntimeError: No Braintrust logger is active.

    Returns:
        Braintrust project id.
    """
    logger = current_logger()
    if logger is None:
        raise RuntimeError("No active Braintrust logger is configured")
    return logger.id


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


def _roots_have_ended(rows: list[dict[str, Any]]) -> bool:
    """Return whether every root span row has an end metric."""
    roots = [row for row in rows if not row.get("span_parents")]
    return bool(roots) and all(
        isinstance(metrics := row.get("metrics"), dict)
        and metrics.get("end") is not None
        for row in roots
    )


class BraintrustAdapter(ImporterBackedAdapter):
    """Adapter importing Braintrust traces of wrapped runs."""

    provider = "braintrust"
    parser = staticmethod(parse)

    def __init__(self, completeness_timeout: float = 120.0) -> None:
        """Initialize the adapter.

        Args:
            completeness_timeout: Seconds to wait for the provider trace to
                complete.
        """
        super().__init__(completeness_timeout)
        self._completed_rows: dict[str, list[dict[str, Any]]] = {}

    @contextmanager
    def trace(self) -> Iterator[str]:
        """Activate a Braintrust span and yield its root span id.

        Raises:
            RuntimeError: No Braintrust logger is active.

        Yields:
            Braintrust root span id.
        """
        span = start_span(name=_ROOT_SPAN_NAME)
        if not isinstance(span, SpanImpl):
            raise RuntimeError("No active Braintrust logger is configured")
        with span:
            yield span.root_span_id

    async def wait_until_complete(self, external_id: str) -> None:
        """Poll the Braintrust API until the trace is complete.

        Args:
            external_id: Braintrust root span id.
        """
        # Flush in a worker thread because the SDK call blocks on network
        # delivery.
        await asyncio.to_thread(flush)
        # Resolve the project id in a worker thread because the first access
        # logs in and registers the project.
        project_id = await asyncio.to_thread(_get_project_id)
        # The trace is complete when it has rows, every root span row has an
        # end metric, and the row count is stable across two consecutive
        # polls.
        previous_count: int | None = None
        async with httpx.AsyncClient() as client:
            while True:
                rows = await _query_spans(client, project_id, external_id)
                if len(rows) == previous_count and _roots_have_ended(rows):
                    self._completed_rows[external_id] = rows
                    return
                previous_count = len(rows)
                await asyncio.sleep(_POLL_INTERVAL)

    async def fetch(self, external_id: str) -> bytes:
        """Fetch the finished trace as Braintrust project-log JSON.

        Args:
            external_id: Braintrust root span id.

        Returns:
            Trace payload bytes.
        """
        rows = self._completed_rows.pop(external_id, None)
        if rows is None:
            project_id = await asyncio.to_thread(_get_project_id)
            async with httpx.AsyncClient() as client:
                rows = await _query_spans(client, project_id, external_id)
        # Serialize with the events envelope the importer parser expects.
        return json.dumps({"events": rows}).encode("utf-8")
