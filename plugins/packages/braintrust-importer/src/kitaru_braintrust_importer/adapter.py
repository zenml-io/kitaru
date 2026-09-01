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
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from braintrust import SpanImpl, current_logger, flush, start_span

from kitaru.importer_adapter import ImporterBackedAdapter

from .api import fetch_spans, serialize_spans, wait_for_spans
from .importer import parse

__all__ = ["BraintrustAdapter"]

_ROOT_SPAN_NAME = "kitaru-run"


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
        self._completed_rows[external_id] = await wait_for_spans(
            project_id, external_id
        )

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
            rows = await fetch_spans(project_id, external_id)
        return serialize_spans(rows)
