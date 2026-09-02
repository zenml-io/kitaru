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

from logfire import force_flush, span
from opentelemetry.trace import format_trace_id

from kitaru.importer_adapter import ImporterBackedAdapter

from .api import fetch_trace, wait_for_trace
from .importer import parse

__all__ = ["LogfireAdapter"]

_PARSER_PARAMS = {"join_on": "trace_id"}

_ROOT_SPAN_NAME = "kitaru-run"


class LogfireAdapter(ImporterBackedAdapter):
    """Adapter importing Logfire traces of wrapped runs."""

    def __init__(self, completeness_timeout: float = 120.0) -> None:
        """Initialize the adapter.

        Args:
            completeness_timeout: Seconds to wait for the provider trace to
                complete.
        """
        super().__init__(
            "logfire", parse, _PARSER_PARAMS, completeness_timeout=completeness_timeout
        )
        self._started_at: dict[str, datetime] = {}

    @contextmanager
    def open_trace(self) -> Iterator[str]:
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
        await wait_for_trace(external_id, self._started_at[external_id])

    async def fetch(self, external_id: str) -> bytes:
        """Fetch the finished trace as Logfire Query API NDJSON.

        Args:
            external_id: Logfire trace id.

        Returns:
            Trace payload bytes.
        """
        started_at = self._started_at.pop(external_id)
        return await fetch_trace(external_id, started_at)
