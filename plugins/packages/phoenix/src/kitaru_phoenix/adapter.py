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
"""Arize Phoenix importer-backed adapter."""

import asyncio
import json
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import format_trace_id, get_tracer, get_tracer_provider
from phoenix.client import AsyncClient
from phoenix.client.utils.config import get_env_project_name

from kitaru.importer_adapter import ImporterBackedAdapter
from kitaru_phoenix_importer.importer import parse

__all__ = ["PhoenixAdapter"]

_POLL_INTERVAL = 2.0
_ROOT_SPAN_NAME = "kitaru-run"
_SPAN_LIMIT = 1000


def _has_root(spans: list[Any]) -> bool:
    """Return whether a root span is among the fetched spans."""
    ids = {span["context"]["span_id"] for span in spans}
    return any(
        span.get("parent_id") is None or span["parent_id"] not in ids for span in spans
    )


class PhoenixAdapter(ImporterBackedAdapter):
    """Adapter importing Phoenix traces of wrapped runs."""

    provider = "phoenix"
    parser = staticmethod(parse)

    def __init__(self, completeness_timeout: float = 120.0) -> None:
        """Initialize the adapter.

        Args:
            completeness_timeout: Seconds to wait for the provider trace to
                complete.
        """
        super().__init__(completeness_timeout)
        self._spans: dict[str, list[Any]] = {}

    @contextmanager
    def trace(self) -> Iterator[str]:
        """Activate an OTel trace and yield its trace id.

        Raises:
            RuntimeError: No OTel tracer provider is configured.

        Yields:
            Phoenix trace id.
        """
        tracer = get_tracer(__name__)
        with tracer.start_as_current_span(_ROOT_SPAN_NAME) as root_span:
            context = root_span.get_span_context()
            if not context.is_valid:
                raise RuntimeError("No OTel tracer provider is configured")
            yield format_trace_id(context.trace_id)

    async def wait_until_complete(self, external_id: str) -> None:
        """Poll the Phoenix span API until the trace is complete.

        Args:
            external_id: Phoenix trace id.
        """
        provider = get_tracer_provider()
        # Flush in a worker thread because the SDK call blocks on network
        # delivery. Only the SDK provider exposes a flush.
        if isinstance(provider, TracerProvider):
            await asyncio.to_thread(provider.force_flush)
        project = get_env_project_name()
        client = AsyncClient()
        # The trace is complete when it has spans, a root span is present,
        # and the span count is stable across two consecutive polls.
        previous_count: int | None = None
        while True:
            spans = await client.spans.get_spans(
                project_identifier=project,
                trace_ids=[external_id],
                limit=_SPAN_LIMIT,
            )
            if len(spans) == previous_count and _has_root(spans):
                self._spans[external_id] = spans
                return
            previous_count = len(spans)
            await asyncio.sleep(_POLL_INTERVAL)

    async def fetch(self, external_id: str) -> bytes:
        """Fetch the finished trace as a Phoenix span JSON array.

        Args:
            external_id: Phoenix trace id.

        Returns:
            Trace payload bytes.
        """
        spans = self._spans.pop(external_id, None)
        if spans is None:
            spans = await AsyncClient().spans.get_spans(
                project_identifier=get_env_project_name(),
                trace_ids=[external_id],
                limit=_SPAN_LIMIT,
            )
        return json.dumps(spans).encode("utf-8")
