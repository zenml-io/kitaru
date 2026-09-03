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
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import format_trace_id, get_tracer, get_tracer_provider

from kitaru.importer_adapter import ImporterBackedAdapter

from .api import fetch_spans, serialize_spans, wait_for_spans
from .importer import parse

__all__ = ["PhoenixAdapter"]

_ROOT_SPAN_NAME = "kitaru-run"


class PhoenixAdapter(ImporterBackedAdapter):
    """Adapter importing Phoenix traces of wrapped runs."""

    def __init__(self, completeness_timeout: float = 120.0) -> None:
        """Initialize the adapter.

        Args:
            completeness_timeout: Seconds to wait for the provider trace to
                complete.
        """
        super().__init__("phoenix", parse, completeness_timeout=completeness_timeout)
        self._spans: dict[str, list[Any]] = {}

    @contextmanager
    def open_trace(self) -> Iterator[str]:
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
        self._spans[external_id] = await wait_for_spans(external_id)

    async def fetch(self, external_id: str) -> bytes:
        """Fetch the finished trace as a Phoenix span JSON array.

        Args:
            external_id: Phoenix trace id.

        Returns:
            Trace payload bytes.
        """
        spans = self._spans.pop(external_id, None)
        if spans is None:
            spans = await fetch_spans(external_id)
        return serialize_spans(spans)
