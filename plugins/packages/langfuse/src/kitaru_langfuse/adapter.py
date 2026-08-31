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
"""Langfuse importer-backed adapter."""

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager

from langfuse import Langfuse, get_client
from langfuse.api import NotFoundError, ObservationsView, TraceWithFullDetails
from langfuse.types import TraceContext

from kitaru.importer_adapter import ImporterBackedAdapter
from kitaru_langfuse_importer.importer import parse

__all__ = ["LangfuseAdapter"]

_POLL_INTERVAL = 2.0
_ROOT_SPAN_NAME = "kitaru-run"


def _roots_have_ended(observations: list[ObservationsView]) -> bool:
    """Return whether every root observation has an end time."""
    ids = {observation.id for observation in observations}
    roots = [
        observation
        for observation in observations
        if observation.parent_observation_id is None
        or observation.parent_observation_id not in ids
    ]
    return bool(roots) and all(
        observation.end_time is not None for observation in roots
    )


class LangfuseAdapter(ImporterBackedAdapter):
    """Adapter importing Langfuse traces of wrapped runs."""

    provider = "langfuse"
    parser = staticmethod(parse)

    def __init__(self, completeness_timeout: float = 120.0) -> None:
        """Initialize the adapter.

        Args:
            completeness_timeout: Seconds to wait for the provider trace to
                complete.
        """
        super().__init__(completeness_timeout)
        self._completed_traces: dict[str, TraceWithFullDetails] = {}

    @contextmanager
    def trace(self) -> Iterator[str]:
        """Activate a Langfuse trace and yield its trace id.

        Yields:
            Langfuse trace id.
        """
        client = get_client()
        trace_id = Langfuse.create_trace_id()
        with client.start_as_current_span(
            name=_ROOT_SPAN_NAME, trace_context=TraceContext(trace_id=trace_id)
        ):
            yield trace_id

    async def wait_until_complete(self, external_id: str) -> None:
        """Poll the Langfuse API until the trace is complete.

        Args:
            external_id: Langfuse trace id.
        """
        client = get_client()
        # Flush in a worker thread because the SDK call blocks on network
        # delivery.
        await asyncio.to_thread(client.flush)
        api = client.async_api
        # The trace is complete when it is fetchable, every root observation
        # has an end time, and the observation count is stable across two
        # consecutive polls.
        previous_count: int | None = None
        while True:
            try:
                trace = await api.trace.get(external_id)
            except NotFoundError:
                previous_count = None
            else:
                if len(trace.observations) == previous_count and _roots_have_ended(
                    trace.observations
                ):
                    self._completed_traces[external_id] = trace
                    return
                previous_count = len(trace.observations)
            await asyncio.sleep(_POLL_INTERVAL)

    async def fetch(self, external_id: str) -> bytes:
        """Fetch the finished trace as Langfuse trace JSON.

        Args:
            external_id: Langfuse trace id.

        Returns:
            Trace payload bytes.
        """
        trace = self._completed_traces.pop(external_id, None)
        if trace is None:
            trace = await get_client().async_api.trace.get(external_id)
        # Serialize with the camelCase wire field names the importer parser
        # expects.
        return trace.json(by_alias=True).encode("utf-8")
