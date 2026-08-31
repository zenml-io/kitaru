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
"""LangSmith importer-backed adapter."""

import asyncio
import json
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, ClassVar

from langsmith import Client, trace, tracing_context
from langsmith.run_trees import get_cached_client
from langsmith.schemas import Run

from kitaru.importer_adapter import ImporterBackedAdapter
from kitaru_langsmith_importer.importer import parse

__all__ = ["LangSmithAdapter"]

_POLL_INTERVAL = 2.0
_ROOT_RUN_NAME = "kitaru-run"


def _list_runs(client: Client, trace_id: str) -> list[Run]:
    """Fetch all runs of one trace via the LangSmith API.

    Args:
        client: LangSmith client.
        trace_id: LangSmith trace id.

    Returns:
        Trace runs.
    """
    return list(client.list_runs(trace_id=trace_id))


def _trace_has_ended(runs: list[Run], trace_id: str) -> bool:
    """Return whether the root run is present and every run has an end time."""
    has_root = any(str(run.id) == trace_id for run in runs)
    return has_root and all(run.end_time is not None for run in runs)


class LangSmithAdapter(ImporterBackedAdapter):
    """Adapter importing LangSmith traces of wrapped runs."""

    provider = "langsmith"
    parser = staticmethod(parse)
    parser_params: ClassVar[dict[str, Any]] = {"join_on": "trace_id"}

    def __init__(self, completeness_timeout: float = 120.0) -> None:
        """Initialize the adapter.

        Args:
            completeness_timeout: Seconds to wait for the provider trace to
                complete.
        """
        super().__init__(completeness_timeout)
        self._client: Client | None = None
        self._completed_runs: dict[str, list[Run]] = {}

    @contextmanager
    def trace(self) -> Iterator[str]:
        """Activate a LangSmith trace and yield its trace id.

        Yields:
            LangSmith trace id.
        """
        trace_id = uuid.uuid4()
        # For a root run the run id is the trace id, so pinning the run id
        # pins the trace id.
        with (
            tracing_context(enabled=True),
            trace(name=_ROOT_RUN_NAME, run_id=trace_id, parent="ignore") as run_tree,
        ):
            self._client = run_tree.client
            yield str(trace_id)

    async def wait_until_complete(self, external_id: str) -> None:
        """Poll the LangSmith API until the trace is complete.

        Args:
            external_id: LangSmith trace id.
        """
        client = self._client or get_cached_client()
        # Flush in a worker thread because the SDK call blocks on network
        # delivery.
        await asyncio.to_thread(client.flush)
        # The trace is complete when the root run is present, every run has
        # an end time, and the run count is stable across two consecutive
        # polls.
        previous_count: int | None = None
        while True:
            runs = await asyncio.to_thread(_list_runs, client, external_id)
            if len(runs) == previous_count and _trace_has_ended(runs, external_id):
                self._completed_runs[external_id] = runs
                return
            previous_count = len(runs)
            await asyncio.sleep(_POLL_INTERVAL)

    async def fetch(self, external_id: str) -> bytes:
        """Fetch the finished trace as LangSmith run JSONL.

        Args:
            external_id: LangSmith trace id.

        Returns:
            Trace payload bytes.
        """
        runs = self._completed_runs.pop(external_id, None)
        if runs is None:
            client = self._client or get_cached_client()
            runs = await asyncio.to_thread(_list_runs, client, external_id)
        # Serialize with the snake_case run field names the importer parser
        # expects.
        return "\n".join(
            json.dumps(run.model_dump(mode="json")) for run in runs
        ).encode("utf-8")
