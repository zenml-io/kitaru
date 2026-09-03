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
"""Shared Langfuse SDK fakes for the Langfuse adapter."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest
from langfuse.api import Traces, TraceWithFullDetails
from langfuse.types import TraceContext

import kitaru_langfuse_importer.adapter as adapter_module
import kitaru_langfuse_importer.api as api_module

TraceBuilder = Callable[[str], TraceWithFullDetails]


def build_observation(
    observation_id: str,
    trace_id: str,
    *,
    parent_id: str | None = None,
    observation_type: str = "SPAN",
    end_time: str | None = "2026-07-24T10:00:01Z",
    **extra: Any,
) -> dict[str, Any]:
    """Build one Langfuse API observation record."""
    record: dict[str, Any] = {
        "id": observation_id,
        "trace_id": trace_id,
        "type": observation_type,
        "name": observation_id,
        "start_time": "2026-07-24T10:00:00Z",
        "usage": {"input": 3, "output": 5, "total": 8, "unit": "TOKENS"},
        "level": "DEFAULT",
        "usage_details": {"input": 3, "output": 5},
        "cost_details": {"total": 0.001},
        "environment": "default",
        "input": None,
        "output": None,
        "metadata": None,
        "model_parameters": None,
        **extra,
    }
    if end_time is not None:
        record["end_time"] = end_time
    if parent_id is not None:
        record["parent_observation_id"] = parent_id
    return record


def build_trace(
    trace_id: str,
    observations: list[dict[str, Any]],
    *,
    session_id: str | None = None,
) -> TraceWithFullDetails:
    """Build one Langfuse API trace response with nested observations."""
    return TraceWithFullDetails.model_validate(
        {
            "id": trace_id,
            "timestamp": "2026-07-24T10:00:00Z",
            "project_id": "project-1",
            "session_id": session_id,
            "name": "run",
            "input": {"prompt": "hello"},
            "output": {"answer": "world"},
            "metadata": None,
            "tags": [],
            "public": False,
            "environment": "default",
            "html_path": f"/project/project-1/traces/{trace_id}",
            "latency": 1.0,
            "total_cost": 0.001,
            "observations": observations,
            "scores": [],
        }
    )


def build_trace_page(trace_ids: list[str], page: int, total_pages: int) -> Traces:
    """Build one page of the Langfuse trace-list response."""
    return Traces.model_validate(
        {
            "data": [
                {
                    "id": trace_id,
                    "timestamp": "2026-07-24T10:00:00Z",
                    "tags": [],
                    "public": False,
                    "environment": "default",
                    "html_path": f"/project/project-1/traces/{trace_id}",
                }
                for trace_id in trace_ids
            ],
            "meta": {
                "page": page,
                "limit": max(len(trace_ids), 1),
                "total_items": len(trace_ids),
                "total_pages": total_pages,
            },
        }
    )


def build_complete_trace(
    trace_id: str, *, session_id: str | None = None
) -> TraceWithFullDetails:
    """Build one finished trace with a root span and a nested generation."""
    return build_trace(
        trace_id,
        [
            build_observation("obs-root", trace_id),
            build_observation(
                "obs-llm",
                trace_id,
                parent_id="obs-root",
                observation_type="GENERATION",
                model="gpt-5-nano",
            ),
        ],
        session_id=session_id,
    )


class FakeLangfuseClient:
    """Langfuse client fake with a scripted trace API."""

    def __init__(self) -> None:
        self.trace_builders: list[TraceBuilder | Exception] = []
        self.trace_list_pages: list[Traces] = []
        self.requested: list[str] = []
        self.list_calls: list[dict[str, Any]] = []
        self.events: list[str] = []
        self.trace_contexts: list[TraceContext] = []
        self.async_api = SimpleNamespace(
            trace=SimpleNamespace(get=self._get, list=self._list)
        )

    async def _get(self, trace_id: str) -> TraceWithFullDetails:
        self.requested.append(trace_id)
        self.events.append("poll")
        assert self.trace_builders, "unexpected trace poll"
        builder = self.trace_builders.pop(0)
        if isinstance(builder, Exception):
            raise builder
        return builder(trace_id)

    async def _list(self, **kwargs: Any) -> Traces:
        self.list_calls.append(kwargs)
        assert self.trace_list_pages, "unexpected trace list call"
        return self.trace_list_pages.pop(0)

    @contextmanager
    def start_as_current_observation(
        self, *, name: str, trace_context: TraceContext
    ) -> Iterator[None]:
        self.trace_contexts.append(trace_context)
        self.events.append("span-enter")
        yield
        self.events.append("span-exit")

    def flush(self) -> None:
        self.events.append("flush")


@pytest.fixture(autouse=True)
def _fast_polling(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api_module, "_POLL_INTERVAL", 0.0)


@pytest.fixture
def fake_langfuse(monkeypatch: pytest.MonkeyPatch) -> FakeLangfuseClient:
    """Create a fake Langfuse client and route the adapter to it."""
    fake = FakeLangfuseClient()
    monkeypatch.setattr(adapter_module, "get_client", lambda: fake)
    monkeypatch.setattr(api_module, "get_client", lambda: fake)
    return fake
