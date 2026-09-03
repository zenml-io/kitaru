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

import asyncio
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest
from langfuse.api import (
    ObservationsV2Meta,
    ObservationsV2Response,
    ObservationV2,
    Traces,
    TraceWithFullDetails,
)
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


def build_observation_v2(
    observation_id: str,
    trace_id: str,
    *,
    parent_id: str | None = None,
    observation_type: str = "SPAN",
    end_time: str | None = "2026-07-24T10:00:01Z",
    project_id: str = "project-1",
    **extra: Any,
) -> ObservationV2:
    """Build one bulk-listed Langfuse v2 observation."""
    fields: dict[str, Any] = {
        "id": observation_id,
        "trace_id": trace_id,
        "project_id": project_id,
        "type": observation_type,
        "name": observation_id,
        "start_time": "2026-07-24T10:00:00Z",
        "level": "DEFAULT",
        "environment": "default",
        "usage_details": {"input": 3, "output": 5},
        "cost_details": {"total": 0.001},
        "input": None,
        "output": None,
        "metadata": None,
        "model_parameters": None,
        "provided_model_name": None,
        **extra,
    }
    if end_time is not None:
        fields["end_time"] = end_time
    if parent_id is not None:
        fields["parent_observation_id"] = parent_id
    return ObservationV2.model_validate(fields)


def build_observations_page(
    observations: list[ObservationV2], *, cursor: str | None = None
) -> ObservationsV2Response:
    """Build one page of the Langfuse bulk observations listing response."""
    return ObservationsV2Response(
        data=observations, meta=ObservationsV2Meta(cursor=cursor)
    )


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


def build_trace_page(
    trace_ids: list[str],
    page: int,
    total_pages: int,
    *,
    session_ids: dict[str, str] | None = None,
) -> Traces:
    """Build one page of the Langfuse trace-list response."""
    session_ids = session_ids or {}
    return Traces.model_validate(
        {
            "data": [
                {
                    "id": trace_id,
                    "timestamp": "2026-07-24T10:00:00Z",
                    "session_id": session_ids.get(trace_id),
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
        self.observation_pages: dict[str, list[ObservationsV2Response]] = {}
        self.observation_calls: list[dict[str, Any]] = []
        self.requested: list[str] = []
        self.list_calls: list[dict[str, Any]] = []
        self.events: list[str] = []
        self.trace_contexts: list[TraceContext] = []
        self.fetch_delays: list[float] = []
        self.in_flight = 0
        self.peak_in_flight = 0
        self.async_api = SimpleNamespace(
            trace=SimpleNamespace(get=self._get, list=self._list),
            observations=SimpleNamespace(get_many=self._get_many),
        )

    async def _get(self, trace_id: str, **kwargs: Any) -> TraceWithFullDetails:
        self.in_flight += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        try:
            if self.fetch_delays:
                await asyncio.sleep(self.fetch_delays.pop(0))
            self.requested.append(trace_id)
            self.events.append("poll")
            assert self.trace_builders, "unexpected trace poll"
            builder = self.trace_builders.pop(0)
            if isinstance(builder, Exception):
                raise builder
            return builder(trace_id)
        finally:
            self.in_flight -= 1

    async def _list(self, **kwargs: Any) -> Traces:
        self.list_calls.append(kwargs)
        assert self.trace_list_pages, "unexpected trace list call"
        return self.trace_list_pages.pop(0)

    async def _get_many(
        self, *, trace_id: str, **kwargs: Any
    ) -> ObservationsV2Response:
        self.observation_calls.append({"trace_id": trace_id, **kwargs})
        pages = self.observation_pages.get(trace_id)
        assert pages, f"unexpected observations listing for trace {trace_id!r}"
        return pages.pop(0)

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


def seed_default_observations(fake: FakeLangfuseClient, trace_ids: list[str]) -> None:
    """Populate the fake's bulk observations listing for the default trace shape.

    Args:
        fake: Fake Langfuse client to populate.
        trace_ids: Trace ids to seed, each with a root span and a nested
            generation matching `build_complete_trace`'s observations.
    """
    for trace_id in trace_ids:
        fake.observation_pages[trace_id] = [
            build_observations_page(
                [
                    build_observation_v2("obs-root", trace_id),
                    build_observation_v2(
                        "obs-llm",
                        trace_id,
                        parent_id="obs-root",
                        observation_type="GENERATION",
                        provided_model_name="gpt-5-nano",
                    ),
                ]
            )
        ]


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
