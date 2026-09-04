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
"""Shared OTel SDK provider and Phoenix client fakes for the Phoenix adapter."""

import asyncio
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import Any

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

import kitaru_phoenix_importer.adapter as adapter_module
import kitaru_phoenix_importer.api as api_module

SpansBuilder = Callable[[str], list[dict[str, Any]]] | Exception
ListPage = list[dict[str, Any]] | Exception

PROJECT = "test-project"


def build_span(
    span_id: str,
    trace_id: str,
    *,
    parent_id: str | None = None,
    name: str | None = None,
    span_kind: str = "AGENT",
    start_time: str = "2026-08-27T10:00:00+00:00",
    end_time: str = "2026-08-27T10:01:00+00:00",
    attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one Phoenix span record."""
    return {
        "name": name or span_id,
        "context": {"trace_id": trace_id, "span_id": span_id},
        "span_kind": span_kind,
        "parent_id": parent_id,
        "start_time": start_time,
        "end_time": end_time,
        "status_code": "OK",
        "status_message": "",
        "attributes": attributes or {},
        "events": [],
    }


def build_complete_spans(trace_id: str) -> list[dict[str, Any]]:
    """Build a finished trace with a root span and a nested LLM span."""
    return [
        build_span("root", trace_id, name="kitaru-run"),
        build_span(
            "llm",
            trace_id,
            parent_id="root",
            name="llm-call",
            span_kind="LLM",
            start_time="2026-08-27T10:00:01+00:00",
            attributes={"gen_ai.request.model": "gpt-5-nano"},
        ),
    ]


class _FakeSpans:
    """Span API fake routing queries to the scripted spans."""

    def __init__(self, fake: "FakePhoenix") -> None:
        self._fake = fake

    async def get_spans(
        self,
        *,
        project_identifier: str,
        trace_ids: Sequence[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int,
    ) -> list[dict[str, Any]]:
        self._fake.project_identifiers.append(project_identifier)
        self._fake.limits.append(limit)
        if trace_ids is not None:
            assert len(trace_ids) == 1
            self._fake.in_flight += 1
            self._fake.peak_in_flight = max(
                self._fake.peak_in_flight, self._fake.in_flight
            )
            try:
                if self._fake.fetch_delays:
                    await asyncio.sleep(self._fake.fetch_delays.pop(0))
                self._fake.requested.append(trace_ids[0])
                self._fake.events.append("get-spans")
                assert self._fake.span_builders, "unexpected span query"
                builder = self._fake.span_builders.pop(0)
                if isinstance(builder, Exception):
                    raise builder
                return builder(trace_ids[0])
            finally:
                self._fake.in_flight -= 1

        self._fake.events.append("list-spans")
        self._fake.list_windows.append((start_time, end_time))
        assert self._fake.list_pages, "unexpected span listing"
        page = self._fake.list_pages.pop(0)
        if isinstance(page, Exception):
            raise page
        return page


class _FakeAsyncClient:
    """Phoenix client fake carrying the fake span API."""

    def __init__(self, fake: "FakePhoenix") -> None:
        self.spans = _FakeSpans(fake)


class FakePhoenix:
    """OTel SDK provider and Phoenix span API fake with scripted spans."""

    def __init__(self) -> None:
        self.span_builders: list[SpansBuilder] = []
        self.requested: list[str] = []
        self.project_identifiers: list[str] = []
        self.limits: list[int] = []
        self.events: list[str] = []
        self.list_pages: list[ListPage] = []
        self.list_windows: list[tuple[datetime | None, datetime | None]] = []
        self.fetch_delays: list[float] = []
        self.in_flight = 0
        self.peak_in_flight = 0
        self.exporter = InMemorySpanExporter()
        self.provider = TracerProvider()
        self.provider.add_span_processor(SimpleSpanProcessor(self.exporter))


@pytest.fixture(autouse=True)
def _fast_polling(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api_module, "_POLL_INTERVAL", 0.0)


@pytest.fixture
def fake_phoenix(monkeypatch: pytest.MonkeyPatch) -> FakePhoenix:
    """Create a fake Phoenix setup and route the adapter to it."""
    fake = FakePhoenix()
    monkeypatch.setenv("PHOENIX_PROJECT", PROJECT)
    provider_flush = fake.provider.force_flush

    def force_flush(timeout_millis: int = 30000) -> bool:
        fake.events.append("flush")
        return provider_flush(timeout_millis)

    # Wrap the flush on the instance to keep the provider a real SDK
    # provider for the adapter's isinstance narrowing.
    monkeypatch.setattr(fake.provider, "force_flush", force_flush)
    monkeypatch.setattr(adapter_module, "get_tracer", fake.provider.get_tracer)
    monkeypatch.setattr(adapter_module, "get_tracer_provider", lambda: fake.provider)
    monkeypatch.setattr(api_module, "AsyncClient", lambda: _FakeAsyncClient(fake))
    return fake
