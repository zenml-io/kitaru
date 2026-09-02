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
"""Focused contract tests for the Phoenix adapter plugin."""

import json
import uuid
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from opentelemetry.trace import NoOpTracerProvider, format_trace_id

import kitaru_phoenix_importer.adapter as adapter_module
from kitaru import importer_adapter
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import NodeType, SessionNodeBatchRequest
from kitaru.task.importer import ImportedSession
from kitaru_phoenix_importer.adapter import PhoenixAdapter
from kitaru_phoenix_importer.importer import parse

from .fixtures import PROJECT, FakePhoenix, build_complete_spans, build_span


class _FakeSessionsResource:
    """Session API fake recording create and ingest calls."""

    def __init__(self) -> None:
        self.session_id = uuid.uuid4()
        self.created: list[SessionCreateRequest] = []
        self.batches: list[SessionNodeBatchRequest] = []

    async def create(self, request: SessionCreateRequest) -> Any:
        self.created.append(request)
        return SimpleNamespace(id=self.session_id)

    async def ingest_nodes(
        self, session_id: uuid.UUID, batch: SessionNodeBatchRequest
    ) -> list[Any]:
        self.batches.append(batch)
        return []


class _FakeClient:
    """API client fake carrying the fake sessions resource."""

    def __init__(self) -> None:
        self.sessions = _FakeSessionsResource()

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None


def _adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[PhoenixAdapter, _FakeClient]:
    client = _FakeClient()
    monkeypatch.setattr(importer_adapter, "KitaruAPIClient", lambda: client)
    adapter = PhoenixAdapter()
    return adapter, client


def _start_trace(adapter: PhoenixAdapter) -> str:
    """Enter and exit the adapter trace to pin a trace id."""
    with adapter.open_trace() as trace_id:
        pass
    return trace_id


def test_run_imports_the_phoenix_trace_around_the_function(
    fake_phoenix: FakePhoenix, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run the function inside an OTel trace, then import the trace."""
    adapter, client = _adapter(monkeypatch)
    fake_phoenix.span_builders = [build_complete_spans, build_complete_spans]

    def func(value: int) -> int:
        fake_phoenix.events.append("func")
        return value * 2

    result = adapter.run(func, 21)

    assert result == 42
    assert fake_phoenix.events == ["func", "flush", "get-spans", "get-spans"]
    [recorded] = fake_phoenix.exporter.get_finished_spans()
    assert recorded.name == "kitaru-run"
    assert recorded.context is not None
    trace_id = format_trace_id(recorded.context.trace_id)
    assert fake_phoenix.requested == [trace_id] * 2
    assert fake_phoenix.project_identifiers == [PROJECT] * 2
    assert fake_phoenix.limits == [1000] * 2
    assert len(client.sessions.created) == 1
    request = client.sessions.created[0]
    assert request.agent_id is None
    assert request.origin == SessionOrigin.RECORDED
    assert request.status == SessionStatus.COMPLETED
    assert request.external_id == trace_id
    assert request.imported_from == "phoenix"
    assert request.metadata["normalization_warnings"] == []
    assert len(client.sessions.batches) == 1
    nodes = client.sessions.batches[0].nodes
    assert [node.name for node in nodes] == ["kitaru-run", "llm-call"]
    assert nodes[0].parent_index is None
    assert nodes[1].parent_index == 0
    assert nodes[1].node_type == NodeType.LLM_CALL


def test_trace_requires_a_tracer_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise when the span context comes from a no-op tracer provider."""
    monkeypatch.setattr(adapter_module, "get_tracer", NoOpTracerProvider().get_tracer)
    adapter = PhoenixAdapter()

    with (
        pytest.raises(RuntimeError, match="tracer provider"),
        adapter.open_trace(),
    ):
        pass


async def test_wait_keeps_polling_while_the_trace_has_no_spans(
    fake_phoenix: FakePhoenix,
) -> None:
    """Keep polling while the span query returns no spans."""
    adapter = PhoenixAdapter()
    trace_id = _start_trace(adapter)
    fake_phoenix.span_builders = [
        lambda trace_id: [],
        lambda trace_id: [],
        build_complete_spans,
        build_complete_spans,
    ]

    await adapter.wait_until_complete(trace_id)

    assert fake_phoenix.requested == [trace_id] * 4


def _status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "http://phoenix.test/v1/projects/p/spans")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("error", request=request, response=response)


async def test_wait_keeps_polling_while_the_project_is_missing(
    fake_phoenix: FakePhoenix,
) -> None:
    """Restart the stability check when the project does not exist yet."""
    adapter = PhoenixAdapter()
    trace_id = _start_trace(adapter)
    fake_phoenix.span_builders = [
        _status_error(404),
        build_complete_spans,
        _status_error(404),
        build_complete_spans,
        build_complete_spans,
    ]

    await adapter.wait_until_complete(trace_id)

    assert fake_phoenix.requested == [trace_id] * 5


async def test_wait_raises_other_span_query_errors(
    fake_phoenix: FakePhoenix,
) -> None:
    """Propagate a span query error that is not a missing project."""
    adapter = PhoenixAdapter()
    trace_id = _start_trace(adapter)
    fake_phoenix.span_builders = [_status_error(500)]

    with pytest.raises(httpx.HTTPStatusError):
        await adapter.wait_until_complete(trace_id)


async def test_wait_polls_until_the_span_count_is_stable(
    fake_phoenix: FakePhoenix,
) -> None:
    """Keep polling until two consecutive polls return the same count."""
    adapter = PhoenixAdapter()
    trace_id = _start_trace(adapter)

    def one_span(trace_id: str) -> list[dict[str, Any]]:
        return [build_span("root", trace_id, name="kitaru-run")]

    fake_phoenix.span_builders = [
        one_span,
        build_complete_spans,
        build_complete_spans,
    ]

    await adapter.wait_until_complete(trace_id)

    assert fake_phoenix.requested == [trace_id] * 3


async def test_wait_completes_when_the_only_root_is_an_orphan(
    fake_phoenix: FakePhoenix,
) -> None:
    """Count a span with a missing parent as the root."""
    adapter = PhoenixAdapter()
    trace_id = _start_trace(adapter)

    def orphan(trace_id: str) -> list[dict[str, Any]]:
        return [build_span("child", trace_id, parent_id="missing")]

    fake_phoenix.span_builders = [orphan, orphan]

    await adapter.wait_until_complete(trace_id)

    assert fake_phoenix.requested == [trace_id] * 2


async def test_wait_skips_the_flush_without_an_sdk_provider(
    fake_phoenix: FakePhoenix, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Poll without flushing when the provider exposes no flush."""
    adapter = PhoenixAdapter()
    trace_id = _start_trace(adapter)
    monkeypatch.setattr(adapter_module, "get_tracer_provider", NoOpTracerProvider)
    fake_phoenix.span_builders = [build_complete_spans, build_complete_spans]

    await adapter.wait_until_complete(trace_id)

    assert "flush" not in fake_phoenix.events
    assert fake_phoenix.requested == [trace_id] * 2


async def test_fetch_round_trips_through_the_real_parser(
    fake_phoenix: FakePhoenix,
) -> None:
    """Return span JSON bytes the real parser groups by trace id."""
    adapter = PhoenixAdapter()
    trace_id = _start_trace(adapter)
    fake_phoenix.span_builders = [build_complete_spans, build_complete_spans]
    await adapter.wait_until_complete(trace_id)

    payload = await adapter.fetch(trace_id)

    # The fetch serves the spans cached by the completeness wait.
    assert fake_phoenix.requested == [trace_id] * 2
    items = list(parse(payload, {}))
    assert len(items) == 1
    session = items[0]
    assert isinstance(session, ImportedSession)
    assert session.external_id == trace_id
    assert session.status == SessionStatus.COMPLETED
    assert session.metadata["normalization_warnings"] == []
    assert [node.name for node in session.nodes] == ["kitaru-run"]
    assert [child.name for child in session.nodes[0].children] == ["llm-call"]
    assert session.nodes[0].trace_id == trace_id


async def test_fetch_refetches_when_the_cache_is_absent(
    fake_phoenix: FakePhoenix,
) -> None:
    """Query the spans once when no cached spans exist."""
    adapter = PhoenixAdapter()
    trace_id = _start_trace(adapter)
    fake_phoenix.span_builders = [build_complete_spans]

    payload = await adapter.fetch(trace_id)

    assert fake_phoenix.requested == [trace_id]
    assert fake_phoenix.project_identifiers == [PROJECT]
    spans = json.loads(payload)
    assert [span["name"] for span in spans] == ["kitaru-run", "llm-call"]
