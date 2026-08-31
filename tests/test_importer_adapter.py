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
"""Tests for the importer-backed adapter base class."""

import asyncio
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from conftest import imported_node, imported_session
from kitaru import importer_adapter
from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import (
    SessionNodeBatchRequest,
)
from kitaru.client.exceptions import APIError
from kitaru.importer_adapter import ImporterBackedAdapter
from kitaru.task.importer import (
    ImportedItem,
    Parser,
    SessionImportError,
)


def _single_session_parser(
    payload: bytes, params: dict[str, Any]
) -> Iterator[ImportedItem]:
    yield imported_session("trace-1", nodes=[imported_node("call-1")])


def _empty_parser(payload: bytes, params: dict[str, Any]) -> Iterator[ImportedItem]:
    return iter(())


def _two_session_parser(
    payload: bytes, params: dict[str, Any]
) -> Iterator[ImportedItem]:
    yield imported_session("trace-1")
    yield imported_session("trace-2")


def _failure_parser(payload: bytes, params: dict[str, Any]) -> Iterator[ImportedItem]:
    yield ImportFailure(line=1, external_id="trace-1", error="unparsable item")


class _FakeSessionsResource:
    """Session API fake recording create and ingest calls."""

    def __init__(self) -> None:
        self.session_id = uuid.uuid4()
        self.created: list[SessionCreateRequest] = []
        self.batches: list[SessionNodeBatchRequest] = []
        self.ingest_error: APIError | None = None

    async def create(self, request: SessionCreateRequest) -> Any:
        self.created.append(request)
        return SimpleNamespace(id=self.session_id)

    async def ingest_nodes(
        self, session_id: uuid.UUID, batch: SessionNodeBatchRequest
    ) -> list[Any]:
        if self.ingest_error is not None:
            raise self.ingest_error
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


class _FakeAdapter(ImporterBackedAdapter):
    """Adapter subclass with scripted hooks."""

    provider = "acme"

    def __init__(
        self,
        parser: Parser,
        completeness_timeout: float = 120.0,
    ) -> None:
        super().__init__(completeness_timeout)
        self.parser = parser
        self.events: list[str] = []
        self.wait_seconds = 0.0
        self.wait_error: Exception | None = None
        self.fetch_error: Exception | None = None

    @contextmanager
    def trace(self) -> Iterator[str]:
        self.events.append("trace-enter")
        yield "trace-1"
        self.events.append("trace-exit")

    async def wait_until_complete(self, external_id: str) -> None:
        self.events.append(f"wait:{external_id}")
        if self.wait_error is not None:
            raise self.wait_error
        await asyncio.sleep(self.wait_seconds)

    async def fetch(self, external_id: str) -> bytes:
        self.events.append(f"fetch:{external_id}")
        if self.fetch_error is not None:
            raise self.fetch_error
        return b"payload"


def _adapter(
    monkeypatch: pytest.MonkeyPatch,
    parser: Parser,
    completeness_timeout: float = 120.0,
) -> tuple[_FakeAdapter, _FakeClient, uuid.UUID]:
    client = _FakeClient()
    agent_id = uuid.uuid4()
    monkeypatch.setenv("KITARU_AGENT_ID", str(agent_id))
    monkeypatch.setattr(importer_adapter, "KitaruAPIClient", lambda: client)
    adapter = _FakeAdapter(parser, completeness_timeout=completeness_timeout)
    return adapter, client, agent_id


def test_run_imports_the_trace_around_the_function(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run the function inside the trace, then import the finished trace."""
    adapter, client, agent_id = _adapter(monkeypatch, _single_session_parser)

    def func(value: int) -> int:
        adapter.events.append("func")
        return value * 2

    result = adapter.run(func, 21)

    assert result == 42
    assert adapter.events == [
        "trace-enter",
        "func",
        "trace-exit",
        "wait:trace-1",
        "fetch:trace-1",
    ]
    assert len(client.sessions.created) == 1
    request = client.sessions.created[0]
    assert request.agent_id == agent_id
    assert request.origin == SessionOrigin.IMPORTED
    assert request.external_id == "trace-1"
    assert request.imported_from == "acme"
    assert len(client.sessions.batches) == 1
    assert client.sessions.batches[0].nodes[0].name == "call-1"


async def test_run_async_imports_the_trace_around_the_function(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run the async function inside the trace, then import the finished trace."""
    adapter, client, _ = _adapter(monkeypatch, _single_session_parser)

    async def func(value: int) -> int:
        adapter.events.append("func")
        return value * 2

    result = await adapter.run_async(func, 21)

    assert result == 42
    assert adapter.events == [
        "trace-enter",
        "func",
        "trace-exit",
        "wait:trace-1",
        "fetch:trace-1",
    ]
    assert len(client.sessions.created) == 1
    assert len(client.sessions.batches) == 1


def test_run_rejects_a_parse_without_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise SessionImportError when the parse yields no session."""
    adapter, client, _ = _adapter(monkeypatch, _empty_parser)

    with pytest.raises(SessionImportError, match="0 sessions"):
        adapter.run(lambda: None)

    assert client.sessions.created == []


def test_run_rejects_a_parse_with_several_sessions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise SessionImportError when the parse yields more than one session."""
    adapter, client, _ = _adapter(monkeypatch, _two_session_parser)

    with pytest.raises(SessionImportError, match="2 sessions"):
        adapter.run(lambda: None)

    assert client.sessions.created == []


def test_run_rejects_a_parse_with_a_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise SessionImportError when the parse yields a failure."""
    adapter, client, _ = _adapter(monkeypatch, _failure_parser)

    with pytest.raises(SessionImportError, match="unparsable item"):
        adapter.run(lambda: None)

    assert client.sessions.created == []


def test_run_creates_a_failed_session_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Create a failed session and return the result on a completeness timeout."""
    adapter, client, agent_id = _adapter(
        monkeypatch, _single_session_parser, completeness_timeout=0.01
    )
    adapter.wait_seconds = 10.0

    result = adapter.run(lambda: "done")

    assert result == "done"
    assert len(client.sessions.created) == 1
    request = client.sessions.created[0]
    assert request.agent_id == agent_id
    assert request.origin == SessionOrigin.IMPORTED
    assert request.status == SessionStatus.FAILED
    assert request.external_id == "trace-1"
    assert request.imported_from == "acme"
    assert request.error is not None
    assert "did not complete" in request.error
    assert client.sessions.batches == []


def test_run_creates_a_failed_session_on_hook_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Treat a TimeoutError raised by the wait hook like an elapsed timeout."""
    adapter, client, _ = _adapter(monkeypatch, _single_session_parser)
    adapter.wait_error = TimeoutError("provider gave up")

    result = adapter.run(lambda: "done")

    assert result == "done"
    assert len(client.sessions.created) == 1
    assert client.sessions.created[0].status == SessionStatus.FAILED


def test_run_raises_a_fetch_error_after_the_function(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise the fetch error after the function has completed."""
    adapter, client, _ = _adapter(monkeypatch, _single_session_parser)
    adapter.fetch_error = RuntimeError("fetch failed")

    with pytest.raises(RuntimeError, match="fetch failed"):
        adapter.run(lambda: adapter.events.append("func"))

    assert "func" in adapter.events
    assert client.sessions.created == []


def test_run_raises_an_ingest_error_after_the_function(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise the ingest error after the function has completed."""
    adapter, client, _ = _adapter(monkeypatch, _single_session_parser)
    client.sessions.ingest_error = APIError(
        httpx.codes.UNPROCESSABLE_ENTITY, "invalid nodes"
    )

    with pytest.raises(APIError, match="invalid nodes"):
        adapter.run(lambda: adapter.events.append("func"))

    assert "func" in adapter.events
    assert len(client.sessions.created) == 1
    assert client.sessions.batches == []
