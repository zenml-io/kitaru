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
"""Focused contract tests for the Logfire adapter plugin."""

import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from opentelemetry.trace import format_trace_id

from kitaru import importer_adapter
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import NodeType, SessionNodeBatchRequest
from kitaru.task.importer import ImportedSession
from kitaru_logfire_importer.adapter import _PARSER_PARAMS, LogfireAdapter
from kitaru_logfire_importer.importer import parse

from .fixtures import PROJECT_ID, FakeLogfire, build_complete_rows, build_row


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
) -> tuple[LogfireAdapter, _FakeClient]:
    client = _FakeClient()
    monkeypatch.setattr(importer_adapter, "KitaruAPIClient", lambda: client)
    adapter = LogfireAdapter()
    return adapter, client


def _start_trace(adapter: LogfireAdapter) -> str:
    """Enter and exit the adapter trace to pin a trace id and start time."""
    with adapter.open_trace() as trace_id:
        pass
    return trace_id


def test_run_imports_the_logfire_trace_around_the_function(
    fake_logfire: FakeLogfire, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run the function inside a Logfire trace, then import the trace."""
    adapter, client = _adapter(monkeypatch)
    fake_logfire.poll_builders = [build_complete_rows, build_complete_rows]
    fake_logfire.fetch_builders = [build_complete_rows]

    def func(value: int) -> int:
        fake_logfire.events.append("func")
        return value * 2

    result = adapter.run(func, 21)

    assert result == 42
    assert fake_logfire.events == [
        "span-enter",
        "func",
        "span-exit",
        "flush",
        "poll",
        "poll",
        "fetch",
    ]
    trace_id = format_trace_id(fake_logfire.trace_ids[0])
    assert fake_logfire.requested == [trace_id] * 3
    assert fake_logfire.poll_min_timestamps[0] == fake_logfire.poll_min_timestamps[1]
    assert fake_logfire.poll_min_timestamps[0].tzinfo is not None
    assert fake_logfire.fetch_min_timestamps == [
        fake_logfire.poll_min_timestamps[0].isoformat()
    ]
    assert len(client.sessions.created) == 1
    request = client.sessions.created[0]
    assert request.agent_id is None
    assert request.origin == SessionOrigin.RECORDED
    assert request.status == SessionStatus.COMPLETED
    assert request.external_id == f"{PROJECT_ID}:{trace_id}"
    assert request.imported_from == "logfire"
    assert request.metadata["normalization_warnings"] == []
    assert len(client.sessions.batches) == 1
    nodes = client.sessions.batches[0].nodes
    assert [node.name for node in nodes] == ["kitaru-run", "llm-call"]
    assert nodes[0].parent_index is None
    assert nodes[1].parent_index == 0
    assert nodes[1].node_type == NodeType.LLM_CALL


async def test_wait_keeps_polling_while_the_trace_has_no_rows(
    fake_logfire: FakeLogfire,
) -> None:
    """Keep polling while the records query returns no rows."""
    adapter = LogfireAdapter()
    trace_id = _start_trace(adapter)
    fake_logfire.poll_builders = [
        lambda trace_id: [],
        lambda trace_id: [],
        build_complete_rows,
        build_complete_rows,
    ]

    await adapter.wait_until_complete(trace_id)

    assert fake_logfire.requested == [trace_id] * 4


async def test_wait_polls_until_the_root_row_has_ended(
    fake_logfire: FakeLogfire,
) -> None:
    """Keep polling while the root row has no end timestamp."""
    adapter = LogfireAdapter()
    trace_id = _start_trace(adapter)

    def unended(trace_id: str) -> list[dict[str, Any]]:
        return [build_row("root", trace_id, end_timestamp=None)]

    def ended(trace_id: str) -> list[dict[str, Any]]:
        return [build_row("root", trace_id)]

    fake_logfire.poll_builders = [unended, unended, ended]

    await adapter.wait_until_complete(trace_id)

    assert fake_logfire.requested == [trace_id] * 3


async def test_wait_polls_until_every_root_row_has_ended(
    fake_logfire: FakeLogfire,
) -> None:
    """Keep polling while a second root row has no end timestamp."""
    adapter = LogfireAdapter()
    trace_id = _start_trace(adapter)

    def second_root_unended(trace_id: str) -> list[dict[str, Any]]:
        return [
            build_row("root", trace_id),
            build_row("root-2", trace_id, end_timestamp=None),
        ]

    fake_logfire.poll_builders = [
        second_root_unended,
        second_root_unended,
        build_complete_rows,
    ]

    await adapter.wait_until_complete(trace_id)

    assert fake_logfire.requested == [trace_id] * 3


async def test_wait_polls_until_the_row_count_is_stable(
    fake_logfire: FakeLogfire,
) -> None:
    """Keep polling until two consecutive polls return the same count."""
    adapter = LogfireAdapter()
    trace_id = _start_trace(adapter)

    def one_row(trace_id: str) -> list[dict[str, Any]]:
        return [build_row("root", trace_id)]

    fake_logfire.poll_builders = [
        one_row,
        build_complete_rows,
        build_complete_rows,
    ]

    await adapter.wait_until_complete(trace_id)

    assert fake_logfire.requested == [trace_id] * 3


async def test_wait_requires_the_read_token(
    fake_logfire: FakeLogfire, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Raise when LOGFIRE_READ_TOKEN is not set."""
    adapter = LogfireAdapter()
    trace_id = _start_trace(adapter)
    monkeypatch.delenv("LOGFIRE_READ_TOKEN")

    with pytest.raises(RuntimeError, match="LOGFIRE_READ_TOKEN"):
        await adapter.wait_until_complete(trace_id)


async def test_fetch_requires_the_read_token(
    fake_logfire: FakeLogfire, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Raise when LOGFIRE_READ_TOKEN is not set."""
    adapter = LogfireAdapter()
    trace_id = _start_trace(adapter)
    monkeypatch.delenv("LOGFIRE_READ_TOKEN")

    with pytest.raises(RuntimeError, match="LOGFIRE_READ_TOKEN"):
        await adapter.fetch(trace_id)


async def test_fetch_round_trips_through_the_real_parser(
    fake_logfire: FakeLogfire,
) -> None:
    """Return NDJSON bytes the real parser groups by trace id."""
    adapter = LogfireAdapter()
    trace_id = _start_trace(adapter)
    fake_logfire.poll_builders = [build_complete_rows, build_complete_rows]
    fake_logfire.fetch_builders = [build_complete_rows]
    await adapter.wait_until_complete(trace_id)

    payload = await adapter.fetch(trace_id)

    assert fake_logfire.requested == [trace_id] * 3
    items = list(parse(payload, _PARSER_PARAMS))
    assert len(items) == 1
    session = items[0]
    assert isinstance(session, ImportedSession)
    # The rows carry a conversation attribute, so grouping by trace id
    # proves the pinned join path.
    assert session.external_id == f"{PROJECT_ID}:{trace_id}"
    assert session.status == SessionStatus.COMPLETED
    assert session.metadata["logfire.join_paths"] == ["trace_id"]
    assert session.metadata["normalization_warnings"] == []
    assert [node.name for node in session.nodes] == ["kitaru-run"]
    assert [child.name for child in session.nodes[0].children] == ["llm-call"]
    assert session.nodes[0].trace_id == trace_id
