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
"""Focused contract tests for the LangSmith adapter plugin."""

import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from langsmith.schemas import Run

from kitaru import importer_adapter
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import NodeType, SessionNodeBatchRequest
from kitaru.task.importer import ImportedSession
from kitaru_langsmith_importer import LangSmithAdapter
from kitaru_langsmith_importer.importer import parse

from .fixtures import PROJECT_ID, FakeLangSmith, build_complete_runs, build_run


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
) -> tuple[LangSmithAdapter, _FakeClient, uuid.UUID]:
    client = _FakeClient()
    agent_id = uuid.uuid4()
    monkeypatch.setenv("KITARU_AGENT_ID", str(agent_id))
    monkeypatch.setattr(importer_adapter, "KitaruAPIClient", lambda: client)
    adapter = LangSmithAdapter()
    return adapter, client, agent_id


def test_run_imports_the_langsmith_trace_around_the_function(
    fake_langsmith: FakeLangSmith, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run the function inside a LangSmith trace, then import the trace."""
    adapter, client, agent_id = _adapter(monkeypatch)
    fake_langsmith.runs_builders = [build_complete_runs, build_complete_runs]

    def func(value: int) -> int:
        fake_langsmith.events.append("func")
        return value * 2

    result = adapter.run(func, 21)

    assert result == 42
    assert fake_langsmith.events == [
        "tracing-enter",
        "trace-enter",
        "func",
        "trace-exit",
        "tracing-exit",
        "flush",
        "poll",
        "poll",
    ]
    trace_id = str(fake_langsmith.pinned_run_ids[0])
    assert fake_langsmith.requested == [trace_id, trace_id]
    assert len(client.sessions.created) == 1
    request = client.sessions.created[0]
    assert request.agent_id == agent_id
    assert request.origin == SessionOrigin.RECORDED
    assert request.status == SessionStatus.COMPLETED
    assert request.external_id == f"{PROJECT_ID}:{trace_id}"
    assert request.imported_from == "langsmith"
    assert request.metadata["normalization_warnings"] == []
    assert len(client.sessions.batches) == 1
    nodes = client.sessions.batches[0].nodes
    assert [node.name for node in nodes] == ["kitaru-run", "llm-call"]
    assert nodes[0].parent_index is None
    assert nodes[1].parent_index == 0
    assert nodes[1].node_type == NodeType.LLM_CALL


async def test_wait_keeps_polling_while_the_trace_has_no_runs(
    fake_langsmith: FakeLangSmith,
) -> None:
    """Keep polling while the run listing returns no runs."""
    adapter = LangSmithAdapter()
    trace_id = str(uuid.uuid4())
    fake_langsmith.runs_builders = [
        lambda trace_id: [],
        lambda trace_id: [],
        build_complete_runs,
        build_complete_runs,
    ]

    await adapter.wait_until_complete(trace_id)

    assert fake_langsmith.requested == [trace_id] * 4


async def test_wait_keeps_polling_while_the_root_run_is_missing(
    fake_langsmith: FakeLangSmith,
) -> None:
    """Keep polling while no run carries the trace id as its run id."""
    adapter = LangSmithAdapter()
    trace_id = str(uuid.uuid4())
    child_id = str(uuid.uuid4())

    def rootless(trace_id: str) -> list[Run]:
        return [build_run(child_id, trace_id, parent_run_id=trace_id)]

    fake_langsmith.runs_builders = [
        rootless,
        rootless,
        build_complete_runs,
        build_complete_runs,
    ]

    await adapter.wait_until_complete(trace_id)

    assert fake_langsmith.requested == [trace_id] * 4


async def test_wait_polls_until_the_root_run_has_ended(
    fake_langsmith: FakeLangSmith,
) -> None:
    """Keep polling while the root run has no end time."""
    adapter = LangSmithAdapter()
    trace_id = str(uuid.uuid4())

    def unended(trace_id: str) -> list[Run]:
        return [build_run(trace_id, trace_id, end_time=None)]

    def ended(trace_id: str) -> list[Run]:
        return [build_run(trace_id, trace_id)]

    fake_langsmith.runs_builders = [unended, unended, ended]

    await adapter.wait_until_complete(trace_id)

    assert fake_langsmith.requested == [trace_id] * 3


async def test_wait_polls_until_every_run_has_ended(
    fake_langsmith: FakeLangSmith,
) -> None:
    """Keep polling while a child run has no end time."""
    adapter = LangSmithAdapter()
    trace_id = str(uuid.uuid4())

    def child_unended(trace_id: str) -> list[Run]:
        runs = build_complete_runs(trace_id)
        runs[-1].end_time = None
        return runs

    fake_langsmith.runs_builders = [
        child_unended,
        child_unended,
        build_complete_runs,
    ]

    await adapter.wait_until_complete(trace_id)

    assert fake_langsmith.requested == [trace_id] * 3


async def test_wait_polls_until_the_run_count_is_stable(
    fake_langsmith: FakeLangSmith,
) -> None:
    """Keep polling until two consecutive polls return the same count."""
    adapter = LangSmithAdapter()
    trace_id = str(uuid.uuid4())

    def one_run(trace_id: str) -> list[Run]:
        return [build_run(trace_id, trace_id)]

    fake_langsmith.runs_builders = [
        one_run,
        build_complete_runs,
        build_complete_runs,
    ]

    await adapter.wait_until_complete(trace_id)

    assert fake_langsmith.requested == [trace_id] * 3


async def test_fetch_round_trips_through_the_real_parser(
    fake_langsmith: FakeLangSmith,
) -> None:
    """Serialize the polled runs into a payload the real parser accepts."""
    adapter = LangSmithAdapter()
    trace_id = str(uuid.uuid4())
    fake_langsmith.runs_builders = [build_complete_runs, build_complete_runs]
    await adapter.wait_until_complete(trace_id)

    payload = await adapter.fetch(trace_id)

    assert len(fake_langsmith.requested) == 2
    items = list(parse(payload, LangSmithAdapter.parser_params))
    assert len(items) == 1
    session = items[0]
    assert isinstance(session, ImportedSession)
    assert session.external_id == f"{PROJECT_ID}:{trace_id}"
    assert session.status == SessionStatus.COMPLETED
    assert session.metadata["langsmith.join_paths"] == ["trace_id"]
    assert session.metadata["normalization_warnings"] == []
    assert [node.name for node in session.nodes] == ["kitaru-run"]
    assert [child.name for child in session.nodes[0].children] == ["llm-call"]
    assert session.nodes[0].trace_id == trace_id


async def test_fetch_refetches_a_trace_missing_from_the_poll_state(
    fake_langsmith: FakeLangSmith,
) -> None:
    """Fetch the runs from the API when no poll state is available."""
    adapter = LangSmithAdapter()
    trace_id = str(uuid.uuid4())
    fake_langsmith.runs_builders = [build_complete_runs]

    payload = await adapter.fetch(trace_id)

    assert fake_langsmith.requested == [trace_id]
    items = list(parse(payload, LangSmithAdapter.parser_params))
    assert len(items) == 1
    assert isinstance(items[0], ImportedSession)
