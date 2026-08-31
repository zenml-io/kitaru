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
"""Focused contract tests for the Langfuse adapter plugin."""

import uuid
from types import SimpleNamespace
from typing import Any, cast

from langfuse.api import NotFoundError

from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import NodeType, SessionNodeBatchRequest
from kitaru.client.api_client import KitaruAPIClient
from kitaru.task.importer import ImportedSession
from kitaru_langfuse import LangfuseAdapter
from kitaru_langfuse_importer.importer import parse

from .fixtures import (
    FakeLangfuseClient,
    build_complete_trace,
    build_observation,
    build_trace,
)


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


def _adapter() -> tuple[LangfuseAdapter, _FakeClient, uuid.UUID]:
    client = _FakeClient()
    agent_id = uuid.uuid4()
    adapter = LangfuseAdapter(cast(KitaruAPIClient, client), agent_id)
    return adapter, client, agent_id


def test_run_imports_the_langfuse_trace_around_the_function(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Run the function inside a Langfuse trace, then import the trace."""
    adapter, client, agent_id = _adapter()
    fake_langfuse.trace_builders = [build_complete_trace, build_complete_trace]

    def func(value: int) -> int:
        fake_langfuse.events.append("func")
        return value * 2

    result = adapter.run(func, 21)

    assert result == 42
    assert fake_langfuse.events == [
        "span-enter",
        "func",
        "span-exit",
        "flush",
        "poll",
        "poll",
    ]
    trace_id = fake_langfuse.trace_contexts[0]["trace_id"]
    assert fake_langfuse.requested == [trace_id, trace_id]
    assert len(client.sessions.created) == 1
    request = client.sessions.created[0]
    assert request.agent_id == agent_id
    assert request.origin == SessionOrigin.IMPORTED
    assert request.status == SessionStatus.COMPLETED
    assert request.external_id == f"project-1:{trace_id}"
    assert request.imported_from == "langfuse"
    assert len(client.sessions.batches) == 1
    nodes = client.sessions.batches[0].nodes
    assert [node.name for node in nodes] == ["obs-root", "obs-llm"]
    assert nodes[0].parent_index is None
    assert nodes[1].parent_index == 0
    assert nodes[1].node_type == NodeType.LLM_CALL


async def test_wait_polls_until_the_root_observation_has_ended(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Keep polling while the root observation has no end time."""
    adapter, _, _ = _adapter()

    def unended(trace_id: str) -> Any:
        return build_trace(
            trace_id, [build_observation("obs-root", trace_id, end_time=None)]
        )

    def ended(trace_id: str) -> Any:
        return build_trace(trace_id, [build_observation("obs-root", trace_id)])

    fake_langfuse.trace_builders = [unended, unended, ended]

    await adapter.wait_until_complete("trace-1")

    assert fake_langfuse.requested == ["trace-1"] * 3


async def test_wait_polls_until_the_observation_count_is_stable(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Keep polling until two consecutive polls return the same count."""
    adapter, _, _ = _adapter()

    def one_observation(trace_id: str) -> Any:
        return build_trace(trace_id, [build_observation("obs-root", trace_id)])

    fake_langfuse.trace_builders = [
        one_observation,
        build_complete_trace,
        build_complete_trace,
    ]

    await adapter.wait_until_complete("trace-1")

    assert fake_langfuse.requested == ["trace-1"] * 3


async def test_wait_keeps_polling_while_the_trace_is_not_fetchable(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Restart the stability check when a poll does not find the trace."""
    adapter, _, _ = _adapter()
    fake_langfuse.trace_builders = [
        build_complete_trace,
        NotFoundError("trace not found"),
        build_complete_trace,
        build_complete_trace,
    ]

    await adapter.wait_until_complete("trace-1")

    assert fake_langfuse.requested == ["trace-1"] * 4


async def test_fetch_round_trips_through_the_real_parser(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Serialize the polled trace into a payload the real parser accepts."""
    adapter, _, _ = _adapter()
    fake_langfuse.trace_builders = [build_complete_trace, build_complete_trace]
    await adapter.wait_until_complete("trace-1")

    payload = await adapter.fetch("trace-1")

    assert len(fake_langfuse.requested) == 2
    items = list(parse(payload, {}))
    assert len(items) == 1
    session = items[0]
    assert isinstance(session, ImportedSession)
    assert session.external_id == "project-1:trace-1"
    assert session.status == SessionStatus.COMPLETED
    assert [node.name for node in session.nodes] == ["obs-root"]
    assert [child.name for child in session.nodes[0].children] == ["obs-llm"]
    assert session.nodes[0].trace_id == "trace-1"


async def test_fetch_refetches_a_trace_missing_from_the_poll_state(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Fetch the trace from the API when no poll state is available."""
    adapter, _, _ = _adapter()
    fake_langfuse.trace_builders = [build_complete_trace]

    payload = await adapter.fetch("trace-1")

    assert fake_langfuse.requested == ["trace-1"]
    items = list(parse(payload, {}))
    assert len(items) == 1
    assert isinstance(items[0], ImportedSession)
