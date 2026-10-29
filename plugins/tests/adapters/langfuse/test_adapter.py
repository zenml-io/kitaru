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
from typing import Any

import pytest
from langfuse.api import NotFoundError

import kitaru_langfuse_importer.api as api_module
from kitaru import importer_adapter
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import NodeType, SessionNodeBatchRequest
from kitaru.task.importer import ImportedSession
from kitaru_langfuse_importer.adapter import LangfuseAdapter
from kitaru_langfuse_importer.importer import parse

from .fixtures import (
    FakeLangfuseClient,
    build_complete_trace,
    build_observation,
    build_observation_v2,
    build_observations_page,
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

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None


def _adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[LangfuseAdapter, _FakeClient]:
    client = _FakeClient()
    monkeypatch.setattr(importer_adapter, "KitaruAPIClient", lambda: client)
    adapter = LangfuseAdapter()
    return adapter, client


def test_run_imports_the_langfuse_trace_around_the_function(
    fake_langfuse: FakeLangfuseClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run the function inside a Langfuse trace, then import the trace."""
    adapter, client = _adapter(monkeypatch)
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
    assert request.agent_id is None
    assert request.origin == SessionOrigin.RECORDED
    assert request.status == SessionStatus.COMPLETED
    assert request.external_id == f"project-1:{trace_id}"
    assert request.imported_from == "langfuse"
    assert len(client.sessions.batches) == 1
    nodes = client.sessions.batches[0].nodes
    assert [node.name for node in nodes] == ["obs-root", "obs-llm"]
    assert nodes[0].parent_external_id is None
    assert nodes[1].parent_external_id == nodes[0].external_id
    assert nodes[1].node_type == NodeType.LLM_CALL


async def test_wait_polls_until_the_root_observation_has_ended(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Keep polling while the root observation has no end time."""
    adapter = LangfuseAdapter()

    def unended(trace_id: str) -> Any:
        return build_trace(
            trace_id, [build_observation("obs-root", trace_id, end_time=None)]
        )

    def ended(trace_id: str) -> Any:
        return build_trace(trace_id, [build_observation("obs-root", trace_id)])

    fake_langfuse.trace_builders = [unended, unended, ended, ended]

    await adapter.wait_until_complete("trace-1")

    assert fake_langfuse.requested == ["trace-1"] * 4


async def test_wait_polls_until_the_observation_count_is_stable(
    fake_langfuse: FakeLangfuseClient,
) -> None:
    """Keep polling until two consecutive polls return the same count."""
    adapter = LangfuseAdapter()

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
    adapter = LangfuseAdapter()
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
    adapter = LangfuseAdapter()
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
    adapter = LangfuseAdapter()
    fake_langfuse.trace_builders = [build_complete_trace]

    payload = await adapter.fetch("trace-1")

    assert fake_langfuse.requested == ["trace-1"]
    items = list(parse(payload, {}))
    assert len(items) == 1
    assert isinstance(items[0], ImportedSession)


async def test_poll_reads_all_pages_and_waits_for_a_delayed_root(fake_langfuse):
    child = build_observation_v2(
        "child", "trace", parent_id="root", is_root_observation=False
    )
    root = build_observation_v2("root", "trace", is_root_observation=True)
    fake_langfuse.observation_pages["trace"] = [
        build_observations_page([]),
        build_observations_page([]),
        build_observations_page([child]),
        build_observations_page([child]),
        build_observations_page([child], cursor="root-page"),
        build_observations_page([root]),
        build_observations_page([child], cursor="root-page"),
        build_observations_page([root]),
    ]
    adapter = LangfuseAdapter()
    await adapter.wait_until_complete("trace")
    assert len(fake_langfuse.requested) == 6
    assert [call["cursor"] for call in fake_langfuse.observation_calls] == [
        None,
        None,
        None,
        None,
        None,
        "root-page",
        None,
        "root-page",
    ]
    [session] = list(parse(await adapter.fetch("trace"), {}))
    assert isinstance(session, ImportedSession)
    assert session.nodes[0].children[0].name == "child"
    assert len(fake_langfuse.observation_calls) == 8


async def test_poll_restarts_stability_when_same_count_has_different_data(
    fake_langfuse,
):
    root = build_observation_v2(
        "root", "trace", is_root_observation=True, output='"old"'
    )
    updated = root.model_copy(update={"output": '"new"'})
    fake_langfuse.observation_pages["trace"] = [
        build_observations_page([root]),
        build_observations_page([updated]),
        build_observations_page([updated]),
    ]
    result = await api_module.wait_for_trace("trace")
    assert len(fake_langfuse.requested) == 3
    assert result.observations[0].output == '"new"'


async def test_poll_accepts_logical_root_with_a_physical_parent(fake_langfuse):
    root = build_observation_v2(
        "root", "trace", parent_id="remote-parent", is_root_observation=True
    )
    fake_langfuse.observations = [root]
    result = await api_module.wait_for_trace("trace")
    assert result.observations == [root]
    assert len(fake_langfuse.requested) == 2


@pytest.mark.parametrize("missing_root", [False, True])
async def test_poll_timeout_keeps_existing_failed_session_behavior(
    fake_langfuse, monkeypatch, missing_root
):
    client = _FakeClient()
    monkeypatch.setattr(importer_adapter, "KitaruAPIClient", lambda: client)
    if missing_root:
        fake_langfuse.observations = [
            build_observation_v2(
                "child", "trace", parent_id="missing", is_root_observation=False
            )
        ]
    adapter = LangfuseAdapter(completeness_timeout=0.01)
    await adapter._import_trace("trace", SessionOrigin.RECORDED)
    [request] = client.sessions.created
    assert request.status == SessionStatus.FAILED
    assert request.external_id is None
    assert request.inputs is None
    assert request.error == "Provider trace did not complete within 0.01 seconds"
    assert request.metadata == {"external_id": "trace"}
    assert not client.sessions.batches
    assert "trace" not in adapter._completed_traces
