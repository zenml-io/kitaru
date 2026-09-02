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
"""Focused contract tests for the Braintrust adapter plugin."""

import uuid
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru import importer_adapter
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import NodeType, SessionNodeBatchRequest
from kitaru.task.importer import ImportedSession
from kitaru_braintrust_importer import BraintrustAdapter
from kitaru_braintrust_importer.importer import parse

from .fixtures import FakeBraintrust, build_complete_rows, build_row


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
) -> tuple[BraintrustAdapter, _FakeClient]:
    client = _FakeClient()
    monkeypatch.setattr(importer_adapter, "KitaruAPIClient", lambda: client)
    adapter = BraintrustAdapter()
    return adapter, client


def test_run_imports_the_braintrust_trace_around_the_function(
    fake_braintrust: FakeBraintrust, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run the function inside a Braintrust span, then import the trace."""
    adapter, client = _adapter(monkeypatch)
    fake_braintrust.rows_builders = [build_complete_rows, build_complete_rows]

    def func(value: int) -> int:
        fake_braintrust.events.append("func")
        return value * 2

    result = adapter.run(func, 21)

    assert result == 42
    assert fake_braintrust.events == [
        "span-enter",
        "func",
        "span-exit",
        "flush",
        "poll",
        "poll",
    ]
    root_span_id = fake_braintrust.spans[0].root_span_id
    assert fake_braintrust.requested == [root_span_id, root_span_id]
    assert len(client.sessions.created) == 1
    request = client.sessions.created[0]
    assert request.agent_id is None
    assert request.origin == SessionOrigin.RECORDED
    assert request.status == SessionStatus.COMPLETED
    assert request.external_id == f"project-1:{root_span_id}"
    assert request.imported_from == "braintrust"
    assert len(client.sessions.batches) == 1
    nodes = client.sessions.batches[0].nodes
    assert [node.name for node in nodes] == [root_span_id, "span-llm"]
    assert nodes[0].parent_index is None
    assert nodes[1].parent_index == 0
    assert nodes[1].node_type == NodeType.LLM_CALL


def test_trace_requires_an_active_braintrust_logger(
    fake_braintrust: FakeBraintrust, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject a run when the module-level span is the no-op span."""
    adapter, client = _adapter(monkeypatch)
    fake_braintrust.no_active_logger = True

    with pytest.raises(RuntimeError, match="No active Braintrust logger"):
        adapter.run(lambda: fake_braintrust.events.append("func"))

    assert "func" not in fake_braintrust.events
    assert client.sessions.created == []


async def test_wait_polls_until_the_root_span_has_ended(
    fake_braintrust: FakeBraintrust,
) -> None:
    """Keep polling while the root span row has no end metric."""
    adapter = BraintrustAdapter()

    def unended(root_span_id: str) -> Any:
        return [build_row(root_span_id, root_span_id, end=None)]

    def ended(root_span_id: str) -> Any:
        return [build_row(root_span_id, root_span_id)]

    fake_braintrust.rows_builders = [unended, unended, ended]

    await adapter.wait_until_complete("root-1")

    assert fake_braintrust.requested == ["root-1"] * 3


async def test_wait_polls_until_the_row_count_is_stable(
    fake_braintrust: FakeBraintrust,
) -> None:
    """Keep polling until two consecutive polls return the same count."""
    adapter = BraintrustAdapter()

    def one_row(root_span_id: str) -> Any:
        return [build_row(root_span_id, root_span_id)]

    fake_braintrust.rows_builders = [
        one_row,
        build_complete_rows,
        build_complete_rows,
    ]

    await adapter.wait_until_complete("root-1")

    assert fake_braintrust.requested == ["root-1"] * 3


async def test_wait_keeps_polling_while_the_trace_has_no_rows(
    fake_braintrust: FakeBraintrust,
) -> None:
    """Keep polling while the BTQL query returns no rows."""
    adapter = BraintrustAdapter()
    fake_braintrust.rows_builders = [
        lambda root_span_id: [],
        lambda root_span_id: [],
        build_complete_rows,
        build_complete_rows,
    ]

    await adapter.wait_until_complete("root-1")

    assert fake_braintrust.requested == ["root-1"] * 4


async def test_fetch_round_trips_through_the_real_parser(
    fake_braintrust: FakeBraintrust,
) -> None:
    """Serialize the polled rows into a payload the real parser accepts."""
    adapter = BraintrustAdapter()
    fake_braintrust.rows_builders = [build_complete_rows, build_complete_rows]
    await adapter.wait_until_complete("root-1")

    payload = await adapter.fetch("root-1")

    assert len(fake_braintrust.requested) == 2
    items = list(parse(payload, {}))
    assert len(items) == 1
    session = items[0]
    assert isinstance(session, ImportedSession)
    assert session.external_id == "project-1:root-1"
    assert session.status == SessionStatus.COMPLETED
    assert [node.name for node in session.nodes] == ["root-1"]
    assert [child.name for child in session.nodes[0].children] == ["span-llm"]
    assert session.nodes[0].trace_id == "root-1"


async def test_fetch_refetches_a_trace_missing_from_the_poll_state(
    fake_braintrust: FakeBraintrust,
) -> None:
    """Fetch the rows from the API when no poll state is available."""
    adapter = BraintrustAdapter()
    fake_braintrust.rows_builders = [build_complete_rows]

    payload = await adapter.fetch("root-1")

    assert fake_braintrust.requested == ["root-1"]
    items = list(parse(payload, {}))
    assert len(items) == 1
    assert isinstance(items[0], ImportedSession)
