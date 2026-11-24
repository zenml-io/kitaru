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
"""Workspace push identity, failure isolation, and retries."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest
from tests.cli.workspace_push_fakes import (
    build_counts,
    build_workspace_client,
    ingest_session,
    push,
    seed_workspace,
)

from kitaru.api_models.v1.agent import AgentCreateRequest
from kitaru.api_models.v1.insight import InsightInput, TextInsightData
from kitaru.api_models.v1.session import SessionResponse
from kitaru.api_models.v1.session_node import (
    SessionIngestRequest,
    SessionNodeListParams,
    SessionNodeResponse,
)
from kitaru.cli.workspace_push import PUSH_IMPORTED_FROM
from kitaru.cli.workspace_push_references import PUSH_METADATA_KEY
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, InvalidServerResponseError


@pytest.fixture
async def source() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide a client for the server pushed from."""
    async with build_workspace_client() as client:
        yield client


@pytest.fixture
async def destination() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide a client for the server pushed to."""
    async with build_workspace_client() as client:
        yield client


async def test_push_keeps_sessions_apart_that_share_half_an_identity(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """Sessions sharing an external id without ``imported_from`` stay distinct."""
    agent = await source.agents.create(AgentCreateRequest(name="partial"))
    first = await ingest_session(source, agent.id, external_id="run-7")
    second = await ingest_session(source, agent.id, external_id="run-7")
    third = await ingest_session(source, agent.id, imported_from="langfuse")
    fourth = await ingest_session(source, agent.id, imported_from="langfuse")

    report = await push(source, destination)

    assert report.counts["sessions"] == build_counts(copied=4)
    copies = {
        session.external_id: session async for session in destination.sessions.iter()
    }
    source_ids = {str(s.id) for s in (first, second, third, fourth)}
    assert set(copies) == source_ids
    assert {copy.imported_from for copy in copies.values()} == {PUSH_IMPORTED_FROM}
    for copy in copies.values():
        nodes = [node async for node in destination.sessions.iter_nodes(copy.id)]
        assert len(nodes) == 2
    rerun = await push(source, destination)
    assert rerun.counts["sessions"] == build_counts(already_present=4)


async def test_push_records_transport_failures_per_session(
    source: KitaruAPIClient,
    destination: KitaruAPIClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dropped connection or unreadable response fails one session only."""
    agent = await source.agents.create(AgentCreateRequest(name="bulk"))
    for index in range(6):
        await ingest_session(
            source, agent.id, imported_from="langfuse", external_id=f"trace-{index}"
        )
    ingest = destination.sessions.ingest

    async def ingest_flaky(request: SessionIngestRequest) -> SessionResponse:
        if request.session.external_id == "trace-1":
            raise httpx.ConnectError("connection reset")
        if request.session.external_id == "trace-4":
            raise InvalidServerResponseError("The response was not JSON.")
        return await ingest(request)

    monkeypatch.setattr(destination.sessions, "ingest", ingest_flaky)

    report = await push(source, destination)

    assert report.counts["sessions"] == build_counts(copied=4, failed=2)
    errors = {item["error"]: item["retryable"] for item in report.failed}
    assert errors == {
        "ConnectError: connection reset": True,
        "InvalidServerResponseError: The response was not JSON.": False,
    }
    assert report.retryable is False


async def test_push_fails_a_session_on_a_conflict_other_than_its_identity(
    source: KitaruAPIClient,
    destination: KitaruAPIClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only a duplicate-identity conflict means the session is already there."""
    agent = await source.agents.create(AgentCreateRequest(name="bulk"))
    for index in range(2):
        await ingest_session(
            source, agent.id, imported_from="langfuse", external_id=f"trace-{index}"
        )
    ingest = destination.sessions.ingest

    async def ingest_conflicting(request: SessionIngestRequest) -> SessionResponse:
        if request.session.external_id == "trace-1":
            raise APIError(409, "Node links conflict", code="node_link_conflict")
        return await ingest(request)

    monkeypatch.setattr(destination.sessions, "ingest", ingest_conflicting)

    report = await push(source, destination)

    assert report.counts["sessions"] == build_counts(copied=1, failed=1)
    [failure] = report.failed
    assert failure["error"] == "HTTP 409: Node links conflict"
    assert failure["retryable"] is False


async def test_push_holds_back_insights_of_a_failed_session(
    source: KitaruAPIClient,
    destination: KitaruAPIClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An insight citing a session that failed is retried, not written stale."""
    seed = await seed_workspace(source)
    await source.insights.create(
        seed.agent_id,
        [
            InsightInput(
                name="still-running",
                title="Still running",
                data=TextInsightData(content=f"See {seed.in_progress_session_id}."),
            )
        ],
    )
    ingest = destination.sessions.ingest

    async def ingest_most(request: SessionIngestRequest) -> SessionResponse:
        if request.session.external_id == "trace-42":
            raise APIError(503, "unavailable")
        return await ingest(request)

    monkeypatch.setattr(destination.sessions, "ingest", ingest_most)

    report = await push(source, destination)

    assert report.counts["insights"] == build_counts(copied=1, failed=1)
    [held] = [item for item in report.failed if item["kind"] == "insights"]
    assert held["record"] == str(seed.insight_id)
    assert str(seed.imported_session_id) in held["error"]
    assert held["retryable"] is True
    assert [i.name async for i in destination.insights.iter()] == ["still-running"]

    monkeypatch.undo()
    retried = await push(source, destination)

    assert retried.failures == 0
    assert retried.counts["insights"] == build_counts(copied=1, already_present=1)
    [copy] = [
        insight
        async for insight in destination.insights.iter()
        if insight.name == "tool-retries"
    ]
    unmapped = copy.metadata[PUSH_METADATA_KEY]["unmapped_references"]
    assert str(seed.imported_session_id) not in {item["id"] for item in unmapped}


async def test_push_holds_back_insights_whose_nodes_cannot_be_mapped(
    source: KitaruAPIClient,
    destination: KitaruAPIClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed node listing ends with a complete report and a retryable insight."""
    seed = await seed_workspace(source)
    iter_nodes = destination.sessions.iter_nodes

    async def time_out(
        session_id: uuid.UUID, params: SessionNodeListParams | None = None
    ) -> AsyncGenerator[SessionNodeResponse, None]:
        raise httpx.ReadTimeout("timed out")
        yield

    monkeypatch.setattr(destination.sessions, "iter_nodes", time_out)

    report = await push(source, destination)

    assert report.counts["sessions"] == build_counts(copied=2, skipped=1)
    assert report.counts["insights"] == build_counts(failed=1)
    [held] = report.failed
    assert held["record"] == str(seed.insight_id)
    assert "ReadTimeout: timed out" in held["error"]
    assert held["retryable"] is True
    assert [insight async for insight in destination.insights.iter()] == []

    monkeypatch.setattr(destination.sessions, "iter_nodes", iter_nodes)
    retried = await push(source, destination)
    assert retried.counts["insights"] == build_counts(copied=1)
