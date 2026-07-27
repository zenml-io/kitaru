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
"""Tests for the session node routes."""

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeJobRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
)
from kitaru.hashing import tool_call_cache_key
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_agent_service,
    get_session_node_service,
    get_session_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.account import Account
from kitaru.server.domain.ids import uuid7

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed session services."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    agent_repository = FakeAgentRepository()
    version_repository = FakeAgentVersionRepository(agent_repository)
    session_repository = FakeSessionRepository(agent_repository, version_repository)
    node_repository = FakeSessionNodeRepository(session_repository)
    job_repository = FakeJobRepository(session_repository, version_repository)
    agent_service = AgentService(repository=agent_repository)
    session_service = SessionService(
        repository=session_repository,
        agent_repository=agent_repository,
        agent_version_repository=version_repository,
        node_repository=node_repository,
        job_repository=job_repository,
    )
    node_service = SessionNodeService(
        repository=node_repository, session_repository=session_repository
    )
    app.dependency_overrides[get_agent_service] = lambda: agent_service
    app.dependency_overrides[get_session_service] = lambda: session_service
    app.dependency_overrides[get_session_node_service] = lambda: node_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def create_session(client: httpx.AsyncClient, **overrides: object) -> str:
    """Store an agent and a recorded session through the API.

    Args:
        client: HTTP client for the app.
        **overrides: Session request body overrides.

    Returns:
        Id of the created session.
    """
    response = await client.post(
        "/v1/agents", json={"name": f"bot-{uuid.uuid4().hex[:8]}"}
    )
    assert response.status_code == 201
    body: dict[str, object] = {
        "agent_id": response.json()["id"],
        "origin": "recorded",
        **overrides,
    }
    response = await client.post("/v1/sessions", json=body)
    assert response.status_code == 201
    return response.json()["id"]


def node_body(
    sequence: int,
    node_type: str = "span",
    name: str = "run",
    parent_id: str | None = None,
    **overrides: object,
) -> dict:
    """Build a node request body.

    Args:
        sequence: Node sequence.
        node_type: Node type.
        name: Node name.
        parent_id: Id of the primary parent.
        **overrides: Field overrides.

    Returns:
        Node request body.
    """
    body: dict[str, object] = {
        "id": str(uuid7()),
        "sequence": sequence,
        "node_type": node_type,
        "name": name,
        "status": "completed",
        **overrides,
    }
    if parent_id is not None:
        body["parent_id"] = parent_id
    return body


async def test_upsert_nodes_computes_keys(client: httpx.AsyncClient) -> None:
    """Upsert a batch and observe the computed keys."""
    session_id = await create_session(client)
    root = node_body(0, "span", "run")
    chat_one = node_body(1, "llm_call", "chat", parent_id=root["id"])
    chat_two = node_body(2, "llm_call", "chat", parent_id=root["id"])
    weather = node_body(
        3,
        "tool_call",
        "get_weather",
        parent_id=chat_two["id"],
        tool_name="get_weather",
        inputs={"city": "Berlin"},
    )
    response = await client.post(
        f"/v1/sessions/{session_id}/nodes",
        json={"nodes": [root, chat_one, chat_two, weather]},
    )
    assert response.status_code == 200
    body = response.json()
    assert [item["key"] for item in body] == [
        "span:run",
        "span:run/llm_call:chat",
        "span:run/llm_call:chat#2",
        "span:run/llm_call:chat#2/tool_call:get_weather",
    ]
    assert body[3]["cache_key"] == tool_call_cache_key(
        "get_weather", {"city": "Berlin"}
    )
    assert body[0]["cache_key"] is None
    assert body[0]["created"] is not None
    assert body[0]["updated"] is not None
    # The upsert response echoes the payloads that were just sent.
    assert body[3]["inputs"] == {"city": "Berlin"}


async def test_upsert_nodes_unknown_parent(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an unknown parent and reject the whole batch."""
    session_id = await create_session(client)
    missing_id = uuid.uuid4()
    response = await client.post(
        f"/v1/sessions/{session_id}/nodes",
        json={"nodes": [node_body(0), node_body(1, parent_id=str(missing_id))]},
    )
    assert response.status_code == 422
    assert response.json() == {"detail": f"Parent node {missing_id} was not found"}
    response = await client.get(f"/v1/sessions/{session_id}/nodes")
    assert response.status_code == 200
    assert response.json() == []


async def test_upsert_nodes_idempotent_retry(client: httpx.AsyncClient) -> None:
    """Retry the same batch and observe stable keys without conflicts."""
    session_id = await create_session(client)
    root = node_body(0, "span", "run")
    chat = node_body(1, "llm_call", "chat", parent_id=root["id"])
    batch = {"nodes": [root, chat]}
    response = await client.post(f"/v1/sessions/{session_id}/nodes", json=batch)
    assert response.status_code == 200
    first = response.json()
    response = await client.post(f"/v1/sessions/{session_id}/nodes", json=batch)
    assert response.status_code == 200
    second = response.json()
    assert [item["id"] for item in second] == [item["id"] for item in first]
    assert [item["key"] for item in second] == ["span:run", "span:run/llm_call:chat"]
    response = await client.get(f"/v1/sessions/{session_id}/nodes")
    assert len(response.json()) == 2


async def test_upsert_nodes_across_batches(client: httpx.AsyncClient) -> None:
    """Resolve parents and occurrences across batches."""
    session_id = await create_session(client)
    root = node_body(0, "span", "run")
    first_chat = node_body(1, "llm_call", "chat", parent_id=root["id"])
    response = await client.post(
        f"/v1/sessions/{session_id}/nodes", json={"nodes": [root, first_chat]}
    )
    assert response.status_code == 200
    second_chat = node_body(2, "llm_call", "chat", parent_id=root["id"])
    response = await client.post(
        f"/v1/sessions/{session_id}/nodes", json={"nodes": [second_chat]}
    )
    assert response.status_code == 200
    assert response.json()[0]["key"] == "span:run/llm_call:chat#2"


async def test_upsert_nodes_duplicate_sequence(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate sequence."""
    session_id = await create_session(client)
    response = await client.post(
        f"/v1/sessions/{session_id}/nodes", json={"nodes": [node_body(0)]}
    )
    assert response.status_code == 200
    response = await client.post(
        f"/v1/sessions/{session_id}/nodes",
        json={"nodes": [node_body(0, name="other")]},
    )
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"A node sequence is already registered in session {session_id}"
    }


async def test_upsert_nodes_finished_session(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for node ingest into a finished recorded session."""
    session_id = await create_session(client)
    response = await client.patch(
        f"/v1/sessions/{session_id}", json={"status": "completed"}
    )
    assert response.status_code == 200
    response = await client.post(
        f"/v1/sessions/{session_id}/nodes", json={"nodes": [node_body(0)]}
    )
    assert response.status_code == 409
    assert response.json() == {"detail": f"Session {session_id} is not in progress"}


async def test_upsert_nodes_imported_session(client: httpx.AsyncClient) -> None:
    """Accept node ingest for a terminal imported session."""
    session_id = await create_session(
        client,
        origin="imported",
        status="completed",
        provider="langfuse",
        external_id="lf-1",
    )
    response = await client.post(
        f"/v1/sessions/{session_id}/nodes", json={"nodes": [node_body(0)]}
    )
    assert response.status_code == 200
    assert response.json()[0]["key"] == "span:run"


async def test_upsert_nodes_unknown_session(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown session id."""
    missing_id = uuid.uuid4()
    response = await client.post(
        f"/v1/sessions/{missing_id}/nodes", json={"nodes": [node_body(0)]}
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Session {missing_id} was not found"}


async def test_upsert_nodes_non_finite_payload(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a raw JSON body with non-finite floats in a node."""
    session_id = await create_session(client)
    for token in ["NaN", "Infinity", "-Infinity"]:
        node = node_body(0, outputs={"value": "PLACEHOLDER"})
        content = json.dumps({"nodes": [node]}).replace('"PLACEHOLDER"', token)
        response = await client.post(
            f"/v1/sessions/{session_id}/nodes",
            content=content,
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 422


async def test_list_nodes_excludes_payloads(client: httpx.AsyncClient) -> None:
    """Exclude inputs, outputs, and attributes unless requested."""
    session_id = await create_session(client)
    response = await client.post(
        f"/v1/sessions/{session_id}/nodes",
        json={
            "nodes": [
                node_body(
                    0,
                    "tool_call",
                    "get_weather",
                    tool_name="get_weather",
                    inputs={"city": "Berlin"},
                    outputs={"temp": 21},
                    attributes={"mocked": False},
                    metadata={"note": "x"},
                )
            ]
        },
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/sessions/{session_id}/nodes")
    assert response.status_code == 200
    body = response.json()
    assert body[0]["inputs"] is None
    assert body[0]["outputs"] is None
    assert body[0]["attributes"] is None
    assert body[0]["metadata"] == {"note": "x"}
    assert body[0]["cache_key"] is not None

    response = await client.get(
        f"/v1/sessions/{session_id}/nodes", params={"include_payloads": True}
    )
    assert response.status_code == 200
    body = response.json()
    assert body[0]["inputs"] == {"city": "Berlin"}
    assert body[0]["outputs"] == {"temp": 21}
    assert body[0]["attributes"] == {"mocked": False}


async def test_list_nodes_ordered_by_sequence(client: httpx.AsyncClient) -> None:
    """List nodes ordered by sequence."""
    session_id = await create_session(client)
    root = node_body(0, name="run")
    late = node_body(5, name="late", parent_id=root["id"])
    early = node_body(2, name="early", parent_id=root["id"])
    response = await client.post(
        f"/v1/sessions/{session_id}/nodes", json={"nodes": [root, late, early]}
    )
    assert response.status_code == 200
    response = await client.get(f"/v1/sessions/{session_id}/nodes")
    assert [item["name"] for item in response.json()] == ["run", "early", "late"]


async def test_list_nodes_unknown_session(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown session id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/sessions/{missing_id}/nodes")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Session {missing_id} was not found"}
