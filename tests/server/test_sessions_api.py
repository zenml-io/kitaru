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
"""Tests for the session routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    FakeTagRepository,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_agent_service,
    get_session_node_service,
    get_session_service,
    get_tag_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.application.services.tag_service import TagService
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
    tag_repository = FakeTagRepository()
    session_repository = FakeSessionRepository(
        agent_repository, version_repository, tag_repository
    )
    node_repository = FakeSessionNodeRepository(session_repository)
    agent_service = AgentService(repository=agent_repository)
    session_service = SessionService(
        repository=session_repository,
        agent_repository=agent_repository,
        agent_version_repository=version_repository,
        node_repository=node_repository,
    )
    node_service = SessionNodeService(
        repository=node_repository, session_repository=session_repository
    )
    tag_service = TagService(repository=tag_repository)
    app.dependency_overrides[get_agent_service] = lambda: agent_service
    app.dependency_overrides[get_session_service] = lambda: session_service
    app.dependency_overrides[get_session_node_service] = lambda: node_service
    app.dependency_overrides[get_tag_service] = lambda: tag_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def create_agent(client: httpx.AsyncClient, name: str = "support-bot") -> str:
    """Store an agent through the API.

    Args:
        client: HTTP client for the app.
        name: Agent name.

    Returns:
        Id of the created agent.
    """
    response = await client.post("/v1/agents", json={"name": name})
    assert response.status_code == 201
    return response.json()["id"]


async def create_session(
    client: httpx.AsyncClient, agent_id: str, **overrides: object
) -> dict:
    """Store a recorded session through the API.

    Args:
        client: HTTP client for the app.
        agent_id: Id of the agent.
        **overrides: Request body overrides.

    Returns:
        Created session body.
    """
    body: dict[str, object] = {
        "agent_id": agent_id,
        "origin": "recorded",
        "inputs": {"prompt": "hi"},
        **overrides,
    }
    response = await client.post("/v1/sessions", json=body)
    assert response.status_code == 201
    return response.json()


async def test_create_recorded_session(client: httpx.AsyncClient) -> None:
    """Create a recorded session and observe HTTP 201."""
    agent_id = await create_agent(client)
    response = await client.post(
        "/v1/sessions",
        json={
            "agent_id": agent_id,
            "origin": "recorded",
            "inputs": {"prompt": "hi"},
            "framework": "pydantic_ai",
            "started_at": "2026-07-01T12:00:00Z",
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["agent_id"] == agent_id
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["origin"] == "recorded"
    assert body["status"] == "in_progress"
    assert body["inputs"] == {"prompt": "hi"}
    assert body["framework"] == "pydantic_ai"
    assert body["scores"] == {}
    assert body["cost"] is None
    assert body["tokens"] is None
    assert body["llm_call_count"] == 0
    assert body["tool_call_count"] == 0
    assert body["created"] is not None
    assert body["updated"] is not None
    assert uuid.UUID(body["id"])


async def test_create_imported_session_duplicate(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate provider and external id pair."""
    agent_id = await create_agent(client)
    body = {
        "agent_id": agent_id,
        "origin": "imported",
        "status": "completed",
        "provider": "langfuse",
        "external_id": "lf-1",
    }
    response = await client.post("/v1/sessions", json=body)
    assert response.status_code == 201
    response = await client.post("/v1/sessions", json=body)
    assert response.status_code == 409
    assert response.json() == {
        "detail": "Session external id 'lf-1' is already registered for "
        "provider 'langfuse'"
    }


async def test_create_replay_session(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for the replay origin."""
    agent_id = await create_agent(client)
    response = await client.post(
        "/v1/sessions", json={"agent_id": agent_id, "origin": "replay"}
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "Session origin 'replay' is not supported"}


async def test_create_imported_session_without_provider(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 for an import without provider and external id."""
    agent_id = await create_agent(client)
    response = await client.post(
        "/v1/sessions",
        json={"agent_id": agent_id, "origin": "imported", "status": "completed"},
    )
    assert response.status_code == 422
    assert response.json() == {
        "detail": "Imported sessions require a provider and an external id"
    }


async def test_create_session_unknown_agent(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown agent id."""
    missing_id = uuid.uuid4()
    response = await client.post(
        "/v1/sessions", json={"agent_id": str(missing_id), "origin": "recorded"}
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Agent {missing_id} was not found"}


async def test_list_sessions(client: httpx.AsyncClient) -> None:
    """List sessions with filters and pagination."""
    agent_id = await create_agent(client)
    other_id = await create_agent(client, name="triage-bot")
    for name in ["one", "two", "three"]:
        await create_session(client, agent_id, name=name)
    await create_session(client, other_id, name="other")

    response = await client.get("/v1/sessions")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 4
    assert body["page"] == 1
    assert body["page_size"] == 20

    response = await client.get("/v1/sessions", params={"agent_id": agent_id})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert [item["name"] for item in body["items"]] == ["one", "two", "three"]

    response = await client.get(
        "/v1/sessions", params={"agent_id": agent_id, "page": 2, "page_size": 2}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert [item["name"] for item in body["items"]] == ["three"]

    response = await client.get("/v1/sessions", params={"name": "other"})
    assert response.status_code == 200
    assert response.json()["total"] == 1


async def test_list_sessions_by_tag(client: httpx.AsyncClient) -> None:
    """List sessions attached to a tag name."""
    agent_id = await create_agent(client)
    tagged = await create_session(client, agent_id)
    await create_session(client, agent_id)
    response = await client.post("/v1/tags", json={"name": "prod"})
    assert response.status_code == 201
    tag_id = response.json()["id"]
    response = await client.post(
        f"/v1/tags/{tag_id}/links",
        json={"resource_type": "session", "resource_id": tagged["id"]},
    )
    assert response.status_code == 201

    response = await client.get("/v1/sessions", params={"tag": "prod"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == tagged["id"]


async def test_list_sessions_naive_datetime(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a naive datetime filter."""
    response = await client.get(
        "/v1/sessions", params={"started_after": "2026-07-01T12:00:00"}
    )
    assert response.status_code == 422


async def test_get_session(client: httpx.AsyncClient) -> None:
    """Get a session by id."""
    agent_id = await create_agent(client)
    created = await create_session(client, agent_id)
    response = await client.get(f"/v1/sessions/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_session_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown session id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/sessions/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Session {missing_id} was not found"}


async def test_finish_session(client: httpx.AsyncClient) -> None:
    """Finish a session and observe the computed rollups."""
    agent_id = await create_agent(client)
    created = await create_session(client, agent_id)
    response = await client.post(
        f"/v1/sessions/{created['id']}/nodes",
        json={
            "nodes": [
                {
                    "id": str(uuid7()),
                    "sequence": 0,
                    "node_type": "llm_call",
                    "name": "chat",
                    "status": "completed",
                    "tokens": {"input_tokens": 100, "output_tokens": 20},
                    "cost": "0.5",
                },
                {
                    "id": str(uuid7()),
                    "sequence": 1,
                    "node_type": "tool_call",
                    "name": "get_weather",
                    "status": "completed",
                    "tool_name": "get_weather",
                    "inputs": {"city": "Berlin"},
                },
            ]
        },
    )
    assert response.status_code == 200

    response = await client.patch(
        f"/v1/sessions/{created['id']}",
        json={
            "status": "completed",
            "outputs": {"answer": "sunny"},
            "ended_at": "2026-07-01T12:05:00Z",
            "log_uri": "s3://logs/run-1",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"
    assert body["outputs"] == {"answer": "sunny"}
    assert body["log_uri"] == "s3://logs/run-1"
    assert body["cost"] == "0.5"
    assert body["tokens"] == {
        "input_tokens": 100,
        "output_tokens": 20,
        "cached_input_tokens": None,
        "reasoning_tokens": None,
    }
    assert body["llm_call_count"] == 1
    assert body["tool_call_count"] == 1


async def test_finish_terminal_session(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 when finishing a session that is not in progress."""
    agent_id = await create_agent(client)
    created = await create_session(client, agent_id)
    response = await client.patch(
        f"/v1/sessions/{created['id']}", json={"status": "completed"}
    )
    assert response.status_code == 200
    response = await client.patch(
        f"/v1/sessions/{created['id']}", json={"status": "failed"}
    )
    assert response.status_code == 409
    assert response.json() == {"detail": f"Session {created['id']} is not in progress"}


async def test_update_session_fields(client: httpx.AsyncClient) -> None:
    """Update name, expected, and metadata without finishing."""
    agent_id = await create_agent(client)
    created = await create_session(client, agent_id)
    response = await client.patch(
        f"/v1/sessions/{created['id']}",
        json={
            "name": "run-1",
            "expected": {"answer": "42"},
            "metadata": {"env": "prod"},
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "run-1"
    assert body["expected"] == {"answer": "42"}
    assert body["metadata"] == {"env": "prod"}
    assert body["status"] == "in_progress"


async def test_update_session_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown session id."""
    response = await client.patch(f"/v1/sessions/{uuid.uuid4()}", json={"name": "x"})
    assert response.status_code == 404


async def test_merge_scores(client: httpx.AsyncClient) -> None:
    """Merge score values with latest wins per scorer name."""
    agent_id = await create_agent(client)
    created = await create_session(client, agent_id)
    response = await client.post(
        f"/v1/sessions/{created['id']}/scores",
        json={"scores": {"conciseness": 0.5, "accuracy": 0.9}},
    )
    assert response.status_code == 200
    assert response.json()["scores"] == {"conciseness": 0.5, "accuracy": 0.9}

    response = await client.post(
        f"/v1/sessions/{created['id']}/scores",
        json={"scores": {"conciseness": 0.7}},
    )
    assert response.status_code == 200
    assert response.json()["scores"] == {"conciseness": 0.7, "accuracy": 0.9}

    response = await client.get("/v1/sessions", params={"has_score": True})
    assert response.status_code == 200
    assert response.json()["total"] == 1


async def test_merge_scores_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown session id."""
    response = await client.post(
        f"/v1/sessions/{uuid.uuid4()}/scores", json={"scores": {"a": 1.0}}
    )
    assert response.status_code == 404


async def test_delete_session(client: httpx.AsyncClient) -> None:
    """Delete a session with its nodes and observe HTTP 204."""
    agent_id = await create_agent(client)
    created = await create_session(client, agent_id)
    response = await client.post(
        f"/v1/sessions/{created['id']}/nodes",
        json={
            "nodes": [
                {
                    "id": str(uuid7()),
                    "sequence": 0,
                    "node_type": "span",
                    "name": "run",
                    "status": "completed",
                }
            ]
        },
    )
    assert response.status_code == 200

    response = await client.delete(f"/v1/sessions/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/sessions/{created['id']}")
    assert response.status_code == 404
    response = await client.get(f"/v1/sessions/{created['id']}/nodes")
    assert response.status_code == 404


async def test_delete_session_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown session id."""
    response = await client.delete(f"/v1/sessions/{uuid.uuid4()}")
    assert response.status_code == 404
