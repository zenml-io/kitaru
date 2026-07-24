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
"""Tests for the cohort routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeCohortRepository,
    FakeReplayConfigRepository,
    FakeReplayRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    FakeTagRepository,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_agent_service,
    get_cohort_service,
    get_session_service,
    get_tag_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.cohort_service import CohortService
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed cohort services."""
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
    cohort_repository = FakeCohortRepository(
        session_repository, agent_repository, tag_repository
    )
    replay_repository = FakeReplayRepository(
        session_repository, version_repository, FakeReplayConfigRepository()
    )
    agent_service = AgentService(repository=agent_repository)
    session_service = SessionService(
        repository=session_repository,
        agent_repository=agent_repository,
        agent_version_repository=version_repository,
        node_repository=node_repository,
        replay_repository=replay_repository,
    )
    cohort_service = CohortService(
        repository=cohort_repository,
        session_repository=session_repository,
        agent_repository=agent_repository,
    )
    tag_service = TagService(repository=tag_repository)
    app.dependency_overrides[get_agent_service] = lambda: agent_service
    app.dependency_overrides[get_session_service] = lambda: session_service
    app.dependency_overrides[get_cohort_service] = lambda: cohort_service
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


async def create_completed_session(
    client: httpx.AsyncClient, agent_id: str, **overrides: object
) -> str:
    """Store a completed recorded session through the API.

    Args:
        client: HTTP client for the app.
        agent_id: Id of the agent.
        **overrides: Create request body overrides.

    Returns:
        Id of the created session.
    """
    body: dict[str, object] = {"agent_id": agent_id, "origin": "recorded", **overrides}
    response = await client.post("/v1/sessions", json=body)
    assert response.status_code == 201
    session_id = response.json()["id"]
    response = await client.patch(
        f"/v1/sessions/{session_id}", json={"status": "completed"}
    )
    assert response.status_code == 200
    return session_id


async def create_cohort(
    client: httpx.AsyncClient,
    agent_id: str,
    session_ids: list[str],
    **overrides: object,
) -> dict:
    """Store a cohort through the API.

    Args:
        client: HTTP client for the app.
        agent_id: Id of the agent.
        session_ids: Ids of the member sessions.
        **overrides: Create request body overrides.

    Returns:
        Created cohort body.
    """
    body: dict[str, object] = {
        "name": "baseline",
        "agent_id": agent_id,
        "session_ids": session_ids,
        **overrides,
    }
    response = await client.post("/v1/cohorts", json=body)
    assert response.status_code == 201
    return response.json()


async def test_create_cohort_from_session_ids(client: httpx.AsyncClient) -> None:
    """Create a cohort from explicit session ids and observe HTTP 201."""
    agent_id = await create_agent(client)
    first = await create_completed_session(client, agent_id)
    second = await create_completed_session(client, agent_id)
    response = await client.post(
        "/v1/cohorts",
        json={
            "name": "baseline",
            "description": "July sessions",
            "agent_id": agent_id,
            "session_ids": [first, second],
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "baseline"
    assert body["description"] == "July sessions"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["agent_id"] == agent_id
    assert body["session_count"] == 2
    assert body["created"] is not None
    assert body["updated"] is not None
    assert uuid.UUID(body["id"])


async def test_create_cohort_missing_agent_id(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a body without an agent id."""
    agent_id = await create_agent(client)
    session_id = await create_completed_session(client, agent_id)
    response = await client.post(
        "/v1/cohorts", json={"name": "baseline", "session_ids": [session_id]}
    )
    assert response.status_code == 422


async def test_create_cohort_missing_session_ids(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a body without session ids."""
    agent_id = await create_agent(client)
    response = await client.post(
        "/v1/cohorts", json={"name": "baseline", "agent_id": agent_id}
    )
    assert response.status_code == 422


async def test_create_cohort_agent_mismatch(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a member session of another agent."""
    agent_id = await create_agent(client)
    other_id = await create_agent(client, name="triage-bot")
    session_id = await create_completed_session(client, other_id)
    response = await client.post(
        "/v1/cohorts",
        json={"name": "baseline", "agent_id": agent_id, "session_ids": [session_id]},
    )
    assert response.status_code == 422
    assert response.json() == {
        "detail": f"Session {session_id} does not belong to agent {agent_id}"
    }


async def test_create_cohort_in_progress_member(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an in-progress member session."""
    agent_id = await create_agent(client)
    response = await client.post(
        "/v1/sessions", json={"agent_id": agent_id, "origin": "recorded"}
    )
    assert response.status_code == 201
    session_id = response.json()["id"]
    response = await client.post(
        "/v1/cohorts",
        json={"name": "baseline", "agent_id": agent_id, "session_ids": [session_id]},
    )
    assert response.status_code == 422
    assert response.json() == {"detail": f"Session {session_id} is in progress"}


async def test_create_empty_cohort(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an empty membership."""
    agent_id = await create_agent(client)
    response = await client.post(
        "/v1/cohorts",
        json={"name": "baseline", "agent_id": agent_id, "session_ids": []},
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "Cohort requires at least one session"}


async def test_create_cohort_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate cohort name."""
    agent_id = await create_agent(client)
    session_id = await create_completed_session(client, agent_id)
    await create_cohort(client, agent_id, [session_id])
    response = await client.post(
        "/v1/cohorts",
        json={"name": "baseline", "agent_id": agent_id, "session_ids": [session_id]},
    )
    assert response.status_code == 409
    assert response.json() == {"detail": "Cohort name 'baseline' is already registered"}


async def test_create_cohort_unknown_agent(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown agent id."""
    missing_id = uuid.uuid4()
    response = await client.post(
        "/v1/cohorts",
        json={
            "name": "baseline",
            "agent_id": str(missing_id),
            "session_ids": [str(uuid.uuid4())],
        },
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Agent {missing_id} was not found"}


async def test_create_cohort_unknown_session(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown session id."""
    agent_id = await create_agent(client)
    missing_id = uuid.uuid4()
    response = await client.post(
        "/v1/cohorts",
        json={
            "name": "baseline",
            "agent_id": agent_id,
            "session_ids": [str(missing_id)],
        },
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Session {missing_id} was not found"}


async def test_list_cohorts(client: httpx.AsyncClient) -> None:
    """List cohorts with filters and pagination."""
    agent_id = await create_agent(client)
    session_id = await create_completed_session(client, agent_id)
    for name in ["one", "two", "three"]:
        await create_cohort(client, agent_id, [session_id], name=name)

    response = await client.get("/v1/cohorts")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert body["page"] == 1
    assert body["page_size"] == 20
    assert [item["name"] for item in body["items"]] == ["one", "two", "three"]

    response = await client.get("/v1/cohorts", params={"page": 2, "page_size": 2})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert [item["name"] for item in body["items"]] == ["three"]

    response = await client.get("/v1/cohorts", params={"name": "two"})
    assert response.status_code == 200
    assert response.json()["total"] == 1


async def test_list_cohorts_by_tag(client: httpx.AsyncClient) -> None:
    """List cohorts attached to a tag name."""
    agent_id = await create_agent(client)
    session_id = await create_completed_session(client, agent_id)
    tagged = await create_cohort(client, agent_id, [session_id], name="tagged")
    await create_cohort(client, agent_id, [session_id], name="other")
    response = await client.post("/v1/tags", json={"name": "prod"})
    assert response.status_code == 201
    tag_id = response.json()["id"]
    response = await client.post(
        f"/v1/tags/{tag_id}/links",
        json={"resource_type": "cohort", "resource_id": tagged["id"]},
    )
    assert response.status_code == 201

    response = await client.get("/v1/cohorts", params={"tag": "prod"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == tagged["id"]


async def test_get_cohort(client: httpx.AsyncClient) -> None:
    """Get a cohort by id."""
    agent_id = await create_agent(client)
    session_id = await create_completed_session(client, agent_id)
    created = await create_cohort(client, agent_id, [session_id])
    response = await client.get(f"/v1/cohorts/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_cohort_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown cohort id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/cohorts/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Cohort {missing_id} was not found"}


async def test_list_cohort_sessions(client: httpx.AsyncClient) -> None:
    """List member sessions in position order with pagination."""
    agent_id = await create_agent(client)
    session_ids = [await create_completed_session(client, agent_id) for _ in range(3)]
    ordered = [session_ids[2], session_ids[0], session_ids[1]]
    created = await create_cohort(client, agent_id, ordered)

    response = await client.get(f"/v1/cohorts/{created['id']}/sessions")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert [item["id"] for item in body["items"]] == ordered

    response = await client.get(
        f"/v1/cohorts/{created['id']}/sessions", params={"page": 2, "page_size": 2}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert [item["id"] for item in body["items"]] == [ordered[2]]


async def test_list_cohort_sessions_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown cohort id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/cohorts/{missing_id}/sessions")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Cohort {missing_id} was not found"}


async def test_update_cohort(client: httpx.AsyncClient) -> None:
    """Update name and description."""
    agent_id = await create_agent(client)
    session_id = await create_completed_session(client, agent_id)
    created = await create_cohort(client, agent_id, [session_id])
    response = await client.patch(
        f"/v1/cohorts/{created['id']}",
        json={"name": "july", "description": "July sessions"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "july"
    assert body["description"] == "July sessions"
    assert body["session_count"] == 1


async def test_update_cohort_absent_fields_unchanged(client: httpx.AsyncClient) -> None:
    """Keep every field on an update with an empty body."""
    agent_id = await create_agent(client)
    session_id = await create_completed_session(client, agent_id)
    created = await create_cohort(client, agent_id, [session_id])
    response = await client.patch(f"/v1/cohorts/{created['id']}", json={})
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == created["name"]
    assert body["description"] == created["description"]


async def test_update_cohort_null_clears_description(
    client: httpx.AsyncClient,
) -> None:
    """Clear the description on an explicit null."""
    agent_id = await create_agent(client)
    session_id = await create_completed_session(client, agent_id)
    created = await create_cohort(client, agent_id, [session_id])
    response = await client.patch(
        f"/v1/cohorts/{created['id']}", json={"description": "July sessions"}
    )
    assert response.status_code == 200
    response = await client.patch(
        f"/v1/cohorts/{created['id']}", json={"description": None}
    )
    assert response.status_code == 200
    assert response.json()["description"] is None


async def test_update_cohort_null_name_rejected(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an explicit null name."""
    agent_id = await create_agent(client)
    session_id = await create_completed_session(client, agent_id)
    created = await create_cohort(client, agent_id, [session_id])
    response = await client.patch(f"/v1/cohorts/{created['id']}", json={"name": None})
    assert response.status_code == 422
    assert response.json() == {"detail": "Cohort name cannot be null"}


async def test_update_cohort_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate name on update."""
    agent_id = await create_agent(client)
    session_id = await create_completed_session(client, agent_id)
    await create_cohort(client, agent_id, [session_id])
    other = await create_cohort(client, agent_id, [session_id], name="other")
    response = await client.patch(
        f"/v1/cohorts/{other['id']}", json={"name": "baseline"}
    )
    assert response.status_code == 409
    assert response.json() == {"detail": "Cohort name 'baseline' is already registered"}


async def test_update_cohort_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown cohort id."""
    response = await client.patch(f"/v1/cohorts/{uuid.uuid4()}", json={"name": "x"})
    assert response.status_code == 404


async def test_delete_cohort(client: httpx.AsyncClient) -> None:
    """Delete a cohort and observe HTTP 204."""
    agent_id = await create_agent(client)
    session_id = await create_completed_session(client, agent_id)
    created = await create_cohort(client, agent_id, [session_id])
    response = await client.delete(f"/v1/cohorts/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/cohorts/{created['id']}")
    assert response.status_code == 404


async def test_delete_cohort_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown cohort id."""
    response = await client.delete(f"/v1/cohorts/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_delete_session_in_cohort(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 when deleting a session that is in a cohort."""
    agent_id = await create_agent(client)
    session_id = await create_completed_session(client, agent_id)
    created = await create_cohort(client, agent_id, [session_id])

    response = await client.delete(f"/v1/sessions/{session_id}")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Session {session_id} is referenced by cohorts"
    }

    response = await client.delete(f"/v1/cohorts/{created['id']}")
    assert response.status_code == 204
    response = await client.delete(f"/v1/sessions/{session_id}")
    assert response.status_code == 204
