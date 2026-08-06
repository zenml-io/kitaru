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
"""Tests for the investigation and investigation session routes."""

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeInvestigationRepository,
    FakeSessionRepository,
    create_agent,
    create_session,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_investigation_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.investigation_service import (
    InvestigationService,
)
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide the fake agent repository backing the app."""
    return FakeAgentRepository()


@pytest.fixture
def session_repository() -> FakeSessionRepository:
    """Provide the fake session repository backing the app."""
    return FakeSessionRepository()


@pytest.fixture
def investigation_repository() -> FakeInvestigationRepository:
    """Provide the fake investigation repository backing the app."""
    return FakeInvestigationRepository()


@pytest.fixture
async def client(
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
    investigation_repository: FakeInvestigationRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed investigation services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    investigation_service = InvestigationService(
        repository=investigation_repository,
        agent_repository=agent_repository,
        session_repository=session_repository,
    )
    app.dependency_overrides[get_investigation_service] = lambda: investigation_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def agent_id(agent_repository: FakeAgentRepository) -> str:
    """Provide the id of an agent to own investigations."""
    agent = await create_agent(agent_repository, ACCOUNT.id)
    return str(agent.id)


@pytest.fixture
async def session_ids(
    session_repository: FakeSessionRepository, agent_id: str
) -> list[str]:
    """Provide two session ids belonging to the agent."""
    sessions = [
        await create_session(
            session_repository, ACCOUNT.id, agent_id=uuid.UUID(agent_id)
        )
        for _ in range(2)
    ]
    return [str(session.id) for session in sessions]


async def test_create_investigation(
    client: httpx.AsyncClient, agent_id: str, session_ids: list[str]
) -> None:
    """Create an investigation with its questions and sessions and observe HTTP 201."""
    response = await client.post(
        "/v1/investigations",
        json={
            "agent_id": agent_id,
            "name": "payment-failures",
            "description": "curator rationale",
            "questions": [{"key": "root_cause", "question": "What caused it?"}],
            "sessions": [
                {"session_id": session_ids[0]},
                {
                    "session_id": session_ids[1],
                    "view": {"summary": "Retry loop without backoff."},
                },
            ],
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "payment-failures"
    assert body["description"] == "curator rationale"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["agent_id"] == agent_id
    assert body["status"] == "pending"
    assert body["questions"] == [{"key": "root_cause", "question": "What caused it?"}]
    assert body["total_sessions"] == 2
    assert body["completed_sessions"] == 0
    assert body["started_at"] is None
    assert uuid.UUID(body["id"])


async def test_create_investigation_missing_agent(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when the agent does not exist."""
    response = await client.post(
        "/v1/investigations",
        json={
            "agent_id": str(uuid.uuid4()),
            "name": "investigation",
            "questions": [],
            "sessions": [],
        },
    )
    assert response.status_code == 404


async def test_create_investigation_duplicate_question_key(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Observe HTTP 422 for a duplicate question key."""
    response = await client.post(
        "/v1/investigations",
        json={
            "agent_id": agent_id,
            "name": "investigation",
            "questions": [
                {"key": "root_cause", "question": "What caused it?"},
                {"key": "root_cause", "question": "Again?"},
            ],
            "sessions": [],
        },
    )
    assert response.status_code == 422


async def test_create_investigation_missing_session(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Observe HTTP 422 when a linked session does not exist."""
    response = await client.post(
        "/v1/investigations",
        json={
            "agent_id": agent_id,
            "name": "investigation",
            "questions": [],
            "sessions": [{"session_id": str(uuid.uuid4())}],
        },
    )
    assert response.status_code == 422


async def test_create_investigation_duplicate_session_ids(
    client: httpx.AsyncClient, agent_id: str, session_ids: list[str]
) -> None:
    """Observe HTTP 422 when the session list repeats a session id."""
    response = await client.post(
        "/v1/investigations",
        json={
            "agent_id": agent_id,
            "name": "investigation",
            "questions": [],
            "sessions": [
                {"session_id": session_ids[0]},
                {"session_id": session_ids[0]},
            ],
        },
    )
    assert response.status_code == 422


async def test_get_investigation(client: httpx.AsyncClient, agent_id: str) -> None:
    """Get an investigation by id."""
    created = (
        await client.post(
            "/v1/investigations",
            json={
                "agent_id": agent_id,
                "name": "investigation",
                "questions": [],
                "sessions": [],
            },
        )
    ).json()
    response = await client.get(f"/v1/investigations/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_investigation_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing investigation."""
    response = await client.get(f"/v1/investigations/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_list_investigations(client: httpx.AsyncClient, agent_id: str) -> None:
    """List investigations newest-first with a status filter."""
    for name in ["alpha", "beta"]:
        await client.post(
            "/v1/investigations",
            json={
                "agent_id": agent_id,
                "name": name,
                "questions": [],
                "sessions": [],
            },
        )

    response = await client.get("/v1/investigations")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["name"] for item in body["items"]] == ["beta", "alpha"]

    filter_expression = {"field": "status", "op": "eq", "value": "completed"}
    response = await client.get(
        "/v1/investigations", params={"filter": json.dumps(filter_expression)}
    )
    assert response.json()["items"] == []


async def test_list_investigations_filters_by_agent_id(
    client: httpx.AsyncClient, agent_repository: FakeAgentRepository, agent_id: str
) -> None:
    """Filter investigations scoped to one agent."""
    await client.post(
        "/v1/investigations",
        json={
            "agent_id": agent_id,
            "name": "investigation",
            "questions": [],
            "sessions": [],
        },
    )
    other_agent = await create_agent(agent_repository, ACCOUNT.id, name="other")
    await client.post(
        "/v1/investigations",
        json={
            "agent_id": str(other_agent.id),
            "name": "other",
            "questions": [],
            "sessions": [],
        },
    )

    filter_expression = {"field": "agent_id", "op": "eq", "value": agent_id}
    response = await client.get(
        "/v1/investigations", params={"filter": json.dumps(filter_expression)}
    )
    body = response.json()
    assert len(body["items"]) == 1
    assert body["items"][0]["agent_id"] == agent_id


async def test_update_investigation(client: httpx.AsyncClient, agent_id: str) -> None:
    """Update an investigation's name and description."""
    created = (
        await client.post(
            "/v1/investigations",
            json={
                "agent_id": agent_id,
                "name": "investigation",
                "description": "old",
                "questions": [],
                "sessions": [],
            },
        )
    ).json()
    response = await client.patch(
        f"/v1/investigations/{created['id']}",
        json={"name": "renamed", "description": "new"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "renamed"
    assert body["description"] == "new"


async def test_update_investigation_cannot_clear_name(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Observe HTTP 422 when clearing the investigation name."""
    created = (
        await client.post(
            "/v1/investigations",
            json={
                "agent_id": agent_id,
                "name": "investigation",
                "questions": [],
                "sessions": [],
            },
        )
    ).json()
    response = await client.patch(
        f"/v1/investigations/{created['id']}", json={"name": None}
    )
    assert response.status_code == 422


async def test_update_investigation_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing investigation."""
    response = await client.patch(
        f"/v1/investigations/{uuid.uuid4()}", json={"description": "x"}
    )
    assert response.status_code == 404


async def test_delete_investigation(client: httpx.AsyncClient, agent_id: str) -> None:
    """Delete an investigation."""
    created = (
        await client.post(
            "/v1/investigations",
            json={
                "agent_id": agent_id,
                "name": "investigation",
                "questions": [],
                "sessions": [],
            },
        )
    ).json()
    response = await client.delete(f"/v1/investigations/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/investigations/{created['id']}")
    assert response.status_code == 404


async def test_delete_investigation_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing investigation."""
    response = await client.delete(f"/v1/investigations/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_list_investigation_sessions_ordered_by_position(
    client: httpx.AsyncClient, agent_id: str, session_ids: list[str]
) -> None:
    """List an investigation's sessions ordered by position ascending."""
    created = (
        await client.post(
            "/v1/investigations",
            json={
                "agent_id": agent_id,
                "name": "investigation",
                "questions": [],
                "sessions": [
                    {"session_id": session_ids[1]},
                    {"session_id": session_ids[0]},
                ],
            },
        )
    ).json()
    response = await client.get(f"/v1/investigations/{created['id']}/sessions")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["session_id"] for item in body["items"]] == [
        session_ids[1],
        session_ids[0],
    ]
    assert [item["position"] for item in body["items"]] == [0, 1]
    assert body["items"][0]["status"] == "pending"


async def test_list_investigation_sessions_walks_pages(
    client: httpx.AsyncClient, agent_id: str, session_repository: FakeSessionRepository
) -> None:
    """Walk every page of an investigation's sessions via next_cursor."""
    sessions = [
        await create_session(
            session_repository, ACCOUNT.id, agent_id=uuid.UUID(agent_id)
        )
        for _ in range(3)
    ]
    session_ids = [str(session.id) for session in sessions]
    created = (
        await client.post(
            "/v1/investigations",
            json={
                "agent_id": agent_id,
                "name": "investigation",
                "questions": [],
                "sessions": [{"session_id": session_id} for session_id in session_ids],
            },
        )
    ).json()

    collected: list[str] = []
    cursor = None
    while True:
        params = {"size": 1}
        if cursor is not None:
            params["cursor"] = cursor
        response = await client.get(
            f"/v1/investigations/{created['id']}/sessions", params=params
        )
        body = response.json()
        collected.extend(item["session_id"] for item in body["items"])
        if body["next_cursor"] is None:
            break
        cursor = body["next_cursor"]

    assert collected == session_ids


async def test_list_investigation_sessions_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing investigation."""
    response = await client.get(f"/v1/investigations/{uuid.uuid4()}/sessions")
    assert response.status_code == 404


async def test_update_investigation_session_status(
    client: httpx.AsyncClient, agent_id: str, session_ids: list[str]
) -> None:
    """Mark a linked session completed."""
    created = (
        await client.post(
            "/v1/investigations",
            json={
                "agent_id": agent_id,
                "name": "investigation",
                "questions": [],
                "sessions": [{"session_id": session_ids[0]}],
            },
        )
    ).json()
    response = await client.patch(
        f"/v1/investigations/{created['id']}/sessions/{session_ids[0]}",
        json={"status": "completed"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"

    investigation = (await client.get(f"/v1/investigations/{created['id']}")).json()
    assert investigation["status"] == "completed"
    assert investigation["completed_sessions"] == 1


async def test_update_investigation_session_status_investigation_not_found(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 404 when no investigation has this id."""
    response = await client.patch(
        f"/v1/investigations/{uuid.uuid4()}/sessions/{uuid.uuid4()}",
        json={"status": "completed"},
    )
    assert response.status_code == 404


async def test_update_investigation_session_status_session_not_found(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Observe HTTP 404 when no investigation session links this pair."""
    created = (
        await client.post(
            "/v1/investigations",
            json={
                "agent_id": agent_id,
                "name": "investigation",
                "questions": [],
                "sessions": [],
            },
        )
    ).json()
    response = await client.patch(
        f"/v1/investigations/{created['id']}/sessions/{uuid.uuid4()}",
        json={"status": "completed"},
    )
    assert response.status_code == 404


async def test_update_investigation_session_status_illegal_transition(
    client: httpx.AsyncClient, agent_id: str, session_ids: list[str]
) -> None:
    """Observe HTTP 409 when the linked session already settled."""
    created = (
        await client.post(
            "/v1/investigations",
            json={
                "agent_id": agent_id,
                "name": "investigation",
                "questions": [],
                "sessions": [{"session_id": session_ids[0]}],
            },
        )
    ).json()
    await client.patch(
        f"/v1/investigations/{created['id']}/sessions/{session_ids[0]}",
        json={"status": "completed"},
    )
    response = await client.patch(
        f"/v1/investigations/{created['id']}/sessions/{session_ids[0]}",
        json={"status": "skipped"},
    )
    assert response.status_code == 409
