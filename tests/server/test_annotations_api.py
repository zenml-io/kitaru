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
"""Tests for the annotation routes."""

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeAnnotationRepository,
    FakeInvestigationRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    create_agent,
    create_session,
)
from kitaru.api_models.v1.investigation import InvestigationSessionQuestion
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_annotation_service,
    get_investigation_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.annotation_service import AnnotationService
from kitaru.server.application.services.investigation_service import (
    InvestigationService,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.investigation import Investigation, InvestigationSession

ACCOUNT = Account(id=uuid.uuid4(), name="ann")
QUESTION_KEY = "cause"


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide the fake agent repository backing the app."""
    return FakeAgentRepository()


@pytest.fixture
def session_repository() -> FakeSessionRepository:
    """Provide the fake session repository backing the app."""
    return FakeSessionRepository()


@pytest.fixture
def session_node_repository(
    session_repository: FakeSessionRepository,
) -> FakeSessionNodeRepository:
    """Provide the fake session node repository backing the app."""
    return FakeSessionNodeRepository(sessions=session_repository)


@pytest.fixture
def investigation_repository() -> FakeInvestigationRepository:
    """Provide the fake investigation repository backing the app."""
    return FakeInvestigationRepository()


@pytest.fixture
def annotation_repository(
    investigation_repository: FakeInvestigationRepository,
) -> FakeAnnotationRepository:
    """Provide the fake annotation repository backing the app."""
    return FakeAnnotationRepository(investigations=investigation_repository)


@pytest.fixture
async def client(
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
    session_node_repository: FakeSessionNodeRepository,
    investigation_repository: FakeInvestigationRepository,
    annotation_repository: FakeAnnotationRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed annotation services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    annotation_service = AnnotationService(
        repository=annotation_repository,
        investigation_repository=investigation_repository,
        session_repository=session_repository,
        session_node_repository=session_node_repository,
    )
    investigation_service = InvestigationService(
        repository=investigation_repository,
        agent_repository=agent_repository,
        session_repository=session_repository,
    )
    app.dependency_overrides[get_annotation_service] = lambda: annotation_service
    app.dependency_overrides[get_investigation_service] = lambda: investigation_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def agent_id(agent_repository: FakeAgentRepository) -> uuid.UUID:
    """Provide an agent id owned by the actor."""
    agent = await create_agent(agent_repository, ACCOUNT.id)
    return agent.id


@pytest.fixture
async def session_id(
    session_repository: FakeSessionRepository, agent_id: uuid.UUID
) -> str:
    """Provide a session id belonging to the agent."""
    session = await create_session(session_repository, ACCOUNT.id, agent_id=agent_id)
    return str(session.id)


@pytest.fixture
async def investigation_session_ids(
    investigation_repository: FakeInvestigationRepository,
    agent_id: uuid.UUID,
    session_id: str,
) -> tuple[str, str]:
    """Provide an investigation id and the id of its single session link."""
    investigation = Investigation(
        owner_id=ACCOUNT.id,
        agent_id=agent_id,
        name="investigation",
        total_sessions=0,
        completed_sessions=0,
    )
    created = await investigation_repository.create(
        investigation,
        [
            InvestigationSession(
                investigation_id=investigation.id,
                session_id=uuid.UUID(session_id),
                position=0,
                questions=[
                    InvestigationSessionQuestion(
                        key=QUESTION_KEY, question="What caused it?"
                    )
                ],
            )
        ],
    )
    linked = await investigation_repository.get_session_by_session_id(
        created.id, uuid.UUID(session_id)
    )
    return str(created.id), str(linked.id)


async def test_create_manual_annotation(
    client: httpx.AsyncClient, session_id: str
) -> None:
    """Create a manual annotation and observe HTTP 201."""
    response = await client.post(
        "/v1/annotations",
        json={
            "session_id": session_id,
            "value": "note",
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["session_id"] == session_id
    assert body["investigation_session_id"] is None
    assert body["value"] == "note"
    assert uuid.UUID(body["id"])


async def test_create_manual_annotation_missing_session(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 404 when the session does not exist."""
    response = await client.post(
        "/v1/annotations",
        json={
            "session_id": str(uuid.uuid4()),
            "value": "note",
        },
    )
    assert response.status_code == 404


async def test_create_manual_annotation_invalid_selector_node(
    client: httpx.AsyncClient, session_id: str
) -> None:
    """Observe HTTP 422 when the selector names a node outside the session."""
    response = await client.post(
        "/v1/annotations",
        json={
            "session_id": session_id,
            "selector": {"node_id": str(uuid.uuid4())},
            "value": "note",
        },
    )
    assert response.status_code == 422


async def test_create_investigation_answer(
    client: httpx.AsyncClient, investigation_session_ids: tuple[str, str]
) -> None:
    """Answer an investigation's linked session and observe HTTP 201."""
    _, investigation_session_id = investigation_session_ids
    response = await client.post(
        "/v1/annotations",
        json={
            "investigation_session_id": investigation_session_id,
            "question_key": QUESTION_KEY,
            "value": "a retry loop",
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["investigation_session_id"] == investigation_session_id
    assert body["question_key"] == QUESTION_KEY
    assert body["value"] == "a retry loop"


async def test_create_investigation_answer_never_conflicts(
    client: httpx.AsyncClient, investigation_session_ids: tuple[str, str]
) -> None:
    """Create a separate annotation for each answer to the same session."""
    _, investigation_session_id = investigation_session_ids
    first = (
        await client.post(
            "/v1/annotations",
            json={
                "investigation_session_id": investigation_session_id,
                "question_key": QUESTION_KEY,
                "value": "first answer",
            },
        )
    ).json()
    second = (
        await client.post(
            "/v1/annotations",
            json={
                "investigation_session_id": investigation_session_id,
                "question_key": QUESTION_KEY,
                "value": "second answer",
            },
        )
    ).json()
    assert second["id"] != first["id"]


async def test_create_investigation_answer_moves_investigation_in_progress(
    client: httpx.AsyncClient, investigation_session_ids: tuple[str, str]
) -> None:
    """Move the investigation from pending to in_progress on the first answer."""
    investigation_id, investigation_session_id = investigation_session_ids
    await client.post(
        "/v1/annotations",
        json={
            "investigation_session_id": investigation_session_id,
            "question_key": QUESTION_KEY,
            "value": "a retry loop",
        },
    )
    investigation = (await client.get(f"/v1/investigations/{investigation_id}")).json()
    assert investigation["status"] == "in_progress"
    assert investigation["started_at"] is not None


async def test_create_investigation_answer_missing_link(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 404 when no investigation session has the given id."""
    response = await client.post(
        "/v1/annotations",
        json={
            "investigation_session_id": str(uuid.uuid4()),
            "question_key": QUESTION_KEY,
            "value": "x",
        },
    )
    assert response.status_code == 404


async def test_create_investigation_answer_unknown_question_key(
    client: httpx.AsyncClient, investigation_session_ids: tuple[str, str]
) -> None:
    """Observe HTTP 422 when the question key does not belong to the session."""
    _, investigation_session_id = investigation_session_ids
    response = await client.post(
        "/v1/annotations",
        json={
            "investigation_session_id": investigation_session_id,
            "question_key": "unknown",
            "value": "x",
        },
    )
    assert response.status_code == 422


async def test_create_annotation_malformed_body(
    client: httpx.AsyncClient, session_id: str
) -> None:
    """Observe HTTP 422 for a body mixing manual and investigation answer fields."""
    response = await client.post(
        "/v1/annotations",
        json={
            "session_id": session_id,
            "investigation_session_id": str(uuid.uuid4()),
            "question_key": QUESTION_KEY,
            "value": "x",
        },
    )
    assert response.status_code == 422


async def test_get_annotation(client: httpx.AsyncClient, session_id: str) -> None:
    """Get an annotation by id."""
    created = (
        await client.post(
            "/v1/annotations",
            json={
                "session_id": session_id,
                "value": "note",
            },
        )
    ).json()
    response = await client.get(f"/v1/annotations/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_annotation_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing annotation."""
    response = await client.get(f"/v1/annotations/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_list_annotations_filters_by_session_id(
    client: httpx.AsyncClient,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
    session_id: str,
) -> None:
    """Filter annotations scoped to one session."""
    other_session = await create_session(
        session_repository, ACCOUNT.id, agent_id=agent_id
    )
    matching = (
        await client.post(
            "/v1/annotations",
            json={
                "session_id": session_id,
                "value": "note",
            },
        )
    ).json()
    await client.post(
        "/v1/annotations",
        json={
            "session_id": str(other_session.id),
            "value": "other",
        },
    )

    filter_expression = {"field": "session_id", "op": "eq", "value": session_id}
    response = await client.get(
        "/v1/annotations", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    body = response.json()
    assert [item["id"] for item in body["items"]] == [matching["id"]]


async def test_list_annotations_filters_by_investigation_id(
    client: httpx.AsyncClient, investigation_session_ids: tuple[str, str]
) -> None:
    """Filter annotations scoped to one investigation through the session link."""
    investigation_id, investigation_session_id = investigation_session_ids
    answer = (
        await client.post(
            "/v1/annotations",
            json={
                "investigation_session_id": investigation_session_id,
                "question_key": QUESTION_KEY,
                "value": "a retry loop",
            },
        )
    ).json()

    filter_expression = {
        "field": "investigation_id",
        "op": "eq",
        "value": investigation_id,
    }
    response = await client.get(
        "/v1/annotations", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    body = response.json()
    assert [item["id"] for item in body["items"]] == [answer["id"]]


async def test_update_annotation(client: httpx.AsyncClient, session_id: str) -> None:
    """Set a new value on an annotation."""
    created = (
        await client.post(
            "/v1/annotations",
            json={
                "session_id": session_id,
                "value": "note",
            },
        )
    ).json()
    response = await client.patch(
        f"/v1/annotations/{created['id']}",
        json={"value": True},
    )
    assert response.status_code == 200
    assert response.json()["value"] is True


async def test_update_annotation_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing annotation."""
    response = await client.patch(
        f"/v1/annotations/{uuid.uuid4()}",
        json={"value": "x"},
    )
    assert response.status_code == 404


async def test_delete_annotation(client: httpx.AsyncClient, session_id: str) -> None:
    """Delete an annotation."""
    created = (
        await client.post(
            "/v1/annotations",
            json={
                "session_id": session_id,
                "value": "note",
            },
        )
    ).json()
    response = await client.delete(f"/v1/annotations/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/annotations/{created['id']}")
    assert response.status_code == 404


async def test_delete_annotation_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing annotation."""
    response = await client.delete(f"/v1/annotations/{uuid.uuid4()}")
    assert response.status_code == 404
