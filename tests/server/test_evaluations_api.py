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
"""Tests for the evaluation routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import FakeEvaluationRepository, FakeSessionRepository, create_session
from kitaru.server.adapters.rest.dependencies import authorize, get_evaluation_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.evaluation_service import EvaluationService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def session_repository() -> FakeSessionRepository:
    """Provide the fake session repository backing the app."""
    return FakeSessionRepository()


@pytest.fixture
def evaluation_repository() -> FakeEvaluationRepository:
    """Provide the fake evaluation repository backing the app."""
    return FakeEvaluationRepository()


@pytest.fixture
async def client(
    evaluation_repository: FakeEvaluationRepository,
    session_repository: FakeSessionRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed evaluation service."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    service = EvaluationService(
        repository=evaluation_repository, session_repository=session_repository
    )
    app.dependency_overrides[get_evaluation_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def _create_session(session_repository: FakeSessionRepository) -> str:
    session = await create_session(
        session_repository, ACCOUNT.id, agent_id=uuid.uuid4()
    )
    return str(session.id)


async def test_list_evaluations(
    client: httpx.AsyncClient, session_repository: FakeSessionRepository
) -> None:
    """List evaluations newest-first with a session_id filter."""
    session_id = await _create_session(session_repository)
    other_session_id = await _create_session(session_repository)
    await client.post(
        f"/v1/sessions/{session_id}/evaluations",
        json={"evaluations": [{"name": "accuracy", "score": 0.9}]},
    )
    await client.post(
        f"/v1/sessions/{other_session_id}/evaluations",
        json={"evaluations": [{"name": "accuracy", "score": 0.5}]},
    )

    response = await client.get("/v1/evaluations")
    assert response.status_code == 200
    assert len(response.json()["items"]) == 2

    response = await client.get("/v1/evaluations", params={"session_id": session_id})
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["session_id"] == session_id


async def test_get_evaluation(
    client: httpx.AsyncClient, session_repository: FakeSessionRepository
) -> None:
    """Get an evaluation by id."""
    session_id = await _create_session(session_repository)
    created = (
        await client.post(
            f"/v1/sessions/{session_id}/evaluations",
            json={"evaluations": [{"name": "accuracy", "score": 0.9}]},
        )
    ).json()[0]
    response = await client.get(f"/v1/evaluations/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_evaluation_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing evaluation."""
    response = await client.get(f"/v1/evaluations/{uuid.uuid4()}")
    assert response.status_code == 404
