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

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeBlobRepository,
    FakeEvaluationRepository,
    FakeJobRepository,
    FakePluginRepository,
    FakeSessionRepository,
    FakeTaskRepository,
    create_plugin,
    create_session,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_evaluation_service,
    get_job_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.events import EventDispatcher
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.task import TaskPolicy
from kitaru.server.application.services.evaluation_service import EvaluationService
from kitaru.server.application.services.job_service import JobService
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.domain.account import Account
from kitaru.server.domain.plugin import PackagePluginSource, PluginKind

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
def plugin_repository() -> FakePluginRepository:
    """Provide the fake plugin repository backing the app."""
    return FakePluginRepository()


@pytest.fixture
async def client(
    evaluation_repository: FakeEvaluationRepository,
    session_repository: FakeSessionRepository,
    plugin_repository: FakePluginRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client with fake-backed evaluation and job services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    evaluation_service = EvaluationService(
        repository=evaluation_repository, session_repository=session_repository
    )
    agents = FakeAgentRepository()
    tasks = FakeTaskRepository(sessions=session_repository)
    jobs = FakeJobRepository(tasks=tasks)
    transitions = TaskTransitions(
        task_repository=tasks, job_repository=jobs, dispatcher=EventDispatcher()
    )
    job_service = JobService(
        repository=jobs,
        task_repository=tasks,
        session_repository=session_repository,
        agent_repository=agents,
        agent_version_repository=FakeAgentVersionRepository(agents),
        plugin_repository=plugin_repository,
        blob_repository=FakeBlobRepository(),
        transitions=transitions,
        policy=TaskPolicy(),
    )
    app.dependency_overrides[get_evaluation_service] = lambda: evaluation_service
    app.dependency_overrides[get_job_service] = lambda: job_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_task] = lambda: AuthContext(account=ACCOUNT)
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

    filter_expression = {"field": "session_id", "op": "eq", "value": session_id}
    response = await client.get(
        "/v1/evaluations", params={"filter": json.dumps(filter_expression)}
    )
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


async def test_create_evaluations(
    client: httpx.AsyncClient,
    session_repository: FakeSessionRepository,
    plugin_repository: FakePluginRepository,
) -> None:
    """Create a job holding one continue evaluator task per pair."""
    session_id = await _create_session(session_repository)
    plugin = await create_plugin(
        plugin_repository, ACCOUNT.id, PluginKind.EVALUATOR, name="scorer"
    )
    await plugin_repository.create_version(
        plugin.id,
        PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"),
        display_version=None,
    )

    response = await client.post(
        "/v1/evaluations",
        json={
            "input_session_ids": [session_id],
            "evaluators": [{"evaluator": "scorer"}],
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "pending"


async def test_create_evaluations_rejects_an_unknown_session(
    client: httpx.AsyncClient,
    plugin_repository: FakePluginRepository,
) -> None:
    """Observe HTTP 422 for an unknown input session."""
    plugin = await create_plugin(
        plugin_repository, ACCOUNT.id, PluginKind.EVALUATOR, name="scorer"
    )
    await plugin_repository.create_version(
        plugin.id,
        PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"),
        display_version=None,
    )

    response = await client.post(
        "/v1/evaluations",
        json={
            "input_session_ids": [str(uuid.uuid4())],
            "evaluators": [{"evaluator": "scorer"}],
        },
    )
    assert response.status_code == 422


async def test_create_evaluations_not_found_for_unknown_evaluator(
    client: httpx.AsyncClient, session_repository: FakeSessionRepository
) -> None:
    """Observe HTTP 404 for an unknown evaluator name."""
    session_id = await _create_session(session_repository)
    response = await client.post(
        "/v1/evaluations",
        json={
            "input_session_ids": [session_id],
            "evaluators": [{"evaluator": "does-not-exist"}],
        },
    )
    assert response.status_code == 404
