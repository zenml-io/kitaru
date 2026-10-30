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
"""Tests for the analysis route."""

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.api_models.v1.task import TaskStatus
from kitaru.server.adapters.rest.dependencies import authorize, get_job_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.task import TaskPolicy
from kitaru.server.domain.account import Account
from kitaru.server.domain.plugin import PackagePluginSource, PluginKind
from kitaru.server.domain.task import AnalysisTask
from kitaru_test_support import (
    FakeSessionRepository,
    FakeTaskRepository,
    JobAndTaskServices,
    build_job_and_task_services,
    create_plugin,
    create_session,
    override_idempotency,
)

ACCOUNT = Account(id=uuid.uuid4(), name="ann")
AGENT_ID = uuid.uuid4()
START = datetime(2026, 10, 1, tzinfo=UTC)


@pytest.fixture
def policy() -> TaskPolicy:
    """Provide the task policy, overridden by tests that probe the cap."""
    return TaskPolicy()


@pytest.fixture
async def services(policy: TaskPolicy) -> JobAndTaskServices:
    """Provide fake-backed job and task services with one registered analyzer."""
    services = build_job_and_task_services(policy)
    plugin = await create_plugin(
        services.plugins, None, PluginKind.ANALYZER, name="trends"
    )
    await services.plugins.create_version(
        plugin.id,
        PackagePluginSource(requirement="kitaru-trends==1.0.0", entrypoint="pkg:run"),
        display_version=None,
    )
    return services


@pytest.fixture
def session_repository(services: JobAndTaskServices) -> FakeSessionRepository:
    """Provide the fake session repository backing the app."""
    return services.sessions


@pytest.fixture
def task_repository(services: JobAndTaskServices) -> FakeTaskRepository:
    """Provide the fake task repository backing the app."""
    return services.tasks


@pytest.fixture
async def client(
    services: JobAndTaskServices,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client with a fake-backed job service."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    app.dependency_overrides[get_job_service] = lambda: services.job_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def _create_session(
    repository: FakeSessionRepository,
    *,
    agent_id: uuid.UUID = AGENT_ID,
    status: SessionStatus = SessionStatus.COMPLETED,
    days: int = 0,
    import_id: uuid.UUID | None = None,
) -> uuid.UUID:
    session = await create_session(
        repository,
        ACCOUNT.id,
        agent_id=agent_id,
        status=status,
        started_at=START + timedelta(days=days),
        import_id=import_id,
        origin=SessionOrigin.RECORDED if import_id is None else SessionOrigin.IMPORTED,
    )
    return session.id


async def _get_analysis_tasks(
    task_repository: FakeTaskRepository, job_id: str
) -> list[AnalysisTask]:
    tasks = await task_repository.list_by_job(uuid.UUID(job_id))
    assert all(isinstance(task, AnalysisTask) for task in tasks)
    return [task for task in tasks if isinstance(task, AnalysisTask)]


async def test_create_analyses_fixes_the_agents_finished_sessions(
    client: httpx.AsyncClient,
    session_repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
) -> None:
    """Store the agent's finished sessions on the task, without an import."""
    finished = {await _create_session(session_repository, days=day) for day in range(3)}
    await _create_session(session_repository, status=SessionStatus.IN_PROGRESS)
    await _create_session(session_repository, agent_id=uuid.uuid4())

    response = await client.post(
        "/api/v1/analyses",
        json={
            "agent_id": str(AGENT_ID),
            "analyzers": [{"analyzer": "trends", "min_sessions": 1}],
        },
    )

    assert response.status_code == 201
    assert response.json()["kind"] == "analysis"
    (task,) = await _get_analysis_tasks(task_repository, response.json()["id"])
    assert task.agent_id == AGENT_ID
    assert task.import_id is None
    assert task.session_ids is not None
    assert set(task.session_ids) == finished
    assert task.status is TaskStatus.PENDING


async def test_create_analyses_combines_scope_fields(
    client: httpx.AsyncClient,
    session_repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
) -> None:
    """Keep only the sessions every scope field matches."""
    import_id = uuid.uuid4()
    early = await _create_session(session_repository, days=0, import_id=import_id)
    late = await _create_session(session_repository, days=5, import_id=import_id)
    live = await _create_session(session_repository, days=5)

    response = await client.post(
        "/api/v1/analyses",
        json={
            "agent_id": str(AGENT_ID),
            "analyzers": [{"analyzer": "trends", "min_sessions": 1}],
            "session_ids": [str(early), str(late), str(live)],
            "import_id": str(import_id),
            "started_after": (START + timedelta(days=2)).isoformat(),
        },
    )

    assert response.status_code == 201
    (task,) = await _get_analysis_tasks(task_repository, response.json()["id"])
    assert task.session_ids == [late]
    assert task.import_id == import_id


async def test_create_analyses_accepts_more_ids_than_one_filter_condition_holds(
    client: httpx.AsyncClient,
    session_repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
) -> None:
    """Select an explicit list longer than the filter's per-condition limit."""
    session_ids = {await _create_session(session_repository) for _ in range(150)}
    await _create_session(session_repository)

    response = await client.post(
        "/api/v1/analyses",
        json={
            "agent_id": str(AGENT_ID),
            "analyzers": [{"analyzer": "trends"}],
            "session_ids": [str(session_id) for session_id in session_ids],
        },
    )

    assert response.status_code == 201
    (task,) = await _get_analysis_tasks(task_repository, response.json()["id"])
    assert task.session_ids is not None
    assert set(task.session_ids) == session_ids


async def test_create_analyses_skips_below_the_session_minimum(
    client: httpx.AsyncClient,
    session_repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
) -> None:
    """Record the task as skipped and settle the job when too few sessions match."""
    await _create_session(session_repository)

    response = await client.post(
        "/api/v1/analyses",
        json={
            "agent_id": str(AGENT_ID),
            "analyzers": [{"analyzer": "trends", "min_sessions": 5}],
        },
    )

    assert response.status_code == 201
    assert response.json()["status"] != "pending"
    (task,) = await _get_analysis_tasks(task_repository, response.json()["id"])
    assert task.status is TaskStatus.SKIPPED


async def test_create_analyses_rejects_a_scope_without_sessions(
    client: httpx.AsyncClient, session_repository: FakeSessionRepository
) -> None:
    """Observe HTTP 422 when no finished session of the agent matches."""
    await _create_session(session_repository, status=SessionStatus.IN_PROGRESS)

    response = await client.post(
        "/api/v1/analyses",
        json={"agent_id": str(AGENT_ID), "analyzers": [{"analyzer": "trends"}]},
    )

    assert response.status_code == 422


@pytest.mark.parametrize("policy", [TaskPolicy(analysis_session_limit=1)])
async def test_create_analyses_rejects_more_sessions_than_the_cap(
    client: httpx.AsyncClient, session_repository: FakeSessionRepository
) -> None:
    """Observe HTTP 422 when the scope matches more sessions than the cap."""
    await _create_session(session_repository)
    await _create_session(session_repository)

    response = await client.post(
        "/api/v1/analyses",
        json={"agent_id": str(AGENT_ID), "analyzers": [{"analyzer": "trends"}]},
    )

    assert response.status_code == 422
    assert "cap is 1" in response.text


@pytest.mark.parametrize(
    "scope",
    [
        {"started_after": "2026-10-01T00:00:00"},
        {"session_ids": [str(uuid.UUID(int=1)), str(uuid.UUID(int=1))]},
        {"session_ids": []},
    ],
)
async def test_create_analyses_rejects_invalid_scope_fields(
    client: httpx.AsyncClient, scope: dict[str, object]
) -> None:
    """Observe HTTP 422 for naive timestamps, duplicate IDs, and empty lists."""
    response = await client.post(
        "/api/v1/analyses",
        json={
            "agent_id": str(AGENT_ID),
            "analyzers": [{"analyzer": "trends"}],
            **scope,
        },
    )

    assert response.status_code == 422
