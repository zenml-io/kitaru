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
"""Round-trip tests for the evaluations SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeBlobRepository,
    FakeEvaluationRepository,
    FakeJobRepository,
    FakePluginRepository,
    FakeReplayRepository,
    FakeSessionRepository,
    FakeTaskRepository,
    asgi_api_client,
    build_payload_store,
    create_plugin,
    override_idempotency,
)
from kitaru.api_models.v1.evaluation import (
    EvaluationBatchCreateRequest,
    EvaluationListParams,
    EvaluationResult,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.job import JobResponse
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionEvaluationsRequest,
    SessionOrigin,
    SessionStatus,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_evaluation_service,
    get_job_service,
    get_session_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.events import EventDispatcher
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.task import TaskPolicy
from kitaru.server.application.services.evaluation_service import EvaluationService
from kitaru.server.application.services.job_service import JobService
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.domain.account import Account
from kitaru.server.domain.plugin import PackagePluginSource, PluginKind

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def plugin_repository() -> FakePluginRepository:
    """Provide the fake plugin repository backing the app."""
    return FakePluginRepository()


@pytest.fixture
async def api_client(
    plugin_repository: FakePluginRepository,
) -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    session_repository = FakeSessionRepository()
    evaluation_repository = FakeEvaluationRepository()
    agents = FakeAgentRepository()
    agent_versions = FakeAgentVersionRepository(agents)
    session_service = SessionService(
        repository=session_repository,
        task_repository=FakeTaskRepository(),
        agent_version_repository=agent_versions,
        replay_repository=FakeReplayRepository(),
        payload_store=build_payload_store().store,
    )
    evaluation_service = EvaluationService(
        repository=evaluation_repository, session_repository=session_repository
    )
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
        agent_version_repository=agent_versions,
        plugin_repository=plugin_repository,
        blob_repository=FakeBlobRepository(),
        transitions=transitions,
        policy=TaskPolicy(),
    )
    app.dependency_overrides[get_session_service] = lambda: session_service
    app.dependency_overrides[get_evaluation_service] = lambda: evaluation_service
    app.dependency_overrides[get_job_service] = lambda: job_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_task] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def _create_session(
    api_client: KitaruAPIClient, status: SessionStatus = SessionStatus.COMPLETED
) -> uuid.UUID:
    session = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            status=status,
            inputs=None,
            outputs=None,
            metadata={},
        )
    )
    return session.id


async def test_create_evaluations(api_client: KitaruAPIClient) -> None:
    """Create manual evaluations on a session through the SDK."""
    session_id = await _create_session(api_client)
    stored = await api_client.sessions.create_evaluations(
        session_id,
        SessionEvaluationsRequest(
            evaluations=[
                EvaluationResult(name="accuracy", score=0.9),
                EvaluationResult(name="verdict", value="good"),
            ]
        ),
    )
    assert [item.name for item in stored] == ["accuracy", "verdict"]
    assert stored[0].score == 0.9
    assert stored[0].evaluator_version_id is None
    assert stored[1].value == "good"


async def test_create_evaluations_passed(api_client: KitaruAPIClient) -> None:
    """Round-trip the optional pass flag through the SDK."""
    session_id = await _create_session(api_client)
    stored = await api_client.sessions.create_evaluations(
        session_id,
        SessionEvaluationsRequest(
            evaluations=[
                EvaluationResult(name="accuracy", score=0.9, passed=True),
                EvaluationResult(name="verdict", value="bad", passed=False),
                EvaluationResult(name="latency", score=1.0),
            ]
        ),
    )
    assert [item.passed for item in stored] == [True, False, None]


async def test_create_evaluations_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.sessions.create_evaluations(
            uuid.uuid4(),
            SessionEvaluationsRequest(
                evaluations=[EvaluationResult(name="accuracy", score=0.9)]
            ),
        )


async def test_create_evaluations_duplicate_name_in_batch(
    api_client: KitaruAPIClient,
) -> None:
    """Surface HTTP 422 for a request naming the same evaluation twice."""
    session_id = await _create_session(api_client)
    with pytest.raises(APIError) as exc_info:
        await api_client.sessions.create_evaluations(
            session_id,
            SessionEvaluationsRequest(
                evaluations=[
                    EvaluationResult(name="accuracy", score=0.9),
                    EvaluationResult(name="accuracy", score=0.1),
                ]
            ),
        )
    assert exc_info.value.status_code == 422


async def test_create_evaluations_name_conflict(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 for a name that already exists on the session."""
    session_id = await _create_session(api_client)
    await api_client.sessions.create_evaluations(
        session_id,
        SessionEvaluationsRequest(
            evaluations=[EvaluationResult(name="accuracy", score=0.9)]
        ),
    )
    with pytest.raises(APIError) as exc_info:
        await api_client.sessions.create_evaluations(
            session_id,
            SessionEvaluationsRequest(
                evaluations=[EvaluationResult(name="accuracy", score=0.1)]
            ),
        )
    assert exc_info.value.status_code == 409


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get an evaluation by id through the SDK."""
    session_id = await _create_session(api_client)
    stored = await api_client.sessions.create_evaluations(
        session_id,
        SessionEvaluationsRequest(
            evaluations=[EvaluationResult(name="accuracy", score=0.9)]
        ),
    )
    loaded = await api_client.evaluations.get(stored[0].id)
    assert loaded == stored[0]


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.evaluations.get(uuid.uuid4())


async def test_list_and_iter(api_client: KitaruAPIClient) -> None:
    """List and iterate evaluations through the SDK."""
    session_id = await _create_session(api_client)
    await api_client.sessions.create_evaluations(
        session_id,
        SessionEvaluationsRequest(
            evaluations=[
                EvaluationResult(name="a", score=1.0),
                EvaluationResult(name="b", score=2.0),
                EvaluationResult(name="c", score=3.0),
            ]
        ),
    )

    session_filter = FilterCondition(
        field="session_id", op=FilterOp.EQ, value=session_id
    )
    page = await api_client.evaluations.list(
        EvaluationListParams(filter=session_filter, size=2)
    )
    assert len(page.items) == 2
    assert page.next_cursor is not None

    collected = [
        item.id
        async for item in api_client.evaluations.iter(
            EvaluationListParams(filter=session_filter, size=2)
        )
    ]
    assert len(collected) == 3


async def test_create(
    api_client: KitaruAPIClient, plugin_repository: FakePluginRepository
) -> None:
    """Create a job holding one continue evaluator task per pair through the SDK."""
    session_id = await _create_session(api_client)
    plugin = await create_plugin(
        plugin_repository, ACCOUNT.id, PluginKind.EVALUATOR, name="scorer"
    )
    await plugin_repository.create_version(
        plugin.id,
        PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"),
        display_version=None,
    )

    job = await api_client.evaluations.create(
        EvaluationBatchCreateRequest(
            input_session_ids=[session_id],
            evaluators=[{"evaluator": "scorer"}],
        )
    )
    assert isinstance(job, JobResponse)
    assert job.status.value == "pending"


async def test_create_not_found_for_unknown_evaluator(
    api_client: KitaruAPIClient,
) -> None:
    """Surface HTTP 404 as a typed error for an unknown evaluator name."""
    session_id = await _create_session(api_client)
    with pytest.raises(NotFoundError):
        await api_client.evaluations.create(
            EvaluationBatchCreateRequest(
                input_session_ids=[session_id],
                evaluators=[{"evaluator": "does-not-exist"}],
            )
        )
