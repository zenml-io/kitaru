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
"""Round-trip tests for the experiment runs SDK resource, and experiments.start_run."""

import uuid
from collections.abc import AsyncGenerator
from functools import partial

import pytest

from conftest import (
    ReplayServices,
    asgi_api_client,
    build_replay_services,
    create_agent,
    create_agent_version,
    create_blob,
    create_cohort,
    create_cohort_version,
    create_plugin,
    create_session,
)
from kitaru.api_models.v1.experiment import ExperimentCreateRequest
from kitaru.api_models.v1.experiment_run import (
    ExperimentRunCreateRequest,
    ExperimentRunJobsListParams,
    ExperimentRunListParams,
    ExperimentRunResponse,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.job import JobResponse
from kitaru.api_models.v1.replay_config import EvaluatorConfig
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_experiment_run_service,
    get_experiment_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.api.run_cancellation import get_run_canceler
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.replay import ReplayStatusCounts
from kitaru.server.application.services.experiment_run_service import (
    ExperimentRunService,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import RunSpec
from kitaru.server.domain.experiment_run import ExperimentRun
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def services() -> ReplayServices:
    """Provide fake-backed experiment, replay, and run services."""
    return build_replay_services()


async def _cancel_run(
    service: ExperimentRunService, experiment_run_id: uuid.UUID, actor: AuthContext
) -> tuple[ExperimentRun, ReplayStatusCounts]:
    """Drive both cancellation phases against one fake-backed service."""
    await service.mark_run_canceling(experiment_run_id, actor=actor)
    return await service.cancel_run_jobs(experiment_run_id, actor=actor)


@pytest.fixture
async def api_client(services: ReplayServices) -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    app.dependency_overrides[get_experiment_service] = lambda: (
        services.experiment_service
    )
    app.dependency_overrides[get_experiment_run_service] = lambda: (
        services.experiment_run_service
    )
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[get_run_canceler] = lambda: partial(
        _cancel_run, services.experiment_run_service
    )
    async with asgi_api_client(app) as client:
        yield client


@pytest.fixture
async def agent_id(services: ReplayServices) -> uuid.UUID:
    """Provide an agent shared by the experiment and its runs."""
    agent = await create_agent(services.agents, ACCOUNT.id)
    return agent.id


@pytest.fixture
async def run_request(
    services: ReplayServices, agent_id: uuid.UUID
) -> ExperimentRunCreateRequest:
    """Provide a run request naming a non-empty cohort version and a runnable
    version."""
    version = await create_agent_version(
        services.agent_versions,
        agent_id=agent_id,
        owner_id=ACCOUNT.id,
        run_spec=RunSpec(command="run.sh"),
    )
    session = await create_session(
        services.sessions,
        ACCOUNT.id,
        agent_id=agent_id,
        agent_version_id=version.id,
        origin=SessionOrigin.RECORDED,
    )
    cohort = await create_cohort(services.cohorts, ACCOUNT.id, agent_id)
    cohort_version = await create_cohort_version(
        services.cohort_versions, ACCOUNT.id, cohort.id, [session.id]
    )
    return ExperimentRunCreateRequest(
        cohort_version_id=cohort_version.id, agent_version_id=version.id
    )


@pytest.fixture
async def experiment_id(
    api_client: KitaruAPIClient, services: ReplayServices, agent_id: uuid.UUID
) -> uuid.UUID:
    """Provide the id of an experiment with a registered evaluator."""
    plugin = await create_plugin(
        services.plugins, ACCOUNT.id, kind=PluginKind.EVALUATOR, name="accuracy"
    )
    blob = await create_blob(services.blobs, ACCOUNT.id, content=b"score")
    await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=blob.id, entrypoint="score"),
        display_version=None,
    )
    experiment = await api_client.experiments.create(
        ExperimentCreateRequest(
            name="exp1",
            agent_id=agent_id,
            evaluators=[EvaluatorConfig(evaluator="accuracy")],
        )
    )
    return experiment.id


async def test_start_run(
    api_client: KitaruAPIClient,
    experiment_id: uuid.UUID,
    run_request: ExperimentRunCreateRequest,
) -> None:
    """Start a run through the experiments resource."""
    run = await api_client.experiments.start_run(experiment_id, run_request)
    assert isinstance(run, ExperimentRunResponse)
    assert run.experiment_id == experiment_id
    assert run.number == 1
    assert run.progress.total == 1
    assert run.progress.pending == 1


async def test_start_run_rejects_empty_cohort_version(
    api_client: KitaruAPIClient, experiment_id: uuid.UUID, services: ReplayServices
) -> None:
    """Surface HTTP 422 as a typed error for an empty cohort version."""
    empty_cohort = await create_cohort(
        services.cohorts, ACCOUNT.id, uuid.uuid4(), name="empty-cohort"
    )
    empty_version = await create_cohort_version(
        services.cohort_versions, ACCOUNT.id, empty_cohort.id, []
    )
    with pytest.raises(APIError) as excinfo:
        await api_client.experiments.start_run(
            experiment_id,
            ExperimentRunCreateRequest(
                cohort_version_id=empty_version.id, agent_version_id=uuid.uuid4()
            ),
        )
    assert excinfo.value.status_code == 422


async def test_get(
    api_client: KitaruAPIClient,
    experiment_id: uuid.UUID,
    run_request: ExperimentRunCreateRequest,
) -> None:
    """Get an experiment run by id through the SDK."""
    created = await api_client.experiments.start_run(experiment_id, run_request)
    loaded = await api_client.experiment_runs.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.experiment_runs.get(uuid.uuid4())


async def test_list_and_iter(
    api_client: KitaruAPIClient,
    experiment_id: uuid.UUID,
    run_request: ExperimentRunCreateRequest,
) -> None:
    """List and iterate experiment runs through the SDK."""
    await api_client.experiments.start_run(experiment_id, run_request)
    page = await api_client.experiment_runs.list(
        ExperimentRunListParams(
            filter=FilterCondition(
                field="experiment_id", op=FilterOp.EQ, value=experiment_id
            )
        )
    )
    assert len(page.items) == 1

    collected = [item async for item in api_client.experiment_runs.iter()]
    assert len(collected) == 1


async def test_list_and_iter_jobs(
    api_client: KitaruAPIClient,
    experiment_id: uuid.UUID,
    run_request: ExperimentRunCreateRequest,
) -> None:
    """List and iterate a run's jobs through the SDK."""
    created = await api_client.experiments.start_run(experiment_id, run_request)
    page = await api_client.experiment_runs.list_jobs(created.id)
    assert len(page.items) == 1
    assert isinstance(page.items[0], JobResponse)

    collected = [
        item
        async for item in api_client.experiment_runs.iter_jobs(
            created.id, ExperimentRunJobsListParams(size=1)
        )
    ]
    assert len(collected) == 1


async def test_cancel(
    api_client: KitaruAPIClient,
    experiment_id: uuid.UUID,
    run_request: ExperimentRunCreateRequest,
) -> None:
    """Cancel a run through the SDK."""
    created = await api_client.experiments.start_run(experiment_id, run_request)
    canceled = await api_client.experiment_runs.cancel(created.id)
    assert canceled.status.value == "canceled"


async def test_delete(
    api_client: KitaruAPIClient,
    experiment_id: uuid.UUID,
    run_request: ExperimentRunCreateRequest,
) -> None:
    """Delete a run through the SDK."""
    created = await api_client.experiments.start_run(experiment_id, run_request)
    await api_client.experiment_runs.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.experiment_runs.get(created.id)
